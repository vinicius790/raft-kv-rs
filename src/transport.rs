//! TCP transport: 4-byte big-endian length-prefixed bincode frames.
//!
//! * [`Frame`] reads/writes frames on any `AsyncRead`/`AsyncWrite` (TCP, TLS,
//!   `tokio::io::duplex` in tests). One size limit, [`MAX_FRAME`], on both sides.
//! * [`PeerPool`] keeps one persistent connection per peer. A connection
//!   starts with a `PeerHello` frame and then carries any number of messages.
//!   On failure it reconnects with exponential backoff plus jitter; every RPC
//!   write has its own timeout.
//! * Clients still use one request/one reply per connection.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::rpc::{self, Message, NodeId};

/// Largest frame payload accepted or produced (bytes).
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

fn invalid(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}

/// Frame codec. Stateless; all functions are generic over the stream type.
pub struct Frame;

impl Frame {
    /// Read one raw payload (length prefix stripped). Rejects > [`MAX_FRAME`]
    /// before allocating.
    pub async fn read_bytes<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Vec<u8>> {
        let len = r.read_u32().await? as usize;
        if len > MAX_FRAME {
            return Err(invalid("frame too large"));
        }
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf).await?;
        Ok(buf)
    }

    /// Write one raw payload with its length prefix (single write call).
    pub async fn write_bytes<W: AsyncWrite + Unpin>(
        w: &mut W,
        payload: &[u8],
    ) -> std::io::Result<()> {
        if payload.len() > MAX_FRAME {
            return Err(invalid("frame too large"));
        }
        let mut out = Vec::with_capacity(4 + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        w.write_all(&out).await?;
        w.flush().await
    }

    pub async fn read<R: AsyncRead + Unpin>(r: &mut R) -> std::io::Result<Message> {
        let buf = Self::read_bytes(r).await?;
        rpc::decode(&buf).map_err(invalid)
    }

    pub async fn write<W: AsyncWrite + Unpin>(w: &mut W, msg: &Message) -> std::io::Result<()> {
        let payload = bincode::serialize(msg).map_err(invalid)?;
        Self::write_bytes(w, &payload).await
    }
}

/// Read one `Message` frame (kept for API compatibility).
pub async fn read_frame<R: AsyncRead + Unpin>(stream: &mut R) -> std::io::Result<Message> {
    Frame::read(stream).await
}

/// Write one `Message` frame (kept for API compatibility).
pub async fn write_frame<W: AsyncWrite + Unpin>(
    stream: &mut W,
    msg: &Message,
) -> std::io::Result<()> {
    Frame::write(stream, msg).await
}

/// Any bidirectional byte stream (plain TCP or TLS).
pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}
pub type BoxStream = Box<dyn AsyncStream>;

/// Transport security. `Security::default()` is plain TCP.
#[derive(Clone, Default, Debug)]
pub struct Security {
    #[cfg(feature = "tls")]
    pub tls: Option<Arc<crate::tls::TlsMaterial>>,
}

impl Security {
    pub fn plain() -> Self {
        Self::default()
    }

    #[cfg(feature = "tls")]
    pub fn tls(material: crate::tls::TlsMaterial) -> Self {
        Self {
            tls: Some(Arc::new(material)),
        }
    }

    /// Wrap an outbound TCP stream. `name` is the server name to verify.
    async fn connect(&self, tcp: TcpStream, _peer: Option<NodeId>) -> std::io::Result<BoxStream> {
        #[cfg(feature = "tls")]
        if let Some(t) = &self.tls {
            let name = match _peer {
                Some(id) => crate::tls::peer_server_name(id),
                None => crate::tls::client_server_name(),
            };
            let s = t.connector().connect(name, tcp).await?;
            return Ok(Box::new(s));
        }
        Ok(Box::new(tcp))
    }
}

/// Decides whether a connection may speak as peer `id` (checked on `PeerHello`).
pub type Authorize = Box<dyn Fn(NodeId) -> bool + Send + Sync>;

/// Inbound framed message with optional peer id (None for anonymous clients).
#[derive(Debug)]
pub struct Inbound {
    pub from: Option<NodeId>,
    pub msg: Message,
    /// Reply channel for client connections (request/response on same socket).
    pub reply: Option<mpsc::Sender<Message>>,
}

/// Shared map of peer id → address.
#[derive(Clone, Default)]
pub struct PeerMap {
    inner: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
}

impl PeerMap {
    pub fn new(peers: HashMap<NodeId, SocketAddr>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(peers)),
        }
    }

    pub async fn get(&self, id: NodeId) -> Option<SocketAddr> {
        self.inner.lock().await.get(&id).copied()
    }

    pub async fn inner_mut(&self) -> tokio::sync::MutexGuard<'_, HashMap<NodeId, SocketAddr>> {
        self.inner.lock().await
    }

    pub async fn reload(&self, peers: HashMap<NodeId, SocketAddr>) {
        *self.inner.lock().await = peers;
    }
}

/// Magic so PeerHello cannot be confused with a bincode `Message` frame.
const PEER_HELLO_MAGIC: u32 = 0x5241_4654; // "RAFT"

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PeerHello {
    magic: u32,
    id: NodeId,
}

fn hello_bytes(from: NodeId) -> std::io::Result<Vec<u8>> {
    bincode::serialize(&PeerHello {
        magic: PEER_HELLO_MAGIC,
        id: from,
    })
    .map_err(invalid)
}

/// Timeouts and backoff for [`PeerPool`].
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub connect_timeout: Duration,
    /// Per-RPC write timeout.
    pub rpc_timeout: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(500),
            rpc_timeout: Duration::from_millis(800),
            backoff_base: Duration::from_millis(20),
            // Kept below a typical election timeout so a restarted peer is
            // reached again before it campaigns.
            backoff_max: Duration::from_millis(300),
        }
    }
}

#[derive(Default)]
struct PeerConn {
    stream: Option<(SocketAddr, BoxStream)>,
    failures: u32,
    retry_at: Option<Instant>,
}

/// One persistent outbound connection per peer.
#[derive(Clone)]
pub struct PeerPool {
    from: NodeId,
    addrs: PeerMap,
    cfg: PoolConfig,
    security: Security,
    /// Last term in which this node became leader (see [`Self::on_leader_term`]).
    leader_term: Arc<std::sync::atomic::AtomicU64>,
    conns: Arc<Mutex<HashMap<NodeId, Arc<Mutex<PeerConn>>>>>,
}

impl PeerPool {
    pub fn new(from: NodeId, addrs: PeerMap, cfg: PoolConfig) -> Self {
        Self {
            from,
            addrs,
            cfg,
            security: Security::default(),
            leader_term: Arc::default(),
            conns: Arc::default(),
        }
    }

    /// Use TLS (or plain) for every outbound peer connection.
    pub fn with_security(mut self, security: Security) -> Self {
        self.security = security;
        self
    }

    pub fn addrs(&self) -> &PeerMap {
        &self.addrs
    }

    /// Replace the address map. Connections to moved peers are re-dialed on
    /// the next send.
    pub async fn reload(&self, peers: HashMap<NodeId, SocketAddr>) {
        self.addrs.reload(peers).await;
    }

    /// Backoff before retry number `failures` (1-based): base·2^(n-1), capped,
    /// times a uniform jitter in [0.5, 1.5).
    pub fn backoff(&self, failures: u32) -> Duration {
        let exp = self
            .cfg
            .backoff_base
            .saturating_mul(1u32 << failures.saturating_sub(1).min(16));
        let capped = exp.min(self.cfg.backoff_max);
        capped.mul_f64(rand::thread_rng().gen_range(0.5..1.5))
    }

    /// Send one message to `to`. Errors are returned, never retried here:
    /// Raft retransmits on its own timers.
    pub async fn send(&self, to: NodeId, msg: &Message) -> std::io::Result<()> {
        let addr = self.addrs.get(to).await.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, format!("unknown peer {to}"))
        })?;
        let slot = self.conns.lock().await.entry(to).or_default().clone();
        let mut conn = slot.lock().await;
        if conn.stream.as_ref().is_some_and(|(a, _)| *a != addr) {
            conn.stream = None;
        }
        if conn.stream.is_none() {
            if let Some(at) = conn.retry_at {
                if Instant::now() < at {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "peer in backoff",
                    ));
                }
            }
            match self.dial(addr, to).await {
                Ok(s) => conn.stream = Some((addr, s)),
                Err(e) => {
                    self.fail(&mut conn);
                    return Err(e);
                }
            }
        }
        let (_, stream) = conn.stream.as_mut().expect("connected above");
        let res = match tokio::time::timeout(self.cfg.rpc_timeout, Frame::write(stream, msg)).await
        {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "rpc write",
            )),
        };
        match res {
            Ok(()) => {
                conn.failures = 0;
                conn.retry_at = None;
                Ok(())
            }
            Err(e) => {
                debug!(to, error = %e, "peer send failed; dropping connection");
                conn.stream = None;
                self.fail(&mut conn);
                Err(e)
            }
        }
    }

    /// We just heard from `id` (inbound peer message): it is up, so drop any
    /// dial backoff. Without this a peer that started after us could stay
    /// unreachable for a backoff window right after an election, delaying the
    /// new leader's first AppendEntries.
    pub async fn mark_alive(&self, id: NodeId) {
        let slot = self.conns.lock().await.get(&id).cloned();
        if let Some(slot) = slot {
            let mut c = slot.lock().await;
            if c.stream.is_none() {
                c.failures = 0;
                c.retry_at = None;
            }
        }
    }

    /// Called while leader. On the first call for a new term, clear every dial
    /// backoff so the leader's first AppendEntries goes out immediately.
    pub async fn on_leader_term(&self, term: u64) {
        use std::sync::atomic::Ordering;
        if self.leader_term.swap(term, Ordering::AcqRel) == term {
            return;
        }
        let slots: Vec<_> = self.conns.lock().await.values().cloned().collect();
        for slot in slots {
            let mut c = slot.lock().await;
            c.failures = 0;
            c.retry_at = None;
        }
    }

    fn fail(&self, conn: &mut PeerConn) {
        conn.failures = conn.failures.saturating_add(1);
        conn.retry_at = Some(Instant::now() + self.backoff(conn.failures));
    }

    async fn dial(&self, addr: SocketAddr, to: NodeId) -> std::io::Result<BoxStream> {
        let fut = async {
            let tcp = TcpStream::connect(addr).await?;
            tcp.set_nodelay(true)?;
            let mut s = self.security.connect(tcp, Some(to)).await?;
            Frame::write_bytes(&mut s, &hello_bytes(self.from)?).await?;
            Ok::<_, std::io::Error>(s)
        };
        tokio::time::timeout(self.cfg.connect_timeout, fut)
            .await
            .unwrap_or_else(|_| Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "connect")))
    }
}

/// One-shot send (new connection per call). Prefer [`PeerPool::send`].
pub async fn send_peer_message(
    peers: &PeerMap,
    to: NodeId,
    from_id: NodeId,
    msg: Message,
) -> std::io::Result<()> {
    let addr = peers.get(to).await.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("unknown peer {to}"))
    })?;
    let fut = async {
        let mut stream = TcpStream::connect(addr).await?;
        Frame::write_bytes(&mut stream, &hello_bytes(from_id)?).await?;
        Frame::write(&mut stream, &msg).await
    };
    tokio::time::timeout(Duration::from_millis(800), fut)
        .await
        .unwrap_or_else(|_| Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "connect")))
}

/// Accept connections: peer streams (hello + messages) or client (request → reply).
pub async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<Inbound>,
) -> anyhow::Result<()> {
    accept_loop_secure(listener, inbound_tx, Security::default()).await
}

/// Like [`accept_loop`], with TLS when `security` carries material.
pub async fn accept_loop_secure(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<Inbound>,
    security: Security,
) -> anyhow::Result<()> {
    #[cfg(feature = "tls")]
    let acceptor = match &security.tls {
        Some(t) => Some(t.acceptor()?),
        None => None,
    };
    let _ = &security;
    loop {
        let (tcp, addr) = listener.accept().await?;
        let _ = tcp.set_nodelay(true);
        let tx = inbound_tx.clone();
        #[cfg(feature = "tls")]
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            #[cfg(feature = "tls")]
            if let Some(acc) = acceptor {
                let hs = tokio::time::timeout(Duration::from_secs(5), acc.accept(tcp)).await;
                let mut s = match hs {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => return debug!(%addr, error = %e, "tls handshake failed"),
                    Err(_) => return debug!(%addr, "tls handshake timeout"),
                };
                let cert = s
                    .get_ref()
                    .1
                    .peer_certificates()
                    .and_then(|c| c.first())
                    .map(|c| c.clone().into_owned());
                // No client certificate: client path only, never a peer.
                let authorize: Authorize = Box::new(move |id| {
                    cert.as_ref()
                        .is_some_and(|c| crate::tls::cert_names_node(c, id))
                });
                if let Err(e) = handle_connection(&mut s, addr, tx, &authorize).await {
                    debug!(%addr, error = %e, "connection closed/error");
                }
                return;
            }
            let mut tcp = tcp;
            let allow: Authorize = Box::new(|_| true);
            if let Err(e) = handle_connection(&mut tcp, addr, tx, &allow).await {
                debug!(%addr, error = %e, "connection closed/error");
            }
        });
    }
}

/// Serve one accepted connection on any stream type.
pub async fn handle_connection<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    addr: SocketAddr,
    tx: mpsc::Sender<Inbound>,
    authorize: &Authorize,
) -> std::io::Result<()> {
    let buf = Frame::read_bytes(stream).await?;

    // Peer path: first frame is PeerHello with magic, then a stream of messages.
    if let Ok(hello) = bincode::deserialize::<PeerHello>(&buf) {
        if hello.magic == PEER_HELLO_MAGIC {
            if !authorize(hello.id) {
                warn!(%addr, claimed = hello.id, "peer hello refused: identity mismatch");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "peer id not authorized",
                ));
            }
            loop {
                let msg = match Frame::read(stream).await {
                    Ok(m) => m,
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                    Err(e) => return Err(e),
                };
                if tx
                    .send(Inbound {
                        from: Some(hello.id),
                        msg,
                        reply: None,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }

    // Client path: first frame is a Message; we reply on the same socket.
    // This path has no verified peer identity, so it may only carry client
    // requests. Raft RPCs here would let anyone (e.g. a TLS client without a
    // certificate) forge votes, entries or terms.
    let msg = rpc::decode(&buf).map_err(invalid)?;
    if !is_client_request(&msg) {
        warn!(%addr, "peer RPC on unauthenticated client connection; dropping");
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "peer RPC requires an authenticated peer connection",
        ));
    }
    let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(1);
    tx.send(Inbound {
        from: None,
        msg,
        reply: Some(reply_tx),
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "inbound closed"))?;

    match tokio::time::timeout(Duration::from_secs(10), reply_rx.recv()).await {
        Ok(Some(reply)) => Frame::write(stream, &reply).await?,
        Ok(None) => warn!(%addr, "client reply channel closed"),
        Err(_) => warn!(%addr, "client reply timeout"),
    }
    Ok(())
}

/// Default-deny classification: `true` only for messages an anonymous client
/// may send. Exhaustive on purpose: a new `Message` variant must be classified
/// here before the crate compiles.
pub fn is_client_request(msg: &Message) -> bool {
    match msg {
        Message::Client(_)
        | Message::StatusRequest
        | Message::MetricsRequest
        | Message::TraceRequest => true,
        Message::RequestVote { .. }
        | Message::RequestVoteResponse { .. }
        | Message::PreVote { .. }
        | Message::PreVoteResponse { .. }
        | Message::AppendEntries { .. }
        | Message::AppendEntriesResponse { .. }
        | Message::InstallSnapshot { .. }
        | Message::InstallSnapshotResponse { .. }
        | Message::TimeoutNow { .. }
        | Message::ClientReply(_)
        | Message::StatusResponse(_) => false,
    }
}

/// Client helper: send one request, wait for one reply.
pub async fn client_request(
    addr: SocketAddr,
    msg: Message,
    timeout: Duration,
) -> std::io::Result<Message> {
    client_request_secure(addr, msg, timeout, &Security::default()).await
}

/// Client helper over TLS (verifies the server as `raft-kv`) or plain TCP.
pub async fn client_request_secure(
    addr: SocketAddr,
    msg: Message,
    timeout: Duration,
    security: &Security,
) -> std::io::Result<Message> {
    let fut = async {
        let tcp = TcpStream::connect(addr).await?;
        let mut stream = security.connect(tcp, None).await?;
        Frame::write(&mut stream, &msg).await?;
        Frame::read(&mut stream).await
    };
    match tokio::time::timeout(timeout, fut).await {
        Ok(r) => r,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "client request",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn client_path_rejects_peer_rpcs() {
        let (tx, mut rx) = mpsc::channel(4);
        let allow: Authorize = Box::new(|_| true);
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (mut a, mut b) = tokio::io::duplex(1024);
        Frame::write(
            &mut a,
            &Message::TimeoutNow {
                term: 9,
                leader_id: 1,
            },
        )
        .await
        .unwrap();
        let e = handle_connection(&mut b, addr, tx, &allow)
            .await
            .unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(rx.try_recv().is_err(), "nothing may reach the core");
        assert!(is_client_request(&Message::StatusRequest));
    }

    #[tokio::test]
    async fn frame_roundtrip_over_duplex() {
        let (mut a, mut b) = tokio::io::duplex(64);
        let msg = Message::TimeoutNow {
            term: 7,
            leader_id: 2,
        };
        let w = tokio::spawn(async move {
            Frame::write(&mut a, &msg).await.unwrap();
            Frame::write(&mut a, &Message::StatusRequest).await.unwrap();
        });
        assert!(matches!(
            Frame::read(&mut b).await.unwrap(),
            Message::TimeoutNow { term: 7, .. }
        ));
        assert!(matches!(
            Frame::read(&mut b).await.unwrap(),
            Message::StatusRequest
        ));
        w.await.unwrap();
    }

    #[tokio::test]
    async fn oversize_frames_rejected_both_ways() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&((MAX_FRAME as u32) + 1).to_be_bytes())
            .await
            .unwrap();
        let e = Frame::read_bytes(&mut b).await.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        let big = vec![0u8; MAX_FRAME + 1];
        let e = Frame::write_bytes(&mut a, &big).await.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn truncated_frame_is_eof_not_panic() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&10u32.to_be_bytes()).await.unwrap();
        a.write_all(&[1, 2, 3]).await.unwrap();
        drop(a);
        let e = Frame::read(&mut b).await.unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let pool = PeerPool::new(1, PeerMap::default(), PoolConfig::default());
        for _ in 0..50 {
            let b1 = pool.backoff(1);
            assert!(b1 >= Duration::from_millis(10) && b1 < Duration::from_millis(30));
            let b20 = pool.backoff(20);
            assert!(b20 < Duration::from_millis(450), "{b20:?}");
            assert!(b20 >= Duration::from_millis(150));
        }
    }
}

//! TCP transport with 4-byte big-endian length-prefixed bincode frames.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, warn};

use crate::rpc::{self, Message, NodeId};

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

/// Largest frame payload accepted (and sent): 16 MiB.
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

/// Read one length-prefixed Raft `Message` frame.
pub async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Message> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    rpc::decode(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Write one length-prefixed Raft `Message` frame.
pub async fn write_frame(stream: &mut TcpStream, msg: &Message) -> std::io::Result<()> {
    let bytes =
        rpc::encode(msg).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    // `encode` prepends the 4-byte length; the peer refuses payloads above the cap.
    if bytes.len() - 4 > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

/// Magic so PeerHello cannot be confused with a bincode `Message` frame.
const PEER_HELLO_MAGIC: u32 = 0x5241_4654; // "RAFT"

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PeerHello {
    magic: u32,
    id: NodeId,
}

async fn write_raw(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    if payload.len() > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    stream.write_all(&out).await?;
    Ok(())
}

/// Send Raft message to peer with identity preamble (hello frame + message frame).
pub async fn send_peer_message(
    peers: &PeerMap,
    to: NodeId,
    from_id: NodeId,
    msg: Message,
) -> std::io::Result<()> {
    let addr = peers.get(to).await.ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::NotFound, format!("unknown peer {to}"))
    })?;
    match tokio::time::timeout(Duration::from_millis(800), async {
        let mut stream = TcpStream::connect(addr).await?;
        let hello = bincode::serialize(&PeerHello {
            magic: PEER_HELLO_MAGIC,
            id: from_id,
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_raw(&mut stream, &hello).await?;
        write_frame(&mut stream, &msg).await?;
        Ok::<(), std::io::Error>(())
    })
    .await
    {
        Ok(r) => r,
        Err(_) => {
            debug!(to, "peer connect timeout");
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "connect"))
        }
    }
}

/// Accept connections: peer RPCs (hello + message) or client (single request → reply).
pub async fn accept_loop(
    listener: TcpListener,
    inbound_tx: mpsc::Sender<Inbound>,
) -> anyhow::Result<()> {
    loop {
        let (mut stream, addr) = listener.accept().await?;
        let tx = inbound_tx.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut stream, addr, tx).await {
                debug!(%addr, error = %e, "connection closed/error");
            }
        });
    }
}

async fn handle_connection(
    stream: &mut TcpStream,
    addr: SocketAddr,
    tx: mpsc::Sender<Inbound>,
) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    // Peer path: first frame is PeerHello with magic (must not collide with Message).
    if let Ok(hello) = bincode::deserialize::<PeerHello>(&buf) {
        if hello.magic == PEER_HELLO_MAGIC {
            let msg = read_frame(stream).await?;
            let _ = tx
                .send(Inbound {
                    from: Some(hello.id),
                    msg,
                    reply: None,
                })
                .await;
            return Ok(());
        }
    }

    // Client path: first frame is a Message; we reply on the same socket.
    let msg =
        rpc::decode(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let (reply_tx, mut reply_rx) = mpsc::channel::<Message>(1);
    tx.send(Inbound {
        from: None,
        msg,
        reply: Some(reply_tx),
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "inbound closed"))?;

    match tokio::time::timeout(Duration::from_secs(10), reply_rx.recv()).await {
        Ok(Some(reply)) => write_frame(stream, &reply).await?,
        Ok(None) => warn!(%addr, "client reply channel closed"),
        Err(_) => warn!(%addr, "client reply timeout"),
    }
    Ok(())
}

/// Client helper: send one request, wait for one reply.
pub async fn client_request(
    addr: SocketAddr,
    msg: Message,
    timeout: Duration,
) -> std::io::Result<Message> {
    let fut = async {
        let mut stream = TcpStream::connect(addr).await?;
        write_frame(&mut stream, &msg).await?;
        read_frame(&mut stream).await
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
    use crate::rpc::ClientRequest;

    /// Regression: the sender must refuse a frame the receiver would reject
    /// (> 16 MiB) instead of pushing it onto the socket.
    #[tokio::test]
    async fn write_frame_refuses_frame_over_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sink = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 64 * 1024];
            while let Ok(n) = s.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let big = Message::Client(ClientRequest::Put {
            key: "k".into(),
            value: "x".repeat(16 * 1024 * 1024 + 1),
        });
        let err = write_frame(&mut stream, &big)
            .await
            .expect_err("oversized frame was sent");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let small = Message::Client(ClientRequest::Get { key: "k".into() });
        write_frame(&mut stream, &small).await.unwrap();
        drop(stream);
        sink.await.unwrap();
    }
}

//! Raft node: TCP accept loop + election/heartbeat timers + client API.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::raft::{Outbound, RaftCore, Role, TimingConfig};
use crate::rpc::{ClientRequest, ClientResponse, LogIndex, Message, NodeId, NodeStatus};
use crate::storage::Storage;
use crate::transport::{self, Inbound, PeerMap};

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub bind: SocketAddr,
    pub peers: HashMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    pub timing: TimingConfig,
    /// Non-voting members. Their addresses live in `peers` so the transport can dial them.
    pub learners: Vec<NodeId>,
    /// Auto-compact after this many committed entries past the snapshot. 0 = off.
    pub snapshot_threshold: u64,
}

struct PendingPut {
    corr: u64,
    wait_index: LogIndex,
    reply: Option<mpsc::Sender<Message>>,
    oneshot: Option<oneshot::Sender<ClientResponse>>,
    read_key: Option<String>,
    read_term: u64,
}

/// Handle to a running node (for tests and embedding).
pub struct NodeHandle {
    pub id: NodeId,
    pub addr: SocketAddr,
    cmd_tx: mpsc::Sender<NodeCmd>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

enum NodeCmd {
    Client {
        req: ClientRequest,
        resp: oneshot::Sender<ClientResponse>,
    },
    Status {
        resp: oneshot::Sender<NodeStatus>,
    },
    Metrics {
        resp: oneshot::Sender<crate::raft::MetricsSnapshot>,
    },
    ReloadPeers {
        peers: std::collections::HashMap<crate::rpc::NodeId, std::net::SocketAddr>,
    },
}

impl NodeHandle {
    pub async fn put(&self, key: impl Into<String>, value: impl Into<String>) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::Put {
                    key: key.into(),
                    value: value.into(),
                },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    /// Linearizable get (read index). A partitioned leader does not answer.
    pub async fn get(&self, key: impl Into<String>) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::Get { key: key.into() },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    /// Local applied-state read. Not linearizable; does not wait for a quorum.
    pub async fn raw_get(&self, key: impl Into<String>) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::RawGet { key: key.into() },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    pub async fn status(&self) -> NodeStatus {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(NodeCmd::Status { resp: tx }).await;
        rx.await.unwrap_or(NodeStatus {
            id: self.id,
            role: "unknown".into(),
            term: 0,
            leader: None,
            commit_index: 0,
            last_applied: 0,
            log_len: 0,
            kv_len: 0,
            peers: Vec::new(),
            config: String::new(),
            commits: 0,
            elections: 0,
            snapshot_bytes: 0,
        })
    }

    pub async fn metrics(&self) -> crate::raft::MetricsSnapshot {
        // Status path is enough for role/term/commit; metrics live on the core.
        // We expose a dedicated command.
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(NodeCmd::Metrics { resp: tx }).await;
        rx.await.unwrap_or(crate::raft::MetricsSnapshot {
            id: self.id,
            term: 0,
            commit_index: 0,
            last_applied: 0,
            last_log_index: 0,
            role: "unknown".into(),
            replication_lag: Default::default(),
            snapshot_index: 0,
            commits: 0,
            elections: 0,
            snapshot_bytes: 0,
        })
    }

    pub async fn read_index(&self, key: impl Into<String>) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::ReadIndex { key: key.into() },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    pub async fn session_put(
        &self,
        client_id: u64,
        serial: u64,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::SessionPut {
                    client_id,
                    serial,
                    key: key.into(),
                    value: value.into(),
                },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    pub async fn change_membership(&self, voters: Vec<u64>, learners: Vec<u64>) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::ChangeMembership { voters, learners },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    pub async fn promote_learner(&self, id: u64) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::PromoteLearner { id },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    pub async fn transfer_leadership(&self, target: u64) -> ClientResponse {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(NodeCmd::Client {
                req: ClientRequest::TransferLeadership { target },
                resp: tx,
            })
            .await;
        rx.await
            .unwrap_or_else(|_| ClientResponse::err("node gone"))
    }

    /// Replace peer address map without restarting the process.
    pub async fn reload_peers(
        &self,
        peers: std::collections::HashMap<crate::rpc::NodeId, std::net::SocketAddr>,
    ) {
        let _ = self.cmd_tx.send(NodeCmd::ReloadPeers { peers }).await;
    }

    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.join.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(3), h).await;
        }
    }
}

/// Spawn a Raft node on `cfg.bind`. Returns a handle.
pub async fn spawn_node(cfg: NodeConfig) -> anyhow::Result<NodeHandle> {
    let listener = TcpListener::bind(cfg.bind).await?;
    let addr = listener.local_addr()?;
    let learners = cfg.learners.clone();
    let voter_peers: Vec<NodeId> = cfg
        .peers
        .keys()
        .copied()
        .filter(|id| !learners.contains(id))
        .collect();
    let peers = PeerMap::new(cfg.peers.clone());

    let storage = Storage::open(&cfg.data_dir).await?;
    let mut core =
        RaftCore::open_with_learners(cfg.id, voter_peers, learners, storage, cfg.timing.clone())
            .await?;
    core.snapshot_threshold = cfg.snapshot_threshold;

    let (inbound_tx, inbound_rx) = mpsc::channel::<Inbound>(256);
    let (cmd_tx, cmd_rx) = mpsc::channel::<NodeCmd>(64);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let id = cfg.id;
    let join = tokio::spawn(async move {
        let accept = tokio::spawn(transport::accept_loop(listener, inbound_tx));
        if let Err(e) = run_loop(core, peers, inbound_rx, cmd_rx, shutdown_rx).await {
            warn!(node = id, error = %e, "node loop exited with error");
        }
        accept.abort();
    });

    // Tiny settle so accept is ready.
    tokio::time::sleep(Duration::from_millis(20)).await;

    Ok(NodeHandle {
        id: cfg.id,
        addr,
        cmd_tx,
        shutdown_tx: Mutex::new(Some(shutdown_tx)),
        join: Mutex::new(Some(join)),
    })
}

async fn run_loop(
    mut core: RaftCore,
    peers: PeerMap,
    mut inbound_rx: mpsc::Receiver<Inbound>,
    mut cmd_rx: mpsc::Receiver<NodeCmd>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let election = tokio::time::sleep(core.random_election_timeout());
    tokio::pin!(election);
    let heartbeat = tokio::time::sleep(Duration::from_secs(3600));
    tokio::pin!(heartbeat);

    let mut pending: Vec<PendingPut> = Vec::new();
    let corr_gen = AtomicU64::new(1);
    // Map corr → pending slot for inbound client replies
    let mut corr_replies: HashMap<u64, PendingPut> = HashMap::new();

    info!(node = core.id, "raft node online");

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!(node = core.id, "shutdown");
                break;
            }
            _ = &mut election => {
                let out = core.on_election_timeout().await?;
                apply_outbound(&mut core, &peers, out, &mut election, &mut heartbeat, &mut pending, &mut corr_replies).await;
                core.maybe_auto_snapshot().await?;
            }
            _ = &mut heartbeat => {
                let out = core.on_heartbeat_timeout().await?;
                apply_outbound(&mut core, &peers, out, &mut election, &mut heartbeat, &mut pending, &mut corr_replies).await;
                core.maybe_auto_snapshot().await?;
            }
            Some(inbound) = inbound_rx.recv() => {
                let corr = corr_gen.fetch_add(1, Ordering::Relaxed);
                if let Some(reply) = inbound.reply {
                    corr_replies.insert(corr, PendingPut {
                        corr,
                        wait_index: 0,
                        reply: Some(reply),
                        oneshot: None,
                        read_key: None,
                        read_term: 0,
                    });
                }
                let out = core.handle_message(inbound.from, inbound.msg, corr).await?;
                apply_outbound(&mut core, &peers, out, &mut election, &mut heartbeat, &mut pending, &mut corr_replies).await;
                core.maybe_auto_snapshot().await?;
                flush_pending_commits(&core, &mut pending, &mut corr_replies).await;
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    NodeCmd::Status { resp } => {
                        let _ = resp.send(core.status());
                    }
                    NodeCmd::Metrics { resp } => {
                        let _ = resp.send(core.metrics());
                    }
                    NodeCmd::ReloadPeers { peers: new_peers } => {
                        let map = new_peers.iter().map(|(id, addr)| (*id, addr.to_string())).collect();
                        core.reload_peer_addrs(map);
                        peers.reload(new_peers).await;
                    }
                    NodeCmd::Client { req, resp } => {
                        let corr = corr_gen.fetch_add(1, Ordering::Relaxed);
                        corr_replies.insert(corr, PendingPut {
                            corr,
                            wait_index: 0,
                            reply: None,
                            oneshot: Some(resp),
                            read_key: None,
                            read_term: 0,
                        });
                        let out = core.handle_message(None, Message::Client(req), corr).await?;
                        apply_outbound(&mut core, &peers, out, &mut election, &mut heartbeat, &mut pending, &mut corr_replies).await;
                        core.maybe_auto_snapshot().await?;
                        flush_pending_commits(&core, &mut pending, &mut corr_replies).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn apply_outbound(
    core: &mut RaftCore,
    peers: &PeerMap,
    out: Vec<Outbound>,
    election: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    heartbeat: &mut std::pin::Pin<&mut tokio::time::Sleep>,
    pending: &mut Vec<PendingPut>,
    corr_replies: &mut HashMap<u64, PendingPut>,
) {
    for ob in out {
        match ob {
            Outbound::Send { to, msg } => {
                let peers = peers.clone();
                let from = core.id;
                tokio::spawn(async move {
                    let _ = transport::send_peer_message(&peers, to, from, msg).await;
                });
            }
            Outbound::ClientReply { corr, resp } => {
                if let Some(mut p) = corr_replies.remove(&corr) {
                    deliver_client(&mut p, resp).await;
                } else if let Some(idx) = pending.iter().position(|p| p.corr == corr) {
                    let mut p = pending.remove(idx);
                    deliver_client(&mut p, resp).await;
                }
            }
            Outbound::ClientWaitCommit { corr, wait_index } => {
                if let Some(mut p) = corr_replies.remove(&corr) {
                    p.wait_index = wait_index;
                    pending.push(p);
                }
            }
            Outbound::ClientWaitRead {
                corr,
                key,
                read_term,
            } => {
                // Core already tracks the read; keep the reply channel until commit/quorum.
                if let Some(mut p) = corr_replies.remove(&corr) {
                    p.wait_index = 0;
                    p.read_key = Some(key);
                    p.read_term = read_term;
                    pending.push(p);
                }
            }
            Outbound::ForceElectionTimeout => {
                election
                    .as_mut()
                    .reset(tokio::time::Instant::now() + Duration::from_millis(1));
            }
            Outbound::ResetElectionTimer => {
                let wait = if core.is_leader() && core.enable_check_quorum {
                    core.check_quorum_timeout()
                } else {
                    core.random_election_timeout()
                };
                election.as_mut().reset(tokio::time::Instant::now() + wait);
            }
            Outbound::ResetHeartbeatTimer => {
                heartbeat
                    .as_mut()
                    .reset(tokio::time::Instant::now() + core.heartbeat_interval());
                // Without check-quorum, a leader must not campaign on the election timer.
                if core.is_leader() && !core.enable_check_quorum {
                    election
                        .as_mut()
                        .reset(tokio::time::Instant::now() + Duration::from_secs(3600));
                }
            }
        }
    }
    if core.volatile.role == Role::Leader && !core.enable_check_quorum {
        election
            .as_mut()
            .reset(tokio::time::Instant::now() + Duration::from_secs(3600));
    }
}

async fn flush_pending_commits(
    core: &RaftCore,
    pending: &mut Vec<PendingPut>,
    _corr_replies: &mut HashMap<u64, PendingPut>,
) {
    let commit = core.commit_index();
    let mut still = Vec::new();
    for mut p in pending.drain(..) {
        if p.wait_index > 0 && commit >= p.wait_index {
            deliver_client(&mut p, ClientResponse::ok_put()).await;
        } else {
            still.push(p);
        }
    }
    *pending = still;
}

async fn deliver_client(p: &mut PendingPut, resp: ClientResponse) {
    if let Some(tx) = p.oneshot.take() {
        let _ = tx.send(resp.clone());
    }
    if let Some(tx) = p.reply.take() {
        let _ = tx.send(Message::ClientReply(resp)).await;
    }
}

/// Convenience: spawn a local 3-node cluster for tests/demos.
pub async fn spawn_cluster(
    base_port: u16,
    data_root: PathBuf,
    timing: TimingConfig,
) -> anyhow::Result<Vec<NodeHandle>> {
    let addrs: Vec<(NodeId, SocketAddr)> = (0..3)
        .map(|i| {
            (
                (i + 1) as NodeId,
                format!("127.0.0.1:{}", base_port + i as u16)
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    let mut handles = Vec::new();
    for (id, bind) in &addrs {
        let mut peers = HashMap::new();
        for (oid, oaddr) in &addrs {
            if oid != id {
                peers.insert(*oid, *oaddr);
            }
        }
        let cfg = NodeConfig {
            id: *id,
            bind: *bind,
            peers,
            data_dir: data_root.join(format!("node-{id}")),
            timing: timing.clone(),
            learners: Vec::new(),
            snapshot_threshold: 0,
        };
        handles.push(spawn_node(cfg).await?);
    }
    Ok(handles)
}

/// Find current leader among handles (poll status).
pub async fn wait_for_leader(handles: &[NodeHandle], timeout: Duration) -> Option<NodeId> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout {
        for h in handles {
            let st = h.status().await;
            if st.role == "Leader" {
                return Some(st.id);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// Put via any node, following leader redirects.
pub async fn put_redirect(
    handles: &[NodeHandle],
    start_id: NodeId,
    key: &str,
    value: &str,
) -> ClientResponse {
    let mut id = start_id;
    for _ in 0..8 {
        let h = match handles.iter().find(|h| h.id == id) {
            Some(h) => h,
            None => return ClientResponse::err("no handle"),
        };
        let resp = h.put(key, value).await;
        if resp.ok {
            return resp;
        }
        if let Some(hint) = resp.leader_hint {
            id = hint;
            continue;
        }
        // Try every node
        for h in handles {
            let r = h.put(key, value).await;
            if r.ok {
                return r;
            }
            if let Some(hint) = r.leader_hint {
                id = hint;
            }
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    ClientResponse::err("put failed after redirects")
}

pub async fn get_redirect(handles: &[NodeHandle], start_id: NodeId, key: &str) -> ClientResponse {
    let mut id = start_id;
    for _ in 0..8 {
        let h = match handles.iter().find(|h| h.id == id) {
            Some(h) => h,
            None => return ClientResponse::err("no handle"),
        };
        let resp = h.get(key).await;
        if resp.ok {
            return resp;
        }
        if let Some(hint) = resp.leader_hint {
            id = hint;
            continue;
        }
        for h in handles {
            let r = h.get(key).await;
            if r.ok {
                return r;
            }
            if let Some(hint) = r.leader_hint {
                id = hint;
            }
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    ClientResponse::err("get failed after redirects")
}

/// Public alias matching the suggested layout.
pub struct RaftNode;

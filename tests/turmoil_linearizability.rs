//! Raft cores over a simulated network (turmoil), driven by real tokio timers
//! in simulated time, with random partitions. Every client operation is
//! recorded and the history is checked for linearizability.
//!
//! Wire: `[u64 BE from][u32 BE len][bincode Message]` over turmoil TCP.
//! `from = 0` marks a client; the node answers on the same connection.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use raft_mvp::linearizability::{self, Op, OpKind};
use raft_mvp::raft::{Outbound, RaftCore, TimingConfig};
use raft_mvp::rpc::{self, ClientRequest, ClientResponse, Message, NodeId};
use raft_mvp::storage::Storage;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use turmoil::net::{TcpListener, TcpStream};

const PORT: u16 = 7000;
const MAX_FRAME: usize = 16 * 1024 * 1024;
const NODES: [NodeId; 3] = [1, 2, 3];

fn host(id: NodeId) -> String {
    format!("n{id}")
}

async fn write_msg(s: &mut TcpStream, from: u64, msg: &Message) -> std::io::Result<()> {
    let frame = rpc::encode(msg).map_err(std::io::Error::other)?;
    let mut buf = from.to_be_bytes().to_vec();
    buf.extend_from_slice(&frame);
    s.write_all(&buf).await
}

async fn read_msg(s: &mut TcpStream) -> std::io::Result<(u64, Message)> {
    let from = s.read_u64().await?;
    let len = s.read_u32().await? as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::other("frame too large"));
    }
    let mut buf = vec![0; len];
    s.read_exact(&mut buf).await?;
    Ok((from, rpc::decode(&buf).map_err(std::io::Error::other)?))
}

enum Ev {
    Peer(NodeId, Message),
    Client(ClientRequest, oneshot::Sender<ClientResponse>),
}

enum Waiter {
    /// (index, term the entry was appended in, reply)
    Commit(LogIdx, u64, oneshot::Sender<ClientResponse>),
    Read(oneshot::Sender<ClientResponse>),
}
type LogIdx = u64;

async fn run_node(id: NodeId) -> turmoil::Result {
    let peers: Vec<NodeId> = NODES.iter().copied().filter(|p| *p != id).collect();
    let timing = TimingConfig {
        heartbeat_ms: 30,
        election_min_ms: 150,
        election_max_ms: 300,
    };
    let mut core = RaftCore::open(id, peers, Storage::ephemeral(), timing).await?;
    let (tx, mut rx) = mpsc::channel::<Ev>(1024);

    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let tx = tx.clone();
            tokio::spawn(async move {
                let Ok((from, msg)) = read_msg(&mut s).await else {
                    return;
                };
                if from != 0 {
                    let _ = tx.send(Ev::Peer(from, msg)).await;
                    return;
                }
                let Message::Client(req) = msg else { return };
                let (otx, orx) = oneshot::channel();
                if tx.send(Ev::Client(req, otx)).await.is_err() {
                    return;
                }
                if let Ok(resp) = orx.await {
                    let _ = write_msg(&mut s, 0, &Message::ClientReply(resp)).await;
                }
            });
        }
    });

    let election = tokio::time::sleep(core.random_election_timeout());
    tokio::pin!(election);
    let heartbeat = tokio::time::sleep(Duration::from_secs(3600));
    tokio::pin!(heartbeat);
    let mut corr = 1u64;
    let mut waiters: HashMap<u64, Waiter> = HashMap::new();
    let mut fresh: Option<oneshot::Sender<ClientResponse>>;

    loop {
        fresh = None;
        let my_corr = corr;
        corr += 1;
        let out = tokio::select! {
            _ = &mut election => core.on_election_timeout().await?,
            _ = &mut heartbeat => core.on_heartbeat_timeout().await?,
            Some(ev) = rx.recv() => match ev {
                Ev::Peer(from, msg) => core.handle_message(Some(from), msg, my_corr).await?,
                Ev::Client(req, reply) => {
                    fresh = Some(reply);
                    core.handle_message(None, Message::Client(req), my_corr).await?
                }
            },
        };
        for ob in out {
            match ob {
                Outbound::Send { to, msg } => {
                    tokio::spawn(async move {
                        let fut = async {
                            let mut s = TcpStream::connect((host(to), PORT)).await?;
                            write_msg(&mut s, id, &msg).await
                        };
                        let _ = tokio::time::timeout(Duration::from_millis(200), fut).await;
                    });
                }
                Outbound::ClientReply { corr: c, resp } => {
                    if c == my_corr {
                        if let Some(r) = fresh.take() {
                            let _ = r.send(resp);
                        }
                    } else if let Some(Waiter::Read(r) | Waiter::Commit(_, _, r)) =
                        waiters.remove(&c)
                    {
                        let _ = r.send(resp);
                    }
                }
                Outbound::ClientWaitCommit {
                    corr: c,
                    wait_index,
                } => {
                    if let (true, Some(r)) = (c == my_corr, fresh.take()) {
                        waiters.insert(
                            c,
                            Waiter::Commit(wait_index, core.persistent.current_term, r),
                        );
                    }
                }
                Outbound::ClientWaitRead { corr: c, .. } => {
                    if let (true, Some(r)) = (c == my_corr, fresh.take()) {
                        waiters.insert(c, Waiter::Read(r));
                    }
                }
                Outbound::ForceElectionTimeout => election
                    .as_mut()
                    .reset(tokio::time::Instant::now() + Duration::from_millis(1)),
                Outbound::ResetElectionTimer => {
                    let wait = if core.is_leader() && core.enable_check_quorum {
                        core.check_quorum_timeout()
                    } else {
                        core.random_election_timeout()
                    };
                    election.as_mut().reset(tokio::time::Instant::now() + wait);
                }
                Outbound::ResetHeartbeatTimer => heartbeat
                    .as_mut()
                    .reset(tokio::time::Instant::now() + core.heartbeat_interval()),
            }
        }
        // A fired timer that the core did not re-arm must not fire again
        // immediately: simulated time only advances between steps.
        if election.is_elapsed() {
            election
                .as_mut()
                .reset(tokio::time::Instant::now() + core.random_election_timeout());
        }
        if heartbeat.is_elapsed() {
            let next = if core.is_leader() {
                core.heartbeat_interval()
            } else {
                Duration::from_secs(3600)
            };
            heartbeat.as_mut().reset(tokio::time::Instant::now() + next);
        }
        if core.is_leader() && !core.enable_check_quorum {
            election
                .as_mut()
                .reset(tokio::time::Instant::now() + Duration::from_secs(3600));
        }
        // A write completes once *its* entry is committed: the index must be
        // committed and still hold the term it was appended in. If a new leader
        // overwrote that slot, the write was lost and the client is told so.
        let commit = core.commit_index();
        let log = &core.persistent.log;
        let done: Vec<(u64, bool)> = waiters
            .iter()
            .filter_map(|(c, w)| match w {
                Waiter::Commit(i, t, _) => match log.term_at(*i) {
                    Some(term) if term != *t => Some((*c, false)),
                    Some(_) if *i <= commit => Some((*c, true)),
                    _ => None,
                },
                Waiter::Read(_) => None,
            })
            .collect();
        for (c, ok) in done {
            if let Some(Waiter::Commit(_, _, r)) = waiters.remove(&c) {
                let _ = r.send(if ok {
                    ClientResponse::ok_put()
                } else {
                    ClientResponse::err("entry superseded")
                });
            }
        }
    }
}

async fn call(node: NodeId, req: ClientRequest) -> Option<ClientResponse> {
    let fut = async {
        let mut s = TcpStream::connect((host(node), PORT)).await?;
        write_msg(&mut s, 0, &Message::Client(req)).await?;
        match read_msg(&mut s).await?.1 {
            Message::ClientReply(r) => Ok(r),
            _ => Err(std::io::Error::other("unexpected")),
        }
    };
    tokio::time::timeout(Duration::from_millis(600), fut)
        .await
        .ok()?
        .ok()
}

type History = Arc<Mutex<Vec<Op>>>;

/// Global simulated time, identical for every host.
fn now_us() -> u64 {
    turmoil::sim_elapsed().unwrap_or_default().as_micros() as u64
}

async fn client(cid: u64, seed: u64, ops: usize, history: History) -> turmoil::Result {
    let mut rng = StdRng::seed_from_u64(seed ^ (cid << 32));
    let mut target = NODES[rng.gen_range(0..3)];
    tokio::time::sleep(Duration::from_millis(400)).await;
    for n in 0..ops {
        let key = format!("k{}", rng.gen_range(0..3));
        let write = rng.gen_bool(0.5);
        let value = format!("c{cid}-{n}");
        let req = if write {
            ClientRequest::Put {
                key: key.clone(),
                value: value.clone(),
            }
        } else {
            ClientRequest::Get { key: key.clone() }
        };
        let call_t = now_us();
        let mut outcome: Option<ClientResponse> = None;
        let mut unknown = false;
        for _ in 0..10 {
            match call(target, req.clone()).await {
                Some(r) if r.ok => {
                    outcome = Some(r);
                    break;
                }
                Some(r) if r.error.as_deref() == Some("not leader") || r.leader_hint.is_some() => {
                    // Definite rejection: nothing was appended, safe to retry.
                    target = r.leader_hint.unwrap_or(NODES[rng.gen_range(0..3)]);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                _ => {
                    // Timeout / unknown: a write might still commit, so never retry it.
                    unknown = true;
                    target = NODES[rng.gen_range(0..3)];
                    if write {
                        break;
                    }
                }
            }
        }
        let ret = now_us();
        let op = match (write, outcome) {
            (true, Some(_)) => Some((OpKind::Write(Some(value)), Some(ret))),
            (true, None) if unknown => Some((OpKind::Write(Some(value)), None)),
            (true, None) => None, // only definite rejections: never applied
            (false, Some(r)) => Some((OpKind::Read(r.value), Some(ret))),
            (false, None) => None, // failed read carries no information
        };
        if let Some((kind, ret)) = op {
            history.lock().unwrap().push(Op {
                key,
                kind,
                call: call_t,
                ret,
            });
        }
        tokio::time::sleep(Duration::from_millis(rng.gen_range(5..60))).await;
    }
    Ok(())
}

fn run_seed(seed: u64) -> Vec<Op> {
    let history: History = Arc::default();
    let mut sim = turmoil::Builder::new()
        .simulation_duration(Duration::from_secs(60))
        .rng_seed(seed)
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(15))
        .enable_random_order()
        .build();
    for id in NODES {
        sim.host(host(id), move || run_node(id));
    }
    for cid in 1..=3u64 {
        let h = history.clone();
        sim.client(format!("c{cid}"), client(cid, seed, 25, h));
    }
    // Fault schedule: isolate one node at a time, then heal.
    let mut rng = StdRng::seed_from_u64(seed);
    let mut step = 0u32;
    loop {
        step += 1;
        if step % 700 == 100 {
            let victim = host(NODES[rng.gen_range(0..3)]);
            for id in NODES {
                if host(id) != victim {
                    sim.partition(victim.clone(), host(id));
                }
            }
        }
        if step % 700 == 550 {
            for a in NODES {
                for b in NODES {
                    if a < b {
                        sim.repair(host(a), host(b));
                    }
                }
            }
        }
        match sim.step() {
            Ok(true) => break,
            Ok(false) => {}
            Err(e) => panic!("seed {seed}: simulation failed: {e}"),
        }
    }
    let h = history.lock().unwrap().clone();
    h
}

#[test]
fn histories_under_partitions_are_linearizable() {
    let seeds: u64 = std::env::var("TURMOIL_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);
    let mut completed = 0;
    for seed in 1..=seeds {
        let h = run_seed(seed);
        completed += h.iter().filter(|o| o.ret.is_some()).count();
        eprintln!(
            "seed {seed}: {} ops, {} unknown",
            h.len(),
            h.iter().filter(|o| o.ret.is_none()).count()
        );
        linearizability::check(&h).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
    // The run must have done real work, not just time out everywhere.
    assert!(
        completed >= seeds as usize * 10,
        "only {completed} ops completed"
    );
}

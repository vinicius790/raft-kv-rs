//! Concurrent client Puts must be replicated in batches: far fewer
//! entry-carrying AppendEntries than writes. Peers 2 and 3 are scripted fakes
//! that grant votes and ack every AppendEntries, counting what they receive.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use raft_mvp::node::{spawn_node, NodeConfig};
use raft_mvp::raft::TimingConfig;
use raft_mvp::rpc::Message;
use raft_mvp::transport::{self, PeerMap};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// Fake voter: grants (pre)votes and acks appends. Returns the count of
/// AppendEntries messages that carried at least one entry.
async fn fake_peer(
    id: u64,
    listener: TcpListener,
    leader: PeerMap,
    appends: Arc<AtomicUsize>,
    entries: Arc<AtomicUsize>,
) {
    loop {
        let Ok((mut s, _)) = listener.accept().await else {
            return;
        };
        let leader = leader.clone();
        let appends = appends.clone();
        let entries = entries.clone();
        tokio::spawn(async move {
            // Hello frame (peer id), then one or more messages.
            let len = s.read_u32().await.unwrap_or(0) as usize;
            let mut hello = vec![0; len];
            if s.read_exact(&mut hello).await.is_err() {
                return;
            }
            while let Ok(msg) = transport::read_frame(&mut s).await {
                let reply = match msg {
                    Message::PreVote { next_term, .. } => Message::PreVoteResponse {
                        next_term,
                        vote_granted: true,
                    },
                    Message::RequestVote { term, .. } => Message::RequestVoteResponse {
                        term,
                        vote_granted: true,
                    },
                    Message::AppendEntries {
                        term,
                        prev_log_index,
                        entries: e,
                        ..
                    } => {
                        if !e.is_empty() {
                            appends.fetch_add(1, Ordering::Relaxed);
                            entries.fetch_add(e.len(), Ordering::Relaxed);
                        }
                        Message::AppendEntriesResponse {
                            term,
                            success: true,
                            match_index: prev_log_index + e.len() as u64,
                            conflict_index: 0,
                            need_snapshot: false,
                        }
                    }
                    _ => continue,
                };
                let _ = transport::send_peer_message(&leader, 1, id, reply).await;
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_puts_are_batched() {
    const N: usize = 200;
    let dir = tempfile::tempdir().unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a1: SocketAddr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let leader_map = PeerMap::new(HashMap::from([(1, a1)]));
    let appends = Arc::new(AtomicUsize::new(0));
    let entries = Arc::new(AtomicUsize::new(0));
    let peers = HashMap::from([(2, l2.local_addr().unwrap()), (3, l3.local_addr().unwrap())]);
    tokio::spawn(fake_peer(
        2,
        l2,
        leader_map.clone(),
        appends.clone(),
        entries.clone(),
    ));
    tokio::spawn(fake_peer(3, l3, leader_map, Arc::default(), Arc::default()));

    let node = spawn_node(NodeConfig {
        id: 1,
        bind: a1,
        peers,
        data_dir: dir.path().join("n1"),
        timing: TimingConfig {
            heartbeat_ms: 50,
            election_min_ms: 100,
            election_max_ms: 200,
        },
        learners: Vec::new(),
        snapshot_threshold: 0,
    })
    .await
    .unwrap();
    for _ in 0..100 {
        if node.status().await.role == "Leader" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(node.status().await.role, "Leader");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let base_appends = appends.load(Ordering::Relaxed);

    let node = Arc::new(node);
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..N {
        let n = node.clone();
        tasks.spawn(async move { n.put(format!("k{i}"), "v").await.ok });
    }
    while let Some(ok) = tasks.join_next().await {
        assert!(ok.unwrap(), "put failed");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let sent = appends.load(Ordering::Relaxed) - base_appends;
    let carried = entries.load(Ordering::Relaxed);
    eprintln!("{N} puts -> {sent} entry-carrying AppendEntries to peer 2 ({carried} entries)");
    assert!(
        sent * 4 <= N,
        "{N} concurrent puts produced {sent} AppendEntries (want <= {})",
        N / 4
    );
    // No duplicated payload: total entries shipped stay close to N.
    assert!(
        carried <= N * 2,
        "resent too much: {carried} entries for {N} puts"
    );

    if let Ok(n) = Arc::try_unwrap(node) {
        n.shutdown().await;
    }
}

//! Stretch: a minority partition must not commit (no split-brain commit).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use raft_kv_rs::node::{spawn_node, wait_for_leader, NodeConfig, NodeHandle};
use raft_kv_rs::raft::TimingConfig;

fn fast_timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 40,
        election_min_ms: 100,
        election_max_ms: 220,
    }
}

/// Ask the OS for free loopback ports (bind 127.0.0.1:0), then release them
/// so the nodes can bind. No fixed ports, so parallel runs do not collide.
fn ephemeral_addrs(n: usize) -> Vec<SocketAddr> {
    let listeners: Vec<std::net::TcpListener> = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

async fn spawn_three(root: &std::path::Path) -> Vec<NodeHandle> {
    let addrs: Vec<(u64, SocketAddr)> = ephemeral_addrs(3)
        .into_iter()
        .enumerate()
        .map(|(i, a)| ((i + 1) as u64, a))
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
            data_dir: root.join(format!("node-{id}")),
            timing: fast_timing(),
            learners: Vec::new(),
            snapshot_threshold: 0,
        };
        handles.push(spawn_node(cfg).await.unwrap());
    }
    handles
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_put_does_not_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut handles = spawn_three(dir.path()).await;
    let _leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader");

    // Keep only node 1; shut down 2 and 3 → minority of 1 in a cluster of 3.
    let mut alone = None;
    let mut rest = Vec::new();
    for h in handles.drain(..) {
        if h.id == 1 {
            alone = Some(h);
        } else {
            rest.push(h);
        }
    }
    for h in rest {
        h.shutdown().await;
    }
    let alone = alone.unwrap();

    tokio::time::sleep(Duration::from_millis(700)).await;

    let put_fut = alone.put("split", "brain");
    let result = tokio::time::timeout(Duration::from_secs(2), put_fut).await;

    match result {
        Err(_) => {
            // Timed out waiting for commit → majority never ack'd (correct).
        }
        Ok(resp) => {
            assert!(
                !resp.ok,
                "minority must not commit Put, got ok err={:?}",
                resp.error
            );
        }
    }

    let st = alone.status().await;
    assert_eq!(
        st.kv_len, 0,
        "minority must not apply uncommitted Put (kv_len={})",
        st.kv_len
    );

    alone.shutdown().await;
}

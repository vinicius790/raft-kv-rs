//! A Put accepted by a leader that is then deposed must not be acknowledged
//! when a *different* entry later commits at the same index.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use raft_kv_rs::node::{spawn_node, NodeConfig, NodeHandle};
use raft_kv_rs::raft::TimingConfig;

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

async fn leader_of(nodes: &[&NodeHandle], timeout: Duration) -> Option<u64> {
    let start = tokio::time::Instant::now();
    while start.elapsed() < timeout {
        for n in nodes {
            let st = n.status().await;
            if st.role == "Leader" {
                return Some(st.id);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deposed_leader_does_not_ack_overwritten_put() {
    let dir = tempfile::tempdir().unwrap();
    let addrs: HashMap<u64, SocketAddr> = (1..=3).map(|i| (i, free_addr())).collect();
    let peers_of = |id: u64, cut: Option<u64>| -> HashMap<u64, SocketAddr> {
        addrs
            .iter()
            .filter(|(o, _)| **o != id)
            .map(|(o, a)| {
                let isolated = cut == Some(id) || cut == Some(*o);
                (
                    *o,
                    if isolated {
                        "127.0.0.1:1".parse().unwrap()
                    } else {
                        *a
                    },
                )
            })
            .collect()
    };
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        nodes.push(
            spawn_node(NodeConfig {
                id,
                bind: addrs[&id],
                peers: peers_of(id, None),
                data_dir: dir.path().join(format!("n{id}")),
                timing: TimingConfig {
                    heartbeat_ms: 30,
                    election_min_ms: 150,
                    election_max_ms: 300,
                },
                learners: Vec::new(),
                snapshot_threshold: 0,
            })
            .await
            .unwrap(),
        );
    }
    let all: Vec<&NodeHandle> = nodes.iter().collect();
    let old = leader_of(&all, Duration::from_secs(5))
        .await
        .expect("leader");
    assert!(
        nodes
            .iter()
            .find(|n| n.id == old)
            .unwrap()
            .put("warm", "up")
            .await
            .ok
    );

    // Isolate the leader in both directions.
    for n in &nodes {
        n.reload_peers(peers_of(n.id, Some(old))).await;
    }
    // The isolated leader appends the Put but cannot commit it. Meanwhile the
    // majority elects a new leader whose entries overwrite that index, then
    // the partition heals and the old leader's commit index passes it.
    let old_node = nodes.iter().find(|n| n.id == old).unwrap();
    let put = tokio::time::timeout(Duration::from_secs(8), old_node.put("lost", "v"));
    let majority: Vec<&NodeHandle> = nodes.iter().filter(|n| n.id != old).collect();
    let rest = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut new = None;
        for _ in 0..100 {
            if let Some(l) = leader_of(&majority, Duration::from_millis(100)).await {
                new = Some(l);
                break;
            }
        }
        let new = new.expect("new leader");
        let nl = nodes.iter().find(|n| n.id == new).unwrap();
        for i in 0..3 {
            assert!(nl.put(format!("x{i}"), "y").await.ok);
        }
        for n in &nodes {
            n.reload_peers(peers_of(n.id, None)).await;
        }
        nl
    };
    let (resp, nl) = tokio::join!(put, rest);
    let resp = resp.expect("old leader must answer once its entry is overwritten");
    {
        assert!(
            !resp.ok,
            "Put acknowledged although its entry was overwritten by the new leader"
        );
    }
    let got = nl.get("lost").await;
    assert!(got.ok);
    assert_eq!(got.value, None, "the lost Put must really be lost");

    for n in nodes {
        n.shutdown().await;
    }
}

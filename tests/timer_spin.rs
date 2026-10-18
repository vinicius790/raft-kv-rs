//! A node that stops being leader must not spin on an already-fired
//! heartbeat timer (tokio `Sleep` stays ready once elapsed).

#![cfg(target_os = "linux")]

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

/// User+system CPU time of this process, in clock ticks (/proc/self/stat).
fn cpu_ticks() -> u64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
    let after = &stat[stat.rfind(')').unwrap() + 2..];
    let f: Vec<&str> = after.split_whitespace().collect();
    // Fields 14 and 15 overall; index 11 and 12 after "pid (comm) ".
    f[11].parse::<u64>().unwrap() + f[12].parse::<u64>().unwrap()
}

async fn leader(nodes: &[NodeHandle]) -> u64 {
    for _ in 0..250 {
        for n in nodes {
            if n.status().await.role == "Leader" {
                return n.id;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("no leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn former_leader_does_not_busy_loop() {
    let dir = tempfile::tempdir().unwrap();
    let addrs: HashMap<u64, SocketAddr> = (1..=3).map(|i| (i, free_addr())).collect();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let peers = addrs
            .iter()
            .filter(|(o, _)| **o != id)
            .map(|(o, a)| (*o, *a))
            .collect();
        nodes.push(
            spawn_node(NodeConfig {
                id,
                bind: addrs[&id],
                peers,
                data_dir: dir.path().join(format!("n{id}")),
                timing: TimingConfig {
                    heartbeat_ms: 40,
                    election_min_ms: 200,
                    election_max_ms: 400,
                },
                learners: Vec::new(),
                snapshot_threshold: 0,
            })
            .await
            .unwrap(),
        );
    }
    let old = leader(&nodes).await;
    let target = (1..=3).find(|i| *i != old).unwrap();
    let ln = nodes.iter().find(|n| n.id == old).unwrap();
    assert!(ln.put("k", "v").await.ok);
    // "target not caught up" is a retryable answer: the put may have
    // committed on the other follower before `target` acked it.
    let mut r = ln.transfer_leadership(target).await;
    for _ in 0..50 {
        if r.ok || r.error.as_deref() != Some("target not caught up") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        r = ln.transfer_leadership(target).await;
    }
    assert!(r.ok, "transfer: {:?}", r.error);
    for _ in 0..100 {
        if ln.status().await.role != "Leader" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_ne!(ln.status().await.role, "Leader");

    // Idle cluster: three nodes heartbeating at 25 Hz need a few ms of CPU.
    // A spinning select loop burns a whole core (~100 ticks per second).
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = cpu_ticks();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let used = cpu_ticks() - before;
    assert!(
        used < 40,
        "idle cluster used {used} CPU ticks in 1s: busy loop"
    );

    for n in nodes {
        n.shutdown().await;
    }
}

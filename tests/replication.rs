//! Integration: Put/Get replication across 3 nodes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use raft_kv_rs::node::{
    get_redirect, put_redirect, spawn_cluster, spawn_node, wait_for_leader, NodeConfig, NodeHandle,
};
use raft_kv_rs::raft::TimingConfig;

fn fast_timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 40,
        election_min_ms: 120,
        election_max_ms: 250,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_get_replicated() {
    let dir = tempfile::tempdir().unwrap();
    let handles = spawn_cluster(0, dir.path().to_path_buf(), fast_timing())
        .await
        .expect("cluster");
    let leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader");

    let resp = put_redirect(&handles, leader, "alpha", "1").await;
    assert!(resp.ok, "put failed: {:?}", resp.error);

    // Read from leader
    let got = get_redirect(&handles, leader, "alpha").await;
    assert!(got.ok, "get failed: {:?}", got.error);
    assert_eq!(got.value.as_deref(), Some("1"));

    // Give followers time to apply via commit heartbeat
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Every node that is still reachable should eventually have applied
    // (followers apply on leader_commit). Check leader commit advanced.
    let st = handles
        .iter()
        .find(|h| h.id == leader)
        .unwrap()
        .status()
        .await;
    assert!(st.commit_index >= 1, "commit_index={}", st.commit_index);
    assert!(st.kv_len >= 1, "kv_len={}", st.kv_len);

    // Overwrite
    let resp2 = put_redirect(&handles, leader, "alpha", "2").await;
    assert!(resp2.ok);
    let got2 = get_redirect(&handles, leader, "alpha").await;
    assert_eq!(got2.value.as_deref(), Some("2"));

    for h in handles {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_redirects_put() {
    let dir = tempfile::tempdir().unwrap();
    let handles = spawn_cluster(0, dir.path().to_path_buf(), fast_timing())
        .await
        .expect("cluster");
    let leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader");

    let follower = handles.iter().find(|h| h.id != leader).unwrap();
    let direct = follower.put("k", "v").await;
    assert!(!direct.ok, "follower must not accept put");
    assert_eq!(direct.leader_hint, Some(leader));

    // Redirect helper should still succeed
    let ok = put_redirect(&handles, follower.id, "k", "v").await;
    assert!(ok.ok, "{:?}", ok.error);

    for h in handles {
        h.shutdown().await;
    }
}

/// Regression (CI run 39167600059): under leader churn `put_get_replicated`
/// got `ok` for a Put whose entry was then overwritten by the next leader's
/// noop at the same index, and the follow-up Get read `None`. A deposed
/// leader must not ack a Put just because *some* entry committed at its index.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deposed_leader_does_not_ack_overwritten_put() {
    let dir = tempfile::tempdir().unwrap();
    let addrs: HashMap<u64, SocketAddr> = (1..=3u64)
        .map(|i| {
            let a = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            (i, a)
        })
        .collect();
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    // Peer map for `id`; links touching `cut` point at a dead address.
    let peers_of = |id: u64, cut: Option<u64>| -> HashMap<u64, SocketAddr> {
        addrs
            .iter()
            .filter(|(o, _)| **o != id)
            .map(|(o, a)| {
                let isolated = cut == Some(id) || cut == Some(*o);
                (*o, if isolated { dead } else { *a })
            })
            .collect()
    };
    let mut nodes: Vec<NodeHandle> = Vec::new();
    for id in 1..=3u64 {
        nodes.push(
            spawn_node(NodeConfig {
                id,
                bind: addrs[&id],
                peers: peers_of(id, None),
                data_dir: dir.path().join(format!("n{id}")),
                timing: fast_timing(),
                learners: Vec::new(),
                snapshot_threshold: 0,
            })
            .await
            .unwrap(),
        );
    }
    let old = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .expect("leader");
    let old_node = nodes.iter().find(|n| n.id == old).unwrap();
    assert!(old_node.put("warm", "up").await.ok);

    // Isolate the old leader, then hand it a Put it can never commit.
    for n in &nodes {
        n.reload_peers(peers_of(n.id, Some(old))).await;
    }
    let before = old_node.status().await.log_len;
    let put = tokio::time::timeout(Duration::from_secs(10), old_node.put("lost", "v"));
    let rest = async {
        let mut appended = false;
        for _ in 0..200 {
            if old_node.status().await.log_len > before {
                appended = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(appended, "old leader must have appended the Put");

        // The majority elects a new leader whose entries overwrite that slot.
        let mut new_leader = None;
        for _ in 0..250 {
            for n in nodes.iter().filter(|n| n.id != old) {
                if n.status().await.role == "Leader" {
                    new_leader = Some(n);
                }
            }
            if new_leader.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let nl = new_leader.expect("majority elects a new leader");
        for i in 0..3 {
            assert!(nl.put(format!("x{i}"), "y").await.ok);
        }

        // Heal: the old leader truncates its entry and its commit index
        // passes the slot, which now holds the new leader's entry.
        for n in &nodes {
            n.reload_peers(peers_of(n.id, None)).await;
        }
        nl
    };
    let (resp, nl) = tokio::join!(put, rest);
    let resp = resp.expect("old leader must answer the Put once its slot is overwritten");
    assert!(
        !resp.ok,
        "Put acked although its entry was overwritten by the new leader"
    );
    let got = nl.get("lost").await;
    assert!(got.ok, "{:?}", got.error);
    assert_eq!(got.value, None, "the unacked Put really is lost");

    for n in nodes {
        n.shutdown().await;
    }
}

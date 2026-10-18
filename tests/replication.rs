//! Integration: Put/Get replication across 3 nodes.

use std::time::Duration;

use raft_mvp::node::{get_redirect, put_redirect, spawn_cluster, wait_for_leader};
use raft_mvp::raft::TimingConfig;

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
    let handles = spawn_cluster(17_200, dir.path().to_path_buf(), fast_timing())
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
    let handles = spawn_cluster(17_210, dir.path().to_path_buf(), fast_timing())
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

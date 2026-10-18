//! Integration: leader election in a 3-node localhost cluster.

use std::time::Duration;

use raft_kv_rs::node::{spawn_cluster, wait_for_leader};
use raft_kv_rs::raft::TimingConfig;

fn fast_timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 40,
        election_min_ms: 120,
        election_max_ms: 250,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_leader_among_three() {
    let dir = tempfile::tempdir().unwrap();
    let handles = spawn_cluster(17_100, dir.path().to_path_buf(), fast_timing())
        .await
        .expect("cluster");
    let leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader elected");
    assert!((1..=3).contains(&leader));

    // Exactly one leader at a time (stable check after settle).
    tokio::time::sleep(Duration::from_millis(300)).await;
    let leaders: Vec<_> = {
        let mut v = Vec::new();
        for h in &handles {
            let st = h.status().await;
            if st.role == "Leader" {
                v.push(st.id);
            }
        }
        v
    };
    assert_eq!(leaders.len(), 1, "split-brain leaders: {leaders:?}");
    assert_eq!(leaders[0], leader);

    for h in handles {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reelects_after_leader_crash() {
    let dir = tempfile::tempdir().unwrap();
    let mut handles = spawn_cluster(17_110, dir.path().to_path_buf(), fast_timing())
        .await
        .expect("cluster");
    let leader_id = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("initial leader");

    let idx = handles.iter().position(|h| h.id == leader_id).unwrap();
    let crashed = handles.remove(idx);
    crashed.shutdown().await;

    let new_leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("new leader after crash");
    assert_ne!(new_leader, leader_id);

    for h in handles {
        h.shutdown().await;
    }
}

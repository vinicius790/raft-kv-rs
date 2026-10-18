//! Integration: kill one follower — commits continue; restart recovers state.

use std::time::Duration;

use raft_kv_rs::node::{
    get_redirect, put_redirect, spawn_cluster, spawn_node, wait_for_leader, NodeConfig,
};
use raft_kv_rs::raft::TimingConfig;
use std::collections::HashMap;
use std::net::SocketAddr;

fn fast_timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 40,
        election_min_ms: 120,
        election_max_ms: 250,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_crash_put_still_works_then_restart_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let base = 17_300u16;
    let mut handles = spawn_cluster(base, dir.path().to_path_buf(), fast_timing())
        .await
        .expect("cluster");

    let leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader");

    // Initial commit while healthy
    let r0 = put_redirect(&handles, leader, "before", "crash").await;
    assert!(r0.ok, "{:?}", r0.error);

    // Pick a follower to kill
    let follower_idx = handles.iter().position(|h| h.id != leader).unwrap();
    let follower_id = handles[follower_idx].id;
    let follower_addr = handles[follower_idx].addr;
    let crashed = handles.remove(follower_idx);
    crashed.shutdown().await;

    // Majority of 2/3 still commits
    let r1 = put_redirect(&handles, leader, "during", "ok").await;
    assert!(r1.ok, "put during follower outage failed: {:?}", r1.error);

    let g = get_redirect(&handles, leader, "during").await;
    assert_eq!(g.value.as_deref(), Some("ok"));

    // Restart follower with same data_dir
    let addrs: Vec<(u64, SocketAddr)> = vec![
        (1, format!("127.0.0.1:{base}").parse().unwrap()),
        (2, format!("127.0.0.1:{}", base + 1).parse().unwrap()),
        (3, format!("127.0.0.1:{}", base + 2).parse().unwrap()),
    ];
    let mut peers = HashMap::new();
    for (oid, oaddr) in &addrs {
        if *oid != follower_id {
            peers.insert(*oid, *oaddr);
        }
    }
    // Bind same address as before
    let cfg = NodeConfig {
        id: follower_id,
        bind: follower_addr,
        peers,
        data_dir: dir.path().join(format!("node-{follower_id}")),
        timing: fast_timing(),
        learners: Vec::new(),
        snapshot_threshold: 0,
    };
    let restarted = spawn_node(cfg).await.expect("restart");
    handles.push(restarted);

    // Allow catch-up via AppendEntries
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Another put after recovery
    let leader2 = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader after restart");
    let r2 = put_redirect(&handles, leader2, "after", "recovered").await;
    assert!(r2.ok, "{:?}", r2.error);

    // Restarted node should know about committed keys once caught up.
    // We verify via leader read (linearizable path) and that cluster still has one leader.
    let got = get_redirect(&handles, leader2, "before").await;
    assert_eq!(got.value.as_deref(), Some("crash"));
    let got2 = get_redirect(&handles, leader2, "during").await;
    assert_eq!(got2.value.as_deref(), Some("ok"));
    let got3 = get_redirect(&handles, leader2, "after").await;
    assert_eq!(got3.value.as_deref(), Some("recovered"));

    // Status on restarted node: log should have grown
    let st = handles
        .iter()
        .find(|h| h.id == follower_id)
        .unwrap()
        .status()
        .await;
    assert!(
        st.log_len >= 2,
        "restarted follower log_len={} commit={}",
        st.log_len,
        st.commit_index
    );

    for h in handles {
        h.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persist_survives_full_cluster_restart() {
    let dir = tempfile::tempdir().unwrap();
    let base = 17_320u16;
    let timing = fast_timing();

    {
        let handles = spawn_cluster(base, dir.path().to_path_buf(), timing.clone())
            .await
            .unwrap();
        let leader = wait_for_leader(&handles, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(put_redirect(&handles, leader, "durable", "yes").await.ok);
        // Ensure commit persisted
        tokio::time::sleep(Duration::from_millis(200)).await;
        for h in handles {
            h.shutdown().await;
        }
    }

    // Cold start
    let handles = spawn_cluster(base, dir.path().to_path_buf(), timing)
        .await
        .unwrap();
    let leader = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .unwrap();
    // After restart, KV is rebuilt from durable commit_index.
    let got = get_redirect(&handles, leader, "durable").await;
    assert!(
        got.ok && got.value.as_deref() == Some("yes"),
        "expected durable value after restart, got {:?}",
        got
    );

    for h in handles {
        h.shutdown().await;
    }
}

#[tokio::test]
async fn crash_between_vote_and_log_persists_only_the_first_write() {
    use raft_kv_rs::raft::LogEntry;
    use raft_kv_rs::storage::{CrashPolicy, Storage};
    use raft_kv_rs::Command;

    let dir = tempfile::tempdir().unwrap();
    let crash = CrashPolicy::new();
    let mut storage = Storage::open_with_crash(dir.path(), crash.clone())
        .await
        .unwrap();
    let mut st = storage.persistent();
    st.current_term = 4;
    st.voted_for = Some(2);
    st.log.append(LogEntry {
        term: 4,
        command: Command::Put {
            key: "only-after-second-write".into(),
            value: "no".into(),
        },
    });
    // Flush #1 is the vote/term stage and succeeds; flush #2 (log) crashes.
    crash.arm_next(2);
    let err = storage.save_vote_then_log(&st, 1).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("injected"), "{msg}");

    let reloaded = Storage::open(dir.path()).await.unwrap();
    let got = reloaded.persistent();
    assert_eq!(got.current_term, 4, "vote/term stage must be durable");
    assert_eq!(got.voted_for, Some(2));
    assert_eq!(
        got.log.last_index(),
        0,
        "log stage must not be visible after a crash between the two writes"
    );
}

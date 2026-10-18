//! Additions on top of the 1.0 core: counters, check-quorum, session retry,
//! learner promotion, and auto-snapshot. No throughput numbers.

use std::time::Duration;

use raft_mvp::node::{spawn_cluster, wait_for_leader};
use raft_mvp::raft::{ClusterConfig, Membership, Outbound, RaftCore, Role, TimingConfig};
use raft_mvp::rpc::{ClientRequest, Message};
use raft_mvp::storage::Storage;
use raft_mvp::SessionClient;

fn timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 5,
        election_min_ms: 20,
        election_max_ms: 40,
    }
}

async fn fresh(id: u64, peers: Vec<u64>) -> (tempfile::TempDir, RaftCore) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).await.unwrap();
    let core = RaftCore::open(id, peers, storage, timing()).await.unwrap();
    (dir, core)
}

fn sends(out: &[Outbound]) -> Vec<(u64, Message)> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Send { to, msg } => Some((*to, msg.clone())),
            _ => None,
        })
        .collect()
}

async fn elect_leader(peers: Vec<u64>) -> (tempfile::TempDir, RaftCore) {
    let (dir, mut leader) = fresh(1, peers.clone()).await;
    leader.enable_pre_vote = false;
    leader.on_election_timeout().await.unwrap();
    let term = leader.persistent.current_term;
    for p in peers {
        leader
            .handle_message(
                Some(p),
                Message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
                0,
            )
            .await
            .unwrap();
        if leader.volatile.role == Role::Leader {
            break;
        }
    }
    assert_eq!(leader.volatile.role, Role::Leader);
    (dir, leader)
}

fn reply_err(out: &[Outbound]) -> Option<String> {
    out.iter().find_map(|o| match o {
        Outbound::ClientReply { resp, .. } if !resp.ok => resp.error.clone(),
        _ => None,
    })
}

#[tokio::test]
async fn check_quorum_steps_down_without_peer_contact() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let term = leader.persistent.current_term;
    let elections = leader.elections;
    assert!(elections >= 1);
    // Only self is in the contact set. That is not a majority of {1,2,3}.
    leader.on_election_timeout().await.unwrap();
    assert_ne!(leader.volatile.role, Role::Leader);
    assert_eq!(
        leader.persistent.current_term, term,
        "step-down must not bump term"
    );
    assert_eq!(
        leader.elections, elections,
        "step-down is not a new election"
    );
}

#[tokio::test]
async fn session_retry_does_not_append_and_zero_id_rejected() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let before = leader.last_log_index();
    let dup = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: 0,
                serial: 1,
                key: "k".into(),
                value: "v".into(),
            }),
            1,
        )
        .await
        .unwrap();
    assert_eq!(reply_err(&dup).as_deref(), Some("client_id 0 is reserved"));
    assert_eq!(leader.last_log_index(), before);

    let mut session = SessionClient::new(9);
    let serial = session.serial_for_attempt();
    assert_eq!(session.serial_for_attempt(), serial);
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: session.client_id(),
                serial,
                key: "k".into(),
                value: "once".into(),
            }),
            2,
        )
        .await
        .unwrap();
    let after = leader.last_log_index();
    assert!(after > before);
    // Majority of 3 is 2; self alone must not have committed this entry.
    assert!(leader.commit_index() < after);
    let again = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: session.client_id(),
                serial: session.serial_for_attempt(),
                key: "k".into(),
                value: "twice".into(),
            }),
            3,
        )
        .await
        .unwrap();
    assert_eq!(leader.last_log_index(), after, "retry must not append");
    assert!(again.iter().any(|o| matches!(
        o,
        Outbound::ClientWaitCommit { wait_index, .. } if *wait_index == after
    )));
}

#[tokio::test]
async fn promote_learner_waits_until_caught_up() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    leader.persistent.config = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![4]));
    let last = leader.last_log_index();
    if let Some(state) = leader.leader.as_mut() {
        state.ensure_peer(4, last);
        state.match_index.insert(4, 0);
    }
    let denied = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::PromoteLearner { id: 4 }),
            1,
        )
        .await
        .unwrap();
    assert_eq!(reply_err(&denied).as_deref(), Some("learner not caught up"));
    assert!(!leader.persistent.config.is_joint());

    if let Some(state) = leader.leader.as_mut() {
        state.match_index.insert(4, last);
    }
    let ok = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::PromoteLearner { id: 4 }),
            2,
        )
        .await
        .unwrap();
    assert!(reply_err(&ok).is_none(), "{ok:?}");
    match &leader.persistent.config {
        ClusterConfig::Joint { new, .. } => {
            assert!(new.is_voter(4), "{}", leader.persistent.config.summary());
            assert!(!new.is_learner(4));
        }
        other => panic!("expected joint, got {}", other.summary()),
    }
}

#[tokio::test]
async fn auto_snapshot_counts_bytes_and_keeps_kv() {
    let (_d, mut n) = fresh(1, vec![]).await;
    n.enable_pre_vote = false;
    n.on_election_timeout().await.unwrap();
    assert_eq!(n.volatile.role, Role::Leader);
    assert!(n.elections >= 1);
    n.handle_message(
        None,
        Message::Client(ClientRequest::Put {
            key: "a".into(),
            value: "1".into(),
        }),
        1,
    )
    .await
    .unwrap();
    assert!(n.metrics().commits >= 1, "commits={}", n.metrics().commits);
    let before = n.snapshot_bytes;
    n.snapshot_threshold = 1;
    n.maybe_auto_snapshot().await.unwrap();
    assert!(n.persistent.log.snapshot_index >= n.commit_index());
    assert!(n.snapshot_bytes > before);
    assert_eq!(n.kv.get("a").map(String::as_str), Some("1"));
    // A second call must not add another image: the suffix is already compacted.
    let frozen = n.snapshot_bytes;
    n.maybe_auto_snapshot().await.unwrap();
    assert_eq!(n.snapshot_bytes, frozen);
}

#[tokio::test]
async fn snapshot_send_adds_chunk_bytes() {
    let (_d, mut leader) = elect_leader(vec![2]).await;
    // 2 voters: ack from 2 commits.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "a".into(),
                value: "zz".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let last = leader.last_log_index();
    let term = leader.persistent.current_term;
    leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert!(leader.commit_index() >= last);
    leader.compact_log(leader.commit_index()).await.unwrap();
    let after_compact = leader.snapshot_bytes;
    assert!(after_compact > 0);
    if let Some(state) = leader.leader.as_mut() {
        state.next_index.insert(2, 1);
        state.snap_offset.insert(2, 0);
        state.snap_busy.remove(&2);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    let chunk = sends(&out).into_iter().find_map(|(_, msg)| match msg {
        Message::InstallSnapshot { data, .. } => Some(data.len() as u64),
        _ => None,
    });
    let chunk = chunk.expect("leader should send InstallSnapshot");
    assert_eq!(leader.snapshot_bytes, after_compact + chunk);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn localhost_cluster_exposes_live_counters() {
    let dir = tempfile::tempdir().unwrap();
    let handles = spawn_cluster(17_610, dir.path().to_path_buf(), timing())
        .await
        .expect("cluster");
    let leader_id = wait_for_leader(&handles, Duration::from_secs(5))
        .await
        .expect("leader");
    let leader = handles.iter().find(|h| h.id == leader_id).unwrap();
    let before = leader.metrics().await;
    assert!(before.elections >= 1, "elections={}", before.elections);
    assert_eq!(before.role, "Leader");
    let put = leader.put("live", "1").await;
    assert!(put.ok, "{:?}", put.error);
    let after = leader.metrics().await;
    assert!(
        after.commits >= before.commits,
        "commits {} -> {}",
        before.commits,
        after.commits
    );
    assert!(after.commits >= 1);
    // No snapshot was requested. The counter must stay whatever the node reports.
    let _ = after.snapshot_bytes;
    for h in handles {
        h.shutdown().await;
    }
}

/// Regression: a quorum ack must not push the leader's check-quorum timer.
/// Logical clock, window W = 10. Check at 10, ack at 9, partition at 9.5.
/// Fixed windows: check at 10 passes (ack at 9), check at 20 steps down.
/// When acks re-armed the timer, the check moved to 19 (passes on the pre-
/// partition ack) and the step-down slipped to 29: one extra window.
#[tokio::test]
async fn check_quorum_window_is_not_extended_by_acks() {
    const W: u64 = 10;
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let term = leader.persistent.current_term;
    let mut deadline = W; // armed by become_leader at t = 0
    let rearm = |out: &[Outbound], now: u64, deadline: &mut u64| {
        if out
            .iter()
            .any(|o| matches!(o, Outbound::ResetElectionTimer))
        {
            *deadline = now + W;
        }
    };
    let li = leader.last_log_index();
    let out = leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: li,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    rearm(&out, 9, &mut deadline);
    // Partition at 9.5: no more acks. Advance the clock to 20.
    let mut stepped_down_at = None;
    for now in 10..=20 {
        if now == deadline {
            let out = leader.on_election_timeout().await.unwrap();
            if leader.volatile.role != Role::Leader {
                stepped_down_at = Some(now);
                break;
            }
            rearm(&out, now, &mut deadline);
        }
    }
    assert_eq!(
        stepped_down_at,
        Some(20),
        "check-quorum took more than one extra window (deadline {deadline})"
    );
}

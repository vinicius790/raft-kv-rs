//! Snapshot checksum refusal and the in-process protocol trace.
//! Plus one simulator partition that is not in the 1..=80 property loop.

use raft_kv_rs::raft::{Outbound, RaftCore, Role, TimingConfig};
use raft_kv_rs::rpc::{ClientRequest, Message};
use raft_kv_rs::sim::Sim;
use raft_kv_rs::storage::{self, Storage};

fn timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 5,
        election_min_ms: 20,
        election_max_ms: 40,
    }
}

fn sends(out: &[Outbound]) -> Vec<(u64, Message)> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Send { to, msg } => Some((*to, msg.clone())),
            _ => None,
        })
        .collect()
}

async fn elect_pair() -> (RaftCore, RaftCore) {
    let mut leader = RaftCore::open(1, vec![2], Storage::ephemeral(), timing())
        .await
        .unwrap();
    let follower = RaftCore::open(2, vec![1], Storage::ephemeral(), timing())
        .await
        .unwrap();
    leader.enable_pre_vote = false;
    leader.on_election_timeout().await.unwrap();
    let term = leader.persistent.current_term;
    leader
        .handle_message(
            Some(2),
            Message::RequestVoteResponse {
                term,
                vote_granted: true,
            },
            0,
        )
        .await
        .unwrap();
    assert_eq!(leader.volatile.role, Role::Leader);
    (leader, follower)
}

async fn replicate_put(leader: &mut RaftCore, follower: &mut RaftCore, key: &str, value: &str) {
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: key.into(),
                value: value.into(),
            }),
            1,
        )
        .await
        .unwrap();
    let msg = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::AppendEntries { entries, .. } if !entries.is_empty()))
        .expect("append")
        .1;
    let resp_out = follower.handle_message(Some(1), msg, 0).await.unwrap();
    let resp = sends(&resp_out).into_iter().next().unwrap().1;
    leader.handle_message(Some(2), resp, 0).await.unwrap();
    let hb = leader.on_heartbeat_timeout().await.unwrap();
    let msg = sends(&hb)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::AppendEntries { .. }))
        .expect("heartbeat")
        .1;
    follower.handle_message(Some(1), msg, 0).await.unwrap();
}

#[tokio::test]
async fn corrupted_snapshot_is_refused_without_truncating() {
    let (mut leader, mut follower) = elect_pair().await;
    replicate_put(&mut leader, &mut follower, "k", "v").await;
    assert_eq!(follower.kv.get("k").map(String::as_str), Some("v"));
    let keep = follower.persistent.log.last_index();
    assert!(keep >= 1);

    leader.compact_log(leader.commit_index()).await.unwrap();
    assert!(leader.persistent.log.snapshot_index >= 1);
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
        ls.snap_offset.insert(2, 0);
        ls.snap_busy.remove(&2);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    let mut snap = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
        .expect("snapshot chunk")
        .1;
    let Message::InstallSnapshot {
        data,
        checksum,
        done,
        ..
    } = &snap
    else {
        unreachable!();
    };
    assert!(*done, "image must fit in one chunk for this test");
    assert_eq!(*checksum, storage::crc32(data));
    assert_ne!(storage::crc32(data), 0);

    match &mut snap {
        Message::InstallSnapshot { data, .. } => {
            data[0] ^= 0xff;
        }
        _ => unreachable!(),
    }
    let before_snap = follower.persistent.log.snapshot_index;
    let before_kv = follower.kv.get("k").cloned();
    let resp_out = follower.handle_message(Some(1), snap, 0).await.unwrap();
    match &sends(&resp_out)[0].1 {
        Message::InstallSnapshotResponse { success, done, .. } => {
            assert!(!success);
            assert!(!done);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(follower.persistent.log.snapshot_index, before_snap);
    assert_eq!(follower.persistent.log.last_index(), keep);
    assert!(follower.persistent.log.entry(1).is_some());
    assert_eq!(follower.kv.get("k"), before_kv.as_ref());
}

#[tokio::test]
async fn trace_records_term_election_and_commit() {
    let mut node = RaftCore::open(1, vec![], Storage::ephemeral(), timing())
        .await
        .unwrap();
    node.enable_pre_vote = false;
    node.on_election_timeout().await.unwrap();
    assert_eq!(node.volatile.role, Role::Leader);
    assert!(node.commit_index() >= 1, "single voter commits the noop");
    let out = node
        .handle_message(None, Message::TraceRequest, 7)
        .await
        .unwrap();
    let text = out
        .iter()
        .find_map(|o| match o {
            Outbound::ClientReply { resp, .. } => resp.value.clone(),
            _ => None,
        })
        .unwrap();
    assert!(text.contains("kind=term"), "{text}");
    assert!(text.contains("kind=election"), "{text}");
    assert!(text.contains("kind=commit"), "{text}");
    assert!(!text.contains("prometheus"));
}

#[tokio::test]
async fn partitioned_node_lags_then_matches_commit_after_heal() {
    let mut sim = Sim::new(7, &[1, 2, 3]).await;
    sim.run(45).await;
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
    let leader = sim.leader_id().expect("leader");
    let isolated = [1u64, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    for other in [1u64, 2, 3] {
        if other != isolated {
            sim.partition_pair(isolated, other);
        }
    }
    let before = sim.commit_of(leader);
    sim.client(
        leader,
        ClientRequest::Put {
            key: "part".into(),
            value: "1".into(),
        },
    )
    .await;
    sim.run(40).await;
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
    assert!(
        sim.commit_of(leader) > before,
        "majority did not commit under partition"
    );
    assert!(
        sim.commit_of(isolated) < sim.commit_of(leader),
        "isolated node advanced with the majority"
    );
    sim.heal();
    sim.run(70).await;
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
    assert_eq!(sim.commit_of(1), sim.commit_of(2));
    assert_eq!(sim.commit_of(2), sim.commit_of(3));
    assert!(!sim.leaders_by_term.is_empty());
}

/// Leader-side snapshot chunk for `peer`, forced by rewinding next_index.
async fn capture_snapshot(leader: &mut RaftCore, peer: u64) -> Message {
    leader.compact_log(leader.commit_index()).await.unwrap();
    let next = leader.persistent.log.last_index() + 1;
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(peer, 1);
        ls.snap_offset.insert(peer, 0);
        ls.snap_busy.remove(&peer);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    let snap = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { done: true, .. }))
        .expect("single-chunk snapshot")
        .1;
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(peer, next);
        ls.snap_offset.remove(&peer);
        ls.snap_busy.remove(&peer);
        ls.inflight.remove(&peer);
        ls.inflight_bytes.insert(peer, 0);
    }
    snap
}

/// Regression: a delayed snapshot older than the follower's commit index must
/// not roll the KV back (etcd ignores it in `restore`).
#[tokio::test]
async fn stale_snapshot_does_not_roll_back_kv() {
    let (mut leader, mut follower) = elect_pair().await;
    replicate_put(&mut leader, &mut follower, "k", "v1").await;
    let old_snap = capture_snapshot(&mut leader, 2).await;
    replicate_put(&mut leader, &mut follower, "k", "v2").await;
    assert_eq!(follower.kv.get("k").map(String::as_str), Some("v2"));
    let commit = follower.commit_index();
    let last = follower.persistent.log.last_index();
    let out = follower.handle_message(Some(1), old_snap, 0).await.unwrap();
    assert_eq!(
        follower.kv.get("k").map(String::as_str),
        Some("v2"),
        "stale snapshot rolled the KV back"
    );
    assert_eq!(follower.commit_index(), commit);
    assert_eq!(follower.volatile.last_applied, commit);
    assert_eq!(follower.persistent.log.last_index(), last);
    assert!(sends(&out).iter().any(|(_, m)| matches!(
        m,
        Message::InstallSnapshotResponse {
            success: true,
            done: true,
            ..
        }
    )));
}

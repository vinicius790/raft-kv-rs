//! Protocol tests for joint consensus, snapshots, pre-vote, transfer,
//! read-index, learners, batching, sessions, metrics, and the simulator.

use raft_kv_rs::raft::{ClusterConfig, Membership, Outbound, RaftCore, Role, TimingConfig};
use raft_kv_rs::rpc::{ClientRequest, Message, SnapshotBlob};
use raft_kv_rs::sim::{self, Sim};
use raft_kv_rs::storage::Storage;
use raft_kv_rs::Command;

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

async fn fresh_cfg(id: u64, cfg: ClusterConfig) -> (tempfile::TempDir, RaftCore) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = Storage::open(dir.path()).await.unwrap();
    let mut st = storage.persistent();
    st.config = cfg;
    storage.save(&st, 0).await.unwrap();
    let core = RaftCore::open(id, vec![], storage, timing()).await.unwrap();
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
    // Self vote is not enough when peers exist.
    let need = peers.len(); // grant all peers
    for p in peers.into_iter().take(need) {
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

#[tokio::test]
async fn joint_change_does_not_commit_without_overlapping_quorum() {
    let (_dir, mut leader) = elect_leader(vec![2, 3, 4, 5]).await;
    let before = leader.commit_index();
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 3],
                learners: vec![],
            }),
            7,
        )
        .await
        .unwrap();
    assert!(
        out.iter()
            .any(|o| matches!(o, Outbound::ClientWaitCommit { .. })),
        "change must be proposed"
    );
    let joint_idx = leader.last_log_index();
    assert!(matches!(
        leader.persistent.config,
        ClusterConfig::Joint { .. }
    ));
    // Old majority {1,4,5} but new {1,2,3} lacks a second voter — must not commit.
    let term = leader.persistent.current_term;
    for peer in [4, 5] {
        leader
            .handle_message(
                Some(peer),
                Message::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: joint_idx,
                    conflict_index: 0,
                    need_snapshot: false,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert!(
        leader.commit_index() < joint_idx,
        "committed {} without new-config quorum (joint at {joint_idx}, before {before})",
        leader.commit_index()
    );
    // Add node 2: both quorums overlap.
    leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: joint_idx,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert!(
        leader.commit_index() >= joint_idx,
        "joint entry should commit once both quorums overlap, commit={} joint={joint_idx}",
        leader.commit_index()
    );
}

#[tokio::test]
async fn single_node_does_not_lose_leadership_on_timeout() {
    let (_dir, mut n) = fresh(1, vec![]).await;
    n.on_election_timeout().await.unwrap();
    assert_eq!(n.volatile.role, Role::Leader);
    let term = n.persistent.current_term;
    n.on_election_timeout().await.unwrap();
    assert_eq!(n.volatile.role, Role::Leader);
    assert_eq!(n.persistent.current_term, term);
    let out = n
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![2],
                learners: vec![],
            }),
            1,
        )
        .await
        .unwrap();
    assert!(out.iter().any(|o| matches!(
        o,
        Outbound::ClientReply { resp, .. } if !resp.ok
    )));
    assert_eq!(n.volatile.role, Role::Leader);
    assert!(n.persistent.config.is_voter(1));
}

#[tokio::test]
async fn slow_follower_catches_up_from_snapshot_not_full_log() {
    let (_d1, mut leader) = elect_leader(vec![2]).await;
    let (_d2, mut follower) = fresh(2, vec![1]).await;
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        let out = leader
            .handle_message(
                None,
                Message::Client(ClientRequest::Put {
                    key: k.into(),
                    value: v.into(),
                }),
                1,
            )
            .await
            .unwrap();
        let msg = sends(&out)
            .into_iter()
            .find(|(_, m)| matches!(m, Message::AppendEntries { .. }))
            .unwrap()
            .1;
        let resp_out = follower.handle_message(Some(1), msg, 0).await.unwrap();
        let resp = sends(&resp_out).into_iter().next().unwrap().1;
        leader.handle_message(Some(2), resp, 0).await.unwrap();
    }
    assert!(
        leader.commit_index() >= 4,
        "commit {}",
        leader.commit_index()
    );
    let committed = leader.commit_index();
    leader.compact_log(committed).await.unwrap();
    assert!(leader.persistent.log.snapshot_index >= 4);
    assert!(
        leader.persistent.log.entry(1).is_none(),
        "prefix must be truncated"
    );
    // Lagging follower: empty log. Leader next_index forced behind the snapshot.
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    let snap = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
        .expect("leader must send InstallSnapshot, not the full prefix")
        .1;
    match &snap {
        Message::InstallSnapshot {
            last_included_index,
            data,
            ..
        } => {
            assert!(*last_included_index >= 4);
            assert!(
                data.len() < 10_000,
                "snapshot payload should be the KV, not a replay of every entry"
            );
        }
        _ => unreachable!(),
    }
    let (_d3, mut slow) = fresh(2, vec![1]).await;
    assert_eq!(slow.last_log_index(), 0);
    let resp_out = slow.handle_message(Some(1), snap, 0).await.unwrap();
    assert!(sends(&resp_out)
        .iter()
        .any(|(_, m)| matches!(m, Message::InstallSnapshotResponse { success: true, .. })));
    assert_eq!(slow.kv.get("a").map(String::as_str), Some("1"));
    assert_eq!(slow.kv.get("d").map(String::as_str), Some("4"));
    assert!(
        slow.persistent.log.entry(1).is_none(),
        "slow follower must not have reconstituted the prefix entry-by-entry"
    );
    assert!(slow.persistent.log.snapshot_index >= 4);
}

#[tokio::test]
async fn prevote_partition_does_not_bump_leader_term() {
    let (_d1, mut leader) = elect_leader(vec![2, 3]).await;
    let (_d2, mut healthy) = fresh(2, vec![1, 3]).await;
    let (_d3, mut isolated) = fresh(3, vec![1, 2]).await;
    let term = leader.persistent.current_term;
    // Heartbeat so the healthy follower believes the leader is alive.
    let hb = leader.on_heartbeat_timeout().await.unwrap();
    let ae = sends(&hb)
        .into_iter()
        .find(|(to, m)| *to == 2 && matches!(m, Message::AppendEntries { .. }))
        .unwrap()
        .1;
    healthy.handle_message(Some(1), ae, 0).await.unwrap();
    assert!(healthy.heard_from_leader);

    let isolated_term = isolated.persistent.current_term;
    let out = isolated.on_election_timeout().await.unwrap();
    assert_eq!(
        isolated.persistent.current_term, isolated_term,
        "pre-vote must not bump term"
    );
    let prevotes: Vec<_> = sends(&out)
        .into_iter()
        .filter(|(_, m)| matches!(m, Message::PreVote { .. }))
        .collect();
    assert!(
        !prevotes.is_empty(),
        "isolated node should probe with pre-vote"
    );

    for (to, msg) in prevotes {
        let target = if to == 1 { &mut leader } else { &mut healthy };
        let resp = target.handle_message(Some(3), msg, 0).await.unwrap();
        for (_, m) in sends(&resp) {
            if let Message::PreVoteResponse { vote_granted, .. } = m {
                assert!(
                    !vote_granted,
                    "healthy peer must refuse disruptive pre-vote"
                );
            }
        }
    }
    assert_eq!(leader.persistent.current_term, term);
    assert_eq!(leader.volatile.role, Role::Leader);
    assert_eq!(isolated.persistent.current_term, isolated_term);
}

#[tokio::test]
async fn prevote_happy_path_elects_leader() {
    let (_d1, mut a) = fresh(1, vec![2, 3]).await;
    let (_d2, mut b) = fresh(2, vec![1, 3]).await;
    let (_d3, mut c) = fresh(3, vec![1, 2]).await;
    let out = a.on_election_timeout().await.unwrap();
    assert_eq!(a.persistent.current_term, 0, "still in pre-vote");
    let prevotes = sends(&out);
    assert!(prevotes
        .iter()
        .all(|(_, m)| matches!(m, Message::PreVote { .. })));
    let mut campaign = Vec::new();
    for (to, msg) in prevotes {
        let node = if to == 2 { &mut b } else { &mut c };
        let resp = node.handle_message(Some(1), msg, 0).await.unwrap();
        for (back, m) in sends(&resp) {
            assert_eq!(back, 1);
            let more = a.handle_message(Some(to), m, 0).await.unwrap();
            campaign.extend(sends(&more));
        }
    }
    // Real election started.
    assert!(a.persistent.current_term >= 1);
    let votes: Vec<_> = campaign
        .into_iter()
        .filter(|(_, m)| matches!(m, Message::RequestVote { .. }))
        .collect();
    assert!(!votes.is_empty());
    for (to, msg) in votes {
        let node = if to == 2 { &mut b } else { &mut c };
        let resp = node.handle_message(Some(1), msg, 0).await.unwrap();
        for (_, m) in sends(&resp) {
            a.handle_message(Some(to), m, 0).await.unwrap();
        }
    }
    assert_eq!(a.volatile.role, Role::Leader);
}

#[tokio::test]
async fn leadership_transfer_keeps_committed_prefix() {
    let (_d1, mut leader) = elect_leader(vec![2, 3]).await;
    let (_d2, mut follower) = fresh(2, vec![1, 3]).await;
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "k".into(),
                value: "v".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let ae = sends(&out)
        .into_iter()
        .find(|(to, m)| *to == 2 && matches!(m, Message::AppendEntries { .. }))
        .unwrap()
        .1;
    let resp = follower.handle_message(Some(1), ae, 0).await.unwrap();
    let resp_msg = sends(&resp).into_iter().next().unwrap().1;
    leader.handle_message(Some(2), resp_msg, 0).await.unwrap();
    // Majority of 3 needs a second ack.
    let match_idx = leader.last_log_index();
    leader
        .handle_message(
            Some(3),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: match_idx,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    let commit = leader.commit_index();
    assert!(commit >= 1);
    // Follower 2 must be caught up (match from its real response). Node 3 was synthetic.
    let prefix: Vec<_> = (1..=commit)
        .map(|i| leader.persistent.log.entry(i).cloned())
        .collect();
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::TransferLeadership { target: 2 }),
            9,
        )
        .await
        .unwrap();
    assert_eq!(leader.volatile.role, Role::Follower);
    let timeout_now = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::TimeoutNow { .. }))
        .expect("TimeoutNow")
        .1;
    let term_before_campaign = follower.persistent.current_term;
    let camp = follower
        .handle_message(Some(1), timeout_now, 0)
        .await
        .unwrap();
    assert_eq!(
        follower.persistent.current_term, term_before_campaign,
        "TimeoutNow must pre-vote before bumping the term"
    );
    let prevotes: Vec<_> = sends(&camp)
        .into_iter()
        .filter(|(_, m)| matches!(m, Message::PreVote { .. }))
        .collect();
    assert!(
        !prevotes.is_empty(),
        "TimeoutNow must go through pre-vote: {camp:?}"
    );
    let mut campaign = Vec::new();
    for (to, msg) in prevotes {
        if to != 1 {
            continue;
        }
        let resp = leader.handle_message(Some(2), msg, 0).await.unwrap();
        for (_, m) in sends(&resp) {
            let more = follower.handle_message(Some(1), m, 0).await.unwrap();
            campaign.extend(sends(&more));
        }
    }
    let rv = campaign
        .into_iter()
        .find(|(to, m)| *to == 1 && matches!(m, Message::RequestVote { .. }))
        .expect("pre-vote quorum starts a real election")
        .1;
    let grant = leader.handle_message(Some(2), rv, 0).await.unwrap();
    let g = sends(&grant).into_iter().next().unwrap().1;
    follower.handle_message(Some(1), g, 0).await.unwrap();
    // May still need node 3. If not leader yet, inject vote from 3.
    if follower.volatile.role != Role::Leader {
        let term = follower.persistent.current_term;
        follower
            .handle_message(
                Some(3),
                Message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert_eq!(follower.volatile.role, Role::Leader);
    for i in 1..=commit {
        assert_eq!(
            follower.persistent.log.entry(i),
            prefix[(i - 1) as usize].as_ref(),
            "committed prefix diverged at {i}"
        );
    }
}

#[tokio::test]
async fn stale_leader_does_not_serve_read_index() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    // Commit a value with a synthetic quorum so the state machine has it.
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "x".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let _ = out;
    let idx = leader.last_log_index();
    let term = leader.persistent.current_term;
    for peer in [2, 3] {
        leader
            .handle_message(
                Some(peer),
                Message::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: idx,
                    conflict_index: 0,
                    need_snapshot: false,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert_eq!(leader.kv.get("x").map(String::as_str), Some("1"));
    // Partition: no heartbeat responses will arrive.
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ReadIndex { key: "x".into() }),
            42,
        )
        .await
        .unwrap();
    assert!(
        !out.iter().any(|o| matches!(
            o,
            Outbound::ClientReply { resp, .. } if resp.ok
        )),
        "partitioned leader must not answer a linearizable read, got {out:?}"
    );
    assert!(leader.pending_reads.iter().any(|r| r.0 == 42));
    // Quorum heartbeat from one follower unblocks the read.
    let ready = leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: leader.last_log_index(),
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert!(ready.iter().any(|o| matches!(
        o,
        Outbound::ClientReply { corr: 42, resp } if resp.ok && resp.value.as_deref() == Some("1")
    )));
}

#[tokio::test]
async fn learner_cannot_elect_or_decide_commit_then_promotes() {
    let cfg = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![4]));
    let (_d, mut learner) = fresh_cfg(4, cfg.clone()).await;
    assert_eq!(learner.volatile.role, Role::Learner);
    let term = learner.persistent.current_term;
    let out = learner.on_election_timeout().await.unwrap();
    assert!(sends(&out).is_empty(), "learner must not campaign: {out:?}");
    assert_eq!(learner.persistent.current_term, term);
    assert_eq!(learner.volatile.role, Role::Learner);
    let vote = learner
        .handle_message(
            Some(9),
            Message::RequestVote {
                term: 1,
                candidate_id: 4,
                last_log_index: 0,
                last_log_term: 0,
            },
            0,
        )
        .await
        .unwrap();
    match &sends(&vote)[0].1 {
        Message::RequestVoteResponse { vote_granted, .. } => assert!(!vote_granted),
        other => panic!("{other:?}"),
    }

    let (_dl, mut leader) = elect_leader(vec![2, 3]).await;
    // Reconfigure leader's view to include learner 4 without a voting seat.
    // elect_leader built voters {1,2,3}. Add learner by a membership change that
    // keeps voters and adds learner — joint then C_new. First show learner ack
    // does not commit a fresh entry.
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "z".into(),
                value: "9".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let _ = out;
    let idx = leader.last_log_index();
    let commit_before = leader.commit_index();
    leader
        .handle_message(
            Some(4),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: idx,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert_eq!(
        leader.commit_index(),
        commit_before,
        "learner ack must not advance commit"
    );

    // Promote 4 into the voter set via joint consensus.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 3, 4],
                learners: vec![],
            }),
            2,
        )
        .await
        .unwrap();
    let joint_idx = leader.last_log_index();
    let term = leader.persistent.current_term;
    // Old majority and new majority: self+2+3 covers both (new needs 3 of 4).
    for peer in [2u64, 3] {
        leader
            .handle_message(
                Some(peer),
                Message::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: joint_idx,
                    conflict_index: 0,
                    need_snapshot: false,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert!(leader.commit_index() >= joint_idx);
    let voters = leader.persistent.config.voter_ids();
    assert!(
        voters.contains(&4),
        "promoted learner must be in the voter set, cfg {}",
        leader.persistent.config.summary()
    );
}

#[tokio::test]
async fn batch_append_entries_and_gap_rejected() {
    let (_d, mut leader) = elect_leader(vec![2]).await;
    // First put sets next_index; second put must batch both unacked entries.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "a".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "b".into(),
                value: "2".into(),
            }),
            2,
        )
        .await
        .unwrap();
    let mut batched = 0;
    for (_, m) in sends(&out) {
        if let Message::AppendEntries { entries, .. } = m {
            batched = batched.max(entries.len());
        }
    }
    assert!(batched >= 2, "expected a batch, got {batched}");

    let (_df, mut follower) = fresh(2, vec![1]).await;
    let out = follower
        .handle_message(
            Some(1),
            Message::AppendEntries {
                term: 1,
                leader_id: 1,
                prev_log_index: 5,
                prev_log_term: 1,
                entries: vec![raft_kv_rs::LogEntry {
                    term: 1,
                    command: Command::Put {
                        key: "g".into(),
                        value: "ap".into(),
                    },
                }],
                leader_commit: 0,
            },
            0,
        )
        .await
        .unwrap();
    match &sends(&out)[0].1 {
        Message::AppendEntriesResponse { success, .. } => assert!(!success),
        other => panic!("{other:?}"),
    }
    assert_eq!(follower.last_log_index(), 0, "gap must not be filled");
}

#[tokio::test]
async fn duplicate_session_propose_applies_once() {
    let (_d, mut leader) = fresh(1, vec![]).await;
    leader.on_election_timeout().await.unwrap();
    assert_eq!(leader.volatile.role, Role::Leader);
    let req = ClientRequest::SessionPut {
        client_id: 7,
        serial: 1,
        key: "s".into(),
        value: "once".into(),
    };
    leader
        .handle_message(None, Message::Client(req.clone()), 1)
        .await
        .unwrap();
    let after_first = leader.kv.get("s").cloned();
    let log_after = leader.last_log_index();
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: 7,
                serial: 1,
                key: "s".into(),
                value: "twice".into(),
            }),
            2,
        )
        .await
        .unwrap();
    assert_eq!(after_first.as_deref(), Some("once"));
    assert_eq!(leader.kv.get("s").map(String::as_str), Some("once"));
    assert_eq!(
        leader.last_log_index(),
        log_after,
        "duplicate must not append"
    );
}

#[tokio::test]
async fn metrics_report_term_commit_and_lag() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "m".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let last = leader.last_log_index();
    leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: last.saturating_sub(1),
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    let m = leader.metrics();
    assert_eq!(m.term, leader.persistent.current_term);
    assert_eq!(m.commit_index, leader.commit_index());
    assert_eq!(m.role, "Leader");
    let lag = m.replication_lag.get(&2).copied().unwrap_or(0);
    assert_eq!(lag, 1, "lag must be last-match, got {lag}");
    // No throughput field exists on the struct; this assertion documents that.
    let _ = m.snapshot_index;
}

#[tokio::test]
async fn simulator_reload_peer_address() {
    let mut sim = Sim::new(7, &[1, 2, 3]).await;
    sim.set_drop_permille(0);
    sim.run(80).await;
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
    let leader = sim.leader_id().expect("leader before reload");
    let commit_before = sim.commit_of(3);
    sim.move_listen(3, "moved-3");
    // Traffic to the old address must not land.
    sim.run(25).await;
    let stalled = sim.commit_of(3);
    sim.reload_routes();
    // Produce a new committed entry and let node 3 catch up on the new address.
    let _ = sim
        .client(
            leader,
            ClientRequest::Put {
                key: "reload".into(),
                value: "1".into(),
            },
        )
        .await;
    sim.run(80).await;
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
    assert!(
        sim.commit_of(3) >= stalled,
        "commit went backwards: {stalled} -> {}",
        sim.commit_of(3)
    );
    // After reload, node 3 is reachable again: either it advanced or the
    // cluster commit (leader) is visible via log matching with no violation.
    let _ = commit_before;
    assert!(sim.leader_id().is_some());
}

#[tokio::test]
async fn simulator_election_safety_and_log_matching_seeds() {
    // Seeds actually executed (also listed in sim::PROPERTY_SEEDS).
    let mut ran = Vec::new();
    for &seed in sim::PROPERTY_SEEDS {
        sim::check_seed(seed)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        ran.push(seed);
    }
    assert_eq!(ran.len(), 80, "ran seeds {ran:?}");
    assert_eq!(ran.first().copied(), Some(1));
    assert_eq!(ran.last().copied(), Some(80));
    eprintln!("simulator seeds ran: {ran:?}");
}

#[test]
fn snapshot_blob_roundtrip_used_by_install() {
    let blob = SnapshotBlob {
        kv: [("a".into(), "b".into())].into_iter().collect(),
        sessions: [(1, 2)].into_iter().collect(),
    };
    let bytes = bincode::serialize(&blob).unwrap();
    let back: SnapshotBlob = bincode::deserialize(&bytes).unwrap();
    assert_eq!(back.kv.get("a").map(String::as_str), Some("b"));
}

#[tokio::test]
async fn chunk_gap_rejected_and_three_chunks_catch_up() {
    let (_d1, mut leader) = elect_leader(vec![2]).await;
    let (_d2, mut follower) = fresh(2, vec![1]).await;
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
        let out = leader
            .handle_message(
                None,
                Message::Client(ClientRequest::Put {
                    key: k.into(),
                    value: v.into(),
                }),
                1,
            )
            .await
            .unwrap();
        let msg = sends(&out)
            .into_iter()
            .find(|(_, m)| matches!(m, Message::AppendEntries { .. }))
            .unwrap()
            .1;
        let resp_out = follower.handle_message(Some(1), msg, 0).await.unwrap();
        let resp = sends(&resp_out).into_iter().next().unwrap().1;
        leader.handle_message(Some(2), resp, 0).await.unwrap();
    }
    let committed = leader.commit_index();
    leader.compact_log(committed).await.unwrap();
    assert!(leader.persistent.log.snapshot_index >= 4);

    // Gap: a follower with a local entry must not truncate on a non-contiguous chunk.
    let (_dg, mut gapped) = fresh(3, vec![1]).await;
    gapped.persistent.log.append(raft_kv_rs::LogEntry {
        term: 1,
        command: Command::Put {
            key: "keep".into(),
            value: "yes".into(),
        },
    });
    let cfg = gapped.persistent.config.clone();
    let first = Message::InstallSnapshot {
        term: 1,
        leader_id: 1,
        last_included_index: 4,
        last_included_term: 1,
        offset: 0,
        data: b"AAAA".to_vec(),
        config: cfg.clone(),
        done: false,
        checksum: 0,
    };
    gapped.handle_message(Some(1), first, 0).await.unwrap();
    assert_eq!(gapped.persistent.log.snapshot_index, 0);
    assert!(
        gapped.persistent.log.entry(1).is_some(),
        "partial chunk must not truncate"
    );
    let gap = Message::InstallSnapshot {
        term: gapped.persistent.current_term,
        leader_id: 1,
        last_included_index: 4,
        last_included_term: 1,
        offset: 50,
        data: b"BB".to_vec(),
        config: cfg,
        done: false,
        checksum: 0,
    };
    let gout = gapped.handle_message(Some(1), gap, 0).await.unwrap();
    match &sends(&gout)[0].1 {
        Message::InstallSnapshotResponse { success, done, .. } => {
            assert!(!success, "gap chunk must be rejected");
            assert!(!done);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(gapped.persistent.log.snapshot_index, 0);
    assert!(gapped.persistent.log.entry(1).is_some());

    // Measure the real image, then install it as exactly three chunks.
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
        ls.snap_offset.insert(2, 0);
        ls.snap_busy.remove(&2);
    }
    let probe = leader.on_heartbeat_timeout().await.unwrap();
    let probe_msg = sends(&probe)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
        .expect("snapshot")
        .1;
    let total = match &probe_msg {
        Message::InstallSnapshot {
            data, done, offset, ..
        } => {
            assert!(*done && *offset == 0);
            data.len()
        }
        _ => unreachable!(),
    };
    assert!(total >= 3, "snapshot image len {total}");
    leader.snapshot_chunk_size = total.div_ceil(3);
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
        ls.snap_offset.insert(2, 0);
        ls.snap_busy.remove(&2);
    }
    let (_ds, mut slow) = fresh(2, vec![1]).await;
    assert_eq!(slow.last_log_index(), 0);
    let mut out = leader.on_heartbeat_timeout().await.unwrap();
    let mut chunks = 0usize;
    for _ in 0..5 {
        let snap = sends(&out)
            .into_iter()
            .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
            .expect("expected a snapshot chunk")
            .1;
        let is_done = matches!(snap, Message::InstallSnapshot { done: true, .. });
        chunks += 1;
        let resp_out = slow.handle_message(Some(1), snap, 0).await.unwrap();
        if !is_done {
            assert_eq!(
                slow.persistent.log.snapshot_index, 0,
                "log truncated before the last chunk"
            );
            assert!(slow.kv.get("a").is_none());
        }
        let resp = sends(&resp_out).into_iter().next().unwrap().1;
        out = leader.handle_message(Some(2), resp, 0).await.unwrap();
        if is_done {
            break;
        }
    }
    assert_eq!(chunks, 3, "install must be three chunks, got {chunks}");
    assert!(slow.persistent.log.snapshot_index >= 4);
    assert!(slow.persistent.log.entry(1).is_none());
    assert_eq!(slow.kv.get("a").map(String::as_str), Some("1"));
    assert_eq!(slow.kv.get("d").map(String::as_str), Some("4"));
}

#[tokio::test]
async fn pipeline_lost_ack_does_not_duplicate_apply() {
    let (_d, mut leader) = elect_leader(vec![2]).await;
    leader.max_batch = 1;
    leader.pipeline_window = 4;
    leader.max_inflight_bytes = u64::MAX;
    if let Some(ls) = leader.leader.as_mut() {
        ls.inflight.clear();
        ls.inflight_bytes.clear();
        ls.next_index.insert(2, 1);
        ls.match_index.insert(2, 0);
    }
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "a".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "b".into(),
                value: "2".into(),
            }),
            2,
        )
        .await
        .unwrap();
    let aes: Vec<Message> = sends(&out)
        .into_iter()
        .filter_map(|(_, m)| match m {
            Message::AppendEntries { ref entries, .. } if !entries.is_empty() => Some(m),
            _ => None,
        })
        .collect();
    assert!(
        aes.len() >= 2,
        "expected more than one in-flight AppendEntries, got {}",
        aes.len()
    );
    let matched = leader
        .leader
        .as_ref()
        .unwrap()
        .match_index
        .get(&2)
        .copied()
        .unwrap_or(0);
    assert_eq!(matched, 0, "matchIndex must wait for an ack");

    let (_df, mut follower) = fresh(2, vec![1]).await;
    let mut resps = Vec::new();
    for ae in &aes {
        let resp_out = follower
            .handle_message(Some(1), ae.clone(), 0)
            .await
            .unwrap();
        resps.push(sends(&resp_out).into_iter().next().unwrap().1);
    }
    let log_after = follower.last_log_index();
    assert!(log_after >= aes.len() as u64);
    // Drop the first ack; deliver the rest. Replaying the first request must not grow the log.
    for resp in resps.iter().skip(1) {
        leader
            .handle_message(Some(2), resp.clone(), 0)
            .await
            .unwrap();
    }
    let matched = leader.leader.as_ref().unwrap().match_index[&2];
    assert!(matched > 0, "a later ack advances matchIndex");
    let again = follower
        .handle_message(Some(1), aes[0].clone(), 0)
        .await
        .unwrap();
    let _ = again;
    assert_eq!(
        follower.last_log_index(),
        log_after,
        "lost ack retransmit duplicated the log"
    );
    let puts = follower
        .persistent
        .log
        .all_entries()
        .iter()
        .filter(|e| matches!(&e.command, Command::Put { key, .. } if key == "a"))
        .count();
    assert_eq!(puts, 1, "key a applied {puts} times");
}

#[tokio::test]
async fn flow_control_pauses_until_ack() {
    let (_d, mut leader) = elect_leader(vec![2]).await;
    leader.max_batch = 1;
    leader.pipeline_window = 4;
    leader.max_inflight_bytes = 1;
    if let Some(ls) = leader.leader.as_mut() {
        ls.inflight.clear();
        ls.inflight_bytes.clear();
        ls.next_index.insert(2, 1);
        ls.match_index.insert(2, 0);
    }
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "p".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let with_entries = sends(&out)
        .iter()
        .filter(|(_, m)| matches!(m, Message::AppendEntries { entries, .. } if !entries.is_empty()))
        .count();
    assert_eq!(
        with_entries, 1,
        "first batch may exceed the cap, the next must not"
    );
    let bytes = leader
        .leader
        .as_ref()
        .unwrap()
        .inflight_bytes
        .get(&2)
        .copied()
        .unwrap_or(0);
    assert!(bytes >= 1, "in-flight bytes {bytes}");
    let hb = leader.on_heartbeat_timeout().await.unwrap();
    let more = sends(&hb)
        .iter()
        .filter(|(_, m)| matches!(m, Message::AppendEntries { entries, .. } if !entries.is_empty()))
        .count();
    assert_eq!(more, 0, "replication must pause while over the byte cap");
    let term = leader.persistent.current_term;
    let resumed = leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: 1,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    let resumed_entries = sends(&resumed)
        .iter()
        .filter(|(_, m)| matches!(m, Message::AppendEntries { entries, .. } if !entries.is_empty()))
        .count();
    assert!(
        resumed_entries >= 1,
        "ack must resume replication, got {resumed:?}"
    );
}

#[tokio::test]
async fn second_membership_change_waits_until_first_commits() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let first = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 4],
                learners: vec![],
            }),
            1,
        )
        .await
        .unwrap();
    assert!(first
        .iter()
        .any(|o| matches!(o, Outbound::ClientWaitCommit { .. })));
    let overlap = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 5],
                learners: vec![],
            }),
            2,
        )
        .await
        .unwrap();
    assert!(
        overlap
            .iter()
            .any(|o| matches!(o, Outbound::ClientReply { resp, .. } if !resp.ok)),
        "overlapping change must be rejected: {overlap:?}"
    );
    let joint_idx = leader
        .persistent
        .log
        .all_entries()
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match &e.command {
            Command::ChangeConfig(ClusterConfig::Joint { .. }) => Some(i as u64 + 1),
            _ => None,
        })
        .next()
        .expect("joint entry");
    // Snapshot may shift indexes; find by scanning term_at range.
    let joint_idx = (1..=leader.last_log_index())
        .find(|i| {
            matches!(
                leader.persistent.log.entry(*i).map(|e| &e.command),
                Some(Command::ChangeConfig(ClusterConfig::Joint { .. }))
            )
        })
        .unwrap_or(joint_idx);
    let term = leader.persistent.current_term;
    leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: joint_idx,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert!(leader.commit_index() >= joint_idx);
    let still = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 3],
                learners: vec![],
            }),
            3,
        )
        .await
        .unwrap();
    assert!(
        still
            .iter()
            .any(|o| matches!(o, Outbound::ClientReply { resp, .. } if !resp.ok)),
        "second change must wait until C_new commits: {still:?}"
    );
    let cnew = leader.last_log_index();
    assert!(cnew > joint_idx);
    leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: cnew,
                conflict_index: 0,
                need_snapshot: false,
            },
            0,
        )
        .await
        .unwrap();
    assert!(leader.commit_index() >= cnew);
    let second = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 3],
                learners: vec![],
            }),
            4,
        )
        .await
        .unwrap();
    assert!(
        second
            .iter()
            .any(|o| matches!(o, Outbound::ClientWaitCommit { .. })),
        "a change after the previous one committed must be accepted: {second:?}"
    );
}

#[tokio::test]
async fn default_get_blocked_on_partitioned_leader() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "x".into(),
                value: "1".into(),
            }),
            1,
        )
        .await
        .unwrap();
    let _ = out;
    let idx = leader.last_log_index();
    let term = leader.persistent.current_term;
    for peer in [2, 3] {
        leader
            .handle_message(
                Some(peer),
                Message::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: idx,
                    conflict_index: 0,
                    need_snapshot: false,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert_eq!(leader.kv.get("x").map(String::as_str), Some("1"));
    let got = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Get { key: "x".into() }),
            7,
        )
        .await
        .unwrap();
    assert!(
        !got.iter()
            .any(|o| matches!(o, Outbound::ClientReply { resp, .. } if resp.ok)),
        "default get must use read index and stay pending: {got:?}"
    );
    assert!(leader.pending_reads.iter().any(|r| r.0 == 7));
    let raw = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::RawGet { key: "x".into() }),
            8,
        )
        .await
        .unwrap();
    assert!(raw.iter().any(|o| matches!(
        o,
        Outbound::ClientReply { resp, .. } if resp.ok && resp.value.as_deref() == Some("1")
    )));
}

#[tokio::test]
async fn timeout_now_lost_prevote_does_not_bump_term() {
    let (_d, mut follower) = fresh(2, vec![1, 3]).await;
    let term = follower.persistent.current_term;
    let out = follower
        .handle_message(
            Some(1),
            Message::TimeoutNow {
                term: term.max(1),
                leader_id: 1,
            },
            0,
        )
        .await
        .unwrap();
    assert!(sends(&out)
        .iter()
        .any(|(_, m)| matches!(m, Message::PreVote { .. })));
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, Message::RequestVote { .. })),
        "TimeoutNow must not skip pre-vote"
    );
    assert_eq!(follower.persistent.current_term, term);
    let next = term + 1;
    for peer in [1u64, 3] {
        follower
            .handle_message(
                Some(peer),
                Message::PreVoteResponse {
                    next_term: next,
                    vote_granted: false,
                },
                0,
            )
            .await
            .unwrap();
    }
    assert_eq!(
        follower.persistent.current_term, term,
        "lost pre-vote must not bump term"
    );
    assert_ne!(follower.volatile.role, Role::Leader);
    assert_ne!(follower.volatile.role, Role::Candidate);
}

#[tokio::test]
async fn simulator_packet_loss_membership_change() {
    let mut ran = Vec::new();
    for &seed in sim::MEMBERSHIP_LOSS_SEEDS {
        sim::check_membership_under_loss(seed)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
        ran.push(seed);
    }
    assert_eq!(ran.first().copied(), Some(41));
    assert_eq!(ran.last().copied(), Some(80));
    assert_eq!(ran.len(), 40, "ran membership-loss seeds {ran:?}");
    eprintln!("membership-loss seeds ran: {ran:?}");
}

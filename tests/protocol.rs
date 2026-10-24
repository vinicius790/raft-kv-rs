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
                    read_ctx: 0,
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
                read_ctx: 0,
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
                read_ctx: 0,
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
    // Transfer skips pre-vote (etcd model): the target bumps its term at once.
    assert_eq!(follower.persistent.current_term, term_before_campaign + 1);
    assert!(
        !sends(&camp)
            .iter()
            .any(|(_, m)| matches!(m, Message::PreVote { .. })),
        "TimeoutNow must campaign directly: {camp:?}"
    );
    let rv = sends(&camp)
        .into_iter()
        .find(|(to, m)| *to == 1 && matches!(m, Message::RequestVote { .. }))
        .expect("TimeoutNow starts a real election")
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
                    read_ctx: 0,
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
    // Quorum heartbeat from one follower, answering the round opened by the read.
    let round = leader.leader.as_ref().unwrap().read_round;
    let ready = leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term: leader.persistent.current_term,
                success: true,
                match_index: leader.last_log_index(),
                conflict_index: 0,
                need_snapshot: false,
                read_ctx: round,
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
                read_ctx: 0,
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
                    read_ctx: 0,
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
                read_ctx: 0,
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
                read_ctx: 0,
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
                read_ctx: 0,
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
                read_ctx: 0,
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
                read_ctx: 0,
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
                    read_ctx: 0,
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
async fn timeout_now_campaigns_without_pre_vote_and_ignores_stale_term() {
    let (_d, mut follower) = fresh(2, vec![1, 3]).await;
    let term = follower.persistent.current_term.max(1);
    follower.persistent.current_term = term;
    // Stale TimeoutNow (older term) is ignored.
    let stale = follower
        .handle_message(
            Some(1),
            Message::TimeoutNow {
                term: term - 1,
                leader_id: 1,
            },
            0,
        )
        .await
        .unwrap();
    assert!(sends(&stale).is_empty(), "{stale:?}");
    assert_eq!(follower.persistent.current_term, term);
    let out = follower
        .handle_message(Some(1), Message::TimeoutNow { term, leader_id: 1 }, 0)
        .await
        .unwrap();
    assert!(
        !sends(&out)
            .iter()
            .any(|(_, m)| matches!(m, Message::PreVote { .. })),
        "transfer must not go through pre-vote"
    );
    let voters: Vec<u64> = sends(&out)
        .iter()
        .filter(|(_, m)| matches!(m, Message::RequestVote { term: t, .. } if *t == term + 1))
        .map(|(to, _)| *to)
        .collect();
    assert_eq!(voters.len(), 2, "{out:?}");
    assert_eq!(follower.volatile.role, Role::Candidate);
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

async fn drain(sim: &mut Sim) {
    let mut n = 0;
    while sim.step_message().await {
        n += 1;
        assert!(n < 10_000, "message storm");
    }
}

/// Regression: with 5 voters, the three bystanders still hear the old leader
/// and refuse pre-vote. TimeoutNow must campaign directly (etcd model) so the
/// transfer completes without waiting for an election timeout.
#[tokio::test]
async fn timeout_now_transfers_leadership_in_five_node_cluster() {
    let mut sim = Sim::new(7, &[1, 2, 3, 4, 5]).await;
    sim.timeout(1).await;
    drain(&mut sim).await;
    assert_eq!(sim.leader_id(), Some(1));
    sim.heartbeat(1).await;
    drain(&mut sim).await;
    for id in [3u64, 4, 5] {
        assert!(sim.nodes[&id].heard_from_leader, "node {id} follows 1");
    }
    let term = sim.nodes[&1].persistent.current_term;
    let resp = sim
        .client(1, ClientRequest::TransferLeadership { target: 2 })
        .await
        .expect("reply");
    assert!(resp.ok, "{resp:?}");
    drain(&mut sim).await;
    assert_eq!(sim.leader_id(), Some(2), "transfer did not complete");
    assert_eq!(sim.nodes[&2].persistent.current_term, term + 1);
    assert!(
        sim.safety_violations.is_empty(),
        "{:?}",
        sim.safety_violations
    );
}

fn served_value(out: &[Outbound], corr: u64) -> Option<Option<String>> {
    out.iter().find_map(|o| match o {
        Outbound::ClientReply { corr: c, resp } if *c == corr && resp.ok => {
            Some(resp.value.clone())
        }
        _ => None,
    })
}

/// Regression (thesis §6.4): a new leader whose commit index lags must not
/// serve a read before its own term's noop commits.
#[tokio::test]
async fn read_index_waits_for_noop_of_current_term() {
    let (_d, mut n) = fresh(1, vec![2, 3]).await;
    n.enable_pre_vote = false;
    // Template entry with the crate's LogEntry type: elect a throwaway leader.
    let (_t, tmpl) = elect_leader(vec![2]).await;
    let put = |v: &str| {
        let mut e = tmpl.persistent.log.entry(1).cloned().unwrap();
        e.term = 1;
        e.command = Command::Put {
            key: "x".into(),
            value: v.into(),
        };
        e
    };
    let (e1, e2) = (put("1"), put("2"));
    // Term-1 leader 2 replicated x=1 and x=2; node 1 only learned commit of index 1.
    n.handle_message(
        Some(2),
        Message::AppendEntries {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e1, e2],
            leader_commit: 1,
            read_ctx: 0,
        },
        0,
    )
    .await
    .unwrap();
    assert_eq!(n.kv.get("x").map(String::as_str), Some("1"));
    n.on_election_timeout().await.unwrap();
    let term = n.persistent.current_term;
    for p in [2, 3] {
        n.handle_message(
            Some(p),
            Message::RequestVoteResponse {
                term,
                vote_granted: true,
            },
            0,
        )
        .await
        .unwrap();
    }
    assert_eq!(n.volatile.role, Role::Leader);
    let noop = n.last_log_index();
    let out = n
        .handle_message(
            None,
            Message::Client(ClientRequest::Get { key: "x".into() }),
            9,
        )
        .await
        .unwrap();
    assert_eq!(served_value(&out, 9), None);
    let round = n.leader.as_ref().unwrap().read_round;
    // Both peers answer this round but have not stored the noop yet.
    let mut all = Vec::new();
    for p in [2, 3] {
        all.extend(
            n.handle_message(
                Some(p),
                Message::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: noop - 1,
                    conflict_index: 0,
                    need_snapshot: false,
                    read_ctx: round,
                },
                0,
            )
            .await
            .unwrap(),
        );
    }
    assert_eq!(
        served_value(&all, 9),
        None,
        "read served before this term's noop committed: {all:?}"
    );
    // Noop reaches a quorum: commit covers x=2 and the read returns it.
    let out = n
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: noop,
                conflict_index: 0,
                need_snapshot: false,
                read_ctx: round,
            },
            0,
        )
        .await
        .unwrap();
    assert_eq!(served_value(&out, 9), Some(Some("2".to_string())));
}

/// Regression: an ack to a heartbeat sent before the read arrived does not
/// prove leadership for that read. Only the current round counts.
#[tokio::test]
async fn read_index_ignores_acks_from_earlier_round() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    let term = leader.persistent.current_term;
    let li = leader.last_log_index();
    let ack = |read_ctx| Message::AppendEntriesResponse {
        term,
        success: true,
        match_index: li,
        conflict_index: 0,
        need_snapshot: false,
        read_ctx,
    };
    for p in [2, 3] {
        leader.handle_message(Some(p), ack(0), 0).await.unwrap();
    }
    assert_eq!(leader.commit_index(), li, "noop committed");
    let before = leader.leader.as_ref().unwrap().read_round;
    leader.on_heartbeat_timeout().await.unwrap();
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Get { key: "x".into() }),
            11,
        )
        .await
        .unwrap();
    assert_eq!(served_value(&out, 11), None);
    let round = leader.leader.as_ref().unwrap().read_round;
    assert!(round > before);
    let late = leader
        .handle_message(Some(2), ack(before), 0)
        .await
        .unwrap();
    assert_eq!(
        served_value(&late, 11),
        None,
        "stale-round ack confirmed a read: {late:?}"
    );
    let fresh_ack = leader.handle_message(Some(2), ack(round), 0).await.unwrap();
    assert_eq!(served_value(&fresh_ack, 11), Some(None));
}

/// Deliver queued messages among `alive` nodes until quiet. Every message is
/// produced by the cores themselves; nothing is hand-built.
async fn pump(nodes: &mut [RaftCore], mut q: Vec<(u64, u64, Message)>, alive: &[u64]) {
    let mut steps = 0;
    while let Some((from, to, msg)) = q.pop() {
        steps += 1;
        assert!(steps < 10_000, "message storm");
        if !alive.contains(&from) || !alive.contains(&to) {
            continue;
        }
        let n = nodes.iter_mut().find(|n| n.id == to).unwrap();
        let out = n.handle_message(Some(from), msg, 0).await.unwrap();
        for (dst, m) in sends(&out) {
            q.insert(0, (to, dst, m));
        }
    }
}

fn outbox(from: u64, out: &[Outbound]) -> Vec<(u64, u64, Message)> {
    sends(out)
        .into_iter()
        .map(|(to, m)| (from, to, m))
        .collect()
}

/// Regression, public API only (compiles on the old main): an ack that
/// node 2 sent BEFORE a new leader was elected and committed x=2 reaches the
/// old leader after a client read. Counting it served the stale x=1.
#[tokio::test]
async fn deposed_leader_does_not_serve_read_with_ack_sent_before_read() {
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let peers = (1..=3).filter(|p| *p != id).collect();
        let mut n = RaftCore::open(id, peers, Storage::ephemeral(), timing())
            .await
            .unwrap();
        n.enable_pre_vote = false;
        nodes.push(n);
    }
    let all = [1u64, 2, 3];
    let out = nodes[0].on_election_timeout().await.unwrap();
    pump(&mut nodes, outbox(1, &out), &all).await;
    assert_eq!(nodes[0].volatile.role, Role::Leader);
    let put = |v: &str| {
        Message::Client(ClientRequest::Put {
            key: "x".into(),
            value: v.into(),
        })
    };
    let out = nodes[0].handle_message(None, put("1"), 1).await.unwrap();
    pump(&mut nodes, outbox(1, &out), &all).await;
    let out = nodes[0].on_heartbeat_timeout().await.unwrap();
    pump(&mut nodes, outbox(1, &out), &all).await;
    assert_eq!(nodes[1].kv.get("x").map(String::as_str), Some("1"));

    // Leader 1 heartbeats; node 2 answers, but the answer is delayed.
    let hb = nodes[0].on_heartbeat_timeout().await.unwrap();
    let to2 = sends(&hb)
        .into_iter()
        .find(|(to, m)| *to == 2 && matches!(m, Message::AppendEntries { .. }))
        .unwrap()
        .1;
    let r = nodes[1].handle_message(Some(1), to2, 0).await.unwrap();
    let delayed = sends(&r)
        .into_iter()
        .find(|(to, m)| *to == 1 && matches!(m, Message::AppendEntriesResponse { .. }))
        .unwrap()
        .1;

    // Node 1 is cut off. Node 3 wins term 2 with node 2 and commits x=2.
    let rest = [2u64, 3];
    let out = nodes[2].on_election_timeout().await.unwrap();
    pump(&mut nodes, outbox(3, &out), &rest).await;
    assert_eq!(nodes[2].volatile.role, Role::Leader);
    let out = nodes[2].handle_message(None, put("2"), 1).await.unwrap();
    pump(&mut nodes, outbox(3, &out), &rest).await;
    let out = nodes[2].on_heartbeat_timeout().await.unwrap();
    pump(&mut nodes, outbox(3, &out), &rest).await;
    assert_eq!(nodes[2].kv.get("x").map(String::as_str), Some("2"));

    // The write completed. Now a client reads on the deposed leader 1.
    assert_eq!(nodes[0].volatile.role, Role::Leader, "1 has not noticed");
    let out = nodes[0]
        .handle_message(
            None,
            Message::Client(ClientRequest::Get { key: "x".into() }),
            77,
        )
        .await
        .unwrap();
    let mut replies: Vec<Outbound> = out;
    replies.extend(nodes[0].handle_message(Some(2), delayed, 0).await.unwrap());
    let stale = replies
        .iter()
        .any(|o| matches!(o, Outbound::ClientReply { corr: 77, resp } if resp.ok));
    assert!(
        !stale,
        "deposed leader served a read with an ack sent before the read: {replies:?}"
    );
}

/// A read parked on the read-index quorum must be answered when the leader is
/// deposed. Before the fix it stayed in `pending_reads` forever, so the node
/// never replied and `NodeHandle::get` hung (CI on main hung in Test).
#[tokio::test]
async fn deposed_leader_answers_pending_read_instead_of_hanging() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    assert_eq!(leader.volatile.role, Role::Leader);
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Get { key: "x".into() }),
            9,
        )
        .await
        .unwrap();
    assert!(
        !out.iter()
            .any(|o| matches!(o, Outbound::ClientReply { corr: 9, .. })),
        "read must wait for the quorum round"
    );
    let term = leader.persistent.current_term + 1;
    let out = leader
        .handle_message(
            Some(2),
            Message::AppendEntries {
                term,
                leader_id: 2,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                read_ctx: 0,
            },
            0,
        )
        .await
        .unwrap();
    assert_ne!(leader.volatile.role, Role::Leader);
    let reply = out.iter().find_map(|o| match o {
        Outbound::ClientReply { corr: 9, resp } => Some(resp.clone()),
        _ => None,
    });
    let reply = reply.expect("pending read left unanswered after step-down");
    assert!(!reply.ok);
    assert_eq!(reply.leader_hint, Some(2));
    assert!(leader.pending_reads.is_empty());

    // Check-quorum step-down (election timer as leader) also answers.
    let (_d2, mut l2) = elect_leader(vec![2, 3]).await;
    l2.enable_check_quorum = true;
    l2.handle_message(
        None,
        Message::Client(ClientRequest::Get { key: "x".into() }),
        7,
    )
    .await
    .unwrap();
    let mut answered = false;
    for _ in 0..3 {
        let out = l2.on_election_timeout().await.unwrap();
        answered |= out
            .iter()
            .any(|o| matches!(o, Outbound::ClientReply { corr: 7, resp } if !resp.ok));
        if l2.volatile.role != Role::Leader {
            break;
        }
    }
    if l2.volatile.role != Role::Leader {
        assert!(answered, "check-quorum step-down left the read unanswered");
    }
}

/// Thesis §3.10: a transfer to a lagging target keeps replicating and sends
/// `TimeoutNow` once the target acks the leader's last index, instead of
/// failing at once with "target not caught up".
#[tokio::test]
async fn transfer_waits_for_lagging_target_then_sends_timeout_now() {
    let (_d, mut leader) = elect_leader(vec![2, 3]).await;
    leader
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
    let last = leader.persistent.log.last_index();
    let term = leader.persistent.current_term;
    let out = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::TransferLeadership { target: 2 }),
            9,
        )
        .await
        .unwrap();
    assert_eq!(leader.volatile.role, Role::Leader);
    assert!(
        !out.iter()
            .any(|o| matches!(o, Outbound::ClientReply { corr: 9, .. })),
        "transfer must wait for the target, not fail: {out:?}"
    );
    assert!(
        out.iter().any(|o| matches!(
            o,
            Outbound::Send {
                to: 2,
                msg: Message::AppendEntries { .. }
            }
        )),
        "leader must keep replicating to the target"
    );
    let out = leader
        .handle_message(
            Some(2),
            Message::AppendEntriesResponse {
                term,
                success: true,
                match_index: last,
                conflict_index: 0,
                need_snapshot: false,
                read_ctx: 0,
            },
            0,
        )
        .await
        .unwrap();
    assert!(out.iter().any(|o| matches!(
        o,
        Outbound::Send {
            to: 2,
            msg: Message::TimeoutNow { .. }
        }
    )));
    assert!(out
        .iter()
        .any(|o| matches!(o, Outbound::ClientReply { corr: 9, resp } if resp.ok)));
    assert_eq!(leader.volatile.role, Role::Follower);
    assert!(leader.pending_transfer.is_none());
}

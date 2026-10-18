//! Invariantes que o núcleo já implementava e que precisavam de prova explícita:
//! recover depois de crash, joint com queda no meio, follower que só volta por
//! InstallSnapshot, put de sessão que não duplica, e no máximo um líder por termo.

use std::collections::BTreeMap;

use raft_mvp::raft::{ClusterConfig, Membership, Outbound, RaftCore, Role, TimingConfig};
use raft_mvp::rpc::{ClientRequest, Message};
use raft_mvp::sim::Sim;
use raft_mvp::storage::Storage;
use raft_mvp::{Command, SessionClient};

fn timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 5,
        election_min_ms: 20,
        election_max_ms: 40,
    }
}

async fn open_at(dir: &std::path::Path, id: u64, peers: Vec<u64>) -> RaftCore {
    let storage = Storage::open(dir).await.unwrap();
    let mut core = RaftCore::open(id, peers, storage, timing()).await.unwrap();
    core.enable_pre_vote = false;
    core
}

fn sends(out: &[Outbound]) -> Vec<(u64, Message)> {
    out.iter()
        .filter_map(|o| match o {
            Outbound::Send { to, msg } => Some((*to, msg.clone())),
            _ => None,
        })
        .collect()
}

fn first_to(out: &[Outbound], to: u64) -> Message {
    sends(out)
        .into_iter()
        .find(|(id, _)| *id == to)
        .unwrap_or_else(|| panic!("no message to {to}"))
        .1
}

async fn elect(leader: &mut RaftCore, voters: &[u64]) {
    leader.enable_pre_vote = false;
    let out = leader.on_election_timeout().await.unwrap();
    if leader.volatile.role == Role::Leader {
        let _ = out;
        return;
    }
    let term = leader.persistent.current_term;
    for &peer in voters {
        if peer == leader.id {
            continue;
        }
        leader
            .handle_message(
                Some(peer),
                Message::RequestVoteResponse {
                    term,
                    vote_granted: true,
                },
                0,
            )
            .await
            .unwrap();
        if leader.volatile.role == Role::Leader {
            return;
        }
    }
    assert_eq!(leader.volatile.role, Role::Leader);
}

fn config_kind(core: &RaftCore) -> &'static str {
    match &core.persistent.config {
        ClusterConfig::Joint { .. } => "joint",
        ClusterConfig::Stable(m) => {
            let _ = m;
            "stable"
        }
    }
}

fn log_has_joint_then_cnew(core: &RaftCore) -> (u64, u64) {
    let mut joint = None;
    let mut cnew = None;
    let last = core.last_log_index();
    for i in 1..=last {
        if let Some(e) = core.persistent.log.entry(i) {
            match &e.command {
                Command::ChangeConfig(ClusterConfig::Joint { .. }) => joint = Some(i),
                Command::ChangeConfig(ClusterConfig::Stable(_)) if joint.is_some() => {
                    cnew = Some(i);
                }
                _ => {}
            }
        }
    }
    (
        joint.expect("joint entry missing"),
        cnew.expect("C_new entry missing"),
    )
}

#[tokio::test]
async fn recover_reloads_term_vote_log_and_applied_kv() {
    let dir = tempfile::tempdir().unwrap();
    let committed = {
        let mut leader = open_at(dir.path(), 1, vec![]).await;
        elect(&mut leader, &[]).await;
        let term = leader.persistent.current_term;
        assert_eq!(leader.persistent.voted_for, Some(1));
        leader
            .handle_message(
                None,
                Message::Client(ClientRequest::Put {
                    key: "durable".into(),
                    value: "yes".into(),
                }),
                1,
            )
            .await
            .unwrap();
        assert_eq!(leader.kv.get("durable").map(String::as_str), Some("yes"));
        let commit = leader.commit_index();
        let last = leader.last_log_index();
        drop(leader);
        (term, commit, last)
    };

    let recovered = open_at(dir.path(), 1, vec![]).await;
    assert_eq!(recovered.volatile.role, Role::Follower);
    assert_eq!(recovered.persistent.current_term, committed.0);
    assert_eq!(recovered.persistent.voted_for, Some(1));
    assert_eq!(recovered.commit_index(), committed.1);
    assert_eq!(recovered.last_log_index(), committed.2);
    assert_eq!(
        recovered.kv.get("durable").map(String::as_str),
        Some("yes"),
        "committed KV must be rebuilt from the durable log, not from a memory cache"
    );
    assert_eq!(
        recovered.commits, 0,
        "process counters do not survive restart"
    );
    assert_eq!(recovered.elections, 0);
}

#[tokio::test]
async fn joint_transition_survives_crash_between_c_old_and_c_new() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let mut leader = open_at(d1.path(), 1, vec![2, 3]).await;
    let mut follower = open_at(d2.path(), 2, vec![1, 3]).await;
    elect(&mut leader, &[2, 3]).await;

    // C_old = {1,2,3}. Target C_new = {1,2,4}. Node 4 is only a config id.
    let proposed = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 4],
                learners: vec![],
            }),
            9,
        )
        .await
        .unwrap();
    assert!(
        matches!(leader.persistent.config, ClusterConfig::Joint { .. }),
        "propose must enter joint, got {}",
        config_kind(&leader)
    );
    let joint_idx = leader.last_log_index();
    assert!(
        leader.commit_index() < joint_idx,
        "joint must still be uncommitted before any ack"
    );
    let ae = first_to(&proposed, 2);

    // Crash before the follower's ack is applied. Disk already has the joint entry.
    let term_at_crash = leader.persistent.current_term;
    drop(leader);
    let mut leader = open_at(d1.path(), 1, vec![2, 3]).await;
    assert!(
        matches!(leader.persistent.config, ClusterConfig::Joint { .. }),
        "recover must keep the uncommitted joint config, got {}",
        leader.persistent.config.summary()
    );
    assert!(leader.commit_index() < joint_idx);
    assert_eq!(leader.persistent.current_term, term_at_crash);
    assert_eq!(leader.volatile.role, Role::Follower);

    // The follower can still accept the joint entry that was in flight.
    let _accepted = follower.handle_message(Some(1), ae, 0).await.unwrap();
    assert!(matches!(
        follower.persistent.config,
        ClusterConfig::Joint { .. }
    ));

    elect(&mut leader, &[2, 3]).await;
    // Finish replication of whatever the new term appended, then the joint entry
    // if the follower does not have it yet. Drive acks from node 2 only: that
    // peer sits in both majorities ({1,2,3} and {1,2,4}).
    let mut guard = 0;
    while leader.commit_index() < joint_idx && guard < 8 {
        guard += 1;
        let out = leader.on_heartbeat_timeout().await.unwrap();
        let msg = first_to(&out, 2);
        let back = follower.handle_message(Some(1), msg, 0).await.unwrap();
        let resp = sends(&back).into_iter().next().unwrap().1;
        leader.handle_message(Some(2), resp, 0).await.unwrap();
    }
    assert!(
        leader.commit_index() >= joint_idx,
        "joint did not commit after recover, commit={} joint={joint_idx}",
        leader.commit_index()
    );
    let (joint_at, cnew_at) = log_has_joint_then_cnew(&leader);
    assert_eq!(joint_at, joint_idx);
    assert!(
        leader.commit_index() < cnew_at,
        "C_new must not be committed in the same step we are about to crash"
    );
    assert!(
        matches!(
            leader.persistent.config,
            ClusterConfig::Stable(Membership { .. })
        ),
        "once C_new is in the log the latest config is C_new, got {}",
        leader.persistent.config.summary()
    );
    match &leader.persistent.config {
        ClusterConfig::Stable(m) => assert_eq!(m.voters, vec![1, 2, 4]),
        other => panic!("expected stable C_new, got {other:?}"),
    }

    // Second crash: C_new is durable and uncommitted. Followers may still be on joint.
    drop(leader);
    let mut leader = open_at(d1.path(), 1, vec![2, 3]).await;
    assert_eq!(leader.volatile.role, Role::Follower);
    match &leader.persistent.config {
        ClusterConfig::Stable(m) => assert_eq!(
            m.voters,
            vec![1, 2, 4],
            "recover must not rewind from C_new back to the committed joint config"
        ),
        other => panic!("rewound config: {other:?}"),
    }
    assert!(leader.commit_index() < cnew_at);
    assert!(leader.persistent.log.entry(cnew_at).is_some());

    elect(&mut leader, &[2]).await;
    assert_eq!(leader.volatile.role, Role::Leader);
    let mut guard = 0;
    while leader.commit_index() < cnew_at && guard < 8 {
        guard += 1;
        let out = leader.on_heartbeat_timeout().await.unwrap();
        let msg = first_to(&out, 2);
        let back = follower.handle_message(Some(1), msg, 0).await.unwrap();
        let resp = sends(&back).into_iter().next().unwrap().1;
        leader.handle_message(Some(2), resp, 0).await.unwrap();
    }
    assert!(
        leader.commit_index() >= cnew_at,
        "C_new never committed, commit={} cnew={cnew_at}",
        leader.commit_index()
    );
    match &leader.persistent.config {
        ClusterConfig::Stable(m) => assert_eq!(m.voters, vec![1, 2, 4]),
        other => panic!("final config {other:?}"),
    }
    match &follower.persistent.config {
        ClusterConfig::Stable(m) => assert_eq!(m.voters, vec![1, 2, 4]),
        other => panic!("follower did not reach C_new: {other:?}"),
    }
}

#[tokio::test]
async fn lagged_follower_refuses_prefix_and_recovers_only_by_install_snapshot() {
    let (_d1, mut leader) = {
        let dir = tempfile::tempdir().unwrap();
        let mut leader = open_at(dir.path(), 1, vec![2]).await;
        elect(&mut leader, &[2]).await;
        (dir, leader)
    };
    let (_d2, mut slow) = {
        let dir = tempfile::tempdir().unwrap();
        let follower = open_at(dir.path(), 2, vec![1]).await;
        (dir, follower)
    };

    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3")] {
        leader
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
        let idx = leader.last_log_index();
        let term = leader.persistent.current_term;
        leader
            .handle_message(
                Some(2),
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
    let committed = leader.commit_index();
    assert!(committed >= 3, "commit {committed}");
    leader.compact_log(committed).await.unwrap();
    assert!(leader.persistent.log.entry(1).is_none());
    let snap_idx = leader.persistent.log.snapshot_index;

    // AppendEntries aimed at the discarded prefix cannot fill the follower.
    let rejected = slow
        .handle_message(
            Some(1),
            Message::AppendEntries {
                term: leader.persistent.current_term,
                leader_id: 1,
                prev_log_index: snap_idx.saturating_sub(1),
                prev_log_term: 1,
                entries: vec![raft_mvp::LogEntry {
                    term: leader.persistent.current_term,
                    command: Command::Put {
                        key: "a".into(),
                        value: "replayed".into(),
                    },
                }],
                leader_commit: snap_idx,
                read_ctx: 0,
            },
            0,
        )
        .await
        .unwrap();
    match &sends(&rejected)[0].1 {
        Message::AppendEntriesResponse {
            success,
            need_snapshot,
            ..
        } => {
            assert!(!success);
            assert!(
                *need_snapshot,
                "a compacted prefix must be answered with need_snapshot"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(slow.last_log_index(), 0);
    assert!(slow.kv.get("a").is_none(), "AE must not install the KV");
    assert_eq!(slow.persistent.log.snapshot_index, 0);

    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
        ls.snap_offset.insert(2, 0);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    let snap = sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
        .expect("catch-up of a compacted follower is InstallSnapshot")
        .1;
    assert!(
        !sends(&out).iter().any(
            |(_, m)| matches!(m, Message::AppendEntries { entries, .. } if !entries.is_empty())
        ),
        "leader must not ship the truncated prefix as AppendEntries"
    );
    let installed = slow.handle_message(Some(1), snap, 0).await.unwrap();
    assert!(sends(&installed)
        .iter()
        .any(|(_, m)| matches!(m, Message::InstallSnapshotResponse { success: true, .. })));
    assert_eq!(slow.kv.get("a").map(String::as_str), Some("1"));
    assert_eq!(slow.kv.get("c").map(String::as_str), Some("3"));
    assert_ne!(slow.kv.get("a").map(String::as_str), Some("replayed"));
    assert!(slow.persistent.log.entry(1).is_none());
    assert!(slow.persistent.log.snapshot_index >= snap_idx);
}

#[tokio::test]
async fn session_retry_does_not_duplicate_and_read_sees_one_value() {
    let dir = tempfile::tempdir().unwrap();
    let mut leader = open_at(dir.path(), 1, vec![]).await;
    elect(&mut leader, &[]).await;
    let mut session = SessionClient::new(42);

    let serial = session.serial_for_attempt();
    let first = ClientRequest::SessionPut {
        client_id: session.client_id(),
        serial,
        key: "balance".into(),
        value: "10".into(),
    };
    leader
        .handle_message(None, Message::Client(first), 1)
        .await
        .unwrap();
    let log_after = leader.last_log_index();
    assert_eq!(leader.kv.get("balance").map(String::as_str), Some("10"));

    // Retry of the same attempt, even with a different payload, must not append.
    assert_eq!(session.serial_for_attempt(), serial);
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: session.client_id(),
                serial: session.serial_for_attempt(),
                key: "balance".into(),
                value: "99".into(),
            }),
            2,
        )
        .await
        .unwrap();
    assert_eq!(leader.last_log_index(), log_after);
    assert_eq!(leader.kv.get("balance").map(String::as_str), Some("10"));

    session.commit();
    let next = session.serial_for_attempt();
    assert_ne!(next, serial);
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: session.client_id(),
                serial: next,
                key: "balance".into(),
                value: "11".into(),
            }),
            3,
        )
        .await
        .unwrap();
    // Retry of the second serial.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::SessionPut {
                client_id: session.client_id(),
                serial: session.serial_for_attempt(),
                key: "balance".into(),
                value: "11-again".into(),
            }),
            4,
        )
        .await
        .unwrap();

    let puts: Vec<_> = (1..=leader.last_log_index())
        .filter_map(|i| leader.persistent.log.entry(i))
        .filter(|e| {
            matches!(
                &e.command,
                Command::SessionPut { client_id, .. } if *client_id == 42
            )
        })
        .collect();
    assert_eq!(
        puts.len(),
        2,
        "one log entry per committed serial, not per retry"
    );

    let read = leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Get {
                key: "balance".into(),
            }),
            5,
        )
        .await
        .unwrap();
    let value = read.iter().find_map(|o| match o {
        Outbound::ClientReply { resp, .. } if resp.ok => resp.value.clone(),
        _ => None,
    });
    assert_eq!(value.as_deref(), Some("11"));
    assert_ne!(value.as_deref(), Some("99"));
    assert_ne!(value.as_deref(), Some("11-again"));
}

#[tokio::test]
async fn at_most_one_leader_per_term_in_simulator() {
    // Seeds actually stepped here. The broader 1..=80 run lives in
    // tests/protocol.rs; this test only asserts the election-safety half.
    let seeds = [1u64, 2, 7, 17, 41, 80];
    for seed in seeds {
        let mut sim = Sim::new(seed, &[1, 2, 3]).await;
        sim.set_drop_permille(50);
        sim.run(40).await;
        sim.partition_pair(1, 3);
        sim.partition_pair(2, 3);
        sim.run(20).await;
        sim.heal();
        sim.run(40).await;

        let dup: Vec<_> = sim
            .safety_violations
            .iter()
            .filter(|s| s.contains("two leaders"))
            .cloned()
            .collect();
        assert!(
            dup.is_empty(),
            "seed {seed} recorded two leaders in one term: {dup:?} all={:?}",
            sim.safety_violations
        );
        assert!(
            !sim.leaders_by_term.is_empty(),
            "seed {seed} never elected a leader"
        );
        let mut terms = BTreeMap::<u64, u64>::new();
        for (term, id) in &sim.leaders_by_term {
            assert!(
                terms.insert(*term, *id).is_none(),
                "seed {seed}: term {term} mapped twice"
            );
        }
        let mut live: BTreeMap<u64, u64> = BTreeMap::new();
        for (id, node) in &sim.nodes {
            if node.volatile.role == Role::Leader {
                let term = node.persistent.current_term;
                if let Some(prev) = live.insert(term, *id) {
                    panic!("seed {seed}: live leaders {prev} and {id} in term {term}");
                }
            }
        }
    }
}

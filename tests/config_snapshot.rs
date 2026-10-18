//! Config base on truncation (bug 3) and snapshot image / compaction
//! consistency (bug 4).

use raft_mvp::raft::{ClusterConfig, LogEntry, Membership, Outbound, RaftCore, Role, TimingConfig};
use raft_mvp::rpc::{ClientRequest, Message, SnapshotBlob};
use raft_mvp::storage::Storage;
use raft_mvp::Command;

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

fn stable(v: Vec<u64>) -> ClusterConfig {
    ClusterConfig::Stable(Membership::new(v, vec![]))
}

fn ae(
    term: u64,
    leader: u64,
    prev: u64,
    prev_term: u64,
    entries: Vec<LogEntry>,
    commit: u64,
) -> Message {
    Message::AppendEntries {
        term,
        leader_id: leader,
        prev_log_index: prev,
        prev_log_term: prev_term,
        entries,
        leader_commit: commit,
    }
}

/// Leader 1 over {1,2}, then `puts` replicated and committed through follower 2.
async fn leader_with_puts(puts: &[(&str, &str)]) -> (tempfile::TempDir, RaftCore) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).await.unwrap();
    let mut leader = RaftCore::open(1, vec![2], storage, timing()).await.unwrap();
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
    let (_d2, mut follower) = fresh(2, vec![1]).await;
    for (k, v) in puts {
        let out = leader
            .handle_message(
                None,
                Message::Client(ClientRequest::Put {
                    key: (*k).into(),
                    value: (*v).into(),
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
    assert_eq!(leader.commit_index(), leader.last_log_index());
    (dir, leader)
}

async fn forced_snapshot(leader: &mut RaftCore) -> (Vec<u8>, ClusterConfig, u64) {
    if let Some(ls) = leader.leader.as_mut() {
        ls.next_index.insert(2, 1);
    }
    let out = leader.on_heartbeat_timeout().await.unwrap();
    match sends(&out)
        .into_iter()
        .find(|(_, m)| matches!(m, Message::InstallSnapshot { .. }))
        .expect("InstallSnapshot")
        .1
    {
        Message::InstallSnapshot {
            data,
            config,
            last_included_index,
            ..
        } => (data, config, last_included_index),
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn truncated_change_config_restores_base_config() {
    let (dir, mut f) = fresh(2, vec![1, 3]).await;
    let base = stable(vec![1, 2, 3]);
    assert_eq!(f.persistent.config, base);
    // Leader 1 (term 1) replicates an uncommitted C change that removes 2.
    let removed = stable(vec![1, 3]);
    f.handle_message(
        Some(1),
        ae(
            1,
            1,
            0,
            0,
            vec![
                LogEntry {
                    term: 1,
                    command: Command::Noop,
                },
                LogEntry {
                    term: 1,
                    command: Command::ChangeConfig(removed.clone()),
                },
            ],
            1,
        ),
        0,
    )
    .await
    .unwrap();
    assert_eq!(f.persistent.config, removed);
    assert_eq!(f.volatile.role, Role::Learner);
    // Leader 3 (term 2) overwrites index 2 with a Noop.
    f.handle_message(
        Some(3),
        ae(
            2,
            3,
            1,
            1,
            vec![LogEntry {
                term: 2,
                command: Command::Noop,
            }],
            1,
        ),
        0,
    )
    .await
    .unwrap();
    assert_eq!(f.persistent.log.last_index(), 2);
    assert_eq!(f.persistent.config, base, "truncated config must be undone");
    assert_eq!(f.volatile.role, Role::Follower, "2 is a voter again");
    // Durable state rebuilds the same config.
    let reopened = RaftCore::open(
        2,
        vec![1, 3],
        Storage::open(dir.path()).await.unwrap(),
        timing(),
    )
    .await
    .unwrap();
    assert_eq!(reopened.persistent.config, base);
}

#[tokio::test]
async fn snapshot_image_is_state_at_snapshot_index() {
    let (_d, mut leader) = leader_with_puts(&[("a", "1"), ("b", "1")]).await;
    let snap_at = leader.commit_index();
    leader.compact_log(snap_at).await.unwrap();
    assert_eq!(leader.persistent.log.snapshot_index, snap_at);
    // Applied after the snapshot: the image must not carry it.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::Put {
                key: "a".into(),
                value: "2".into(),
            }),
            9,
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
    assert_eq!(leader.kv.get("a").map(String::as_str), Some("2"));
    let (data, _, idx) = forced_snapshot(&mut leader).await;
    assert_eq!(idx, snap_at);
    let blob: SnapshotBlob = bincode::deserialize(&data).unwrap();
    assert_eq!(
        blob.kv.get("a").map(String::as_str),
        Some("1"),
        "image at index {idx} must not include later writes"
    );
}

#[tokio::test]
async fn compaction_never_persists_uncommitted_config() {
    let (dir, mut leader) = leader_with_puts(&[("a", "1")]).await;
    let committed = leader.commit_index();
    // Joint config appended but never acked: uncommitted.
    leader
        .handle_message(
            None,
            Message::Client(ClientRequest::ChangeMembership {
                voters: vec![1, 2, 3],
                learners: vec![],
            }),
            5,
        )
        .await
        .unwrap();
    assert!(leader.persistent.config.is_joint());
    assert_eq!(leader.commit_index(), committed);
    leader.compact_log(committed).await.unwrap();
    assert_eq!(leader.persistent.log.snapshot_index, committed);
    assert!(
        leader.persistent.config.is_joint(),
        "effective config keeps the log suffix"
    );
    let (_, cfg, _) = forced_snapshot(&mut leader).await;
    assert_eq!(
        cfg,
        stable(vec![1, 2]),
        "snapshot config must be the committed one"
    );
    let disk = Storage::open(dir.path()).await.unwrap();
    assert_eq!(disk.persistent().config, stable(vec![1, 2]));
}

#[tokio::test]
async fn compact_below_last_applied_keeps_state_at_that_index() {
    let (dir, mut leader) = leader_with_puts(&[("a", "1"), ("a", "2"), ("a", "3")]).await;
    // Index of the ("a","1") put: noop is 1, puts are 2..=4.
    let at = leader.commit_index() - 2;
    leader.compact_log(at).await.unwrap();
    assert_eq!(leader.persistent.log.snapshot_index, at);
    assert_eq!(leader.kv.get("a").map(String::as_str), Some("3"));
    let disk = Storage::open(dir.path()).await.unwrap();
    let (idx, _, kv, _) = disk.snapshot_meta();
    assert_eq!(idx, at);
    assert_eq!(kv.get("a").map(String::as_str), Some("1"));
    // Restart replays the suffix on top and lands on the live state.
    let re = RaftCore::open(1, vec![2], disk, timing()).await.unwrap();
    assert_eq!(re.kv.get("a").map(String::as_str), Some("3"));
}

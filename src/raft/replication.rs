//! Replication / commit helpers (stable + joint configs).

use std::collections::{HashMap, HashSet};

use crate::quorum::has_config_quorum;
use crate::raft::config::ClusterConfig;
use crate::raft::log::RaftLog;
use crate::rpc::{LogIndex, NodeId, Term};

/// Advance commit_index: largest N replicated on a config quorum with
/// log[N].term == currentTerm (Raft §5.3 / §5.4.2 / §6 joint).
pub fn advance_commit_index(
    current_term: Term,
    log: &RaftLog,
    match_index: &HashMap<NodeId, LogIndex>,
    self_id: NodeId,
    self_last_index: LogIndex,
    cfg: &ClusterConfig,
    current_commit: LogIndex,
) -> LogIndex {
    let mut candidates: Vec<LogIndex> = match_index.values().copied().collect();
    candidates.push(self_last_index);
    candidates.sort_unstable_by(|a, b| b.cmp(a));
    candidates.dedup();

    let mut new_commit = current_commit;
    for &n in &candidates {
        if n <= current_commit {
            continue;
        }
        if log.term_at(n) != Some(current_term) {
            continue;
        }
        let mut acks = HashSet::new();
        acks.insert(self_id);
        for (&id, &m) in match_index {
            if m >= n {
                acks.insert(id);
            }
        }
        if has_config_quorum(cfg, &acks) && n > new_commit {
            new_commit = n;
            break;
        }
    }
    new_commit
}

/// Check AppendEntries consistency: prev_log_index/term must match.
pub fn log_matches(log: &RaftLog, prev_log_index: LogIndex, prev_log_term: Term) -> bool {
    match log.term_at(prev_log_index) {
        Some(t) => t == prev_log_term,
        None => false,
    }
}

/// True when follower cannot accept because prev is below compacted prefix
/// or creates a gap past last_index+1.
pub fn needs_snapshot(log: &RaftLog, prev_log_index: LogIndex) -> bool {
    prev_log_index < log.snapshot_index
        || (prev_log_index > log.last_index() && prev_log_index != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;
    use crate::raft::config::Membership;
    use crate::raft::log::LogEntry;

    #[test]
    fn commit_when_majority_replicates_current_term() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 1,
            command: Command::Noop,
        });
        log.append(LogEntry {
            term: 1,
            command: Command::Put {
                key: "a".into(),
                value: "1".into(),
            },
        });
        let mut match_index = HashMap::new();
        match_index.insert(2, 2);
        match_index.insert(3, 0);
        let cfg = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![]));
        let c = advance_commit_index(1, &log, &match_index, 1, 2, &cfg, 0);
        assert_eq!(c, 2);
    }

    #[test]
    fn no_commit_without_majority() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 1,
            command: Command::Noop,
        });
        let mut match_index = HashMap::new();
        match_index.insert(2, 0);
        match_index.insert(3, 0);
        let cfg = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![]));
        let c = advance_commit_index(1, &log, &match_index, 1, 1, &cfg, 0);
        assert_eq!(c, 0);
    }

    #[test]
    fn joint_commit_requires_overlap_quorum() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 1,
            command: Command::Noop,
        });
        let mut match_index = HashMap::new();
        // Old={1,2,3} New={3,4,5}; leader=1 has index 1; only 2 matches →
        // old majority {1,2} but new has only... none of 3,4,5 → no commit.
        match_index.insert(2, 1);
        let cfg = ClusterConfig::Joint {
            old: Membership::new(vec![1, 2, 3], vec![]),
            new: Membership::new(vec![3, 4, 5], vec![]),
        };
        let c = advance_commit_index(1, &log, &match_index, 1, 1, &cfg, 0);
        assert_eq!(c, 0);
        match_index.insert(3, 1);
        match_index.insert(4, 1);
        let c2 = advance_commit_index(1, &log, &match_index, 1, 1, &cfg, 0);
        assert_eq!(c2, 1);
    }
}

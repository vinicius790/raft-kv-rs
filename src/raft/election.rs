//! Election helpers: vote eligibility. Quorum math lives in `crate::quorum`.

use crate::raft::log::RaftLog;
use crate::rpc::{LogIndex, NodeId, Term};

/// Should we grant a vote to this candidate? (Raft §5.2 + §5.4.1)
pub fn should_grant_vote(
    current_term: Term,
    voted_for: Option<NodeId>,
    log: &RaftLog,
    candidate_term: Term,
    candidate_id: NodeId,
    last_log_index: LogIndex,
    last_log_term: Term,
) -> bool {
    if candidate_term < current_term {
        return false;
    }
    let can_vote = voted_for.is_none() || voted_for == Some(candidate_id);
    can_vote && log.is_other_up_to_date(last_log_index, last_log_term)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;
    use crate::raft::log::LogEntry;

    #[test]
    fn refuse_stale_term() {
        let log = RaftLog::new();
        assert!(!should_grant_vote(5, None, &log, 4, 1, 0, 0));
    }

    #[test]
    fn refuse_if_already_voted_elsewhere() {
        let log = RaftLog::new();
        assert!(!should_grant_vote(5, Some(2), &log, 5, 1, 0, 0));
    }

    #[test]
    fn refuse_outdated_log() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 3,
            command: Command::Noop,
        });
        assert!(!should_grant_vote(3, None, &log, 3, 1, 0, 0));
    }
}

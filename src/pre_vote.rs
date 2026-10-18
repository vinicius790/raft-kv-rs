//! Pre-Vote (§4.2.3 / Diego Ongaro thesis): probe before incrementing term.
//!
//! A partitioned node that times out asks for *pre-votes* without bumping
//! `currentTerm`. Only if a quorum of peers would grant a real vote does the
//! candidate start a real election. This prevents a zombie from disrupting a
//! healthy leader by forcing term bumps on every isolation timeout.

use std::collections::HashSet;

use crate::quorum::has_config_quorum;
use crate::raft::config::ClusterConfig;
use crate::raft::log::RaftLog;
use crate::rpc::{LogIndex, NodeId, Term};

#[derive(Clone, Debug, Default)]
pub struct PreVoteState {
    pub term: Term,
    pub granted: HashSet<NodeId>,
}

impl PreVoteState {
    pub fn new(term: Term, self_id: NodeId) -> Self {
        let mut granted = HashSet::new();
        granted.insert(self_id);
        Self { term, granted }
    }

    pub fn record(&mut self, from: NodeId, ok: bool) {
        if ok {
            self.granted.insert(from);
        }
    }

    pub fn ready(&self, cfg: &ClusterConfig) -> bool {
        has_config_quorum(cfg, &self.granted)
    }

    pub fn count(&self) -> usize {
        self.granted.len()
    }
}

/// Whether a peer should grant a *pre-vote* (does not mutate voted_for/term).
/// Pre-vote is granted if the candidate's log is up-to-date and the candidate's
/// proposed next term is at least our current term (we treat `candidate_term`
/// as the term they *would* run with = our term + 1 typically).
pub fn should_grant_pre_vote(
    current_term: Term,
    log: &RaftLog,
    candidate_next_term: Term,
    last_log_index: LogIndex,
    last_log_term: Term,
) -> bool {
    // Candidate plans to run in candidate_next_term; refuse if we already know
    // a higher term, or if their next term is stale relative to us.
    if candidate_next_term < current_term {
        return false;
    }
    // If we are already in a higher or equal term and have a leader recently,
    // the caller may still ask; log up-to-date is the main filter here.
    log.is_other_up_to_date(last_log_index, last_log_term)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::Command;
    use crate::raft::config::Membership;
    use crate::raft::log::LogEntry;

    #[test]
    fn refuse_stale_next_term() {
        let log = RaftLog::new();
        assert!(!should_grant_pre_vote(5, &log, 4, 0, 0));
    }

    #[test]
    fn grant_when_log_ok() {
        let mut log = RaftLog::new();
        log.append(LogEntry {
            term: 2,
            command: Command::Noop,
        });
        assert!(should_grant_pre_vote(2, &log, 3, 1, 2));
        assert!(!should_grant_pre_vote(2, &log, 3, 0, 1));
    }

    #[test]
    fn quorum_ready() {
        let cfg = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![]));
        let mut pv = PreVoteState::new(3, 1);
        assert!(!pv.ready(&cfg));
        pv.record(2, true);
        assert!(pv.ready(&cfg));
    }
}

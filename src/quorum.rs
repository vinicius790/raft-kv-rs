//! Quorum helpers for stable and joint (C_old,new) configurations.

use crate::raft::config::{ClusterConfig, Membership};
use crate::rpc::NodeId;
use std::collections::HashSet;

pub fn majority(n: usize) -> usize {
    n / 2 + 1
}

pub fn has_quorum(granted: usize, n: usize) -> bool {
    granted >= majority(n)
}

fn majority_of_set(voters: &[NodeId], acks: &HashSet<NodeId>) -> bool {
    if voters.is_empty() {
        return false;
    }
    let need = majority(voters.len());
    let have = voters.iter().filter(|id| acks.contains(id)).count();
    have >= need
}

/// True if `acks` forms a commit/election quorum under `cfg`.
pub fn has_config_quorum(cfg: &ClusterConfig, acks: &HashSet<NodeId>) -> bool {
    match cfg {
        ClusterConfig::Stable(m) => majority_of_set(&m.voters, acks),
        ClusterConfig::Joint { old, new } => {
            majority_of_set(&old.voters, acks) && majority_of_set(&new.voters, acks)
        }
    }
}

/// Vote/commit quorum for a single membership set.
pub fn membership_quorum(m: &Membership, acks: &HashSet<NodeId>) -> bool {
    majority_of_set(&m.voters, acks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::config::Membership;

    #[test]
    fn joint_needs_both_sides() {
        let old = Membership::new(vec![1, 2, 3], vec![]);
        let new = Membership::new(vec![3, 4, 5], vec![]);
        let cfg = ClusterConfig::Joint { old, new };
        // Overlap only {3} — not enough for either side alone as majority of 3.
        let mut acks = HashSet::new();
        acks.insert(3);
        assert!(!has_config_quorum(&cfg, &acks));
        // Full old majority without new.
        acks.insert(1);
        acks.insert(2);
        assert!(!has_config_quorum(&cfg, &acks));
        // Both sides.
        acks.insert(4);
        assert!(has_config_quorum(&cfg, &acks));
    }

    #[test]
    fn stable_majority() {
        let cfg = ClusterConfig::Stable(Membership::new(vec![1, 2, 3], vec![]));
        let mut acks = HashSet::new();
        acks.insert(1);
        assert!(!has_config_quorum(&cfg, &acks));
        acks.insert(2);
        assert!(has_config_quorum(&cfg, &acks));
    }
}

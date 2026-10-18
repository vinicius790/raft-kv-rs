//! Cluster membership: stable config or joint consensus (C_old,new).

use serde::{Deserialize, Serialize};

use crate::rpc::NodeId;

/// Voting set plus non-voting learners.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Membership {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

impl Membership {
    pub fn new(mut voters: Vec<NodeId>, mut learners: Vec<NodeId>) -> Self {
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        learners.retain(|id| !voters.contains(id));
        Self { voters, learners }
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.binary_search(&id).is_ok()
    }

    pub fn is_learner(&self, id: NodeId) -> bool {
        self.learners.binary_search(&id).is_ok()
    }
}

/// Latest cluster configuration known to a server (may be uncommitted).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClusterConfig {
    /// Empty voters means "not yet bootstrapped" on disk.
    Stable(Membership),
    /// Commit requires a majority of `old` AND a majority of `new`.
    Joint { old: Membership, new: Membership },
}

impl Default for ClusterConfig {
    fn default() -> Self {
        ClusterConfig::Stable(Membership::new(vec![], vec![]))
    }
}

impl ClusterConfig {
    pub fn bootstrap(self_id: NodeId, peers: &[NodeId], learners: &[NodeId]) -> Self {
        let mut voters = vec![self_id];
        voters.extend_from_slice(peers);
        ClusterConfig::Stable(Membership::new(voters, learners.to_vec()))
    }

    pub fn is_uninitialized(&self) -> bool {
        match self {
            ClusterConfig::Stable(m) => m.voters.is_empty(),
            ClusterConfig::Joint { .. } => false,
        }
    }

    pub fn is_joint(&self) -> bool {
        matches!(self, ClusterConfig::Joint { .. })
    }

    /// Distinct voting members (both sides during a joint config).
    pub fn voter_ids(&self) -> Vec<NodeId> {
        match self {
            ClusterConfig::Stable(m) => m.voters.clone(),
            ClusterConfig::Joint { old, new } => {
                let mut v = old.voters.clone();
                v.extend_from_slice(&new.voters);
                v.sort_unstable();
                v.dedup();
                v
            }
        }
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        match self {
            ClusterConfig::Stable(m) => m.is_voter(id),
            ClusterConfig::Joint { old, new } => old.is_voter(id) || new.is_voter(id),
        }
    }

    pub fn learner_ids(&self) -> Vec<NodeId> {
        match self {
            ClusterConfig::Stable(m) => m.learners.clone(),
            ClusterConfig::Joint { old, new } => {
                let voters = self.voter_ids();
                let mut l = old.learners.clone();
                l.extend_from_slice(&new.learners);
                l.retain(|id| !voters.contains(id));
                l.sort_unstable();
                l.dedup();
                l
            }
        }
    }

    /// Everyone who should receive the log (voters and learners).
    pub fn all_members(&self) -> Vec<NodeId> {
        let mut ids = self.voter_ids();
        ids.extend(self.learner_ids());
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn summary(&self) -> String {
        match self {
            ClusterConfig::Stable(m) => {
                format!("stable voters={:?} learners={:?}", m.voters, m.learners)
            }
            ClusterConfig::Joint { old, new } => format!(
                "joint old={:?} new={:?} learners_old={:?} learners_new={:?}",
                old.voters, new.voters, old.learners, new.learners
            ),
        }
    }
}

//! Persistent + volatile Raft state.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::config::ClusterConfig;
use super::log::RaftLog;
use crate::rpc::{LogIndex, NodeId, Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
    /// Non-voting replica: receives log/snapshots, never campaigns or counts in quorum.
    Learner,
}

impl Role {
    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Follower => "Follower",
            Role::Candidate => "Candidate",
            Role::Leader => "Leader",
            Role::Learner => "Learner",
        }
    }
}

/// Persistent state — must be durable before responding to RPCs that change it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: RaftLog,
    /// Latest known cluster configuration (may be ahead of commit).
    pub config: ClusterConfig,
}

/// Volatile state on all servers.
#[derive(Debug, Clone)]
pub struct VolatileState {
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub role: Role,
    pub leader_id: Option<NodeId>,
    /// Votes received in the current election (candidate only).
    pub votes_received: HashSet<NodeId>,
}

impl Default for VolatileState {
    fn default() -> Self {
        Self {
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            leader_id: None,
            votes_received: HashSet::new(),
        }
    }
}

/// One AppendEntries batch sent and not yet fully covered by `match_index`.
#[derive(Debug, Clone)]
pub struct InflightAppend {
    pub prev_index: LogIndex,
    pub last_index: LogIndex,
    pub bytes: u64,
}

/// Leader-only volatile state (rebuilt on each win).
#[derive(Debug, Clone)]
pub struct LeaderState {
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,
    /// Peers (plus self) that answered an AppendEntries carrying the current
    /// `read_round`. Acks from earlier rounds are not counted (read-index).
    pub heartbeat_acks: HashSet<NodeId>,
    /// Read-index round. Bumped by each linearizable read; sent as `read_ctx`.
    pub read_round: u64,
    /// Pipelined AppendEntries not yet covered by match_index.
    pub inflight: HashMap<NodeId, Vec<InflightAppend>>,
    /// Sum of `inflight` bytes per peer. Replication pauses at the cap.
    pub inflight_bytes: HashMap<NodeId, u64>,
    /// Next byte offset of an in-progress chunked InstallSnapshot.
    pub snap_offset: HashMap<NodeId, u64>,
    /// Peer has a snapshot chunk outstanding (acked offset not yet applied).
    pub snap_busy: HashSet<NodeId>,
    /// Peers (plus self) that acked since the last check-quorum window.
    /// Not cleared by read-index.
    pub contact: HashSet<NodeId>,
}

impl LeaderState {
    pub fn new(peers: &[NodeId], last_log_index: LogIndex) -> Self {
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for &p in peers {
            next_index.insert(p, last_log_index + 1);
            match_index.insert(p, 0);
        }
        Self {
            next_index,
            match_index,
            heartbeat_acks: HashSet::new(),
            read_round: 0,
            inflight: HashMap::new(),
            inflight_bytes: HashMap::new(),
            snap_offset: HashMap::new(),
            snap_busy: HashSet::new(),
            contact: HashSet::new(),
        }
    }

    pub fn ensure_peer(&mut self, peer: NodeId, last_log_index: LogIndex) {
        self.next_index.entry(peer).or_insert(last_log_index + 1);
        self.match_index.entry(peer).or_insert(0);
    }
}

/// Library-facing metrics (no invented throughput).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub id: NodeId,
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub last_log_index: LogIndex,
    pub role: String,
    /// Per-peer replication lag = leader_last_index - match_index (0 if unknown).
    pub replication_lag: HashMap<NodeId, u64>,
    pub snapshot_index: LogIndex,
    /// Entries whose commit index advanced on this process (not a cluster total).
    pub commits: u64,
    /// Times `start_election` ran (a lost pre-vote does not count).
    pub elections: u64,
    /// Snapshot payload bytes this process serialized, sent, or accepted.
    /// Retries of the same chunk count again. Not a throughput figure.
    pub snapshot_bytes: u64,
}

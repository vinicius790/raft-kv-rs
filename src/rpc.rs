//! RPC messages and length-prefixed framing helpers.

use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

use crate::kv::{ClientId, Serial};
use crate::raft::config::ClusterConfig;
use crate::raft::LogEntry;

/// Wire messages between Raft peers and clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
    },
    /// Pre-vote: does not increment term on the candidate until a quorum grants.
    PreVote {
        /// Term the candidate *would* use (typically current+1).
        next_term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    PreVoteResponse {
        next_term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        /// Leader's read-index round when this was sent. The follower echoes it.
        read_ctx: u64,
    },
    AppendEntriesResponse {
        term: Term,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
        /// True when follower needs InstallSnapshot (gap / compacted).
        need_snapshot: bool,
        /// Echo of the `read_ctx` of the AppendEntries being answered.
        read_ctx: u64,
    },
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        /// Byte offset of `data` within the snapshot image.
        offset: u64,
        /// Chunk bytes (not the whole image unless this is the only chunk).
        data: Vec<u8>,
        /// Configuration as of last_included_index (same on every chunk).
        config: ClusterConfig,
        /// Last chunk. Follower truncates the log only when this is true.
        done: bool,
        /// CRC-32/ISO-HDLC of the full snapshot image (same value on every chunk).
        checksum: u32,
    },
    InstallSnapshotResponse {
        term: Term,
        success: bool,
        last_included_index: LogIndex,
        /// Next byte the follower expects (length of the assembled buffer).
        next_offset: u64,
        /// True only when the follower has installed the full image.
        done: bool,
    },
    /// Leader asks a caught-up follower to start an election immediately.
    TimeoutNow {
        term: Term,
        leader_id: NodeId,
    },
    Client(ClientRequest),
    ClientReply(ClientResponse),
    StatusRequest,
    StatusResponse(NodeStatus),
    /// Ask for [`crate::raft::MetricsSnapshot`] rendered as text (no invented rates).
    MetricsRequest,
    /// Newest protocol events from the in-process ring (not a Prometheus scrape).
    TraceRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotBlob {
    pub kv: std::collections::HashMap<String, String>,
    pub sessions: std::collections::HashMap<ClientId, Serial>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    Put {
        key: String,
        value: String,
    },
    /// Linearizable read (read index). This is the default KV get.
    Get {
        key: String,
    },
    /// Applied-state read that does **not** wait for a read-index quorum.
    RawGet {
        key: String,
    },
    /// Linearizable read (read-index). Same path as [`ClientRequest::Get`].
    ReadIndex {
        key: String,
    },
    /// At-most-once put with client session.
    SessionPut {
        client_id: ClientId,
        serial: Serial,
        key: String,
        value: String,
    },
    /// Propose joint membership change to `new_voters` (+ optional learners).
    ChangeMembership {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
    /// Ask leader to transfer leadership to `target` (must be caught up).
    TransferLeadership {
        target: NodeId,
    },
    /// Promote a caught-up learner into the voter set (joint consensus).
    PromoteLearner {
        id: NodeId,
    },
    /// Delete a key (appended last: existing wire tags unchanged).
    Delete {
        key: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientResponse {
    pub ok: bool,
    pub value: Option<String>,
    pub leader_hint: Option<NodeId>,
    pub error: Option<String>,
}

impl ClientResponse {
    pub fn ok_put() -> Self {
        Self {
            ok: true,
            value: None,
            leader_hint: None,
            error: None,
        }
    }

    pub fn ok_get(value: Option<String>) -> Self {
        Self {
            ok: true,
            value,
            leader_hint: None,
            error: None,
        }
    }

    pub fn not_leader(hint: Option<NodeId>) -> Self {
        Self {
            ok: false,
            value: None,
            leader_hint: hint,
            error: Some("not leader".into()),
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            value: None,
            leader_hint: None,
            error: Some(msg.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub role: String,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub log_len: u64,
    pub kv_len: usize,
    pub peers: Vec<NodeId>,
    pub config: String,
    /// Entries newly committed on this process since start.
    pub commits: u64,
    /// Elections started on this process since start.
    pub elections: u64,
    /// Snapshot bytes serialized, sent, or accepted on this process.
    pub snapshot_bytes: u64,
}

/// Encode a message as `[u32 BE length][bincode payload]`.
pub fn encode(msg: &Message) -> Result<Vec<u8>, bincode::Error> {
    let payload = bincode::serialize(msg)?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode payload bytes (without the length prefix).
pub fn decode(payload: &[u8]) -> Result<Message, bincode::Error> {
    bincode::deserialize(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_request_vote() {
        let msg = Message::RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 5,
            last_log_term: 2,
        };
        let bytes = encode(&msg).unwrap();
        assert_eq!(
            u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize,
            bytes.len() - 4
        );
        let got = decode(&bytes[4..]).unwrap();
        match got {
            Message::RequestVote {
                term, candidate_id, ..
            } => {
                assert_eq!(term, 3);
                assert_eq!(candidate_id, 1);
            }
            _ => panic!("wrong variant"),
        }
    }
}

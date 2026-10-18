//! Raft consensus — leader election, log replication, linearizable KV,
//! joint membership, chunked snapshots, pre-vote, read index, leadership transfer.

pub mod client;
pub mod kv;
pub mod node;
pub mod observe;
pub mod pre_vote;
pub mod quorum;
pub mod raft;
pub mod rpc;
pub mod sim;
pub mod storage;
#[cfg(feature = "tls")]
pub mod tls;
pub mod transport;

pub use client::SessionClient;
pub use kv::{Command, KvStore};
pub use node::{NodeConfig, NodeHandle, RaftNode};
pub use raft::{ClusterConfig, LogEntry, Membership, MetricsSnapshot, Role};
pub use rpc::{ClientRequest, ClientResponse, Message, NodeId};

/// Default heartbeat interval (ms). Leaders send AppendEntries at least this often.
pub const DEFAULT_HEARTBEAT_MS: u64 = 80;
/// Election timeout lower bound (ms), inclusive.
pub const DEFAULT_ELECTION_MIN_MS: u64 = 300;
/// Election timeout upper bound (ms), inclusive.
pub const DEFAULT_ELECTION_MAX_MS: u64 = 600;

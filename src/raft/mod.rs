//! Raft core state machine (pure protocol logic + persistence hooks).

pub mod config;
pub mod election;
pub mod log;
pub mod replication;
pub mod state;

pub use config::{ClusterConfig, Membership};
pub use log::LogEntry;
pub use state::{LeaderState, MetricsSnapshot, PersistentState, Role, VolatileState};

use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, info};

use crate::kv::{Command, KvStore};
use crate::pre_vote::{should_grant_pre_vote, PreVoteState};
use crate::quorum::has_config_quorum;
use crate::rpc::{
    ClientRequest, ClientResponse, LogIndex, Message, NodeId, NodeStatus, SnapshotBlob, Term,
};
use crate::storage::Storage;
use election::should_grant_vote;
use replication::{advance_commit_index, log_matches};

/// Max entries per AppendEntries (batching / pipeline).
pub const MAX_APPEND_BATCH: usize = 64;

#[derive(Debug, Clone)]
pub struct TimingConfig {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: crate::DEFAULT_HEARTBEAT_MS,
            election_min_ms: crate::DEFAULT_ELECTION_MIN_MS,
            election_max_ms: crate::DEFAULT_ELECTION_MAX_MS,
        }
    }
}

#[derive(Debug)]
pub enum Outbound {
    Send {
        to: NodeId,
        msg: Message,
    },
    ClientReply {
        corr: u64,
        resp: ClientResponse,
    },
    ClientWaitCommit {
        corr: u64,
        wait_index: LogIndex,
    },
    /// Read-index: wait until a quorum of heartbeats in this term, then reply get.
    ClientWaitRead {
        corr: u64,
        key: String,
        read_term: Term,
    },
    ResetElectionTimer,
    /// Force immediate election timeout (leadership transfer target).
    ForceElectionTimeout,
    ResetHeartbeatTimer,
}

pub struct RaftCore {
    pub id: NodeId,
    pub persistent: PersistentState,
    pub volatile: VolatileState,
    pub leader: Option<LeaderState>,
    pub kv: KvStore,
    pub timing: TimingConfig,
    storage: Storage,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
    /// Active pre-vote round (before bumping term).
    pub pre_vote: Option<PreVoteState>,
    /// Pending read-index requests awaiting quorum heartbeat.
    pub pending_reads: Vec<(u64, String, Term)>,
    /// Disable pre-vote (tests that assert classic disruption can set false).
    pub enable_pre_vote: bool,
    /// Max AppendEntries batch size.
    pub max_batch: usize,
    /// Peer address overrides (config reload without restart).
    pub peer_addrs: HashMap<NodeId, String>,
    /// True while we have accepted a heartbeat/AE from the current leader.
    /// Cleared only on election timeout so a partitioned node cannot get pre-votes
    /// from a healthy quorum.
    pub heard_from_leader: bool,
    /// How many AppendEntries batches may be outstanding to one peer.
    pub pipeline_window: usize,
    /// Pause replication to a peer once unacked AppendEntries bytes reach this.
    pub max_inflight_bytes: u64,
    /// InstallSnapshot chunk size in bytes. The follower installs only on `done`.
    pub snapshot_chunk_size: usize,
    /// Cached snapshot image so chunks of one install share identical bytes.
    snap_cache: Option<SnapImage>,
    /// Follower-side assembly of a chunked snapshot. Log is untouched until done.
    snap_in: Option<IncomingSnap>,
    /// Volatile counters. Reset on process start; not written to disk.
    pub commits: u64,
    pub elections: u64,
    pub snapshot_bytes: u64,
    /// Leader steps down if a check-quorum window closes without a config quorum of acks.
    pub enable_check_quorum: bool,
    /// Compact when `commit_index - snapshot_index` reaches this. 0 disables.
    pub snapshot_threshold: u64,
    /// Protocol events (term, election, commit, snapshot). Not persisted.
    pub trace: crate::observe::TraceRing,
}

struct SnapImage {
    index: LogIndex,
    term: Term,
    data: Vec<u8>,
    config: ClusterConfig,
}

struct IncomingSnap {
    index: LogIndex,
    term: Term,
    buf: Vec<u8>,
    config: ClusterConfig,
    /// CRC-32 of the full image, copied from every chunk.
    checksum: u32,
}

impl RaftCore {
    pub async fn open(
        id: NodeId,
        peers: Vec<NodeId>,
        storage: Storage,
        timing: TimingConfig,
    ) -> anyhow::Result<Self> {
        Self::open_with_learners(id, peers, vec![], storage, timing).await
    }

    pub async fn open_with_learners(
        id: NodeId,
        peers: Vec<NodeId>,
        learners: Vec<NodeId>,
        storage: Storage,
        timing: TimingConfig,
    ) -> anyhow::Result<Self> {
        let mut persistent = storage.persistent();
        if persistent.config.is_uninitialized() {
            persistent.config = ClusterConfig::bootstrap(id, &peers, &learners);
        }
        let commit_index = storage.commit_index();
        let (snap_idx, snap_term, snap_kv, snap_sess) = storage.snapshot_meta();
        let mut kv = KvStore::new();
        if !snap_kv.is_empty() || !snap_sess.is_empty() {
            kv.restore_full(snap_kv, snap_sess);
        }
        // Align in-memory snapshot markers with durable log.
        if persistent.log.snapshot_index == 0 && snap_idx > 0 {
            persistent.log.snapshot_index = snap_idx;
            persistent.log.snapshot_term = snap_term;
        }
        let upto = commit_index.min(persistent.log.last_index());
        for i in (snap_idx + 1)..=upto {
            if let Some(e) = persistent.log.entry(i) {
                kv.apply(&e.command);
                if let Command::ChangeConfig(cfg) = &e.command {
                    persistent.config = cfg.clone();
                }
            }
        }
        // Raft uses the latest configuration stored in the log, committed or
        // not. A crash between C_old,new and C_new must not rewind the server
        // to the last committed config and forget the entry already on disk.
        let last = persistent.log.last_index();
        for i in (upto + 1)..=last {
            if let Some(e) = persistent.log.entry(i) {
                if let Command::ChangeConfig(cfg) = &e.command {
                    persistent.config = cfg.clone();
                }
            }
        }
        let role = if persistent.config.is_voter(id) {
            Role::Follower
        } else {
            Role::Learner
        };
        let volatile = VolatileState {
            commit_index: upto,
            last_applied: upto,
            role,
            ..VolatileState::default()
        };

        Ok(Self {
            id,
            persistent,
            volatile,
            leader: None,
            kv,
            timing,
            storage,
            snapshot_index: snap_idx,
            snapshot_term: snap_term,
            pre_vote: None,
            pending_reads: Vec::new(),
            enable_pre_vote: true,
            max_batch: MAX_APPEND_BATCH,
            peer_addrs: HashMap::new(),
            heard_from_leader: false,
            pipeline_window: 8,
            max_inflight_bytes: 1 << 20,
            snapshot_chunk_size: 1 << 20,
            snap_cache: None,
            snap_in: None,
            commits: 0,
            elections: 0,
            snapshot_bytes: 0,
            enable_check_quorum: true,
            snapshot_threshold: 0,
            trace: crate::observe::TraceRing::default(),
        })
    }

    pub fn cluster_size(&self) -> usize {
        self.persistent.config.voter_ids().len().max(1)
    }

    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.persistent
            .config
            .all_members()
            .into_iter()
            .filter(|&p| p != self.id)
            .collect()
    }

    pub fn random_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(self.timing.election_min_ms..=self.timing.election_max_ms);
        Duration::from_millis(ms)
    }

    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.timing.heartbeat_ms)
    }

    pub fn status(&self) -> NodeStatus {
        NodeStatus {
            id: self.id,
            role: self.volatile.role.as_str().to_string(),
            term: self.persistent.current_term,
            leader: self.volatile.leader_id,
            commit_index: self.volatile.commit_index,
            last_applied: self.volatile.last_applied,
            log_len: self.persistent.log.last_index(),
            kv_len: self.kv.len(),
            peers: self.peer_ids(),
            config: self.persistent.config.summary(),
            commits: self.commits,
            elections: self.elections,
            snapshot_bytes: self.snapshot_bytes,
        }
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        let last = self.persistent.log.last_index();
        let mut lag = HashMap::new();
        if let Some(leader) = self.leader.as_ref() {
            for (&peer, &m) in &leader.match_index {
                lag.insert(peer, last.saturating_sub(m));
            }
        }
        MetricsSnapshot {
            id: self.id,
            term: self.persistent.current_term,
            commit_index: self.volatile.commit_index,
            last_applied: self.volatile.last_applied,
            last_log_index: last,
            role: self.volatile.role.as_str().to_string(),
            replication_lag: lag,
            snapshot_index: self.persistent.log.snapshot_index,
            commits: self.commits,
            elections: self.elections,
            snapshot_bytes: self.snapshot_bytes,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.volatile.role == Role::Leader
    }

    pub fn commit_index(&self) -> LogIndex {
        self.volatile.commit_index
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.persistent.log.last_index()
    }

    /// Reload peer address map without restart (transport layer consumes this).
    pub fn reload_peer_addrs(&mut self, addrs: HashMap<NodeId, String>) {
        self.peer_addrs = addrs;
    }

    async fn persist(&mut self) -> anyhow::Result<()> {
        self.storage
            .save(&self.persistent, self.volatile.commit_index)
            .await?;
        Ok(())
    }

    /// Persist vote/term first then log (for crash-between-writes tests).
    pub async fn persist_vote_then_log(&mut self) -> anyhow::Result<()> {
        self.storage
            .save_vote_then_log(&self.persistent, self.volatile.commit_index)
            .await?;
        Ok(())
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        let bumped = term > self.persistent.current_term;
        if bumped {
            self.persistent.current_term = term;
            self.persistent.voted_for = None;
        }
        if self.persistent.config.is_voter(self.id) {
            self.volatile.role = Role::Follower;
        } else {
            self.volatile.role = Role::Learner;
        }
        self.volatile.leader_id = leader;
        self.leader = None;
        self.volatile.votes_received.clear();
        self.pre_vote = None;
        if bumped {
            self.trace.push("term", self.persistent.current_term, 0);
        }
    }

    pub fn check_quorum_timeout(&self) -> Duration {
        // Four times the election upper bound: same order of magnitude as an
        // election, with room for a stalled executor. Not a measured latency.
        let ms = self
            .timing
            .election_max_ms
            .saturating_mul(4)
            .max(self.timing.election_max_ms);
        Duration::from_millis(ms.max(1))
    }

    pub async fn on_election_timeout(&mut self) -> anyhow::Result<Vec<Outbound>> {
        let mut out = self.election_timeout_inner().await?;
        out.extend(self.abort_reads_if_deposed());
        Ok(out)
    }

    async fn election_timeout_inner(&mut self) -> anyhow::Result<Vec<Outbound>> {
        if self.volatile.role == Role::Leader {
            return self.on_check_quorum().await;
        }
        // Losing contact with the leader is what authorizes pre-vote.
        self.heard_from_leader = false;
        if self.volatile.role == Role::Learner || !self.persistent.config.is_voter(self.id) {
            // Learners never campaign.
            return Ok(vec![Outbound::ResetElectionTimer]);
        }
        // Single-node: never lose leadership via timeout (already leader handled above).
        if self.cluster_size() == 1 && self.volatile.role == Role::Follower {
            // Become leader via election.
        }
        if self.enable_pre_vote {
            self.start_pre_vote().await
        } else {
            self.start_election().await
        }
    }

    async fn start_pre_vote(&mut self) -> anyhow::Result<Vec<Outbound>> {
        let next_term = self.persistent.current_term + 1;
        self.pre_vote = Some(PreVoteState::new(next_term, self.id));
        let last_log_index = self.persistent.log.last_index();
        let last_log_term = self.persistent.log.last_term();
        info!(node = self.id, next_term, "starting pre-vote");
        let mut out = vec![Outbound::ResetElectionTimer];
        if self.cluster_size() == 1 {
            // Lone voter: pre-vote self-grants → real election.
            return self.start_election().await;
        }
        for peer in self.peer_ids() {
            if !self.persistent.config.is_voter(peer) {
                continue;
            }
            out.push(Outbound::Send {
                to: peer,
                msg: Message::PreVote {
                    next_term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                },
            });
        }
        Ok(out)
    }

    async fn on_check_quorum(&mut self) -> anyhow::Result<Vec<Outbound>> {
        if !self.enable_check_quorum {
            return Ok(vec![]);
        }
        let ok = self
            .leader
            .as_ref()
            .is_some_and(|l| has_config_quorum(&self.persistent.config, &l.contact));
        if ok {
            if let Some(leader) = self.leader.as_mut() {
                leader.contact.clear();
                leader.contact.insert(self.id);
            }
            return Ok(vec![Outbound::ResetElectionTimer]);
        }
        info!(
            node = self.id,
            term = self.persistent.current_term,
            "check quorum failed; stepping down"
        );
        self.become_follower(self.persistent.current_term, None);
        self.heard_from_leader = false;
        Ok(vec![Outbound::ResetElectionTimer])
    }

    async fn start_election(&mut self) -> anyhow::Result<Vec<Outbound>> {
        self.pre_vote = None;
        self.elections = self.elections.saturating_add(1);
        self.persistent.current_term += 1;
        self.trace.push("term", self.persistent.current_term, 0);
        self.persistent.voted_for = Some(self.id);
        self.volatile.role = Role::Candidate;
        self.volatile.leader_id = None;
        self.leader = None;
        self.volatile.votes_received.clear();
        self.volatile.votes_received.insert(self.id);
        self.persist().await?;
        self.trace.push(
            "election",
            self.persistent.current_term,
            self.persistent.log.last_index(),
        );

        let term = self.persistent.current_term;
        let last_log_index = self.persistent.log.last_index();
        let last_log_term = self.persistent.log.last_term();
        info!(node = self.id, term, "starting election");

        let mut out = vec![Outbound::ResetElectionTimer];
        for peer in self.peer_ids() {
            if !self.persistent.config.is_voter(peer) {
                continue;
            }
            out.push(Outbound::Send {
                to: peer,
                msg: Message::RequestVote {
                    term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                },
            });
        }

        if has_config_quorum(&self.persistent.config, &self.volatile.votes_received) {
            out.extend(self.become_leader().await?);
        }
        Ok(out)
    }

    async fn become_leader(&mut self) -> anyhow::Result<Vec<Outbound>> {
        info!(
            node = self.id,
            term = self.persistent.current_term,
            "became leader"
        );
        self.volatile.role = Role::Leader;
        self.volatile.leader_id = Some(self.id);
        let peers = self.peer_ids();
        self.leader = Some(LeaderState::new(&peers, self.persistent.log.last_index()));
        if let Some(leader) = self.leader.as_mut() {
            leader.contact.insert(self.id);
        }
        self.persistent.log.append(LogEntry {
            term: self.persistent.current_term,
            command: Command::Noop,
        });
        self.persist().await?;
        let mut out = self.broadcast_append_entries();
        out.extend(self.try_commit());
        self.persist().await?;
        out.push(Outbound::ResetHeartbeatTimer);
        // Arm check-quorum. Later heartbeats must not push this timer; only acks do.
        out.push(Outbound::ResetElectionTimer);
        Ok(out)
    }

    pub async fn on_heartbeat_timeout(&mut self) -> anyhow::Result<Vec<Outbound>> {
        let mut out = self.heartbeat_timeout_inner().await?;
        out.extend(self.abort_reads_if_deposed());
        Ok(out)
    }

    async fn heartbeat_timeout_inner(&mut self) -> anyhow::Result<Vec<Outbound>> {
        if self.volatile.role != Role::Leader {
            return Ok(vec![]);
        }
        let mut out = self.broadcast_append_entries();
        out.push(Outbound::ResetHeartbeatTimer);
        Ok(out)
    }

    fn broadcast_append_entries(&mut self) -> Vec<Outbound> {
        let peers = self.peer_ids();
        let mut out = Vec::new();
        for peer in peers {
            out.extend(self.replicate_peer(peer, true));
        }
        out
    }

    /// Send the next pipelined AppendEntries batches, or the next snapshot chunk.
    /// `match_index` is not touched here — only an ack moves it.
    fn replicate_peer(&mut self, peer: NodeId, allow_empty_heartbeat: bool) -> Vec<Outbound> {
        if self.volatile.role != Role::Leader || self.leader.is_none() {
            return vec![];
        }
        let snap_idx = self.persistent.log.snapshot_index;
        let next = self
            .leader
            .as_ref()
            .unwrap()
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(1);
        if snap_idx > 0 && next <= snap_idx {
            return self.send_snapshot_chunk(peer);
        }

        let cap = self.max_inflight_bytes.max(1);
        let window = self.pipeline_window.max(1);
        let (already_bytes, already_n) = {
            let leader = self.leader.as_ref().unwrap();
            (
                leader.inflight_bytes.get(&peer).copied().unwrap_or(0),
                leader.inflight.get(&peer).map(|v| v.len()).unwrap_or(0),
            )
        };
        if already_n > 0 && already_bytes >= cap {
            return self.flow_paused_heartbeat(peer, allow_empty_heartbeat);
        }

        let mut cursor = next;
        let last = self.persistent.log.last_index();
        let mut planned: Vec<(LogIndex, u64)> = Vec::new();
        let mut bytes_acc = 0u64;
        while planned.len() < window && cursor <= last {
            let entries = self
                .persistent
                .log
                .entries_range(cursor, self.max_batch.max(1));
            if entries.is_empty() {
                break;
            }
            let msg_bytes = bincode::serialized_size(&entries).unwrap_or(0);
            if !planned.is_empty() && bytes_acc.saturating_add(msg_bytes) > cap {
                break;
            }
            let n = entries.len() as LogIndex;
            planned.push((cursor, msg_bytes));
            bytes_acc = bytes_acc.saturating_add(msg_bytes);
            cursor += n;
            if bytes_acc >= cap {
                break;
            }
        }

        if planned.is_empty() {
            return self.flow_paused_heartbeat(peer, allow_empty_heartbeat);
        }

        let mut inflight = Vec::with_capacity(planned.len());
        let mut out = Vec::with_capacity(planned.len());
        for (start, msg_bytes) in &planned {
            let msg = self.make_append_entries(*start);
            let n = match &msg {
                Message::AppendEntries { entries, .. } => entries.len() as LogIndex,
                _ => 0,
            };
            if n == 0 {
                continue;
            }
            inflight.push(state::InflightAppend {
                prev_index: start.saturating_sub(1),
                last_index: start + n - 1,
                bytes: *msg_bytes,
            });
            out.push(Outbound::Send { to: peer, msg });
        }
        if let Some(leader) = self.leader.as_mut() {
            leader.inflight.insert(peer, inflight);
            leader.inflight_bytes.insert(peer, bytes_acc);
        }
        if out.is_empty() {
            return self.flow_paused_heartbeat(peer, allow_empty_heartbeat);
        }
        out
    }

    /// Heartbeat that does not carry entries and does not add in-flight bytes.
    fn flow_paused_heartbeat(&self, peer: NodeId, allow: bool) -> Vec<Outbound> {
        if !allow {
            return vec![];
        }
        let matched = self
            .leader
            .as_ref()
            .and_then(|l| l.match_index.get(&peer).copied())
            .unwrap_or(0);
        let prev_term = self.persistent.log.term_at(matched).unwrap_or(0);
        vec![Outbound::Send {
            to: peer,
            msg: Message::AppendEntries {
                term: self.persistent.current_term,
                leader_id: self.id,
                prev_log_index: matched,
                prev_log_term: prev_term,
                entries: Vec::new(),
                leader_commit: self.volatile.commit_index,
                read_ctx: self.read_ctx(),
            },
        }]
    }

    fn note_append_ack(&mut self, peer: NodeId, match_index: LogIndex) {
        let self_id = self.id;
        let Some(leader) = self.leader.as_mut() else {
            return;
        };
        let prev = leader.match_index.get(&peer).copied().unwrap_or(0);
        if match_index > prev {
            leader.match_index.insert(peer, match_index);
        }
        let effective = leader.match_index.get(&peer).copied().unwrap_or(0);
        if let Some(q) = leader.inflight.get_mut(&peer) {
            q.retain(|m| m.last_index > effective);
        }
        let bytes = leader
            .inflight
            .get(&peer)
            .map(|q| q.iter().map(|m| m.bytes).sum())
            .unwrap_or(0);
        leader.inflight_bytes.insert(peer, bytes);
        let ni = leader.next_index.get(&peer).copied().unwrap_or(1);
        // Never rewind: a late ack for an earlier batch must not pull next_index back.
        if effective + 1 > ni {
            leader.next_index.insert(peer, effective + 1);
        }
        leader.contact.insert(peer);
        leader.contact.insert(self_id);
    }

    fn clear_peer_pipeline(&mut self, peer: NodeId) {
        if let Some(leader) = self.leader.as_mut() {
            leader.inflight.remove(&peer);
            leader.inflight_bytes.insert(peer, 0);
        }
    }

    fn snapshot_image(&mut self) -> SnapImage {
        let index = self.persistent.log.snapshot_index;
        let term = self.persistent.log.snapshot_term;
        if let Some(c) = &self.snap_cache {
            if c.index == index && c.term == term {
                return SnapImage {
                    index: c.index,
                    term: c.term,
                    data: c.data.clone(),
                    config: c.config.clone(),
                };
            }
        }
        let blob = SnapshotBlob {
            kv: self.kv.snapshot(),
            sessions: self.kv.sessions_snapshot(),
        };
        let data = bincode::serialize(&blob).unwrap_or_default();
        let config = self.persistent.config.clone();
        self.snap_cache = Some(SnapImage {
            index,
            term,
            data: data.clone(),
            config: config.clone(),
        });
        SnapImage {
            index,
            term,
            data,
            config,
        }
    }

    fn send_snapshot_chunk(&mut self, peer: NodeId) -> Vec<Outbound> {
        let image = self.snapshot_image();
        let offset = self
            .leader
            .as_ref()
            .and_then(|l| l.snap_offset.get(&peer).copied())
            .unwrap_or(0);
        let chunk = self.snapshot_chunk_size.max(1) as u64;
        let total = image.data.len() as u64;
        let offset = offset.min(total);
        let end = (offset + chunk).min(total);
        let slice = image.data[offset as usize..end as usize].to_vec();
        let done = end == total;
        let checksum = crate::storage::crc32(&image.data);
        self.snapshot_bytes = self.snapshot_bytes.saturating_add(slice.len() as u64);
        if let Some(leader) = self.leader.as_mut() {
            leader.snap_busy.insert(peer);
            leader.snap_offset.insert(peer, offset);
        }
        vec![Outbound::Send {
            to: peer,
            msg: Message::InstallSnapshot {
                term: self.persistent.current_term,
                leader_id: self.id,
                last_included_index: image.index,
                last_included_term: image.term,
                offset,
                data: slice,
                config: image.config,
                done,
                checksum,
            },
        }]
    }

    fn make_append_entries(&self, next_index: LogIndex) -> Message {
        let prev = next_index.saturating_sub(1);
        let prev_term = self.persistent.log.term_at(prev).unwrap_or(0);
        let entries = self
            .persistent
            .log
            .entries_range(next_index, self.max_batch);
        Message::AppendEntries {
            term: self.persistent.current_term,
            leader_id: self.id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.volatile.commit_index,
            read_ctx: self.read_ctx(),
        }
    }

    /// Current read-index round, sent in every AppendEntries (0 when not leader).
    fn read_ctx(&self) -> u64 {
        self.leader.as_ref().map_or(0, |l| l.read_round)
    }

    pub async fn handle_message(
        &mut self,
        from: Option<NodeId>,
        msg: Message,
        corr: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        let mut out = self.handle_message_inner(from, msg, corr).await?;
        out.extend(self.abort_reads_if_deposed());
        Ok(out)
    }

    /// A read parked on the read-index quorum can only be served by the
    /// leader of the term it arrived in. Once this node is no longer that
    /// leader, no ack will ever confirm it: answer `not leader` now instead of
    /// leaving the client waiting forever.
    fn abort_reads_if_deposed(&mut self) -> Vec<Outbound> {
        if self.pending_reads.is_empty() {
            return vec![];
        }
        let term = self.persistent.current_term;
        let leading = self.volatile.role == Role::Leader;
        let hint = self.volatile.leader_id.filter(|&l| l != self.id);
        let (stale, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending_reads)
            .into_iter()
            .partition(|(_, _, read_term)| !leading || *read_term != term);
        self.pending_reads = keep;
        stale
            .into_iter()
            .map(|(corr, _, _)| Outbound::ClientReply {
                corr,
                resp: ClientResponse::not_leader(hint),
            })
            .collect()
    }

    async fn handle_message_inner(
        &mut self,
        from: Option<NodeId>,
        msg: Message,
        corr: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(from, term, candidate_id, last_log_index, last_log_term)
                    .await
            }
            Message::RequestVoteResponse { term, vote_granted } => {
                self.handle_vote_response(from, term, vote_granted).await
            }
            Message::PreVote {
                next_term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_pre_vote(from, next_term, candidate_id, last_log_index, last_log_term)
                    .await
            }
            Message::PreVoteResponse {
                next_term,
                vote_granted,
            } => {
                self.handle_pre_vote_response(from, next_term, vote_granted)
                    .await
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            } => {
                self.handle_append_entries(
                    from,
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_ctx,
                )
                .await
            }
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_index,
                need_snapshot,
                read_ctx,
            } => {
                self.handle_ae_response(
                    from,
                    term,
                    success,
                    match_index,
                    conflict_index,
                    need_snapshot,
                    read_ctx,
                )
                .await
            }
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                data,
                config,
                done,
                checksum,
            } => {
                self.handle_install_snapshot(
                    from,
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    offset,
                    data,
                    config,
                    done,
                    checksum,
                )
                .await
            }
            Message::InstallSnapshotResponse {
                term,
                success,
                last_included_index,
                next_offset,
                done,
            } => {
                self.handle_install_snapshot_response(
                    from,
                    term,
                    success,
                    last_included_index,
                    next_offset,
                    done,
                )
                .await
            }
            Message::TimeoutNow { term, leader_id } => {
                self.handle_timeout_now(term, leader_id).await
            }
            Message::Client(req) => self.handle_client(req, corr).await,
            Message::StatusRequest => Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse {
                    ok: true,
                    value: Some(format_status(&self.status())),
                    leader_hint: self.volatile.leader_id,
                    error: None,
                },
            }]),
            Message::MetricsRequest => Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse {
                    ok: true,
                    value: Some(format_metrics(&self.metrics())),
                    leader_hint: self.volatile.leader_id,
                    error: None,
                },
            }]),
            Message::TraceRequest => Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse {
                    ok: true,
                    value: Some(self.trace.render()),
                    leader_hint: self.volatile.leader_id,
                    error: None,
                },
            }]),
            Message::ClientReply(_) | Message::StatusResponse(_) => Ok(vec![]),
        }
    }

    async fn handle_pre_vote(
        &mut self,
        from: Option<NodeId>,
        next_term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> anyhow::Result<Vec<Outbound>> {
        let mut grant = should_grant_pre_vote(
            self.persistent.current_term,
            &self.persistent.log,
            next_term,
            last_log_index,
            last_log_term,
        );
        // Healthy leader or a follower that still hears heartbeats refuses,
        // so an isolated node cannot force a term bump.
        if self.volatile.role == Role::Leader || self.heard_from_leader {
            grant = false;
        }
        // Do NOT bump term or voted_for on pre-vote.
        let to = from.unwrap_or(candidate_id);
        Ok(vec![Outbound::Send {
            to,
            msg: Message::PreVoteResponse {
                next_term,
                vote_granted: grant,
            },
        }])
    }

    async fn handle_pre_vote_response(
        &mut self,
        from: Option<NodeId>,
        next_term: Term,
        vote_granted: bool,
    ) -> anyhow::Result<Vec<Outbound>> {
        let Some(pv) = self.pre_vote.as_mut() else {
            return Ok(vec![]);
        };
        if pv.term != next_term {
            return Ok(vec![]);
        }
        if let Some(id) = from {
            pv.record(id, vote_granted);
        }
        if pv.ready(&self.persistent.config) {
            return self.start_election().await;
        }
        Ok(vec![])
    }

    async fn handle_request_vote(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> anyhow::Result<Vec<Outbound>> {
        let mut out = Vec::new();
        // Learners never grant votes (non-voting).
        if !self.persistent.config.is_voter(self.id) {
            let to = from.unwrap_or(candidate_id);
            out.push(Outbound::Send {
                to,
                msg: Message::RequestVoteResponse {
                    term: self.persistent.current_term.max(term),
                    vote_granted: false,
                },
            });
            return Ok(out);
        }

        if term > self.persistent.current_term {
            self.become_follower(term, None);
            self.persist().await?;
            out.push(Outbound::ResetElectionTimer);
        }

        let grant = should_grant_vote(
            self.persistent.current_term,
            self.persistent.voted_for,
            &self.persistent.log,
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        );

        if grant {
            self.persistent.voted_for = Some(candidate_id);
            if self.persistent.config.is_voter(self.id) {
                self.volatile.role = Role::Follower;
            }
            self.persist().await?;
            out.push(Outbound::ResetElectionTimer);
            debug!(node = self.id, candidate_id, term, "granted vote");
        }

        let to = from.unwrap_or(candidate_id);
        out.push(Outbound::Send {
            to,
            msg: Message::RequestVoteResponse {
                term: self.persistent.current_term,
                vote_granted: grant,
            },
        });
        Ok(out)
    }

    async fn handle_vote_response(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        vote_granted: bool,
    ) -> anyhow::Result<Vec<Outbound>> {
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            self.persist().await?;
            return Ok(vec![Outbound::ResetElectionTimer]);
        }
        if self.volatile.role != Role::Candidate || term != self.persistent.current_term {
            return Ok(vec![]);
        }
        if vote_granted {
            if let Some(id) = from {
                self.volatile.votes_received.insert(id);
            }
        }
        if has_config_quorum(&self.persistent.config, &self.volatile.votes_received) {
            return self.become_leader().await;
        }
        Ok(vec![])
    }

    // Field list mirrors AppendEntries. A struct would only rename the same arguments.
    #[allow(clippy::too_many_arguments)]
    async fn handle_append_entries(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        read_ctx: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        let mut out = Vec::new();
        let reply_to = from.unwrap_or(leader_id);

        if term < self.persistent.current_term {
            out.push(Outbound::Send {
                to: reply_to,
                msg: Message::AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: self.persistent.log.last_index(),
                    need_snapshot: false,
                    read_ctx,
                },
            });
            return Ok(out);
        }

        self.become_follower(term, Some(leader_id));
        self.heard_from_leader = true;
        self.persist().await?;
        out.push(Outbound::ResetElectionTimer);

        // Gap: prev beyond our last+0 or below compacted prefix without match.
        if prev_log_index > self.persistent.log.last_index() {
            out.push(Outbound::Send {
                to: reply_to,
                msg: Message::AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: self.persistent.log.last_index(),
                    need_snapshot: prev_log_index > self.persistent.log.last_index() + 1
                        || prev_log_index < self.persistent.log.snapshot_index,
                    read_ctx,
                },
            });
            return Ok(out);
        }

        if prev_log_index < self.persistent.log.snapshot_index {
            out.push(Outbound::Send {
                to: reply_to,
                msg: Message::AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: self.persistent.log.snapshot_index,
                    need_snapshot: true,
                    read_ctx,
                },
            });
            return Ok(out);
        }

        if !log_matches(&self.persistent.log, prev_log_index, prev_log_term) {
            let conflict = if prev_log_index > self.persistent.log.last_index() {
                self.persistent.log.last_index()
            } else {
                prev_log_index.saturating_sub(1)
            };
            out.push(Outbound::Send {
                to: reply_to,
                msg: Message::AppendEntriesResponse {
                    term: self.persistent.current_term,
                    success: false,
                    match_index: 0,
                    conflict_index: conflict,
                    need_snapshot: false,
                    read_ctx,
                },
            });
            return Ok(out);
        }

        if !entries.is_empty() {
            self.persistent.log.append_entries(prev_log_index, &entries);
            // Pick up config from any ChangeConfig in new entries.
            for e in &entries {
                if let Command::ChangeConfig(cfg) = &e.command {
                    self.persistent.config = cfg.clone();
                    self.refresh_role_from_config();
                }
            }
            self.persist().await?;
        }

        let last_new = if entries.is_empty() {
            prev_log_index
        } else {
            prev_log_index + entries.len() as LogIndex
        };

        if leader_commit > self.volatile.commit_index {
            let mut next = leader_commit.min(last_new);
            if next > self.persistent.log.last_index() {
                next = self.persistent.log.last_index();
            }
            if next > self.volatile.commit_index {
                self.commits = self
                    .commits
                    .saturating_add(next - self.volatile.commit_index);
                self.volatile.commit_index = next;
                self.trace
                    .push("commit", self.persistent.current_term, next);
            }
            self.apply_committed();
            self.persist().await?;
        }

        out.push(Outbound::Send {
            to: reply_to,
            msg: Message::AppendEntriesResponse {
                term: self.persistent.current_term,
                success: true,
                match_index: last_new,
                conflict_index: 0,
                need_snapshot: false,
                read_ctx,
            },
        });
        Ok(out)
    }

    // Field list mirrors AppendEntriesResponse.
    #[allow(clippy::too_many_arguments)]
    async fn handle_ae_response(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
        need_snapshot: bool,
        read_ctx: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            self.persist().await?;
            return Ok(vec![Outbound::ResetElectionTimer]);
        }
        if self.volatile.role != Role::Leader || term != self.persistent.current_term {
            return Ok(vec![]);
        }
        let Some(peer) = from else {
            return Ok(vec![]);
        };
        // Any same-term answer proves `peer` still follows us, but only an
        // answer to the current read round may confirm a pending read.
        if let Some(leader) = self.leader.as_mut() {
            if read_ctx == leader.read_round {
                leader.heartbeat_acks.insert(peer);
            }
        }

        if need_snapshot {
            self.clear_peer_pipeline(peer);
            if let Some(leader) = self.leader.as_mut() {
                leader
                    .next_index
                    .insert(peer, self.persistent.log.snapshot_index);
                leader.snap_offset.insert(peer, 0);
                leader.snap_busy.remove(&peer);
            }
            return Ok(self.send_snapshot_chunk(peer));
        }

        if success {
            // match_index moves only because of this ack, never because we sent.
            self.note_append_ack(peer, match_index);
        } else if self.leader.is_some() {
            self.clear_peer_pipeline(peer);
            let next = self
                .leader
                .as_ref()
                .and_then(|l| l.next_index.get(&peer).copied())
                .unwrap_or(1);
            let new_next = if conflict_index > 0 {
                conflict_index.min(next.saturating_sub(1)).max(1)
            } else {
                next.saturating_sub(1).max(1)
            };
            if let Some(leader) = self.leader.as_mut() {
                leader.next_index.insert(peer, new_next);
            }
            let snap_idx = self.persistent.log.snapshot_index;
            if new_next <= snap_idx && snap_idx > 0 {
                if let Some(leader) = self.leader.as_mut() {
                    leader.snap_offset.insert(peer, 0);
                    leader.snap_busy.remove(&peer);
                }
                return Ok(self.send_snapshot_chunk(peer));
            }
            return Ok(vec![Outbound::Send {
                to: peer,
                msg: self.make_append_entries(new_next),
            }]);
        }

        let mut out = self.try_commit();
        out.extend(self.check_read_index_ready());
        if self.quorum_contact() {
            out.push(Outbound::ResetElectionTimer);
        }
        if !out.is_empty() {
            self.persist().await?;
        }
        // Resume this peer if the ack freed in-flight budget. try_commit may
        // already have broadcast; a second pass is idempotent for the follower.
        let mut more = self.replicate_peer(peer, false);
        out.append(&mut more);
        Ok(out)
    }

    fn snap_reject(term: Term, to: NodeId) -> Outbound {
        Outbound::Send {
            to,
            msg: Message::InstallSnapshotResponse {
                term,
                success: false,
                last_included_index: 0,
                next_offset: 0,
                done: false,
            },
        }
    }

    // Field list mirrors InstallSnapshot.
    #[allow(clippy::too_many_arguments)]
    async fn handle_install_snapshot(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        config: ClusterConfig,
        done: bool,
        checksum: u32,
    ) -> anyhow::Result<Vec<Outbound>> {
        let reply_to = from.unwrap_or(leader_id);
        if term < self.persistent.current_term {
            return Ok(vec![Self::snap_reject(
                self.persistent.current_term,
                reply_to,
            )]);
        }
        self.become_follower(term, Some(leader_id));
        self.heard_from_leader = true;

        let restart = offset == 0
            || self
                .snap_in
                .as_ref()
                .map(|s| s.index != last_included_index || s.term != last_included_term)
                .unwrap_or(true);
        if restart {
            if offset != 0 {
                // Gap: chunk does not continue the buffer we have (we have none).
                return Ok(vec![
                    Outbound::ResetElectionTimer,
                    Self::snap_reject(self.persistent.current_term, reply_to),
                ]);
            }
            self.snap_in = Some(IncomingSnap {
                index: last_included_index,
                term: last_included_term,
                buf: Vec::new(),
                config: config.clone(),
                checksum,
            });
        } else if self.snap_in.as_ref().map(|s| s.checksum) != Some(checksum) {
            self.snap_in = None;
            return Ok(vec![
                Outbound::ResetElectionTimer,
                Self::snap_reject(self.persistent.current_term, reply_to),
            ]);
        }
        let assembled = self.snap_in.as_mut().unwrap();
        if offset > assembled.buf.len() as u64 {
            return Ok(vec![
                Outbound::ResetElectionTimer,
                Self::snap_reject(self.persistent.current_term, reply_to),
            ]);
        }
        if offset < assembled.buf.len() as u64 {
            // Duplicate or overlapping replay. Accept if it doesn't extend past
            // a conflicting suffix; do not truncate the log.
            let end = offset as usize + data.len();
            if end > assembled.buf.len() {
                return Ok(vec![
                    Outbound::ResetElectionTimer,
                    Self::snap_reject(self.persistent.current_term, reply_to),
                ]);
            }
            if assembled.buf[offset as usize..end] != data[..] {
                return Ok(vec![
                    Outbound::ResetElectionTimer,
                    Self::snap_reject(self.persistent.current_term, reply_to),
                ]);
            }
        } else {
            self.snapshot_bytes = self.snapshot_bytes.saturating_add(data.len() as u64);
            assembled.buf.extend_from_slice(&data);
            assembled.config = config.clone();
        }
        let next_offset = self.snap_in.as_ref().unwrap().buf.len() as u64;
        if !done {
            // Partial chunk: log prefix stays put.
            return Ok(vec![
                Outbound::ResetElectionTimer,
                Outbound::Send {
                    to: reply_to,
                    msg: Message::InstallSnapshotResponse {
                        term: self.persistent.current_term,
                        success: true,
                        last_included_index: 0,
                        next_offset,
                        done: false,
                    },
                },
            ]);
        }

        let incoming = self.snap_in.take().unwrap();
        if crate::storage::crc32(&incoming.buf) != incoming.checksum {
            // Corrupt or truncated image. Log, config and KV stay as they were.
            return Ok(vec![
                Outbound::ResetElectionTimer,
                Self::snap_reject(self.persistent.current_term, reply_to),
            ]);
        }
        let blob: SnapshotBlob = match bincode::deserialize(&incoming.buf) {
            Ok(b) => b,
            Err(_) => {
                return Ok(vec![
                    Outbound::ResetElectionTimer,
                    Self::snap_reject(self.persistent.current_term, reply_to),
                ]);
            }
        };
        if incoming.index <= self.volatile.commit_index {
            // Stale image (etcd `restore`): we already committed past it.
            // Installing would roll the KV back. Ack so the leader advances
            // match_index, but keep log, KV and config as they are.
            return Ok(vec![
                Outbound::ResetElectionTimer,
                Outbound::Send {
                    to: reply_to,
                    msg: Message::InstallSnapshotResponse {
                        term: self.persistent.current_term,
                        success: true,
                        last_included_index: incoming.index,
                        next_offset,
                        done: true,
                    },
                },
            ]);
        }
        self.kv.restore_full(blob.kv.clone(), blob.sessions.clone());
        self.persistent
            .log
            .install_snapshot(incoming.index, incoming.term);
        self.persistent.config = incoming.config.clone();
        self.refresh_role_from_config();
        self.snapshot_index = incoming.index;
        self.snapshot_term = incoming.term;
        self.snap_cache = None;
        if self.volatile.commit_index < incoming.index {
            self.commits = self
                .commits
                .saturating_add(incoming.index - self.volatile.commit_index);
            self.volatile.commit_index = incoming.index;
        }
        self.volatile.last_applied = incoming.index;
        self.trace
            .push("snapshot", self.persistent.current_term, incoming.index);

        self.storage
            .install_snapshot(
                incoming.index,
                incoming.term,
                blob.kv,
                blob.sessions,
                self.persistent.log.clone(),
                self.volatile.commit_index,
                incoming.config,
                self.persistent.current_term,
                self.persistent.voted_for,
            )
            .await?;

        Ok(vec![
            Outbound::ResetElectionTimer,
            Outbound::Send {
                to: reply_to,
                msg: Message::InstallSnapshotResponse {
                    term: self.persistent.current_term,
                    success: true,
                    last_included_index: incoming.index,
                    next_offset,
                    done: true,
                },
            },
        ])
    }

    async fn handle_install_snapshot_response(
        &mut self,
        from: Option<NodeId>,
        term: Term,
        success: bool,
        last_included_index: LogIndex,
        next_offset: u64,
        done: bool,
    ) -> anyhow::Result<Vec<Outbound>> {
        if term > self.persistent.current_term {
            self.become_follower(term, None);
            self.persist().await?;
            return Ok(vec![Outbound::ResetElectionTimer]);
        }
        if self.volatile.role != Role::Leader {
            return Ok(vec![]);
        }
        let Some(peer) = from else {
            return Ok(vec![]);
        };
        if !success {
            if let Some(leader) = self.leader.as_mut() {
                leader.snap_busy.remove(&peer);
                leader.snap_offset.insert(peer, 0);
            }
            return Ok(vec![]);
        }
        if let Some(leader) = self.leader.as_mut() {
            leader.snap_busy.remove(&peer);
            let cur = leader.snap_offset.get(&peer).copied().unwrap_or(0);
            if next_offset > cur {
                leader.snap_offset.insert(peer, next_offset);
            }
        }
        if !done {
            return Ok(self.send_snapshot_chunk(peer));
        }
        if let Some(leader) = self.leader.as_mut() {
            let prev = leader.match_index.get(&peer).copied().unwrap_or(0);
            if last_included_index > prev {
                leader.match_index.insert(peer, last_included_index);
            }
            let ni = leader.next_index.get(&peer).copied().unwrap_or(1);
            if last_included_index + 1 > ni {
                leader.next_index.insert(peer, last_included_index + 1);
            }
            leader.snap_offset.remove(&peer);
            leader.contact.insert(peer);
        }
        let self_id = self.id;
        if let Some(leader) = self.leader.as_mut() {
            leader.contact.insert(self_id);
        }
        let mut out = self.try_commit();
        out.extend(self.check_read_index_ready());
        if self.quorum_contact() {
            out.push(Outbound::ResetElectionTimer);
        }
        out.extend(self.replicate_peer(peer, false));
        Ok(out)
    }

    fn quorum_contact(&self) -> bool {
        self.leader
            .as_ref()
            .is_some_and(|l| has_config_quorum(&self.persistent.config, &l.contact))
    }

    async fn handle_timeout_now(
        &mut self,
        term: Term,
        _leader_id: NodeId,
    ) -> anyhow::Result<Vec<Outbound>> {
        if term < self.persistent.current_term {
            return Ok(vec![]);
        }
        if !self.persistent.config.is_voter(self.id) {
            return Ok(vec![]);
        }
        // TimeoutNow campaigns through pre-vote. A lost pre-vote must not bump term.
        self.heard_from_leader = false;
        if self.enable_pre_vote {
            self.start_pre_vote().await
        } else {
            self.start_election().await
        }
    }

    fn refresh_role_from_config(&mut self) {
        if self.volatile.role == Role::Leader || self.volatile.role == Role::Candidate {
            if !self.persistent.config.is_voter(self.id) {
                self.volatile.role = Role::Learner;
                self.leader = None;
            }
            return;
        }
        if self.persistent.config.is_voter(self.id) {
            if self.volatile.role == Role::Learner {
                self.volatile.role = Role::Follower;
            }
        } else {
            self.volatile.role = Role::Learner;
        }
    }

    fn apply_committed(&mut self) {
        while self.volatile.last_applied < self.volatile.commit_index {
            self.volatile.last_applied += 1;
            let idx = self.volatile.last_applied;
            let cmd = self.persistent.log.entry(idx).map(|e| e.command.clone());
            if let Some(cmd) = cmd {
                if let Command::ChangeConfig(cfg) = &cmd {
                    self.persistent.config = cfg.clone();
                    self.refresh_role_from_config();
                }
                self.kv.apply(&cmd);
                debug!(node = self.id, index = idx, "applied");
            }
        }
        // Committed entries can be older than an uncommitted C_new already
        // stored in the log. The latest config entry wins, committed or not.
        self.adopt_latest_config_from_log();
        // Leader: if joint config committed, append C_new to leave joint.
        if self.volatile.role == Role::Leader {
            if let ClusterConfig::Joint { new, .. } = self.persistent.config.clone() {
                // Only leave joint once the joint entry itself is committed and
                // we haven't already appended C_new after it.
                let last = self.persistent.log.last_index();
                let already = self
                    .persistent
                    .log
                    .entry(last)
                    .map(|e| matches!(&e.command, Command::ChangeConfig(ClusterConfig::Stable(_))))
                    .unwrap_or(false);
                // Check committed entry is joint.
                let committed_joint = (1..=self.volatile.commit_index)
                    .rev()
                    .find_map(|i| {
                        self.persistent.log.entry(i).and_then(|e| match &e.command {
                            Command::ChangeConfig(ClusterConfig::Joint { .. }) => Some(true),
                            Command::ChangeConfig(ClusterConfig::Stable(_)) => Some(false),
                            _ => None,
                        })
                    })
                    .unwrap_or(false);
                if committed_joint && !already {
                    let stable = ClusterConfig::Stable(new);
                    // Append happens in try_leave_joint called from try_commit.
                    let _ = stable;
                }
            }
        }
    }

    /// Latest ChangeConfig in the log, including entries that are not committed yet.
    fn adopt_latest_config_from_log(&mut self) {
        let last = self.persistent.log.last_index();
        let mut found = None;
        for i in 1..=last {
            if let Some(e) = self.persistent.log.entry(i) {
                if let Command::ChangeConfig(cfg) = &e.command {
                    found = Some(cfg.clone());
                }
            }
        }
        if let Some(cfg) = found {
            self.persistent.config = cfg;
            self.refresh_role_from_config();
        }
    }

    fn try_leave_joint(&mut self) -> bool {
        if self.volatile.role != Role::Leader {
            return false;
        }
        let ClusterConfig::Joint { new, .. } = self.persistent.config.clone() else {
            return false;
        };
        // Find if joint entry is committed and no stable C_new after it in log.
        let mut joint_idx = None;
        for i in 1..=self.persistent.log.last_index() {
            if let Some(e) = self.persistent.log.entry(i) {
                match &e.command {
                    Command::ChangeConfig(ClusterConfig::Joint { .. }) => joint_idx = Some(i),
                    Command::ChangeConfig(ClusterConfig::Stable(_)) if joint_idx.is_some() => {
                        return false; // already transitioning
                    }
                    _ => {}
                }
            }
        }
        let Some(ji) = joint_idx else {
            return false;
        };
        if self.volatile.commit_index < ji {
            return false;
        }
        // Append C_new.
        self.persistent.log.append(LogEntry {
            term: self.persistent.current_term,
            command: Command::ChangeConfig(ClusterConfig::Stable(new.clone())),
        });
        self.persistent.config = ClusterConfig::Stable(new);
        self.refresh_role_from_config();
        true
    }

    async fn handle_client(
        &mut self,
        req: ClientRequest,
        corr: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        match &req {
            ClientRequest::Get { .. }
            | ClientRequest::RawGet { .. }
            | ClientRequest::Put { .. }
            | ClientRequest::ReadIndex { .. }
            | ClientRequest::SessionPut { .. }
            | ClientRequest::ChangeMembership { .. }
            | ClientRequest::TransferLeadership { .. }
            | ClientRequest::PromoteLearner { .. } => {
                if self.volatile.role != Role::Leader {
                    return Ok(vec![Outbound::ClientReply {
                        corr,
                        resp: ClientResponse::not_leader(self.volatile.leader_id),
                    }]);
                }
            }
        }

        match req {
            ClientRequest::RawGet { key } => {
                let value = self.kv.get(&key).cloned();
                Ok(vec![Outbound::ClientReply {
                    corr,
                    resp: ClientResponse::ok_get(value),
                }])
            }
            ClientRequest::Get { key } | ClientRequest::ReadIndex { key } => {
                // Raft thesis §6.4: open a fresh round so only acks to the
                // heartbeats sent after this read arrived can confirm it.
                if let Some(leader) = self.leader.as_mut() {
                    leader.read_round = leader.read_round.wrapping_add(1);
                    leader.heartbeat_acks.clear();
                    leader.heartbeat_acks.insert(self.id);
                }
                let term = self.persistent.current_term;
                let mut out = self.broadcast_append_entries();
                // Single voter: our own ack is the quorum.
                if self.read_index_ready() {
                    let value = self.kv.get(&key).cloned();
                    out.push(Outbound::ClientReply {
                        corr,
                        resp: ClientResponse::ok_get(value),
                    });
                } else {
                    self.pending_reads.push((corr, key.clone(), term));
                    out.push(Outbound::ClientWaitRead {
                        corr,
                        key,
                        read_term: term,
                    });
                }
                out.push(Outbound::ResetHeartbeatTimer);
                Ok(out)
            }
            ClientRequest::Put { key, value } => {
                self.persistent.log.append(LogEntry {
                    term: self.persistent.current_term,
                    command: Command::Put { key, value },
                });
                self.persist().await?;
                let wait_index = self.persistent.log.last_index();
                let mut out = self.broadcast_append_entries();
                out.extend(self.try_commit());
                self.persist().await?;
                out.push(Outbound::ClientWaitCommit { corr, wait_index });
                out.push(Outbound::ResetHeartbeatTimer);
                Ok(out)
            }
            ClientRequest::SessionPut {
                client_id,
                serial,
                key,
                value,
            } => {
                if client_id == 0 {
                    return Ok(vec![Outbound::ClientReply {
                        corr,
                        resp: ClientResponse::err("client_id 0 is reserved"),
                    }]);
                }
                if self.kv.is_duplicate(client_id, serial) {
                    return Ok(vec![Outbound::ClientReply {
                        corr,
                        resp: ClientResponse::ok_put(),
                    }]);
                }
                if let Some(idx) = self.session_inflight(client_id, serial) {
                    if self.volatile.commit_index >= idx {
                        return Ok(vec![Outbound::ClientReply {
                            corr,
                            resp: ClientResponse::ok_put(),
                        }]);
                    }
                    return Ok(vec![Outbound::ClientWaitCommit {
                        corr,
                        wait_index: idx,
                    }]);
                }
                self.persistent.log.append(LogEntry {
                    term: self.persistent.current_term,
                    command: Command::SessionPut {
                        client_id,
                        serial,
                        key,
                        value,
                    },
                });
                self.persist().await?;
                let wait_index = self.persistent.log.last_index();
                let mut out = self.broadcast_append_entries();
                out.extend(self.try_commit());
                self.persist().await?;
                out.push(Outbound::ClientWaitCommit { corr, wait_index });
                out.push(Outbound::ResetHeartbeatTimer);
                Ok(out)
            }
            ClientRequest::ChangeMembership { voters, learners } => {
                self.propose_membership_change(voters, learners, corr).await
            }
            ClientRequest::TransferLeadership { target } => {
                self.transfer_leadership(target, corr).await
            }
            ClientRequest::PromoteLearner { id } => self.promote_learner(id, corr).await,
        }
    }

    /// Index of an identical session put still sitting in the log.
    fn session_inflight(&self, client_id: u64, serial: u64) -> Option<LogIndex> {
        let mut i = self.persistent.log.snapshot_index.saturating_add(1);
        let last = self.persistent.log.last_index();
        while i <= last {
            if let Some(e) = self.persistent.log.entry(i) {
                if let Command::SessionPut {
                    client_id: c,
                    serial: s,
                    ..
                } = &e.command
                {
                    if *c == client_id && *s == serial {
                        return Some(i);
                    }
                }
            }
            i += 1;
        }
        None
    }

    /// Move `id` into the voter set only after its match index covers the log.
    /// `ChangeMembership` stays unrestricted; this is the caught-up path.
    async fn promote_learner(&mut self, id: NodeId, corr: u64) -> anyhow::Result<Vec<Outbound>> {
        if self.membership_change_pending() {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("membership change still in progress"),
            }]);
        }
        let ClusterConfig::Stable(old) = self.persistent.config.clone() else {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("promote requires a stable config"),
            }]);
        };
        if old.is_voter(id) {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("already a voter"),
            }]);
        }
        if !old.is_learner(id) {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("id is not a learner"),
            }]);
        }
        let last = self.persistent.log.last_index();
        let matched = self
            .leader
            .as_ref()
            .and_then(|l| l.match_index.get(&id).copied())
            .unwrap_or(0);
        if matched < last {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("learner not caught up"),
            }]);
        }
        let mut voters = old.voters.clone();
        voters.push(id);
        let mut learners = old.learners.clone();
        learners.retain(|x| *x != id);
        self.propose_membership_change(voters, learners, corr).await
    }

    /// True while a config entry is uncommitted, including the C_new that
    /// follows a committed joint entry. A second change must wait.
    fn membership_change_pending(&self) -> bool {
        if self.persistent.config.is_joint() {
            return true;
        }
        let last = self.persistent.log.last_index();
        let mut i = self.volatile.commit_index.saturating_add(1);
        while i <= last {
            if let Some(e) = self.persistent.log.entry(i) {
                if matches!(e.command, Command::ChangeConfig(_)) {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    async fn propose_membership_change(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        corr: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        if self.membership_change_pending() {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("membership change still in progress"),
            }]);
        }
        let ClusterConfig::Stable(old) = self.persistent.config.clone() else {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("bad config"),
            }]);
        };
        // Refuse leaving a solitary voter without including self unless intentional
        // multi-node target — still allow, but never empty voters.
        if voters.is_empty() {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("voters must be non-empty"),
            }]);
        }
        // A one-node cluster must not drop its only leader by a config change.
        if old.voters.len() == 1 && old.voters[0] == self.id && !voters.contains(&self.id) {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err(
                    "single-node leader cannot remove itself from the voter set",
                ),
            }]);
        }
        let new = Membership::new(voters, learners);
        let joint = ClusterConfig::Joint {
            old: old.clone(),
            new: new.clone(),
        };
        self.persistent.log.append(LogEntry {
            term: self.persistent.current_term,
            command: Command::ChangeConfig(joint.clone()),
        });
        self.persistent.config = joint;
        self.refresh_role_from_config();
        // Ensure leader state tracks new peers.
        let peers = self.peer_ids();
        let last = self.persistent.log.last_index();
        if let Some(leader) = self.leader.as_mut() {
            for p in peers {
                leader.ensure_peer(p, last);
            }
        }
        self.persist().await?;
        let wait_index = self.persistent.log.last_index();
        let mut out = self.broadcast_append_entries();
        out.extend(self.try_commit());
        self.persist().await?;
        out.push(Outbound::ClientWaitCommit { corr, wait_index });
        out.push(Outbound::ResetHeartbeatTimer);
        Ok(out)
    }

    async fn transfer_leadership(
        &mut self,
        target: NodeId,
        corr: u64,
    ) -> anyhow::Result<Vec<Outbound>> {
        if !self.persistent.config.is_voter(target) {
            return Ok(vec![Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("target not a voter"),
            }]);
        }
        let last = self.persistent.log.last_index();
        let match_i = self
            .leader
            .as_ref()
            .and_then(|l| l.match_index.get(&target).copied())
            .unwrap_or(0);
        if match_i < last {
            // Do not step down until the target has the same log prefix.
            let mut out = self.broadcast_append_entries();
            out.push(Outbound::ClientReply {
                corr,
                resp: ClientResponse::err("target not caught up"),
            });
            return Ok(out);
        }
        let mut out = vec![Outbound::Send {
            to: target,
            msg: Message::TimeoutNow {
                term: self.persistent.current_term,
                leader_id: self.id,
            },
        }];
        // Step down only after the follower is caught up.
        self.become_follower(self.persistent.current_term, None);
        self.persist().await?;
        out.push(Outbound::ClientReply {
            corr,
            resp: ClientResponse::ok_put(),
        });
        out.push(Outbound::ResetElectionTimer);
        Ok(out)
    }

    fn try_commit(&mut self) -> Vec<Outbound> {
        if self.volatile.role != Role::Leader {
            return vec![];
        }
        let self_last = self.persistent.log.last_index();
        let match_index = self
            .leader
            .as_ref()
            .map(|l| l.match_index.clone())
            .unwrap_or_default();
        let new_commit = advance_commit_index(
            self.persistent.current_term,
            &self.persistent.log,
            &match_index,
            self.id,
            self_last,
            &self.persistent.config,
            self.volatile.commit_index,
        );
        let mut out = Vec::new();
        if new_commit > self.volatile.commit_index {
            self.commits = self
                .commits
                .saturating_add(new_commit - self.volatile.commit_index);
            self.volatile.commit_index = new_commit;
            self.trace
                .push("commit", self.persistent.current_term, new_commit);
            self.apply_committed();
            if self.try_leave_joint() {
                // Broadcast the C_new entry.
                out.extend(self.broadcast_append_entries());
            }
            out.extend(self.broadcast_append_entries());
        }
        out
    }

    /// A read may be served once (1) this term's noop is committed, so the
    /// commit index covers every entry committed by earlier leaders, and
    /// (2) a config quorum answered the current read round.
    fn read_index_ready(&self) -> bool {
        let Some(leader) = self.leader.as_ref() else {
            return false;
        };
        let term = self.persistent.current_term;
        let noop_committed = self.persistent.log.term_at(self.volatile.commit_index) == Some(term);
        noop_committed && has_config_quorum(&self.persistent.config, &leader.heartbeat_acks)
    }

    fn check_read_index_ready(&mut self) -> Vec<Outbound> {
        if self.pending_reads.is_empty() || !self.read_index_ready() {
            return vec![];
        }
        let term = self.persistent.current_term;
        let mut out = Vec::new();
        let pending = std::mem::take(&mut self.pending_reads);
        for (corr, key, read_term) in pending {
            if read_term != term {
                out.push(Outbound::ClientReply {
                    corr,
                    resp: ClientResponse::not_leader(None),
                });
                continue;
            }
            // Quorum reached — serve read from applied state.
            let value = self.kv.get(&key).cloned();
            out.push(Outbound::ClientReply {
                corr,
                resp: ClientResponse::ok_get(value),
            });
        }
        // pending_reads drained into replies; keep none.
        self.pending_reads.clear();
        out
    }

    /// Register a wait-read (called from node loop when Outbound::ClientWaitRead seen).
    pub fn register_read_wait(&mut self, corr: u64, key: String, read_term: Term) {
        self.pending_reads.push((corr, key, read_term));
    }

    /// Create a local snapshot compacting the log prefix through commit_index.
    pub async fn compact_log(&mut self, retain_index: LogIndex) -> anyhow::Result<()> {
        let last_included = retain_index.min(self.volatile.commit_index);
        if last_included == 0 || last_included <= self.persistent.log.snapshot_index {
            return Ok(());
        }
        let term = self
            .persistent
            .log
            .term_at(last_included)
            .unwrap_or(self.persistent.current_term);
        // Ensure KV reflects applied state through last_included.
        while self.volatile.last_applied < last_included.min(self.volatile.commit_index) {
            self.volatile.last_applied += 1;
            if let Some(e) = self.persistent.log.entry(self.volatile.last_applied) {
                self.kv.apply(&e.command);
            }
        }
        self.persistent.log.compact_through(last_included, term);
        self.snapshot_index = last_included;
        self.snapshot_term = term;
        let image = SnapshotBlob {
            kv: self.kv.snapshot(),
            sessions: self.kv.sessions_snapshot(),
        };
        let image_bytes = bincode::serialized_size(&image).unwrap_or(0);
        self.snapshot_bytes = self.snapshot_bytes.saturating_add(image_bytes);
        self.snap_cache = None;
        self.trace.push("snapshot", term, last_included);
        self.storage
            .install_snapshot(
                last_included,
                term,
                image.kv,
                image.sessions,
                self.persistent.log.clone(),
                self.volatile.commit_index,
                self.persistent.config.clone(),
                self.persistent.current_term,
                self.persistent.voted_for,
            )
            .await?;
        info!(node = self.id, last_included, "log compacted via snapshot");
        Ok(())
    }

    /// Compact when the committed suffix since the last snapshot reaches
    /// `snapshot_threshold`. A threshold of 0 does nothing.
    pub async fn maybe_auto_snapshot(&mut self) -> anyhow::Result<()> {
        let threshold = self.snapshot_threshold;
        if threshold == 0 {
            return Ok(());
        }
        let pending = self
            .volatile
            .commit_index
            .saturating_sub(self.persistent.log.snapshot_index);
        if pending < threshold {
            return Ok(());
        }
        self.compact_log(self.volatile.commit_index).await
    }

    /// Snapshot stub kept for API compat — now performs real compaction.
    pub async fn maybe_snapshot_stub(&mut self, threshold: LogIndex) -> anyhow::Result<()> {
        if self.volatile.commit_index < threshold {
            return Ok(());
        }
        self.compact_log(self.volatile.commit_index).await
    }
}

fn format_metrics(m: &crate::raft::MetricsSnapshot) -> String {
    let mut lags: Vec<_> = m.replication_lag.iter().collect();
    lags.sort_by_key(|(id, _)| *id);
    let lag = lags
        .into_iter()
        .map(|(id, v)| format!("{id}:{v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "id={} role={} term={} commit={} applied={} last_log={} snapshot_index={} commits={} elections={} snapshot_bytes={} lag={}",
        m.id,
        m.role,
        m.term,
        m.commit_index,
        m.last_applied,
        m.last_log_index,
        m.snapshot_index,
        m.commits,
        m.elections,
        m.snapshot_bytes,
        lag
    )
}

fn format_status(s: &NodeStatus) -> String {
    format!(
        "id={} role={} term={} leader={:?} commit={} applied={} log={} kv={} peers={:?} config={} commits={} elections={} snapshot_bytes={}",
        s.id,
        s.role,
        s.term,
        s.leader,
        s.commit_index,
        s.last_applied,
        s.log_len,
        s.kv_len,
        s.peers,
        s.config,
        s.commits,
        s.elections,
        s.snapshot_bytes
    )
}

//! Deterministic Raft simulator.
//!
//! Time is a logical tick counter. Delivery, drop, delay and partitions are
//! decided by a seeded RNG — wall-clock sleeps are not the source of truth.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::raft::{RaftCore, Role, TimingConfig};
use crate::rpc::{ClientRequest, Message, NodeId};
use crate::storage::Storage;

#[derive(Clone, Debug)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    msg: Message,
    /// Address the sender believed `to` was listening on.
    dest_addr: String,
}

pub struct Sim {
    pub seed: u64,
    rng: StdRng,
    pub nodes: BTreeMap<NodeId, RaftCore>,
    /// Current listen address per node.
    listen: BTreeMap<NodeId, String>,
    /// Sender's view of peer addresses (config reload updates this).
    routes: BTreeMap<NodeId, BTreeMap<NodeId, String>>,
    /// Directed partition: (from, to) dropped.
    partition: BTreeSet<(NodeId, NodeId)>,
    inbox: VecDeque<Envelope>,
    /// Nodes whose election timer was reset by a heartbeat or vote grant.
    reset_election: BTreeSet<NodeId>,
    tick: u64,
    drop_permille: u32,
    pub leaders_by_term: BTreeMap<u64, NodeId>,
    pub safety_violations: Vec<String>,
}

impl Sim {
    pub async fn new(seed: u64, ids: &[NodeId]) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut nodes = BTreeMap::new();
        let mut listen = BTreeMap::new();
        let mut routes = BTreeMap::new();
        for &id in ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|p| *p != id).collect();
            let storage = Storage::ephemeral();
            let timing = TimingConfig {
                heartbeat_ms: 3,
                election_min_ms: 10,
                election_max_ms: 20,
            };
            let core = RaftCore::open(id, peers.clone(), storage, timing)
                .await
                .unwrap();
            nodes.insert(id, core);
            let addr = format!("n{id}");
            listen.insert(id, addr.clone());
            let mut rt = BTreeMap::new();
            for p in peers {
                rt.insert(p, format!("n{p}"));
            }
            routes.insert(id, rt);
            let _ = rng.gen::<u8>();
        }
        Self {
            seed,
            rng,
            nodes,
            listen,
            routes,
            partition: BTreeSet::new(),
            inbox: VecDeque::new(),
            reset_election: BTreeSet::new(),
            tick: 0,
            drop_permille: 0,
            leaders_by_term: BTreeMap::new(),
            safety_violations: Vec::new(),
        }
    }

    pub fn set_drop_permille(&mut self, v: u32) {
        self.drop_permille = v;
    }

    pub fn partition_pair(&mut self, a: NodeId, b: NodeId) {
        self.partition.insert((a, b));
        self.partition.insert((b, a));
    }

    pub fn heal(&mut self) {
        self.partition.clear();
    }

    /// Move a node's listen address. Senders still route to the old address
    /// until [`reload_routes`].
    pub fn move_listen(&mut self, id: NodeId, addr: impl Into<String>) {
        self.listen.insert(id, addr.into());
    }

    /// Config reload: every node's route table is refreshed to current listen addrs.
    pub fn reload_routes(&mut self) {
        let listen = self.listen.clone();
        for (from, table) in self.routes.iter_mut() {
            for (to, addr) in table.iter_mut() {
                if let Some(cur) = listen.get(to) {
                    *addr = cur.clone();
                }
            }
            let _ = from;
        }
    }

    fn record_safety(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for &id in &ids {
            let n = &self.nodes[&id];
            if n.volatile.role == Role::Leader {
                let term = n.persistent.current_term;
                if let Some(prev) = self.leaders_by_term.get(&term) {
                    if *prev != id {
                        self.safety_violations
                            .push(format!("two leaders in term {term}: {prev} and {id}"));
                    }
                } else {
                    self.leaders_by_term.insert(term, id);
                }
            }
        }
        // Log matching: if two logs have same index+term, commands match
        // and the prefix up to that index matches on overlapping stored entries.
        for i in 0..ids.len() {
            for j in (i + 1)..ids.len() {
                let a = &self.nodes[&ids[i]];
                let b = &self.nodes[&ids[j]];
                let last = a.last_log_index().min(b.last_log_index());
                for idx in 1..=last {
                    let (ta, tb) = (a.persistent.log.term_at(idx), b.persistent.log.term_at(idx));
                    if ta.is_some() && ta == tb {
                        let ea = a.persistent.log.entry(idx);
                        let eb = b.persistent.log.entry(idx);
                        if ea.is_some() && eb.is_some() && ea != eb {
                            self.safety_violations.push(format!(
                                "log mismatch idx {idx} between {} and {}",
                                ids[i], ids[j]
                            ));
                        }
                    }
                }
            }
        }
    }

    async fn drain_out(&mut self, from: NodeId, out: Vec<crate::raft::Outbound>) {
        use crate::raft::Outbound::*;
        for ob in out {
            match ob {
                Send { to, msg } => {
                    if self.partition.contains(&(from, to)) {
                        continue;
                    }
                    if self.drop_permille > 0 && self.rng.gen_range(0..1000) < self.drop_permille {
                        continue;
                    }
                    let dest = self
                        .routes
                        .get(&from)
                        .and_then(|t| t.get(&to))
                        .cloned()
                        .unwrap_or_default();
                    self.inbox.push_back(Envelope {
                        from,
                        to,
                        msg,
                        dest_addr: dest,
                    });
                }
                ResetElectionTimer | ForceElectionTimeout => {
                    self.reset_election.insert(from);
                }
                ResetHeartbeatTimer => {}
                ClientReply { .. } | ClientWaitCommit { .. } | ClientWaitRead { .. } => {}
            }
        }
    }

    /// Deliver one queued message if the destination is still listening on dest_addr.
    pub async fn step_message(&mut self) -> bool {
        let Some(env) = self.inbox.pop_front() else {
            return false;
        };
        let listen = self.listen.get(&env.to).cloned().unwrap_or_default();
        if listen != env.dest_addr {
            // Stale route — dropped until config reload.
            return true;
        }
        let mut out = Vec::new();
        if let Some(node) = self.nodes.get_mut(&env.to) {
            match node.handle_message(Some(env.from), env.msg, 0).await {
                Ok(o) => out = o,
                Err(_) => return true,
            }
        }
        let to = env.to;
        self.drain_out(to, out).await;
        self.record_safety();
        true
    }

    pub async fn timeout(&mut self, id: NodeId) {
        let mut out = Vec::new();
        if let Some(n) = self.nodes.get_mut(&id) {
            if let Ok(o) = n.on_election_timeout().await {
                out = o;
            }
        }
        self.drain_out(id, out).await;
        self.record_safety();
    }

    pub async fn heartbeat(&mut self, id: NodeId) {
        let mut out = Vec::new();
        if let Some(n) = self.nodes.get_mut(&id) {
            if let Ok(o) = n.on_heartbeat_timeout().await {
                out = o;
            }
        }
        self.drain_out(id, out).await;
        self.record_safety();
    }

    pub async fn client(
        &mut self,
        id: NodeId,
        req: ClientRequest,
    ) -> Option<crate::rpc::ClientResponse> {
        let mut out = Vec::new();
        let mut immediate = None;
        if let Some(n) = self.nodes.get_mut(&id) {
            if let Ok(o) = n.handle_message(None, Message::Client(req), 1).await {
                for ob in &o {
                    if let crate::raft::Outbound::ClientReply { resp, .. } = ob {
                        immediate = Some(resp.clone());
                    }
                }
                out = o;
            }
        }
        self.drain_out(id, out).await;
        self.record_safety();
        immediate
    }

    /// Run `ticks` of logical time. Each tick: maybe election, heartbeat, deliver up to 8 msgs.
    pub async fn run(&mut self, ticks: u64) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        // per-node next election tick
        let mut next_el: BTreeMap<NodeId, u64> = BTreeMap::new();
        for &id in &ids {
            let d = self.rng.gen_range(10..21);
            next_el.insert(id, self.tick + d);
        }
        let mut next_hb: BTreeMap<NodeId, u64> = ids.iter().map(|id| (*id, 0)).collect();
        for _ in 0..ticks {
            self.tick += 1;
            // Deliver before timers so a heartbeat cancels an election, like the node loop.
            for _ in 0..8 {
                if self.inbox.is_empty() {
                    break;
                }
                if self.rng.gen_range(0..5) == 0 {
                    if let Some(env) = self.inbox.pop_front() {
                        self.inbox.push_back(env);
                    }
                    break;
                }
                self.step_message().await;
            }
            for &id in &ids {
                if self.reset_election.remove(&id) {
                    let d = self.rng.gen_range(10..21);
                    next_el.insert(id, self.tick + d);
                }
                let role_leader = self.nodes[&id].volatile.role == Role::Leader;
                if role_leader && self.tick >= next_hb[&id] {
                    self.heartbeat(id).await;
                    next_hb.insert(id, self.tick + 3);
                }
                if !role_leader && self.tick >= next_el[&id] {
                    self.timeout(id).await;
                    let d = self.rng.gen_range(10..21);
                    // Timeout may itself reset the timer; either way the next
                    // deadline is `d` ticks from now.
                    self.reset_election.remove(&id);
                    next_el.insert(id, self.tick + d);
                }
            }
            self.record_safety();
            if !self.safety_violations.is_empty() {
                break;
            }
        }
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.nodes.iter().find_map(|(id, n)| {
            if n.volatile.role == Role::Leader {
                Some(*id)
            } else {
                None
            }
        })
    }

    pub fn commit_of(&self, id: NodeId) -> u64 {
        self.nodes[&id].commit_index()
    }
}

/// Seeds actually exercised by the property test (reported, not invented).
pub const PROPERTY_SEEDS: &[u64] = &[
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
    75, 76, 77, 78, 79, 80,
];

/// Seeds for the packet-loss + membership-change scenario. Same range as the
/// extension past the 0.5.0 set; listed separately so a report can name them.
pub const MEMBERSHIP_LOSS_SEEDS: &[u64] = &[
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64,
    65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
];

pub async fn check_seed(seed: u64) -> Result<(), String> {
    let mut sim = Sim::new(seed, &[1, 2, 3]).await;
    sim.set_drop_permille(50); // 5% drop
                               // occasional partition of node 3 for the middle of the run
    sim.run(40).await;
    sim.partition_pair(1, 3);
    sim.partition_pair(2, 3);
    sim.run(20).await;
    sim.heal();
    sim.run(40).await;
    if !sim.safety_violations.is_empty() {
        return Err(format!("seed {seed}: {}", sim.safety_violations.join("; ")));
    }
    if sim.leaders_by_term.is_empty() {
        return Err(format!("seed {seed}: no leader elected"));
    }
    Ok(())
}

/// Packet loss plus one membership change. Election safety and log matching
/// are checked the same way as [`check_seed`]. No timing numbers are recorded.
pub async fn check_membership_under_loss(seed: u64) -> Result<(), String> {
    let mut sim = Sim::new(seed, &[1, 2, 3]).await;
    sim.set_drop_permille(100);
    sim.run(50).await;
    if !sim.safety_violations.is_empty() {
        return Err(format!(
            "seed {seed} before change: {}",
            sim.safety_violations.join("; ")
        ));
    }
    let Some(leader) = sim.leader_id() else {
        return Err(format!("seed {seed}: no leader before membership change"));
    };
    let other = [1u64, 2, 3].into_iter().find(|id| *id != leader).unwrap();
    let _ = sim
        .client(
            leader,
            ClientRequest::ChangeMembership {
                voters: vec![leader, other],
                learners: vec![],
            },
        )
        .await;
    sim.run(70).await;
    if !sim.safety_violations.is_empty() {
        return Err(format!(
            "seed {seed} after membership change: {}",
            sim.safety_violations.join("; ")
        ));
    }
    if sim.leaders_by_term.is_empty() {
        return Err(format!("seed {seed}: no leader elected"));
    }
    Ok(())
}

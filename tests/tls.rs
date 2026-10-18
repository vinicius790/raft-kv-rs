//! Mutual TLS between nodes (feature `tls`). Certificates are generated with
//! rcgen at test time; nothing is read from disk.

#![cfg(feature = "tls")]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use raft_kv_rs::node::{get_redirect, put_redirect, spawn_node_with, wait_for_leader, NodeConfig};
use raft_kv_rs::raft::TimingConfig;
use raft_kv_rs::rpc::Message;
use raft_kv_rs::tls::TlsMaterial;
use raft_kv_rs::transport::{PeerMap, PeerPool, PoolConfig, Security};
use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair};

struct Ca {
    params: CertificateParams,
    key: KeyPair,
    pem: String,
}

impl Ca {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let pem = params.self_signed(&key).unwrap().pem();
        Self { params, key, pem }
    }

    /// (cert PEM, key PEM) for node `id`.
    fn node(&self, id: u64) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let params =
            CertificateParams::new(vec![format!("node-{id}"), "raft-kv".to_string()]).unwrap();
        let issuer = Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn material(&self, id: u64, trust: &Ca) -> TlsMaterial {
        let (c, k) = self.node(id);
        TlsMaterial::from_pem(c.as_bytes(), k.as_bytes(), trust.pem.as_bytes()).unwrap()
    }
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

fn timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 40,
        election_min_ms: 150,
        election_max_ms: 300,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_cluster_elects_and_replicates() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let addrs: HashMap<u64, SocketAddr> = (1..=3).map(|i| (i, free_addr())).collect();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let peers = addrs
            .iter()
            .filter(|(o, _)| **o != id)
            .map(|(o, a)| (*o, *a))
            .collect();
        let cfg = NodeConfig {
            id,
            bind: addrs[&id],
            peers,
            data_dir: dir.path().join(format!("n{id}")),
            timing: timing(),
            learners: Vec::new(),
            snapshot_threshold: 0,
        };
        nodes.push(
            spawn_node_with(cfg, Security::tls(ca.material(id, &ca)))
                .await
                .unwrap(),
        );
    }
    let leader = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .expect("leader over TLS");
    assert!(put_redirect(&nodes, leader, "k", "v").await.ok);
    let got = get_redirect(&nodes, leader, "k").await;
    assert_eq!(got.value.as_deref(), Some("v"));

    // An anonymous TLS client (CA only, no cert) can use the client path.
    let client = Security::tls(TlsMaterial::client_only(ca.pem.as_bytes()).unwrap());
    let reply = raft_kv_rs::transport::client_request_secure(
        addrs[&leader],
        Message::StatusRequest,
        Duration::from_secs(3),
        &client,
    )
    .await
    .unwrap();
    assert!(matches!(
        reply,
        Message::StatusResponse(_) | Message::ClientReply(_)
    ));
    // A plain-TCP client gets nothing from a TLS node.
    assert!(raft_kv_rs::transport::client_request(
        addrs[&leader],
        Message::StatusRequest,
        Duration::from_millis(500)
    )
    .await
    .is_err());

    for n in nodes {
        n.shutdown().await;
    }
}

/// Node 1 alone (peers never start), long timeouts so it does not campaign.
/// A RequestVote with term 1000 that is *accepted* raises its term.
async fn probe(ca: &Ca, from: u64, sec: Security) -> u64 {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let cfg = NodeConfig {
        id: 1,
        bind: addr,
        peers: HashMap::from([(2, free_addr()), (3, free_addr())]),
        data_dir: dir.path().join("n1"),
        timing: TimingConfig {
            heartbeat_ms: 1000,
            election_min_ms: 30_000,
            election_max_ms: 40_000,
        },
        learners: Vec::new(),
        snapshot_threshold: 0,
    };
    let node = spawn_node_with(cfg, Security::tls(ca.material(1, ca)))
        .await
        .unwrap();
    let pool = PeerPool::new(
        from,
        PeerMap::new(HashMap::from([(1, addr)])),
        PoolConfig::default(),
    )
    .with_security(sec);
    let vote = Message::RequestVote {
        term: 1000,
        candidate_id: from,
        last_log_index: 100,
        last_log_term: 100,
    };
    let _ = pool.send(1, &vote).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let term = node.status().await.term;
    node.shutdown().await;
    term
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_identity_comes_from_certificate() {
    let ca = Ca::new();
    // Control: node 3 with its own certificate is accepted.
    assert_eq!(
        probe(&ca, 3, Security::tls(ca.material(3, &ca))).await,
        1000
    );
    // Valid certificate for node 3, hello claims node 2: refused.
    assert!(probe(&ca, 2, Security::tls(ca.material(3, &ca))).await < 1000);
    // Certificate for node 2 from a different CA: handshake refused.
    let rogue = Ca::new();
    assert!(probe(&ca, 2, Security::tls(rogue.material(2, &ca))).await < 1000);
    // Plain TCP peer to a TLS node: refused.
    assert!(probe(&ca, 2, Security::plain()).await < 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn anonymous_tls_client_cannot_send_peer_rpcs() {
    let ca = Ca::new();
    let dir = tempfile::tempdir().unwrap();
    let addrs: HashMap<u64, SocketAddr> = (1..=3).map(|i| (i, free_addr())).collect();
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        let peers = addrs
            .iter()
            .filter(|(o, _)| **o != id)
            .map(|(o, a)| (*o, *a))
            .collect();
        let cfg = NodeConfig {
            id,
            bind: addrs[&id],
            peers,
            data_dir: dir.path().join(format!("n{id}")),
            timing: timing(),
            learners: Vec::new(),
            snapshot_threshold: 0,
        };
        nodes.push(
            spawn_node_with(cfg, Security::tls(ca.material(id, &ca)))
                .await
                .unwrap(),
        );
    }
    let leader = wait_for_leader(&nodes, Duration::from_secs(5))
        .await
        .expect("leader");
    let term = nodes[0].status().await.term;

    // No client certificate: only the client path is reachable.
    let anon = Security::tls(TlsMaterial::client_only(ca.pem.as_bytes()).unwrap());
    let attacks = [
        Message::AppendEntries {
            term: term + 100,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
            read_ctx: 0,
        },
        Message::RequestVote {
            term: term + 200,
            candidate_id: 2,
            last_log_index: 1_000,
            last_log_term: term + 200,
        },
        Message::TimeoutNow {
            term: term + 300,
            leader_id: leader,
        },
    ];
    for target in addrs.values() {
        for msg in &attacks {
            let r = raft_kv_rs::transport::client_request_secure(
                *target,
                msg.clone(),
                Duration::from_millis(300),
                &anon,
            )
            .await;
            assert!(r.is_err(), "anonymous peer RPC got a reply: {r:?}");
        }
    }
    tokio::time::sleep(Duration::from_millis(400)).await;
    for n in &nodes {
        let st = n.status().await;
        assert!(
            st.term < term + 100,
            "node {} term moved to {} via anonymous RPC",
            st.id,
            st.term
        );
    }
    assert_eq!(
        wait_for_leader(&nodes, Duration::from_secs(2)).await,
        Some(leader)
    );

    for n in nodes {
        n.shutdown().await;
    }
}

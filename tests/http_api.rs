//! Integration: HTTP admin/KV API on a 3-node cluster, ephemeral ports only.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use raft_kv_rs::node::{spawn_node, NodeConfig, NodeHandle};
use raft_kv_rs::raft::TimingConfig;
use tokio::net::TcpListener;

struct Cluster {
    nodes: Vec<NodeHandle>,
    http: HashMap<u64, SocketAddr>,
    _dir: tempfile::TempDir,
}

fn free_addr() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

async fn cluster() -> Cluster {
    let dir = tempfile::tempdir().unwrap();
    let raft: Vec<(u64, SocketAddr)> = (1..=3).map(|i| (i, free_addr())).collect();
    let mut listeners = Vec::new();
    let mut http = HashMap::new();
    for (id, _) in &raft {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        http.insert(*id, l.local_addr().unwrap());
        listeners.push(l);
    }
    let mut nodes = Vec::new();
    for ((id, bind), listener) in raft.iter().zip(listeners) {
        let peers = raft
            .iter()
            .filter(|(o, _)| o != id)
            .map(|(o, a)| (*o, *a))
            .collect();
        let h = spawn_node(NodeConfig {
            id: *id,
            bind: *bind,
            peers,
            data_dir: dir.path().join(format!("n{id}")),
            timing: TimingConfig {
                heartbeat_ms: 40,
                election_min_ms: 150,
                election_max_ms: 300,
            },
            learners: Vec::new(),
            snapshot_threshold: 0,
        })
        .await
        .unwrap();
        let client = h.client();
        let hp = http.clone();
        tokio::spawn(async move { raft_kv_rs::http::serve(listener, client, hp).await });
        nodes.push(h);
    }
    Cluster {
        nodes,
        http,
        _dir: dir,
    }
}

fn no_redirect() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()
        .unwrap()
}

async fn statuses(c: &Cluster, http: &reqwest::Client) -> Vec<serde_json::Value> {
    let mut out = Vec::new();
    for addr in c.http.values() {
        let v: serde_json::Value = http
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        out.push(v);
    }
    out
}

async fn wait_leader(c: &Cluster, http: &reqwest::Client) -> u64 {
    for _ in 0..100 {
        let st = statuses(c, http).await;
        let leaders: Vec<_> = st.iter().filter(|s| s["role"] == "Leader").collect();
        if leaders.len() == 1 {
            let term = leaders[0]["term"].as_u64().unwrap();
            // Everyone who knows a leader agrees on it and is not ahead in term.
            if st.iter().all(|s| s["term"].as_u64().unwrap() == term) {
                return leaders[0]["id"].as_u64().unwrap();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no single stable leader");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_metrics_single_leader_and_consistent_term() {
    let c = cluster().await;
    let http = no_redirect();
    let leader = wait_leader(&c, &http).await;

    for addr in c.http.values() {
        let r = http
            .get(format!("http://{addr}/health"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }

    // A write so commit/applied move, then scrape every node.
    let la = c.http[&leader];
    let r = http
        .put(format!("http://{la}/kv/k"))
        .body("v")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);

    let mut leaders_in_metrics = 0;
    let mut terms = Vec::new();
    for (id, addr) in &c.http {
        let text = http
            .get(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let find = |name: &str| -> f64 {
            let prefix = format!("{name}{{node=\"{id}\"}} ");
            text.lines()
                .find_map(|l| l.strip_prefix(&prefix))
                .unwrap_or_else(|| panic!("{name} missing for node {id}:\n{text}"))
                .parse()
                .unwrap()
        };
        if find("raft_role") == 2.0 {
            leaders_in_metrics += 1;
        }
        terms.push(find("raft_term"));
        assert!(find("raft_commit_index") >= 1.0);
    }
    assert_eq!(leaders_in_metrics, 1, "exactly one leader in /metrics");
    assert!(
        terms.windows(2).all(|w| w[0] == w[1]),
        "terms differ: {terms:?}"
    );

    for n in c.nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_crud_and_follower_redirect() {
    let c = cluster().await;
    let http = no_redirect();
    let leader = wait_leader(&c, &http).await;
    let la = c.http[&leader];
    let follower = *c.http.keys().find(|id| **id != leader).unwrap();
    let fa = c.http[&follower];

    // Follower → 307 to the leader, method preserved.
    let r = http
        .put(format!("http://{fa}/kv/a"))
        .body("1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 307);
    assert_eq!(
        r.headers()["location"].to_str().unwrap(),
        format!("http://{la}/kv/a")
    );

    // A redirect-following client lands on the leader.
    let follow = reqwest::Client::new();
    let r = follow
        .put(format!("http://{fa}/kv/a"))
        .body("1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);

    let r = http.get(format!("http://{la}/kv/a")).send().await.unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "1");

    let r = http
        .delete(format!("http://{la}/kv/a"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = http.get(format!("http://{la}/kv/a")).send().await.unwrap();
    assert_eq!(r.status(), 404);

    let r = http
        .get(format!("http://{la}/metrics"))
        .send()
        .await
        .unwrap();
    let text = r.text().await.unwrap();
    assert!(text.contains(&format!(
        "kv_requests_total{{node=\"{leader}\",op=\"delete\",result=\"ok\"}} 1"
    )));
    assert!(text.contains("raft_append_latency_seconds_bucket"));

    for n in c.nodes {
        n.shutdown().await;
    }
}

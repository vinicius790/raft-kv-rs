//! Prometheus metrics (`metrics` 0.24 facade + `metrics-exporter-prometheus`).
//!
//! One process-wide recorder is installed lazily. Every series carries a `node`
//! label so several in-process nodes (tests) do not overwrite each other.
//!
//! | Series                          | Kind      | Meaning                                  |
//! |---------------------------------|-----------|------------------------------------------|
//! | `raft_term`                     | gauge     | current term                             |
//! | `raft_commit_index`             | gauge     | highest committed index                  |
//! | `raft_applied_index`            | gauge     | last index applied to the KV             |
//! | `raft_role`                     | gauge     | 0 follower, 1 candidate, 2 leader, 3 learner/other |
//! | `raft_elections_total`          | counter   | elections started by this process        |
//! | `raft_snapshot_bytes_total`     | counter   | snapshot bytes serialized/sent/accepted  |
//! | `raft_append_latency_seconds`   | histogram | AppendEntries send → response, per peer  |
//! | `kv_requests_total{op,result}`  | counter   | HTTP KV requests                         |

use std::sync::OnceLock;
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use crate::raft::MetricsSnapshot;
use crate::rpc::NodeId;

static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

/// Install (once) and return the Prometheus handle. If another global
/// recorder is already installed, a detached recorder is used so rendering
/// still works (it will simply be empty).
pub fn handle() -> &'static PrometheusHandle {
    HANDLE.get_or_init(|| {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full("raft_append_latency_seconds".into()),
                &[
                    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
                ],
            )
            .expect("static bucket list is non-empty")
            .build_recorder();
        let h = recorder.handle();
        let _ = metrics::set_global_recorder(recorder);
        h
    })
}

/// Prometheus text exposition of all series.
pub fn render() -> String {
    handle().render()
}

pub fn role_code(role: &str) -> f64 {
    match role {
        "Follower" => 0.0,
        "Candidate" => 1.0,
        "Leader" => 2.0,
        _ => 3.0,
    }
}

/// Publish the core's gauges/counters.
pub fn record_snapshot(m: &MetricsSnapshot) {
    handle();
    let node = m.id.to_string();
    gauge!("raft_term", "node" => node.clone()).set(m.term as f64);
    gauge!("raft_commit_index", "node" => node.clone()).set(m.commit_index as f64);
    gauge!("raft_applied_index", "node" => node.clone()).set(m.last_applied as f64);
    gauge!("raft_role", "node" => node.clone()).set(role_code(&m.role));
    counter!("raft_elections_total", "node" => node.clone()).absolute(m.elections);
    counter!("raft_snapshot_bytes_total", "node" => node).absolute(m.snapshot_bytes);
}

pub fn record_append_latency(node: NodeId, peer: NodeId, d: Duration) {
    handle();
    histogram!(
        "raft_append_latency_seconds",
        "node" => node.to_string(),
        "peer" => peer.to_string()
    )
    .record(d.as_secs_f64());
}

pub fn record_kv(node: NodeId, op: &'static str, result: &'static str) {
    handle();
    counter!("kv_requests_total", "node" => node.to_string(), "op" => op, "result" => result)
        .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_labeled_series() {
        record_kv(99, "get", "ok");
        record_append_latency(99, 1, Duration::from_millis(3));
        let text = render();
        assert!(text.contains("kv_requests_total{node=\"99\",op=\"get\",result=\"ok\"} 1"));
        assert!(text.contains("raft_append_latency_seconds_bucket"));
    }
}

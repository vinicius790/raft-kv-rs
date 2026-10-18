//! HTTP/JSON front end (axum).
//!
//! | Method | Path        | Behaviour                                              |
//! |--------|-------------|--------------------------------------------------------|
//! | GET    | `/kv/{key}` | Linearizable read (read index). 404 if absent.         |
//! | PUT    | `/kv/{key}` | Body is the raw value. Returns after commit.           |
//! | DELETE | `/kv/{key}` | Returns after commit.                                  |
//! | GET    | `/status`   | [`NodeStatus`] as JSON.                                |
//! | GET    | `/health`   | `200 ok` while the node loop answers.                  |
//! | GET    | `/metrics`  | Prometheus text format (see [`crate::telemetry`]).     |
//!
//! A follower answers `307 Temporary Redirect` with `Location` pointing at the
//! leader's HTTP address when it is known (from `http_peers`), else `503`.
//! 307 keeps the method and body, so `curl -L -X PUT` follows it correctly.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;

use crate::node::NodeHandle;
use crate::rpc::{ClientResponse, NodeId, NodeStatus};

/// Upper bound for one consensus round trip seen by an HTTP client.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Values larger than this are rejected with 413 (well below the 16 MiB frame cap).
pub const MAX_VALUE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct AppState {
    node: Arc<NodeHandle>,
    http_peers: Arc<HashMap<NodeId, SocketAddr>>,
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
    leader_hint: Option<NodeId>,
}

/// Build the router. `http_peers` maps node id → HTTP address (for redirects).
pub fn router(node: NodeHandle, http_peers: HashMap<NodeId, SocketAddr>) -> Router {
    let state = AppState {
        node: Arc::new(node),
        http_peers: Arc::new(http_peers),
    };
    Router::new()
        .route("/kv/{key}", get(get_key).put(put_key).delete(delete_key))
        .route("/status", get(status))
        .route("/health", get(health))
        .route("/metrics", get(metrics_text))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_VALUE_BYTES))
        .with_state(state)
}

/// Serve on an already-bound listener until the future resolves.
pub async fn serve(
    listener: TcpListener,
    node: NodeHandle,
    http_peers: HashMap<NodeId, SocketAddr>,
) -> std::io::Result<()> {
    axum::serve(listener, router(node, http_peers)).await
}

async fn with_timeout(fut: impl std::future::Future<Output = ClientResponse>) -> ClientResponse {
    tokio::time::timeout(REQUEST_TIMEOUT, fut)
        .await
        .unwrap_or_else(|_| ClientResponse::err("timeout"))
}

fn failure(st: &AppState, resp: &ClientResponse, key: &str) -> Response {
    let msg = resp.error.as_deref().unwrap_or("error");
    if let Some(addr) = resp.leader_hint.and_then(|id| st.http_peers.get(&id)) {
        let loc = format!("http://{addr}/kv/{key}");
        return (StatusCode::TEMPORARY_REDIRECT, [(header::LOCATION, loc)]).into_response();
    }
    let code = if msg == "timeout" {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(ErrorBody {
            error: msg,
            leader_hint: resp.leader_hint,
        }),
    )
        .into_response()
}

async fn get_key(State(st): State<AppState>, Path(key): Path<String>) -> Response {
    let resp = with_timeout(st.node.get(key.clone())).await;
    kv(&st, "get", &resp);
    if !resp.ok {
        return failure(&st, &resp, &key);
    }
    match resp.value {
        Some(v) => (StatusCode::OK, v).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn put_key(State(st): State<AppState>, Path(key): Path<String>, body: Bytes) -> Response {
    let Ok(value) = String::from_utf8(body.to_vec()) else {
        return (StatusCode::BAD_REQUEST, "value must be UTF-8").into_response();
    };
    let resp = with_timeout(st.node.put(key.clone(), value)).await;
    kv(&st, "put", &resp);
    if resp.ok {
        StatusCode::NO_CONTENT.into_response()
    } else {
        failure(&st, &resp, &key)
    }
}

async fn delete_key(State(st): State<AppState>, Path(key): Path<String>) -> Response {
    let resp = with_timeout(st.node.delete(key.clone())).await;
    kv(&st, "delete", &resp);
    if resp.ok {
        StatusCode::NO_CONTENT.into_response()
    } else {
        failure(&st, &resp, &key)
    }
}

async fn status(State(st): State<AppState>) -> Json<NodeStatus> {
    Json(st.node.status().await)
}

async fn metrics_text(State(st): State<AppState>) -> Response {
    crate::telemetry::record_snapshot(&st.node.metrics().await);
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::telemetry::render(),
    )
        .into_response()
}

fn kv(st: &AppState, op: &'static str, resp: &ClientResponse) {
    let result = if resp.ok {
        "ok"
    } else if resp.leader_hint.is_some() {
        "redirect"
    } else {
        "error"
    };
    crate::telemetry::record_kv(st.node.id, op, result);
}

async fn health(State(st): State<AppState>) -> Response {
    match tokio::time::timeout(Duration::from_secs(1), st.node.status()).await {
        Ok(s) if s.role != "unknown" => (StatusCode::OK, "ok").into_response(),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "node loop not responding").into_response(),
    }
}

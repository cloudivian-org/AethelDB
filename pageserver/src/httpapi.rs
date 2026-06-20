// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The HTTP/JSON control-plane API for the page server.
//!
//! A management interface over the same tenant operations as the line-oriented
//! [`crate::control`] endpoint, but speaking HTTP/JSON so a control plane (or
//! `curl`/an `aethelctl` CLI) can drive it. Routes (all under `/v1` except
//! health):
//!
//! * `GET  /healthz`                  — liveness.
//! * `GET  /v1/timelines`             — list timeline ids.
//! * `POST /v1/timelines`             — `{ "id": "<hex>" }` create a root timeline.
//! * `POST /v1/branches`              — `{ "timeline", "parent", "lsn" }` branch.
//! * `POST /v1/timelines/receive`     — `{ "timeline", "safekeeper", "start_lsn" }`
//!                                       attach a WAL receiver to a timeline.
//! * `POST /v1/gc`                    — `{ "horizon_lsn" }` compact + branch-aware GC.
//!
//! The HTTP is hand-rolled (one request per connection, `Connection: close`) to
//! keep the dependency footprint small and consistent with the rest of the
//! services; bodies are parsed with `serde_json`.

use std::net::SocketAddr;
use std::sync::Arc;

use common::{Lsn, TenantId, TimelineId};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

use crate::objstore::ObjectStore;
use crate::offload::layer_key;
use crate::tenant::Tenant;
use crate::walreceiver::{WalReceiver, WalReceiverConfig};

/// Serve the HTTP control-plane API on `listener`.
pub async fn serve_http_api(
    tenant: Arc<Tenant>,
    store: Option<Arc<dyn ObjectStore>>,
    listener: TcpListener,
) -> anyhow::Result<()> {
    loop {
        let (socket, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "http-api accept failed; continuing");
                continue;
            }
        };
        let tenant = tenant.clone();
        let store = store.clone();
        tokio::spawn(async move {
            if let Err(err) = handle(tenant, store, socket).await {
                warn!(error = %format!("{err:#}"), "http-api connection error");
            }
        });
    }
}

async fn handle(
    tenant: Arc<Tenant>,
    store: Option<Arc<dyn ObjectStore>>,
    mut socket: TcpStream,
) -> anyhow::Result<()> {
    let (method, path, body) = match read_request(&mut socket).await? {
        Some(req) => req,
        None => return Ok(()),
    };
    let (status, json) = route(&tenant, store.as_ref(), &method, &path, &body).await;
    let reason = if (200..300).contains(&status) { "OK" } else { "Error" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",
        json.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.flush().await?;
    Ok(())
}

// ---- Routing ----

#[derive(Deserialize)]
struct CreateBody {
    id: String,
}
#[derive(Deserialize)]
struct BranchBody {
    timeline: String,
    parent: String,
    lsn: u64,
}
#[derive(Deserialize)]
struct ReceiveBody {
    timeline: String,
    safekeeper: String,
    start_lsn: u64,
}
#[derive(Deserialize)]
struct GcBody {
    horizon_lsn: u64,
}
#[derive(Serialize)]
struct ErrorMsg {
    error: String,
}

fn err(status: u16, msg: impl Into<String>) -> (u16, String) {
    (status, serde_json::to_string(&ErrorMsg { error: msg.into() }).unwrap())
}

async fn route(
    tenant: &Arc<Tenant>,
    store: Option<&Arc<dyn ObjectStore>>,
    method: &str,
    path: &str,
    body: &[u8],
) -> (u16, String) {
    match (method, path) {
        ("GET", "/healthz") => (200, r#"{"status":"ok"}"#.to_string()),

        ("GET", "/v1/timelines") => {
            let mut ids: Vec<String> = tenant.timeline_ids().iter().map(|t| t.to_string()).collect();
            ids.sort();
            (200, serde_json::json!({ "timelines": ids }).to_string())
        }

        ("POST", "/v1/timelines") => {
            let b: CreateBody = match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, format!("invalid body: {e}")),
            };
            let id = match b.id.parse::<TimelineId>() {
                Ok(id) => id,
                Err(_) => return err(400, "id must be 32 hex chars"),
            };
            match tenant.create_timeline(id) {
                Ok(_) => (201, serde_json::json!({ "created": id.to_string() }).to_string()),
                Err(e) => err(409, e.to_string()),
            }
        }

        ("POST", "/v1/branches") => {
            let b: BranchBody = match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, format!("invalid body: {e}")),
            };
            let (Ok(new), Ok(parent)) = (b.timeline.parse::<TimelineId>(), b.parent.parse::<TimelineId>())
            else {
                return err(400, "timeline and parent must be 32 hex chars");
            };
            match tenant.branch_timeline(new, parent, Lsn(b.lsn)) {
                Ok(_) => (
                    201,
                    serde_json::json!({ "branched": new.to_string(), "parent": parent.to_string(), "lsn": b.lsn })
                        .to_string(),
                ),
                Err(e) => err(409, e.to_string()),
            }
        }

        ("POST", "/v1/timelines/receive") => {
            let b: ReceiveBody = match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, format!("invalid body: {e}")),
            };
            let Ok(timeline) = b.timeline.parse::<TimelineId>() else {
                return err(400, "timeline must be 32 hex chars");
            };
            let Ok(addr) = b.safekeeper.parse::<SocketAddr>() else {
                return err(400, "safekeeper must be host:port");
            };
            let Some(tl) = tenant.get_timeline(timeline) else {
                return err(404, format!("unknown timeline {timeline}"));
            };
            let cfg = WalReceiverConfig::new(addr, TenantId::ZERO, timeline, Lsn(b.start_lsn));
            match WalReceiver::connect(tl, cfg).await {
                Ok(receiver) => {
                    tokio::spawn(async move {
                        if let Err(e) = receiver.run().await {
                            warn!(error = %format!("{e:#}"), "http-api WAL receiver stopped");
                        }
                    });
                    (200, serde_json::json!({ "receiving": timeline.to_string(), "from": b.safekeeper }).to_string())
                }
                Err(e) => err(502, format!("connecting to safekeeper: {e:#}")),
            }
        }

        ("POST", "/v1/gc") => {
            let b: GcBody = match serde_json::from_slice(body) {
                Ok(b) => b,
                Err(e) => return err(400, format!("invalid body: {e}")),
            };
            let stats = tenant.gc(Lsn(b.horizon_lsn));
            let versions: usize = stats.iter().map(|(_, s)| s.versions_removed).sum();
            let mut objects_deleted = 0;
            if let Some(store) = store {
                for (_, s) in &stats {
                    for id in &s.removed_layer_ids {
                        if store.delete(&layer_key(*id)).await.is_ok() {
                            objects_deleted += 1;
                        }
                    }
                }
            }
            (
                200,
                serde_json::json!({
                    "horizon_lsn": b.horizon_lsn,
                    "timelines": stats.len(),
                    "versions_removed": versions,
                    "objects_deleted": objects_deleted,
                })
                .to_string(),
            )
        }

        _ => err(404, "not found"),
    }
}

// ---- Minimal HTTP request reader ----

/// Read one HTTP/1.1 request: returns `(method, path, body)`, or `None` on a
/// clean EOF before any bytes.
async fn read_request(socket: &mut TcpStream) -> anyhow::Result<Option<(String, String, Vec<u8>)>> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];

    // Read until we have the header terminator.
    let header_end = loop {
        if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return if buf.is_empty() { Ok(None) } else { Err(anyhow::anyhow!("incomplete request")) };
        }
        buf.extend_from_slice(&chunk[..n]);
        anyhow::ensure!(buf.len() < (1 << 20), "request headers too large");
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    // Find Content-Length (case-insensitive).
    let content_length = lines
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            if k.trim().eq_ignore_ascii_case("content-length") {
                v.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);

    // Read the rest of the body.
    let body_start = header_end + 4;
    let mut body = buf[body_start..].to_vec();
    while body.len() < content_length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok(Some((method, path, body)))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

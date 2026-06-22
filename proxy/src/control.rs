// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A tiny HTTP control API for compute lifecycle.
//!
//! The proxy is the component that knows whether a tenant's compute is **running**
//! or **hibernated** (scaled to zero) and can start/stop it via the
//! [`Activator`](crate::activator::Activator). This endpoint exposes that so a
//! console or CLI can show per-database state and start / hibernate on demand —
//! the same machinery the idle reaper uses automatically.
//!
//! Routes (hand-rolled HTTP/JSON, one request per connection):
//! * `GET    /healthz`
//! * `GET    /tenants`                  — `[{tenant, running, connections}]`
//! * `POST   /tenants/<name>`           — **register** a route (automatic routing)
//! * `DELETE /tenants/<name>`           — deregister a route
//! * `POST   /tenants/<name>/start`     — wake compute
//! * `POST   /tenants/<name>/stop`      — hibernate (scale to zero)

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::proxy::Proxy;
use crate::tenant::TenantState;

/// Serve the compute-control API on `listener`. `backend_template` is how a
/// newly-registered tenant's backend address is derived (`{tenant}` is
/// substituted) — the basis for routing a database without manual config.
pub async fn serve_control(
    proxy: Arc<Proxy>,
    backend_template: String,
    listener: TcpListener,
) -> anyhow::Result<()> {
    let backend_template = Arc::new(backend_template);
    loop {
        let (socket, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "control accept failed; continuing");
                continue;
            }
        };
        let proxy = proxy.clone();
        let tmpl = backend_template.clone();
        tokio::spawn(async move {
            if let Err(err) = handle(proxy, tmpl, socket).await {
                warn!(error = %format!("{err:#}"), "control connection error");
            }
        });
    }
}

async fn handle(proxy: Arc<Proxy>, tmpl: Arc<String>, mut socket: TcpStream) -> anyhow::Result<()> {
    // Read the header block (these requests carry no body).
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    loop {
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        anyhow::ensure!(buf.len() < 8192, "request too large");
    }
    let text = String::from_utf8_lossy(&buf);
    let line = text.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");

    let (status, body) = route(&proxy, &tmpl, method, path).await;
    let reason = if (200..300).contains(&status) { "OK" } else { "Error" };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.flush().await.ok();
    Ok(())
}

async fn route(proxy: &Arc<Proxy>, tmpl: &str, method: &str, path: &str) -> (u16, String) {
    let segs: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    match (method, segs.first().copied(), segs.get(1).copied(), segs.get(2).copied()) {
        ("GET", Some("healthz"), None, None) => (200, r#"{"status":"ok"}"#.to_string()),
        ("GET", Some("tenants"), None, None) => (200, list_tenants(proxy)),
        ("POST", Some("tenants"), Some(name), None) => register(proxy, tmpl, name),
        ("DELETE", Some("tenants"), Some(name), None) => deregister(proxy, name),
        ("POST", Some("tenants"), Some(name), Some(action))
            if action == "start" || action == "stop" =>
        {
            set_compute(proxy, name, action == "start").await
        }
        _ => (404, r#"{"error":"not found"}"#.to_string()),
    }
}

fn list_tenants(proxy: &Arc<Proxy>) -> String {
    let mut items: Vec<String> = proxy
        .registry()
        .tenants()
        .into_iter()
        .map(|(name, state)| {
            format!(
                r#"{{"tenant":{},"running":{},"connections":{}}}"#,
                json_str(&name),
                state.is_running(),
                state.active_conns()
            )
        })
        .collect();
    items.sort();
    format!(r#"{{"tenants":[{}]}}"#, items.join(","))
}

/// Register a tenant route, deriving its backend from the template. Idempotent.
fn register(proxy: &Arc<Proxy>, tmpl: &str, name: &str) -> (u16, String) {
    let backend = tmpl.replace("{tenant}", name);
    match backend.parse::<SocketAddr>() {
        Ok(addr) => {
            // Start "asleep" so the first connection wakes compute via the activator.
            proxy.registry().register(name, TenantState::new(addr, false));
            debug!(tenant = name, %addr, "registered tenant route");
            (200, format!(r#"{{"tenant":{},"backend":{}}}"#, json_str(name), json_str(&backend)))
        }
        Err(_) => (
            400,
            format!(
                r#"{{"error":{}}}"#,
                json_str(&format!("backend {backend:?} is not host:port"))
            ),
        ),
    }
}

/// Deregister a tenant route.
fn deregister(proxy: &Arc<Proxy>, name: &str) -> (u16, String) {
    let removed = proxy.registry().remove(name);
    debug!(tenant = name, removed, "deregistered tenant route");
    (200, format!(r#"{{"tenant":{},"removed":{}}}"#, json_str(name), removed))
}

/// Start or hibernate a tenant's compute via the activator, updating its state.
async fn set_compute(proxy: &Arc<Proxy>, name: &str, start: bool) -> (u16, String) {
    let Some(state) = proxy.registry().get(name) else {
        return (404, format!(r#"{{"error":"unknown tenant {name}"}}"#));
    };
    let result = if start {
        proxy.activator().start(name).await
    } else {
        proxy.activator().stop(name).await
    };
    match result {
        Ok(()) => {
            state.set_running(start);
            if start {
                state.touch();
            }
            debug!(tenant = name, running = start, "compute state changed via control API");
            (200, format!(r#"{{"tenant":{},"running":{}}}"#, json_str(name), start))
        }
        Err(e) => (502, format!(r#"{{"error":{}}}"#, json_str(&format!("{e:#}")))),
    }
}

/// Minimal JSON string escaping (quotes + backslashes).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_str_escapes() {
        assert_eq!(json_str("a\"b\\c"), r#""a\"b\\c""#);
    }
}

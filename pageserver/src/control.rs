// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A small control endpoint for branch management.
//!
//! Branching is a control-plane operation, not part of the hot page/WAL path, so
//! it gets a simple line-oriented text protocol: one command per line, one
//! reply line back. Commands:
//!
//! * `auth <token>` — authenticate the connection when the server is started
//!   with a control token; required before any other command.
//! * `tenants` — list known tenant ids.
//! * `tenant <tenant-hex>` — switch the connection's current tenant (creating it
//!   on first use); subsequent commands act on it. Defaults to `TenantId::ZERO`.
//! * `create <timeline-hex>` — create a fresh root timeline.
//! * `branch <new-hex> <parent-hex> <lsn>` — branch `new` off `parent` at `lsn`.
//! * `list` — list known timeline ids.
//!
//! Replies are `ok …` or `err …`. This is the seam a real control plane (or a
//! `aethelctl` CLI) drives; it is intentionally tiny and human-typable.

use std::net::SocketAddr;
use std::sync::Arc;

use common::{Lsn, TenantId, TimelineId};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::objstore::ObjectStore;
use crate::offload::layer_key;
use crate::tenant::Tenant;
use crate::tenant_manager::TenantManager;
use crate::walreceiver::{WalReceiver, WalReceiverConfig};

/// Serve the control endpoint: branch-management commands. When `store` is set,
/// `gc` also deletes the compacted-away layer objects from it.
///
/// Each connection operates on a *current tenant*, defaulting to
/// [`TenantId::ZERO`]; `tenant <hex>` switches it (provisioning on first use) and
/// `tenants` lists known tenant ids. Every other command (`create` / `branch` /
/// `receive` / `gc` / `list`) acts on the current tenant.
pub async fn serve_control(
    tenants: Arc<TenantManager>,
    store: Option<Arc<dyn ObjectStore>>,
    listener: TcpListener,
    token: Option<Arc<str>>,
) -> anyhow::Result<()> {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(err) => {
                warn!(error = %err, "control accept failed; continuing");
                continue;
            }
        };
        let tenants = tenants.clone();
        let store = store.clone();
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_conn(tenants, store, token, socket).await {
                warn!(%peer, error = %format!("{err:#}"), "control connection error");
            }
        });
    }
}

async fn handle_conn(
    tenants: Arc<TenantManager>,
    store: Option<Arc<dyn ObjectStore>>,
    token: Option<Arc<str>>,
    socket: TcpStream,
) -> anyhow::Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut lines = BufReader::new(read_half).lines();

    // The connection's current tenant; defaults to the root tenant.
    let mut tenant_id = TenantId::ZERO;
    let mut tenant = tenants.get_or_create(tenant_id);

    // When a token is configured, the connection must `auth <token>` before any
    // other command is accepted.
    let mut authed = token.is_none();

    while let Some(line) = lines.next_line().await? {
        // Auth gate: until authenticated, only `auth <token>` is allowed.
        if !authed {
            let mut p = line.split_whitespace();
            let reply = match (p.next(), p.next()) {
                (Some("auth"), Some(tok)) if token.as_deref() == Some(tok) => {
                    authed = true;
                    "ok authenticated".to_string()
                }
                (Some("auth"), _) => {
                    crate::metrics::CONTROL_AUTH_FAILURES.inc();
                    "err invalid token".to_string()
                }
                _ => {
                    crate::metrics::CONTROL_AUTH_FAILURES.inc();
                    "err unauthorized: send 'auth <token>' first".to_string()
                }
            };
            write_half.write_all(reply.as_bytes()).await?;
            write_half.write_all(b"\n").await?;
            write_half.flush().await?;
            continue;
        }

        let reply = match line.split_whitespace().next() {
            // Tenant selection / listing.
            Some("tenants") => {
                let mut ids: Vec<String> =
                    tenants.tenant_ids().iter().map(|t| t.to_string()).collect();
                ids.sort();
                format!("ok {}", ids.join(" "))
            }
            Some("tenant") => match line.split_whitespace().nth(1).map(|s| s.parse::<TenantId>()) {
                Some(Ok(id)) => {
                    tenant_id = id;
                    tenant = tenants.get_or_create(id);
                    format!("ok tenant {id}")
                }
                _ => "err usage: tenant <tenant-hex>".to_string(),
            },
            // `gc` and `receive` are async; the rest are pure and handled by `exec`.
            Some("gc") => gc_command(&tenant, store.as_ref(), &line).await,
            Some("receive") => receive_command(&tenant, tenant_id, &line).await,
            _ => exec(&tenant, &line),
        };
        // Persist the topology after a successful create/branch/tenant op.
        let token = line.split_whitespace().next().unwrap_or("");
        if matches!(token, "create" | "branch" | "tenant") && reply.starts_with("ok") {
            tenants.persist().await;
        }
        debug!(%line, %reply, "control command");
        write_half.write_all(reply.as_bytes()).await?;
        write_half.write_all(b"\n").await?;
        write_half.flush().await?;
    }
    Ok(())
}

/// Run a branch-aware GC and, if an object store is configured, delete the
/// compacted-away layer files from it.
async fn gc_command(
    tenant: &Arc<Tenant>,
    store: Option<&Arc<dyn ObjectStore>>,
    line: &str,
) -> String {
    let horizon = match line.split_whitespace().nth(1).and_then(|s| s.parse::<u64>().ok()) {
        Some(h) => h,
        None => return "err usage: gc <horizon-lsn>".to_string(),
    };
    let stats = tenant.gc(Lsn(horizon));
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
    format!(
        "ok gc @ {horizon}: {} timelines, {versions} versions removed, {objects_deleted} objects deleted",
        stats.len()
    )
}

/// Attach a WAL receiver to a timeline, streaming committed WAL from a
/// safekeeper into that branch. This is what makes a branch ingestible over the
/// network (each timeline can stream from its own safekeeper position).
async fn receive_command(tenant: &Arc<Tenant>, tenant_id: TenantId, line: &str) -> String {
    let mut parts = line.split_whitespace();
    parts.next(); // "receive"
    let timeline = parts.next().and_then(|s| s.parse::<TimelineId>().ok());
    let addr = parts.next().and_then(|s| s.parse::<SocketAddr>().ok());
    let start_lsn = parts.next().and_then(|s| s.parse::<u64>().ok());
    let (timeline, addr, start_lsn) = match (timeline, addr, start_lsn) {
        (Some(t), Some(a), Some(l)) => (t, a, l),
        _ => {
            return "err usage: receive <timeline-hex> <safekeeper-host:port> <start-lsn>"
                .to_string()
        }
    };

    let Some(tl) = tenant.get_timeline(timeline) else {
        return format!("err unknown timeline {timeline}");
    };
    let cfg = WalReceiverConfig::new(addr, tenant_id, timeline, Lsn(start_lsn));
    match WalReceiver::connect(tl, cfg).await {
        Ok(receiver) => {
            tokio::spawn(async move {
                if let Err(e) = receiver.run().await {
                    warn!(error = %format!("{e:#}"), "per-branch WAL receiver stopped");
                }
            });
            format!("ok receiving {timeline} from {addr} @ {start_lsn}")
        }
        Err(e) => format!("err connecting to safekeeper: {e:#}"),
    }
}

/// Execute one command line against the tenant, returning the reply line.
fn exec(tenant: &Arc<Tenant>, line: &str) -> String {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("create") => match parts.next().and_then(|s| s.parse::<TimelineId>().ok()) {
            Some(id) => match tenant.create_timeline(id) {
                Ok(_) => format!("ok created {id}"),
                Err(e) => format!("err {e}"),
            },
            None => "err usage: create <timeline-hex>".to_string(),
        },
        Some("branch") => {
            let new = parts.next().and_then(|s| s.parse::<TimelineId>().ok());
            let parent = parts.next().and_then(|s| s.parse::<TimelineId>().ok());
            let lsn = parts.next().and_then(|s| s.parse::<u64>().ok());
            match (new, parent, lsn) {
                (Some(n), Some(p), Some(l)) => match tenant.branch_timeline(n, p, Lsn(l)) {
                    Ok(_) => format!("ok branched {n} from {p} @ {l}"),
                    Err(e) => format!("err {e}"),
                },
                _ => "err usage: branch <new-hex> <parent-hex> <lsn>".to_string(),
            }
        }
        Some("list") => {
            let mut ids: Vec<String> =
                tenant.timeline_ids().iter().map(|i| i.to_string()).collect();
            ids.sort();
            format!("ok {}", ids.join(" "))
        }
        Some("gc") => match parts.next().and_then(|s| s.parse::<u64>().ok()) {
            Some(horizon) => {
                let stats = tenant.gc(Lsn(horizon));
                let removed: usize = stats.iter().map(|(_, s)| s.versions_removed).sum();
                format!("ok gc @ {horizon}: {} timelines, {removed} versions removed", stats.len())
            }
            None => "err usage: gc <horizon-lsn>".to_string(),
        },
        Some(other) => format!("err unknown command '{other}'"),
        None => "err empty command".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> TimelineId {
        TimelineId::from_bytes([n; 16])
    }

    #[test]
    fn create_branch_and_list() {
        let tenant = Tenant::new(1_000);
        let main = id(1).to_string();
        let dev = id(2).to_string();

        assert!(exec(&tenant, &format!("create {main}")).starts_with("ok created"));
        // Re-creating the same timeline is an error.
        assert!(exec(&tenant, &format!("create {main}")).starts_with("err"));

        // Branch dev off main at LSN 100.
        assert!(exec(&tenant, &format!("branch {dev} {main} 100")).starts_with("ok branched"));
        // Branching off a missing parent fails.
        assert!(exec(&tenant, &format!("branch {} {} 1", id(3), id(9))).starts_with("err"));

        let listed = exec(&tenant, "list");
        assert!(listed.contains(&main) && listed.contains(&dev));

        // GC is reachable over the control endpoint.
        assert!(exec(&tenant, "gc 50").starts_with("ok gc"));

        // The branch really exists in the tenant.
        let branch = tenant.get_timeline(id(2)).expect("branch created");
        assert_eq!(branch.ancestor_timeline(), Some(id(1)));
        assert_eq!(branch.ancestor_lsn(), Some(Lsn(100)));
    }

    #[test]
    fn malformed_commands_are_rejected() {
        let tenant = Tenant::new(1_000);
        assert!(exec(&tenant, "create").starts_with("err usage"));
        assert!(exec(&tenant, "branch only-one-arg").starts_with("err usage"));
        assert!(exec(&tenant, "create not-hex").starts_with("err usage"));
        assert!(exec(&tenant, "gc not-a-number").starts_with("err usage"));
        assert!(exec(&tenant, "frobnicate").starts_with("err unknown"));
        assert_eq!(exec(&tenant, ""), "err empty command");
    }
}

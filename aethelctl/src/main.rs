// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! `aethelctl` — the AethelDB command-line interface.
//!
//! A thin, scriptable wrapper over the page-server control plane: manage
//! tenants, timelines (branches), point-in-time recovery, WAL receivers, and
//! GC. Talks to the same HTTP/JSON API documented in the README; honors a
//! control token via `--token` / `AETHEL_TOKEN`.

use anyhow::Result;
use clap::{Parser, Subcommand};

use aethelctl::{Client, DEFAULT_SERVER};

#[derive(Parser)]
#[command(name = "aethelctl", version, about = "AethelDB control-plane CLI")]
struct Cli {
    /// Page-server control-plane URL.
    #[arg(long, env = "AETHEL_SERVER", default_value = DEFAULT_SERVER, global = true)]
    server: String,

    /// Bearer token (when the control plane requires auth).
    #[arg(long, env = "AETHEL_TOKEN", global = true)]
    token: Option<String>,

    /// Print raw JSON responses instead of human-readable summaries.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show control-plane health and a tenant/timeline summary.
    Status,
    /// Manage tenants.
    #[command(subcommand)]
    Tenant(TenantCmd),
    /// Manage timelines (branches).
    #[command(subcommand)]
    Timeline(TimelineCmd),
    /// Branch a timeline at an LSN — the point-in-time recovery primitive.
    #[command(alias = "pitr")]
    Branch {
        /// New timeline id (32 hex chars).
        new: String,
        /// Parent timeline id to branch from.
        #[arg(long)]
        from: String,
        /// LSN to branch at (the recovery / divergence point).
        #[arg(long)]
        lsn: u64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Attach a WAL receiver to a timeline, streaming from a safekeeper.
    Receive {
        timeline: String,
        #[arg(long)]
        safekeeper: String,
        #[arg(long, default_value_t = 0)]
        start_lsn: u64,
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Run compaction + branch-aware GC below an LSN horizon.
    Gc {
        horizon_lsn: u64,
        #[arg(long)]
        tenant: Option<String>,
    },
}

#[derive(Subcommand)]
enum TenantCmd {
    /// List tenant ids.
    List,
    /// Create a tenant.
    Create { id: String },
}

#[derive(Subcommand)]
enum TimelineCmd {
    /// List timeline ids (optionally for a tenant).
    List {
        #[arg(long)]
        tenant: Option<String>,
    },
    /// Create a root timeline (optionally in a tenant).
    Create {
        id: String,
        #[arg(long)]
        tenant: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new(cli.server.clone(), cli.token.clone());
    let json = cli.json;

    match cli.command {
        Command::Status => {
            client.healthz()?;
            let tenants = client.list_tenants()?;
            let timelines = client.list_timelines(None)?;
            if json {
                print(
                    true,
                    &serde_json::json!({ "tenants": tenants, "root_timelines": timelines }),
                );
            } else {
                println!("control plane: ok ({})", cli.server);
                println!("tenants ({}): {}", tenants.len(), tenants.join(" "));
                println!("root-tenant timelines ({}): {}", timelines.len(), timelines.join(" "));
            }
        }

        Command::Tenant(TenantCmd::List) => {
            let tenants = client.list_tenants()?;
            if json {
                print(true, &serde_json::json!({ "tenants": tenants }));
            } else {
                for t in tenants {
                    println!("{t}");
                }
            }
        }
        Command::Tenant(TenantCmd::Create { id }) => {
            let v = client.create_tenant(&id)?;
            report(json, &v, format!("created tenant {id}"));
        }

        Command::Timeline(TimelineCmd::List { tenant }) => {
            let timelines = client.list_timelines(tenant.as_deref())?;
            if json {
                print(true, &serde_json::json!({ "timelines": timelines }));
            } else {
                for t in timelines {
                    println!("{t}");
                }
            }
        }
        Command::Timeline(TimelineCmd::Create { id, tenant }) => {
            let v = client.create_timeline(&id, tenant.as_deref())?;
            report(json, &v, format!("created timeline {id}"));
        }

        Command::Branch { new, from, lsn, tenant } => {
            let v = client.branch(&new, &from, lsn, tenant.as_deref())?;
            report(json, &v, format!("branched {new} from {from} @ {lsn}"));
        }

        Command::Receive { timeline, safekeeper, start_lsn, tenant } => {
            let v = client.receive(&timeline, &safekeeper, start_lsn, tenant.as_deref())?;
            report(json, &v, format!("receiving {timeline} from {safekeeper} @ {start_lsn}"));
        }

        Command::Gc { horizon_lsn, tenant } => {
            let v = client.gc(horizon_lsn, tenant.as_deref())?;
            if json {
                print(true, &v);
            } else {
                let removed = v.get("versions_removed").and_then(|n| n.as_u64()).unwrap_or(0);
                let tls = v.get("timelines").and_then(|n| n.as_u64()).unwrap_or(0);
                let objs = v.get("objects_deleted").and_then(|n| n.as_u64()).unwrap_or(0);
                println!(
                    "gc @ {horizon_lsn}: {tls} timelines, {removed} versions removed, {objs} objects deleted"
                );
            }
        }
    }
    Ok(())
}

/// Print a success line, or the raw JSON when `--json` is set.
fn report(json: bool, v: &serde_json::Value, human: String) {
    if json {
        print(true, v);
    } else {
        println!("{human}");
    }
}

fn print(pretty: bool, v: &serde_json::Value) {
    if pretty {
        println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
    }
}

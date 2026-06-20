// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # aethel-safekeeper — durable WAL ingest buffer (binary)
//!
//! Wires the [`safekeeper`] library to a CLI: opens the durable WAL store,
//! builds consensus state for this node's group, and serves the ingest port.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;

use safekeeper::consensus::{Consensus, NodeId};
use safekeeper::replicator::{LocalSimReplicator, Replicator};
use safekeeper::server::{self, serve, Safekeeper};
use safekeeper::storage::{WalConfig, WalStorage};

/// Command-line / environment configuration for a safekeeper node.
#[derive(Debug, Parser)]
#[command(name = "aethel-safekeeper", version, about = "AethelDB WAL safekeeper")]
struct Args {
    /// Address to accept WAL streams from compute on.
    #[arg(long, env = "SP_SK_LISTEN", default_value = "0.0.0.0:6500")]
    listen: SocketAddr,

    /// Directory backing the durable WAL store.
    #[arg(long, env = "SP_SK_DATA_DIR", default_value = ".data/safekeeper")]
    data_dir: PathBuf,

    /// This node's id within the consensus group.
    #[arg(long, env = "SP_SK_NODE_ID", default_value_t = 1)]
    node_id: NodeId,

    /// All member ids of the consensus group, comma-separated (must include this
    /// node). Defaults to a single-node group of just this node.
    #[arg(long, env = "SP_SK_MEMBERS", value_delimiter = ',')]
    members: Vec<NodeId>,

    /// Segment file size in bytes.
    #[arg(long, env = "SP_SK_SEGMENT_SIZE", default_value_t = 16 * 1024 * 1024)]
    segment_size: u64,

    /// In-memory ring cache capacity in bytes.
    #[arg(long, env = "SP_SK_RING_BYTES", default_value_t = 8 * 1024 * 1024)]
    ring_bytes: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    // Default to a single-node group containing just this node.
    let members = if args.members.is_empty() { vec![args.node_id] } else { args.members.clone() };
    anyhow::ensure!(
        members.contains(&args.node_id),
        "members ({members:?}) must include this node's id ({})",
        args.node_id
    );

    let storage = WalStorage::open(WalConfig {
        data_dir: args.data_dir.clone(),
        segment_size: args.segment_size,
        ring_capacity: args.ring_bytes,
    })
    .with_context(|| format!("opening WAL store at {}", args.data_dir.display()))?;

    let consensus = Consensus::new(args.node_id, members.clone());
    let quorum = consensus.quorum();

    // Peers = all members except ourselves; simulated as instantly durable.
    let peers: Vec<NodeId> = members.into_iter().filter(|&m| m != args.node_id).collect();
    let replicator: Arc<dyn Replicator> = Arc::new(LocalSimReplicator::new(peers));

    let sk = Safekeeper::new(storage, consensus, replicator);

    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    server::log_started(args.listen, args.node_id, quorum);
    serve(sk, listener).await
}

/// Configure structured logging. Honors `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}

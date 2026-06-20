// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # aethel-pageserver — log-structured page storage (binary)
//!
//! Wires the [`pageserver`] library to a CLI: builds the repository and object
//! store, starts the offload worker, and serves the page-service and ingest
//! endpoints.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use pageserver::objstore::{LocalObjectStore, ObjectStore};
use pageserver::offload;
use pageserver::repository::Repository;
use pageserver::server::{serve_ingest, serve_pages};

/// Command-line / environment configuration for the page server.
#[derive(Debug, Parser)]
#[command(name = "aethel-pageserver", version, about = "AethelDB page server")]
struct Args {
    /// Address to serve `GetPage`/`GetRelSize` requests from compute on.
    #[arg(long, env = "SP_PS_LISTEN", default_value = "0.0.0.0:6400")]
    listen: SocketAddr,

    /// Address to accept WAL modifications (from the WAL decoder) on.
    #[arg(long, env = "SP_PS_INGEST_LISTEN", default_value = "0.0.0.0:6401")]
    ingest_listen: SocketAddr,

    /// Local directory used as the (mock MinIO/S3) object store for layers.
    #[arg(long, env = "SP_PS_OBJECT_DIR", default_value = ".data/pageserver/objstore")]
    object_dir: PathBuf,

    /// Freeze the memtable into a layer every N versions.
    #[arg(long, env = "SP_PS_FREEZE_THRESHOLD", default_value_t = 100_000)]
    freeze_threshold: usize,

    /// How often the offload worker scans for layers to upload, in seconds.
    #[arg(long, env = "SP_PS_OFFLOAD_TICK_SECS", default_value_t = 10)]
    offload_tick_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    let repo = Repository::new(args.freeze_threshold);
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalObjectStore::new(&args.object_dir)
            .with_context(|| format!("opening object store at {}", args.object_dir.display()))?,
    );

    info!(
        listen = %args.listen,
        ingest = %args.ingest_listen,
        object_dir = %args.object_dir.display(),
        "starting aethel-pageserver"
    );

    // Background layer offload.
    tokio::spawn(offload::run(repo.clone(), store.clone(), Duration::from_secs(args.offload_tick_secs)));

    // Ingest endpoint.
    let ingest_listener = TcpListener::bind(args.ingest_listen)
        .await
        .with_context(|| format!("failed to bind ingest {}", args.ingest_listen))?;
    tokio::spawn(serve_ingest(repo.clone(), ingest_listener));
    info!(addr = %args.ingest_listen, "accepting WAL modifications");

    // Page-service endpoint (runs on the main task).
    let page_listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    info!(addr = %args.listen, "ready to serve pages");
    serve_pages(repo, page_listener).await
}

/// Configure structured logging. Honors `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}

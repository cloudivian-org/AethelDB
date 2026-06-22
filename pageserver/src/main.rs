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
use common::{Lsn, TenantId, TimelineId};
use tokio::net::TcpListener;
use tracing::{info, warn};

use pageserver::control::serve_control;
use pageserver::objstore::{LocalObjectStore, ObjectStore};
use pageserver::offload;
use pageserver::server::{serve_ingest, serve_pages};
use pageserver::tenant_manager::TenantManager;
use pageserver::walreceiver::{WalReceiver, WalReceiverConfig};

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

    /// Optional safekeeper address to stream committed WAL from. When set, the
    /// page server pulls and decodes WAL itself (Phase 4) instead of only
    /// accepting pushed `Modification`s on the ingest endpoint.
    #[arg(long, env = "SP_PS_SAFEKEEPER")]
    safekeeper: Option<SocketAddr>,

    /// LSN to begin streaming WAL from (used with `--safekeeper`).
    #[arg(long, env = "SP_PS_WAL_START_LSN", default_value_t = 0)]
    wal_start_lsn: u64,

    /// Local directory used as the object store for layers when no S3 endpoint
    /// is configured.
    #[arg(long, env = "SP_PS_OBJECT_DIR", default_value = ".data/pageserver/objstore")]
    object_dir: PathBuf,

    /// Object-store URL for layer offload — deploy to any cloud with one binary:
    /// `s3://bucket` (AWS S3), `az://container` (Azure Blob), or `gs://bucket`
    /// (Google Cloud Storage). Credentials come from the standard per-cloud
    /// environment variables (`AWS_*`, `AZURE_STORAGE_*`,
    /// `GOOGLE_APPLICATION_CREDENTIALS`). Takes precedence over the local dir;
    /// the explicit `--s3-endpoint` flags below remain for MinIO/keyed S3.
    #[arg(long, env = "SP_PS_OBJECT_STORE_URL")]
    object_store_url: Option<String>,

    /// S3-compatible endpoint (e.g. `http://localhost:9000` for MinIO). When set,
    /// layers are offloaded to S3 instead of the local object directory.
    #[arg(long, env = "SP_PS_S3_ENDPOINT", requires = "s3_bucket")]
    s3_endpoint: Option<String>,
    /// S3 bucket for offloaded layers (used with --s3-endpoint).
    #[arg(long, env = "SP_PS_S3_BUCKET")]
    s3_bucket: Option<String>,
    /// S3 region (used with --s3-endpoint).
    #[arg(long, env = "SP_PS_S3_REGION", default_value = "us-east-1")]
    s3_region: String,
    /// S3 access key id (used with --s3-endpoint).
    #[arg(long, env = "SP_PS_S3_ACCESS_KEY", default_value = "minioadmin")]
    s3_access_key: String,
    /// S3 secret access key (used with --s3-endpoint).
    #[arg(long, env = "SP_PS_S3_SECRET_KEY", default_value = "minioadmin")]
    s3_secret_key: String,

    /// Freeze the memtable into a layer every N versions.
    #[arg(long, env = "SP_PS_FREEZE_THRESHOLD", default_value_t = 100_000)]
    freeze_threshold: usize,

    /// How often the offload worker scans for layers to upload, in seconds.
    #[arg(long, env = "SP_PS_OFFLOAD_TICK_SECS", default_value_t = 10)]
    offload_tick_secs: u64,

    /// Address for the Prometheus `/metrics` endpoint.
    #[arg(long, env = "SP_PS_METRICS_LISTEN", default_value = "0.0.0.0:9400")]
    metrics_listen: SocketAddr,

    /// Address for the line-oriented branch-management control endpoint.
    #[arg(long, env = "SP_PS_CONTROL_LISTEN", default_value = "0.0.0.0:6402")]
    control_listen: SocketAddr,

    /// Address for the HTTP/JSON control-plane API.
    #[arg(long, env = "SP_PS_HTTP_LISTEN", default_value = "0.0.0.0:6403")]
    http_listen: SocketAddr,

    /// Path to a `postgres` binary built with the `--wal-redo` patch. When set,
    /// the page server applies non-full-page WAL records through a real Postgres
    /// wal-redo process instead of the native (FPI-only) backend.
    #[arg(long, env = "SP_PS_WAL_REDO")]
    wal_redo: Option<PathBuf>,

    /// Data directory for the wal-redo Postgres process (an initdb'd cluster).
    #[arg(long, env = "SP_PS_WAL_REDO_DATADIR", default_value = ".data/pageserver/walredo")]
    wal_redo_datadir: PathBuf,

    /// Database name the wal-redo process connects to.
    #[arg(long, env = "SP_PS_WAL_REDO_DB", default_value = "postgres")]
    wal_redo_db: String,

    /// Shared secret protecting the control plane. When set, the line control
    /// endpoint requires `auth <token>` and the HTTP API requires an
    /// `Authorization: Bearer <token>` header (`/healthz` stays open). Unset =
    /// unauthenticated (keep the endpoints on an internal network).
    #[arg(long, env = "SP_PS_CONTROL_TOKEN")]
    control_token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    // Object store selection (most specific first):
    //  1. --object-store-url s3://|az://|gs://  — any cloud, env-resolved creds.
    //  2. --s3-endpoint + --s3-bucket           — MinIO / keyed S3 (unchanged).
    //  3. --object-dir                          — local directory (default).
    let store: Arc<dyn ObjectStore> = if let Some(url) = &args.object_store_url {
        info!(%url, "offloading layers to cloud object storage");
        Arc::new(
            pageserver::objstore::CloudObjectStore::from_url(url)
                .with_context(|| format!("connecting to object store {url}"))?,
        )
    } else {
        match (&args.s3_endpoint, &args.s3_bucket) {
            (Some(endpoint), Some(bucket)) => {
                info!(%endpoint, %bucket, "offloading layers to S3");
                Arc::new(
                    pageserver::objstore::S3ObjectStore::new(
                        endpoint,
                        bucket,
                        &args.s3_region,
                        &args.s3_access_key,
                        &args.s3_secret_key,
                    )
                    .context("connecting to S3 object store")?,
                )
            }
            _ => Arc::new(LocalObjectStore::new(&args.object_dir).with_context(|| {
                format!("opening object store at {}", args.object_dir.display())
            })?),
        }
    };

    // Select the WAL-redo backend: a real Postgres wal-redo process if one is
    // configured, else the native (FPI-only) backend. It is stateless and shared
    // across every tenant the manager provisions.
    let redo: Option<Arc<dyn pageserver::WalRedoManager>> = match &args.wal_redo {
        Some(postgres) => {
            let datadir = args.wal_redo_datadir.to_string_lossy().into_owned();
            let redo_args =
                vec!["--wal-redo".to_string(), "-D".to_string(), datadir, args.wal_redo_db.clone()];
            info!(postgres = %postgres.display(), "using the Postgres wal-redo backend");
            Some(Arc::new(pageserver::PostgresRedoManager::new(postgres, redo_args)))
        }
        None => None,
    };

    // The page server hosts many tenants; reads and control ops route by id.
    // The topology (tenants, timelines, branch ancestry) is persisted to the
    // object store and restored here, so it survives a restart.
    let tenants = TenantManager::with_catalog(args.freeze_threshold, redo, store.clone());
    tenants.load_persisted().await;

    // Pre-provision the root tenant and its root timeline so the default
    // single-tenant path (and the legacy ingest endpoint) works out of the box.
    let tenant = tenants.get_or_create(TenantId::ZERO);
    let root = match tenant.get_timeline(TimelineId::ZERO) {
        Some(tl) => tl,
        None => tenant.create_timeline(TimelineId::ZERO).context("creating root timeline")?,
    };
    tenants.persist().await; // capture the root tenant/timeline if freshly created

    info!(
        listen = %args.listen,
        ingest = %args.ingest_listen,
        control = %args.control_listen,
        object_dir = %args.object_dir.display(),
        "starting aethel-pageserver"
    );

    // Prometheus metrics endpoint.
    let metrics_listener = TcpListener::bind(args.metrics_listen)
        .await
        .with_context(|| format!("failed to bind metrics {}", args.metrics_listen))?;
    tokio::spawn(common::metrics::serve_metrics(metrics_listener));
    info!(addr = %args.metrics_listen, "serving Prometheus metrics");

    // Background layer offload (across every timeline of every tenant).
    tokio::spawn(offload::run(
        tenants.clone(),
        store.clone(),
        Duration::from_secs(args.offload_tick_secs),
    ));

    // Ingest endpoint (legacy push path) targets the root timeline.
    let ingest_listener = TcpListener::bind(args.ingest_listen)
        .await
        .with_context(|| format!("failed to bind ingest {}", args.ingest_listen))?;
    tokio::spawn(serve_ingest(root.clone(), ingest_listener));
    info!(addr = %args.ingest_listen, "accepting WAL modifications");

    // Branch-management control endpoint.
    let control_listener = TcpListener::bind(args.control_listen)
        .await
        .with_context(|| format!("failed to bind control {}", args.control_listen))?;
    let control_token: Option<Arc<str>> = args.control_token.as_deref().map(Arc::from);
    if control_token.is_some() {
        info!("control plane requires authentication (token configured)");
    }
    tokio::spawn(serve_control(
        tenants.clone(),
        Some(store.clone()),
        control_listener,
        control_token.clone(),
    ));
    info!(addr = %args.control_listen, "branch control endpoint ready");

    // HTTP/JSON control-plane API.
    let http_listener = TcpListener::bind(args.http_listen)
        .await
        .with_context(|| format!("failed to bind http api {}", args.http_listen))?;
    tokio::spawn(pageserver::serve_http_api(
        tenants.clone(),
        Some(store.clone()),
        http_listener,
        control_token.clone(),
    ));
    info!(addr = %args.http_listen, "HTTP control-plane API ready");

    // Optional: pull committed WAL directly from a safekeeper (Phase 4) into the
    // root timeline. Per-branch receivers will follow with the control plane.
    if let Some(sk_addr) = args.safekeeper {
        let cfg = WalReceiverConfig::new(
            sk_addr,
            TenantId::ZERO,
            TimelineId::ZERO,
            Lsn(args.wal_start_lsn),
        );
        let timeline_for_wal = root.clone();
        tokio::spawn(async move {
            match WalReceiver::connect(timeline_for_wal, cfg).await {
                Ok(receiver) => {
                    if let Err(e) = receiver.run().await {
                        warn!(error = %format!("{e:#}"), "WAL receiver stopped");
                    }
                }
                Err(e) => warn!(error = %format!("{e:#}"), "WAL receiver failed to connect"),
            }
        });
        info!(safekeeper = %sk_addr, start_lsn = args.wal_start_lsn, "streaming committed WAL from safekeeper");
    }

    // Page-service endpoint (runs on the main task).
    let page_listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    info!(addr = %args.listen, "ready to serve pages");
    serve_pages(tenants, page_listener).await
}

/// Configure structured logging. Honors `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    // Shared fmt subscriber; also exports OTLP spans when built with the `otlp`
    // feature and OTEL_EXPORTER_OTLP_ENDPOINT is set.
    common::telemetry::init("aethel-pageserver");
}

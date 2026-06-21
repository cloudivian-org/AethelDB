// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # aethel-proxy — the activation proxy (binary)
//!
//! Wires the [`proxy`] library to a command line: builds the tenant registry
//! and activator from flags, starts the idle reaper, and runs the accept loop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::net::TcpListener;
use tracing::info;

use proxy::activator::{Activator, CommandActivator, NoopActivator};
use proxy::idle::{self, ReaperConfig};
use proxy::proxy::{serve, HealthConfig, Proxy};
use proxy::tenant::{Registry, TenantState};

/// Command-line / environment configuration for the proxy.
#[derive(Debug, Parser)]
#[command(name = "aethel-proxy", version, about = "AethelDB activation proxy")]
struct Args {
    /// Address to accept PostgreSQL client connections on.
    #[arg(long, env = "SP_PROXY_LISTEN", default_value = "0.0.0.0:5432")]
    listen: SocketAddr,

    /// A tenant routing entry, `name=host:port`. Repeatable, one per tenant.
    /// Tenants start "asleep" so the first connection exercises the wake path.
    #[arg(long = "tenant", value_name = "NAME=ADDR")]
    tenants: Vec<String>,

    /// Shell command run to start a tenant's compute. `{tenant}` is substituted.
    /// If unset, a no-op activator is used (compute managed externally).
    #[arg(long, env = "SP_PROXY_START_COMMAND")]
    start_command: Option<String>,

    /// Shell command run to stop a tenant's compute. `{tenant}` is substituted.
    #[arg(long, env = "SP_PROXY_STOP_COMMAND")]
    stop_command: Option<String>,

    /// Kubernetes namespace for the compute Deployments. When set (with the
    /// `kubernetes` build feature), the proxy scales a per-tenant Deployment
    /// instead of running shell commands.
    #[cfg(feature = "kubernetes")]
    #[arg(long, env = "SP_PROXY_KUBE_NAMESPACE")]
    kube_namespace: Option<String>,

    /// Deployment name template for a tenant's compute (`{tenant}` is substituted).
    #[cfg(feature = "kubernetes")]
    #[arg(long, env = "SP_PROXY_KUBE_NAME_TEMPLATE", default_value = "compute-{tenant}")]
    kube_name_template: String,

    /// Wake budget: hold the client this long waiting for compute to be ready.
    #[arg(long, env = "SP_PROXY_WAKE_BUDGET_MS", default_value_t = 500)]
    wake_budget_ms: u64,

    /// Scale a tenant to zero after this many seconds with no active connections.
    #[arg(long, env = "SP_PROXY_IDLE_SECS", default_value_t = 300)]
    idle_secs: u64,

    /// How often the idle reaper scans, in seconds.
    #[arg(long, env = "SP_PROXY_REAP_TICK_SECS", default_value_t = 10)]
    reap_tick_secs: u64,

    /// Address for the Prometheus `/metrics` endpoint.
    #[arg(long, env = "SP_PROXY_METRICS_LISTEN", default_value = "0.0.0.0:9432")]
    metrics_listen: SocketAddr,

    /// PEM certificate-chain file for TLS termination. Set with --tls-key to
    /// accept TLS (`SSLRequest`) from clients; omit for plaintext only.
    #[arg(long, env = "SP_PROXY_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<std::path::PathBuf>,

    /// PEM private-key file matching --tls-cert.
    #[arg(long, env = "SP_PROXY_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<std::path::PathBuf>,
}

/// Parse a `name=host:port` tenant spec into a registry entry.
fn parse_tenant(spec: &str) -> anyhow::Result<(String, TenantState)> {
    let (name, addr) = spec
        .split_once('=')
        .with_context(|| format!("tenant spec `{spec}` must be NAME=host:port"))?;
    let addr: SocketAddr =
        addr.parse().with_context(|| format!("invalid backend address in tenant spec `{spec}`"))?;
    // Start asleep: the first connection triggers the activator + readiness probe.
    Ok((name.to_owned(), TenantState::new(addr, false)))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();

    // Build the tenant registry from --tenant flags.
    let mut entries = Vec::new();
    for spec in &args.tenants {
        entries.push(parse_tenant(spec)?);
    }
    let registry = Arc::new(Registry::from_iter(entries));

    // Choose an activator: Kubernetes (if a namespace is configured and the
    // feature is built), else command-based, else no-op.
    let activator: Arc<dyn Activator>;
    #[cfg(feature = "kubernetes")]
    if let Some(ns) = &args.kube_namespace {
        activator = Arc::new(
            proxy::k8s::KubeActivator::try_default(ns.clone(), args.kube_name_template.clone())
                .await
                .context("connecting to the Kubernetes API")?,
        );
        info!(namespace = %ns, "using the Kubernetes activator");
    } else {
        activator = command_or_noop(&args.start_command, &args.stop_command)?;
    }
    #[cfg(not(feature = "kubernetes"))]
    {
        activator = command_or_noop(&args.start_command, &args.stop_command)?;
    }

    let health = HealthConfig {
        budget: Duration::from_millis(args.wake_budget_ms),
        ..HealthConfig::default()
    };
    // Enable TLS termination when a cert+key pair is configured.
    let proxy = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let acceptor =
                proxy::tls::acceptor_from_pem(cert, key).context("loading TLS cert/key")?;
            info!(cert = %cert.display(), "TLS termination enabled");
            Proxy::with_tls(registry, activator, health, acceptor)
        }
        _ => Proxy::new(registry, activator, health),
    };

    info!(
        listen = %args.listen,
        tenants = proxy.registry().len(),
        wake_budget_ms = args.wake_budget_ms,
        idle_secs = args.idle_secs,
        "starting aethel-proxy"
    );

    // Spawn the idle reaper.
    let reaper_cfg = ReaperConfig {
        idle_after: Duration::from_secs(args.idle_secs),
        tick: Duration::from_secs(args.reap_tick_secs),
    };
    tokio::spawn(idle::run(proxy.clone(), reaper_cfg));

    // Prometheus metrics endpoint.
    let metrics_listener = TcpListener::bind(args.metrics_listen)
        .await
        .with_context(|| format!("failed to bind metrics {}", args.metrics_listen))?;
    tokio::spawn(common::metrics::serve_metrics(metrics_listener));
    info!(addr = %args.metrics_listen, "serving Prometheus metrics");

    // Bind and serve.
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    info!(addr = %args.listen, "accepting connections");
    serve(proxy, listener).await
}

/// Build the shell-command activator (if both commands are set) or the no-op.
fn command_or_noop(
    start: &Option<String>,
    stop: &Option<String>,
) -> anyhow::Result<Arc<dyn Activator>> {
    Ok(match (start, stop) {
        (Some(s), Some(t)) => Arc::new(CommandActivator::new(s.clone(), t.clone())),
        (None, None) => Arc::new(NoopActivator),
        _ => anyhow::bail!("--start-command and --stop-command must be set together"),
    })
}

/// Configure structured logging. Honors `RUST_LOG`, defaulting to `info`.
fn init_tracing() {
    // Shared fmt subscriber; also exports OTLP spans when built with the `otlp`
    // feature and OTEL_EXPORTER_OTLP_ENDPOINT is set.
    common::telemetry::init("aethel-proxy");
}

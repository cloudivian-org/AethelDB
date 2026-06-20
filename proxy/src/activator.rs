// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Starting and stopping compute nodes, plus the readiness probe.
//!
//! The proxy itself stays agnostic about *how* a compute node is launched. That
//! mechanism is behind the [`Activator`] trait so the same proxy can drive a
//! local shell script, a `docker start`, or a cloud micro-VM API by swapping
//! the implementation. Step 1 ships two: [`CommandActivator`] (the "mock shell
//! script / Docker CLI" path) and [`NoopActivator`] (compute managed
//! externally, used in tests).

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::net::TcpStream;
use tokio::process::Command;
use tracing::{debug, warn};

/// Launches and tears down a tenant's compute node.
#[async_trait]
pub trait Activator: Send + Sync {
    /// Ensure the named tenant's compute node is starting. Should return once
    /// the launch has been *triggered*; readiness is confirmed separately by
    /// [`wait_until_ready`].
    async fn start(&self, tenant: &str) -> anyhow::Result<()>;

    /// Scale the named tenant's compute node to zero.
    async fn stop(&self, tenant: &str) -> anyhow::Result<()>;
}

/// Runs configurable shell commands to start/stop compute.
///
/// The literal token `{tenant}` in either template is replaced with the tenant
/// name before execution, e.g. `docker start pg-{tenant}` or
/// `./scripts/wake.sh {tenant}`. Commands run via `sh -c` so operators can use
/// shell features; in production you would prefer a structured Docker/API call,
/// which is exactly what a different `Activator` impl would provide.
#[derive(Debug, Clone)]
pub struct CommandActivator {
    start_template: String,
    stop_template: String,
}

impl CommandActivator {
    /// Build from start/stop command templates.
    pub fn new(start_template: impl Into<String>, stop_template: impl Into<String>) -> Self {
        CommandActivator { start_template: start_template.into(), stop_template: stop_template.into() }
    }

    /// Substitute `{tenant}` and run a command, surfacing a non-zero exit as an error.
    async fn run(&self, template: &str, tenant: &str) -> anyhow::Result<()> {
        let rendered = template.replace("{tenant}", tenant);
        debug!(tenant, command = %rendered, "running activator command");
        let status = Command::new("sh").arg("-c").arg(&rendered).status().await?;
        if !status.success() {
            anyhow::bail!("activator command `{rendered}` exited with {status}");
        }
        Ok(())
    }
}

#[async_trait]
impl Activator for CommandActivator {
    async fn start(&self, tenant: &str) -> anyhow::Result<()> {
        self.run(&self.start_template.clone(), tenant).await
    }

    async fn stop(&self, tenant: &str) -> anyhow::Result<()> {
        self.run(&self.stop_template.clone(), tenant).await
    }
}

/// An activator that does nothing, for environments where compute is managed
/// out of band (and for tests that pre-start their own backend).
#[derive(Debug, Clone, Default)]
pub struct NoopActivator;

#[async_trait]
impl Activator for NoopActivator {
    async fn start(&self, tenant: &str) -> anyhow::Result<()> {
        debug!(tenant, "noop activator: start ignored");
        Ok(())
    }

    async fn stop(&self, tenant: &str) -> anyhow::Result<()> {
        debug!(tenant, "noop activator: stop ignored");
        Ok(())
    }
}

/// Poll `addr` until a TCP connection succeeds or `budget` is exhausted.
///
/// This is the cold-start readiness probe; the platform targets sub-500 ms wake
/// latency, so the default `budget` is 500 ms and we retry on a short interval
/// to catch the backend the instant it begins listening. Returns the elapsed
/// time to ready on success.
pub async fn wait_until_ready(
    addr: SocketAddr,
    budget: Duration,
    interval: Duration,
) -> anyhow::Result<Duration> {
    let start = Instant::now();
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        match TcpStream::connect(addr).await {
            Ok(_probe) => {
                let elapsed = start.elapsed();
                debug!(%addr, ?elapsed, attempt, "compute is ready");
                return Ok(elapsed);
            }
            Err(err) => {
                if start.elapsed() >= budget {
                    warn!(%addr, attempt, error = %err, "compute not ready within budget");
                    anyhow::bail!(
                        "compute at {addr} not ready within {budget:?} ({attempt} attempts): {err}"
                    );
                }
                tokio::time::sleep(interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn command_activator_succeeds_and_reports_failure() {
        let act = CommandActivator::new("true", "false");
        assert!(act.start("t").await.is_ok());
        // `false` exits non-zero -> stop must surface an error.
        assert!(act.stop("t").await.is_err());
    }

    #[tokio::test]
    async fn command_activator_substitutes_tenant() {
        // Succeeds only if {tenant} expanded to "shop"; `[ x = y ]` exits 1 otherwise.
        let act = CommandActivator::new("[ {tenant} = shop ]", "true");
        assert!(act.start("shop").await.is_ok());
        assert!(act.start("other").await.is_err());
    }

    #[tokio::test]
    async fn wait_until_ready_resolves_when_listener_is_up() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let elapsed =
            wait_until_ready(addr, Duration::from_millis(500), Duration::from_millis(5))
                .await
                .unwrap();
        assert!(elapsed < Duration::from_millis(500));
    }

    #[tokio::test]
    async fn wait_until_ready_times_out_on_dead_address() {
        // 127.0.0.1:1 is reserved and refuses connections immediately.
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let res = wait_until_ready(addr, Duration::from_millis(80), Duration::from_millis(10)).await;
        assert!(res.is_err());
    }
}

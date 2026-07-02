// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The idle reaper — the "scale to zero" half of the proxy.
//!
//! A background task wakes on a fixed tick and asks each tenant whether it is
//! reapable: running, with no in-flight connections, and idle past the
//! threshold. For each one it calls the activator's `stop` and clears the
//! running flag, so the next connection cold-starts it again.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::proxy::Proxy;

/// How often the reaper scans the registry, and how long a tenant must be idle.
#[derive(Debug, Clone, Copy)]
pub struct ReaperConfig {
    /// Idle duration after which a tenant is scaled to zero.
    pub idle_after: Duration,
    /// Interval between reaper scans.
    pub tick: Duration,
}

/// Run the reaper loop forever. Intended to be `tokio::spawn`ed at startup.
pub async fn run(proxy: Arc<Proxy>, config: ReaperConfig) {
    info!(idle_after = ?config.idle_after, tick = ?config.tick, "idle reaper started");
    let mut ticker = tokio::time::interval(config.tick);
    loop {
        ticker.tick().await;
        reap_once(&proxy, config.idle_after).await;
        warm_once(&proxy).await;
    }
}

/// A single warmer pass: start any **keep-warm** tenant whose compute isn't
/// running, so a latency-sensitive database never pays a cold start. A no-op
/// unless a tenant has been marked keep-warm, so default behavior is unchanged.
pub async fn warm_once(proxy: &Arc<Proxy>) {
    let cold: Vec<(String, Arc<crate::tenant::TenantState>)> = proxy
        .registry()
        .tenants()
        .into_iter()
        .filter(|(_, state)| state.keep_warm() && !state.is_running())
        .collect();

    for (name, state) in cold {
        // Re-check: a connection may have already woken it since collection.
        if state.is_running() || !state.keep_warm() {
            continue;
        }
        info!(tenant = %name, "keep-warm: starting compute");
        match proxy.activator().start(&name, state.pinned_timeline().as_deref()).await {
            Ok(()) => {
                state.set_running(true);
                state.touch();
                crate::metrics::set_compute_up(&name, true);
            }
            Err(err) => {
                warn!(tenant = %name, error = %format!("{err:#}"), "keep-warm start failed")
            }
        }
    }
}

/// A single reaper pass over all tenants. Split out so it can be unit-tested
/// without waiting on the timer.
pub async fn reap_once(proxy: &Arc<Proxy>, idle_after: Duration) {
    // Collect first so we don't hold any registry iterator across an await.
    let reapable: Vec<(String, Arc<crate::tenant::TenantState>)> = proxy
        .registry()
        .tenants()
        .into_iter()
        .filter(|(_, state)| state.is_reapable(idle_after))
        .collect();

    for (name, state) in reapable {
        // Re-check under the current instant: a connection may have arrived
        // between collection and now.
        if !state.is_reapable(idle_after) {
            continue;
        }
        info!(tenant = %name, idle_for = ?state.idle_for(), "scaling tenant to zero");
        match proxy.activator().stop(&name).await {
            Ok(()) => {
                state.set_running(false);
                crate::metrics::set_compute_up(&name, false);
                crate::metrics::IDLE_REAPS.inc();
            }
            Err(err) => warn!(tenant = %name, error = %format!("{err:#}"), "failed to stop tenant"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activator::NoopActivator;
    use crate::proxy::HealthConfig;
    use crate::tenant::{Registry, TenantState};

    #[tokio::test]
    async fn reaper_stops_idle_running_tenants() {
        let backend = "127.0.0.1:5432";
        let registry = Registry::from_iter([
            ("idle".to_string(), TenantState::new(backend, true)),
            ("busy".to_string(), TenantState::new(backend, true)),
        ]);
        let registry = Arc::new(registry);
        // "busy" has an open connection and must survive reaping.
        registry.get("busy").unwrap().connection_started();

        let proxy = Proxy::new(registry.clone(), Arc::new(NoopActivator), HealthConfig::default());

        // Idle threshold of zero => every connection-free running tenant is reaped.
        reap_once(&proxy, Duration::ZERO).await;

        assert!(!registry.get("idle").unwrap().is_running(), "idle tenant should be stopped");
        assert!(registry.get("busy").unwrap().is_running(), "busy tenant must stay running");
    }

    #[tokio::test]
    async fn keep_warm_tenant_survives_reaping() {
        let registry = Arc::new(Registry::from_iter([(
            "hot".to_string(),
            TenantState::new("127.0.0.1:5432", true),
        )]));
        registry.get("hot").unwrap().set_keep_warm(true);
        let proxy = Proxy::new(registry.clone(), Arc::new(NoopActivator), HealthConfig::default());

        // Even fully idle, a keep-warm tenant must not be scaled to zero.
        reap_once(&proxy, Duration::ZERO).await;
        assert!(registry.get("hot").unwrap().is_running(), "keep-warm tenant must stay running");
    }

    #[tokio::test]
    async fn warmer_restarts_a_cold_keep_warm_tenant() {
        let registry = Arc::new(Registry::from_iter([(
            "hot".to_string(),
            TenantState::new("127.0.0.1:5432", false), // starts cold
        )]));
        registry.get("hot").unwrap().set_keep_warm(true);
        let proxy = Proxy::new(registry.clone(), Arc::new(NoopActivator), HealthConfig::default());

        warm_once(&proxy).await;
        assert!(
            registry.get("hot").unwrap().is_running(),
            "warmer should start a keep-warm tenant"
        );
    }

    #[tokio::test]
    async fn warmer_leaves_ordinary_tenants_cold() {
        let registry = Arc::new(Registry::from_iter([(
            "dev".to_string(),
            TenantState::new("127.0.0.1:5432", false),
        )]));
        let proxy = Proxy::new(registry.clone(), Arc::new(NoopActivator), HealthConfig::default());

        warm_once(&proxy).await;
        assert!(
            !registry.get("dev").unwrap().is_running(),
            "non-warm tenant must stay scaled to zero"
        );
    }
}

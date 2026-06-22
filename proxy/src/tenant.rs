// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Per-tenant runtime state and the registry that holds it.
//!
//! A tenant maps to one stateless compute backend (`host:port`). The proxy
//! tracks, for each tenant, whether its compute is believed to be running, how
//! many connections are currently splicing through it, and when it last saw
//! activity — the three facts the wake path and the idle reaper need.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Mutable lifecycle state for a single tenant's compute node.
#[derive(Debug)]
pub struct TenantState {
    /// Where this tenant's compute server listens once awake.
    backend: SocketAddr,
    /// Whether the proxy currently believes compute is up. This is a hint that
    /// lets the hot path skip the activator; the health check is authoritative.
    running: AtomicBool,
    /// Number of client connections currently splicing through this tenant.
    active_conns: AtomicU64,
    /// Wall-clock time of the most recent activity (connect or disconnect).
    last_active: Mutex<Instant>,
    /// Optional SCRAM verifier: when present, the proxy authenticates the client
    /// against it before waking compute.
    scram: Option<crate::scram::ScramSecret>,
}

impl TenantState {
    /// Create state for a tenant whose backend lives at `backend`.
    pub fn new(backend: SocketAddr, running: bool) -> Self {
        TenantState {
            backend,
            running: AtomicBool::new(running),
            active_conns: AtomicU64::new(0),
            last_active: Mutex::new(Instant::now()),
            scram: None,
        }
    }

    /// Like [`new`](Self::new), but with a SCRAM verifier for proxy-side auth.
    pub fn with_scram(
        backend: SocketAddr,
        running: bool,
        scram: crate::scram::ScramSecret,
    ) -> Self {
        TenantState { scram: Some(scram), ..Self::new(backend, running) }
    }

    /// The tenant's SCRAM verifier, if proxy-side authentication is enabled.
    pub fn scram(&self) -> Option<&crate::scram::ScramSecret> {
        self.scram.as_ref()
    }

    /// Backend socket address.
    pub fn backend(&self) -> SocketAddr {
        self.backend
    }

    /// Whether compute is believed to be running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Record the believed running state of compute.
    pub fn set_running(&self, running: bool) {
        self.running.store(running, Ordering::Release);
    }

    /// Current number of in-flight connections.
    pub fn active_conns(&self) -> u64 {
        self.active_conns.load(Ordering::Acquire)
    }

    /// Mark a connection as started: bump the gauge and refresh activity.
    pub fn connection_started(&self) {
        self.active_conns.fetch_add(1, Ordering::AcqRel);
        self.touch();
    }

    /// Mark a connection as finished: drop the gauge and refresh activity so the
    /// idle clock starts counting from the moment the last connection closed.
    pub fn connection_finished(&self) {
        self.active_conns.fetch_sub(1, Ordering::AcqRel);
        self.touch();
    }

    /// Update the last-activity timestamp to now.
    pub fn touch(&self) {
        *self.last_active.lock().expect("last_active poisoned") = Instant::now();
    }

    /// How long since the last recorded activity.
    pub fn idle_for(&self) -> Duration {
        self.last_active.lock().expect("last_active poisoned").elapsed()
    }

    /// True when compute is running, no connections are open, and the idle
    /// threshold has elapsed — i.e. it is safe to scale this tenant to zero.
    pub fn is_reapable(&self, idle_threshold: Duration) -> bool {
        self.is_running() && self.active_conns() == 0 && self.idle_for() >= idle_threshold
    }
}

/// Immutable map from tenant name to its [`TenantState`].
///
/// The set of tenants is fixed at startup for the local/dev proxy. A
/// cloud control plane would make this dynamic (behind an `RwLock` or a
/// concurrent map); the lookup API below is deliberately the same shape so that
/// swap is localized.
#[derive(Debug, Default)]
pub struct Registry {
    tenants: std::sync::RwLock<HashMap<String, std::sync::Arc<TenantState>>>,
}

impl FromIterator<(String, TenantState)> for Registry {
    /// Build a registry from `(name, state)` pairs.
    fn from_iter<I: IntoIterator<Item = (String, TenantState)>>(entries: I) -> Self {
        let map =
            entries.into_iter().map(|(name, state)| (name, std::sync::Arc::new(state))).collect();
        Registry { tenants: std::sync::RwLock::new(map) }
    }
}

impl Registry {
    /// Look up a tenant by name.
    pub fn get(&self, tenant: &str) -> Option<std::sync::Arc<TenantState>> {
        self.tenants.read().unwrap().get(tenant).cloned()
    }

    /// A snapshot of `(name, state)` pairs (used by the reaper and control API).
    pub fn tenants(&self) -> Vec<(String, std::sync::Arc<TenantState>)> {
        self.tenants.read().unwrap().iter().map(|(n, s)| (n.clone(), s.clone())).collect()
    }

    /// Register (or replace) a tenant route at runtime — the basis for automatic
    /// routing of newly-provisioned databases.
    pub fn register(&self, name: impl Into<String>, state: TenantState) {
        self.tenants.write().unwrap().insert(name.into(), std::sync::Arc::new(state));
    }

    /// Remove a tenant route; returns whether it existed.
    pub fn remove(&self, name: &str) -> bool {
        self.tenants.write().unwrap().remove(name).is_some()
    }

    /// Number of registered tenants.
    pub fn len(&self) -> usize {
        self.tenants.read().unwrap().len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.tenants.read().unwrap().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> SocketAddr {
        "127.0.0.1:5432".parse().unwrap()
    }

    #[test]
    fn connection_gauge_tracks_in_flight_count() {
        let s = TenantState::new(addr(), true);
        assert_eq!(s.active_conns(), 0);
        s.connection_started();
        s.connection_started();
        assert_eq!(s.active_conns(), 2);
        s.connection_finished();
        assert_eq!(s.active_conns(), 1);
    }

    #[test]
    fn reapable_only_when_idle_running_and_unused() {
        let s = TenantState::new(addr(), true);
        // Running, no conns, but only just touched: not yet idle.
        assert!(!s.is_reapable(Duration::from_secs(60)));
        // Idle threshold of zero => immediately reapable.
        assert!(s.is_reapable(Duration::ZERO));
        // An open connection blocks reaping regardless of idle time.
        s.connection_started();
        assert!(!s.is_reapable(Duration::ZERO));
        // Stopped tenants are never reapable.
        s.connection_finished();
        s.set_running(false);
        assert!(!s.is_reapable(Duration::ZERO));
    }

    #[test]
    fn registry_lookup() {
        let reg = Registry::from_iter([("shop".to_string(), TenantState::new(addr(), false))]);
        assert!(reg.get("shop").is_some());
        assert!(reg.get("missing").is_none());
        assert_eq!(reg.len(), 1);
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Multi-tenant ownership: a registry of isolated [`Tenant`]s keyed by
//! [`TenantId`].
//!
//! One page server hosts many tenants at once. Each tenant is a fully isolated
//! set of timelines (branches) with its own page store; nothing is shared
//! between tenants except the process and the stateless WAL-redo backend. The
//! manager routes page reads, WAL ingest, and control-plane operations to the
//! right tenant by id, and lazily provisions a tenant the first time it is
//! referenced — so a new `TenantId` appearing in a request (or a control call)
//! simply comes into being, no separate provisioning round-trip required.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common::{Lsn, TenantId, TimelineId};
use thiserror::Error;
use tracing::{info, warn};

use crate::catalog;
use crate::objstore::ObjectStore;
use crate::repository::CompactionStats;
use crate::tenant::Tenant;
use crate::walredo::WalRedoManager;

/// Errors from explicit tenant management.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TenantManagerError {
    /// `create` was asked to make a tenant that already exists.
    #[error("tenant {0} already exists")]
    AlreadyExists(TenantId),
}

/// A registry of isolated tenants, each owning its own timelines.
pub struct TenantManager {
    freeze_threshold: usize,
    /// Shared, stateless redo backend handed to every tenant (when configured).
    redo: Option<Arc<dyn WalRedoManager>>,
    /// Object store used to persist the tenant/timeline catalog (when set).
    store: Option<Arc<dyn ObjectStore>>,
    tenants: RwLock<HashMap<TenantId, Arc<Tenant>>>,
}

impl TenantManager {
    /// Build a manager that constructs each new tenant with `freeze_threshold`
    /// and, when set, the shared `redo` backend. The topology is **not**
    /// persisted; use [`with_catalog`](Self::with_catalog) for durability.
    pub fn new(freeze_threshold: usize, redo: Option<Arc<dyn WalRedoManager>>) -> Arc<Self> {
        Arc::new(Self { freeze_threshold, redo, store: None, tenants: RwLock::new(HashMap::new()) })
    }

    /// Like [`new`](Self::new), but persists the tenant/timeline topology to
    /// `store` (see [`crate::catalog`]) so it survives a restart.
    pub fn with_catalog(
        freeze_threshold: usize,
        redo: Option<Arc<dyn WalRedoManager>>,
        store: Arc<dyn ObjectStore>,
    ) -> Arc<Self> {
        Arc::new(Self {
            freeze_threshold,
            redo,
            store: Some(store),
            tenants: RwLock::new(HashMap::new()),
        })
    }

    /// Wrap a single, already-constructed tenant at [`TenantId::ZERO`] — for
    /// single-tenant embeddings and tests. Any tenant lazily provisioned later
    /// uses `freeze_threshold` and no redo backend.
    pub fn single(tenant: Arc<Tenant>) -> Arc<Self> {
        let mut map = HashMap::new();
        map.insert(TenantId::ZERO, tenant);
        Arc::new(Self {
            freeze_threshold: 100_000,
            redo: None,
            store: None,
            tenants: RwLock::new(map),
        })
    }

    /// Persist the current topology to the catalog (no-op without a store).
    pub async fn persist(&self) {
        if let Some(store) = &self.store {
            if let Err(e) = catalog::save(store, &catalog::snapshot(self)).await {
                warn!(error = %format!("{e:#}"), "failed to persist tenant catalog");
            }
        }
    }

    /// Restore the topology from the catalog at startup (no-op without a store).
    pub async fn load_persisted(&self) {
        if let Some(store) = &self.store {
            match catalog::load(store).await {
                Ok(Some(doc)) => {
                    catalog::restore(self, &doc);
                    info!(tenants = doc.tenants.len(), "restored tenant catalog");
                }
                Ok(None) => {}
                Err(e) => warn!(error = %format!("{e:#}"), "failed to load tenant catalog"),
            }
        }
    }

    fn build_tenant(&self) -> Arc<Tenant> {
        match &self.redo {
            Some(redo) => Tenant::with_redo(self.freeze_threshold, redo.clone()),
            None => Tenant::new(self.freeze_threshold),
        }
    }

    /// Look up a tenant, returning `None` if it has not been provisioned.
    pub fn get(&self, id: TenantId) -> Option<Arc<Tenant>> {
        self.tenants.read().unwrap().get(&id).cloned()
    }

    /// Look up a tenant, provisioning an empty one on first reference.
    pub fn get_or_create(&self, id: TenantId) -> Arc<Tenant> {
        if let Some(t) = self.get(id) {
            return t;
        }
        // `entry` under the write lock resolves the get→insert race.
        self.tenants.write().unwrap().entry(id).or_insert_with(|| self.build_tenant()).clone()
    }

    /// Explicitly create a tenant; errors if it already exists.
    pub fn create(&self, id: TenantId) -> Result<Arc<Tenant>, TenantManagerError> {
        let mut map = self.tenants.write().unwrap();
        if map.contains_key(&id) {
            return Err(TenantManagerError::AlreadyExists(id));
        }
        let tenant = self.build_tenant();
        map.insert(id, tenant.clone());
        Ok(tenant)
    }

    /// Remove (deprovision) a tenant, returning whether it existed. The tenant's
    /// in-memory timelines are dropped; its object-store layers are reclaimed by
    /// GC separately.
    pub fn remove(&self, id: TenantId) -> bool {
        self.tenants.write().unwrap().remove(&id).is_some()
    }

    /// Ids of every provisioned tenant.
    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.read().unwrap().keys().copied().collect()
    }

    /// A snapshot of every `(id, tenant)`, for cross-tenant work (offload, GC).
    pub fn tenants(&self) -> Vec<(TenantId, Arc<Tenant>)> {
        self.tenants.read().unwrap().iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Run a branch-aware GC across every tenant, returning per-timeline stats
    /// tagged with their owning tenant.
    pub fn gc_all(&self, horizon: Lsn) -> Vec<(TenantId, TimelineId, CompactionStats)> {
        let mut out = Vec::new();
        for (tid, tenant) in self.tenants() {
            for (tlid, stats) in tenant.gc(horizon) {
                out.push((tid, tlid, stats));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(n: u8) -> TenantId {
        TenantId::from_bytes([n; 16])
    }

    #[test]
    fn provisions_lazily_and_isolates_tenants() {
        let mgr = TenantManager::new(1_000, None);
        assert!(mgr.get(tid(1)).is_none());

        // First reference creates the tenant; a second returns the same one.
        let a = mgr.get_or_create(tid(1));
        let a2 = mgr.get_or_create(tid(1));
        assert!(Arc::ptr_eq(&a, &a2));

        // A different id is a different, isolated tenant.
        let b = mgr.get_or_create(tid(2));
        assert!(!Arc::ptr_eq(&a, &b));

        a.create_timeline(TimelineId::ZERO).unwrap();
        // Tenant b does not see tenant a's timeline.
        assert!(a.get_timeline(TimelineId::ZERO).is_some());
        assert!(b.get_timeline(TimelineId::ZERO).is_none());

        let mut ids = mgr.tenant_ids();
        ids.sort_by_key(|t| t.to_string());
        assert_eq!(ids, vec![tid(1), tid(2)]);
    }

    #[test]
    fn explicit_create_rejects_duplicates() {
        let mgr = TenantManager::new(1_000, None);
        assert!(mgr.create(tid(7)).is_ok());
        assert!(
            matches!(mgr.create(tid(7)), Err(TenantManagerError::AlreadyExists(id)) if id == tid(7))
        );
    }

    #[test]
    fn gc_all_spans_every_tenant() {
        let mgr = TenantManager::new(1_000, None);
        mgr.get_or_create(tid(1)).create_timeline(TimelineId::ZERO).unwrap();
        mgr.get_or_create(tid(2)).create_timeline(TimelineId::ZERO).unwrap();
        // GC touches a timeline in each tenant (two timelines total).
        let stats = mgr.gc_all(Lsn(10));
        assert_eq!(stats.len(), 2);
    }
}

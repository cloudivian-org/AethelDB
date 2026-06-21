// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Durable tenant/timeline **catalog**.
//!
//! Tenants and their timelines are provisioned in memory (see
//! [`crate::tenant_manager`]) and their *page data* lives durably in the object
//! store as immutable layers. What was missing is the **topology**: which
//! tenants exist, which timelines (branches) each has, and each branch's
//! ancestry. Without it, a restarted page server forgets the shape of the world
//! until every id happens to be referenced again.
//!
//! This module persists that topology as a single small JSON object in the same
//! object store, rewritten after each create/branch, and reloads it at startup.
//! Restoring is ancestry-ordered: roots first, then each branch once its parent
//! exists.
//!
//! Note: this restores the **structure** (tenants, timelines, branch points).
//! Rehydrating a timeline's *pages* from its object-store layers on restart is
//! the complementary step and is tracked separately.

use std::sync::Arc;

use common::{Lsn, TimelineId};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::objstore::ObjectStore;
use crate::tenant_manager::TenantManager;

/// Object key under which the catalog is stored.
pub const CATALOG_KEY: &str = "catalog/topology.json";

/// One timeline's record: its id and (for a branch) its ancestry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimelineRecord {
    /// Timeline id (32 hex chars).
    pub id: String,
    /// Parent timeline id for a branch; `None` for a root timeline.
    pub ancestor: Option<String>,
    /// The LSN this branch diverged from its parent at.
    pub ancestor_lsn: Option<u64>,
}

/// One tenant's record: its id and its timelines.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantRecord {
    /// Tenant id (32 hex chars).
    pub id: String,
    /// The tenant's timelines.
    pub timelines: Vec<TimelineRecord>,
}

/// The whole topology: every tenant and its timelines.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Catalog {
    /// Schema version, for forward compatibility.
    pub version: u32,
    /// Every provisioned tenant.
    pub tenants: Vec<TenantRecord>,
}

/// Capture the manager's current topology into a [`Catalog`].
pub fn snapshot(manager: &TenantManager) -> Catalog {
    let mut tenants = Vec::new();
    for (tenant_id, tenant) in manager.tenants() {
        let mut timelines = Vec::new();
        for id in tenant.timeline_ids() {
            if let Some(tl) = tenant.get_timeline(id) {
                timelines.push(TimelineRecord {
                    id: id.to_string(),
                    ancestor: tl.ancestor_timeline().map(|a| a.to_string()),
                    ancestor_lsn: tl.ancestor_lsn().map(|l| l.0),
                });
            }
        }
        tenants.push(TenantRecord { id: tenant_id.to_string(), timelines });
    }
    // Stable order so the persisted bytes are deterministic.
    tenants.sort_by(|a, b| a.id.cmp(&b.id));
    for t in &mut tenants {
        t.timelines.sort_by(|a, b| a.id.cmp(&b.id));
    }
    Catalog { version: 1, tenants }
}

/// Rebuild the manager's tenants and timelines from a [`Catalog`]. Idempotent:
/// timelines that already exist are left as-is.
pub fn restore(manager: &TenantManager, catalog: &Catalog) {
    for tenant_rec in &catalog.tenants {
        let Ok(tenant_id) = tenant_rec.id.parse() else {
            warn!(id = %tenant_rec.id, "skipping tenant with unparseable id");
            continue;
        };
        let tenant = manager.get_or_create(tenant_id);

        // Create roots first, then branches whose parent already exists. Repeat
        // until no further progress (handles arbitrary branch depth/order).
        let mut pending: Vec<&TimelineRecord> = tenant_rec.timelines.iter().collect();
        loop {
            let before = pending.len();
            pending.retain(|rec| {
                let Ok(id) = rec.id.parse::<TimelineId>() else { return false };
                if tenant.get_timeline(id).is_some() {
                    return false; // already present
                }
                match &rec.ancestor {
                    None => {
                        let _ = tenant.create_timeline(id);
                        false
                    }
                    Some(parent_hex) => {
                        let Ok(parent) = parent_hex.parse::<TimelineId>() else { return false };
                        if tenant.get_timeline(parent).is_some() {
                            let lsn = Lsn(rec.ancestor_lsn.unwrap_or(0));
                            let _ = tenant.branch_timeline(id, parent, lsn);
                            false
                        } else {
                            true // parent not created yet; retry next pass
                        }
                    }
                }
            });
            if pending.is_empty() || pending.len() == before {
                break;
            }
        }
        if !pending.is_empty() {
            warn!(
                tenant = %tenant_rec.id,
                orphans = pending.len(),
                "some timelines had unresolved ancestry and were skipped"
            );
        }
    }
}

/// Persist `catalog` to the object store.
pub async fn save(store: &Arc<dyn ObjectStore>, catalog: &Catalog) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(catalog)?;
    store.put(CATALOG_KEY, bytes).await
}

/// Load the catalog from the object store, or `None` if none has been written.
pub async fn load(store: &Arc<dyn ObjectStore>) -> anyhow::Result<Option<Catalog>> {
    match store.get(CATALOG_KEY).await {
        Ok(bytes) => {
            let catalog = serde_json::from_slice(&bytes)?;
            Ok(Some(catalog))
        }
        // A missing catalog is normal on first boot — not an error.
        Err(e) => {
            debug!(error = %e, "no tenant catalog found (first boot?)");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objstore::LocalObjectStore;
    use common::TenantId;

    fn tid(n: u8) -> TenantId {
        TenantId::from_bytes([n; 16])
    }
    fn tl(n: u8) -> TimelineId {
        TimelineId::from_bytes([n; 16])
    }

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!("sp-catalog-{}-{}", tag, std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            TempDir(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn topology_survives_a_save_load_into_a_fresh_manager() {
        let dir = TempDir::new("roundtrip");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&dir.0).unwrap());

        // Build a topology: two tenants; tenant 1 has a root + a branch.
        let m1 = TenantManager::new(1_000, None);
        let t1 = m1.get_or_create(tid(1));
        t1.create_timeline(tl(0)).unwrap();
        t1.branch_timeline(tl(2), tl(0), Lsn(100)).unwrap();
        m1.get_or_create(tid(3)).create_timeline(tl(0)).unwrap();

        // Persist, then restore into a brand-new manager.
        save(&store, &snapshot(&m1)).await.unwrap();
        let loaded = load(&store).await.unwrap().expect("catalog present");
        let m2 = TenantManager::new(1_000, None);
        restore(&m2, &loaded);

        // Tenants and timelines came back.
        let mut tenants = m2.tenant_ids();
        tenants.sort_by_key(|t| t.to_string());
        assert_eq!(tenants, vec![tid(1), tid(3)]);

        let r1 = m2.get(tid(1)).unwrap();
        assert!(r1.get_timeline(tl(0)).is_some());
        let branch = r1.get_timeline(tl(2)).expect("branch restored");
        assert_eq!(branch.ancestor_timeline(), Some(tl(0)));
        assert_eq!(branch.ancestor_lsn(), Some(Lsn(100)));

        assert!(m2.get(tid(3)).unwrap().get_timeline(tl(0)).is_some());

        // Restoring again is idempotent.
        restore(&m2, &loaded);
        assert_eq!(m2.get(tid(1)).unwrap().timeline_ids().len(), 2);
    }

    #[tokio::test]
    async fn load_returns_none_when_absent() {
        let dir = TempDir::new("absent");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&dir.0).unwrap());
        assert!(load(&store).await.unwrap().is_none());
    }
}

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! The offload worker — pushes frozen layers to object storage.
//!
//! Frozen layers are immutable, so once written they can live anywhere. This
//! background worker periodically serializes any not-yet-uploaded layer into a
//! single object and stores it under `layers/<id>.layer`, then marks it
//! uploaded in the repository. That is the "bundle immutable blocks into unified
//! files and push them to object storage" step; it keeps the local working set
//! bounded while preserving full history cheaply and durably.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::objstore::ObjectStore;
use crate::repository::Repository;
use crate::tenant::Tenant;

/// Object-key prefix under which layer files are stored.
pub const LAYER_PREFIX: &str = "layers/";

/// Build the object key for a layer id.
pub fn layer_key(id: u64) -> String {
    format!("{LAYER_PREFIX}{id:016x}.layer")
}

/// Run one offload pass: upload every pending frozen layer. Returns the number
/// of layers uploaded. Split out from [`run`] so it can be unit-tested without
/// the timer.
pub async fn offload_pending(repo: &Arc<Repository>, store: &Arc<dyn ObjectStore>) -> usize {
    let pending = repo.pending_offload();
    let mut uploaded = 0;
    for layer in pending {
        let key = layer_key(layer.id());
        let bytes = layer.serialize();
        match store.put(&key, bytes).await {
            Ok(()) => {
                repo.mark_uploaded(layer.id());
                uploaded += 1;
                info!(layer_id = layer.id(), %key, versions = layer.len(), "offloaded layer");
            }
            Err(err) => warn!(layer_id = layer.id(), error = %format!("{err:#}"), "offload failed"),
        }
    }
    uploaded
}

/// Run one offload pass across every timeline in the tenant.
pub async fn offload_tenant(tenant: &Arc<Tenant>, store: &Arc<dyn ObjectStore>) -> usize {
    let mut uploaded = 0;
    for id in tenant.timeline_ids() {
        if let Some(tl) = tenant.get_timeline(id) {
            uploaded += offload_pending(&tl.repository(), store).await;
        }
    }
    uploaded
}

/// Run the offload loop forever, scanning every `tick`. Spawn at startup.
/// Offloads frozen layers from every timeline of the tenant.
pub async fn run(tenant: Arc<Tenant>, store: Arc<dyn ObjectStore>, tick: Duration) {
    info!(?tick, "layer offload worker started");
    let mut ticker = tokio::time::interval(tick);
    loop {
        ticker.tick().await;
        offload_tenant(&tenant, &store).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::Layer;
    use crate::objstore::LocalObjectStore;
    use crate::page::{Modification, PageVersion};
    use common::{ForkNumber, Lsn, RelTag, PAGE_SIZE};
    use std::path::PathBuf;

    struct TempDir(PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let p = std::env::temp_dir().join(format!("sp-offload-{}-{}", tag, std::process::id()));
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
    async fn offloads_frozen_layers_and_is_idempotent() {
        let dir = TempDir::new("push");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalObjectStore::new(&dir.0).unwrap());
        let repo = Repository::new(1_000);

        let rel = RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main };
        repo.ingest([Modification {
            rel,
            block: 0,
            lsn: Lsn(10),
            version: PageVersion::Image(vec![0xEE; PAGE_SIZE]),
        }]);
        repo.freeze();

        // First pass uploads the one layer; a second pass uploads nothing.
        assert_eq!(offload_pending(&repo, &store).await, 1);
        assert_eq!(offload_pending(&repo, &store).await, 0);

        // The object exists and deserializes back to a usable layer.
        let keys = store.list(LAYER_PREFIX).await.unwrap();
        assert_eq!(keys.len(), 1);
        let bytes = store.get(&keys[0]).await.unwrap();
        let layer = Layer::deserialize(&bytes).unwrap();
        assert_eq!(layer.len(), 1);
    }
}

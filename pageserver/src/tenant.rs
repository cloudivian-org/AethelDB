// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! A **tenant** — one isolated database instance and the set of [`Timeline`]s
//! (branches) that make up its history.
//!
//! The tenant is the branch manager: it creates the root timeline, branches new
//! child timelines off any existing one at a chosen LSN (the "instant branch"
//! and point-in-time operations), and resolves a [`TimelineId`] to its
//! [`Timeline`] for the page-service and ingest paths.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common::{Lsn, TimelineId};
use thiserror::Error;

use crate::repository::Repository;
use crate::timeline::Timeline;
use crate::walredo::{RustApplyRedoManager, WalRedoManager};

/// Errors from tenant/branch management.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TenantError {
    #[error("timeline {0} already exists")]
    AlreadyExists(TimelineId),
    #[error("parent timeline {0} does not exist")]
    NoSuchParent(TimelineId),
}

/// One isolated database instance: a collection of branchable timelines.
pub struct Tenant {
    timelines: Mutex<HashMap<TimelineId, Arc<Timeline>>>,
    freeze_threshold: usize,
    redo: Arc<dyn WalRedoManager>,
}

impl Tenant {
    /// Create an empty tenant using the native Rust redo backend.
    pub fn new(freeze_threshold: usize) -> Arc<Self> {
        Self::with_redo(freeze_threshold, Arc::new(RustApplyRedoManager))
    }

    /// Create an empty tenant with an explicit WAL-redo backend (e.g. a
    /// Postgres wal-redo process), shared by every timeline's store.
    pub fn with_redo(freeze_threshold: usize, redo: Arc<dyn WalRedoManager>) -> Arc<Self> {
        Arc::new(Tenant {
            timelines: Mutex::new(HashMap::new()),
            freeze_threshold,
            redo,
        })
    }

    fn new_repo(&self) -> Arc<Repository> {
        Repository::with_redo(self.freeze_threshold, self.redo.clone())
    }

    /// Create a fresh root timeline (no ancestor).
    pub fn create_timeline(&self, id: TimelineId) -> Result<Arc<Timeline>, TenantError> {
        let mut timelines = self.timelines.lock().unwrap();
        if timelines.contains_key(&id) {
            return Err(TenantError::AlreadyExists(id));
        }
        let tl = Timeline::root(id, self.new_repo());
        timelines.insert(id, tl.clone());
        Ok(tl)
    }

    /// Branch a new timeline `new_id` off `parent_id` at `ancestor_lsn`.
    ///
    /// Instant: the branch starts empty and shares all of the parent's history
    /// up to `ancestor_lsn`, diverging only as it is written to.
    pub fn branch_timeline(
        &self,
        new_id: TimelineId,
        parent_id: TimelineId,
        ancestor_lsn: Lsn,
    ) -> Result<Arc<Timeline>, TenantError> {
        let mut timelines = self.timelines.lock().unwrap();
        if timelines.contains_key(&new_id) {
            return Err(TenantError::AlreadyExists(new_id));
        }
        let parent = timelines.get(&parent_id).cloned().ok_or(TenantError::NoSuchParent(parent_id))?;
        let tl = Timeline::branched(new_id, self.new_repo(), parent, ancestor_lsn);
        timelines.insert(new_id, tl.clone());
        Ok(tl)
    }

    /// Resolve a timeline by id.
    pub fn get_timeline(&self, id: TimelineId) -> Option<Arc<Timeline>> {
        self.timelines.lock().unwrap().get(&id).cloned()
    }

    /// All timeline ids currently known to this tenant.
    pub fn timeline_ids(&self) -> Vec<TimelineId> {
        self.timelines.lock().unwrap().keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{ByteEdit, Modification, PageVersion};
    use crate::repository::PageLookup;
    use common::{ForkNumber, PageKey, RelTag, PAGE_SIZE};

    fn rel() -> RelTag {
        RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
    }
    fn key(block: u32) -> PageKey {
        PageKey { rel: rel(), block }
    }
    fn tl(n: u8) -> TimelineId {
        TimelineId::from_bytes([n; 16])
    }
    fn image(byte: u8, lsn: u64, block: u32) -> Modification {
        Modification { rel: rel(), block, lsn: Lsn(lsn), version: PageVersion::Image(vec![byte; PAGE_SIZE]) }
    }
    fn delta(offset: u16, byte: u8, lsn: u64, block: u32) -> Modification {
        Modification {
            rel: rel(),
            block,
            lsn: Lsn(lsn),
            version: PageVersion::Delta(vec![ByteEdit { offset, data: vec![byte] }]),
        }
    }
    fn page(tl: &Timeline, block: u32, lsn: u64) -> Vec<u8> {
        match tl.get_page(key(block), Lsn(lsn)).unwrap() {
            PageLookup::Page(p) => p,
            other => panic!("expected page, got {other:?}"),
        }
    }

    #[test]
    fn branch_inherits_unmodified_pages_from_parent() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(7, 10, 0)]);

        // Branch at LSN 20; the page was last written at 10, so the branch sees it.
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(page(&branch, 0, 100)[0], 7, "branch inherits the parent's page");
    }

    #[test]
    fn branch_writes_are_copy_on_write_and_isolated() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0)]);

        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        // Modify the page only on the branch (a delta over the inherited base).
        branch.ingest([delta(0, 0xAB, 30, 0)]);

        // Branch reflects the change; parent is untouched.
        assert_eq!(page(&branch, 0, 100)[0], 0xAB, "branch sees its own write");
        assert_eq!(page(&main, 0, 100)[0], 0, "parent is unaffected (isolation)");
    }

    #[test]
    fn parent_changes_after_branch_point_are_not_visible() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(1, 10, 0)]);
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();

        // Parent writes again *after* the branch point.
        main.ingest([delta(0, 0x99, 50, 0)]);

        assert_eq!(page(&main, 0, 100)[0], 0x99, "parent advances");
        assert_eq!(page(&branch, 0, 100)[0], 1, "branch is frozen at the branch point");
    }

    #[test]
    fn pitr_time_travel_within_a_timeline() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), delta(0, 0x11, 20, 0), delta(0, 0x22, 30, 0)]);

        assert_eq!(page(&main, 0, 10)[0], 0, "at LSN 10: base image");
        assert_eq!(page(&main, 0, 25)[0], 0x11, "at LSN 25: first delta only");
        assert_eq!(page(&main, 0, 30)[0], 0x22, "at LSN 30: both deltas");
    }

    #[test]
    fn branch_at_a_past_lsn_sees_history_as_of_that_point() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), delta(0, 0x11, 20, 0), delta(0, 0x22, 30, 0)]);

        // Branch as of LSN 20 (before the second delta): should see 0x11, not 0x22.
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(page(&branch, 0, 100)[0], 0x11, "branch captures the parent at LSN 20");
    }

    #[test]
    fn multi_level_branches_inherit_through_the_chain() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(5, 10, 0)]);
        let child = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        child.ingest([delta(1, 0x44, 30, 0)]); // modify byte 1 on the child
        let grandchild = tenant.branch_timeline(tl(3), tl(2), Lsn(40)).unwrap();

        // Grandchild inherits byte 0 from main and byte 1 from the child.
        let p = page(&grandchild, 0, 100);
        assert_eq!(p[0], 5, "byte 0 inherited from the grandparent");
        assert_eq!(p[1], 0x44, "byte 1 inherited from the parent branch");
    }

    #[test]
    fn rel_size_inherited_from_ancestor() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0), image(0, 12, 4)]); // 5 blocks by LSN 12
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(branch.get_rel_size(rel(), Lsn(100)), Some(5), "branch inherits the size");

        // Extending the relation on the branch grows it further.
        branch.ingest([image(0, 30, 9)]); // block 9 -> 10 blocks
        assert_eq!(branch.get_rel_size(rel(), Lsn(100)), Some(10));
        assert_eq!(main.get_rel_size(rel(), Lsn(100)), Some(5), "parent size unchanged");
    }

    #[test]
    fn duplicate_and_missing_parent_are_errors() {
        let tenant = Tenant::new(1_000);
        tenant.create_timeline(tl(1)).unwrap();
        assert_eq!(tenant.create_timeline(tl(1)).err(), Some(TenantError::AlreadyExists(tl(1))));
        assert_eq!(
            tenant.branch_timeline(tl(2), tl(9), Lsn(10)).err(),
            Some(TenantError::NoSuchParent(tl(9))),
        );
    }

    #[test]
    fn page_absent_in_branch_and_ancestor_is_not_found() {
        let tenant = Tenant::new(1_000);
        let main = tenant.create_timeline(tl(1)).unwrap();
        main.ingest([image(0, 10, 0)]);
        let branch = tenant.branch_timeline(tl(2), tl(1), Lsn(20)).unwrap();
        assert_eq!(branch.get_page(key(999), Lsn(100)).unwrap(), PageLookup::NotFound);
    }
}

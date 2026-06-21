// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! WAL redo: turning a page's version history into a concrete 8 KiB image.
//!
//! Reconstruction gathers every version of a page up to the requested LSN, in
//! order, and replays them over a base. *How* a version is applied depends on
//! its kind, and one kind — a raw [`PageVersion::WalRecord`] — can only be
//! applied by Postgres's own per-resource-manager redo routines. The
//! [`WalRedoManager`] trait is that seam:
//!
//! * [`RustApplyRedoManager`] (this phase) applies full images and `ByteEdit`
//!   deltas natively. It is correct and dependency-free, and keeps the existing
//!   reconstruction behaviour intact — but it cannot apply a real WAL record,
//!   so it returns [`RedoError::NeedsPostgres`] when it meets one.
//! * `PostgresRedoManager` (Phase 3, see `docs/design/wal-redo.md`) will drive a
//!   child *wal-redo* Postgres process behind this same trait, making real WAL
//!   records materialize correctly.
//!
//! Keeping the trait here lets the repository reconstruct pages without knowing
//! which backend is in use.

use common::{Lsn, PageKey, PAGE_SIZE};
use thiserror::Error;

use crate::page::{PageError, PageVersion};

/// Errors from reconstructing a page out of its version history.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RedoError {
    /// A native apply step (image install or byte-edit) failed.
    #[error("applying page version: {0}")]
    Apply(#[from] PageError),
    /// The history contains a raw WAL record that needs a Postgres redo backend,
    /// which this build does not provide (the native [`RustApplyRedoManager`]).
    #[error("WAL record at {lsn} requires a Postgres wal-redo backend")]
    NeedsPostgres {
        /// LSN of the record that could not be applied natively.
        lsn: Lsn,
    },
    /// The wal-redo process could not be spawned or driven (I/O or protocol).
    #[error("wal-redo process error: {0}")]
    Process(String),
    /// The wal-redo process reported that applying the records failed.
    #[error("wal-redo failed: {0}")]
    RedoFailed(String),
}

/// Reconstructs a page image from an LSN-ordered slice of its versions.
pub trait WalRedoManager: Send + Sync {
    /// Reconstruct page `key` as of `request_lsn` from `versions`, which must be
    /// every version of the page with LSN ≤ `request_lsn`, sorted ascending.
    ///
    /// Returns `Ok(None)` when there is no base (image or `will_init` record) to
    /// start from — i.e. the page does not exist at that LSN.
    fn reconstruct(
        &self,
        key: PageKey,
        request_lsn: Lsn,
        versions: &[(Lsn, &PageVersion)],
    ) -> Result<Option<Vec<u8>>, RedoError>;
}

/// A redo manager that applies images and byte-edit deltas natively in Rust.
///
/// Sufficient for full-page images and the synthetic `ByteEdit` deltas; defers
/// raw WAL records to a Postgres backend via [`RedoError::NeedsPostgres`].
#[derive(Debug, Default, Clone, Copy)]
pub struct RustApplyRedoManager;

impl WalRedoManager for RustApplyRedoManager {
    fn reconstruct(
        &self,
        _key: PageKey,
        _request_lsn: Lsn,
        versions: &[(Lsn, &PageVersion)],
    ) -> Result<Option<Vec<u8>>, RedoError> {
        // Start from the most recent base (full image or will_init record); any
        // version before it is irrelevant to the result.
        let base = match versions.iter().rposition(|(_, v)| v.is_base()) {
            Some(i) => i,
            None => return Ok(None),
        };

        let mut page = vec![0u8; PAGE_SIZE];
        // Apply the base. A will_init WAL record base still needs Postgres to
        // materialize; only a full image can be installed natively.
        match versions[base].1 {
            PageVersion::Image(_) => versions[base].1.apply_to(&mut page)?,
            PageVersion::WalRecord(_) => {
                return Err(RedoError::NeedsPostgres { lsn: versions[base].0 })
            }
            // is_base() is false for Delta, so the base is never a delta.
            PageVersion::Delta(_) => unreachable!("delta cannot be a reconstruction base"),
        }

        // Replay everything after the base in LSN order.
        for (lsn, v) in &versions[base + 1..] {
            match v {
                PageVersion::WalRecord(_) => return Err(RedoError::NeedsPostgres { lsn: *lsn }),
                _ => v.apply_to(&mut page)?,
            }
        }
        Ok(Some(page))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{ByteEdit, WalRecord};
    use common::{ForkNumber, RelTag};

    fn key() -> PageKey {
        PageKey {
            rel: RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main },
            block: 0,
        }
    }

    #[test]
    fn applies_image_then_deltas() {
        let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
        let d1 = PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![0xAA] }]);
        let d2 = PageVersion::Delta(vec![ByteEdit { offset: 1, data: vec![0xBB] }]);
        let versions = [(Lsn(10), &img), (Lsn(20), &d1), (Lsn(30), &d2)];

        let page = RustApplyRedoManager
            .reconstruct(key(), Lsn(30), &versions)
            .unwrap()
            .expect("page exists");
        assert_eq!(page[0], 0xAA);
        assert_eq!(page[1], 0xBB);
    }

    #[test]
    fn no_base_is_none() {
        let d = PageVersion::Delta(vec![ByteEdit { offset: 0, data: vec![1] }]);
        let versions = [(Lsn(10), &d)];
        assert_eq!(RustApplyRedoManager.reconstruct(key(), Lsn(10), &versions).unwrap(), None);
    }

    #[test]
    fn ignores_history_before_the_latest_image() {
        let old = PageVersion::Image(vec![1u8; PAGE_SIZE]);
        let newer = PageVersion::Image(vec![2u8; PAGE_SIZE]);
        let versions = [(Lsn(10), &old), (Lsn(20), &newer)];
        let page = RustApplyRedoManager.reconstruct(key(), Lsn(20), &versions).unwrap().unwrap();
        assert!(page.iter().all(|&b| b == 2));
    }

    #[test]
    fn raw_wal_record_defers_to_postgres() {
        let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
        let wal = PageVersion::WalRecord(WalRecord { will_init: false, rec: vec![0u8; 32] });
        let versions = [(Lsn(10), &img), (Lsn(20), &wal)];
        assert_eq!(
            RustApplyRedoManager.reconstruct(key(), Lsn(20), &versions),
            Err(RedoError::NeedsPostgres { lsn: Lsn(20) }),
        );
    }

    #[test]
    fn will_init_wal_record_base_defers_to_postgres() {
        let wal = PageVersion::WalRecord(WalRecord { will_init: true, rec: vec![0u8; 32] });
        let versions = [(Lsn(10), &wal)];
        assert_eq!(
            RustApplyRedoManager.reconstruct(key(), Lsn(10), &versions),
            Err(RedoError::NeedsPostgres { lsn: Lsn(10) }),
        );
    }
}

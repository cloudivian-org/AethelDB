// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Tests for [`PostgresRedoManager`] driving a real child wal-redo process.
//!
//! The process is the reference `aethel-walredo-mock` binary (toy byte-edit
//! semantics), spawned from the path Cargo exposes via `CARGO_BIN_EXE_*`. These
//! exercise the full plumbing — framing, record batching, end-to-end through the
//! repository, forced errors, and transparent restart after the child exits.

use std::sync::Arc;

use common::{ForkNumber, Lsn, PageKey, RelTag, PAGE_SIZE};
use pageserver::page::{Modification, PageVersion, WalRecord};
use pageserver::walredo::{RedoError, WalRedoManager};
use pageserver::{PageLookup, PostgresRedoManager, Repository};

/// Path to the compiled mock wal-redo binary.
fn mock_bin() -> &'static str {
    env!("CARGO_BIN_EXE_aethel-walredo-mock")
}

fn manager(args: &[&str]) -> PostgresRedoManager {
    PostgresRedoManager::new(mock_bin(), args.iter().map(|s| s.to_string()).collect())
}

fn rel() -> RelTag {
    RelTag { spc_node: 1, db_node: 2, rel_node: 3, fork: ForkNumber::Main }
}
fn key() -> PageKey {
    PageKey { rel: rel(), block: 0 }
}

/// Encode toy `[offset:u16][len:u16][data]` edits, the mock's apply format.
fn toy(edits: &[(u16, &[u8])]) -> Vec<u8> {
    let mut v = Vec::new();
    for (off, data) in edits {
        v.extend_from_slice(&off.to_be_bytes());
        v.extend_from_slice(&(data.len() as u16).to_be_bytes());
        v.extend_from_slice(data);
    }
    v
}

fn wal(will_init: bool, edits: &[(u16, &[u8])]) -> PageVersion {
    PageVersion::WalRecord(WalRecord { will_init, rec: toy(edits) })
}

#[test]
fn applies_a_record_onto_an_image_base() {
    let mgr = manager(&[]);
    let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
    let rec = wal(false, &[(0, &[1, 2, 3])]);
    let versions = [(Lsn(10), &img), (Lsn(20), &rec)];

    let page = mgr.reconstruct(key(), Lsn(20), &versions).unwrap().expect("page");
    assert_eq!(&page[0..3], &[1, 2, 3]);
    assert!(page[3..].iter().all(|&b| b == 0));
}

#[test]
fn will_init_record_starts_from_zeros() {
    let mgr = manager(&[]);
    let rec = wal(true, &[(5, &[9, 9])]);
    let versions = [(Lsn(10), &rec)];

    let page = mgr.reconstruct(key(), Lsn(10), &versions).unwrap().expect("page");
    assert_eq!(&page[5..7], &[9, 9]);
    assert_eq!(page[0..5], [0; 5]);
}

#[test]
fn applies_multiple_records_in_lsn_order() {
    let mgr = manager(&[]);
    let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
    let a = wal(false, &[(0, &[1, 1])]);
    let b = wal(false, &[(0, &[2])]); // overwrites the first byte
    let versions = [(Lsn(10), &img), (Lsn(20), &a), (Lsn(30), &b)];

    let page = mgr.reconstruct(key(), Lsn(30), &versions).unwrap().expect("page");
    assert_eq!(page[0], 2); // last write wins
    assert_eq!(page[1], 1);
}

#[test]
fn reconstructs_through_the_repository() {
    let repo = Repository::with_redo(1_000, Arc::new(manager(&[])));
    repo.ingest([
        Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(10),
            version: PageVersion::Image(vec![0u8; PAGE_SIZE]),
        },
        Modification {
            rel: rel(),
            block: 0,
            lsn: Lsn(20),
            version: wal(false, &[(100, &[7, 7, 7])]),
        },
    ]);

    match repo.get_page(key(), Lsn(100)).unwrap() {
        PageLookup::Page(p) => {
            assert_eq!(&p[100..103], &[7, 7, 7]);
            assert!(p[0..100].iter().all(|&b| b == 0));
        }
        other => panic!("expected page, got {other:?}"),
    }
}

#[test]
fn surfaces_a_process_error() {
    let mgr = manager(&["--fail"]);
    let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
    let rec = wal(false, &[(0, &[1])]);
    let versions = [(Lsn(10), &img), (Lsn(20), &rec)];

    let err = mgr.reconstruct(key(), Lsn(20), &versions).unwrap_err();
    assert!(matches!(err, RedoError::RedoFailed(_)), "expected RedoFailed, got {err:?}");
}

#[test]
fn restarts_the_process_transparently_after_it_exits() {
    // The child exits after serving each request; the manager must respawn it,
    // so repeated reconstructions all succeed despite the process dying.
    let mgr = manager(&["--exit-after=1"]);
    let img = PageVersion::Image(vec![0u8; PAGE_SIZE]);
    let rec = wal(false, &[(0, &[5])]);
    let versions = [(Lsn(10), &img), (Lsn(20), &rec)];

    for round in 0..3 {
        let page = mgr
            .reconstruct(key(), Lsn(20), &versions)
            .unwrap_or_else(|e| panic!("round {round} failed: {e:?}"))
            .expect("page");
        assert_eq!(page[0], 5, "round {round}");
    }
}

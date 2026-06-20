// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # pageserver — log-structured page storage (library)
//!
//! The `aethel-pageserver` binary is a thin CLI over the pieces here:
//!
//! * [`page`] — page versions (image/delta) and the ingest `Modification`.
//! * [`layer`] — immutable, offloadable layers and their file format.
//! * [`repository`] — the memtable + layers store and the reconstruction engine.
//! * [`objstore`] — the object-store abstraction and a local (mock MinIO) impl.
//! * [`offload`] — the background worker that pushes layers to object storage.
//! * [`server`] — the page-service and ingest network endpoints.
//! * [`waldecode`] — PostgreSQL WAL stream framing + record decoding (Phase 1 of
//!   the WAL decode/redo subsystem; see `docs/design/wal-redo.md`).
//! * [`walredo`] — reconstructs a page from its version history (Phase 2): a
//!   native Rust apply backend.
//! * [`walredo_proto`] / [`walredo_process`] — the wal-redo pipe protocol and a
//!   Postgres-process redo backend (Phase 3).
//! * [`walreceiver`] — streams committed WAL from a safekeeper into the store
//!   (Phase 4), closing the safekeeper→page-server link.

pub mod control;
pub mod layer;
pub mod metrics;
pub mod objstore;
pub mod offload;
pub mod page;
pub mod repository;
pub mod server;
pub mod tenant;
pub mod timeline;
pub mod waldecode;
pub mod walreceiver;
pub mod walredo;
pub mod walredo_process;
pub mod walredo_proto;

pub use layer::{Layer, LayerId};
pub use objstore::{LocalObjectStore, ObjectStore, S3ObjectStore};
pub use page::{ByteEdit, Modification, PageVersion, WalRecord};
pub use control::serve_control;
pub use repository::{PageLookup, Repository};
pub use server::{serve_ingest, serve_pages};
pub use tenant::{Tenant, TenantError};
pub use timeline::Timeline;
pub use waldecode::{
    decode_wal_record, Compression, DecodedBlock, DecodedImage, DecodedWalRecord, WalDecodeError,
    WalStreamDecoder,
};
pub use walreceiver::{WalReceiver, WalReceiverConfig};
pub use walredo::{RedoError, RustApplyRedoManager, WalRedoManager};
pub use walredo_process::PostgresRedoManager;

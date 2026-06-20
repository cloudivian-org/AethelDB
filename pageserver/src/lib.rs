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

pub mod layer;
pub mod objstore;
pub mod offload;
pub mod page;
pub mod repository;
pub mod server;

pub use layer::{Layer, LayerId};
pub use objstore::{LocalObjectStore, ObjectStore};
pub use page::{ByteEdit, Modification, PageVersion};
pub use repository::{PageLookup, Repository};
pub use server::{serve_ingest, serve_pages};

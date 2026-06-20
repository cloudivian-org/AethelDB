// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Shared vocabulary types for the AethelDB platform.
//!
//! The compute node, safekeeper, and pageserver are separate processes that
//! talk over the network, so they must agree byte-for-byte on a handful of
//! primitives. Those primitives live here rather than being redefined in each
//! service:
//!
//! * [`Lsn`] — a PostgreSQL Log Sequence Number, the global clock of the system.
//! * [`TenantId`] / [`TimelineId`] — opaque 16-byte identifiers for an isolated
//!   database and one of its (branchable) history lines.
//! * [`RelTag`] and [`PageKey`] — address a single 8 KiB page within a tenant.
//!
//! Keeping these in one crate means a change to, say, the on-the-wire LSN
//! format is a single edit that the whole workspace recompiles against.

pub mod error;
pub mod ids;
pub mod lsn;
pub mod metrics;
pub mod page;
pub mod page_service;
pub mod wal_service;

pub use error::{Error, Result};
pub use ids::{TenantId, TimelineId};
pub use lsn::Lsn;
pub use page::{ForkNumber, PageKey, RelTag, PAGE_SIZE};
pub use page_service::{Request as PageRequest, Response as PageResponse};
pub use wal_service::{AppendRequest, AppendResponse};

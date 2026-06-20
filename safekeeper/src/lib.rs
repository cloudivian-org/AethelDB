// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! # safekeeper — durable, consensus-backed WAL ingest (library)
//!
//! The `aethel-safekeeper` binary is a thin CLI over the pieces here:
//!
//! * [`storage`] — the disk-backed, segmented WAL store with an in-memory ring.
//! * [`consensus`] — terms, quorum commit-LSN, and leader election.
//! * [`replicator`] — pluggable replication to peer safekeepers.
//! * [`server`] — the ingest handler and accept loop.

pub mod consensus;
pub mod replicator;
pub mod server;
pub mod storage;

pub use consensus::{Consensus, NodeId, Role, Term};
pub use replicator::{LocalSimReplicator, PeerAck, Replicator};
pub use server::{serve, Safekeeper};
pub use storage::{WalConfig, WalStorage};

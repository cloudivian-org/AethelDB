// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Prometheus metrics for the safekeeper. Exposed by
//! [`common::metrics::serve_metrics`].

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

/// WAL append requests received from compute.
pub static APPENDS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_safekeeper_appends_total", "WAL append requests received").unwrap()
});

/// WAL bytes durably appended.
pub static WAL_BYTES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_safekeeper_wal_bytes_total", "WAL bytes durably appended").unwrap()
});

/// Replication appends received from a leader (acceptor role).
pub static REPLICATED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_safekeeper_replicated_total", "Replication appends received from a leader").unwrap()
});

/// Leadership votes granted.
pub static VOTES_GRANTED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_safekeeper_votes_granted_total", "Leadership votes granted").unwrap()
});

/// The current quorum-committed LSN.
pub static COMMIT_LSN: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("aethel_safekeeper_commit_lsn", "Current quorum-committed LSN").unwrap()
});

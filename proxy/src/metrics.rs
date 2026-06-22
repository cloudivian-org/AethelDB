// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Prometheus metrics for the activation proxy.
//!
//! Counters/gauges register with the process-global registry and are exposed by
//! [`common::metrics::serve_metrics`].

use once_cell::sync::Lazy;
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

// ---- Per-database (labelled) metrics — the basis for the console's per-database
// charts. Cardinality is bounded by the number of databases. ----

/// Client connections spliced, per database.
pub static DB_CONNECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "aethel_proxy_database_connections_total",
        "Client connections spliced, per database",
        &["database"]
    )
    .unwrap()
});

/// Connections currently splicing, per database.
pub static DB_ACTIVE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "aethel_proxy_database_active_connections",
        "Connections currently splicing, per database",
        &["database"]
    )
    .unwrap()
});

/// Cold starts (wakes) triggered, per database.
pub static DB_WAKES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "aethel_proxy_database_wakes_total",
        "Compute cold starts triggered, per database",
        &["database"]
    )
    .unwrap()
});

/// 1 when a database's compute is running, 0 when hibernated. Integrate over
/// time (e.g. `sum_over_time`) for compute-seconds and idle ratio.
pub static DB_COMPUTE_UP: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec!(
        "aethel_proxy_database_compute_up",
        "1 when a database's compute is running, else 0",
        &["database"]
    )
    .unwrap()
});

/// Set the compute-up gauge for `database` (true = running).
pub fn set_compute_up(database: &str, up: bool) {
    DB_COMPUTE_UP.with_label_values(&[database]).set(up as i64);
}

/// Client connections accepted.
pub static CONNECTIONS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_proxy_connections_total", "Client connections accepted").unwrap()
});

/// Compute cold starts triggered (the activator was asked to wake a tenant).
pub static WAKES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_proxy_wakes_total", "Compute cold starts triggered").unwrap()
});

/// SCRAM authentication failures (rejected before any cold start).
pub static AUTH_FAILURES: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_proxy_auth_failures_total", "SCRAM authentication failures")
        .unwrap()
});

/// Idle tenants stopped by the reaper.
pub static IDLE_REAPS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_proxy_idle_reaps_total", "Idle compute nodes scaled to zero")
        .unwrap()
});

/// Connections currently splicing client <-> backend.
pub static ACTIVE_CONNECTIONS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("aethel_proxy_active_connections", "Connections currently splicing")
        .unwrap()
});

/// `CancelRequest`s successfully routed to the owning backend.
pub static CANCELS_ROUTED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_proxy_cancels_routed_total", "CancelRequests routed to a backend")
        .unwrap()
});

/// `CancelRequest`s for an unknown/expired key (dropped).
pub static CANCELS_UNKNOWN: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "aethel_proxy_cancels_unknown_total",
        "CancelRequests for an unknown session key"
    )
    .unwrap()
});

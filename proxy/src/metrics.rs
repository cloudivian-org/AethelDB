// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Prometheus metrics for the activation proxy.
//!
//! Counters/gauges register with the process-global registry and are exposed by
//! [`common::metrics::serve_metrics`].

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

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

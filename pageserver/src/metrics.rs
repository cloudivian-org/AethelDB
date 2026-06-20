// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The AethelDB Authors

//! Prometheus metrics for the page server. Exposed by
//! [`common::metrics::serve_metrics`].

use once_cell::sync::Lazy;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

/// `GetPage` requests served.
pub static GET_PAGE: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_pageserver_get_page_total", "GetPage requests served").unwrap()
});

/// WAL records ingested into the page store.
pub static WAL_RECORDS: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_pageserver_wal_records_total", "WAL records ingested").unwrap()
});

/// Layers offloaded to the object store.
pub static LAYERS_OFFLOADED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_pageserver_layers_offloaded_total", "Layers uploaded to object storage").unwrap()
});

/// Page versions removed by compaction/GC.
pub static GC_VERSIONS_REMOVED: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!("aethel_pageserver_gc_versions_removed_total", "Page versions removed by compaction/GC").unwrap()
});

/// Timelines (branches) currently known.
pub static TIMELINES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge!("aethel_pageserver_timelines", "Timelines (branches) currently known").unwrap()
});

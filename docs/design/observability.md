<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: observability (metrics, dashboards, tracing)

AethelDB is infrastructure — when it misbehaves, an operator needs to see *why*
fast. Three layers cover that: metrics (what's happening, cheaply, always),
dashboards (the at-a-glance view), and tracing (the per-request story).

## Metrics

Every service registers counters/gauges with a process-global Prometheus
registry and serves them over a tiny HTTP endpoint (`common::metrics`):

| Service | Port | Examples |
|---|---|---|
| proxy | `:9432` | `aethel_proxy_connections_total`, `…_active_connections`, `…_wakes_total`, `…_auth_failures_total`, `…_cancels_routed_total` |
| page server | `:9400` | `aethel_pageserver_get_page_total`, `…_wal_records_total`, `…_layers_offloaded_total`, `…_gc_versions_removed_total`, `…_timelines`, `…_control_auth_failures_total` |
| safekeeper | `:9500` | `aethel_safekeeper_appends_total` |

Counters are `Lazy` and only exported once first touched, so a metric appearing
in `/metrics` is itself a signal that the code path ran.

## Dashboards

`deploy/monitoring/` ships a ready-to-run **Prometheus + Grafana** stack: a
scrape config for the three services and a pre-provisioned **AethelDB Overview**
dashboard (rows per service). It runs as an optional Compose overlay; in
Kubernetes the pods are annotated for automatic scrape discovery. See
[`deploy/monitoring/README.md`](../../deploy/monitoring/README.md).

## Tracing

Logs/spans go through `tracing`. `common::telemetry::init(service_name)` sets up
the formatting subscriber for every binary and, **when built with the `otlp`
feature and `OTEL_EXPORTER_OTLP_ENDPOINT` is set**, also exports spans over OTLP
to a collector (Tempo / Jaeger / the OpenTelemetry Collector). The feature is
off by default, so the standard build pulls none of the OTLP dependencies and
behaves exactly as before — tracing export is purely opt-in.

The span context flows with the request: a cold start, the WAL append, and the
page reconstruction it unblocks share a trace, so a slow query can be followed
across the proxy, safekeeper, and page server.

## What's next

Exemplars linking metrics to traces, RED/USE alerting rules shipped alongside
the dashboard, and per-tenant metric labels (bounded to avoid cardinality
blow-ups).

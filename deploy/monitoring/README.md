<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Monitoring (Prometheus + Grafana)

Every AethelDB service exposes Prometheus metrics on its own `/metrics` port
(proxy `:9432`, page server `:9400`, safekeeper `:9500` — see
`common::metrics`). This directory wires a ready-to-run monitoring stack on top.

## Run it

Alongside the main Compose stack, on the same network:

```bash
docker compose -f docker-compose.yml \
               -f deploy/monitoring/docker-compose.monitoring.yml up -d
```

- **Prometheus** — http://localhost:9090 (scrapes the three services; config in
  [`prometheus.yml`](prometheus.yml)).
- **Grafana** — http://localhost:3000 (`admin`/`admin` — change it), with the
  Prometheus datasource and the **AethelDB Overview** dashboard pre-provisioned.

## What's in the dashboard

[`grafana/dashboards/aetheldb.json`](grafana/dashboards/aetheldb.json) — one
overview with rows per service:

- **Proxy** — client connection rate, active connections, cold starts, auth
  failures, cancels routed.
- **Page server** — GetPage rate, WAL records & layers offloaded, timelines
  (branches) known, control-plane auth failures.
- **Safekeeper** — quorum-committed WAL append rate.

## Kubernetes

The pods are annotated `prometheus.io/scrape: "true"` with their metrics port,
so a cluster Prometheus (e.g. kube-prometheus-stack) discovers them
automatically. Import the same dashboard JSON into your Grafana.

## Distributed tracing (optional)

Build the services with the `otlp` feature and set `OTEL_EXPORTER_OTLP_ENDPOINT`
to export spans to an OTLP collector (Tempo / Jaeger / the OpenTelemetry
Collector). See [`docs/design/observability.md`](../../docs/design/observability.md).

> The default Grafana credentials here are for local use only — change them
> before exposing anything (see [`../README.md`](../README.md) hardening).

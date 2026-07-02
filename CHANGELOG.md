<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Changelog

All notable changes to AethelDB are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Real compute boots from the page store** (base-image import): the patched
  PostgreSQL compute (`aethel_smgr`) can now serve real reads from the page
  server, not just a mock. A new `aethel-basebackup-import` tool seeds a timeline
  from an `initdb`'d data directory (full-page images over the ingest port, at
  LSN 0 — the LSN `aethel_smgr` requests as "latest"); an env-driven
  `compute/entrypoint.sh` renders the compute config from the environment
  (tenant / timeline / page server / safekeepers) and the Dockerfile runs it. The
  Kubernetes compute example maps the `aethel.io/timeline` annotation to
  `AETHEL_TIMELINE` (Downward API), so a **PITR restore takes effect in the real
  compute**. Verified locally end-to-end (`compute/verify-local.sh`): a real
  compute served 50 rows reconstructed from the page server. See `compute/README.md`
  for the honest remaining gaps (compute-side basebackup-on-start, seed-at-provision).
- **SQL-level metrics (Performance-Insights tier)**: the per-database Metrics view
  gains a **SQL · Postgres** section — transactions/sec, cache-hit ratio, active
  backends, and rows/sec — read from a `postgres_exporter` scraped per compute
  (labelled `database=<name>`). Until an exporter is wired the console shows a
  friendly hint; nothing breaks. Wiring is documented in
  `deploy/monitoring/postgres-exporter.md`. Verified end-to-end against a real
  Postgres 16 + `postgres_exporter` (tps, cache-hit %, backends, rows/sec all
  populated).
- **Opt-in keep-warm (zero cold start for chosen databases)**: mark a
  latency-sensitive database **keep-warm** and it is exempt from scale-to-zero —
  the idle reaper skips it and a background warmer keeps its compute started, so
  it never pays a cold start. Everything else keeps scaling to zero. New control
  routes `POST /tenants/<name>/keepwarm` and `/nokeepwarm`, a per-database toggle
  in the console, and a `keepWarm` flag in the tenant/database listings. Off by
  default — existing scale-to-zero behavior is unchanged. (This is cold-start
  *avoidance*; making the boot itself faster — snapshot/restore, working-set
  prefetch — needs the real compute image and is still on the roadmap.)
- **Compute timeline-pinning (PITR takes effect end-to-end)**: a point-in-time
  restore now **pins the database's compute to the restored timeline**. The proxy
  records the pin, hibernates compute, and passes the timeline to the activator on
  the next wake — `{timeline}` in a `CommandActivator` template, or the
  `aethel.io/timeline` pod annotation for the Kubernetes activator. New control
  routes `POST /tenants/<name>/pin/<tl>` and `/unpin`; the console's **Restore**
  wires them automatically. Non-restored tenants are unchanged (timeline `None`),
  so existing behavior is preserved.
- **Per-database metrics & charts** in the console: each database has a **📈
  Metrics** view with tiles (active now, peak, connections/cold-starts in the
  last hour) and native sparklines (connections/sec, active, compute up-vs-
  hibernated, cold starts). The headline is the **scale-to-zero economics** —
  *compute uptime* and *saved by scale-to-zero* — signals an always-on database
  (Aurora/RDS) can't show. Backed by new per-database (labelled) proxy metrics
  (`aethel_proxy_database_*`) and a Prometheus query proxy in `aethelctl serve`
  (`--prometheus-url`). Fully additive: no `--prometheus-url`, no charts; the
  engine and existing metrics are unchanged.

## [0.2.0] - 2026-06-21

The operational-layer release: multi-tenancy with a durable catalog, optional
pooling, control-plane auth, and full observability — on top of the 0.1.0 data
plane.

### Changed
- **DNS backends**: a tenant's backend is now a `host:port` string resolved at
  connect, so the proxy (and the auto-routing `--backend-template`, e.g.
  `compute-{tenant}:5432`) work with **Kubernetes Service DNS names**, not just
  IPs.

### Added
- **Automatic proxy routing**: the proxy's tenant registry is now dynamic — the
  control API gains `POST`/`DELETE /tenants/<name>` and a `--backend-template`, so
  a newly-provisioned database is routed **immediately, without `--tenant`
  flags**. The console registers/deregisters routes on create/delete.
- **Delete database (deprovision)**: the page server gains `DELETE
  /v1/tenants/<id>` (+ `TenantManager::remove`, persisted); the console's
  Databases view deletes a database — removing its tenant, proxy route, and local
  record.
- **Point-in-time recovery per database**: the Databases view re-surfaces
  branching as friendly **named restore points** — create one at an LSN (instant
  copy-on-write), see the list per database, **restore in-place** to any point,
  and **restore to live**. The console shows which point a database is serving.
- **Compute lifecycle (start / hibernate)**: the proxy exposes an optional
  compute-control HTTP API (`--control-listen`) for per-tenant running state and
  start/stop, over the same activator the idle reaper uses. The console's
  **Databases** view shows running vs hibernated and starts/hibernates on demand
  (`aethelctl serve --proxy-url …`), with a "running" count in the Overview.
- **Release workflow + published images**: a GitHub Actions workflow builds and
  pushes the `proxy`/`safekeeper`/`pageserver` images (multi-arch) to GHCR and
  publishes the Helm chart as an OCI artifact on each release tag. The chart now
  **defaults to those images** (`ghcr.io/<owner>/aetheldb/*`, appVersion tag), so
  `helm install` / `aethelctl deploy` need no `--image-repo`.
- **Web console** (`aethelctl serve`): a provisioning-first, embedded single-page
  GUI. **Databases** — create a database by *name* and get a connection string
  instantly (a friendly layer over tenants; the engine is unchanged). **Deploy**
  — on-prem / AWS / Azure / GCP from presets, with a live `helm --dry-run`
  preview and an optional streamed real apply (`--allow-apply`). `--grafana-url`
  embeds live Grafana panels. The browser never holds the control token. (The
  earlier low-level Tenants / Branches & PITR views were removed from the GUI in
  favor of the database/deploy flows.)
- **Helm autoscaling & availability** (opt-in): HPA for the proxy,
  PodDisruptionBudgets, and topology spread.
- **`aethelctl` CLI**: a scriptable client over the control plane —
  `status` / `tenant` / `timeline` / `branch` (alias `pitr`) / `receive` / `gc`,
  with `--json` output and bearer-token auth — plus `up` / `down` (Docker
  Compose) and `deploy` / `uninstall` (Helm). The Helm chart is **embedded in
  the binary**, so `aethelctl deploy --cloud aws|azure|gcp` works from anywhere.
  A new binary; the engine is unchanged.
- **Deploy to any cloud**: the page server offloads to **AWS S3 / Azure Blob /
  GCS** via `--object-store-url` (one binary, env-resolved credentials), plus a
  **Helm chart** (`deploy/helm/`) for EKS/AKS/GKE — server-side validated against
  a real cluster. See `deploy/helm/README.md`.
- **Multi-tenancy**: one page server hosts many isolated tenants. Reads route by
  `(TenantId, TimelineId)`; the line and HTTP/JSON control planes are
  tenant-aware (defaulting to the root tenant); tenants are provisioned on first
  reference. See `docs/design/multi-tenancy.md`.
- **Durable tenant catalog**: the tenant/timeline topology (ids + branch
  ancestry) is persisted to the object store and restored at startup, so it
  survives a restart. See `pageserver/src/catalog.rs`.
- **Optional PgBouncer pooling tier**: verified end-to-end (Docker) and on
  Kubernetes, with applyable demo fixtures. See `deploy/pooling/`.
- **Control-plane auth**: optional `--control-token` gates the line endpoint
  (`auth <token>`) and the HTTP API (`Authorization: Bearer <token>`);
  `/healthz` stays open.
- **Monitoring stack**: a ready-to-run Prometheus + Grafana setup
  (`deploy/monitoring/`) with a scrape config and a pre-provisioned **AethelDB
  Overview** dashboard. Verified by a real scrape of a live service.
- **OTLP tracing (optional)**: a shared `common::telemetry::init` installs the
  fmt subscriber for every binary and, when built with the `otlp` feature and
  `OTEL_EXPORTER_OTLP_ENDPOINT` is set, exports spans over OTLP. Off by default —
  the standard build pulls no OpenTelemetry dependencies. See
  `docs/design/observability.md`.
- **`--wal-redo` wiring**: select the real Postgres wal-redo backend at runtime.
- **Compressed full-page images**: decode `pglz` / `lz4` / `zstd` FPIs.
- **Proxy `CancelRequest` routing**: cancels reach the backend that owns the
  session.
- CI (GitHub Actions) and an expanded end-to-end suite (HTTP control plane +
  metrics); OSS community files (`SECURITY.md`, `CODE_OF_CONDUCT.md`, issue/PR
  templates); a deploy **Security hardening** checklist.

## [0.1.0] - 2026-06-21

The first release: a working, end-to-end-tested serverless-PostgreSQL data plane.

### Compute & WAL redo
- PostgreSQL 16 storage-manager patch (`smgr` pluggable) so compute fetches pages
  over the network and streams WAL out.
- WAL decode/redo subsystem: a real PG WAL stream decoder, a page-store redo
  seam, and a `PostgresRedoManager` driving a child `postgres --wal-redo` process
  (a verified core-mode patch) so non-full-page records redo through Postgres
  itself. Selectable at runtime via `--wal-redo`.

### Page server
- Log-structured page store with reconstruction at any LSN.
- **Instant branching & point-in-time**: timeline-aware store with copy-on-write
  reconstruction across the ancestor chain.
- **Compaction & branch-aware GC**, with offload to S3-compatible object storage
  (AWS S3 / MinIO) and post-compaction object deletion.
- Control plane: a line-oriented endpoint and an **HTTP/JSON API**
  (timelines, branches, per-branch WAL ingest, GC).

### Safekeeper
- Durable, segmented WAL with quorum commit.
- **Real over-the-network replication** to peers and **leader election** (vote
  RPC) to prevent split-brain.

### Proxy
- Scale-to-zero activation (cold-start on connect, idle reaping).
- **TLS termination** and proxy-side **SCRAM-SHA-256** authentication (rejects
  bad credentials before a cold start).
- **Kubernetes-native activator** (opt-in) that scales a per-tenant compute
  Deployment, verified against a real cluster.

### Operations
- Prometheus `/metrics` on every service.
- Docker Compose stack and Kubernetes (Kustomize) manifests with RBAC.
- CI (fmt, clippy, build, test, MSRV) and a Python end-to-end suite.

[Unreleased]: https://github.com/cloudivian-org/AethelDB/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/cloudivian-org/AethelDB/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cloudivian-org/AethelDB/releases/tag/v0.1.0

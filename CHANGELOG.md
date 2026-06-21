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

[Unreleased]: https://github.com/cloudivian-org/AethelDB/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cloudivian-org/AethelDB/releases/tag/v0.1.0

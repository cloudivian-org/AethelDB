<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: multi-tenancy

One page server hosts many tenants. A **tenant** is a fully isolated database
("project"): its own set of timelines (branches), its own page store, its own
GC. Nothing is shared between tenants except the process and the stateless
WAL-redo backend.

## Identity is already on the wire

The page and WAL protocols (`common::page_service`, `common::wal_service`) carry
a `TenantId` on every request — a `GetPage`/`GetRelSize` names its
`(tenant, timeline)`, and WAL ingest names its tenant. So multi-tenancy is a
*routing* problem on the server, not a protocol change.

## The manager

`TenantManager` (`pageserver/src/tenant_manager.rs`) owns a
`HashMap<TenantId, Arc<Tenant>>` behind an `RwLock`:

- **`get(id)`** — resolve a tenant, or `None`.
- **`get_or_create(id)`** — resolve, **lazily provisioning** an empty tenant on
  first reference (an `entry(...)` under the write lock resolves the
  get→insert race). A `TenantId` that appears in a request or control call
  simply comes into being — no separate provisioning round-trip.
- **`create(id)`** — explicit creation; errors if the tenant already exists.
- **`tenants()` / `gc_all(horizon)`** — cross-tenant work (offload, GC).

Each tenant is built with the configured freeze threshold and the shared redo
backend (`build_tenant`). `single(tenant)` wraps one pre-built tenant at
`TenantId::ZERO` for single-tenant embeddings and tests.

## Routing

- **Reads** — `serve_pages` takes the manager and routes each request by
  `(req.tenant, req.timeline)`: `manager.get(tenant).get_timeline(timeline)`. An
  unknown tenant *or* timeline is a clean `Response::Error`, never a wrong page.
- **WAL ingest** — the per-branch WAL receiver targets a specific timeline; the
  control plane threads the tenant id through when it attaches one.
- **Offload** — the background worker iterates every tenant's every timeline.

## Control plane

Both control planes are tenant-aware, defaulting to the root tenant
(`TenantId::ZERO`) for backward compatibility:

- **Line protocol** (`control.rs`) — a connection has a *current tenant*;
  `tenant <hex>` switches it (provisioning on first use) and `tenants` lists
  known ids. `create` / `branch` / `receive` / `gc` / `list` act on the current
  tenant.
- **HTTP/JSON** (`httpapi.rs`) — `GET`/`POST /v1/tenants` manage tenants; every
  `/v1` operation takes an optional `"tenant": "<hex>"` (POST body) or
  `?tenant=<hex>` (GET), defaulting to the root tenant.

The default tenant and its root timeline are pre-provisioned at startup, so the
single-tenant path (and the legacy ingest endpoint) works out of the box.

## Durability

The tenant/timeline **topology** (which tenants exist, each timeline, and each
branch's ancestry) is persisted to the object store as a single small JSON
catalog (`catalog.rs`, `catalog/topology.json`), rewritten after each
create/branch and reloaded at startup — so the shape of the world survives a
restart rather than being reconstructed only as ids are referenced. Page *data*
already lives durably as immutable layers; rehydrating a timeline's pages from
those layers on restart is the complementary step, tracked separately.

## What this is not (yet)

Still ahead on the operational layer: per-tenant resource limits/quotas, per-
tenant object-store prefixes, and authn/authz on the control plane — today it is
unauthenticated, so keep it internal (see `deploy/README.md`).

<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# AethelDB compute

The **compute node**: a PostgreSQL 16 server, patched so its storage manager
(`aethel_smgr`) fetches every non-temp page from the **page server** over the
network and streams WAL to the **safekeeper** quorum for durability — no local
heap files. This is what makes compute stateless and scale-to-zero: the data
lives in the page store, and any compute pod can serve any timeline.

## Layout

- `Dockerfile` — two-stage build: compile patched Postgres + the `aethel_smgr`
  extension, then a slim runtime carrying only the install tree.
- `patches/` — the core patches (`smgr` pluggable, wal-redo mode).
- `extension/aethel_smgr/` — the network storage manager (installs the
  `smgr_hook`; `smgr_read` → a GetPage over TCP; writes are no-ops because
  durability is the safekeeper quorum).
- `entrypoint.sh` — env-driven bootstrap: render the compute config from the
  environment (tenant / timeline / page server / safekeepers) and exec postgres.
- `postgresql.compute.conf` — the annotated reference config.
- `verify-local.sh` — an end-to-end local proof (below).

## Entrypoint environment

| Env | Meaning |
|-----|---------|
| `AETHEL_TENANT` | tenant id (32 hex) |
| `AETHEL_TIMELINE` | timeline id (32 hex) — a **PITR restore** sets this via the `aethel.io/timeline` pod annotation (Downward API → env) |
| `AETHEL_PAGESERVER_HOST` / `AETHEL_PAGESERVER_PORT` | where pages come from |
| `AETHEL_SAFEKEEPERS` | comma-separated `application_name`s for the `ANY (n/2+1)` commit quorum |

See `deploy/k8s/compute-example.yaml` for a per-tenant Deployment that maps the
annotation to `AETHEL_TIMELINE`, closing the timeline-pinning loop.

## Base-image import

A freshly provisioned timeline has **no pages**, so a compute can't read from it
yet. `aethel-basebackup-import` seeds it: it walks an `initdb`'d data directory
and pushes every relation block to the page server's ingest port as a full-page
image (at LSN 0, since `aethel_smgr` requests "latest" as LSN 0).

```
aethel-basebackup-import --pgdata <DIR> --ingest <HOST:PORT> [--lsn N]
```

## Prove it locally

With a local compute build at `compute/install` (see `compute/Makefile`):

```
bash compute/verify-local.sh
```

It seeds a table on local disk, imports the data dir into a **fresh page
server**, then boots the **real patched Postgres** with `aethel_smgr` and
`SELECT`s the rows back — reconstructed from the page store, not local disk:

```
rows read from the page server: 50
PASS: the real compute served 50 rows from the page server (not local disk).
```

## Status — what works, what's next

**Works (verified locally):** the patched Postgres + `aethel_smgr` compiles,
loads, and serves real `SELECT`s from the page server after a base-image import;
the entrypoint renders a timeline-pinned config from the environment.

**Next (honest gaps):**

- **Compute-side basebackup on start** — a stateless wake still relies on a local
  `PGDATA` for the non-relation files (`pg_control`, `pg_filenode.map`, config).
  A fully stateless compute pulls those from the page server at start; today the
  entrypoint `initdb`s them locally.
- **Seed-at-provision** — wire the base-image import into database provisioning
  (initdb a template once, import to the new timeline) so a first connection
  finds a populated timeline.
- **Segmented relations** (>1 GiB, `<rel>.N`) and the init fork in the importer.
- **Snapshot/prefetch** cold-start optimizations (see `docs/world-class.md`).

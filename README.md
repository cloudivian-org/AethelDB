<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# AethelDB

A decoupled, serverless PostgreSQL platform. Compute (a stateless PostgreSQL
engine) is separated from storage (a virtual, log-structured page layer), so a
database can scale to zero when idle, cold-start on the next connection, and
branch its history instantly.

The name comes from *aether* — the clear upper air — for a database whose
compute is stateless and "in the air": scale-to-zero, serverless, weightless
when idle.

> **Project status: all six steps complete — end-to-end validated.** A runnable
> integration test drives the real `aethel-proxy`, `aethel-safekeeper`, and
> `aethel-pageserver` through their real wire protocols and proves the full
> lifecycle: a query cold-starts compute via the proxy, an INSERT streams WAL to
> the quorum-committed safekeeper and materializes a block in the page server,
> and the data survives an idle scale-to-zero and is read back after
> re-activation. The PostgreSQL compute engine is a verified patch + extension
> (it compiles against real PG 16) with a mock standing in for it where a live
> server cannot boot; a `psycopg`/Docker suite covers the full-PG deployment.

> **In progress — real PostgreSQL WAL.** A WAL decode/redo subsystem makes the
> page server ingest genuine PostgreSQL WAL (not modelled deltas) and materialize
> pages through Postgres's own redo routines. Landed: the WAL stream decoder, the
> page-store redo seam, a live safekeeper→page-server WAL receiver, the wal-redo
> pipe protocol + process supervisor, and a `postgres --wal-redo` core mode
> (`compute/patches/0002-wal-redo-mode.patch`) verified to reconstruct a page
> byte-identically from a real WAL record. See
> [`docs/design/wal-redo.md`](./docs/design/wal-redo.md).

## Architecture

```
           ┌──────────────┐   startup packet   ┌──────────────────┐
client ───▶│ aethel-proxy │───── wake/route ──▶│  compute (PG 16)  │
  5432     └──────────────┘   scale-to-zero    └───────┬───────────┘
                                                get_page│    │ stream WAL
                                                @lsn    │    ▼
                                    ┌───────────────────▼─┐ ┌───────────────────┐
                                    │  aethel-pageserver  │ │  aethel-safekeeper │
                                    │  reconstruct pages  │ │  WAL quorum +      │
                                    │  @ LSN              │◀┘  durability        │
                                    └──────────┬──────────┘ └───────────────────┘
                                       S3 /    │ MinIO (immutable layers)
                                               ▼
```

- **compute** — PostgreSQL 16 built from source and patched so its storage
  manager fetches 8 KiB pages over the network and streams WAL out instead of
  touching local disk. **(Step 3 — implemented: `0001-smgr-pluggable.patch` +
  the `aethel_smgr` extension)**
- **aethel-proxy** — activation proxy on `:5432`; parses the startup packet, wakes
  the tenant's compute node (holding the client socket open), splices the
  connection, and scales compute to zero after an idle timeout. **(Step 2 —
  implemented)**
- **aethel-safekeeper** — durable WAL ingest buffer; acknowledges an LSN to compute
  only once a quorum has the bytes. **(Step 4 — implemented: segmented durable
  store + consensus)**
- **aethel-pageserver** — indexes WAL by `(PageKey, LSN)`, reconstructs any
  historical page by replaying deltas over a base image, and offloads immutable
  layers to S3-compatible storage. **(Step 5 — implemented: memtable + layers +
  reconstruction + offload worker)**

## Repository layout

```
AethelDB/
├── Cargo.toml            # virtual Rust workspace
├── Makefile              # top-level dev entry point (`make help`)
├── docker-compose.yml    # local stack: services + MinIO object store
├── rust-toolchain.toml   # recommended toolchain (stable); MSRV pinned to 1.75
├── common/               # shared wire types: Lsn, TenantId, PageKey, …
├── proxy/                # aethel-proxy        (Step 2)
├── safekeeper/           # aethel-safekeeper   (Step 4)
├── pageserver/           # aethel-pageserver   (Step 5)
├── compute/              # patchable PostgreSQL build (Dockerfile + Makefile)
│   ├── patches/          # 0001-smgr-pluggable.patch (Step 3)
│   └── extension/aethel_smgr/# network storage-manager extension (Step 3)
├── deploy/               # shared Dockerfile for the Rust services
└── e2e-tests/            # Python lifecycle suite (Step 6 — runnable + Docker)
```

## Quickstart

```bash
# Build and test the Rust control/data-plane services
make build
make test

# Build the patchable PostgreSQL compute image (requires Docker)
make compute-image

# Bring up the local stack (safekeeper, pageserver, MinIO, proxy)
make up

# Run the end-to-end lifecycle tests
make e2e
```

Each service is independently runnable and configurable by flag or environment
variable, e.g.:

```bash
cargo run --bin aethel-pageserver -- --listen 0.0.0.0:6400 --object-store http://localhost:9001
```

## Licensing

All source files carry an `SPDX-License-Identifier: Apache-2.0` header and the
full license text is in [`LICENSE`](./LICENSE).

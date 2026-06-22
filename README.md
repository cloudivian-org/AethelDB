<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# AethelDB

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/cloudivian-org/AethelDB?color=success)](https://github.com/cloudivian-org/AethelDB/releases)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange.svg?logo=rust)](https://www.rust-lang.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Tests](https://img.shields.io/badge/tests-147%20passing-brightgreen.svg)](#status--roadmap)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

**An open-source, serverless PostgreSQL platform** — compute (a stateless
PostgreSQL engine) is separated from storage (a virtual, log-structured page
layer), so a database can **scale to zero** when idle, **cold-start** on the next
connection, **branch its history instantly**, and **time-travel** to any past
point — all while speaking the real PostgreSQL wire protocol to unmodified
clients.

The name comes from *aether* — the clear upper air — for a database whose compute
is stateless and "in the air": scale-to-zero, serverless, weightless when idle.

> **Status:** a working, end-to-end-tested data plane. The four services build
> from source, **147 tests pass with zero warnings**, and the hard parts are real
> — including a verified `postgres --wal-redo` backend that reconstructs pages
> byte-for-byte from genuine PostgreSQL WAL. The remaining work is the production
> *operational* layer (orchestration, multi-tenant control plane); see
> [Status & roadmap](#status--roadmap).

---

## Capabilities

| Area | What works |
|---|---|
| **Serverless compute** | Scale-to-zero activation proxy: cold-starts a tenant's compute on connect, holds the client socket during wake, scales back to zero after idle. |
| **Decoupled storage** | A stateless PostgreSQL fetches 8 KiB pages over the network and streams WAL out — no local durable disk. |
| **Real WAL decode + redo** | The page server decodes genuine PostgreSQL WAL and materializes any page at any LSN through Postgres's own `rm_redo` (a verified `postgres --wal-redo` core mode). |
| **Instant branching** | Create a branch off any timeline at any LSN in O(1) — copies nothing; reads are copy-on-write across the ancestor chain. |
| **Point-in-time (PITR)** | Read any page as of any past LSN; branch from a past point to recover or experiment. |
| **Durable WAL** | Quorum-replicated safekeepers over the network, with leadership election to prevent split-brain. |
| **Storage at scale** | LSM-style layers, compaction + branch-aware garbage collection, and offload to S3-compatible object storage (AWS S3 / MinIO). |
| **Secure ingress** | TLS termination and proxy-side SCRAM-SHA-256 authentication (rejects bad credentials *before* a cold start). |
| **Multi-tenancy** | One page server hosts many fully-isolated tenants; reads and control ops route by `TenantId`, tenants are provisioned on first reference, and the tenant/timeline topology is **persisted to the object store** so it survives a restart. |
| **Observability** | Prometheus `/metrics` on every service, a ready-to-run Grafana dashboard, and optional OpenTelemetry/OTLP trace export. |

## Architecture

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/architecture-dark.png">
    <img src="docs/assets/architecture.png" width="900"
         alt="AethelDB architecture: a client connects to aethel-proxy (wake, route, TLS, SCRAM, scale-to-zero), optionally through a PgBouncer pooling tier, to a stateless PostgreSQL 16 compute. Compute streams WAL to quorum-replicated aethel-safekeepers and fetches pages (get_page @LSN) from the aethel-pageserver, which decodes and redoes WAL through Postgres, reconstructs any page at any LSN for instant branching and PITR, compacts and GCs, and offloads immutable layers to S3/MinIO. Every service exposes Prometheus metrics with a Grafana dashboard and optional OTLP tracing.">
  </picture>
</p>

<p align="center"><sub>Vector sources: <a href="docs/assets/architecture.svg">architecture.svg</a> (light) · <a href="docs/assets/architecture-dark.svg">architecture-dark.svg</a> (dark).</sub></p>

- **compute** — PostgreSQL 16 from source, patched so its storage manager fetches
  pages over the network and streams WAL out, plus a `--wal-redo` mode used by the
  page server. (`compute/`)
- **aethel-proxy** — scale-to-zero activation proxy: parses the startup packet,
  wakes the tenant's compute, terminates TLS, optionally authenticates via SCRAM,
  splices the connection, and idles compute to zero. (`proxy/`)
- **aethel-safekeeper** — durable WAL ingest with quorum commit, real
  over-the-network replication, and leader election. (`safekeeper/`)
- **aethel-pageserver** — decodes WAL, reconstructs pages through a WAL-redo
  backend, serves branches with copy-on-write, compacts/GCs, and offloads layers
  to object storage. (`pageserver/`)

## Repository layout

```
AethelDB/
├── Cargo.toml            # virtual Rust workspace
├── Makefile              # top-level dev entry point (make help)
├── docker-compose.yml    # local stack: services + MinIO
├── common/               # shared wire types, protocols, metrics helper
├── proxy/                # aethel-proxy
├── safekeeper/           # aethel-safekeeper
├── pageserver/           # aethel-pageserver
├── compute/              # patchable PostgreSQL build + wal-redo
│   ├── patches/          # 0001-smgr-pluggable, 0002-wal-redo-mode
│   └── walredo/          # wal-redo verification harness
├── deploy/               # Dockerfile, k8s manifests, hardening guide
│   ├── k8s/              # Kustomize manifests (+ optional pgbouncer.yaml)
│   ├── pooling/          # optional PgBouncer tier + verify scripts
│   └── monitoring/       # Prometheus + Grafana stack & dashboard
├── e2e-tests/            # Python lifecycle suite
└── docs/
    ├── design/           # design docs (one per subsystem)
    └── assets/           # architecture diagram (svg + png)
```

## Quickstart

```bash
# Build and test the Rust control/data-plane services
make build
make test

# Build the patchable PostgreSQL compute image + wal-redo (requires Docker or a
# local toolchain); see compute/ for the source build.
make compute-image

# Bring up the local stack (safekeeper, pageserver, MinIO, proxy)
make up        # tear down with: make down

# Run the Python end-to-end lifecycle suite
make e2e
```

For deployment — the wired-together Compose stack and Kubernetes manifests —
see [`deploy/README.md`](deploy/README.md).

Each service is an independently runnable binary configured by flag or env var:

```bash
aethel-pageserver --listen 0.0.0.0:6400 --safekeeper 127.0.0.1:6500 \
  --s3-endpoint http://localhost:9000 --s3-bucket aethel
aethel-safekeeper --node-id 1 --members 1,2,3 --peer-addrs 2=h2:6500,3=h3:6500
aethel-proxy --tenant mydb=127.0.0.1:5433 --tls-cert cert.pem --tls-key key.pem
```

## Using it

### Branching, PITR, and GC (page-server control endpoint)

A small line-oriented control endpoint (default `:6402`) manages timelines:

```
create <timeline-hex>                       # a fresh root timeline
branch <new-hex> <parent-hex> <lsn>         # instant branch at an LSN (PITR point)
receive <timeline-hex> <sk-host:port> <lsn> # stream WAL into a timeline
gc <horizon-lsn>                            # compact + branch-aware GC (+ S3 cleanup)
list                                        # known timelines
```

```bash
# Branch "dev" off main as of LSN 5000, then stream WAL into it:
printf 'branch 0000...02 0000...00 5000\nreceive 0000...02 127.0.0.1:6500 5000\n' \
  | nc localhost 6402
```

A branch shares all of its parent's history up to the branch point and diverges
only as it's written — reading a page the branch hasn't touched transparently
reconstructs it from the parent (copy-on-write). See
[`docs/design/branching.md`](docs/design/branching.md).

### HTTP control-plane API

The same operations are available as a JSON API (default `:6403`) for a control
plane or `aethelctl`-style tooling:

```bash
# Start the page server with --control-token <secret> to require auth; then
# pass `-H "Authorization: Bearer <secret>"` on every /v1 call (/healthz is open).
curl localhost:6403/healthz
curl -XPOST localhost:6403/v1/tenants    -d '{"id":"<32-hex>"}'
curl       localhost:6403/v1/tenants
curl -XPOST localhost:6403/v1/timelines  -d '{"id":"<32-hex>"}'                       # optional "tenant":"<hex>"
curl -XPOST localhost:6403/v1/branches   -d '{"timeline":"<hex>","parent":"<hex>","lsn":5000}'
curl -XPOST localhost:6403/v1/timelines/receive -d '{"timeline":"<hex>","safekeeper":"127.0.0.1:6500","start_lsn":0}'
curl -XPOST localhost:6403/v1/gc          -d '{"horizon_lsn":4000}'
curl       'localhost:6403/v1/timelines?tenant=<hex>'   # omit ?tenant for the root tenant
```

### `aethelctl` (CLI)

The same control plane, as a scriptable CLI (`aethelctl`) instead of raw `curl`:

```bash
export AETHEL_SERVER=http://localhost:6403   # and AETHEL_TOKEN=… if auth is on
aethelctl status                              # health + tenant/timeline summary
aethelctl tenant create <32-hex>
aethelctl timeline create <32-hex> --tenant <hex>
aethelctl pitr <new-hex> --from <parent-hex> --lsn 5000 --tenant <hex>   # branch = PITR
aethelctl gc 4000 --tenant <hex> --json
```

Every command takes `--json` for scripting. See [`ROADMAP.md`](ROADMAP.md) for the
`aethelctl deploy --cloud …` work.

### Metrics

```bash
curl localhost:9400/metrics   # pageserver
curl localhost:9432/metrics   # proxy
curl localhost:9500/metrics   # safekeeper
```

## Design docs

Each subsystem has a focused design doc under [`docs/design/`](docs/design/):

- [`wal-redo.md`](docs/design/wal-redo.md) — WAL decode + redo (incl. the
  `postgres --wal-redo` core mode).
- [`branching.md`](docs/design/branching.md) — timelines, instant branching, PITR.
- [`compaction-gc.md`](docs/design/compaction-gc.md) — layer compaction, branch-aware GC.
- [`safekeeper-replication.md`](docs/design/safekeeper-replication.md) — WAL replication + leader election.
- [`proxy-tls.md`](docs/design/proxy-tls.md) — TLS termination, SCRAM auth, CancelRequest routing, the pooling decision.
- [`multi-tenancy.md`](docs/design/multi-tenancy.md) — tenant isolation and routing across one page server.
- [`observability.md`](docs/design/observability.md) — metrics, Grafana dashboards, and optional OTLP tracing.

## Status & roadmap

See the full [**Roadmap**](ROADMAP.md) (including the cloud / BYOC deploy plan and
the compatibility charter) and an honest [**comparison vs Neon & Supabase**](docs/comparison.md).

**Done and tested:**

- **Data path** — scale-to-zero proxy (TLS + SCRAM + `CancelRequest` routing),
  quorum-replicated safekeepers with leader election, a page server that decodes
  real PostgreSQL WAL (incl. `pglz`/`lz4`/`zstd` FPIs) and redoes it through
  Postgres, instant branching + PITR with copy-on-write, compaction + branch-aware
  GC, and S3 offload.
- **Operational layer** — **multi-tenancy** (route by `TenantId`) with a
  **durable tenant catalog** that survives restart; a **token-gated** HTTP/JSON +
  line control plane; the **Kubernetes activator** (`proxy --features kubernetes`);
  an **optional PgBouncer pooling** tier (verified on Docker + Kubernetes);
  and **observability** — Prometheus metrics, a Grafana dashboard, and optional
  OTLP tracing.

**Next:**

- **Per-tenant quotas & fine-grained authz** (per-tenant identities/scopes beyond
  the single operator token) and per-tenant object-store prefixes.
- **Page-data rehydration on restart** — the catalog restores topology today;
  reloading a timeline's pages from its object-store layers is the complement.
- **Alerting & exemplars** — alert rules shipped with the dashboard and
  metric↔trace exemplars, on top of the existing
  [observability](docs/design/observability.md) stack.

## Contributing

Contributions are welcome — see [`CONTRIBUTING.md`](CONTRIBUTING.md) for the dev
setup, testing expectations, and the conventions this project follows (one
focused PR per change, a design doc per subsystem, every change building green
with zero warnings).

- **Code of conduct:** [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- **Security:** report privately — see [`SECURITY.md`](SECURITY.md)
- **Changes:** [`CHANGELOG.md`](CHANGELOG.md)

## License

Apache 2.0. Every source file carries an `SPDX-License-Identifier: Apache-2.0`
header; the full text is in [`LICENSE`](LICENSE).

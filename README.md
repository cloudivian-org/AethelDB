<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# AethelDB

**An open-source, serverless PostgreSQL platform** — compute (a stateless
PostgreSQL engine) is separated from storage (a virtual, log-structured page
layer), so a database can **scale to zero** when idle, **cold-start** on the next
connection, **branch its history instantly**, and **time-travel** to any past
point — all while speaking the real PostgreSQL wire protocol to unmodified
clients.

The name comes from *aether* — the clear upper air — for a database whose compute
is stateless and "in the air": scale-to-zero, serverless, weightless when idle.

> **Status:** a working, end-to-end-tested data plane. The four services build
> from source, **142 tests pass with zero warnings**, and the hard parts are real
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

```
            ┌──────────────┐   startup / SSLRequest / SASL   ┌──────────────────┐
 client ───▶│ aethel-proxy │────── wake / route / TLS ──────▶│  compute (PG 16)  │
   5432     └──────────────┘       scale-to-zero             └───────┬───────────┘
                                                       get_page @lsn │   │ stream WAL
                                                                     │   ▼
                                          ┌──────────────────────────▼─┐ ┌────────────────────┐
                                          │     aethel-pageserver       │ │   aethel-safekeeper │
                                          │  decode WAL · redo · branch │ │  WAL quorum +       │
                                          │  reconstruct pages @ LSN    │◀┘  replication + vote │
                                          │  compaction · GC            │ └────────────────────┘
                                          └──────────────┬──────────────┘
                                              S3 / MinIO  │ (immutable layers)
                                                          ▼
```

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
├── e2e-tests/            # Python lifecycle suite
└── docs/design/          # design docs (one per subsystem)
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
curl -XPOST localhost:6403/v1/timelines  -d '{"id":"<32-hex>"}'
curl -XPOST localhost:6403/v1/branches   -d '{"timeline":"<hex>","parent":"<hex>","lsn":5000}'
curl -XPOST localhost:6403/v1/timelines/receive -d '{"timeline":"<hex>","safekeeper":"127.0.0.1:6500","start_lsn":0}'
curl -XPOST localhost:6403/v1/gc          -d '{"horizon_lsn":4000}'
curl       localhost:6403/v1/timelines
```

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

**Done and tested:** the full data path — scale-to-zero proxy (TLS + SCRAM),
quorum-replicated safekeepers with election, a page server that decodes real
PostgreSQL WAL and redoes it through Postgres, instant branching + PITR with
copy-on-write, compaction + branch-aware GC, S3 offload, and Prometheus metrics.

**Next — the operational layer** (production hosting, not core architecture):

- **Control-plane catalog & authz** — the page server is **multi-tenant** today
  (reads and the HTTP/JSON + line control planes route by `TenantId`, tenants
  provisioned on first reference; see
  [`docs/design/multi-tenancy.md`](docs/design/multi-tenancy.md)). Still ahead: a
  *durable* tenant/project catalog that survives restart, per-tenant
  quotas, and authn/authz on the control plane. Compute orchestration is handled
  by the **Kubernetes activator** — `proxy --features kubernetes`, see
  [`docs/design/k8s-activator.md`](docs/design/k8s-activator.md).
- **Alerting & exemplars** on top of the metrics — the Prometheus + Grafana
  stack and optional OTLP tracing already ship (see
  [`docs/design/observability.md`](docs/design/observability.md)); alert rules
  and metric↔trace exemplars are next.
- **Pooling** — composed via PgBouncer rather than reimplemented, available as an
  optional tier (verified end-to-end + on Kubernetes; see
  [`deploy/pooling/README.md`](deploy/pooling/README.md) and
  [`docs/design/proxy-tls.md`](docs/design/proxy-tls.md)).

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

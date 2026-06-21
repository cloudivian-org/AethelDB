<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Optional connection pooling (PgBouncer)

AethelDB does **not** reimplement connection pooling — it composes with
[PgBouncer](https://www.pgbouncer.org/), the mature, battle-tested pooler (see
the rationale in [`docs/design/proxy-tls.md`](../../docs/design/proxy-tls.md)).
Pooling is **optional**: enable it when you want many short-lived client
connections to share a small set of server connections — exactly what a
serverless, scale-to-zero compute benefits from.

## Topology

```
client ──▶ aethel-proxy ──▶ PgBouncer ──▶ compute (PostgreSQL)
   5432    wake/route/TLS    pooling       the tenant's stateless PG
```

The activation proxy stays in front (it does the things PgBouncer does *not* —
scale-to-zero wake, tenant routing, TLS termination, SCRAM pre-auth). PgBouncer
sits between the proxy and compute and does transaction pooling.

## Verify it end to end

Two self-contained tests let **anyone** reproduce the pooling verification — no
patched compute image, no full AethelDB stack required.

**Docker** — stock Postgres + PgBouncer + the `aethel-proxy` binary, driving a
real `psql` query through the whole chain:

```bash
cargo build -p proxy
deploy/pooling/verify-pooling.sh
# -> PASS: psql -> aethel-proxy -> pgbouncer -> postgres works (pooling tier verified)
```

**Kubernetes** — apply the self-contained demo
([`k8s-demo.yaml`](k8s-demo.yaml): stock Postgres + PgBouncer in their own
namespace) and drive `psql` through the pooler Service:

```bash
deploy/pooling/verify-pooling-k8s.sh            # apply, verify, leave running
deploy/pooling/verify-pooling-k8s.sh --cleanup  # ... and tear down afterwards
# -> PASS: psql -> pgbouncer (Service) -> compute works on Kubernetes
```

Or drive it by hand:

```bash
kubectl apply -f deploy/pooling/k8s-demo.yaml
kubectl -n aethel-pool-demo port-forward svc/pgbouncer 6432:6432 &
PGPASSWORD=postgres psql "host=127.0.0.1 port=6432 dbname=mydb user=postgres" -c "select 1"
kubectl delete -f deploy/pooling/k8s-demo.yaml
```

## Enable it

**Kubernetes** — apply the optional manifest (it is intentionally **not** in the
default `kustomization.yaml`):

```bash
kubectl apply -f deploy/k8s/pgbouncer.yaml
```

Point the upstream at your compute by setting the Deployment's `DB_HOST` (default
`compute`), then route clients at the `pgbouncer` Service (`:6432`) — either by
pointing the proxy's tenant target at it, or by running PgBouncer as a sidecar in
the compute Pod so `compute:5432` clients transparently pool.

**Standalone / Compose** — run PgBouncer next to compute and set the proxy's
tenant backend to the pooler instead of compute directly:

```bash
aethel-proxy --tenant 'mydb=<pgbouncer-host>:6432' ...
```

`POOL_MODE=transaction` is the default and the right choice for most serverless
workloads; use `session` only if a client needs session-level features
(`SET`, advisory locks, `WITH HOLD` cursors, prepared statements without
`max_prepared_statements`).

## Security

The bundled config uses `AUTH_TYPE=trust` for local/in-cluster testing. In
production set `AUTH_TYPE=scram-sha-256` with a userlist or `auth_query`, keep
PgBouncer on the trusted network, and follow the
[deploy hardening checklist](../README.md#security-hardening).

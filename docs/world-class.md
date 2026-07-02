<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# The path to a world-class product

This is the north star: what AethelDB **is**, what it is **deliberately not**, the
observability bet (better than Aurora *for the things that matter*), and the
honest gap-list to "world-class."

## What we are — and are not (don't drift)

**We are:** a fully open, **self-hostable, Bring-Your-Own-Cloud serverless
PostgreSQL** — the Neon *architecture* (disaggregated compute/storage, scale-to-
zero, instant branching, PITR), open all the way down to the control plane, that
you run in **your** infrastructure.

**We are not** (and should not drift into):

- a **managed SaaS** (that's Neon's business — ours is the open engine you run);
- a **Postgres app platform** — Auth, auto-REST/GraphQL, Realtime, object
  Storage, Edge Functions (that's Supabase's axis). Tempting, but **off-intent**.
  Every feature we add should make the *database infrastructure* better to
  **provision, operate, observe, and trust** — not turn AethelDB into a backend-
  as-a-service.

The guardrail: when evaluating a feature, ask *"does this make the open,
self-hosted, serverless Postgres engine better to run?"* If yes (deploy, scale,
branch, recover, observe, secure, cost), build it. If it's app-layer DX, it
belongs in a separate project that sits **on top** of AethelDB.

## Differentiators (what we lean into)

1. **Truly open & self-hostable** — including the control plane, CLI, and
   console. Neon's engine is open but the product is SaaS; we are the product,
   open.
2. **BYOC / data sovereignty** — `aethelctl deploy --cloud …` into your own
   account; data never leaves your tenancy.
3. **Scale-to-zero economics** — pay (and run) compute only when used. This is
   also our best *metric* story (below).
4. **Instant branching + PITR as first-class UX** — copy-on-write branches and
   point-in-time restore that no traditional Postgres (or Aurora) offers.

## Observability — "better than Aurora" for what matters

Aurora's monitoring (CloudWatch + Performance Insights) is excellent at
*always-on instance* metrics: CPU, IOPS, connections, top SQL, wait events. We
should match the table stakes **and win on the signals our architecture uniquely
produces** — which Aurora structurally cannot show.

### Table stakes (match): per-database SQL-level metrics
- QPS, query latency (p50/p95/p99), cache hit ratio, active sessions, locks,
  replication/branch lag, errors.
- **How:** a `postgres_exporter` alongside each compute (scraped per database),
  plus `pg_stat_statements` for a Performance-Insights-style "top queries / load
  by wait event" view. Real, but needs running compute + the exporter — a deploy
  wiring, not an engine change.

### Where we win (signals Aurora can't show)
- **Scale-to-zero economics** — active **compute-seconds** per database, idle vs
  active time, **cold-start latency** distribution, and an estimated **$ saved by
  hibernation**. Aurora is always-on (or slow to pause); we make "you paid for
  exactly what you used" *visible*.
- **Branch / PITR lineage** — a visual tree of a database's branches and restore
  points with **per-branch storage and divergence** (how much each branch
  actually costs, since history is shared copy-on-write). Unique to our storage.
- **Storage internals as product metrics** — page-reconstruction latency, WAL
  ingest/apply lag, layers offloaded, GC reclaimed. The disaggregation we built
  becomes a dashboard.

### How to deliver it (incremental, honest)
1. **Per-tenant proxy metrics** — label `connections`/`active`/`wakes`/
   `compute-seconds` by database (bounded cardinality = number of databases).
   We own these end-to-end today.
2. **Per-database console view** — a database's own page: native tiles +
   sparklines from (1), plus **embedded Grafana panels templated by database**
   (the monitoring stack already ships; add a `database` template variable).
3. **Compute SQL metrics** — wire `postgres_exporter` into the compute image /
   activator; surface the Performance-Insights-style view.
4. **Lineage & cost** — compute per-branch sizes from layer metadata; render the
   branch tree + $ saved.

Net: we don't out-feature CloudWatch on instance plumbing — we make **the
serverless and branching story measurable**, which is the part users actually
came to us for.

## Gap-list to world-class (prioritized)

**Engine durability & scale**
- **Cold-start latency**: opt-in **keep-warm** (zero cold start for chosen
  databases) is **shipped** — *avoidance*. Making the boot itself faster
  (snapshot/restore, working-set prefetch) needs the real compute image and is
  not yet built.
- **Page-data rehydration on restart** (catalog already restores topology; reload
  a timeline's pages from object-store layers). *Closes the durability story.*
- **Read replicas** (multiple read-only computes off one timeline).
- **Compute autoscaling** (Aurora-Serverless-v2-style sizing, on top of
  scale-to-zero).
- **Multi-region** async layer replication for DR.
- ~~**Compute timeline pinning** so an in-place PITR restore takes effect
  automatically~~ — **shipped**: the activator starts compute on the restored
  timeline (`{timeline}` / `aethel.io/timeline`).

**Productization**
- **Per-database metrics & lineage** (the observability plan above).
- **Backups/export & import** (logical + physical, beyond live PITR).
- **Per-tenant quotas, RBAC, and fine-grained authz** on the control plane.
- **`aethelctl deploy --region`** — embedded Terraform to provision the cluster +
  bucket + IAM (full one-command BYOC).

**Trust & operations**
- **Security audit**, supply-chain hardening, and a public threat model.
- **Soak / chaos testing** at scale; published SLOs (cold-start, durability).
- **CI green** (unblock GitHub Actions billing) + prebuilt `aethelctl` binaries.
- Alerting rules + metric↔trace exemplars shipped with the dashboards.

**Ecosystem (boundaries, not scope creep)**
- A stable **control-plane API + SDK** so others build *on* AethelDB.
- Postgres extension compatibility matrix; connection-pooler guidance (PgBouncer
  already composed).
- **Not** building the Supabase app layer ourselves.

## One-line test for every future feature

> Does it make an **open, self-hosted, serverless Postgres** better to run, scale,
> branch, recover, observe, secure, or pay for? Ship it. Otherwise it lives on top
> of us, not inside us.

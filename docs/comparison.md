<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Where AethelDB stands — vs Neon and Supabase

A candid, engineering-first comparison. The goal is to be honest about what's
real today (v0.2.0), what isn't, and where AethelDB is actually differentiated.

## Positioning in one line

> **AethelDB is a from-scratch, fully open, self-hostable implementation of the
> serverless-Postgres *architecture*** — the same compute/storage split Neon
> pioneered. It is **not** a managed cloud service, and it is **not** a
> Supabase-style application backend. Think "the engine, open and yours to run,"
> not "the SaaS."

Neon and Supabase solve different problems:

- **Neon** — the closest analog. Serverless Postgres that decouples stateless
  compute from log-structured page storage (pageserver + safekeepers + a patched
  compute). AethelDB shares this architecture by design. Neon's open-source engine
  is Apache-2.0; its **value and moat are the managed service** (console, control
  plane, autoscaling, multi-region, billing).
- **Supabase** — a Postgres **application platform** ("Firebase alternative"):
  auto-generated REST/GraphQL APIs, Auth, Realtime, file Storage, Edge Functions,
  a polished Studio GUI, and client SDKs, on top of (largely) dedicated Postgres.
  Different axis entirely — it's about app DX, not storage disaggregation.

So the meaningful comparison is **against Neon's architecture**; the comparison
against Supabase is mostly about scope (we deliberately don't do the app layer).

## Feature matrix

Legend: ✅ implemented · 🟡 partial/primitive · ❌ not yet · ⬚ out of scope

| Capability | **AethelDB v0.2.0** | Neon | Supabase |
|---|---|---|---|
| Decoupled compute/storage | ✅ | ✅ | ❌ (dedicated PG) |
| Scale-to-zero compute | ✅ proxy activation | ✅ | 🟡 (pause on some plans) |
| Instant branching (copy-on-write) | ✅ | ✅ | 🟡 (different mechanism) |
| Point-in-time reads/recovery | ✅ any LSN | ✅ | 🟡 (backups) |
| WAL redo via real Postgres | ✅ | ✅ | ⬚ |
| Quorum-durable WAL + election | ✅ safekeepers | ✅ | ⬚ (uses PG replication) |
| Compaction + branch-aware GC | ✅ | ✅ | ⬚ |
| Object storage backend | ✅ S3 (Azure/GCS pluggable) | ✅ | ✅ (file storage) |
| Multi-tenancy (engine) | ✅ + durable catalog | ✅ (managed) | ✅ (per-project) |
| Connection pooling | ✅ optional PgBouncer | ✅ | ✅ (Supavisor) |
| Observability (metrics/dash/trace) | ✅ Prometheus+Grafana+OTLP | ✅ | ✅ |
| Control plane | 🟡 HTTP/line API (token-gated) | ✅ full SaaS | ✅ |
| **CLI** | ❌ (`curl`/`nc` today) | ✅ `neonctl` | ✅ `supabase` |
| **Web console / GUI** | ❌ | ✅ | ✅ (Studio) |
| Compute autoscaling | ❌ | ✅ | 🟡 |
| Read replicas | ❌ | ✅ | ✅ |
| Multi-region | ❌ | ✅ | ✅ |
| Managed provisioning / billing | ❌ | ✅ | ✅ |
| Auth / REST / GraphQL / Realtime / Storage / Edge | ⬚ | 🟡 (Neon Auth) | ✅ (core value) |
| Self-host / on-prem | ✅ compose + k8s | 🟡 (engine OSS, hard to run) | ✅ (docker) |
| Fully open source (incl. control plane) | ✅ Apache-2.0 | 🟡 (engine yes, service no) | ✅ |
| Production maturity / scale | 🟡 early (v0.2.0) | ✅ | ✅ |

## What we genuinely have

The **hard distributed-systems core is real and tested** — WAL decode/redo
through Postgres's own machinery, instant branching + PITR, quorum-replicated
safekeepers with leader election, scale-to-zero, multi-tenancy with a durable
catalog, S3 offload — plus an opinionated operational layer (optional pooling,
token-gated control plane, Prometheus/Grafana/OTLP) and a clean self-host story
(compose + Kubernetes). That is a lot of correct, non-trivial machinery, fully
open under Apache-2.0.

## What we honestly lack (vs Neon)

- **Productization**: no CLI, no web console, no managed provisioning/billing.
- **Elasticity at scale**: no compute autoscaling, no read replicas, single
  region.
- **Durability completeness**: the catalog restores *topology* on restart; a
  timeline's *pages* are not yet rehydrated from object-store layers on restart.
- **Hardening**: no security audit, no large-scale soak/chaos testing, CI
  currently gated on account billing.

These are the difference between "an impressive working architecture" and "a
service you'd bet a company on." They're tractable and on the roadmap.

## The wedge — why AethelDB can matter

Neon's engine is open, but its **value is the SaaS**; running it yourself is
hard, and your data lives in their cloud. Supabase is self-hostable but is an
app platform, not disaggregated storage.

AethelDB's opportunity is the gap between them:

1. **Truly open, truly self-hostable serverless Postgres** — every part
   (including the control plane) is Apache-2.0; you run it.
2. **Bring-Your-Own-Cloud (BYOC)** — deploy it into *your* AWS/Azure/GCP account
   so data never leaves your tenancy. Compliance, sovereignty, and cost control
   that a managed multi-tenant SaaS can't offer. This is the strategic focus of
   the [roadmap](../ROADMAP.md): make "deploy AethelDB to my own cloud with one
   CLI command" the headline experience.

We don't out-feature Neon's service or Supabase's platform — we offer the one
thing neither does: **the full serverless-Postgres engine, open, in infrastructure
you control.**

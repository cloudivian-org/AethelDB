<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# AethelDB Roadmap

**Vision:** the easiest *fully open* serverless PostgreSQL you can run **in
infrastructure you control** — on-prem or in your own cloud account. We don't try
to be a managed SaaS (Neon) or an app backend (Supabase); we own
**self-hostable, Bring-Your-Own-Cloud serverless Postgres**. See
[`docs/comparison.md`](docs/comparison.md) for where we stand today.

## Compatibility charter (non-negotiable)

> **New work must not break existing functionality.** Every item below is
> additive and opt-in. This is a hard rule, enforced the same way the project has
> grown so far.

How we keep that promise:

1. **Additive & opt-in by default.** New capabilities ship behind a Cargo feature
   (e.g. `otlp`, `kubernetes`) or a flag/env that defaults to today's behavior
   (e.g. `--control-token`, `--wal-redo`, `--s3-endpoint`). Omitting them yields
   exactly the current behavior.
2. **Versioned, append-only formats.** The wire protocols (`page_service`,
   `wal_service`) and the on-disk/object formats are extended via new message
   types or version bytes — never by repurposing existing fields. The tenant
   catalog already carries a `version`; readers tolerate older versions and
   migrate forward.
3. **Layered surfaces, untouched core.** The CLI, operator, and GUI are **new
   layers over the existing control-plane API** — they do not change the data
   plane. Cloud object stores are additional `ObjectStore` implementations behind
   the existing trait; callers don't change.
4. **SemVer + deprecation.** Pre-1.0 may evolve, but breaking changes are called
   out in `CHANGELOG.md`; post-1.0 we deprecate before we remove.
5. **Regression-guarded.** The test suite (and golden tests for wire formats) is
   the guardrail; every change stays `fmt`/`clippy`/`test` green. New features add
   tests; they don't loosen old ones.

## Done (v0.2.0)

Data plane (WAL decode/redo, branching + PITR, quorum safekeepers, scale-to-zero,
compaction/GC, S3 offload) **and** an operational layer: multi-tenancy with a
durable catalog, token-gated control plane, optional PgBouncer pooling, the
Kubernetes activator, and observability (Prometheus + Grafana + optional OTLP).

## The headline goal: "deploy to any cloud with one command"

Every major cloud offers **managed Kubernetes** (EKS / AKS / GKE) and
**S3-compatible object storage** (S3 / Azure Blob / GCS). AethelDB already targets
both abstractions, so cloud-portability is mostly *packaging and a thin driver* —
not a rewrite. The plan turns that into a one-liner.

### Phase 1 — Multi-cloud storage + Helm chart (foundation) — ✅ shipped
*Additive: new `ObjectStore` impls + packaging; no engine change.*
- **Azure Blob + GCS object-store backends** behind the existing `ObjectStore`
  trait — selected by `--object-store-url` (`s3://` / `az://` / `gs://`), creds
  from standard env vars. S3 (MinIO) and Azure (Azurite) verified end-to-end.
- **Helm chart** (`deploy/helm/aetheldb`) packaging safekeeper / pageserver /
  proxy, with values for the object-store backend, credentials (Secrets), control
  token, pooling, and the Kubernetes activator. Server-side validated on a real
  cluster; see [`deploy/helm/README.md`](deploy/helm/README.md).
- **Outcome:** `helm install` on EKS/AKS/GKE (pointed at S3/Blob/GCS) is a working
  cloud deploy today.

### Phase 2 — `aethelctl` CLI — ✅ shipped (provisioning is next)
*A new binary over the existing HTTP control plane; the engine is unaware of it.*
- **Operate:** `aethelctl status | tenant | timeline | branch (alias pitr) |
  receive | gc` — wraps the HTTP control plane, honoring the bearer token, with
  `--json` output. ✅
- **Run locally:** `aethelctl up` / `down` (Docker Compose). ✅
- **Deploy to cloud:** `aethelctl deploy --cloud aws|azure|gcp …` — runs
  `helm upgrade --install` of the **embedded** Helm chart onto the target
  cluster (works from anywhere; no repo checkout needed). ✅
- **Next:** `--region`-style turnkey provisioning via embedded Terraform modules
  (managed K8s + bucket + IAM + DNS) for full **BYOC** in *your* account.
- Shipped as a single Rust binary, in keeping with the project.

### Phase 3 — Operator + CRDs
*Declarative management; composes with, doesn't replace, the control plane.*
- `AethelCluster`, `AethelTenant`, `AethelBranch` custom resources reconciled by
  an operator (built on the existing Kubernetes activator). GitOps-friendly
  fleet management.

### Phase 4 — Web console (GUI) — ✅ shipped (initial)
*Served by `aethelctl serve`; embedded in the binary.*
- A polished single-page console to **operate** (tenants, branches, PITR, GC over
  the control plane — the browser never holds the token) and **deploy** (on-prem /
  AWS / Azure / GCP, with autoscaling + PDB + topology-spread toggles and a live
  `helm --dry-run` preview). A real **streamed apply** is gated behind
  `--allow-apply`, and `--grafana-url` embeds live Grafana panels. ✅
- **Next:** offer the console as a container in the Helm chart, and add per-panel
  Grafana embeds + apply history.

## Productization depth (parallel, ongoing)

Each is additive and independently shippable:

- **Page-data rehydration on restart** — reload a timeline's pages from its
  object-store layers (the catalog already restores topology). Closes the
  durability story.
- **Read replicas** — multiple read-only computes off one timeline.
- **Compute autoscaling** — size compute to load (with the activator).
- **Multi-region** — async layer replication across regions for DR.
- **Per-tenant quotas & fine-grained authz** — identities/scopes beyond the
  single operator token; per-tenant object-store prefixes.
- **Backup/export & import** — logical and physical, beyond live PITR.
- **Marketplace images / Terraform provider / Crossplane** — for IaC-native users.

## How to read this

Phases are ordered by leverage, not rigid sequence — Phase 1 unlocks real cloud
deploys immediately; the CLI (Phase 2) is the experience most users will touch.
Contributions are welcome on any item — see
[`CONTRIBUTING.md`](CONTRIBUTING.md). Nothing here changes how the current data
plane behaves.

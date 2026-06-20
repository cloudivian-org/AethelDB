<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Deploying AethelDB

Two paths: a single-host **Docker Compose** stack for local development, and
**Kubernetes** manifests for a cluster.

## Docker Compose (local)

Brings up MinIO, a safekeeper, a page server, and the proxy, wired together
(page server streams WAL from the safekeeper and offloads layers to MinIO):

```bash
make up      # docker compose up --build -d
make down    # docker compose down -v
```

| Service | Ports |
|---|---|
| proxy | `5432` (PostgreSQL), `9432` (`/metrics`) |
| pageserver | `6400` page, `6401` ingest, `6402` control (text), `6403` control (HTTP), `9400` `/metrics` |
| safekeeper | `6500` WAL, `9500` `/metrics` |
| MinIO | `9000` S3, `9090` console |

Compute is **not** an always-on service — the proxy starts and stops it on
demand (scale-to-zero), so it is built as an image and launched at runtime.

```bash
# Once up, drive the control plane:
curl localhost:6403/healthz
curl -XPOST localhost:6403/v1/branches -d '{"timeline":"<hex>","parent":"<hex>","lsn":5000}'
curl localhost:9400/metrics
```

## Kubernetes

Manifests live in [`deploy/k8s/`](./k8s/) as a Kustomize package:

```bash
# 1. Build and push the service images (one binary per image).
make images
#    docker push aetheldb/{proxy,safekeeper,pageserver}:dev   # to your registry

# 2. Render locally (no cluster needed) to review:
kubectl kustomize deploy/k8s

# 3. Apply to a cluster:
kubectl apply -k deploy/k8s
```

What it deploys to the `aetheldb` namespace:

- **minio** — Deployment + Service + a one-shot bucket-create Job (dev-grade;
  swap for managed S3 and a Secret in production).
- **safekeeper** — StatefulSet (stable identity + durable WAL PVC) + headless
  Service. Scale to 3 replicas and pass `--members`/`--peer-addrs` for a real
  quorum group with replication and leader election.
- **pageserver** — StatefulSet streaming WAL from `safekeeper-0.safekeeper` and
  offloading layers to MinIO/S3; `/healthz` readiness on the HTTP control port.
- **proxy** — Deployment behind a `LoadBalancer` Service (the only externally
  exposed component), serving the PostgreSQL wire protocol on `5432`.

Every pod is annotated `prometheus.io/scrape: "true"` with its metrics port.

### Production notes

- **Object store** — point the page server at managed S3
  (`--s3-endpoint`/`--s3-bucket`/keys) and store credentials in a `Secret`.
- **TLS & auth** — give the proxy a cert (`--tls-cert`/`--tls-key`) and
  per-tenant SCRAM verifiers.
- **Compute orchestration** — build the proxy with `--features kubernetes` and
  set `--kube-namespace` to use the **Kubernetes activator**, which scales a
  per-tenant `compute-<tenant>` Deployment to zero/one (RBAC in
  `proxy-rbac.yaml`). See [`docs/design/k8s-activator.md`](../docs/design/k8s-activator.md);
  verify against a real cluster with `deploy/k8s/verify-activator.sh`.

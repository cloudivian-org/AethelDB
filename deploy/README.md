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

## Security hardening

> ⚠️ **The bundled Compose and Kubernetes manifests are configured for local
> development, not production.** They ship with well-known default credentials
> and convenient-but-unsafe defaults. Before exposing AethelDB anywhere, work
> through this checklist.

1. **Replace every default credential.** The MinIO root user/password are
   `minioadmin`/`minioadmin` in `docker-compose.yml` and `deploy/k8s/minio.yaml`,
   and the page server's S3 keys default to the same. In production, run against
   managed S3 (or a hardened MinIO) and inject credentials from a secret store —
   never commit them. The k8s manifests read S3 keys from a `Secret`; create it
   with your own values (`kubectl create secret generic aethel-s3 ...`) and do
   **not** apply `minio.yaml` as-is.

2. **Do not mount the host Docker socket.** The Compose stack bind-mounts
   `/var/run/docker.sock` into the proxy so the `CommandActivator` can start a
   compute container — this grants the proxy **root-equivalent control of the
   host** and is for local use only. In production, orchestrate compute with the
   **Kubernetes activator** (scoped RBAC in `proxy-rbac.yaml`), which needs only
   `get`/`patch` on a single Deployment's `scale` subresource — not Docker.

3. **Terminate TLS and require authentication.** Run the proxy with
   `--tls-cert`/`--tls-key` and configure per-tenant **SCRAM-SHA-256** verifiers
   so credentials are checked *before* a cold start. Without these the proxy
   speaks plaintext and wakes compute for any connection.

4. **Lock down the network.** The safekeeper (WAL), page server (GetPage +
   ingest + control + HTTP), and MinIO endpoints are **unauthenticated** and must
   never be exposed beyond the trusted cluster network. Use Kubernetes
   `NetworkPolicy` (or security groups) so only the intended services can reach
   them; expose **only** the proxy's client port externally.

5. **Protect the control plane.** The page server's line-oriented control
   endpoint (`:6402`) and HTTP/JSON API (`:6403`) can create/branch/GC timelines
   with no auth. Keep them on an internal interface and front them with your
   control plane's authn/authz.

6. **Run as non-root, read-only.** Set `runAsNonRoot`, a read-only root
   filesystem, and dropped capabilities on the service containers; persist only
   the safekeeper's WAL volume and the page server's local cache.

7. **Pin images by digest.** Replace the `:dev` tags with immutable digests and
   scan them in CI before promotion.

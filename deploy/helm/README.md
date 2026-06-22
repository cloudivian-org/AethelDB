<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Deploy AethelDB to any cloud (Helm)

One Helm chart deploys AethelDB to **any managed Kubernetes** (EKS / AKS / GKE)
or a local cluster, backed by **any cloud's object storage** (S3 / Azure Blob /
GCS). The same `aethel-pageserver` binary speaks all three — you just point it at
a URL and supply credentials.

```
client ──▶ proxy (scale-to-zero) ──▶ compute (PostgreSQL)
                                       │  ▲
                              stream WAL│  │ get_page @LSN
                                       ▼  │
                          safekeeper(s) ──▶ pageserver ──▶ ☁ object storage
```

## Prerequisites

- A Kubernetes cluster and `helm` 3.8+ / 4.x.
- Service container images published as `<registry>/<repo>/{proxy,safekeeper,pageserver}:<tag>`.
  Build them from `deploy/Dockerfile.rust` (one image per `BIN`) and push to your
  registry; set `image.registry` / `image.repository` / `image.tag`.
- A bucket/container in your cloud, plus credentials.

## Quickstart

```bash
helm install aethel deploy/helm/aetheldb \
  --namespace aethel --create-namespace \
  --set image.registry=ghcr.io --set image.repository=you/aetheldb --set image.tag=v0.2.0 \
  -f my-values.yaml
```

## Cloud object storage

Set `objectStore.url` and the matching credentials. Credentials are injected as
environment variables (resolved by the page server's object-store client), either
from a chart-managed Secret (`objectStore.credentials`) or your own
(`objectStore.existingSecret`).

### AWS S3 (EKS)

```yaml
objectStore:
  url: s3://my-aethel-bucket
  credentials:
    AWS_ACCESS_KEY_ID: "AKIA..."
    AWS_SECRET_ACCESS_KEY: "..."
    AWS_REGION: "us-east-1"
```

> On EKS, prefer **IRSA** (IAM Roles for Service Accounts): omit the keys, attach
> a role to the page server's ServiceAccount, and the client picks up credentials
> automatically.

### Azure Blob (AKS)

```yaml
objectStore:
  url: az://my-container
  credentials:
    AZURE_STORAGE_ACCOUNT_NAME: "myaccount"
    AZURE_STORAGE_ACCOUNT_KEY: "..."
```

### Google Cloud Storage (GKE)

```yaml
objectStore:
  url: gs://my-aethel-bucket
  # Use Workload Identity on GKE, or mount a service-account JSON and set
  # GOOGLE_APPLICATION_CREDENTIALS via an existingSecret.
  existingSecret: aethel-gcs
```

## Expose the database

```yaml
proxy:
  service:
    type: LoadBalancer   # gets a cloud LB / external IP for :5432
```

Clients then connect to the LB address on port 5432 (the proxy cold-starts
compute and routes). Use `ClusterIP` (default) + `kubectl port-forward` for
private access.

## Durability & scale

```yaml
safekeeper:
  replicas: 3            # real quorum; node-id, members, and peers are derived
  storage: 50Gi
pageserver:
  cacheStorage: 100Gi    # local working-set / layer cache (history lives in object storage)
```

## Control-plane auth

```yaml
controlToken:
  value: "a-long-random-secret"   # requires auth on :6402 / :6403 (/healthz stays open)
```

## Autoscaling & availability (production best practices)

All opt-in; defaults preserve single-replica behavior.

```yaml
autoscaling:
  proxy:                 # HPA on the stateless proxy (its replicas become HPA-managed)
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70

podDisruptionBudget:     # keep quorum / ingress during node drains & upgrades
  safekeeper: { enabled: true, minAvailable: 2 }   # for a 3-replica quorum
  proxy:      { enabled: true, minAvailable: 1 }

topologySpread:          # spread the quorum & proxies across nodes/zones
  enabled: true
  topologyKey: topology.kubernetes.io/zone   # or kubernetes.io/hostname
```

> The page server and safekeeper are **stateful** and scaled deliberately
> (`safekeeper.replicas`), not by an HPA. The HPA targets only the stateless
> proxy. Pair `topologySpread` with a real `topologyKey` (zone) on multi-AZ
> clusters for genuine fault isolation.

## Options

| Value | Default | Purpose |
|---|---|---|
| `objectStore.url` | `""` (local PVC) | `s3://` / `az://` / `gs://` bucket |
| `objectStore.allowHttp` | `false` | permit HTTP endpoints (MinIO/emulators) |
| `proxy.service.type` | `ClusterIP` | `LoadBalancer` to expose `:5432` |
| `proxy.tenants` | `[]` | static `db=host:port` routes |
| `proxy.activator.kubernetes` | `false` | Kubernetes scale-to-zero activator (+ RBAC; needs an image built with the `kubernetes` feature) |
| `pooling.enabled` | `false` | deploy the PgBouncer pooling tier |
| `safekeeper.replicas` | `1` | set `3` for quorum |
| `controlToken.value` | `""` | gate the control plane |

## Validate before installing

```bash
helm lint deploy/helm/aetheldb
helm template aethel deploy/helm/aetheldb -f my-values.yaml | kubectl apply --dry-run=server -f -
```

(Both are run in CI-style checks; the rendered manifests are validated
server-side against a real Kubernetes API.)

## Security

Treat the bundled defaults as a starting point — set real credentials (or
cloud-native identity), a strong `controlToken`, a `LoadBalancer`/network policy
that exposes **only** the proxy, and pinned image digests. See
[`../README.md`](../README.md#security-hardening).

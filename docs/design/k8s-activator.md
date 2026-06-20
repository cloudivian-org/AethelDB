<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# Design: Kubernetes-native activator

Status: **built + verified against a real cluster.** Feature-gated
(`--features kubernetes`); off by default so the proxy build stays light.

## Model

Each tenant's stateless compute is a Kubernetes **Deployment** (default
`compute-<tenant>`) behind a Service. Scale-to-zero is then just scaling that
Deployment:

- **`start(tenant)`** patches the Deployment's `scale` subresource to **1**.
- **`stop(tenant)`** patches it to **0**.

The proxy connects to the tenant's Service, which routes to the (now-running)
Pod; the existing readiness probe (`wait_until_ready`) covers the cold-start
window. This reuses Kubernetes' own scheduling, health, and networking rather
than managing Pods by hand.

```
client ─▶ proxy ──(start: scale compute-<tenant> → 1)──▶ kube-apiserver
                                                            │
                                                            ▼
                                                   Deployment compute-<tenant>
   proxy ◀── connect to Service compute-<tenant> ◀── Pod (running)
   ...idle...
   proxy ──(stop: scale → 0)──▶ kube-apiserver
```

## Implementation

`proxy/src/k8s.rs` — `KubeActivator` implements the `Activator` trait using
`kube-rs`. `start`/`stop` issue a single `PATCH` to
`/apis/apps/v1/namespaces/<ns>/deployments/<name>/scale` with
`{"spec":{"replicas":N}}`. The client is built from the ambient config — the
in-cluster service account in a Pod, or the local kubeconfig in development.

Enable it on the proxy:

```bash
# build the image with the feature, then:
aethel-proxy --kube-namespace aetheldb --kube-name-template 'compute-{tenant}'
```

## RBAC

The proxy's ServiceAccount needs to read and scale Deployments in its namespace
(`deploy/k8s/proxy-rbac.yaml`): `get/list/watch` on `deployments` and
`get/patch/update` on `deployments/scale`.

## Verification

The activator is verified two ways:

- **Unit** — `render_name` (naming) and a mock-API test asserting `start` issues
  the exact scale `PATCH` with `replicas: 1` (runs in CI, no cluster).
- **Real cluster** — `deploy/k8s/verify-activator.sh` spins up a `kind` cluster,
  creates a `compute-shop` Deployment at 0 replicas, and runs the gated
  `scales_a_real_deployment` test, which scales it **0 → 1** on `start` and
  **1 → 0** on `stop` through the live API and asserts the result. Verified to
  pass against a real kind cluster.

```bash
deploy/k8s/verify-activator.sh
```

## Next

- **Provisioning** — create the Deployment + Service for a new tenant (today they
  are assumed to exist); pairs with the multi-tenant control plane.
- **Firecracker / microVMs** — an alternative `Activator` for stronger isolation
  than a shared-kernel Pod, behind the same trait.

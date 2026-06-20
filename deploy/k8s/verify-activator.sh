#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Verify the Kubernetes activator against a REAL cluster: spin up a kind cluster,
# create a dummy per-tenant compute Deployment at 0 replicas, and run the gated
# Rust test that scales it up (start) and down (stop) through the live API.
#
# Requires: kind, kubectl, docker, and a Rust toolchain. Usage:
#   deploy/k8s/verify-activator.sh           # create cluster, test, leave it up
#   KEEP=0 deploy/k8s/verify-activator.sh    # also tear the cluster down at the end
set -euo pipefail

CLUSTER="${CLUSTER:-aethel-test}"
NS=aetheldb

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo ">> creating kind cluster '$CLUSTER'"
  kind create cluster --name "$CLUSTER"
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo ">> ensuring namespace + a dummy compute-shop Deployment (0 replicas)"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create deployment compute-shop \
  --image=registry.k8s.io/pause:3.9 --replicas=0 \
  --dry-run=client -o yaml | kubectl apply -f -

echo ">> running the activator against the live cluster"
AETHEL_K8S_TEST=1 cargo test -p proxy --features kubernetes \
  --lib k8s::tests::scales_a_real_deployment -- --nocapture

echo ">> final replicas (expect 0 after stop):"
kubectl -n "$NS" get deploy compute-shop -o jsonpath='{.spec.replicas}'; echo

if [ "${KEEP:-1}" = "0" ]; then
  echo ">> tearing down kind cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER"
else
  echo ">> leaving cluster up; remove with: kind delete cluster --name $CLUSTER"
fi

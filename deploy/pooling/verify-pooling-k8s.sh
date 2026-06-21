#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Verify the optional pooling tier on a real Kubernetes cluster, using the
# self-contained demo (stock Postgres + PgBouncer):
#
#     psql  ->  PgBouncer Service  ->  compute (PostgreSQL)   [in-cluster]
#
# Applies deploy/pooling/k8s-demo.yaml, waits for the pods, port-forwards the
# PgBouncer Service, and drives a real psql query through the pooler.
#
# Requires: kubectl (pointed at a cluster), psql.
# Usage:
#   deploy/pooling/verify-pooling-k8s.sh              # apply, verify, LEAVE running
#   deploy/pooling/verify-pooling-k8s.sh --cleanup    # ... then delete the demo
set -euo pipefail

NS=aethel-pool-demo
LOCAL_PORT="${LOCAL_PORT:-6533}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MANIFEST="$ROOT/deploy/pooling/k8s-demo.yaml"
CLEANUP=0
[[ "${1:-}" == "--cleanup" ]] && CLEANUP=1

PF_PID=""
finish() {
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null || true
  if [[ "$CLEANUP" == "1" ]]; then
    echo "==> tearing down the demo"
    kubectl delete -f "$MANIFEST" --wait=false >/dev/null 2>&1 || true
  fi
}
trap finish EXIT

echo "==> applying the demo (namespace $NS)"
kubectl apply -f "$MANIFEST" >/dev/null

echo "==> waiting for compute + pgbouncer to be Ready"
kubectl -n "$NS" wait --for=condition=ready pod -l app=compute --timeout=180s >/dev/null
kubectl -n "$NS" wait --for=condition=ready pod -l app=pgbouncer --timeout=120s >/dev/null
kubectl -n "$NS" get pods

echo "==> port-forward svc/pgbouncer :6432 -> 127.0.0.1:$LOCAL_PORT"
kubectl -n "$NS" port-forward svc/pgbouncer "$LOCAL_PORT:6432" >/tmp/pf-pgbouncer-k8s.log 2>&1 &
PF_PID=$!
sleep 4

echo "==> driving SQL through the pooler"
OUT=$(PGPASSWORD=postgres psql "host=127.0.0.1 port=$LOCAL_PORT dbname=mydb user=postgres" \
  -tAc "select 'k8s-pool-ok', current_database()")
echo "    $OUT"
[[ "$OUT" == k8s-pool-ok\|mydb ]] || { echo "FAIL: unexpected result"; exit 1; }

echo "PASS: psql -> pgbouncer (Service) -> compute works on Kubernetes"
[[ "$CLEANUP" == "1" ]] || echo "(demo left running; delete with: kubectl delete -f $MANIFEST)"

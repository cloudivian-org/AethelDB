#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Verify the OPTIONAL connection-pooling tier end to end:
#
#     psql  ->  aethel-proxy  ->  PgBouncer  ->  PostgreSQL
#               (wake/route/TLS)  (pooling)      (stock PG 16)
#
# This proves PgBouncer composes with the activation proxy in the real topology
# (see docs/design/proxy-tls.md). It uses a *stock* Postgres so it runs anywhere
# Docker is available — the patched compute image is not required.
#
# Requires: docker, psql, and a built `aethel-proxy` (cargo build -p proxy).
# Usage:    deploy/pooling/verify-pooling.sh
set -euo pipefail

NET=aethel-pool-test
PG=aethel-pg
BOUNCER=aethel-pgbouncer
PROXY_PORT=55432
BOUNCER_PORT=6432
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROXY_BIN="${AETHEL_PROXY:-$ROOT/target/debug/aethel-proxy}"

cleanup() {
  [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null || true
  docker rm -f "$PG" "$BOUNCER" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ! -x "$PROXY_BIN" ]]; then
  echo "building aethel-proxy ..."
  (cd "$ROOT" && cargo build -p proxy)
fi

echo "==> network + stock PostgreSQL 16"
cleanup
docker network create "$NET" >/dev/null
docker run -d --name "$PG" --network "$NET" \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=mydb \
  postgres:16-alpine >/dev/null
for _ in $(seq 1 30); do
  docker exec "$PG" pg_isready -U postgres >/dev/null 2>&1 && break
  sleep 1
done

echo "==> PgBouncer (transaction pooling) in front of Postgres"
docker run -d --name "$BOUNCER" --network "$NET" -p "$BOUNCER_PORT:$BOUNCER_PORT" \
  -e DB_HOST="$PG" -e DB_PORT=5432 -e DB_NAME=mydb -e DB_USER=postgres \
  -e LISTEN_PORT="$BOUNCER_PORT" -e AUTH_TYPE=trust -e POOL_MODE=transaction \
  -e MAX_CLIENT_CONN=100 -e DEFAULT_POOL_SIZE=20 \
  -e IGNORE_STARTUP_PARAMETERS=extra_float_digits,options \
  edoburu/pgbouncer:latest >/dev/null
sleep 4

echo "==> aethel-proxy routing tenant 'mydb' through the pooler"
"$PROXY_BIN" --listen "127.0.0.1:$PROXY_PORT" \
  --tenant "mydb=127.0.0.1:$BOUNCER_PORT" \
  --start-command 'true' --stop-command 'true' \
  --metrics-listen 127.0.0.1:59432 >/tmp/aethel-proxy-pool.log 2>&1 &
PROXY_PID=$!
sleep 2

echo "==> driving SQL through the full chain"
OUT=$(PGPASSWORD=postgres psql "host=127.0.0.1 port=$PROXY_PORT dbname=mydb user=postgres" \
  -tAc "select 'chain-ok', current_database()")
echo "    $OUT"
[[ "$OUT" == chain-ok\|mydb ]] || { echo "FAIL: unexpected result"; exit 1; }

# Exercise the pool with several short connections.
for i in 1 2 3 4 5; do
  PGPASSWORD=postgres psql "host=127.0.0.1 port=$PROXY_PORT dbname=mydb user=postgres" \
    -tAc "select $i" >/dev/null
done

echo "PASS: psql -> aethel-proxy -> pgbouncer -> postgres works (pooling tier verified)"

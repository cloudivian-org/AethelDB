#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# End-to-end verification of the `postgres --wal-redo` backend: have a real
# server emit one heap-insert WAL record (a true delta, full_page_writes off),
# then drive the wal-redo process with it and confirm it reconstructs the page
# byte-for-byte. Requires a built install (run `make -C compute all` first).
#
# Usage: compute/walredo/verify.sh [PREFIX]
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${1:-$HERE/../install}"
PGDATA="$(mktemp -d -t walredo-verify-XXXX)"
PORT="${PORT:-5455}"
trap 'rm -rf "$PGDATA"' EXIT

if [ ! -x "$PREFIX/bin/postgres" ]; then
  echo "no postgres at $PREFIX/bin/postgres -- run 'make -C compute all' first" >&2
  exit 1
fi

"$PREFIX/bin/initdb" -D "$PGDATA" -U postgres --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<CONF
full_page_writes = off
fsync = off
autovacuum = off
CONF

"$PREFIX/bin/pg_ctl" -D "$PGDATA" -w -l "$PGDATA/server.log" \
  -o "-p $PORT -k $PGDATA -c listen_addresses=''" start >/dev/null

q() { "$PREFIX/bin/psql" -h "$PGDATA" -p "$PORT" -U postgres -d postgres -At -c "$1"; }

q "CREATE TABLE t (id int);" >/dev/null
q "SELECT pg_switch_wal();" >/dev/null          # fresh segment -> record won't span a page
BEFORE="$(q "SELECT pg_current_wal_insert_lsn();")"
q "INSERT INTO t VALUES (42);" >/dev/null
AFTER="$(q "SELECT pg_current_wal_insert_lsn();")"
q "CHECKPOINT;" >/dev/null                       # flush the heap page to disk

"$PREFIX/bin/pg_ctl" -D "$PGDATA" -w stop >/dev/null

python3 "$HERE/test_real_redo.py" "$PREFIX" "$PGDATA" "$BEFORE" "$AFTER"

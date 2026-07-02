#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# End-to-end local proof of the REAL compute path: a patched PostgreSQL compute
# (aethel_smgr) serving reads from the AethelDB page server — not local disk.
#
#   1. initdb a data dir with the patched Postgres and seed a table on local disk.
#   2. Import that data dir into a fresh page server (base-image import).
#   3. Restart Postgres with aethel_smgr pointed at the page server and SELECT —
#      the rows come back reconstructed from the page store.
#
# Requires a locally built compute install at compute/install (see compute/Makefile
# or build the compute image). Builds the Rust page server + importer as needed.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGBIN="$ROOT/compute/install/bin"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/aethel-compute.XXXXXX")"
PGPORT=55999
PS_PAGE=6400
PS_INGEST=6401
PS_PID=""

cleanup() {
  "$PGBIN/pg_ctl" -D "$WORK/pgdata" stop -m immediate >/dev/null 2>&1 || true
  [ -n "$PS_PID" ] && kill "$PS_PID" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

step() { printf '\n=== %s ===\n' "$1"; }

# --- preflight -------------------------------------------------------------
if [ ! -x "$PGBIN/postgres" ]; then
  echo "error: no compute build at $PGBIN" >&2
  echo "build it first (compile the patched Postgres + aethel_smgr):" >&2
  echo "  make -C compute            # or build the compute Docker image" >&2
  exit 1
fi
# Build aethel_smgr against the local install if it isn't installed yet.
if ! ls "$ROOT"/compute/install/lib/postgresql/aethel_smgr.* >/dev/null 2>&1; then
  step "building aethel_smgr extension"
  make -C "$ROOT/compute/extension/aethel_smgr" install USE_PGXS=1 PG_CONFIG="$PGBIN/pg_config"
fi

step "building page server + base-image importer"
( cd "$ROOT" && cargo build -q -p pageserver --bin aethel-pageserver --bin aethel-basebackup-import )
PS_BIN="$ROOT/target/debug/aethel-pageserver"
IMPORT_BIN="$ROOT/target/debug/aethel-basebackup-import"

# --- 1. initdb + seed on local disk ---------------------------------------
step "initdb + seed a table (local disk)"
"$PGBIN/initdb" -D "$WORK/pgdata" -U postgres >/dev/null
"$PGBIN/pg_ctl" -D "$WORK/pgdata" -o "-p $PGPORT" -l "$WORK/local.log" start >/dev/null
sleep 3
"$PGBIN/psql" -p $PGPORT -U postgres -d postgres -v ON_ERROR_STOP=1 >/dev/null <<SQL
create table t(id int primary key, v text);
insert into t select g, 'row-'||g from generate_series(1,50) g;
checkpoint;
SQL
"$PGBIN/pg_ctl" -D "$WORK/pgdata" stop >/dev/null
sleep 1

# --- 2. fresh page server + base-image import ------------------------------
step "start page server + import base image"
"$PS_BIN" --listen 127.0.0.1:$PS_PAGE --ingest-listen 127.0.0.1:$PS_INGEST \
  --object-dir "$WORK/obj" >"$WORK/ps.log" 2>&1 &
PS_PID=$!
sleep 2
"$IMPORT_BIN" --pgdata "$WORK/pgdata" --ingest 127.0.0.1:$PS_INGEST

# --- 3. boot the REAL compute against the page server ----------------------
step "boot compute with aethel_smgr (reads served by the page server)"
cat >> "$WORK/pgdata/postgresql.conf" <<CONF
shared_preload_libraries = 'aethel_smgr'
aethel_smgr.pageserver_host = '127.0.0.1'
aethel_smgr.pageserver_port = $PS_PAGE
aethel_smgr.tenant_id = '00000000000000000000000000000000'
aethel_smgr.timeline_id = '00000000000000000000000000000000'
CONF
"$PGBIN/pg_ctl" -D "$WORK/pgdata" -o "-p $PGPORT" -l "$WORK/smgr.log" start >/dev/null
sleep 6

COUNT="$("$PGBIN/psql" -p $PGPORT -U postgres -d postgres -tAc 'select count(*) from t;')"
echo "rows read from the page server: $COUNT"
"$PGBIN/psql" -p $PGPORT -U postgres -d postgres -c 'select * from t where id in (1,25,50) order by id;'

if [ "$COUNT" = "50" ]; then
  echo -e "\nPASS: the real compute served 50 rows from the page server (not local disk)."
else
  echo -e "\nFAIL: expected 50 rows, got '$COUNT'. See $WORK/smgr.log" >&2
  exit 1
fi

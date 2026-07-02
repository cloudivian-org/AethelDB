#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# AethelDB compute entrypoint: turn a patched PostgreSQL into an AethelDB compute
# node, configured entirely from the environment so the activator (or a restore)
# can steer it without editing files.
#
#   AETHEL_TENANT             tenant id (32 hex)         [default: all-zero root]
#   AETHEL_TIMELINE           timeline id (32 hex)       [default: all-zero root]
#                             — a point-in-time restore sets this (the
#                               aethel.io/timeline pod annotation -> env).
#   AETHEL_PAGESERVER_HOST    page server host           [default: 127.0.0.1]
#   AETHEL_PAGESERVER_PORT    page server GetPage port   [default: 6400]
#   AETHEL_SAFEKEEPERS        comma-separated application_names for the WAL quorum
#   PGDATA                    data directory             [default: .../data]
#   AETHEL_RENDER_ONLY=1      render the config and exit (for tests/CI)
#
# Reads come from the page server via aethel_smgr; durability is the safekeeper
# quorum. The timeline the compute serves is AETHEL_TIMELINE, so an in-place PITR
# restore takes effect the next time this compute starts.
set -euo pipefail

# `:-` (not `:=`) so an *empty* value — e.g. a missing `aethel.io/timeline`
# annotation surfaced by the Kubernetes Downward API — falls back to the root,
# not to a blank id.
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
AETHEL_TENANT="${AETHEL_TENANT:-00000000000000000000000000000000}"
AETHEL_TIMELINE="${AETHEL_TIMELINE:-00000000000000000000000000000000}"
AETHEL_PAGESERVER_HOST="${AETHEL_PAGESERVER_HOST:-127.0.0.1}"
AETHEL_PAGESERVER_PORT="${AETHEL_PAGESERVER_PORT:-6400}"
AETHEL_SAFEKEEPERS="${AETHEL_SAFEKEEPERS:-safekeeper1,safekeeper2,safekeeper3}"

# Control files (pg_control, pg_filenode.map, config) live locally; relation
# pages are served by the page server. initdb on first boot to create them.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  initdb -D "$PGDATA" -U "${POSTGRES_USER:-postgres}" >/dev/null
fi

# Quorum over the safekeeper application_names: ANY (n/2 + 1) of them.
sk_list="$(echo "$AETHEL_SAFEKEEPERS" | sed 's/, */, /g')"
sk_n="$(echo "$AETHEL_SAFEKEEPERS" | awk -F, '{print NF}')"
sk_q="$(( sk_n / 2 + 1 ))"

# Render the AethelDB compute config from the environment.
conf="$PGDATA/postgresql.auto.conf"
cat > "$conf" <<CONF
# Managed by the AethelDB compute entrypoint — do not edit by hand.
shared_preload_libraries = 'aethel_smgr'
aethel_smgr.pageserver_host = '$AETHEL_PAGESERVER_HOST'
aethel_smgr.pageserver_port = $AETHEL_PAGESERVER_PORT
aethel_smgr.tenant_id = '$AETHEL_TENANT'
aethel_smgr.timeline_id = '$AETHEL_TIMELINE'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
synchronous_commit = on
synchronous_standby_names = 'ANY $sk_q ($sk_list)'
CONF

echo "aethel compute: tenant=$AETHEL_TENANT timeline=$AETHEL_TIMELINE" \
     "pageserver=$AETHEL_PAGESERVER_HOST:$AETHEL_PAGESERVER_PORT quorum=ANY $sk_q of $sk_n"

if [ "${AETHEL_RENDER_ONLY:-0}" = "1" ]; then
  echo "--- rendered $conf ---"
  cat "$conf"
  exit 0
fi

exec postgres -D "$PGDATA"

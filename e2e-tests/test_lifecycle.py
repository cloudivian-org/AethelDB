# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
"""Full-deployment lifecycle tests against a *real* PostgreSQL compute node.

The runnable, sandbox-friendly end-to-end test lives in
``test_e2e_lifecycle.py`` and drives the real proxy/safekeeper/pageserver with a
mock compute node. This module is its counterpart for a *complete* deployment:
the patched PostgreSQL compute image is built and the stack is up
(``make compute-image`` + ``make up``), so these talk SQL over ``psycopg``
through the proxy exactly as a real client would.

They are skipped unless ``SP_E2E_REAL_STACK=1`` (and ``psycopg`` is installed),
because they require Docker and the compiled compute image.
"""
from __future__ import annotations

import os

import pytest

REAL_STACK = os.environ.get("SP_E2E_REAL_STACK") == "1"

psycopg = None
if REAL_STACK:
    try:
        import psycopg  # type: ignore
    except ImportError:
        psycopg = None

requires_real_stack = pytest.mark.skipif(
    not REAL_STACK or psycopg is None,
    reason="set SP_E2E_REAL_STACK=1 and install psycopg, with `make up` running",
)


def _dsn() -> str:
    host = os.environ.get("SP_PROXY_HOST", "127.0.0.1")
    port = os.environ.get("SP_PROXY_PORT", "5432")
    return f"host={host} port={port} dbname=e2e user=postgres"


@requires_real_stack
def test_cold_start_runs_sql():
    """A SELECT through the proxy while compute is asleep cold-starts it."""
    with psycopg.connect(_dsn()) as conn:  # type: ignore[union-attr]
        assert conn.execute("SELECT 1").fetchone()[0] == 1


@requires_real_stack
def test_write_then_read_back():
    """INSERTed rows (WAL -> safekeeper -> pageserver) read back correctly."""
    with psycopg.connect(_dsn(), autocommit=True) as conn:  # type: ignore[union-attr]
        conn.execute("CREATE TABLE IF NOT EXISTS kv (k text primary key, v text)")
        conn.execute("INSERT INTO kv VALUES ('a', 'durable') ON CONFLICT (k) DO UPDATE SET v=excluded.v")
        assert conn.execute("SELECT v FROM kv WHERE k='a'").fetchone()[0] == "durable"


@requires_real_stack
def test_data_survives_scale_to_zero():
    """Data written before an idle scale-to-zero is still present afterwards."""
    import time

    with psycopg.connect(_dsn(), autocommit=True) as conn:  # type: ignore[union-attr]
        conn.execute("CREATE TABLE IF NOT EXISTS kv (k text primary key, v text)")
        conn.execute("INSERT INTO kv VALUES ('persist', 'yes') ON CONFLICT (k) DO UPDATE SET v=excluded.v")

    # Wait past the configured idle window so compute scales to zero.
    time.sleep(int(os.environ.get("SP_IDLE_WAIT_SECS", "30")))

    with psycopg.connect(_dsn()) as conn:  # type: ignore[union-attr]
        assert conn.execute("SELECT v FROM kv WHERE k='persist'").fetchone()[0] == "yes"


def test_harness_imports():
    """A no-stack smoke test so this module always collects cleanly."""
    assert callable(_dsn) and _dsn().startswith("host=")

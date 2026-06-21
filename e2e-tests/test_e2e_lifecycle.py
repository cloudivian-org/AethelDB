# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
"""End-to-end lifecycle validation for AethelDB.

This is the comprehensive integration test the design calls for. It runs the
**real** `aethel-proxy`, `aethel-safekeeper`, and `aethel-pageserver` binaries and drives
them through their real wire protocols, with a thin mock compute node standing
in for the patched PostgreSQL engine (which cannot boot in this environment).

It validates the four required behaviours:

  1. A query fires at the proxy while all compute is scaled to zero, and the
     proxy cold-starts a compute node and returns the response.
  2. An INSERT streams WAL to the safekeeper (quorum-committed) and the page
     server materializes the resulting block.
  3. Forcing an idle scale-to-zero stops compute, and a follow-up query
     re-activates it and reads back the previously written data — proving data
     continuity across the compute going to zero.

If the Rust binaries are not built (`cargo build`), the whole module is skipped
with an explanatory message.
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import protocol as proto

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
BIN = ROOT / "target" / "debug"
PROXY, SAFEKEEPER, PAGESERVER = BIN / "aethel-proxy", BIN / "aethel-safekeeper", BIN / "aethel-pageserver"

# Idle window after which the proxy scales a tenant to zero (kept short for the test).
IDLE_SECS = 3
REAP_TICK_SECS = 1


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------
def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"port {port} did not come up within {timeout}s")


def compute_running(backend_port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", backend_port), timeout=0.3):
            return True
    except OSError:
        return False


def connect_through_proxy(proxy_port: int) -> socket.socket:
    """Do the SSL decline + PG startup handshake, returning a spliced socket."""
    sock = socket.create_connection(("127.0.0.1", proxy_port), timeout=10)
    sock.sendall(proto.ssl_request())
    assert sock.recv(1) == b"N", "proxy should decline SSL"
    sock.sendall(proto.startup_message(database="e2e"))
    sock.settimeout(10)
    return sock


ZERO_TL = "00" * 16  # TimelineId::ZERO (the root)


def http(method: str, port: int, path: str, body: dict | None = None):
    """Issue an HTTP request to a control/metrics endpoint; return (status, text)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def command(sock: socket.socket, line: str) -> str:
    sock.sendall(line.encode() + b"\n")
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("compute closed unexpectedly")
        buf += chunk
    return buf.split(b"\n", 1)[0].decode()


# ---------------------------------------------------------------------------
# The running stack (session-scoped).
# ---------------------------------------------------------------------------
class Stack:
    def __init__(self, tmp: Path):
        self.tmp = tmp
        self.procs: list[subprocess.Popen] = []
        self.proxy_port = free_port()
        self.backend_port = free_port()
        self.sk_port = free_port()
        self.ps_port = free_port()
        self.ps_ingest_port = free_port()
        # Control / HTTP / metrics endpoints (ephemeral to avoid fixed-port clashes).
        self.ps_control_port = free_port()
        self.ps_http_port = free_port()
        self.ps_metrics_port = free_port()
        self.sk_metrics_port = free_port()
        self.proxy_metrics_port = free_port()

    def _spawn(self, name: str, argv: list[str]) -> subprocess.Popen:
        log = open(self.tmp / f"{name}.log", "w")
        p = subprocess.Popen([str(a) for a in argv], stdout=log, stderr=subprocess.STDOUT)
        self.procs.append(p)
        return p

    def start(self) -> None:
        # Durable backend services (long-running).
        self._spawn("safekeeper", [
            SAFEKEEPER, "--listen", f"127.0.0.1:{self.sk_port}",
            "--data-dir", self.tmp / "sk", "--node-id", "1",
            "--metrics-listen", f"127.0.0.1:{self.sk_metrics_port}",
        ])
        self._spawn("pageserver", [
            PAGESERVER, "--listen", f"127.0.0.1:{self.ps_port}",
            "--ingest-listen", f"127.0.0.1:{self.ps_ingest_port}",
            "--object-dir", self.tmp / "obj", "--offload-tick-secs", "1",
            "--control-listen", f"127.0.0.1:{self.ps_control_port}",
            "--http-listen", f"127.0.0.1:{self.ps_http_port}",
            "--metrics-listen", f"127.0.0.1:{self.ps_metrics_port}",
        ])
        wait_port(self.sk_port)
        wait_port(self.ps_port)
        wait_port(self.ps_ingest_port)
        wait_port(self.ps_http_port)

        # Activation scripts the proxy runs to start/stop the mock compute.
        start_sh = self.tmp / "start.sh"
        stop_sh = self.tmp / "stop.sh"
        pidfile = self.tmp / "compute.pid"
        start_sh.write_text(
            f"#!/bin/sh\n"
            f'"{sys.executable}" "{HERE / "mock_compute.py"}" '
            f"--port {self.backend_port} "
            f"--safekeeper 127.0.0.1:{self.sk_port} "
            f"--pageserver 127.0.0.1:{self.ps_port} "
            f"--pageserver-ingest 127.0.0.1:{self.ps_ingest_port} "
            f'--state "{self.tmp / "lsn.state"}" '
            f'>> "{self.tmp / "compute.log"}" 2>&1 &\n'
            f'echo $! > "{pidfile}"\n'
        )
        stop_sh.write_text(
            f"#!/bin/sh\n"
            f'if [ -f "{pidfile}" ]; then kill "$(cat "{pidfile}")" 2>/dev/null; rm -f "{pidfile}"; fi\n'
        )

        # The proxy: routes tenant "e2e" to the (initially dead) compute backend.
        self._spawn("proxy", [
            PROXY, "--listen", f"127.0.0.1:{self.proxy_port}",
            "--tenant", f"e2e=127.0.0.1:{self.backend_port}",
            "--start-command", f'sh "{start_sh}"',
            "--stop-command", f'sh "{stop_sh}"',
            "--wake-budget-ms", "8000",
            "--idle-secs", str(IDLE_SECS),
            "--reap-tick-secs", str(REAP_TICK_SECS),
            "--metrics-listen", f"127.0.0.1:{self.proxy_metrics_port}",
        ])
        wait_port(self.proxy_port)

    def stop(self) -> None:
        # Stop the mock compute first (if running), then the services.
        try:
            pid = int((self.tmp / "compute.pid").read_text().strip())
            os.kill(pid, signal.SIGKILL)
        except (FileNotFoundError, ValueError, ProcessLookupError):
            pass
        for p in reversed(self.procs):
            p.terminate()
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


@pytest.fixture(scope="module")
def stack(tmp_path_factory) -> Stack:
    for b in (PROXY, SAFEKEEPER, PAGESERVER):
        if not b.exists():
            pytest.skip(f"binary {b} not built — run `cargo build` first")
    s = Stack(tmp_path_factory.mktemp("e2e"))
    s.start()
    try:
        yield s
    finally:
        s.stop()


# ---------------------------------------------------------------------------
# The lifecycle tests.
# ---------------------------------------------------------------------------
def test_cold_start_boots_compute_and_responds(stack: Stack):
    """(1)+(2) A request while compute is dead cold-starts it via the proxy."""
    assert not compute_running(stack.backend_port), "compute should start dead"

    sock = connect_through_proxy(stack.proxy_port)
    try:
        assert command(sock, "PING") == "PONG"
    finally:
        sock.close()

    # The proxy must have launched the compute backend to serve that request.
    assert compute_running(stack.backend_port), "proxy should have woken compute"


def test_insert_streams_wal_and_materializes_page(stack: Stack):
    """(3) INSERT commits WAL on the safekeeper and materializes a page."""
    sock = connect_through_proxy(stack.proxy_port)
    try:
        reply = command(sock, "INSERT alpha hello-world")
    finally:
        sock.close()

    assert reply.startswith("OK"), reply
    # WAL reached the safekeeper and was quorum-committed (commit_lsn advanced).
    commit_lsn = int(dict(kv.split("=") for kv in reply.split()[1:])["commit_lsn"])
    assert commit_lsn > 0, "safekeeper should report a non-zero commit LSN"

    # Independently confirm the page server materialized the block, by issuing a
    # real GetPage straight to the page server.
    block = proto.block_for("alpha")
    with socket.create_connection(("127.0.0.1", stack.ps_port), timeout=5) as ps:
        ps.sendall(proto.get_page_request(1663, 5, 16384, 0, block, proto.LATEST_LSN))
        status, payload = proto.read_page_response(ps)
    assert status == 0, "page server should have the materialized block"
    assert proto.decode_page(payload) == b"hello-world"


def test_data_survives_scale_to_zero(stack: Stack):
    """(4) After an idle scale-to-zero, a new query re-wakes compute and the
    previously written data is still readable — proving continuity."""
    # Write a value, then close the connection so the idle timer can start.
    sock = connect_through_proxy(stack.proxy_port)
    try:
        assert command(sock, "INSERT beta durable-value").startswith("OK")
    finally:
        sock.close()

    # Wait out the idle window; the proxy reaper must scale compute to zero.
    deadline = time.time() + IDLE_SECS + REAP_TICK_SECS + 6
    while time.time() < deadline and compute_running(stack.backend_port):
        time.sleep(0.2)
    assert not compute_running(stack.backend_port), "compute should be scaled to zero when idle"

    # A fresh query re-activates a brand-new compute process, which can only
    # return the value by reading it from the durable page server.
    sock = connect_through_proxy(stack.proxy_port)
    try:
        reply = command(sock, "SELECT beta")
    finally:
        sock.close()
    assert reply == "VALUE durable-value", reply
    assert compute_running(stack.backend_port), "the read should have re-woken compute"


def test_http_control_plane_api(stack: Stack):
    """The HTTP/JSON control plane creates timelines, branches, lists, and GCs."""
    # Health.
    status, text = http("GET", stack.ps_http_port, "/healthz")
    assert status == 200 and "ok" in text

    dev = "0a" * 16
    # Create a root timeline, then branch it.
    status, text = http("POST", stack.ps_http_port, "/v1/timelines", {"id": dev})
    assert status == 201, text

    branch = "0b" * 16
    status, text = http(
        "POST", stack.ps_http_port, "/v1/branches",
        {"timeline": branch, "parent": ZERO_TL, "lsn": 1},
    )
    assert status == 201, text

    # List shows the root, the new timeline, and the branch.
    status, text = http("GET", stack.ps_http_port, "/v1/timelines")
    assert status == 200
    listed = json.loads(text)["timelines"]
    assert ZERO_TL in listed and dev in listed and branch in listed

    # GC runs and reports stats.
    status, text = http("POST", stack.ps_http_port, "/v1/gc", {"horizon_lsn": 1})
    assert status == 200, text
    assert "versions_removed" in json.loads(text)


def test_prometheus_metrics_exposed(stack: Stack):
    """Each service exposes Prometheus metrics reflecting the work done above."""
    status, text = http("GET", stack.ps_metrics_port, "/metrics")
    assert status == 200
    # The earlier GetPage incremented this counter; the control-plane test created
    # timelines (the gauge is exported once touched).
    assert "aethel_pageserver_get_page_total" in text
    assert "aethel_pageserver_timelines" in text

    status, sk = http("GET", stack.sk_metrics_port, "/metrics")
    assert status == 200 and "aethel_safekeeper_appends_total" in sk

    status, px = http("GET", stack.proxy_metrics_port, "/metrics")
    assert status == 200 and "aethel_proxy_connections_total" in px

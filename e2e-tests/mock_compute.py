# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
"""Mock compute node for the AethelDB end-to-end test.

This stands in for the patched PostgreSQL compute engine (which cannot boot in
the test sandbox). It is deliberately thin, but it exercises the *real* services
through their *real* wire protocols:

  * The activation proxy boots and stops this process (scale-to-zero) and splices
    client connections to it after parsing the PostgreSQL startup packet.
  * On INSERT it streams a WAL record to the **real safekeeper** (and checks the
    returned quorum commit LSN) and materializes the resulting page in the
    **real page server** — i.e. it plays the role of both the WAL proposer and
    the WAL decoder.
  * On SELECT it reconstructs the value by asking the **real page server** for
    the page — so reads survive this process being killed (data continuity).

After the PostgreSQL startup packet (which the proxy forwards), the spliced
connection speaks a tiny line protocol: `INSERT <key> <value>`, `SELECT <key>`,
`PING`. The value lives only in the page server; the only local state is the
next WAL LSN, persisted so a restarted (re-activated) compute resumes
contiguously — standing in for the safekeeper resume handshake.
"""
from __future__ import annotations

import argparse
import os
import socket
import threading

import protocol as proto

# Fixed relation this mock writes into (a stand-in single table).
SPC, DB, REL, FORK = 1663, 5, 16384, 0
TERM = 1


class MockCompute:
    def __init__(self, args):
        self.sk_addr = _split(args.safekeeper)
        self.ps_addr = _split(args.pageserver)
        self.ingest_addr = _split(args.pageserver_ingest)
        self.state_path = args.state
        self.lock = threading.Lock()

    # --- WAL LSN bookkeeping (persisted across restarts) ---
    def _next_lsn(self) -> int:
        try:
            with open(self.state_path) as f:
                return int(f.read().strip() or "0")
        except FileNotFoundError:
            return 0

    def _set_next_lsn(self, lsn: int) -> None:
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(lsn))
        os.replace(tmp, self.state_path)

    # --- command handlers ---
    def do_insert(self, key: str, value: str) -> str:
        block = proto.block_for(key)
        payload = f"INSERT {key}={value}".encode()
        with self.lock:
            lsn = self._next_lsn()

            # 1. Stream the WAL record to the real safekeeper and require a
            #    quorum-committed acknowledgement.
            with socket.create_connection(self.sk_addr, timeout=5) as sk:
                sk.sendall(proto.wal_append(TERM, lsn, payload))
                resp = proto.read_wal_response(sk)
            if resp["status"] != 0:
                return f"ERR safekeeper status {resp['status']}"

            # 2. Materialize the page in the real page server (the role of the
            #    WAL decoder applying the record to storage).
            page = proto.encode_page(value.encode())
            body = proto.modification_image(SPC, DB, REL, FORK, block, lsn, page)
            with socket.create_connection(self.ingest_addr, timeout=5) as ing:
                if proto.send_modification(ing, body) != 0:
                    return "ERR pageserver ingest rejected"

            self._set_next_lsn(lsn + len(payload))
        return f"OK commit_lsn={resp['commit_lsn']} block={block}"

    def do_select(self, key: str) -> str:
        block = proto.block_for(key)
        with socket.create_connection(self.ps_addr, timeout=5) as ps:
            ps.sendall(proto.get_page_request(SPC, DB, REL, FORK, block, proto.LATEST_LSN))
            status, payload = proto.read_page_response(ps)
        if status != 0:
            return "VALUE "  # not found
        return "VALUE " + proto.decode_page(payload).decode(errors="replace")

    def handle(self, conn: socket.socket) -> None:
        # The proxy forwards the PostgreSQL startup packet first; consume it.
        hdr = proto.recv_exact(conn, 4)
        if hdr is None:
            return
        total = int.from_bytes(hdr, "big")
        if proto.recv_exact(conn, total - 4) is None:
            return

        # Then speak the line protocol over the spliced connection.
        buf = b""
        while True:
            while b"\n" not in buf:
                chunk = conn.recv(4096)
                if not chunk:
                    return  # client (proxy) closed -> end the session
                buf += chunk
            line, _, buf = buf.partition(b"\n")
            cmd = line.decode(errors="replace").strip()
            if not cmd:
                continue
            try:
                reply = self.dispatch(cmd)
            except Exception as e:  # surface backend errors to the client
                reply = f"ERR {e}"
            conn.sendall(reply.encode() + b"\n")

    def dispatch(self, cmd: str) -> str:
        parts = cmd.split(" ", 2)
        op = parts[0].upper()
        if op == "PING":
            return "PONG"
        if op == "INSERT" and len(parts) == 3:
            return self.do_insert(parts[1], parts[2])
        if op == "SELECT" and len(parts) == 2:
            return self.do_select(parts[1])
        return f"ERR bad command: {cmd!r}"


def _split(hostport: str) -> tuple[str, int]:
    host, port = hostport.rsplit(":", 1)
    return host, int(port)


def main() -> None:
    ap = argparse.ArgumentParser(description="AethelDB mock compute node")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--safekeeper", required=True, help="host:port")
    ap.add_argument("--pageserver", required=True, help="page-service host:port")
    ap.add_argument("--pageserver-ingest", required=True, help="ingest host:port")
    ap.add_argument("--state", required=True, help="LSN state file")
    args = ap.parse_args()

    compute = MockCompute(args)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", args.port))
    srv.listen(16)

    while True:
        conn, _ = srv.accept()
        threading.Thread(target=compute.handle, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()

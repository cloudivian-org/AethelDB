# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
"""Python encoders for the AethelDB wire protocols.

These mirror, byte-for-byte, the formats defined and tested in the Rust `common`
crate (`page_service`, `wal_service`) and the PostgreSQL v3 startup packet. They
are shared by the mock compute node and the end-to-end test so both speak the
exact protocols the real services expect.
"""
from __future__ import annotations

import socket
import struct
import zlib

PAGE_SIZE = 8192

# Magic numbers (match common::page_service / common::wal_service).
PAGE_MAGIC = 0x53504731  # "SPG1"
WAL_MAGIC = 0x53574C31   # "SWL1"

ZERO_ID = b"\x00" * 16   # all-zero tenant/timeline (single-tenant dev)

# ---------------------------------------------------------------------------
# Low-level socket helpers.
# ---------------------------------------------------------------------------
def recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes, or return None on clean EOF."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ---------------------------------------------------------------------------
# PostgreSQL v3 startup (what the proxy parses to route the connection).
# ---------------------------------------------------------------------------
def ssl_request() -> bytes:
    return struct.pack(">ii", 8, 80877103)


def startup_message(database: str, user: str = "compute") -> bytes:
    body = struct.pack(">i", 196608)  # protocol 3.0
    for k, v in (("user", user), ("database", database)):
        body += k.encode() + b"\x00" + v.encode() + b"\x00"
    body += b"\x00"
    return struct.pack(">i", len(body) + 4) + body


# ---------------------------------------------------------------------------
# WAL ingest protocol (compute -> safekeeper).
# ---------------------------------------------------------------------------
def wal_append(term: int, start_lsn: int, payload: bytes,
               tenant: bytes = ZERO_ID, timeline: bytes = ZERO_ID) -> bytes:
    b = struct.pack(">IBBH", WAL_MAGIC, 1, 1, 0)   # magic, ver, type=append, reserved
    b += tenant + timeline
    b += struct.pack(">QQI", term, start_lsn, len(payload))
    return b + payload


def read_wal_response(sock: socket.socket) -> dict:
    raw = recv_exact(sock, 32)
    if raw is None:
        raise ConnectionError("safekeeper closed during response")
    magic, ver, status, _r, term, flush, commit = struct.unpack(">IBBHQQQ", raw)
    return {"status": status, "term": term, "flush_lsn": flush, "commit_lsn": commit}


# ---------------------------------------------------------------------------
# Page service protocol (compute -> pageserver).
# ---------------------------------------------------------------------------
LATEST_LSN = 0xFFFFFFFFFFFFFFFF  # request the newest version of a page


def get_page_request(spc: int, db: int, rel: int, fork: int, block: int, lsn: int,
                     tenant: bytes = ZERO_ID, timeline: bytes = ZERO_ID) -> bytes:
    b = struct.pack(">IBBBB", PAGE_MAGIC, 1, 1, fork, 0)  # magic, ver, type=getpage, fork, flags
    b += tenant + timeline
    b += struct.pack(">III", spc, db, rel)
    b += struct.pack(">I", block)
    b += struct.pack(">Q", lsn)
    return b


def read_page_response(sock: socket.socket) -> tuple[int, bytes]:
    """Return (status, payload). status: 0 ok, 1 not_found, 2 error."""
    head = recv_exact(sock, 12)  # magic(4) ver(1) status(1) rsvd(2) len(4)
    if head is None:
        raise ConnectionError("pageserver closed during response")
    status = head[5]
    payload_len = struct.unpack(">I", head[8:12])[0]
    payload = recv_exact(sock, payload_len) if payload_len else b""
    if payload_len and payload is None:
        raise ConnectionError("pageserver truncated payload")
    return status, payload or b""


# ---------------------------------------------------------------------------
# WAL-modification ingest (the WAL decoder -> pageserver).
# ---------------------------------------------------------------------------
def modification_image(spc: int, db: int, rel: int, fork: int, block: int,
                       lsn: int, page: bytes) -> bytes:
    """A full-page-image modification body (matches pageserver::page::Modification)."""
    assert len(page) == PAGE_SIZE
    b = struct.pack(">III", spc, db, rel) + bytes([fork])
    b += struct.pack(">I", block) + struct.pack(">Q", lsn)
    b += bytes([0]) + page  # version tag 0 = Image
    return b


def send_modification(sock: socket.socket, body: bytes) -> int:
    """Length-prefix and send a modification; return the 1-byte ack status."""
    sock.sendall(struct.pack(">I", len(body)) + body)
    ack = recv_exact(sock, 1)
    if ack is None:
        raise ConnectionError("pageserver closed during ingest ack")
    return ack[0]


# ---------------------------------------------------------------------------
# Tiny value<->page codec used by the mock compute (a stand-in for heap tuples).
# ---------------------------------------------------------------------------
def block_for(key: str) -> int:
    """Deterministically map a key to a block number (no stored mapping needed)."""
    return zlib.crc32(key.encode()) % 4096


def encode_page(value: bytes) -> bytes:
    """Lay a value into an 8 KiB page as [u32 len][value][zero padding]."""
    body = struct.pack(">I", len(value)) + value
    assert len(body) <= PAGE_SIZE, "value too large for one page"
    return body + b"\x00" * (PAGE_SIZE - len(body))


def decode_page(page: bytes) -> bytes:
    if len(page) < 4:
        return b""
    n = struct.unpack(">I", page[:4])[0]
    return page[4:4 + n]

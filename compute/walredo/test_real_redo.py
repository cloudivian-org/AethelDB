#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The AethelDB Authors
#
# Drive `postgres --wal-redo` with one real WAL record and verify it reconstructs
# the page byte-for-byte (modulo the pd_lsn stamp). Invoked by verify.sh, which
# generates the record; see compute/walredo/README.md.
#
# Usage: test_real_redo.py PREFIX PGDATA BEFORE_LSN AFTER_LSN

import os
import re
import struct
import subprocess
import sys

PAGE = 8192
SEGSZ = 16 * 1024 * 1024
REQ_MAGIC, RESP_MAGIC, VER = 0x57524431, 0x57525231, 1


def main():
    prefix, pgdata, before, after = sys.argv[1:5]

    # 1. Find the heap INSERT record via pg_waldump.
    dump = subprocess.run(
        [f"{prefix}/bin/pg_waldump", "-p", f"{pgdata}/pg_wal", "--start", before, "--end", after],
        capture_output=True, text=True).stdout
    lines = [l for l in dump.splitlines() if "Heap" in l and "INSERT" in l]
    if not lines:
        print("FAIL: no heap INSERT record found in range")
        sys.exit(1)
    line = lines[0]
    print("record:", line.strip())
    tot = int(re.search(r"rec/tot\):\s*\d+/\s*(\d+)", line).group(1))
    lsn_s = re.search(r"lsn: ([0-9A-Fa-f]+/[0-9A-Fa-f]+)", line).group(1)
    spc, db, rel, blk = map(int, re.search(r"rel (\d+)/(\d+)/(\d+) blk (\d+)", line).groups())
    hi, lo = lsn_s.split("/")
    lsn = (int(hi, 16) << 32) | int(lo, 16)

    # 2. Read the raw record bytes out of the WAL segment.
    segno = lsn // SEGSZ
    segs_per_id = (1 << 32) // SEGSZ
    fname = "%08X%08X%08X" % (1, segno // segs_per_id, segno % segs_per_id)
    offset = lsn - segno * SEGSZ
    if offset % PAGE + tot > PAGE:
        print("SKIP: record crosses a WAL page boundary (rerun; pg_switch_wal should avoid this)")
        sys.exit(2)
    with open(f"{pgdata}/pg_wal/{fname}", "rb") as f:
        f.seek(offset)
        record = f.read(tot)
    print(f"extracted {tot}-byte record at {fname}+{offset} (lsn {lsn:#x}) rel {spc}/{db}/{rel} blk {blk}")

    # 3. The page the real server produced.
    with open(f"{pgdata}/base/{db}/{rel}", "rb") as f:
        expected = f.read(PAGE)

    # 4. Drive postgres --wal-redo with a zero base + this one record.
    req = bytearray(struct.pack(">I", REQ_MAGIC)) + bytes([VER, 0x01, 0, 0])
    req += struct.pack(">III", spc, db, rel) + bytes([0, 0, 0, 0]) + struct.pack(">I", blk)
    req += b"\x00" * PAGE
    req += struct.pack(">I", 1) + struct.pack(">Q", lsn) + struct.pack(">I", len(record)) + record

    proc = subprocess.Popen(
        [f"{prefix}/bin/postgres", "--wal-redo", "-D", pgdata, "postgres"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = proc.communicate(input=bytes(req), timeout=30)
    if len(out) < 8 or struct.unpack(">I", out[:4])[0] != RESP_MAGIC or out[5] != 0:
        print("FAIL: bad response", out[:16])
        print(err.decode(errors="replace")[-1500:])
        sys.exit(1)
    result = out[8:8 + PAGE]

    # 5. Compare, ignoring pd_lsn (first 8 bytes), which differs by WAL framing.
    diff = [i for i in range(PAGE) if result[i] != expected[i]]
    if not diff:
        print("PASS: redone page is byte-identical to the real server's page")
    elif all(i < 8 for i in diff):
        print("PASS: redone page matches the real page except the pd_lsn stamp "
              f"({len(diff)} byte(s); expected: WAL framing)")
    else:
        print(f"FAIL: content mismatch at {len(diff)} offsets:", diff[:20])
        sys.exit(1)


if __name__ == "__main__":
    main()

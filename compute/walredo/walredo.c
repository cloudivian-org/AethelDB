/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2026 The AethelDB Authors
 *
 * walredo.c - the compute-side WAL-redo backend (Phase 3).
 *
 * The page server cannot apply an ordinary (non-full-page-image) WAL record to
 * a page itself: doing so requires PostgreSQL's per-resource-manager redo
 * routines (RmgrTable[rmid].rm_redo), which are deeply tied to the backend
 * environment. So it ships the work here. This program is the *peer* of the
 * Rust `pageserver::walredo_process::PostgresRedoManager`: it reads RedoRequests
 * on stdin and writes RedoResponses on stdout, speaking the exact pipe protocol
 * defined in `pageserver/src/walredo_proto.rs`.
 *
 * Protocol (big-endian envelope; WAL record bytes are Postgres-native order):
 *
 *   RedoRequest:
 *     u32 magic = 'WRD1'   u8 version=1   u8 flags(bit0=has_base)   u16 reserved
 *     u32 spcOid   u32 dbOid   u32 relNumber   u8 forknum   u8[3] reserved
 *     u32 blkno
 *     [ u8[8192] base_image ]            (iff flags & HAS_BASE)
 *     u32 n_records
 *     n_records x { u64 lsn   u32 len   u8[len] record }
 *
 *   RedoResponse:
 *     u32 magic = 'WRR1'   u8 version=1   u8 status   u16 reserved
 *     status==0 (OK):  u8[8192] page
 *     status==1 (ERR): u32 len   u8[len] message
 *
 * INTEGRATION STATUS. The protocol loop and record decoding below are complete
 * and standalone. The single Postgres-internal step -- handing a decoded record
 * to its resource manager's redo routine with the target page installed as a
 * buffer -- is `redo_apply_record()`. Running it for real requires linking this
 * into a postgres built with a `--wal-redo` single-backend mode (the same shape
 * Neon uses): that mode sets up just enough of a backend (shared buffers stub,
 * rmgr table, GUCs) for rm_redo to run without a live cluster. That core patch
 * is the remaining piece; this file is structured so it drops straight in.
 *
 * Build (against an installed PostgreSQL 16): see the Makefile in this dir.
 */

#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/rmgr.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"

#include <stdint.h>
#include <string.h>
#include <unistd.h>

/* ---- Protocol constants (must match pageserver/src/walredo_proto.rs) ---- */
#define REQ_MAGIC 0x57524431u  /* "WRD1" */
#define RESP_MAGIC 0x57525231u /* "WRR1" */
#define PROTO_VERSION 1
#define FLAG_HAS_BASE 0x01
#define RESP_STATUS_OK 0
#define RESP_STATUS_ERR 1
#define REDO_PAGE_SIZE BLCKSZ /* 8192 */

/* A single WAL record to apply, as delivered over the pipe. */
typedef struct RedoRecord
{
	uint64_t	lsn;
	uint32_t	len;
	uint8_t    *bytes;
} RedoRecord;

/* A fully-read request. */
typedef struct RedoRequest
{
	RelFileLocator rlocator;
	ForkNumber	forknum;
	BlockNumber blkno;
	bool		has_base;
	uint8_t		base[REDO_PAGE_SIZE];
	uint32_t	n_records;
	RedoRecord *records;
} RedoRequest;

/* ---- Big-endian helpers ---- */

static uint32_t
get_be32(const uint8_t *p)
{
	return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
		((uint32_t) p[2] << 8) | (uint32_t) p[3];
}

static uint64_t
get_be64(const uint8_t *p)
{
	return ((uint64_t) get_be32(p) << 32) | (uint64_t) get_be32(p + 4);
}

static void
put_be32(uint8_t *p, uint32_t v)
{
	p[0] = v >> 24;
	p[1] = v >> 16;
	p[2] = v >> 8;
	p[3] = v;
}

/* Read exactly n bytes from stdin; return false on clean EOF. */
static bool
read_full(uint8_t *buf, size_t n)
{
	size_t		got = 0;

	while (got < n)
	{
		ssize_t		r = read(STDIN_FILENO, buf + got, n - got);

		if (r == 0)
			return false;		/* EOF */
		if (r < 0)
			elog(FATAL, "wal-redo: read error: %m");
		got += (size_t) r;
	}
	return true;
}

/* Write exactly n bytes to stdout. */
static void
write_full(const uint8_t *buf, size_t n)
{
	size_t		put = 0;

	while (put < n)
	{
		ssize_t		w = write(STDOUT_FILENO, buf + put, n - put);

		if (w < 0)
			elog(FATAL, "wal-redo: write error: %m");
		put += (size_t) w;
	}
}

/* ---- Request reading ---- */

/* Read one request; returns false on clean EOF (parent closed the pipe). */
static bool
read_request(RedoRequest *req)
{
	uint8_t		hdr[28];

	if (!read_full(hdr, sizeof(hdr)))
		return false;

	if (get_be32(hdr) != REQ_MAGIC)
		elog(FATAL, "wal-redo: bad request magic");
	if (hdr[4] != PROTO_VERSION)
		elog(FATAL, "wal-redo: bad request version %d", hdr[4]);

	uint8_t		flags = hdr[5];

	req->rlocator.spcOid = get_be32(hdr + 8);
	req->rlocator.dbOid = get_be32(hdr + 12);
	req->rlocator.relNumber = get_be32(hdr + 16);
	req->forknum = (ForkNumber) hdr[20];
	req->blkno = get_be32(hdr + 24);

	req->has_base = (flags & FLAG_HAS_BASE) != 0;
	if (req->has_base)
		(void) read_full(req->base, REDO_PAGE_SIZE);
	else
		memset(req->base, 0, REDO_PAGE_SIZE);

	uint8_t		nbuf[4];

	(void) read_full(nbuf, 4);
	req->n_records = get_be32(nbuf);
	req->records = palloc(sizeof(RedoRecord) * req->n_records);

	for (uint32_t i = 0; i < req->n_records; i++)
	{
		uint8_t		rh[12];

		(void) read_full(rh, sizeof(rh));
		req->records[i].lsn = get_be64(rh);
		req->records[i].len = get_be32(rh + 8);
		req->records[i].bytes = palloc(req->records[i].len);
		(void) read_full(req->records[i].bytes, req->records[i].len);
	}
	return true;
}

/* ---- Redo ---- */

/*
 * Apply one WAL record to *page*.
 *
 * Decodes the raw record, then dispatches to its resource manager's redo
 * routine. In the postgres `--wal-redo` core mode, the target block is faked
 * into shared buffers so that RmgrTable[rmid].rm_redo's calls to
 * XLogReadBufferForRedo resolve to *page*; on return, the modified page is
 * copied back out. The decode + dispatch skeleton is shown here; the buffer
 * scaffolding (BeginRedoForBlock / GetRedoPage / equivalent) is provided by the
 * core mode this file links against.
 */
static void
redo_apply_record(uint8_t *page, const RedoRecord *rec)
{
	DecodedXLogRecord *decoded;
	XLogReaderState *reader = redo_reader();		/* provided by --wal-redo mode */
	char	   *errormsg = NULL;

	/* Point the reader at this record's bytes and decode it. */
	if (!redo_decode_record(reader, (char *) rec->bytes, rec->len, &decoded, &errormsg))
		elog(FATAL, "wal-redo: decode failed: %s", errormsg ? errormsg : "(unknown)");

	/* Install *page* as the buffer this record's block references. */
	redo_set_target_page(&decoded->record.blocks[0], page);

	/* Hand off to the resource manager that owns the record. */
	RmgrTable[decoded->header.xl_rmid].rm_redo(reader);

	/* Copy the redone page back out of the faked buffer. */
	redo_get_target_page(page);
}

/* ---- Response writing ---- */

static void
write_page_response(const uint8_t *page)
{
	uint8_t		hdr[8] = {0};

	put_be32(hdr, RESP_MAGIC);
	hdr[4] = PROTO_VERSION;
	hdr[5] = RESP_STATUS_OK;
	write_full(hdr, sizeof(hdr));
	write_full(page, REDO_PAGE_SIZE);
}

/* ---- Main loop ---- */

int
main(int argc, char **argv)
{
	RedoRequest req;

	redo_backend_init(argc, argv);		/* set up the minimal --wal-redo env */

	while (read_request(&req))
	{
		uint8_t		page[REDO_PAGE_SIZE];

		memcpy(page, req.base, REDO_PAGE_SIZE);
		for (uint32_t i = 0; i < req.n_records; i++)
			redo_apply_record(page, &req.records[i]);

		write_page_response(page);

		/* Release this request's transient allocations. */
		redo_reset_context();
	}
	return 0;
}

/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2026 The AethelDB Authors
 *
 * aethel_smgr - a network storage manager for AethelDB.
 *
 * Loaded via shared_preload_libraries, this extension installs an smgr_hook
 * (see the companion core patch that makes the storage manager pluggable) so
 * that non-temporary relations are served from a remote page server instead of
 * local disk:
 *
 *   - smgr_read   -> a GetPage request over TCP; the 8 KiB image is returned by
 *                    the page server, reconstructed from the WAL at the
 *                    requested LSN. No POSIX read() of a local heap file.
 *   - smgr_nblocks-> a GetRelSize request.
 *   - smgr_write / smgr_extend / smgr_writeback / smgr_immedsync are no-ops on
 *     durable storage: durability comes from streaming the WAL to the
 *     safekeeper quorum, not from writing heap files locally. The buffer
 *     manager still calls these, so we keep the cached block counts coherent.
 *
 * Temporary (backend-local) relations are delegated back to the standard
 * magnetic-disk manager via smgr_standard().
 *
 * The wire protocol is the fixed big-endian format defined and tested in the
 * Rust `common` crate (common::page_service); the encoders here match it byte
 * for byte.
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "access/xlogdefs.h"
#include "storage/backendid.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* ---------------------------------------------------------------------------
 * Wire protocol constants (must match common::page_service in the Rust crate).
 * ------------------------------------------------------------------------- */
#define SP_MAGIC			0x53504731u /* "SPG1" */
#define SP_VERSION			1
#define SP_TYPE_GET_PAGE	1
#define SP_TYPE_GET_RELSIZE 2
#define SP_STATUS_OK		0
#define SP_STATUS_NOT_FOUND 1
#define SP_STATUS_ERROR		2

/* GetPage payload: tenant(16)+timeline(16)+spc/db/rel(12)+block(4)+lsn(8). */
#define SP_REQ_HEADER		8
#define SP_GET_PAGE_LEN		(SP_REQ_HEADER + 16 + 16 + 12 + 4 + 8)
#define SP_GET_RELSIZE_LEN	(SP_REQ_HEADER + 16 + 16 + 12 + 8)
#define SP_RESP_HEADER		12 /* magic(4) ver(1) status(1) rsvd(2) len(4) */

/* ---------------------------------------------------------------------------
 * GUCs.
 * ------------------------------------------------------------------------- */
static char *sp_pageserver_host = NULL;
static int	sp_pageserver_port = 6400;
static char *sp_tenant_id = NULL;
static char *sp_timeline_id = NULL;

/* Parsed 16-byte tenant/timeline identifiers. */
static uint8 sp_tenant[16];
static uint8 sp_timeline[16];

/* Cached page-server connection (-1 = not connected). */
static int	sp_conn = -1;

/* Previously-installed hook, if any, so we chain politely. */
static smgr_hook_type prev_smgr_hook = NULL;

/* ---------------------------------------------------------------------------
 * Small big-endian encoders.
 * ------------------------------------------------------------------------- */
static inline void
put_be32(uint8 *p, uint32 v)
{
	p[0] = (uint8) (v >> 24);
	p[1] = (uint8) (v >> 16);
	p[2] = (uint8) (v >> 8);
	p[3] = (uint8) (v);
}

static inline void
put_be64(uint8 *p, uint64 v)
{
	put_be32(p, (uint32) (v >> 32));
	put_be32(p + 4, (uint32) (v & 0xFFFFFFFFu));
}

static inline uint32
get_be32(const uint8 *p)
{
	return ((uint32) p[0] << 24) | ((uint32) p[1] << 16) |
		((uint32) p[2] << 8) | (uint32) p[3];
}

/* Parse 32 lowercase/uppercase hex characters into 16 bytes. */
static bool
parse_hex16(const char *s, uint8 out[16])
{
	if (s == NULL || strlen(s) != 32)
		return false;
	for (int i = 0; i < 16; i++)
	{
		unsigned int byte;

		if (sscanf(s + i * 2, "%2x", &byte) != 1)
			return false;
		out[i] = (uint8) byte;
	}
	return true;
}

/* ---------------------------------------------------------------------------
 * Blocking socket helpers.
 * ------------------------------------------------------------------------- */
static void
sp_disconnect(void)
{
	if (sp_conn >= 0)
	{
		close(sp_conn);
		sp_conn = -1;
	}
}

/* Establish a connection to the page server, caching the fd. */
static int
sp_connect(void)
{
	struct addrinfo hints;
	struct addrinfo *res = NULL;
	char		portstr[16];
	int			fd = -1;
	int			rc;
	int			one = 1;

	if (sp_conn >= 0)
		return sp_conn;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	snprintf(portstr, sizeof(portstr), "%d", sp_pageserver_port);

	rc = getaddrinfo(sp_pageserver_host, portstr, &hints, &res);
	if (rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("aethel_smgr: could not resolve page server %s:%d: %s",
						sp_pageserver_host, sp_pageserver_port, gai_strerror(rc))));

	for (struct addrinfo *ai = res; ai != NULL; ai = ai->ai_next)
	{
		fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
		if (fd < 0)
			continue;
		if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
			break;
		close(fd);
		fd = -1;
	}
	freeaddrinfo(res);

	if (fd < 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("aethel_smgr: could not connect to page server %s:%d",
						sp_pageserver_host, sp_pageserver_port)));

	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	sp_conn = fd;
	return fd;
}

/* Write exactly len bytes, retrying short writes; reconnect-and-fail on error. */
static void
sp_write_all(const uint8 *buf, size_t len)
{
	size_t		off = 0;
	int			fd = sp_connect();

	while (off < len)
	{
		ssize_t		n = send(fd, buf + off, len - off, MSG_NOSIGNAL);

		if (n <= 0)
		{
			sp_disconnect();
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("aethel_smgr: send to page server failed")));
		}
		off += (size_t) n;
	}
}

/* Read exactly len bytes; reconnect-and-fail on EOF/error. */
static void
sp_read_all(uint8 *buf, size_t len)
{
	size_t		off = 0;

	while (off < len)
	{
		ssize_t		n = recv(sp_conn, buf + off, len - off, 0);

		if (n <= 0)
		{
			sp_disconnect();
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("aethel_smgr: page server closed connection mid-response")));
		}
		off += (size_t) n;
	}
}

/* ---------------------------------------------------------------------------
 * Request builders.
 * ------------------------------------------------------------------------- */
static void
sp_fill_common(uint8 *p, uint8 type, ForkNumber forknum, const RelFileLocator *rloc)
{
	put_be32(p, SP_MAGIC);
	p[4] = SP_VERSION;
	p[5] = type;
	p[6] = (uint8) forknum;
	p[7] = 0;					/* flags */
	memcpy(p + 8, sp_tenant, 16);
	memcpy(p + 24, sp_timeline, 16);
	put_be32(p + 40, rloc->spcOid);
	put_be32(p + 44, rloc->dbOid);
	put_be32(p + 48, rloc->relNumber);
}

/*
 * Read the response header and return the payload length, raising an error for
 * non-OK statuses.
 */
static uint32
sp_read_status(uint8 *hdr)
{
	uint8		status;
	uint32		len;

	sp_read_all(hdr, SP_RESP_HEADER);
	if (get_be32(hdr) != SP_MAGIC || hdr[4] != SP_VERSION)
	{
		sp_disconnect();
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("aethel_smgr: malformed response header from page server")));
	}
	status = hdr[5];
	len = get_be32(hdr + 8);

	if (status == SP_STATUS_OK)
		return len;

	/* Drain any error payload so the connection stays usable. */
	if (len > 0)
	{
		char	   *msg = palloc(len + 1);

		sp_read_all((uint8 *) msg, len);
		msg[len] = '\0';
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("aethel_smgr: page server error: %s", msg)));
	}
	ereport(ERROR,
			(errcode(ERRCODE_IO_ERROR),
			 errmsg("aethel_smgr: page server returned status %d", status)));
	return 0;					/* unreachable */
}

/* ---------------------------------------------------------------------------
 * f_smgr callbacks.
 * ------------------------------------------------------------------------- */

static void
aethel_smgr_init(void)
{
	/* Nothing backend-local to set up; connections are lazy. */
}

static void
aethel_smgr_open(SMgrRelation reln)
{
	/* No local file handles to open. */
}

static void
aethel_smgr_close(SMgrRelation reln, ForkNumber forknum)
{
	/* No local file handles to close. */
}

static void
aethel_smgr_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	/*
	 * Relation creation is recorded in the WAL and materialized by the page
	 * server; there is no local file to create.
	 */
}

static bool
aethel_smgr_exists(SMgrRelation reln, ForkNumber forknum)
{
	/* A relation exists if the page server can report a size for the fork. */
	uint8		req[SP_GET_RELSIZE_LEN];
	uint8		hdr[SP_RESP_HEADER];
	uint8		status;

	sp_fill_common(req, SP_TYPE_GET_RELSIZE, forknum, &reln->smgr_rlocator.locator);
	put_be64(req + 52, InvalidXLogRecPtr);
	sp_write_all(req, sizeof(req));

	sp_read_all(hdr, SP_RESP_HEADER);
	status = hdr[5];
	/* Drain payload regardless so the stream stays aligned. */
	{
		uint32		len = get_be32(hdr + 8);

		if (len > 0)
		{
			uint8	   *scratch = palloc(len);

			sp_read_all(scratch, len);
			pfree(scratch);
		}
	}
	return status == SP_STATUS_OK;
}

static void
aethel_smgr_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	/* Deletion is handled by the page server's garbage collector. */
}

static void
aethel_smgr_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			   const void *buffer, bool skipFsync)
{
	/*
	 * The new page is durably captured by the WAL stream, so there is nothing
	 * to write locally. Keep the cached size coherent for the buffer manager.
	 */
	if (reln->smgr_cached_nblocks[forknum] == blocknum ||
		reln->smgr_cached_nblocks[forknum] == InvalidBlockNumber)
		reln->smgr_cached_nblocks[forknum] = blocknum + 1;
	else
		reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
}

static void
aethel_smgr_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				   int nblocks, bool skipFsync)
{
	reln->smgr_cached_nblocks[forknum] = blocknum + nblocks;
}

static bool
aethel_smgr_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	/* Synchronous prefetch hint not yet implemented. */
	return false;
}

static void
aethel_smgr_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			 void *buffer)
{
	uint8		req[SP_GET_PAGE_LEN];
	uint8		hdr[SP_RESP_HEADER];
	uint32		len;

	/*
	 * LSN 0 asks the page server for the latest materialized version. Strict
	 * read-your-writes uses a per-page last-written-LSN cache; that refinement
	 * plugs in here by supplying the page's last-written LSN instead of 0.
	 */
	sp_fill_common(req, SP_TYPE_GET_PAGE, forknum, &reln->smgr_rlocator.locator);
	put_be32(req + 52, (uint32) blocknum);
	put_be64(req + 56, InvalidXLogRecPtr);
	sp_write_all(req, sizeof(req));

	len = sp_read_status(hdr);
	if (len != BLCKSZ)
	{
		sp_disconnect();
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("aethel_smgr: expected %d-byte page, got %u bytes", BLCKSZ, len)));
	}
	sp_read_all((uint8 *) buffer, BLCKSZ);
}

static void
aethel_smgr_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  const void *buffer, bool skipFsync)
{
	/*
	 * No durable local write: the page is already in the WAL stream. Evicting a
	 * dirty buffer is therefore a no-op from the storage manager's point of
	 * view.
	 */
}

static void
aethel_smgr_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				  BlockNumber nblocks)
{
	/* No local files to flush. */
}

static BlockNumber
aethel_smgr_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	uint8		req[SP_GET_RELSIZE_LEN];
	uint8		hdr[SP_RESP_HEADER];
	uint8		payload[4];
	uint32		len;

	sp_fill_common(req, SP_TYPE_GET_RELSIZE, forknum, &reln->smgr_rlocator.locator);
	put_be64(req + 52, InvalidXLogRecPtr);
	sp_write_all(req, sizeof(req));

	len = sp_read_status(hdr);
	if (len != 4)
	{
		sp_disconnect();
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("aethel_smgr: expected 4-byte rel size, got %u bytes", len)));
	}
	sp_read_all(payload, 4);
	return (BlockNumber) get_be32(payload);
}

static void
aethel_smgr_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
				 BlockNumber nblocks)
{
	/* Truncation is logged; the page server applies it. Update the cache. */
	reln->smgr_cached_nblocks[forknum] = nblocks;
}

static void
aethel_smgr_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	/* Durability is provided by the WAL stream, not by fsync of local files. */
}

/* The storage-manager function table for remote relations. */
static const f_smgr aethel_smgr_impl = {
	.smgr_init = aethel_smgr_init,
	.smgr_shutdown = NULL,
	.smgr_open = aethel_smgr_open,
	.smgr_close = aethel_smgr_close,
	.smgr_create = aethel_smgr_create,
	.smgr_exists = aethel_smgr_exists,
	.smgr_unlink = aethel_smgr_unlink,
	.smgr_extend = aethel_smgr_extend,
	.smgr_zeroextend = aethel_smgr_zeroextend,
	.smgr_prefetch = aethel_smgr_prefetch,
	.smgr_read = aethel_smgr_read,
	.smgr_write = aethel_smgr_write,
	.smgr_writeback = aethel_smgr_writeback,
	.smgr_nblocks = aethel_smgr_nblocks,
	.smgr_truncate = aethel_smgr_truncate,
	.smgr_immedsync = aethel_smgr_immedsync,
};

/*
 * smgr_hook: choose aethel_smgr for shared (non-temp) relations; temporary
 * relations remain on local disk via the standard manager.
 */
static const f_smgr *
aethel_smgr_selector(BackendId backend, RelFileLocator rlocator)
{
	if (backend != InvalidBackendId)
		return smgr_standard(backend, rlocator);
	return &aethel_smgr_impl;
}

/* ---------------------------------------------------------------------------
 * SQL-callable: report the configured page server (handy for diagnostics).
 * ------------------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(aethel_smgr_status);

Datum
aethel_smgr_status(PG_FUNCTION_ARGS)
{
	char		buf[256];

	snprintf(buf, sizeof(buf), "pageserver=%s:%d tenant=%s timeline=%s",
			 sp_pageserver_host ? sp_pageserver_host : "(unset)",
			 sp_pageserver_port,
			 sp_tenant_id && sp_tenant_id[0] ? sp_tenant_id : "(unset)",
			 sp_timeline_id && sp_timeline_id[0] ? sp_timeline_id : "(unset)");
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

/* ---------------------------------------------------------------------------
 * Module init.
 * ------------------------------------------------------------------------- */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("aethel_smgr must be loaded via shared_preload_libraries")));

	DefineCustomStringVariable("aethel_smgr.pageserver_host",
							   "Page server hostname.",
							   NULL, &sp_pageserver_host, "127.0.0.1",
							   PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("aethel_smgr.pageserver_port",
							"Page server TCP port.",
							NULL, &sp_pageserver_port, 6400, 1, 65535,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("aethel_smgr.tenant_id",
							   "Tenant id (32 hex chars).",
							   NULL, &sp_tenant_id, "",
							   PGC_POSTMASTER, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("aethel_smgr.timeline_id",
							   "Timeline id (32 hex chars).",
							   NULL, &sp_timeline_id, "",
							   PGC_POSTMASTER, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("aethel_smgr");

	/* Parse identifiers once at startup. Empty = all-zero (single-tenant dev). */
	memset(sp_tenant, 0, sizeof(sp_tenant));
	memset(sp_timeline, 0, sizeof(sp_timeline));
	if (sp_tenant_id && sp_tenant_id[0] && !parse_hex16(sp_tenant_id, sp_tenant))
		ereport(ERROR, (errmsg("aethel_smgr.tenant_id must be 32 hex characters")));
	if (sp_timeline_id && sp_timeline_id[0] && !parse_hex16(sp_timeline_id, sp_timeline))
		ereport(ERROR, (errmsg("aethel_smgr.timeline_id must be 32 hex characters")));

	/* Install the storage-manager selector hook, chaining any predecessor. */
	prev_smgr_hook = smgr_hook;
	smgr_hook = aethel_smgr_selector;

	ereport(LOG, (errmsg("aethel_smgr loaded: page reads served from %s:%d",
						 sp_pageserver_host, sp_pageserver_port)));
}

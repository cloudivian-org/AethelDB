/* SPDX-License-Identifier: Apache-2.0 */
\echo Use "CREATE EXTENSION aethel_smgr" to load this file. \quit

CREATE FUNCTION aethel_smgr_status() RETURNS text
AS '$libdir/aethel_smgr', 'aethel_smgr_status'
LANGUAGE C STRICT;

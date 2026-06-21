# End-to-end tests

Two complementary suites validate the full system.

## `test_e2e_lifecycle.py` — runnable, no Docker required

Drives the **real** `aethel-proxy`, `aethel-safekeeper`, and `aethel-pageserver` binaries
through their real wire protocols, with `mock_compute.py` standing in for the
patched PostgreSQL engine. It validates all four required behaviours:

1. a query while compute is scaled to zero cold-starts it via the proxy;
2. an INSERT streams WAL to the safekeeper (quorum-committed);
3. the page server materializes the resulting block;
4. data survives an idle scale-to-zero and is read back after re-activation;
5. the HTTP control-plane API creates timelines, branches, lists, and GCs;
6. every service exposes Prometheus metrics reflecting the work done.

Run it (after `cargo build`):

```bash
cargo build                      # from the repo root, builds the three binaries
cd e2e-tests && python -m pytest test_e2e_lifecycle.py -v
```

The module skips itself if the binaries are not built.

## `test_lifecycle.py` — full PostgreSQL deployment

The same lifecycle, but against a real patched PostgreSQL compute node over
`psycopg`. Requires the built compute image and a running stack, so it is
skipped unless `SP_E2E_REAL_STACK=1`:

```bash
make compute-image && make up
cd e2e-tests && pip install -r requirements.txt
SP_E2E_REAL_STACK=1 python -m pytest test_lifecycle.py -v
```

## Files

- `protocol.py` — Python encoders matching the Rust `common` wire formats.
- `mock_compute.py` — the stand-in compute node.

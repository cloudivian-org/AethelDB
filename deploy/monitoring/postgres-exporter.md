<!--
SPDX-License-Identifier: Apache-2.0
Copyright 2026 The AethelDB Authors
-->

# SQL-level metrics (`postgres_exporter`)

The console's per-database **SQL · Postgres (Performance Insights)** panels —
transactions/sec, cache-hit ratio, active backends, rows/sec — read from
[`postgres_exporter`](https://github.com/prometheus-community/postgres_exporter)
scraped **per compute** and labelled with the AethelDB database name.

AethelDB does not run the exporter for you (it lives next to a database's compute
Postgres, which the operator provides). Wiring is two steps.

## 1. Run one exporter per database's compute

Run `postgres_exporter` as a sidecar to each compute Pod (or one container per
compute), pointed at that Postgres:

```yaml
# in the compute Pod spec, alongside the Postgres container
- name: postgres-exporter
  image: quay.io/prometheuscommunity/postgres-exporter:latest
  env:
    - name: DATA_SOURCE_NAME
      value: "postgresql://postgres@localhost:5432/postgres?sslmode=disable"
  ports:
    - { name: pg-metrics, containerPort: 9187 }
```

`pg_stat_statements` (for a top-queries view, a later step) is enabled with
`shared_preload_libraries = 'pg_stat_statements'` on the compute Postgres.

## 2. Add the `database` label at scrape time

The panels select `pg_stat_database_*{database="<name>"}`, so each exporter's
metrics must carry a `database` label equal to the AethelDB database name. Add it
via a scrape relabel — e.g. from the Pod annotation the activator already sets, or
statically per target:

```yaml
scrape_configs:
  - job_name: aethel-compute-sql
    kubernetes_sd_configs: [{ role: pod }]
    relabel_configs:
      # keep only compute pods exposing the exporter
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: pg-metrics
        action: keep
      # database=<name> from the compute pod's label (set it on the Deployment)
      - source_labels: [__meta_kubernetes_pod_label_aethel_database]
        target_label: database
```

Static (Docker/compose) equivalent:

```yaml
  - job_name: aethel-compute-sql
    static_configs:
      - targets: ["orders-compute:9187"]
        labels: { database: "orders" }
      - targets: ["analytics-compute:9187"]
        labels: { database: "analytics" }
```

Once Prometheus is scraping the exporter with the `database` label, start the
console with `--prometheus-url` and the SQL panels light up per database. Until
then the console shows a friendly "attach a `postgres_exporter`" hint — nothing
breaks.

## Metric selectors used

| Panel | PromQL |
|-------|--------|
| Transactions / sec | `sum(rate(pg_stat_database_xact_commit{database="<n>"}[5m])) + sum(rate(pg_stat_database_xact_rollback{database="<n>"}[5m]))` |
| Cache hit ratio | `100 * H / (H + R)` where `H=sum(rate(pg_stat_database_blks_hit…))`, `R=sum(rate(pg_stat_database_blks_read…))` |
| Active backends | `sum(pg_stat_database_numbackends{database="<n>"})` |
| Rows read / sec | `sum(rate(pg_stat_database_tup_fetched{database="<n>"}[5m]))` |

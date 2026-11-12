# Roadmap

The project grows incrementally: each phase adds one group of tools, and data must
flow end-to-end before the next tool is introduced.

| # | Phase | Tools | Status |
|---|---|---|---|
| 1 | Generator → Bronze → Silver (local) | Python, Faker, DuckDB | ✅ Done |
| 2 | Gold star schema, SCD2 `dim_customer`, data tests | dbt-duckdb | 🚧 In progress |
| 3 | Move storage to MinIO, serve from Postgres | Docker Compose | ⏳ Todo |
| 4 | Orchestration + data quality gate | Airflow, Soda Core | ⏳ Todo |
| 5 | Streaming ingestion | Redpanda (+ optional Spark) | ⏳ Todo |
| 6 | Dashboard, architecture diagram, trade-offs | Superset | ⏳ Todo |

## Design decisions

- **Fact grain = order line** (one product within an order). Putting `product_key` on an
  order-level fact is a grain error, because one order contains many products.
- **Silver keeps two views**: `order_events` (full status history) and `orders`
  (latest state, unique `order_id`). The "order_id is unique" check applies to `orders`, not to events.
- **Money is DECIMAL(12,2)**, never double/float.
- **Surrogate keys are md5 hashes of the natural key**, not integer counters. Gold tables are fully
  rebuilt on every run; a `row_number()` counter would renumber rows whenever a new key sorts in between,
  silently re-pointing facts. A hash is identical on every run and portable to Postgres.
  Exception: `dim_date.date_key` is the conventional integer `YYYYMMDD`.
- **`dim_customer` (SCD Type 2) is rebuilt from order history**, not with `dbt snapshot`. A snapshot only
  sees changes between two runs, dates them by when the job ran, and cannot be rebuilt once lost; Silver
  `order_events` already holds every observation with its exact timestamp. Versions are ordered by the
  order's `created_at` (when the customer snapshot was taken), never by `updated_at`. `dbt snapshot` remains
  the right tool for a source that only exposes its current state.
- **Reference data nobody sends** (channel names, platform types) lives in dbt seeds, versioned in git.
- **PII** is normalized, then hashed as `sha256(salt || value)`; customer names are dropped.
- **Every job is idempotent**: reruns never duplicate data.
- Once Docker is introduced: Airflow gets its own metadata database (not the `warehouse` DB),
  and every image is pinned to an exact version.

## Definition of done for each phase

Runs with the commands in the README, CI is green, and reruns produce identical results.

# Multi-Channel E-Commerce Lakehouse

An end-to-end data lakehouse that ingests orders from multiple sales channels
(Shopify, Shopee, TikTok Shop, Lazada) and models them with the Medallion
architecture (Bronze → Silver → Gold). 100% open-source, runs locally.

The project is built incrementally, one phase at a time. See [docs/roadmap.md](docs/roadmap.md).

**Live dashboard:** <https://nodeholic.github.io/multichannel-lakehouse/>. It is rebuilt from a fresh pipeline
run on every merge into `dev`. All data is synthetic.

## Current state: Phase 2 done, Bronze → Silver → Gold star schema

```
data_generator.py ──► data/bronze/orders/year=/month=/day=/*.jsonl   (raw JSON, append-only)
                              │
bronze_to_silver.py (DuckDB)  ▼
                      data/silver/order_events/    status history, PII hashed
                      data/silver/orders/          one row per order (latest state)
                      data/silver/order_items/     one row per product in an order
                      data/silver/rejected_events/ quarantined bad records
                              │
dbt build                     ▼
                      data/warehouse.duckdb
                        staging.stg_*              thin views over Silver
                        reference.channels         seed: channel names and platform types
                        gold.dim_product           one row per product
                        gold.dim_channel           one row per sales channel
                        gold.dim_date              one row per calendar day
                        gold.dim_customer          one row per customer version (SCD Type 2)
                        gold.fact_order_items      one row per product in an order (the fact)
```

### Gold star schema

```
                    dim_date
                       │
   dim_customer ── fact_order_items ── dim_product
   (SCD Type 2)        │
                    dim_channel
```

`fact_order_items` holds quantities, money and keys. Each sale points to the customer **version** that was
valid when the order was placed, so revenue by membership tier reflects the tier at purchase time.

## Run it

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt        # Linux/macOS: .venv/bin/pip
.venv/Scripts/python src/data_generator.py --start-date 2026-11-01 --days 7 --orders-per-day 200
.venv/Scripts/python src/bronze_to_silver.py
.venv/Scripts/python src/query.py "SELECT channel, sum(net_amount) FROM silver_orders WHERE order_status='COMPLETED' GROUP BY 1"
.venv/Scripts/python src/run_dbt.py build                 # dbt models + data tests
```

`dbt build` runs every model and its data tests in dependency order. `src/run_dbt.py` passes its arguments to
dbt and picks the target from `STORAGE` (plain `dbt build --project-dir dbt --profiles-dir dbt` also works for
local runs). To browse the lineage graph:
`dbt docs generate --project-dir dbt --profiles-dir dbt && dbt docs serve --project-dir dbt --profiles-dir dbt`.

## Local infrastructure (Docker)

Phase 3 moves storage to an S3-compatible object store and serves Gold from Postgres. The stack starts with one command:

```bash
cp .env.example .env        # once; change the passwords
docker compose up -d        # RustFS + Postgres, buckets created automatically
docker compose ps -a        # rustfs and postgres: healthy, create-buckets: exited (0)
```

| Service | Image | Host port (default) | Purpose |
|---|---|---|---|
| `rustfs` | `rustfs/rustfs:1.0.4` | `9000` S3 API, `9001` console | Object store for Bronze and Silver (buckets `lakehouse-bronze`, `lakehouse-silver`) |
| `create-buckets` | `amazon/aws-cli:2.38.6` | – | One-shot job that creates the buckets, safe to rerun |
| `postgres` | `postgres:17.10-alpine` | `5432` | Serving database for the Gold star schema |

- RustFS console: <http://localhost:9001>, log in with `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env`.
- Postgres: `docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'` (uses the values from `.env`).
- Port already taken (for example another Postgres on 5432)? Change `POSTGRES_PORT`, `S3_PORT` or `S3_CONSOLE_PORT` in `.env`.
- Ports listen on `127.0.0.1` only. To reach the stack from another machine, remove the `127.0.0.1:` prefix in
  `docker-compose.yml` and set strong passwords first: the ones in `.env.example` are public.
- `docker compose down` stops the stack and keeps the data; `docker compose down -v` also deletes the volumes.

RustFS replaces MinIO, the object store in the original design: MinIO images are no longer published on Docker Hub.

### Running Bronze and Silver on S3

`STORAGE` switches every job between local disk (default) and the RustFS buckets. Connection settings and
credentials are read from the same `.env` that docker compose uses.

```bash
docker compose up -d
STORAGE=s3 .venv/Scripts/python src/data_generator.py     # -> s3://lakehouse-bronze/orders/year=/month=/day=/
STORAGE=s3 .venv/Scripts/python src/bronze_to_silver.py   # -> s3://lakehouse-silver/<table>/
STORAGE=s3 .venv/Scripts/python src/query.py "SELECT count(*) FROM silver_orders"
```

On PowerShell, set the variable first: `$env:STORAGE = "s3"`. It stays set for the whole session; tests are not
affected, because `tests/conftest.py` pins every test run to local storage.

- Both backends produce the same Silver row counts and money totals for the same seed (checked by the integration tests).
- Reruns stay idempotent on S3 too: each Silver table prefix is deleted before it is rewritten, so partitions that
  disappeared from Bronze are removed as well.
- Delete-then-write is not atomic: if a run fails halfway, that Silver table is incomplete until the next successful
  run. Bronze is untouched, so a rerun always recovers.
- The first S3 run downloads DuckDB's `httpfs` extension (network access needed once; it is cached afterwards).
- `STORAGE=s3 python src/run_dbt.py build` makes dbt read Silver from S3 (target `s3` in `dbt/profiles.yml`,
  credentials exported from `.env`). The warehouse file stays local under `DATA_DIR`.

### Publishing Gold to Postgres

```bash
python src/run_dbt.py build                    # Gold in the DuckDB warehouse
python src/publish_gold_to_postgres.py         # Gold in Postgres, schema `gold`
```

Query it from the host with any SQL client (DBeaver, pgAdmin, psql) on `127.0.0.1`, **the port from
`POSTGRES_PORT` in `.env`** (another Postgres on the machine may already own 5432), database `warehouse`,
credentials from `.env`. For example, revenue per channel:

```bash
set -a; . ./.env; set +a          # export the POSTGRES_* values from .env into this shell
PGPASSWORD="$POSTGRES_PASSWORD" psql -h 127.0.0.1 -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
  SELECT ch.channel_name, count(DISTINCT f.order_id) AS orders, sum(f.net_amount) AS revenue
  FROM gold.fact_order_items f JOIN gold.dim_channel ch USING (channel_key)
  WHERE f.order_status = 'COMPLETED'
  GROUP BY 1 ORDER BY revenue DESC"
```

Without a local psql: `docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'`.

**How the publish stays safe.**

1. The five Gold tables are loaded into a per-run schema `gold__loading_<id>`.
2. They are checked against the warehouse (row counts plus gross, discount and net totals). Nothing is swapped if
   they differ.
3. They get primary keys, `SELECT` for `POSTGRES_READER_ROLE` (optional), and a "managed by
   publish_gold_to_postgres" marker.
4. One Postgres statement swaps them in. Postgres DDL is transactional, so a BI tool reads either the previous Gold
   or the new one, never a half-loaded mix.

The swap replaces only what the tool owns, and otherwise fails loudly without changing anything:

- **Unmarked schema.** It refuses to replace a schema without the marker, so `POSTGRES_SCHEMA` can never wipe an
  application schema. System schemas such as `public` are rejected at startup.
- **No `CASCADE`.** The old Gold tables and schema are dropped with `RESTRICT`, never `CASCADE`. If a BI view
  depends on Gold, the publish stops and the view survives. Recreate such views after the publish, or build them in
  dbt instead.
- **Concurrent publishes.** An advisory lock serializes them, and the swap checks that the loading schema is
  complete.
- **Locks.** `lock_timeout` is 10 s, so a long-running query on Gold makes the publish fail rather than queue every
  reader behind it. Rerun it later.
- **Grants.** Every publish creates new tables, so grants made by hand on Gold are gone after the next publish.
  Grant to `POSTGRES_READER_ROLE` (re-applied on every publish) and make users members of that role.
- **Hard crashes.** Errors and Ctrl-C drop the run's loading schema. A killed process or a machine crash can leave
  a `gold__loading_<id>` schema behind; Gold itself is unaffected. List leftovers with
  `SELECT nspname FROM pg_namespace WHERE nspname LIKE 'gold\_\_loading\_%';` and drop them when no publish is running.

| Variable | Default | Purpose |
|---|---|---|
| `STORAGE` | `local` | `local` or `s3` |
| `S3_ENDPOINT` | `http://localhost:${S3_PORT}` | S3 API endpoint, must start with `http://` or `https://` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | from `.env` | credentials, required when `STORAGE=s3` |
| `BRONZE_URI`, `SILVER_URI` | `data/bronze`, `data/silver` (local); `s3://lakehouse-bronze`, `s3://lakehouse-silver` (s3) | override the layer roots; must match `STORAGE` |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB` | `127.0.0.1`, `5432`, `warehouse` | serving database |
| `POSTGRES_USER`, `POSTGRES_PASSWORD` | from `.env` | credentials, required to publish |
| `POSTGRES_SCHEMA` | `gold` | target schema: `[a-z_][a-z0-9_]{0,39}`, not a system schema |
| `POSTGRES_READER_ROLE` | none | role that gets read access to the published Gold |

Invalid settings (empty URI, `s3://` without a bucket, an endpoint without scheme, missing credentials, an `s3://`
URI while `STORAGE=local`, a system or over-long schema name) stop the job at startup with a message naming the
variable.

## Answering business questions

`src/query.py` opens the warehouse read-only, so Gold tables can be queried directly. Every question has the
same shape: numbers from the fact, grouped by a dimension.

```sql
-- Revenue per channel
SELECT c.channel_name, count(DISTINCT f.order_id) AS orders, sum(f.net_amount) AS revenue
FROM gold.fact_order_items f JOIN gold.dim_channel c USING (channel_key)
WHERE f.order_status = 'COMPLETED'
GROUP BY ALL ORDER BY revenue DESC;

-- Top 5 products
SELECT p.product_name, p.category, sum(f.quantity) AS units, sum(f.net_amount) AS revenue
FROM gold.fact_order_items f JOIN gold.dim_product p USING (product_key)
WHERE f.order_status = 'COMPLETED'
GROUP BY ALL ORDER BY revenue DESC LIMIT 5;

-- Refund rate per channel (refunded / delivered orders)
SELECT c.channel_name,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'REFUNDED') AS refunded,
       count(DISTINCT f.order_id) FILTER (WHERE f.order_status IN ('COMPLETED', 'REFUNDED')) AS delivered,
       round(100.0 * refunded / delivered, 1) AS refund_rate_pct
FROM gold.fact_order_items f JOIN gold.dim_channel c USING (channel_key)
GROUP BY ALL ORDER BY refund_rate_pct DESC;

-- Revenue per membership tier at the time of purchase (uses SCD Type 2)
SELECT cu.membership_tier, sum(f.net_amount) AS revenue
FROM gold.fact_order_items f JOIN gold.dim_customer cu USING (customer_key)
WHERE f.order_status = 'COMPLETED'
GROUP BY ALL ORDER BY revenue DESC;
```

Sample results (seed 42, 7 days × 200 orders):

| Channel | Orders | Revenue (VND) | Refund rate |
|---|---|---|---|
| Shopify | 302 | 703,583,900 | 7.1% |
| TikTok Shop | 269 | 703,306,000 | 9.1% |
| Lazada | 267 | 634,391,100 | 9.5% |
| Shopee | 256 | 616,067,800 | 9.2% |

## Files

| File | Purpose |
|---|---|
| `src/config.py` | Storage backend (`STORAGE`), layer URIs, S3 settings, PII hashing salt (`PII_SALT` env var) |
| `src/storage.py` | File and DuckDB helpers that work the same on local disk and on S3 |
| `src/data_generator.py` | Simulates multi-channel order events, with injected dirty data (duplicates, missing `order_id`, messy emails, malformed timestamps, unknown statuses, broken items) |
| `src/bronze_to_silver.py` | Validates and cleans data with DuckDB SQL, writes Parquet partitioned by `order_date`, idempotent |
| `src/query.py` | Runs ad-hoc SQL against the `bronze_events` and `silver_*` views and the `gold.*` tables |
| `src/run_dbt.py` | Runs dbt with the pipeline's storage settings (target `local` or `s3`) |
| `src/publish_gold_to_postgres.py` | Publishes the Gold star schema to Postgres with an atomic schema swap |
| `dbt/` | dbt project for the Gold layer: Silver sources, staging views, star schema, seeds, data tests |
| `src/export_dashboard_data.py` | Exports Gold and Silver quarantine numbers as JSON for the dashboard |
| `dashboard/` | Observable Framework site published on GitHub Pages |

## Design decisions

- **Bronze stores events, not orders**: every status change is a new row. Never updated, never deleted.
- **Silver has three grains**: `order_events` (history), `orders` (latest state, unique `order_id`), `order_items` (the grain of the future Gold fact table).
- **Money is `DECIMAL(12,2)`**, never float.
- **PII**: normalized first, then hashed as `sha256(salt || value)`; customer names are dropped.
- **Idempotent**: Silver is fully rebuilt on every run, so reruns never duplicate data.

## Validation rules

Nobody can list every way source data breaks, so Silver describes what a **valid** event looks like.
An event that breaks a rule is quarantined in `silver/rejected_events` with the first rule it broke:

| `reject_reason` | A valid event has… |
|---|---|
| `missing_order_id` | a non-empty `order_id` |
| `missing_customer_id` | a `customer.customer_id` |
| `invalid_timestamp` | `created_at` and `updated_at` that parse as timestamps |
| `unknown_status` | a status in PENDING, PROCESSING, COMPLETED, CANCELLED, REFUNDED |
| `unknown_channel` | a channel in SHOPIFY, SHOPEE, TIKTOK_SHOP, LAZADA |
| `empty_items` | at least one item |
| `invalid_item` | items with quantity > 0, price ≥ 0, and 0 ≤ discount ≤ quantity × price |

Bronze is read with an explicit all-text schema and Silver converts with `TRY_CAST`, so a single malformed
row can never crash the job, and inferred types never drift with the data.

## Public dashboard

A static site built with [Observable Framework](https://observablehq.com/framework/) and published on GitHub
Pages by `.github/workflows/dashboard.yml`. Two pages: **Sales overview** (KPIs, revenue per day and channel,
products, categories, membership tiers) and **Data quality** (Bronze → Silver funnel, quarantine reasons).

GitHub Pages serves static files only, so CI runs the pipeline on sample data, `src/export_dashboard_data.py`
writes the numbers as JSON, and Framework turns them into HTML. Nothing on the site is typed by hand.

Build it locally (Node 20+):

```bash
.venv/Scripts/python src/export_dashboard_data.py   # after the pipeline and dbt build
cd dashboard
npm ci
npm run dev      # live preview at http://127.0.0.1:3000
npm run build    # static site in dashboard/dist
```

Set `OBSERVABLE_TELEMETRY_DISABLE=true` to turn off Framework's anonymous usage telemetry (CI does).
Evidence.dev was the first choice for a BI-as-code site, but its open-source npm workflow is deprecated in favour
of the commercial Evidence Studio.

## Tests

```bash
.venv/Scripts/pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest -v                                   # offline, S3 tests skipped
RUN_INTEGRATION_TESTS=1 .venv/Scripts/python -m pytest -v          # also S3 + Postgres; needs docker compose up -d
```

The integration tests never touch real data. S3 tests write under a unique `pytest-<id>` prefix, compare S3
Silver and the Gold built from it with the local run for the same seed, check that reruns replace files instead
of adding them, and delete their prefix afterwards. Postgres tests publish into a `pytest_<id>` schema, compare
every Gold table row by row with the warehouse (`EXCEPT ALL` in both directions), check that republishing is
idempotent and leaves no loading schema, that a dependent BI view blocks the swap and survives, that an unmanaged
schema is never replaced, and that the reader role can read Gold. They drop their schemas and roles afterwards.

The tests run the whole pipeline in a temporary directory (`DATA_DIR`) and assert Silver invariants:
every order appears exactly once, no order disappears without a trace, broken events are quarantined with a known reason,
no raw PII leaks, money is DECIMAL and adds up, and reruns produce identical results. Handcrafted events cover every
validation rule. CI (GitHub Actions) runs the tests on every PR.

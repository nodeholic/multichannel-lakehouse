# Multi-Channel E-Commerce Lakehouse

An end-to-end data lakehouse that ingests orders from multiple sales channels
(Shopify, Shopee, TikTok Shop, Lazada) and models them with the Medallion
architecture (Bronze → Silver → Gold). 100% open-source, runs locally.

The project is built incrementally, one phase at a time. See [docs/roadmap.md](docs/roadmap.md).

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
.venv/Scripts/dbt build --project-dir dbt --profiles-dir dbt      # models + data tests
```

`dbt build` runs every model and its data tests in dependency order. To browse the lineage graph:
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
The pipeline does not use the stack yet; wiring Bronze, Silver and Gold to it follows in the next PRs.

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
| `src/config.py` | Layer paths and the PII hashing salt (`PII_SALT` env var) |
| `src/data_generator.py` | Simulates multi-channel order events, with injected dirty data (duplicates, missing `order_id`, messy emails, malformed timestamps, unknown statuses, broken items) |
| `src/bronze_to_silver.py` | Validates and cleans data with DuckDB SQL, writes Parquet partitioned by `order_date`, idempotent |
| `src/query.py` | Runs ad-hoc SQL against the `bronze_events` and `silver_*` views and the `gold.*` tables |
| `dbt/` | dbt project for the Gold layer: Silver sources, staging views, star schema, seeds, data tests |

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

## Tests

```bash
.venv/Scripts/pip install -r requirements-dev.txt
.venv/Scripts/python -m pytest -v
```

The tests run the whole pipeline in a temporary directory (`DATA_DIR`) and assert Silver invariants:
every order appears exactly once, no order disappears without a trace, broken events are quarantined with a known reason,
no raw PII leaks, money is DECIMAL and adds up, and reruns produce identical results. Handcrafted events cover every
validation rule. CI (GitHub Actions) runs the tests on every PR.

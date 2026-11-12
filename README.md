# Multi-Channel E-Commerce Lakehouse

An end-to-end data lakehouse that ingests orders from multiple sales channels
(Shopify, Shopee, TikTok Shop, Lazada) and models them with the Medallion
architecture (Bronze → Silver → Gold). 100% open-source, runs locally.

The project is built incrementally, one phase at a time. See [docs/roadmap.md](docs/roadmap.md).

## Current state: Phase 1, Generator → Bronze → Silver (local, DuckDB)

```
data_generator.py ──► data/bronze/orders/year=/month=/day=/*.jsonl   (raw JSON, append-only)
                              │
bronze_to_silver.py (DuckDB)  ▼
                      data/silver/order_events/    status history, PII hashed
                      data/silver/orders/          one row per order (latest state)
                      data/silver/order_items/     one row per product in an order
                      data/silver/rejected_events/ quarantined bad records
```

## Run it

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt        # Linux/macOS: .venv/bin/pip
.venv/Scripts/python src/data_generator.py --start-date 2026-11-01 --days 7 --orders-per-day 200
.venv/Scripts/python src/bronze_to_silver.py
.venv/Scripts/python src/query.py "SELECT channel, sum(net_amount) FROM silver_orders WHERE order_status='COMPLETED' GROUP BY 1"
```

## Files

| File | Purpose |
|---|---|
| `src/config.py` | Layer paths and the PII hashing salt (`PII_SALT` env var) |
| `src/data_generator.py` | Simulates multi-channel order events, with injected dirty data (duplicates, missing `order_id`, messy emails, malformed timestamps, unknown statuses, broken items) |
| `src/bronze_to_silver.py` | Validates and cleans data with DuckDB SQL, writes Parquet partitioned by `order_date`, idempotent |
| `src/query.py` | Runs ad-hoc SQL against the `bronze_events` and `silver_*` views |

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

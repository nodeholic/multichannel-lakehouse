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
| `src/data_generator.py` | Simulates multi-channel order events, with injected dirty data (duplicates, missing `order_id`, messy emails) |
| `src/bronze_to_silver.py` | Cleans data with DuckDB SQL, writes Parquet partitioned by `order_date`, idempotent |
| `src/query.py` | Runs ad-hoc SQL against the `bronze_events` and `silver_*` views |

## Design decisions

- **Bronze stores events, not orders**: every status change is a new row. Never updated, never deleted.
- **Silver has three grains**: `order_events` (history), `orders` (latest state, unique `order_id`), `order_items` (the grain of the future Gold fact table).
- **Money is `DECIMAL(12,2)`**, never float.
- **PII**: normalized first, then hashed as `sha256(salt || value)`; customer names are dropped.
- **Idempotent**: Silver is fully rebuilt on every run, so reruns never duplicate data.

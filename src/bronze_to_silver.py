"""Turn raw Bronze data (JSON) into clean Silver data (Parquet) with DuckDB.

Steps (one SQL statement each, read top to bottom):
  1. raw_events      : read every JSON line in Bronze
  2. rejected_events : quarantine events missing order_id
  3. order_events    : cast types, hash PII, drop duplicates on (order_id, updated_at)
  4. latest_orders   : the LATEST state of each order -> one row per order
  5. order_items     : explode the items array -> one row per product in an order

Outputs (fully rebuilt on every run -> rerunning gives identical results = idempotent):
  data/silver/order_events/     status history (used for customer SCD2 later)
  data/silver/orders/           partitioned by order_date
  data/silver/order_items/      partitioned by order_date
  data/silver/rejected_events/

Run:  python src/bronze_to_silver.py
"""
import shutil

import duckdb

from config import BRONZE_ORDERS_DIR, PII_SALT, SILVER_DIR

BRONZE_GLOB = (BRONZE_ORDERS_DIR / "**" / "*.jsonl").as_posix()

# --- Step 1: read Bronze ---------------------------------------------------------
# hive_partitioning=true: DuckDB turns year=2026/month=11/day=01 in the path into columns.
# filename=true: keep the source file name for traceability (the simplest form of lineage).
SQL_RAW = f"""
CREATE OR REPLACE TEMP TABLE raw_events AS
SELECT *
FROM read_json('{BRONZE_GLOB}',
               format = 'newline_delimited',
               hive_partitioning = true,
               filename = true)
"""

# --- Step 2: quarantine broken records --------------------------------------------
# Never drop silently: keep them aside so we can investigate why the source sent bad data.
SQL_REJECTED = """
CREATE OR REPLACE TEMP TABLE rejected_events AS
SELECT 'missing_order_id' AS reject_reason, *
FROM raw_events
WHERE order_id IS NULL
"""

# --- Step 3: clean each event ------------------------------------------------------
# - Cast strings -> TIMESTAMP.
# - Normalize email (trim + lower) BEFORE hashing, otherwise " A@x.com " and "a@x.com"
#   produce two different hashes -> one customer is counted twice.
# - Hash = sha256(salt || value). Customer names are dropped: analytics never needs them.
# - QUALIFY row_number() ... = 1 keeps exactly one row per duplicate group (order_id, updated_at).
#   A retried webhook produces two events with the same content but different event_ids.
SQL_ORDER_EVENTS = """
CREATE OR REPLACE TEMP TABLE order_events AS
SELECT
    event_id,
    order_id,
    upper(channel)                                         AS channel,
    upper(order_status)                                    AS order_status,
    CAST(created_at AS TIMESTAMP)                          AS created_at,
    CAST(updated_at AS TIMESTAMP)                          AS updated_at,
    CAST(created_at AS DATE)                               AS order_date,
    customer.customer_id                                   AS customer_id,
    sha256(getvariable('pii_salt') || lower(trim(customer.email)))                      AS hashed_email,
    sha256(getvariable('pii_salt') || regexp_replace(customer.phone, '[^0-9]', '', 'g')) AS hashed_phone,
    customer.city                                          AS city,
    customer.membership_tier                               AS membership_tier,
    items,
    filename                                               AS _source_file
FROM raw_events
WHERE order_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY order_id, updated_at ORDER BY event_id) = 1
"""

# --- Step 4: latest snapshot of each order -------------------------------------------
SQL_LATEST = """
CREATE OR REPLACE TEMP TABLE latest_orders AS
SELECT *
FROM order_events
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY updated_at DESC) = 1
"""

# --- Step 5: explode items -> grain "one product within an order" ----------------------
# Money is DECIMAL(12,2), NEVER float/double (in floating point 0.1 + 0.2 = 0.30000000000000004).
SQL_ORDER_ITEMS = """
CREATE OR REPLACE TEMP TABLE order_items AS
WITH exploded AS (
    SELECT order_id, order_date, channel, customer_id, order_status, updated_at,
           unnest(items) AS item
    FROM latest_orders
)
SELECT
    order_id,
    item.product_id                                        AS product_id,
    item.product_name                                      AS product_name,
    item.category                                          AS category,
    CAST(item.quantity AS INTEGER)                         AS quantity,
    CAST(item.unit_price AS DECIMAL(12, 2))                AS unit_price,
    CAST(item.quantity * item.unit_price AS DECIMAL(12, 2)) AS gross_amount,
    CAST(item.discount_amount AS DECIMAL(12, 2))           AS discount_amount,
    CAST(item.quantity * item.unit_price - item.discount_amount AS DECIMAL(12, 2)) AS net_amount,
    order_date, channel, customer_id, order_status, updated_at
FROM exploded
"""

# One row per order, with totals summed from order_items.
SQL_ORDERS = """
CREATE OR REPLACE TEMP TABLE orders AS
SELECT
    o.order_id, o.channel, o.order_status, o.created_at, o.updated_at, o.order_date,
    o.customer_id, o.hashed_email, o.hashed_phone, o.city, o.membership_tier,
    count(*)               AS item_count,
    sum(i.gross_amount)    AS gross_amount,
    sum(i.discount_amount) AS discount_amount,
    sum(i.net_amount)      AS net_amount
FROM latest_orders o
JOIN order_items i USING (order_id)
GROUP BY ALL
"""


def write_parquet(con: duckdb.DuckDBPyConnection, table: str, partition_by: str | None) -> None:
    """Delete the old folder, then rewrite it -> idempotent (reruns never duplicate data)."""
    target = SILVER_DIR / table
    shutil.rmtree(target, ignore_errors=True)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    if partition_by:
        con.execute(
            f"COPY {table} TO '{target.as_posix()}' (FORMAT parquet, PARTITION_BY ({partition_by}))"
        )
    else:
        target.mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY {table} TO '{(target / 'data.parquet').as_posix()}' (FORMAT parquet)")


def count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def main() -> None:
    con = duckdb.connect()  # in-memory DB: DuckDB is just the SQL engine, the data lives in files
    con.execute("SET VARIABLE pii_salt = ?", [PII_SALT])

    for sql in (SQL_RAW, SQL_REJECTED, SQL_ORDER_EVENTS, SQL_LATEST, SQL_ORDER_ITEMS, SQL_ORDERS):
        con.execute(sql)

    write_parquet(con, "order_events", partition_by="order_date")
    write_parquet(con, "orders", partition_by="order_date")
    write_parquet(con, "order_items", partition_by="order_date")
    write_parquet(con, "rejected_events", partition_by=None)

    raw, rejected, events = count(con, "raw_events"), count(con, "rejected_events"), count(con, "order_events")
    print("Bronze -> Silver complete")
    print(f"  raw_events      : {raw:>6}")
    print(f"  rejected        : {rejected:>6}  (missing order_id)")
    print(f"  duplicates      : {raw - rejected - events:>6}  (same order_id + updated_at)")
    print(f"  order_events    : {events:>6}")
    print(f"  orders          : {count(con, 'orders'):>6}  (one row per order, latest state)")
    print(f"  order_items     : {count(con, 'order_items'):>6}  (one row per product in an order)")
    print(f"Written to: {SILVER_DIR}")


if __name__ == "__main__":
    main()

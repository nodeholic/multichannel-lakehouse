"""Turn raw Bronze data (JSON) into clean Silver data (Parquet) with DuckDB.

Steps (one SQL statement each, read top to bottom):
  1. raw_events       : read every JSON line in Bronze with an explicit all-text schema
  2. validated_events : check every event against the rules of a VALID event;
                        the first rule an event breaks becomes its reject_reason
  3. order_events     : valid events only -> typed, PII hashed, duplicates dropped
  4. latest_orders    : the LATEST state of each order -> one row per order
  5. order_items      : explode the items array -> one row per product in an order

Outputs (fully rebuilt on every run -> rerunning gives identical results = idempotent):
  data/silver/order_events/     status history (used for customer SCD2 later)
  data/silver/orders/           partitioned by order_date
  data/silver/order_items/      partitioned by order_date
  data/silver/rejected_events/  every quarantined event with its reject_reason

Run:  python src/bronze_to_silver.py
"""
import shutil

import duckdb

from config import BRONZE_ORDERS_DIR, PII_SALT, SILVER_DIR

BRONZE_GLOB = (BRONZE_ORDERS_DIR / "**" / "*.jsonl").as_posix()

VALID_CHANNELS = ("SHOPIFY", "SHOPEE", "TIKTOK_SHOP", "LAZADA")
VALID_STATUSES = ("PENDING", "PROCESSING", "COMPLETED", "CANCELLED", "REFUNDED")

# Every rule a valid event must satisfy, in the order they are checked.
REJECT_REASONS = (
    "missing_order_id",
    "missing_customer_id",
    "invalid_timestamp",
    "unknown_status",
    "unknown_channel",
    "empty_items",
    "invalid_item",
)

# Bronze is read with an explicit schema where every scalar is TEXT. If DuckDB inferred types,
# the inferred schema would depend on the data itself: one malformed timestamp turns the whole
# column into VARCHAR, a clean day turns it back into TIMESTAMP. Silver decides types on purpose.
BRONZE_SCHEMA = {
    "event_id": "VARCHAR",
    "order_id": "VARCHAR",
    "channel": "VARCHAR",
    "order_status": "VARCHAR",
    "created_at": "VARCHAR",
    "updated_at": "VARCHAR",
    "customer": "STRUCT(customer_id VARCHAR, name VARCHAR, email VARCHAR, phone VARCHAR, "
    "city VARCHAR, membership_tier VARCHAR)",
    "items": "STRUCT(product_id VARCHAR, product_name VARCHAR, category VARCHAR, quantity VARCHAR, "
    "unit_price VARCHAR, discount_amount VARCHAR)[]",
    "currency": "VARCHAR",
}


def sql_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def sql_struct(schema: dict[str, str]) -> str:
    return "{" + ", ".join(f"'{k}': '{v}'" for k, v in schema.items()) + "}"


# --- Step 1: read Bronze ---------------------------------------------------------
# hive_partitioning=true: DuckDB turns year=2026/month=11/day=01 in the path into columns.
# filename=true: keep the source file name for traceability (the simplest form of lineage).
SQL_RAW = f"""
CREATE OR REPLACE TEMP TABLE raw_events AS
SELECT *
FROM read_json('{BRONZE_GLOB}',
               format = 'newline_delimited',
               columns = {sql_struct(BRONZE_SCHEMA)},
               hive_partitioning = true,
               filename = true)
"""

# --- Step 2: validate against the rules of a VALID event ---------------------------
# We describe what good data looks like instead of listing every way data can be bad.
# Anything that does not look good is quarantined, including kinds of breakage nobody
# has seen yet. TRY_CAST returns NULL instead of raising, so one malformed row can never
# crash the whole job.
SQL_VALIDATED = f"""
CREATE OR REPLACE TEMP TABLE validated_events AS
SELECT
    *,
    TRY_CAST(created_at AS TIMESTAMP) AS created_at_ts,
    TRY_CAST(updated_at AS TIMESTAMP) AS updated_at_ts,
    CASE
        WHEN order_id IS NULL OR trim(order_id) = ''                    THEN 'missing_order_id'
        WHEN customer.customer_id IS NULL                               THEN 'missing_customer_id'
        WHEN TRY_CAST(created_at AS TIMESTAMP) IS NULL
          OR TRY_CAST(updated_at AS TIMESTAMP) IS NULL                  THEN 'invalid_timestamp'
        WHEN order_status IS NULL
          OR upper(order_status) NOT IN ({sql_list(VALID_STATUSES)})    THEN 'unknown_status'
        WHEN channel IS NULL
          OR upper(channel) NOT IN ({sql_list(VALID_CHANNELS)})         THEN 'unknown_channel'
        WHEN items IS NULL OR len(items) = 0                            THEN 'empty_items'
        WHEN len(list_filter(items, lambda i:
                 coalesce(TRY_CAST(i.quantity AS INTEGER), 0) <= 0
              OR coalesce(TRY_CAST(i.unit_price AS DECIMAL(12, 2)), -1) < 0
              OR coalesce(TRY_CAST(i.discount_amount AS DECIMAL(12, 2)), -1) < 0
              OR TRY_CAST(i.discount_amount AS DECIMAL(12, 2))
                   > TRY_CAST(i.quantity AS INTEGER) * TRY_CAST(i.unit_price AS DECIMAL(12, 2))
             )) > 0                                                     THEN 'invalid_item'
    END AS reject_reason
FROM raw_events
"""

SQL_REJECTED = """
CREATE OR REPLACE TEMP TABLE rejected_events AS
SELECT reject_reason, * EXCLUDE (reject_reason, created_at_ts, updated_at_ts)
FROM validated_events
WHERE reject_reason IS NOT NULL
"""

# --- Step 3: clean each valid event ------------------------------------------------
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
    created_at_ts                                          AS created_at,
    updated_at_ts                                          AS updated_at,
    CAST(created_at_ts AS DATE)                            AS order_date,
    customer.customer_id                                   AS customer_id,
    sha256(getvariable('pii_salt') || lower(trim(customer.email)))                      AS hashed_email,
    sha256(getvariable('pii_salt') || regexp_replace(customer.phone, '[^0-9]', '', 'g')) AS hashed_phone,
    customer.city                                          AS city,
    customer.membership_tier                               AS membership_tier,
    items,
    filename                                               AS _source_file
FROM validated_events
WHERE reject_reason IS NULL
QUALIFY row_number() OVER (PARTITION BY order_id, updated_at_ts ORDER BY event_id) = 1
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
# Plain CAST is safe here: the invalid_item rule already proved every value converts.
SQL_ORDER_ITEMS = """
CREATE OR REPLACE TEMP TABLE order_items AS
WITH exploded AS (
    SELECT order_id, order_date, channel, customer_id, order_status, updated_at,
           unnest(items) AS item
    FROM latest_orders
),
typed AS (
    SELECT *,
           CAST(item.quantity AS INTEGER)           AS qty,
           CAST(item.unit_price AS DECIMAL(12, 2))  AS price,
           CAST(item.discount_amount AS DECIMAL(12, 2)) AS discount
    FROM exploded
)
SELECT
    order_id,
    item.product_id                                AS product_id,
    item.product_name                              AS product_name,
    item.category                                  AS category,
    qty                                            AS quantity,
    price                                          AS unit_price,
    CAST(qty * price AS DECIMAL(12, 2))            AS gross_amount,
    discount                                       AS discount_amount,
    CAST(qty * price - discount AS DECIMAL(12, 2)) AS net_amount,
    order_date, channel, customer_id, order_status, updated_at
FROM typed
"""

# One row per order, with totals summed from order_items. The inner JOIN cannot drop an
# order anymore: the empty_items rule guarantees every surviving order has at least one line.
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

    for sql in (SQL_RAW, SQL_VALIDATED, SQL_REJECTED, SQL_ORDER_EVENTS, SQL_LATEST, SQL_ORDER_ITEMS, SQL_ORDERS):
        con.execute(sql)

    write_parquet(con, "order_events", partition_by="order_date")
    write_parquet(con, "orders", partition_by="order_date")
    write_parquet(con, "order_items", partition_by="order_date")
    write_parquet(con, "rejected_events", partition_by=None)

    raw, rejected, events = count(con, "raw_events"), count(con, "rejected_events"), count(con, "order_events")
    print("Bronze -> Silver complete")
    print(f"  raw_events      : {raw:>6}")
    print(f"  rejected        : {rejected:>6}")
    by_reason = dict(con.execute("SELECT reject_reason, count(*) FROM rejected_events GROUP BY 1").fetchall())
    for reason in REJECT_REASONS:
        print(f"    {reason:<20}: {by_reason.get(reason, 0):>4}")
    print(f"  duplicates      : {raw - rejected - events:>6}  (same order_id + updated_at)")
    print(f"  order_events    : {events:>6}")
    print(f"  orders          : {count(con, 'orders'):>6}  (one row per order, latest state)")
    print(f"  order_items     : {count(con, 'order_items'):>6}  (one row per product in an order)")
    print(f"Written to: {SILVER_DIR}")


if __name__ == "__main__":
    main()

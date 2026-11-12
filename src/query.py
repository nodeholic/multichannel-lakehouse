"""Run a quick SQL query against Bronze, Silver and Gold data to explore it.

Available views: bronze_events, silver_orders, silver_order_items,
                 silver_order_events, silver_rejected
Gold tables (after `dbt build`): gold.fact_order_items, gold.dim_customer,
                 gold.dim_product, gold.dim_channel, gold.dim_date

Examples:
  python src/query.py "SELECT channel, count(*), sum(net_amount) FROM silver_orders GROUP BY 1"
  python src/query.py "SELECT * FROM gold.dim_customer WHERE customer_id = 'C00002'"
"""
import sys

import duckdb

from config import BRONZE_ORDERS_DIR, DATA_DIR, SILVER_DIR

WAREHOUSE = DATA_DIR / "warehouse.duckdb"

VIEWS = {
    "bronze_events": f"read_json('{(BRONZE_ORDERS_DIR / '**' / '*.jsonl').as_posix()}', "
    "format='newline_delimited', hive_partitioning=true)",
    "silver_orders": f"read_parquet('{(SILVER_DIR / 'orders' / '**' / '*.parquet').as_posix()}', hive_partitioning=true)",
    "silver_order_items": f"read_parquet('{(SILVER_DIR / 'order_items' / '**' / '*.parquet').as_posix()}', hive_partitioning=true)",
    "silver_order_events": f"read_parquet('{(SILVER_DIR / 'order_events' / '**' / '*.parquet').as_posix()}', hive_partitioning=true)",
    "silver_rejected": f"read_parquet('{(SILVER_DIR / 'rejected_events' / '*.parquet').as_posix()}')",
}


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    # Open the dbt warehouse read-only when it exists, so Gold tables are queryable
    # without ever risking a write. Silver views are temporary and live in memory.
    con = duckdb.connect(str(WAREHOUSE), read_only=True) if WAREHOUSE.exists() else duckdb.connect()
    for name, source in VIEWS.items():
        try:
            con.execute(f"CREATE TEMP VIEW {name} AS SELECT * FROM {source}")
        except duckdb.Error:
            pass  # that layer has no data yet -> skip this view
    con.sql(sys.argv[1]).show(max_rows=50)


if __name__ == "__main__":
    main()

"""Run a quick SQL query against Bronze/Silver data to explore it.

Available views: bronze_events, silver_orders, silver_order_items,
                 silver_order_events, silver_rejected

Examples:
  python src/query.py "SELECT channel, count(*), sum(net_amount) FROM silver_orders GROUP BY 1"
  python src/query.py "SELECT * FROM silver_order_events WHERE order_id = 'ORD-0000001'"
"""
import sys

import duckdb

from config import BRONZE_ORDERS_DIR, SILVER_DIR

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
    con = duckdb.connect()
    for name, source in VIEWS.items():
        try:
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM {source}")
        except duckdb.Error:
            pass  # that layer has no data yet -> skip this view
    con.sql(sys.argv[1]).show(max_rows=50)


if __name__ == "__main__":
    main()

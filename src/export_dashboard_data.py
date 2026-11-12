"""Export the numbers behind the public dashboard as small JSON files.

Reads the Gold star schema (dbt warehouse) and Silver quarantine stats, and writes one JSON
file per dataset into dashboard/src/data/. Observable Framework then turns them into a static
site. Every number on the site comes from here, nothing is typed by hand.

Run after the pipeline and `dbt build`:
  python src/export_dashboard_data.py
"""
import json
import os
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from config import BRONZE_ORDERS_URI, DATA_DIR, PROJECT_ROOT, SILVER_URI
from storage import connect_duckdb

OUT_DIR = PROJECT_ROOT / "dashboard" / "src" / "data"
WAREHOUSE = DATA_DIR / "warehouse.duckdb"

# Revenue counts COMPLETED orders only: refunded and cancelled orders earned nothing.
DATASETS = {
    "kpis": """
        SELECT
            sum(f.net_amount) FILTER (WHERE f.order_status = 'COMPLETED')                    AS revenue,
            count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'COMPLETED')           AS completed_orders,
            count(DISTINCT f.order_id)                                                        AS all_orders,
            count(DISTINCT c.customer_id)                                                     AS customers,
            count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'REFUNDED')
              / count(DISTINCT f.order_id) FILTER (WHERE f.order_status IN ('COMPLETED', 'REFUNDED'))
                                                                                              AS refund_rate,
            min(d.full_date)                                                                  AS first_day,
            max(d.full_date)                                                                  AS last_day
        FROM gold.fact_order_items f
        JOIN gold.dim_customer c USING (customer_key)
        JOIN gold.dim_date d USING (date_key)
    """,
    "revenue_by_day": """
        SELECT d.full_date AS day, ch.channel_name AS channel, sum(f.net_amount) AS revenue
        FROM gold.fact_order_items f
        JOIN gold.dim_date d USING (date_key)
        JOIN gold.dim_channel ch USING (channel_key)
        WHERE f.order_status = 'COMPLETED'
        GROUP BY ALL ORDER BY day, channel
    """,
    "channels": """
        SELECT
            ch.channel_name AS channel,
            ch.platform_type,
            count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'COMPLETED')  AS orders,
            sum(f.net_amount) FILTER (WHERE f.order_status = 'COMPLETED')           AS revenue,
            count(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'REFUNDED')
              / count(DISTINCT f.order_id) FILTER (WHERE f.order_status IN ('COMPLETED', 'REFUNDED'))
                                                                                     AS refund_rate
        FROM gold.fact_order_items f
        JOIN gold.dim_channel ch USING (channel_key)
        GROUP BY ALL ORDER BY revenue DESC
    """,
    "products": """
        SELECT p.product_name AS product, p.category, sum(f.quantity) AS units, sum(f.net_amount) AS revenue
        FROM gold.fact_order_items f
        JOIN gold.dim_product p USING (product_key)
        WHERE f.order_status = 'COMPLETED'
        GROUP BY ALL ORDER BY revenue DESC
    """,
    "tiers": """
        SELECT c.membership_tier AS tier, count(DISTINCT f.order_id) AS orders, sum(f.net_amount) AS revenue
        FROM gold.fact_order_items f
        JOIN gold.dim_customer c USING (customer_key)
        WHERE f.order_status = 'COMPLETED'
        GROUP BY ALL
        ORDER BY min(list_position(['Standard', 'Silver', 'Gold', 'Platinum'], c.membership_tier))
    """,
}


def to_json(value):
    """JSON encoder for DuckDB values. Money becomes a float for display only."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def rows(con, sql: str) -> list[dict]:
    cursor = con.execute(sql)
    names = [col[0] for col in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def pipeline_stats(con) -> dict:
    """Bronze -> Silver funnel and quarantine reasons: the data-quality story of the run."""
    raw = con.execute(f"SELECT count(*) FROM read_json_objects('{BRONZE_ORDERS_URI}/**/*.jsonl')").fetchone()[0]
    count = lambda table, pattern="**/*.parquet": con.execute(  # noqa: E731
        f"SELECT count(*) FROM read_parquet('{SILVER_URI}/{table}/{pattern}')"
    ).fetchone()[0]
    rejected = count("rejected_events", "*.parquet")
    events = count("order_events")
    return {
        "raw_events": raw,
        "rejected_events": rejected,
        "duplicate_events": raw - rejected - events,
        "clean_events": events,
        "orders": count("orders"),
        "order_items": count("order_items"),
        "reject_reasons": rows(con, f"""
            SELECT reject_reason, count(*) AS events
            FROM read_parquet('{SILVER_URI}/rejected_events/*.parquet')
            GROUP BY 1 ORDER BY events DESC
        """),
    }


def main() -> None:
    if not WAREHOUSE.exists():
        raise SystemExit(f"{WAREHOUSE} not found: run the pipeline and `dbt build` first")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = connect_duckdb(str(WAREHOUSE), read_only=True)
    outputs = {name: rows(con, sql) for name, sql in DATASETS.items()}
    outputs["kpis"] = outputs["kpis"][0]
    outputs["pipeline"] = pipeline_stats(con)
    outputs["build"] = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "commit": os.environ.get("GITHUB_SHA", "local")[:7],
    }

    for name, data in outputs.items():
        (OUT_DIR / f"{name}.json").write_text(json.dumps(data, default=to_json, indent=1), encoding="utf-8")
    print(f"Wrote {len(outputs)} datasets to {OUT_DIR}")


if __name__ == "__main__":
    main()

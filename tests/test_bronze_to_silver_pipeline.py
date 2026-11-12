"""End-to-end test of Generator -> Bronze -> Silver in a temporary directory.

Each test asserts one INVARIANT that the Silver layer must always satisfy,
no matter how dirty the source data is. This is the precursor of the data quality gate (Soda).
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
DAYS, ORDERS_PER_DAY = 3, 50


def run_script(script: str, data_dir: Path, *args: str) -> None:
    env = {**os.environ, "DATA_DIR": str(data_dir), "PYTHONIOENCODING": "utf-8"}
    subprocess.run([sys.executable, str(SRC / script), *args], env=env, check=True, capture_output=True)


def silver(data_dir: Path, table: str) -> str:
    """DuckDB expression that reads one Silver table."""
    pattern = "*.parquet" if table == "rejected_events" else "**/*.parquet"
    return f"read_parquet('{(data_dir / 'silver' / table / pattern).as_posix()}', hive_partitioning=true)"


def scalar(sql: str):
    return duckdb.execute(sql).fetchone()[0]


@pytest.fixture(scope="module")
def data_dir(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("lakehouse")
    run_script("data_generator.py", path, "--days", str(DAYS), "--orders-per-day", str(ORDERS_PER_DAY))
    run_script("bronze_to_silver.py", path)
    return path


def test_every_generated_order_reaches_silver_exactly_once(data_dir):
    orders = silver(data_dir, "orders")
    assert scalar(f"SELECT count(*) FROM {orders}") == DAYS * ORDERS_PER_DAY
    assert scalar(f"SELECT count(*) - count(DISTINCT order_id) FROM {orders}") == 0


def test_events_missing_order_id_are_quarantined_not_dropped(data_dir):
    assert scalar(f"SELECT count(*) FROM {silver(data_dir, 'rejected_events')}") > 0
    assert scalar(f"SELECT count(*) FROM {silver(data_dir, 'order_events')} WHERE order_id IS NULL") == 0


def test_duplicate_events_are_removed(data_dir):
    events = silver(data_dir, "order_events")
    assert scalar(
        f"SELECT count(*) FROM (SELECT order_id, updated_at FROM {events} GROUP BY ALL HAVING count(*) > 1)"
    ) == 0


def test_pii_is_hashed_and_normalized(data_dir):
    events = silver(data_dir, "order_events")
    columns = [row[0] for row in duckdb.execute(f"DESCRIBE SELECT * FROM {events}").fetchall()]
    assert not {"name", "email", "phone", "customer"} & set(columns), "raw PII leaked into Silver"
    # Dirty emails (upper case / whitespace) must hash identically -> exactly one hashed_email per customer
    assert scalar(
        f"SELECT max(n) FROM (SELECT count(DISTINCT hashed_email) n FROM {events} GROUP BY customer_id)"
    ) == 1
    assert scalar(f"SELECT min(length(hashed_email)) FROM {events}") == 64


def test_money_columns_are_decimal_and_consistent(data_dir):
    items = silver(data_dir, "order_items")
    types = dict(duckdb.execute(f"SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM {items})").fetchall())
    assert types["net_amount"] == "DECIMAL(12,2)"
    assert scalar(f"SELECT count(*) FROM {items} WHERE net_amount < 0") == 0
    assert scalar(f"SELECT count(*) FROM {items} WHERE net_amount <> gross_amount - discount_amount") == 0


def test_order_totals_match_sum_of_items(data_dir):
    assert scalar(
        f"""SELECT count(*) FROM {silver(data_dir, 'orders')} o
            JOIN (SELECT order_id, sum(net_amount) s FROM {silver(data_dir, 'order_items')} GROUP BY 1) i
            USING (order_id) WHERE o.net_amount <> i.s"""
    ) == 0


def test_rerun_is_idempotent(data_dir):
    def snapshot():
        return duckdb.execute(
            f"SELECT count(*), sum(net_amount) FROM {silver(data_dir, 'orders')}"
        ).fetchone()

    before = snapshot()
    run_script("bronze_to_silver.py", data_dir)
    assert snapshot() == before

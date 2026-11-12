"""End-to-end test of Generator -> Bronze -> Silver in a temporary directory.

Each test asserts one INVARIANT that the Silver layer must always satisfy,
no matter how dirty the source data is. This is the precursor of the data quality gate (Soda).
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))
from bronze_to_silver import REJECT_REASONS  # noqa: E402

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


def bronze(data_dir: Path) -> str:
    """DuckDB expression that reads every Bronze event as raw JSON objects."""
    return f"read_json_objects('{(data_dir / 'bronze' / 'orders' / '**' / '*.jsonl').as_posix()}')"


def test_orders_are_unique(data_dir):
    orders = silver(data_dir, "orders")
    assert scalar(f"SELECT count(*) FROM {orders}") > 0
    assert scalar(f"SELECT count(*) - count(DISTINCT order_id) FROM {orders}") == 0


def test_no_order_disappears_silently(data_dir):
    # Every order_id seen in Bronze must end up in Silver orders, or in quarantine.
    assert scalar(
        f"""SELECT count(*) FROM (
                SELECT DISTINCT json ->> 'order_id' AS order_id FROM {bronze(data_dir)}
                WHERE json ->> 'order_id' IS NOT NULL
            ) b
            WHERE order_id NOT IN (SELECT order_id FROM {silver(data_dir, 'orders')})
              AND order_id NOT IN (SELECT order_id FROM {silver(data_dir, 'rejected_events')}
                                   WHERE order_id IS NOT NULL)"""
    ) == 0


def test_every_rejected_event_has_a_known_reason(data_dir):
    rejected = silver(data_dir, "rejected_events")
    assert scalar(f"SELECT count(*) FROM {rejected}") > 0
    reasons = {r[0] for r in duckdb.execute(f"SELECT DISTINCT reject_reason FROM {rejected}").fetchall()}
    assert reasons <= set(REJECT_REASONS)
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


# --- One handcrafted event per validation rule -------------------------------------

def make_event(event_id: str, **overrides) -> dict:
    """A valid event; overrides break exactly one rule."""
    event = {
        "event_id": event_id,
        "order_id": f"ORD-{event_id}",
        "channel": "SHOPEE",
        "order_status": "COMPLETED",
        "created_at": "2026-11-01T10:00:00",
        "updated_at": "2026-11-01T12:00:00",
        "customer": {"customer_id": "C00001", "name": "Test User", "email": "test@example.com",
                     "phone": "0900000000", "city": "Hanoi", "membership_tier": "Standard"},
        "items": [{"product_id": "P001", "product_name": "Bluetooth Headphones", "category": "Electronics",
                   "quantity": 1, "unit_price": 590000, "discount_amount": 0}],
        "currency": "VND",
    }
    event.update(overrides)
    return event


ITEM = make_event("x")["items"][0]
CASES = {
    "no-order": ({"order_id": None}, "missing_order_id"),
    "no-customer": ({"customer": dict(make_event("x")["customer"], customer_id=None)}, "missing_customer_id"),
    "bad-ts": ({"created_at": "2026-13-45T99:99:99"}, "invalid_timestamp"),
    "bad-status": ({"order_status": "SHIPPED"}, "unknown_status"),
    "bad-channel": ({"channel": "EBAY"}, "unknown_channel"),
    "no-items": ({"items": []}, "empty_items"),
    "neg-qty": ({"items": [dict(ITEM, quantity=-1)]}, "invalid_item"),
    "text-qty": ({"items": [dict(ITEM, quantity="two")]}, "invalid_item"),
    "big-discount": ({"items": [dict(ITEM, discount_amount=999999999)]}, "invalid_item"),
}


@pytest.fixture(scope="module")
def handcrafted_dir(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("handcrafted")
    folder = path / "bronze" / "orders" / "year=2026" / "month=11" / "day=01"
    folder.mkdir(parents=True)
    events = [make_event("ok")] + [make_event(eid, **overrides) for eid, (overrides, _) in CASES.items()]
    (folder / "events-test.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    run_script("bronze_to_silver.py", path)  # must not crash on the malformed timestamp
    return path


def test_each_rule_quarantines_its_case(handcrafted_dir):
    rejected = dict(duckdb.execute(
        f"SELECT event_id, reject_reason FROM {silver(handcrafted_dir, 'rejected_events')}"
    ).fetchall())
    assert rejected == {eid: reason for eid, (_, reason) in CASES.items()}


def test_valid_event_survives_next_to_broken_ones(handcrafted_dir):
    orders = [r[0] for r in duckdb.execute(f"SELECT order_id FROM {silver(handcrafted_dir, 'orders')}").fetchall()]
    assert orders == ["ORD-ok"]

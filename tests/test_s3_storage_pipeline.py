"""Integration test: the pipeline on S3 (RustFS) produces the same Silver and Gold as on local disk.

Needs the docker compose stack (see integration_helpers). Every run writes under a unique prefix
(s3://lakehouse-bronze/pytest-<id>/...) and deletes it afterwards, so the real Bronze and Silver
data in the buckets are never touched.
"""
import sys
import uuid

import duckdb
import pytest

from integration_helpers import SRC, requires_stack, run_script

sys.path.insert(0, str(SRC))
import storage  # noqa: E402

pytestmark = requires_stack

DAYS, ORDERS_PER_DAY = 2, 50
TABLES = ("order_events", "orders", "order_items", "rejected_events")


def run_pipeline(env: dict) -> None:
    run_script("data_generator.py", env, "--days", str(DAYS), "--orders-per-day", str(ORDERS_PER_DAY))
    run_script("bronze_to_silver.py", env)


def table_glob(silver_uri: str, table: str) -> str:
    pattern = "*.parquet" if table == "rejected_events" else "**/*.parquet"
    return f"{silver_uri}/{table}/{pattern}"


def summary(con, silver_uri: str) -> dict:
    """Row count of every Silver table, plus the money total of orders."""
    result = {
        t: con.execute(f"SELECT count(*) FROM read_parquet('{table_glob(silver_uri, t)}')").fetchone()[0]
        for t in TABLES
    }
    result["net_amount"] = con.execute(
        f"SELECT sum(net_amount) FROM read_parquet('{table_glob(silver_uri, 'orders')}')"
    ).fetchone()[0]
    return result


def count_objects(uri: str) -> int:
    bucket, prefix = storage.split_s3(uri)
    pages = storage.s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/")
    return sum(len(page.get("Contents", [])) for page in pages)


@pytest.fixture(scope="module")
def s3_env():
    prefix = f"pytest-{uuid.uuid4().hex[:8]}"
    env = {
        "STORAGE": "s3",
        "BRONZE_URI": f"s3://lakehouse-bronze/{prefix}",
        "SILVER_URI": f"s3://lakehouse-silver/{prefix}",
    }
    yield env
    storage.delete_prefix(env["BRONZE_URI"])
    storage.delete_prefix(env["SILVER_URI"])


@pytest.fixture(scope="module")
def results(s3_env, tmp_path_factory):
    local_dir = tmp_path_factory.mktemp("local")
    run_pipeline({"STORAGE": "local", "DATA_DIR": str(local_dir)})
    run_pipeline(s3_env)
    con = storage.connect_duckdb(s3=True)
    yield {
        "local": summary(con, (local_dir / "silver").as_posix()),
        "s3": summary(con, s3_env["SILVER_URI"]),
        "con": con,
        "local_dir": local_dir,
    }
    con.close()


def gold_fingerprint(warehouse) -> tuple:
    """Row counts of the star schema plus the fact's money total."""
    with duckdb.connect(str(warehouse), read_only=True) as con:
        return con.execute("""
            SELECT (SELECT count(*) FROM gold.fact_order_items), (SELECT sum(net_amount) FROM gold.fact_order_items),
                   (SELECT count(*) FROM gold.dim_customer), (SELECT count(*) FROM gold.dim_product)
        """).fetchone()


def test_dbt_on_s3_silver_matches_dbt_on_local_silver(s3_env, results, tmp_path):
    run_script("run_dbt.py", {"STORAGE": "local", "DATA_DIR": str(results["local_dir"])}, "build")
    run_script("run_dbt.py", {**s3_env, "DATA_DIR": str(tmp_path)}, "build")
    local, on_s3 = gold_fingerprint(results["local_dir"] / "warehouse.duckdb"), gold_fingerprint(tmp_path / "warehouse.duckdb")
    assert on_s3 == local
    assert on_s3[0] > 0


def test_s3_silver_matches_local_silver(results):
    assert results["s3"]["orders"] == DAYS * ORDERS_PER_DAY
    assert results["s3"] == results["local"]


def test_s3_rerun_is_idempotent(s3_env, results):
    objects_before = count_objects(s3_env["SILVER_URI"])
    # Plant a partition that no longer exists in Bronze: a correct rebuild must remove it.
    stale = f"{s3_env['SILVER_URI']}/orders/order_date=1999-01-01/stale.parquet"
    results["con"].execute(f"COPY (SELECT 1 AS x) TO '{stale}' (FORMAT parquet)")
    assert count_objects(s3_env["SILVER_URI"]) == objects_before + 1

    run_script("bronze_to_silver.py", s3_env)

    assert summary(results["con"], s3_env["SILVER_URI"]) == results["s3"]
    assert count_objects(s3_env["SILVER_URI"]) == objects_before  # stale partition gone, nothing added
    assert count_objects(f"{s3_env['SILVER_URI']}/orders/order_date=1999-01-01") == 0


def test_delete_prefix_spares_sibling_prefixes(s3_env):
    base = s3_env["SILVER_URI"] + "/prefix-check"
    storage.write_text(f"{base}/orders/a.txt", "a")
    storage.write_text(f"{base}/orders_old/b.txt", "b")
    assert storage.delete_prefix(f"{base}/orders") == 1
    assert count_objects(f"{base}/orders_old") == 1


def test_delete_prefix_refuses_a_bucket_root():
    with pytest.raises(ValueError, match="bucket root"):
        storage.delete_prefix("s3://lakehouse-silver")
    with pytest.raises(ValueError, match="bucket root"):
        storage.delete_prefix("s3://lakehouse-silver/")

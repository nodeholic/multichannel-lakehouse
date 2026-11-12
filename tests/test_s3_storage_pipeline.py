"""Integration test: the pipeline on S3 (RustFS) produces the same Silver as on local disk.

Needs the docker compose stack and opts in explicitly, so the default test run stays offline:
    docker compose up -d
    RUN_S3_TESTS=1 pytest tests/test_s3_storage_pipeline.py -v

Every run writes under a unique prefix (s3://lakehouse-bronze/pytest-<id>/...) and deletes it
afterwards, so the real Bronze and Silver data in the buckets are never touched.
"""
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))
import storage  # noqa: E402

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_S3_TESTS") != "1",
    reason="set RUN_S3_TESTS=1 and run `docker compose up -d` to test against RustFS",
)

DAYS, ORDERS_PER_DAY = 2, 50
TABLES = ("order_events", "orders", "order_items", "rejected_events")


def run_script(script: str, env: dict, *args: str) -> None:
    result = subprocess.run(
        [sys.executable, str(SRC / script), *args],
        env={**os.environ, "PYTHONIOENCODING": "utf-8", **env},
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:  # show why the script failed, not just CalledProcessError
        raise AssertionError(f"{script} exited {result.returncode}:\n{result.stderr[-2000:]}")


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
    }
    con.close()


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

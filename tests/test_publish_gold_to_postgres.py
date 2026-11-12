"""Integration test: Gold published to Postgres equals the dbt warehouse, and republishing is idempotent.

Needs the docker compose stack (see integration_helpers). Every run publishes into its own schema
(pytest_<id>) and drops it afterwards, so the real `gold` schema is never touched.
"""
import sys
import uuid

import duckdb
import pytest

from integration_helpers import SRC, requires_stack, run_script

sys.path.insert(0, str(SRC))
from publish_gold_to_postgres import GOLD_TABLES, attach_postgres  # noqa: E402

pytestmark = requires_stack


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("gold")
    schema = f"pytest_{uuid.uuid4().hex[:8]}"
    env = {"STORAGE": "local", "DATA_DIR": str(data_dir), "POSTGRES_SCHEMA": schema}
    run_script("data_generator.py", env, "--days", "2", "--orders-per-day", "50")
    run_script("bronze_to_silver.py", env)
    run_script("run_dbt.py", env, "build")
    run_script("publish_gold_to_postgres.py", env)

    con = duckdb.connect()
    con.execute("ATTACH ? AS wh (READ_ONLY)", [(data_dir / "warehouse.duckdb").as_posix()])
    attach_postgres(con)
    yield {"con": con, "schema": schema, "env": env}
    con.execute("CALL postgres_execute('pg', ?)", [f"DROP SCHEMA IF EXISTS {schema} CASCADE"])
    con.execute("CALL postgres_execute('pg', ?)", [f"DROP SCHEMA IF EXISTS {schema}__loading CASCADE"])
    con.close()


def table_fingerprints(con, catalog_schema: str) -> dict:
    """Row count and an order-independent checksum of every row, per Gold table."""
    return {
        table: con.execute(f"SELECT count(*), bit_xor(hash(columns(*)::varchar)) FROM {catalog_schema}.{table}").fetchone()
        for table in GOLD_TABLES
    }


def test_postgres_gold_equals_warehouse_gold(published):
    con, schema = published["con"], published["schema"]
    con.execute("CALL postgres_clear_cache()")
    assert table_fingerprints(con, f"pg.{schema}") == table_fingerprints(con, "wh.gold")


def test_republish_is_idempotent_and_leaves_no_loading_schema(published):
    con, schema = published["con"], published["schema"]
    run_script("publish_gold_to_postgres.py", published["env"])
    con.execute("CALL postgres_clear_cache()")
    assert table_fingerprints(con, f"pg.{schema}") == table_fingerprints(con, "wh.gold")
    leftovers = con.execute(
        "SELECT count(*) FROM pg.information_schema.schemata WHERE schema_name = ?", [f"{schema}__loading"]
    ).fetchone()[0]
    assert leftovers == 0


def test_every_gold_table_has_its_primary_key(published):
    con, schema = published["con"], published["schema"]
    keys = dict(con.execute(
        """SELECT tc.table_name, kcu.column_name
           FROM pg.information_schema.table_constraints tc
           JOIN pg.information_schema.key_column_usage kcu
             ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
           WHERE tc.table_schema = ? AND tc.constraint_type = 'PRIMARY KEY'""",
        [schema],
    ).fetchall())
    assert keys == GOLD_TABLES

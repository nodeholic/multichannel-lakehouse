"""Integration test: publishing Gold to Postgres is exact, idempotent, and never destroys what it does not own.

Needs the docker compose stack (see integration_helpers). Every run publishes into its own schemas
(pytest_<id>...) and roles, and drops them afterwards, so the real `gold` schema is never touched.
"""
import sys
import uuid
from contextlib import contextmanager

import duckdb
import pytest

from integration_helpers import SRC, requires_stack, run_script, run_script_expecting_failure

sys.path.insert(0, str(SRC))
from publish_gold_to_postgres import GOLD_TABLES, MARKER, attach_postgres  # noqa: E402

pytestmark = requires_stack

RUN_ID = uuid.uuid4().hex[:8]
SCHEMA = f"pytest_{RUN_ID}"
READER_ROLE = f"pytest_reader_{RUN_ID}"


def pg(con, sql: str) -> None:
    con.execute("CALL postgres_execute('pg', ?)", [sql])


@contextmanager
def connect(warehouse=None):
    """A short-lived connection. Never keep one open across a publish: an open read transaction
    on the Gold tables blocks the swap (it fails after lock_timeout, by design)."""
    con = duckdb.connect()
    try:
        attach_postgres(con)
        if warehouse:
            con.execute("ATTACH ? AS wh (READ_ONLY)", [warehouse.as_posix()])
        yield con
    finally:
        con.close()


def scalar(sql: str, params=()):
    with connect() as con:
        return con.execute(sql, list(params)).fetchone()[0]


def execute(sql: str) -> None:
    with connect() as con:
        pg(con, sql)


@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    """Build Gold once, publish it, and guarantee cleanup even if setup fails half-way."""
    data_dir = tmp_path_factory.mktemp("gold")
    # POSTGRES_READER_ROLE is pinned empty so a role set in the developer's .env cannot leak in.
    env = {"STORAGE": "local", "DATA_DIR": str(data_dir), "POSTGRES_SCHEMA": SCHEMA, "POSTGRES_READER_ROLE": ""}
    execute(f"CREATE ROLE {READER_ROLE} NOLOGIN")
    try:
        run_script("data_generator.py", env, "--days", "2", "--orders-per-day", "50")
        run_script("bronze_to_silver.py", env)
        run_script("run_dbt.py", env, "build")
        run_script("publish_gold_to_postgres.py", {**env, "POSTGRES_READER_ROLE": READER_ROLE})
        yield {"warehouse": data_dir / "warehouse.duckdb", "env": env}
    finally:
        with connect() as con:
            leftovers = con.execute(
                "SELECT schema_name FROM pg.information_schema.schemata WHERE schema_name LIKE ?", [f"pytest_{RUN_ID}%"]
            ).fetchall()
            for (name,) in leftovers:
                pg(con, f"DROP SCHEMA IF EXISTS {name} CASCADE")
            pg(con, f"DROP ROLE IF EXISTS {READER_ROLE}")


def assert_same_rows(warehouse, schema: str) -> None:
    """Exact comparison, row by row, in both directions (duplicates included)."""
    with connect(warehouse) as con:
        _assert_same_rows(con, schema)


def _assert_same_rows(con, schema: str) -> None:
    for table in GOLD_TABLES:
        for left, right in ((f"wh.gold.{table}", f"pg.{schema}.{table}"), (f"pg.{schema}.{table}", f"wh.gold.{table}")):
            extra = con.execute(f"SELECT count(*) FROM (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right})").fetchone()[0]
            assert extra == 0, f"{left} has {extra} rows missing from {right}"


def loading_schemas(schema: str) -> int:
    return scalar("SELECT count(*) FROM pg.information_schema.schemata WHERE schema_name LIKE ?",
                  [f"{schema}__loading%"])


def pg_value(sql: str):
    """Run a query inside Postgres itself (for catalog functions DuckDB does not expose)."""
    return scalar("SELECT * FROM postgres_query('pg', ?)", [sql])


def test_postgres_gold_equals_warehouse_gold(stack):
    assert_same_rows(stack["warehouse"], SCHEMA)


def test_republish_is_idempotent_and_leaves_no_loading_schema(stack):
    run_script("publish_gold_to_postgres.py", stack["env"])
    assert_same_rows(stack["warehouse"], SCHEMA)
    assert loading_schemas(SCHEMA) == 0


def test_every_gold_table_has_its_primary_key(stack):
    with connect() as con:
        keys = dict(con.execute(
            """SELECT tc.table_name, kcu.column_name
               FROM pg.information_schema.table_constraints tc
               JOIN pg.information_schema.key_column_usage kcu
                 ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
               WHERE tc.table_schema = ? AND tc.constraint_type = 'PRIMARY KEY'""",
            [SCHEMA],
        ).fetchall())
    assert keys == GOLD_TABLES


def test_published_schema_carries_the_managed_marker(stack):
    comment = pg_value(f"SELECT obj_description(oid, 'pg_namespace') FROM pg_namespace WHERE nspname = '{SCHEMA}'")
    assert comment == MARKER


def test_reader_role_can_read_gold(stack):
    run_script("publish_gold_to_postgres.py", {**stack["env"], "POSTGRES_READER_ROLE": READER_ROLE})
    assert pg_value(f"SELECT has_table_privilege('{READER_ROLE}', '{SCHEMA}.fact_order_items', 'SELECT')") is True
    assert pg_value(f"SELECT has_schema_privilege('{READER_ROLE}', '{SCHEMA}', 'USAGE')") is True


def test_dependent_view_blocks_the_swap_and_survives(stack):
    other = f"{SCHEMA}_bi"
    execute(f"CREATE SCHEMA {other}")
    execute(f"CREATE VIEW {other}.revenue AS SELECT sum(net_amount) AS revenue FROM {SCHEMA}.fact_order_items")
    try:
        stderr = run_script_expecting_failure("publish_gold_to_postgres.py", stack["env"])
        assert "depend" in stderr  # Postgres: "cannot drop ... because other objects depend on it"
        assert scalar(f"SELECT count(*) FROM pg.{other}.revenue") == 1  # the BI view still works
        assert_same_rows(stack["warehouse"], SCHEMA)  # the published Gold is untouched
        assert loading_schemas(SCHEMA) == 0  # the failed run cleaned up after itself
    finally:
        execute(f"DROP SCHEMA {other} CASCADE")


def test_refuses_to_replace_a_schema_it_does_not_manage(stack):
    foreign = f"{SCHEMA}_app"
    execute(f"CREATE SCHEMA {foreign}")
    execute(f"CREATE TABLE {foreign}.customers (id int)")
    try:
        stderr = run_script_expecting_failure("publish_gold_to_postgres.py", {**stack["env"], "POSTGRES_SCHEMA": foreign})
        assert "not managed by this tool" in stderr
        assert scalar("SELECT count(*) FROM pg.information_schema.tables WHERE table_schema = ?", [foreign]) == 1
        assert loading_schemas(foreign) == 0
    finally:
        execute(f"DROP SCHEMA {foreign} CASCADE")


def test_foreign_object_inside_gold_blocks_the_swap_and_survives(stack):
    execute(f"CREATE TABLE {SCHEMA}.analyst_notes (note text)")
    try:
        stderr = run_script_expecting_failure("publish_gold_to_postgres.py", stack["env"])
        assert "depend" in stderr  # DROP SCHEMA ... RESTRICT refuses a schema that is not empty
        assert scalar("SELECT count(*) FROM pg.information_schema.tables WHERE table_schema = ?", [SCHEMA]) == 6
        assert_same_rows(stack["warehouse"], SCHEMA)
        assert loading_schemas(SCHEMA) == 0
    finally:
        execute(f"DROP TABLE {SCHEMA}.analyst_notes")


def test_missing_reader_role_fails_before_the_swap(stack):
    stderr = run_script_expecting_failure(
        "publish_gold_to_postgres.py", {**stack["env"], "POSTGRES_READER_ROLE": f"pytest_missing_{RUN_ID}"}
    )
    assert "does not exist" in stderr
    assert "DO $$" not in stderr  # the error shows Postgres' reason, not the echoed statement
    assert_same_rows(stack["warehouse"], SCHEMA)
    assert loading_schemas(SCHEMA) == 0

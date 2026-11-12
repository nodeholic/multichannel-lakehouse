"""Publish the Gold star schema from the dbt warehouse (DuckDB) to Postgres for BI tools.

  1. load every Gold table into a fresh per-run schema  <schema>__loading_<id>
  2. verify row counts and money totals against the warehouse; stop if anything differs
  3. add primary keys, grant read access to POSTGRES_READER_ROLE (optional), mark the schema
     as managed by this tool
  4. swap in ONE Postgres statement (a DO block, so one transaction): readers see either the
     previous Gold or the new one, never a half-loaded mix

The swap only replaces what this tool owns, and fails loudly instead of destroying anything else:
  - it refuses to replace a target schema that does not carry the "managed" marker
    (so POSTGRES_SCHEMA can never wipe an application schema)
  - it drops the old Gold tables and schema with RESTRICT, never CASCADE: if a BI view or any
    other object depends on Gold, the swap aborts, the transaction rolls back, and both the
    published Gold and the dependent objects stay as they were
  - an advisory lock serializes concurrent publishes, and the swap checks that the loading
    schema holds exactly the expected tables
Rerunning replaces the schema: the publish is idempotent.

  python src/publish_gold_to_postgres.py        # after `python src/run_dbt.py build`
"""
import sys
import uuid

import duckdb

import config

WAREHOUSE = config.DATA_DIR / "warehouse.duckdb"
MARKER = "managed by publish_gold_to_postgres"

# Table -> primary key column. Dimensions first: they are small and the fact points to them.
GOLD_TABLES = {
    "dim_date": "date_key",
    "dim_channel": "channel_key",
    "dim_product": "product_key",
    "dim_customer": "customer_key",
    "fact_order_items": "order_item_key",
}
MONEY_COLUMNS = {"fact_order_items": ("gross_amount", "discount_amount", "net_amount")}


class PublishError(RuntimeError):
    """The publish stopped; the previously published Gold is untouched."""


def attach_postgres(con: duckdb.DuckDBPyConnection) -> None:
    """Attach Postgres as catalog `pg`. Credentials go through a TEMPORARY secret: they never
    appear in a connection string and never touch disk."""
    if not (config.POSTGRES_USER and config.POSTGRES_PASSWORD):
        raise SystemExit("POSTGRES_USER and POSTGRES_PASSWORD are required: copy .env.example to .env")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(
        "CREATE OR REPLACE TEMPORARY SECRET lakehouse_pg (TYPE postgres, HOST ?, PORT ?, DATABASE ?, USER ?, PASSWORD ?)",
        [config.POSTGRES_HOST, config.POSTGRES_PORT, config.POSTGRES_DB, config.POSTGRES_USER, config.POSTGRES_PASSWORD],
    )
    con.execute("ATTACH '' AS pg (TYPE postgres, SECRET lakehouse_pg)")


def pg(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Run one statement directly in Postgres."""
    con.execute("CALL postgres_execute('pg', ?)", [sql])


def fingerprint(con: duckdb.DuckDBPyConnection, catalog_schema: str, table: str) -> tuple:
    """Row count plus every money total of a table."""
    sums = "".join(f", sum({col})" for col in MONEY_COLUMNS.get(table, ()))
    return con.execute(f"SELECT count(*){sums} FROM {catalog_schema}.{table}").fetchone()


def swap_statement(schema: str, loading: str) -> str:
    """The single DO block that replaces the published schema. Runs as one transaction."""
    tables = ", ".join(f"'{t}'" for t in GOLD_TABLES)
    old_tables = ", ".join(f"{schema}.{t}" for t in GOLD_TABLES)
    return f"""
DO $$
DECLARE
    target_oid oid := (SELECT oid FROM pg_namespace WHERE nspname = '{schema}');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('publish_gold:{schema}'));  -- one swap at a time
    SET LOCAL lock_timeout = '10s';  -- a long BI query must not queue every reader behind the swap

    IF (SELECT count(*) FROM pg_tables WHERE schemaname = '{loading}' AND tablename IN ({tables}))
       <> {len(GOLD_TABLES)} THEN
        RAISE EXCEPTION 'loading schema {loading} is incomplete, nothing published';
    END IF;

    IF target_oid IS NOT NULL THEN
        IF coalesce(obj_description(target_oid, 'pg_namespace'), '') <> '{MARKER}' THEN
            RAISE EXCEPTION 'schema {schema} exists but is not managed by this tool; '
                            'rename or drop it yourself, or choose another POSTGRES_SCHEMA';
        END IF;
        -- RESTRICT: if anything (a BI view, another table, ...) depends on Gold or lives in the
        -- schema, this raises and the whole transaction rolls back.
        DROP TABLE IF EXISTS {old_tables} RESTRICT;
        DROP SCHEMA {schema} RESTRICT;
    END IF;

    ALTER SCHEMA {loading} RENAME TO {schema};
END $$"""


def publish(schema: str = config.POSTGRES_SCHEMA) -> dict[str, int]:
    if not WAREHOUSE.exists():
        raise SystemExit(f"{WAREHOUSE} not found: run `python src/run_dbt.py build` first")
    loading = f"{schema}__loading_{uuid.uuid4().hex[:8]}"  # per run: concurrent publishes never share it

    # In-memory DuckDB with two attached catalogs: the warehouse read-only (`wh`, so the publish
    # can never modify Gold) and Postgres writable (`pg`). Opening the warehouse itself read-only
    # would make the whole connection read-only, Postgres included.
    con = duckdb.connect()
    try:
        con.execute("ATTACH ? AS wh (READ_ONLY)", [WAREHOUSE.as_posix()])
        attach_postgres(con)
        try:
            pg(con, f"CREATE SCHEMA {loading}")
            con.execute("CALL postgres_clear_cache()")  # DuckDB caches the Postgres catalog
            for table in GOLD_TABLES:
                con.execute(f"CREATE TABLE pg.{loading}.{table} AS SELECT * FROM wh.gold.{table}")

            mismatches = {
                table: (expected, actual)
                for table in GOLD_TABLES
                if (expected := fingerprint(con, "wh.gold", table)) != (actual := fingerprint(con, f"pg.{loading}", table))
            }
            if mismatches:
                raise PublishError(f"Gold in Postgres would differ from the warehouse: {mismatches}")

            for table, key in GOLD_TABLES.items():
                pg(con, f"ALTER TABLE {loading}.{table} ADD PRIMARY KEY ({key})")
            if config.POSTGRES_READER_ROLE:
                pg(con, f"GRANT USAGE ON SCHEMA {loading} TO {config.POSTGRES_READER_ROLE}")
                pg(con, f"GRANT SELECT ON ALL TABLES IN SCHEMA {loading} TO {config.POSTGRES_READER_ROLE}")
            pg(con, f"COMMENT ON SCHEMA {loading} IS '{MARKER}'")

            pg(con, swap_statement(schema, loading))
        except Exception:
            # Whatever failed, the published Gold is untouched; drop only this run's loading schema.
            pg(con, f"DROP SCHEMA IF EXISTS {loading} CASCADE")
            raise

        con.execute("CALL postgres_clear_cache()")
        return {table: fingerprint(con, f"pg.{schema}", table)[0] for table in GOLD_TABLES}
    finally:
        con.close()


def main() -> int:
    try:
        counts = publish()
    except (PublishError, duckdb.Error) as error:
        print(f"Publish failed, the published Gold is unchanged: {error}", file=sys.stderr)
        return 1
    target = f"{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
    print(f"Published Gold to Postgres {target}, schema {config.POSTGRES_SCHEMA}")
    for table, rows in counts.items():
        print(f"  {table:<18}: {rows:>6} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())

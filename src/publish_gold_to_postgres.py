"""Publish the Gold star schema from the dbt warehouse (DuckDB) to Postgres for BI tools.

The swap is atomic, so a dashboard never reads a half-loaded schema:
  1. load every Gold table into a fresh schema  <schema>__loading
  2. verify row counts and money totals against the warehouse; stop if anything differs
  3. add primary keys on the surrogate keys
  4. in ONE Postgres statement: drop <schema> and rename <schema>__loading to <schema>
     (Postgres DDL is transactional, so readers see either the old or the new Gold, never a mix)

Rerunning replaces the schema: the publish is idempotent.

  python src/publish_gold_to_postgres.py        # after `python src/run_dbt.py build`
"""
import sys

import duckdb

import config
from storage import connect_duckdb

WAREHOUSE = config.DATA_DIR / "warehouse.duckdb"

# Table -> primary key column. Dimensions first: they are small and the fact points to them.
GOLD_TABLES = {
    "dim_date": "date_key",
    "dim_channel": "channel_key",
    "dim_product": "product_key",
    "dim_customer": "customer_key",
    "fact_order_items": "order_item_key",
}
MONEY_COLUMN = {"fact_order_items": "net_amount"}


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
    """(row count, money total) of a table; the total is None for tables without money."""
    money = MONEY_COLUMN.get(table)
    total = f"sum({money})" if money else "NULL"
    return con.execute(f"SELECT count(*), {total} FROM {catalog_schema}.{table}").fetchone()


def publish(schema: str = config.POSTGRES_SCHEMA) -> dict[str, int]:
    if not WAREHOUSE.exists():
        raise SystemExit(f"{WAREHOUSE} not found: run `python src/run_dbt.py build` first")
    loading = f"{schema}__loading"

    # In-memory DuckDB with two attached catalogs: the warehouse read-only (`wh`, so the
    # publish can never modify Gold) and Postgres writable (`pg`). Opening the warehouse
    # itself read-only would make the whole connection read-only, Postgres included.
    con = connect_duckdb()
    con.execute("ATTACH ? AS wh (READ_ONLY)", [WAREHOUSE.as_posix()])
    attach_postgres(con)

    pg(con, f"DROP SCHEMA IF EXISTS {loading} CASCADE")
    pg(con, f"CREATE SCHEMA {loading}")
    con.execute("CALL postgres_clear_cache()")  # DuckDB caches the Postgres catalog
    for table in GOLD_TABLES:
        con.execute(f"CREATE TABLE pg.{loading}.{table} AS SELECT * FROM wh.gold.{table}")

    # Verify before swapping: if anything differs, the published Gold stays untouched.
    mismatches = {
        table: (expected, actual)
        for table in GOLD_TABLES
        if (expected := fingerprint(con, "wh.gold", table)) != (actual := fingerprint(con, f"pg.{loading}", table))
    }
    if mismatches:
        pg(con, f"DROP SCHEMA {loading} CASCADE")
        raise SystemExit(f"Gold in Postgres would differ from the warehouse, nothing published: {mismatches}")

    for table, key in GOLD_TABLES.items():
        pg(con, f"ALTER TABLE {loading}.{table} ADD PRIMARY KEY ({key})")

    pg(con, f"DO $$ BEGIN DROP SCHEMA IF EXISTS {schema} CASCADE; ALTER SCHEMA {loading} RENAME TO {schema}; END $$")
    con.execute("CALL postgres_clear_cache()")

    counts = {table: fingerprint(con, f"pg.{schema}", table)[0] for table in GOLD_TABLES}
    con.close()
    return counts


def main() -> None:
    counts = publish()
    target = f"{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}"
    print(f"Published Gold to Postgres {target}, schema {config.POSTGRES_SCHEMA}")
    for table, rows in counts.items():
        print(f"  {table:<18}: {rows:>6} rows")


if __name__ == "__main__":
    sys.exit(main())

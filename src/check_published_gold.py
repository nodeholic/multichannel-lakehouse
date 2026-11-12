"""Check that every Gold table is published in Postgres and not empty. Exits 1 otherwise.

Used by CI after the S3 + Postgres pipeline run, and handy locally after a publish:
  python src/check_published_gold.py
"""
import sys

import duckdb

import config
from publish_gold_to_postgres import GOLD_TABLES, attach_postgres


def published_row_counts(schema: str) -> dict[str, int | None]:
    """Rows per Gold table in Postgres; None when the table does not exist."""
    con = duckdb.connect()
    try:
        attach_postgres(con)
        existing = {
            name for (name,) in con.execute(
                "SELECT table_name FROM pg.information_schema.tables WHERE table_schema = ?", [schema]
            ).fetchall()
        }
        return {
            table: con.execute(f'SELECT count(*) FROM pg."{schema}".{table}').fetchone()[0] if table in existing else None
            for table in GOLD_TABLES
        }
    finally:
        con.close()


def main() -> int:
    schema = config.POSTGRES_SCHEMA
    counts = published_row_counts(schema)
    problems = [f"{table}: {'missing' if rows is None else 'empty'}" for table, rows in counts.items() if not rows]
    for table, rows in counts.items():
        print(f"  {schema}.{table:<18}: {'missing' if rows is None else rows}")
    if problems:
        print(f"Gold in Postgres is incomplete: {', '.join(problems)}", file=sys.stderr)
        return 1
    print(f"All {len(GOLD_TABLES)} Gold tables are published in {schema} and not empty")
    return 0


if __name__ == "__main__":
    sys.exit(main())

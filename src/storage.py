"""Storage helpers that work the same on local disk and on S3.

Every job goes through these functions, so switching STORAGE=local|s3 never touches
pipeline logic:
  - write_text(uri, text)    write one file (used for Bronze JSON Lines)
  - delete_prefix(uri)       remove a whole folder / key prefix (keeps Silver rebuilds idempotent)
  - prepare_target(uri)      make sure DuckDB can write under uri
  - connect_duckdb(path)     a DuckDB connection that can read and write s3:// URIs
"""
import shutil
from pathlib import Path
from urllib.parse import urlparse

import duckdb

import config


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def split_s3(uri: str) -> tuple[str, str]:
    """s3://bucket/a/b -> ("bucket", "a/b")."""
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")


def s3_client():
    import boto3  # imported lazily: local runs never need it

    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name=config.S3_REGION,
    )


def write_text(uri: str, text: str) -> None:
    if is_s3(uri):
        bucket, key = split_s3(uri)
        s3_client().put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
    else:
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")


def delete_prefix(uri: str) -> int:
    """Delete everything under uri. Returns the number of objects/files removed."""
    if not is_s3(uri):
        path = Path(uri)
        count = sum(1 for p in path.rglob("*") if p.is_file()) if path.exists() else 0
        shutil.rmtree(path, ignore_errors=True)
        return count

    bucket, prefix = split_s3(uri)
    prefix = prefix.rstrip("/") + "/"  # never match a sibling such as "orders_old/"
    client = s3_client()
    removed = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            removed += len(keys)
    return removed


def prepare_target(uri: str) -> None:
    """DuckDB needs the parent folder to exist on local disk; S3 has no folders."""
    if not is_s3(uri):
        Path(uri).parent.mkdir(parents=True, exist_ok=True)


def connect_duckdb(
    database: str = ":memory:", read_only: bool = False, s3: bool | None = None
) -> duckdb.DuckDBPyConnection:
    """DuckDB connection that can read and write s3:// URIs on RustFS.
    s3 defaults to STORAGE == "s3"; tests pass s3=True explicitly."""
    con = duckdb.connect(database, read_only=read_only)
    if config.STORAGE == "s3" if s3 is None else s3:
        endpoint = urlparse(config.S3_ENDPOINT)
        con.execute("INSTALL httpfs; LOAD httpfs;")
        # A TEMPORARY secret lives only in this connection: credentials never touch disk.
        con.execute(
            """
            CREATE OR REPLACE TEMPORARY SECRET lakehouse_s3 (
                TYPE s3, KEY_ID ?, SECRET ?, REGION ?, ENDPOINT ?,
                URL_STYLE 'path', USE_SSL ?
            )
            """,
            [config.S3_ACCESS_KEY, config.S3_SECRET_KEY, config.S3_REGION,
             endpoint.netloc, endpoint.scheme == "https"],
        )
    return con

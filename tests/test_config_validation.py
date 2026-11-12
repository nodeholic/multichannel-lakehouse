"""Invalid storage settings must fail at startup with a message naming the variable.

config.py is evaluated at import time, so each case imports it in a fresh subprocess with
its own environment. The .env file is ignored by pointing every variable explicitly.
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
VALID_S3 = {"STORAGE": "s3", "S3_ACCESS_KEY": "key", "S3_SECRET_KEY": "secret", "S3_ENDPOINT": "http://localhost:9000"}


def import_config(env: dict) -> subprocess.CompletedProcess:
    prefixes = ("S3_", "BRONZE_", "SILVER_", "STORAGE", "POSTGRES_SCHEMA", "POSTGRES_READER_ROLE")
    base = {k: v for k, v in os.environ.items() if not k.startswith(prefixes)}
    return subprocess.run(
        [sys.executable, "-c", "import config; print(config.BRONZE_URI); print(config.SILVER_URI)"],
        cwd=SRC, env={**base, **env}, capture_output=True, text=True,
    )


def test_empty_uri_falls_back_to_the_default(tmp_path):
    result = import_config({"STORAGE": "local", "DATA_DIR": str(tmp_path), "SILVER_URI": ""})
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[1] == (tmp_path / "silver").as_posix()


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"STORAGE": "local", "SILVER_URI": "/"}, "SILVER_URI must not be empty"),
        ({"STORAGE": "local", "SILVER_URI": "s3://lakehouse-silver"}, "STORAGE must be 's3'"),
        ({**VALID_S3, "SILVER_URI": "s3:///orders"}, "has no bucket name"),
        ({**VALID_S3, "BRONZE_URI": "data/bronze"}, "STORAGE=s3 needs s3:// URIs"),
        ({**VALID_S3, "S3_ENDPOINT": "localhost:9000"}, "S3_ENDPOINT must look like"),
        ({**VALID_S3, "S3_SECRET_KEY": ""}, "needs S3_ACCESS_KEY and S3_SECRET_KEY"),
        ({"STORAGE": "minio"}, "STORAGE must be 'local' or 's3'"),
        ({"STORAGE": "local", "POSTGRES_SCHEMA": "public"}, "is a system schema"),
        ({"STORAGE": "local", "POSTGRES_SCHEMA": "pg_temp"}, "is a system schema"),
        ({"STORAGE": "local", "POSTGRES_SCHEMA": "Gold"}, "POSTGRES_SCHEMA must match"),
        ({"STORAGE": "local", "POSTGRES_SCHEMA": "g" * 41}, "POSTGRES_SCHEMA must match"),
        ({"STORAGE": "local", "POSTGRES_SCHEMA": "gold; drop"}, "POSTGRES_SCHEMA must match"),
        ({"STORAGE": "local", "POSTGRES_READER_ROLE": "bi reader"}, "POSTGRES_READER_ROLE must be"),
    ],
)
def test_invalid_settings_fail_at_startup(env, message):
    result = import_config(env)
    assert result.returncode != 0
    assert message in result.stderr


def test_valid_s3_settings_are_accepted():
    result = import_config(VALID_S3)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["s3://lakehouse-bronze", "s3://lakehouse-silver"]


def test_tests_always_run_against_local_storage():
    # Guaranteed by conftest.pin_local_storage, whatever the shell exports.
    assert os.environ["STORAGE"] == "local"
    assert "BRONZE_URI" not in os.environ and "SILVER_URI" not in os.environ

"""Shared helpers for integration tests that need the docker compose stack (RustFS + Postgres).

They opt in explicitly, so the default test run stays offline and needs no Docker:
    docker compose up -d
    RUN_INTEGRATION_TESTS=1 pytest -v
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"

requires_stack = pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS") != "1",
    reason="set RUN_INTEGRATION_TESTS=1 and run `docker compose up -d` to test against RustFS and Postgres",
)


def _run(script: str, env: dict, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SRC / script), *args],
        env={**os.environ, "PYTHONIOENCODING": "utf-8", **env},
        capture_output=True,
        text=True,
    )


def run_script(script: str, env: dict, *args: str) -> None:
    """Run a pipeline script in a subprocess with extra env vars; show its stderr on failure."""
    result = _run(script, env, *args)
    if result.returncode != 0:
        raise AssertionError(f"{script} exited {result.returncode}:\n{(result.stderr or result.stdout)[-2000:]}")


def run_script_expecting_failure(script: str, env: dict, *args: str) -> str:
    """Run a script that must fail; return its stderr so the test can check the reason."""
    result = _run(script, env, *args)
    if result.returncode == 0:
        raise AssertionError(f"{script} was expected to fail but exited 0:\n{result.stdout[-2000:]}")
    return result.stderr

"""Test-wide safety net: every test runs against LOCAL storage unless it opts into S3 explicitly.

Without this, a shell where `STORAGE=s3` (or BRONZE_URI / SILVER_URI) is still exported would
make the local pipeline tests write test events into the real Bronze bucket and rebuild the
real Silver tables. The S3 integration tests pass their own environment to each subprocess,
so they are unaffected.

Session scope, so the environment is pinned before any module-scoped fixture runs a pipeline.
"""
import pytest

STORAGE_VARS = ("BRONZE_URI", "SILVER_URI")


@pytest.fixture(scope="session", autouse=True)
def pin_local_storage():
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("STORAGE", "local")
        for name in STORAGE_VARS:
            patch.delenv(name, raising=False)
        yield

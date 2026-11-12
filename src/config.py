"""Shared configuration: where each data layer lives, S3 connection settings, and the PII salt.

STORAGE selects the backend:
  local (default) : data/bronze and data/silver on disk, no Docker needed (tests, CI)
  s3              : s3://lakehouse-bronze and s3://lakehouse-silver on the RustFS stack

Layer locations are plain URI strings (a local path or s3://bucket/prefix), so the jobs
never care which backend is in use. BRONZE_URI and SILVER_URI override them, which lets
integration tests write to an isolated prefix instead of the real buckets.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_dotenv(path: Path) -> None:
    """Read KEY=VALUE lines from .env into os.environ without overriding real env vars.
    Keeps the S3 credentials of docker-compose and of the pipeline in one file."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


load_dotenv(PROJECT_ROOT / ".env")

# The data directory can be overridden via an env var (tests use a temp directory).
# It also holds the dbt warehouse file, whatever the storage backend.
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_ROOT / "data"))

STORAGE = os.environ.get("STORAGE", "local").lower()
if STORAGE not in ("local", "s3"):
    raise ValueError(f"STORAGE must be 'local' or 's3', got {STORAGE!r}")

if STORAGE == "s3":
    BRONZE_URI = os.environ.get("BRONZE_URI", "s3://lakehouse-bronze").rstrip("/")
    SILVER_URI = os.environ.get("SILVER_URI", "s3://lakehouse-silver").rstrip("/")
else:
    BRONZE_URI = os.environ.get("BRONZE_URI", (DATA_DIR / "bronze").as_posix()).rstrip("/")
    SILVER_URI = os.environ.get("SILVER_URI", (DATA_DIR / "silver").as_posix()).rstrip("/")

BRONZE_ORDERS_URI = f"{BRONZE_URI}/orders"

# S3 connection (RustFS from docker-compose.yml by default).
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", f"http://localhost:{os.environ.get('S3_PORT', '9000')}")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")

# The salt is mixed into values before hashing. Without it, hashed phone numbers are
# trivial to reverse: there are only ~10^9 possible numbers, brute-forced in minutes.
PII_SALT = os.environ.get("PII_SALT", "dev-only-salt-change-me")

"""Shared configuration: where each data layer lives, S3 connection settings, and the PII salt.

STORAGE selects the backend:
  local (default) : data/bronze and data/silver on disk, no Docker needed (tests, CI)
  s3              : s3://lakehouse-bronze and s3://lakehouse-silver on the RustFS stack

Layer locations are plain URI strings (a local path or s3://bucket/prefix), so the jobs
never care which backend is in use. BRONZE_URI and SILVER_URI override them, which lets
integration tests write to an isolated prefix instead of the real buckets.

Invalid settings fail here, at startup, with a message naming the variable, instead of
surfacing later as a confusing S3 or file-system error.
"""
import os
import re
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class ConfigError(ValueError):
    """Raised at import time when the environment describes an unusable configuration."""


def load_dotenv(path: Path) -> None:
    """Read KEY=VALUE lines from .env into os.environ without overriding real env vars.
    Keeps the S3 credentials of docker compose and of the pipeline in one file. Like docker
    compose, surrounding single or double quotes are stripped from values."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = (part.strip() for part in line.split("=", 1))
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        os.environ.setdefault(key, value)


def env(name: str, default: str) -> str:
    """An env var, or the default when it is unset OR empty (a blank `NAME=` line in .env)."""
    return os.environ.get(name) or default


def layer_uri(name: str, default: str) -> str:
    """Read and validate a layer root such as SILVER_URI."""
    uri = env(name, default).strip().rstrip("/")
    if not uri:
        raise ConfigError(f"{name} must not be empty or '/'")
    if uri.startswith("s3://"):
        if STORAGE != "s3":
            raise ConfigError(f"{name}={uri} is an s3:// URI, so STORAGE must be 's3' (got {STORAGE!r})")
        if not urlparse(uri).netloc:
            raise ConfigError(f"{name}={uri} has no bucket name (expected s3://bucket/prefix)")
    elif STORAGE == "s3":
        raise ConfigError(f"STORAGE=s3 needs s3:// URIs, but {name}={uri}")
    return uri


load_dotenv(PROJECT_ROOT / ".env")

# The data directory can be overridden via an env var (tests use a temp directory).
# It also holds the dbt warehouse file, whatever the storage backend.
DATA_DIR = Path(env("DATA_DIR", str(PROJECT_ROOT / "data")))

STORAGE = env("STORAGE", "local").strip().lower()
if STORAGE not in ("local", "s3"):
    raise ConfigError(f"STORAGE must be 'local' or 's3', got {STORAGE!r}")

if STORAGE == "s3":
    BRONZE_URI = layer_uri("BRONZE_URI", "s3://lakehouse-bronze")
    SILVER_URI = layer_uri("SILVER_URI", "s3://lakehouse-silver")
else:
    BRONZE_URI = layer_uri("BRONZE_URI", (DATA_DIR / "bronze").as_posix())
    SILVER_URI = layer_uri("SILVER_URI", (DATA_DIR / "silver").as_posix())

BRONZE_ORDERS_URI = f"{BRONZE_URI}/orders"

# S3 connection (RustFS from docker-compose.yml by default).
S3_ENDPOINT = env("S3_ENDPOINT", f"http://localhost:{env('S3_PORT', '9000')}")
S3_ACCESS_KEY = env("S3_ACCESS_KEY", "")
S3_SECRET_KEY = env("S3_SECRET_KEY", "")
S3_REGION = env("S3_REGION", "us-east-1")

_endpoint = urlparse(S3_ENDPOINT)
if _endpoint.scheme not in ("http", "https") or not _endpoint.netloc:
    # Without a scheme, DuckDB silently falls back to s3.amazonaws.com with the RustFS keys.
    raise ConfigError(f"S3_ENDPOINT must look like http://host:port, got {S3_ENDPOINT!r}")

if STORAGE == "s3" and not (S3_ACCESS_KEY and S3_SECRET_KEY):
    raise ConfigError("STORAGE=s3 needs S3_ACCESS_KEY and S3_SECRET_KEY: copy .env.example to .env")

# Postgres serving database (docker-compose.yml). Gold is published to POSTGRES_SCHEMA.
POSTGRES_HOST = env("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(env("POSTGRES_PORT", "5432"))
POSTGRES_DB = env("POSTGRES_DB", "warehouse")
POSTGRES_USER = env("POSTGRES_USER", "")
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", "")
POSTGRES_SCHEMA = env("POSTGRES_SCHEMA", "gold")
# The name is interpolated into DDL and gets a "__loading_<8 hex>" suffix during a publish.
# Postgres silently truncates identifiers to 63 bytes, so the base name stays <= 40 characters:
# otherwise the loading name could be truncated into the live schema name.
if not re.fullmatch(r"[a-z_][a-z0-9_]{0,39}", POSTGRES_SCHEMA):
    raise ConfigError(f"POSTGRES_SCHEMA must match [a-z_][a-z0-9_]{{0,39}}, got {POSTGRES_SCHEMA!r}")
if POSTGRES_SCHEMA in ("public", "information_schema") or POSTGRES_SCHEMA.startswith("pg_"):
    raise ConfigError(f"POSTGRES_SCHEMA={POSTGRES_SCHEMA} is a system schema; the publish replaces its target")
# Optional role that gets read access to the published Gold (USAGE on the schema, SELECT on tables).
POSTGRES_READER_ROLE = env("POSTGRES_READER_ROLE", "")
if POSTGRES_READER_ROLE and not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", POSTGRES_READER_ROLE):
    raise ConfigError(f"POSTGRES_READER_ROLE must be a plain lower-case role name, got {POSTGRES_READER_ROLE!r}")

# The salt is mixed into values before hashing. Without it, hashed phone numbers are
# trivial to reverse: there are only ~10^9 possible numbers, brute-forced in minutes.
PII_SALT = env("PII_SALT", "dev-only-salt-change-me")

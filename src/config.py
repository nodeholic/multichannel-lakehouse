"""Shared configuration: storage paths for each data layer and the PII hashing salt.

Phase 1 keeps everything on local disk under data/.
When storage moves to MinIO (phase 3), only these paths change to s3://...
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

BRONZE_ORDERS_DIR = DATA_DIR / "bronze" / "orders"
SILVER_DIR = DATA_DIR / "silver"

# The salt is mixed into values before hashing. Without it, hashed phone numbers are
# trivial to reverse: there are only ~10^9 possible numbers, brute-forced in minutes.
PII_SALT = os.environ.get("PII_SALT", "dev-only-salt-change-me")

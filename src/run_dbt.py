"""Run dbt with the same storage settings as the rest of the pipeline.

Picks the dbt target from STORAGE (local or s3) and exports what profiles.yml and the Silver
sources need (DATA_DIR, SILVER_URI and, for S3, the endpoint and credentials from .env).
Every argument is passed through to dbt.

  python src/run_dbt.py build                 # local Silver
  STORAGE=s3 python src/run_dbt.py build      # Silver on RustFS
"""
import os
import sys
from urllib.parse import urlparse

from dbt.cli.main import dbtRunner

import config

DBT_DIR = config.PROJECT_ROOT / "dbt"


def dbt_environment() -> dict[str, str]:
    env = {"DATA_DIR": config.DATA_DIR.as_posix(), "SILVER_URI": config.SILVER_URI}
    if config.STORAGE == "s3":
        endpoint = urlparse(config.S3_ENDPOINT)
        env |= {
            "S3_ACCESS_KEY": config.S3_ACCESS_KEY,
            "S3_SECRET_KEY": config.S3_SECRET_KEY,
            "S3_REGION": config.S3_REGION,
            "S3_ENDPOINT_HOST": endpoint.netloc,  # DuckDB wants host:port, no scheme
            "S3_USE_SSL": str(endpoint.scheme == "https").lower(),
        }
    return env


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    os.environ.update(dbt_environment())
    args = [*sys.argv[1:], "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    if "--target" not in args and "-t" not in args:  # an explicit target from the user wins
        args += ["--target", config.STORAGE]
    return 0 if dbtRunner().invoke(args).success else 1


if __name__ == "__main__":
    sys.exit(main())

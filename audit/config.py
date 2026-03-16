"""Config loader for Oracle + Snowflake audit scripts."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_FILE = ROOT / "config" / "pipeline.yaml"
ENV_FILE = ROOT / "config" / "env.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def load_pipeline() -> dict[str, Any]:
    return _load_yaml(PIPELINE_FILE)


def load_env() -> dict[str, Any]:
    base: dict[str, Any] = {
        "oracle_rds": {
            "host":         os.environ.get("ORACLE_HOST"),
            "port":         int(os.environ.get("ORACLE_PORT", "1521")),
            "service_name": os.environ.get("ORACLE_SERVICE", "ORCL"),
            "user":         os.environ.get("ORACLE_USER"),
            "password":     os.environ.get("ORACLE_PASSWORD"),
        },
        "snowflake": {
            "account":   os.environ.get("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "database":  os.environ.get("SNOWFLAKE_DATABASE"),
            "role":      os.environ.get("SNOWFLAKE_ROLE"),
            "user":      os.environ.get("SNOWFLAKE_USER"),
            "password":  os.environ.get("SNOWFLAKE_PASSWORD"),
        },
        "aws": {
            "region": os.environ.get("AWS_REGION", "us-east-1"),
        },
        "secrets": {
            "oracle_password_secret_id":    os.environ.get("ORACLE_PASSWORD_SECRET_ID"),
            "snowflake_password_secret_id": os.environ.get("SNOWFLAKE_PASSWORD_SECRET_ID"),
        },
        "paths": {
            "logs_dir":    os.environ.get("LOGS_DIR",    str(ROOT / "logs")),
            "reports_dir": os.environ.get("REPORTS_DIR", str(ROOT / "reports")),
        },
    }
    # env.yaml values override env var defaults
    file_env = _load_yaml(ENV_FILE)
    return {**base, **file_env}


def get_schemas() -> list[str]:
    pipeline = load_pipeline()
    return [s["name"] for s in pipeline.get("source", {}).get("schemas", [])]


def get_audit_workers(default: int = 8) -> int:
    pipeline = load_pipeline()
    return int(pipeline.get("audit", {}).get("parallel_workers", default))

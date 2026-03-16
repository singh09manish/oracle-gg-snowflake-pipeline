#!/usr/bin/env python3
"""
Target-side audit: compute per-table row counts on Snowflake.

Produces a JSON report in the same format as control_totals.py so
recon.py can compare the two directly.

Usage:
  python3 audit/snowflake_totals.py
  python3 audit/snowflake_totals.py --schemas SCHEMA1 --max-tables 10
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
import snowflake.connector

from audit.config import get_audit_workers, load_env, load_pipeline


log = logging.getLogger("snowflake_totals")


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "snowflake_totals.log"),
            logging.StreamHandler(),
        ],
    )


def _get_snowflake_password(env: dict[str, Any]) -> str:
    sf = env.get("snowflake", {})
    if sf.get("password"):
        return sf["password"]
    secret_id = env.get("secrets", {}).get("snowflake_password_secret_id")
    if not secret_id:
        raise ValueError("Snowflake password not set (SNOWFLAKE_PASSWORD or Secrets Manager id).")
    region = env.get("aws", {}).get("region", "us-east-1")
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=secret_id)
    s = val["SecretString"]
    try:
        j = json.loads(s)
        return j.get("password") or j.get("SNOWFLAKE_PASSWORD") or s
    except Exception:
        return s


def _connect(env: dict[str, Any]):
    sf = env["snowflake"]
    return snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=_get_snowflake_password(env),
        warehouse=sf["warehouse"],
        database=sf["database"],
        role=sf.get("role"),
    )


def _list_tables(conn, schema: str, database: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT TABLE_NAME FROM information_schema.tables "
            "WHERE TABLE_SCHEMA = %s AND TABLE_CATALOG = %s "
            "AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
            (schema.upper(), database.upper()),
        )
        return [r[0] for r in cur.fetchall()]


def _count_table(conn, database: str, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{database}"."{schema}"."{table}"')
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _compute_one(
    env: dict[str, Any], database: str, schema: str, table: str
) -> dict[str, Any]:
    conn = _connect(env)
    try:
        return {
            "schema":    schema,
            "table":     table,
            "row_count": _count_table(conn, database, schema, table),
            "error":     None,
        }
    except Exception as e:
        return {"schema": schema, "table": table, "row_count": None, "error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schemas", nargs="*", help="Snowflake schemas to audit (default: target schemas from pipeline.yaml)")
    ap.add_argument("--max-tables", type=int, default=0)
    ap.add_argument("--workers", type=int, default=0)
    args = ap.parse_args()

    env = load_env()
    pipeline = load_pipeline()
    logs_dir = Path(env.get("paths", {}).get("logs_dir", "logs"))
    reports_dir = Path(env.get("paths", {}).get("reports_dir", "reports"))
    _setup_logging(logs_dir)

    sf_cfg = env.get("snowflake", {})
    database = sf_cfg.get("database") or pipeline["gg21c"]["snowflake"]["database"]

    # Default: target schemas from replicat_groups
    if args.schemas:
        schemas = args.schemas
    else:
        schemas = [rg["target_schema"] for rg in pipeline["gg21c"]["replicat_groups"]]

    workers = args.workers or get_audit_workers()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    conn = _connect(env)
    table_tasks: list[tuple[str, str]] = []
    try:
        for schema in schemas:
            tables = _list_tables(conn, schema, database)
            if args.max_tables > 0:
                tables = tables[: args.max_tables]
            table_tasks.extend((schema, t) for t in tables)
    finally:
        conn.close()

    log.info("run_id=%s database=%s schemas=%s tables=%d workers=%d",
             run_id, database, schemas, len(table_tasks), workers)

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_compute_one, env, database, s, t) for s, t in table_tasks]
        for fut in as_completed(futs):
            results.append(fut.result())

    errors = [r for r in results if r.get("error")]
    summary = {
        "run_id":        run_id,
        "source":        "snowflake",
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "database":      database,
        "schemas":       schemas,
        "tables_total":  len(results),
        "tables_ok":     len(results) - len(errors),
        "tables_error":  len(errors),
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / f"snowflake_totals_{run_id}.json"
    out_path.write_text(json.dumps({"summary": summary, "tables": results}, indent=2))
    log.info("Wrote %s", out_path)
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":
    main()

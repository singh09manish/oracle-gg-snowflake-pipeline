#!/usr/bin/env python3
"""
Source-side audit: compute per-table row counts on Oracle RDS.

Outputs a JSON report under reports/ used as the source-of-truth
for reconciliation against Snowflake target counts.

Usage:
  python3 audit/control_totals.py
  python3 audit/control_totals.py --schemas SCHEMA1 --max-tables 10
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
import oracledb

from audit.config import get_audit_workers, load_env, load_pipeline


log = logging.getLogger("control_totals")


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "control_totals.log"),
            logging.StreamHandler(),
        ],
    )


def _get_oracle_password(env: dict[str, Any]) -> str:
    rds = env.get("oracle_rds", {})
    if rds.get("password"):
        return rds["password"]
    secret_id = env.get("secrets", {}).get("oracle_password_secret_id")
    if not secret_id:
        raise ValueError("Oracle password not set (ORACLE_PASSWORD or Secrets Manager id).")
    region = env.get("aws", {}).get("region", "us-east-1")
    sm = boto3.client("secretsmanager", region_name=region)
    val = sm.get_secret_value(SecretId=secret_id)
    s = val["SecretString"]
    try:
        j = json.loads(s)
        return j.get("password") or j.get("ORACLE_PASSWORD") or s
    except Exception:
        return s


def _connect(env: dict[str, Any]):
    rds = env["oracle_rds"]
    dsn = oracledb.makedsn(rds["host"], int(rds.get("port", 1521)), service_name=rds["service_name"])
    return oracledb.connect(user=rds["user"], password=_get_oracle_password(env), dsn=dsn)


def _list_tables(conn, schema: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM all_tables WHERE owner = :o ORDER BY table_name",
            o=schema.upper(),
        )
        return [r[0] for r in cur.fetchall()]


def _count_table(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _compute_one(schema: str, table: str, env: dict[str, Any]) -> dict[str, Any]:
    conn = _connect(env)
    try:
        return {"schema": schema, "table": table, "row_count": _count_table(conn, schema, table), "error": None}
    except Exception as e:
        return {"schema": schema, "table": table, "row_count": None, "error": str(e)}
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--schemas", nargs="*", help="Schemas to audit (default: all from pipeline.yaml)")
    ap.add_argument("--max-tables", type=int, default=0, help="Limit tables per schema (0 = no limit)")
    ap.add_argument("--workers", type=int, default=0, help="Parallel workers (0 = from config)")
    args = ap.parse_args()

    env = load_env()
    pipeline = load_pipeline()
    logs_dir = Path(env.get("paths", {}).get("logs_dir", "logs"))
    reports_dir = Path(env.get("paths", {}).get("reports_dir", "reports"))
    _setup_logging(logs_dir)

    schemas = args.schemas or [s["name"] for s in pipeline["source"]["schemas"]]
    workers = args.workers or get_audit_workers()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    conn = _connect(env)
    table_tasks: list[tuple[str, str]] = []
    try:
        for s in schemas:
            tables = _list_tables(conn, s)
            if args.max_tables > 0:
                tables = tables[: args.max_tables]
            table_tasks.extend((s, t) for t in tables)
    finally:
        conn.close()

    log.info("run_id=%s schemas=%s tables=%d workers=%d", run_id, schemas, len(table_tasks), workers)

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_compute_one, s, t, env) for s, t in table_tasks]
        for fut in as_completed(futs):
            results.append(fut.result())

    errors = [r for r in results if r.get("error")]
    summary = {
        "run_id":        run_id,
        "source":        "oracle_rds",
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "schemas":       schemas,
        "tables_total":  len(results),
        "tables_ok":     len(results) - len(errors),
        "tables_error":  len(errors),
    }

    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / f"oracle_totals_{run_id}.json"
    out_path.write_text(json.dumps({"summary": summary, "tables": results}, indent=2))
    log.info("Wrote %s", out_path)
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":
    main()

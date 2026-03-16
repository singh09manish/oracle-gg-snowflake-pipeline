#!/usr/bin/env python3
"""
Reconciliation: compare Oracle source counts vs Snowflake target counts.

Workflow (nightly cron):
  02:00  python3 audit/control_totals.py      → reports/oracle_totals_<id>.json
  03:00  python3 audit/snowflake_totals.py    → reports/snowflake_totals_<id>.json
  04:00  python3 audit/recon.py               → reports/recon_<id>.json

Usage:
  # Auto-discover latest oracle + snowflake reports:
  python3 audit/recon.py

  # Explicit paths:
  python3 audit/recon.py \
    --oracle  reports/oracle_totals_20240101_020000.json \
    --snowflake reports/snowflake_totals_20240101_030000.json

  # Include matched tables in output (default: mismatches only):
  python3 audit/recon.py --show-all
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from audit.config import load_env


ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("recon")


def _setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "recon.log"),
            logging.StreamHandler(),
        ],
    )


def _latest(reports_dir: Path, prefix: str) -> Path:
    candidates = sorted(reports_dir.glob(f"{prefix}_*.json"), reverse=True)
    if not candidates:
        raise SystemExit(f"No {prefix}_*.json found in {reports_dir}")
    return candidates[0]


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Report not found: {path}")
    return json.loads(path.read_text())


def _index(report: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    return {
        (r["schema"].upper(), r["table"].upper()): r
        for r in report.get("tables", [])
    }


def _compare(
    oracle_report: dict[str, Any],
    sf_report: dict[str, Any],
    show_all: bool,
) -> dict[str, Any]:
    src = _index(oracle_report)
    tgt = _index(sf_report)
    all_keys = sorted(set(src) | set(tgt))

    results, mismatches, missing_tgt, missing_src, errors = [], 0, 0, 0, 0

    for schema, table in all_keys:
        key = (schema, table)
        s = src.get(key)
        t = tgt.get(key)

        if s and s.get("error"):
            status = "ORACLE_ERROR"; errors += 1
        elif t and t.get("error"):
            status = "SNOWFLAKE_ERROR"; errors += 1
        elif s is None:
            status = "MISSING_IN_ORACLE"; missing_src += 1
        elif t is None:
            status = "MISSING_IN_SNOWFLAKE"; missing_tgt += 1
        else:
            sc = s.get("row_count") or 0
            tc = t.get("row_count") or 0
            status = "OK" if sc == tc else "MISMATCH"
            if status == "MISMATCH":
                mismatches += 1

        if show_all or status != "OK":
            results.append({
                "schema":          schema,
                "table":           table,
                "oracle_count":    (s or {}).get("row_count"),
                "snowflake_count": (t or {}).get("row_count"),
                "delta":           (
                    ((s or {}).get("row_count") or 0) - ((t or {}).get("row_count") or 0)
                    if s and t and not s.get("error") and not t.get("error")
                    else None
                ),
                "status":          status,
            })

    ok = len(all_keys) - mismatches - missing_src - missing_tgt - errors
    passed = mismatches == 0 and missing_tgt == 0 and errors == 0

    return {
        "summary": {
            "recon_run_id":       datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
            "timestamp_utc":      datetime.utcnow().isoformat() + "Z",
            "oracle_run_id":      oracle_report.get("summary", {}).get("run_id"),
            "snowflake_run_id":   sf_report.get("summary", {}).get("run_id"),
            "tables_compared":    len(all_keys),
            "tables_ok":          ok,
            "mismatches":         mismatches,
            "missing_in_snowflake": missing_tgt,
            "missing_in_oracle":  missing_src,
            "errors":             errors,
            "passed":             passed,
        },
        "tables": results,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle",    help="Oracle totals JSON (default: latest oracle_totals_*.json)")
    ap.add_argument("--snowflake", help="Snowflake totals JSON (default: latest snowflake_totals_*.json)")
    ap.add_argument("--show-all",  action="store_true")
    args = ap.parse_args()

    env = load_env()
    logs_dir    = Path(env.get("paths", {}).get("logs_dir",    "logs"))
    reports_dir = Path(env.get("paths", {}).get("reports_dir", "reports"))
    _setup_logging(logs_dir)

    oracle_path = Path(args.oracle) if args.oracle else _latest(reports_dir, "oracle_totals")
    sf_path     = Path(args.snowflake) if args.snowflake else _latest(reports_dir, "snowflake_totals")

    log.info("Oracle    report: %s", oracle_path)
    log.info("Snowflake report: %s", sf_path)

    result = _compare(_load(oracle_path), _load(sf_path), args.show_all)
    s = result["summary"]

    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / f"recon_{s['recon_run_id']}.json"
    out_path.write_text(json.dumps(result, indent=2))
    log.info("Wrote %s", out_path)

    log.info(
        "compared=%d  ok=%d  mismatches=%d  missing_snowflake=%d  errors=%d",
        s["tables_compared"], s["tables_ok"], s["mismatches"],
        s["missing_in_snowflake"], s["errors"],
    )

    for row in result["tables"]:
        if row["status"] != "OK":
            log.warning(
                "[%s] %s.%s  oracle=%s  snowflake=%s  delta=%s",
                row["status"], row["schema"], row["table"],
                row["oracle_count"], row["snowflake_count"], row["delta"],
            )

    if s["passed"]:
        log.info("RECON PASSED")
        raise SystemExit(0)
    else:
        log.error("RECON FAILED — see %s", out_path)
        raise SystemExit(1)


if __name__ == "__main__":
    main()

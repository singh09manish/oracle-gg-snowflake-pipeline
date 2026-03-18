#!/usr/bin/env python3
"""
Reconciliation Dashboard: compare Oracle source counts vs Snowflake target counts.

Compares row counts per table from Oracle (source of truth) against Snowflake
(CDC target via GoldenGate INSERTALLRECORDS) and flags mismatches.

Usage:
  # Live comparison (connects to both databases):
  python3 scripts/reconcile.py --input input/table_inventory.xlsx \
      --oracle-dsn user/pass@host:1521/service

  # Using env vars for Snowflake:
  export SNOWFLAKE_ACCOUNT=xxx SNOWFLAKE_USER=xxx SNOWFLAKE_PASSWORD=xxx
  export SNOWFLAKE_DATABASE=xxx SNOWFLAKE_WAREHOUSE=xxx
  python3 scripts/reconcile.py --input input/table_inventory.xlsx \
      --oracle-dsn user/pass@host:1521/service

  # Offline comparison from CSV exports:
  python3 scripts/reconcile.py --input input/table_inventory.xlsx \
      --from-oracle-csv oracle_counts.csv --from-snowflake-csv snowflake_counts.csv

  # Dry-run — just show which tables would be checked:
  python3 scripts/reconcile.py --input input/table_inventory.xlsx --dry-run

  # Custom mismatch threshold (default: 1%):
  python3 scripts/reconcile.py --input input/table_inventory.xlsx \
      --oracle-dsn user/pass@host:1521/service --threshold 5

Via ggctl:
  ggctl recon --oracle-dsn user/pass@host:1521/service
  ggctl recon --from-oracle-csv counts_ora.csv --from-snowflake-csv counts_sf.csv
  ggctl recon --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.inventory import read_inventory
from app.models import TableInfo

log = logging.getLogger("recon")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class TableRecon:
    """Row count comparison result for a single table."""

    def __init__(
        self,
        schema: str,
        table: str,
        oracle_count: Optional[int] = None,
        snowflake_total: Optional[int] = None,
        snowflake_active: Optional[int] = None,
    ):
        self.schema = schema.upper()
        self.table = table.upper()
        self.oracle_count = oracle_count
        self.snowflake_total = snowflake_total
        self.snowflake_active = snowflake_active

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.table}"

    def delta(self) -> Optional[int]:
        """Difference between Oracle count and Snowflake active count."""
        if self.oracle_count is not None and self.snowflake_active is not None:
            return self.oracle_count - self.snowflake_active
        return None

    def delta_pct(self) -> Optional[float]:
        """Percentage difference relative to Oracle count."""
        if self.oracle_count is not None and self.snowflake_active is not None:
            if self.oracle_count == 0:
                return 0.0 if self.snowflake_active == 0 else 100.0
            return abs(self.oracle_count - self.snowflake_active) / self.oracle_count * 100
        return None

    def status(self, threshold_pct: float = 1.0) -> str:
        """
        MATCH     — counts within threshold
        MISMATCH  — counts differ beyond threshold
        MISSING   — table not found in Snowflake
        """
        if self.snowflake_total is None and self.snowflake_active is None:
            return "MISSING"
        pct = self.delta_pct()
        if pct is None:
            return "MISSING"
        if pct <= threshold_pct:
            return "MATCH"
        return "MISMATCH"


# ---------------------------------------------------------------------------
# Oracle: get row counts
# ---------------------------------------------------------------------------

def _parse_oracle_dsn(dsn: str) -> Dict[str, Any]:
    """
    Parse DSN string:  user/password@host:port/service_name
    Returns dict with keys: user, password, host, port, service_name.
    """
    if "@" not in dsn:
        raise ValueError(
            f"Invalid Oracle DSN format: {dsn}\n"
            "  Expected: user/password@host:port/service_name"
        )
    creds, hostpart = dsn.split("@", 1)
    if "/" not in creds:
        raise ValueError("DSN must include user/password before @")
    user, password = creds.split("/", 1)

    if "/" not in hostpart:
        raise ValueError("DSN must include /service_name after host:port")
    hostport, service_name = hostpart.rsplit("/", 1)

    if ":" in hostport:
        host, port_str = hostport.split(":", 1)
        port = int(port_str)
    else:
        host = hostport
        port = 1521

    return {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "service_name": service_name,
    }


def get_oracle_counts(
    tables: List[TableInfo],
    dsn: str,
) -> Dict[str, int]:
    """
    Connect to Oracle and get row counts for each table.
    Returns dict of FQN -> count.
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError("oracledb is required for Oracle connectivity.\n  pip install oracledb")

    params = _parse_oracle_dsn(dsn)
    log.info("Connecting to Oracle: %s:%d/%s as %s",
             params["host"], params["port"], params["service_name"], params["user"])

    conn = oracledb.connect(
        user=params["user"],
        password=params["password"],
        dsn=f"{params['host']}:{params['port']}/{params['service_name']}",
    )

    counts: Dict[str, int] = {}
    cursor = conn.cursor()

    for t in tables:
        fqn = t.fqn
        try:
            # Use COUNT(*) for accuracy; for very large tables, consider SAMPLE
            cursor.execute(f'SELECT COUNT(*) FROM "{t.schema}"."{t.name}"')
            row = cursor.fetchone()
            counts[fqn] = row[0] if row else 0
            log.debug("  %s = %d", fqn, counts[fqn])
        except Exception as e:
            log.warning("  %s: Oracle count failed — %s", fqn, e)

    cursor.close()
    conn.close()
    log.info("Oracle counts collected: %d/%d tables", len(counts), len(tables))
    return counts


def load_oracle_csv(path: str) -> Dict[str, int]:
    """
    Load Oracle row counts from a CSV file.
    Expected columns: schema, table_name, row_count
    """
    counts: Dict[str, int] = {}
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize column names to lowercase
            normed = {k.strip().lower(): v.strip() for k, v in row.items() if k}
            schema = (normed.get("schema") or normed.get("owner") or "").upper()
            table = (normed.get("table_name") or normed.get("table") or "").upper()
            count_str = normed.get("row_count") or normed.get("count") or "0"
            if schema and table:
                try:
                    counts[f"{schema}.{table}"] = int(count_str)
                except ValueError:
                    counts[f"{schema}.{table}"] = 0
    log.info("Loaded %d Oracle counts from %s", len(counts), path)
    return counts


# ---------------------------------------------------------------------------
# Snowflake: get row counts
# ---------------------------------------------------------------------------

def get_snowflake_counts(
    tables: List[TableInfo],
    sf_account: Optional[str] = None,
    sf_user: Optional[str] = None,
    sf_password: Optional[str] = None,
    sf_database: Optional[str] = None,
    sf_warehouse: Optional[str] = None,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Connect to Snowflake and get both total row count and active row count
    (WHERE IS_DELETED = 0) for each table.

    Returns (total_counts, active_counts) dicts of FQN -> count.
    """
    try:
        import snowflake.connector
    except ImportError:
        raise ImportError(
            "snowflake-connector-python is required for Snowflake connectivity.\n"
            "  pip install snowflake-connector-python"
        )

    account = sf_account or os.environ.get("SNOWFLAKE_ACCOUNT")
    user = sf_user or os.environ.get("SNOWFLAKE_USER")
    password = sf_password or os.environ.get("SNOWFLAKE_PASSWORD")
    database = sf_database or os.environ.get("SNOWFLAKE_DATABASE")
    warehouse = sf_warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE")

    if not all([account, user, password, database]):
        raise ValueError(
            "Snowflake credentials required. Provide via --snowflake-dsn or env vars:\n"
            "  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, "
            "SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE"
        )

    log.info("Connecting to Snowflake: %s / %s as %s", account, database, user)

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        database=database,
        warehouse=warehouse,
    )

    total_counts: Dict[str, int] = {}
    active_counts: Dict[str, int] = {}
    cursor = conn.cursor()

    for t in tables:
        fqn = t.fqn
        target_schema = t.target_schema or t.schema

        # Total count (all operations including deletes — INSERTALLRECORDS mode)
        try:
            cursor.execute(
                f'SELECT COUNT(*) FROM "{target_schema}"."{t.name}"'
            )
            row = cursor.fetchone()
            total_counts[fqn] = row[0] if row else 0
        except Exception as e:
            log.warning("  %s: Snowflake total count failed — %s", fqn, e)

        # Active count (net active rows — exclude soft-deleted)
        try:
            cursor.execute(
                f'SELECT COUNT(*) FROM "{target_schema}"."{t.name}" '
                f'WHERE "IS_DELETED" = 0'
            )
            row = cursor.fetchone()
            active_counts[fqn] = row[0] if row else 0
        except Exception:
            # IS_DELETED column may not exist — fall back to total count
            if fqn in total_counts:
                active_counts[fqn] = total_counts[fqn]
                log.debug("  %s: no IS_DELETED column, using total count", fqn)

    cursor.close()
    conn.close()
    log.info("Snowflake counts collected: %d/%d tables", len(total_counts), len(tables))
    return total_counts, active_counts


def load_snowflake_csv(path: str) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Load Snowflake row counts from a CSV file.
    Expected columns: schema, table_name, total_count, active_count
    (active_count is optional — defaults to total_count)
    """
    total_counts: Dict[str, int] = {}
    active_counts: Dict[str, int] = {}

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            normed = {k.strip().lower(): v.strip() for k, v in row.items() if k}
            schema = (normed.get("schema") or normed.get("table_schema") or "").upper()
            table = (normed.get("table_name") or normed.get("table") or "").upper()
            total_str = normed.get("total_count") or normed.get("row_count") or normed.get("count") or "0"
            active_str = normed.get("active_count") or total_str

            if schema and table:
                fqn = f"{schema}.{table}"
                try:
                    total_counts[fqn] = int(total_str)
                except ValueError:
                    total_counts[fqn] = 0
                try:
                    active_counts[fqn] = int(active_str)
                except ValueError:
                    active_counts[fqn] = total_counts[fqn]

    log.info("Loaded %d Snowflake counts from %s", len(total_counts), path)
    return total_counts, active_counts


# ---------------------------------------------------------------------------
# Comparison engine
# ---------------------------------------------------------------------------

def compare_counts(
    tables: List[TableInfo],
    oracle_counts: Dict[str, int],
    sf_total_counts: Dict[str, int],
    sf_active_counts: Dict[str, int],
    threshold_pct: float = 1.0,
) -> List[TableRecon]:
    """Compare Oracle vs Snowflake counts and return reconciliation results."""
    results: List[TableRecon] = []

    for t in tables:
        fqn = t.fqn
        recon = TableRecon(
            schema=t.schema,
            table=t.name,
            oracle_count=oracle_counts.get(fqn),
            snowflake_total=sf_total_counts.get(fqn),
            snowflake_active=sf_active_counts.get(fqn),
        )
        results.append(recon)

    return results


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def print_results(
    results: List[TableRecon],
    threshold_pct: float,
) -> Tuple[int, int, int]:
    """Print reconciliation table and return (matched, mismatched, missing) counts."""
    matched = mismatched = missing = 0

    # Header
    header = (
        f"{'Schema':<20} {'Table':<30} {'Oracle':>12} {'SF Total':>12} "
        f"{'SF Active':>12} {'Delta':>10} {'Status':<10}"
    )
    sep = "-" * len(header)

    print()
    print("=" * len(header))
    print("RECONCILIATION RESULTS")
    print("=" * len(header))
    print(f"Threshold: {threshold_pct}%")
    print(sep)
    print(header)
    print(sep)

    for r in sorted(results, key=lambda x: (x.status(threshold_pct) != "MATCH", x.schema, x.table)):
        st = r.status(threshold_pct)
        oracle_str = str(r.oracle_count) if r.oracle_count is not None else "N/A"
        sf_total_str = str(r.snowflake_total) if r.snowflake_total is not None else "N/A"
        sf_active_str = str(r.snowflake_active) if r.snowflake_active is not None else "N/A"
        delta_str = str(r.delta()) if r.delta() is not None else "N/A"

        if st == "MATCH":
            matched += 1
        elif st == "MISSING":
            missing += 1
        else:
            mismatched += 1

        print(
            f"{r.schema:<20} {r.table:<30} {oracle_str:>12} {sf_total_str:>12} "
            f"{sf_active_str:>12} {delta_str:>10} {st:<10}"
        )

    print(sep)
    return matched, mismatched, missing


def write_csv(
    results: List[TableRecon],
    threshold_pct: float,
    output_dir: Path,
) -> Path:
    """Write results to output/recon/recon_YYYYMMDD_HHMMSS.csv and return the path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"recon_{timestamp}.csv"

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "schema", "table_name", "oracle_count", "snowflake_total",
            "snowflake_active", "delta", "delta_pct", "status",
        ])
        for r in sorted(results, key=lambda x: (x.schema, x.table)):
            st = r.status(threshold_pct)
            writer.writerow([
                r.schema,
                r.table,
                r.oracle_count if r.oracle_count is not None else "",
                r.snowflake_total if r.snowflake_total is not None else "",
                r.snowflake_active if r.snowflake_active is not None else "",
                r.delta() if r.delta() is not None else "",
                f"{r.delta_pct():.2f}" if r.delta_pct() is not None else "",
                st,
            ])

    log.info("Results written to %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="reconcile",
        description="Compare Oracle source counts vs Snowflake target counts per table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Live comparison:
  python3 scripts/reconcile.py -i input/table_inventory.xlsx \\
      --oracle-dsn user/pass@host:1521/service

  # Offline from CSV exports:
  python3 scripts/reconcile.py -i input/table_inventory.xlsx \\
      --from-oracle-csv oracle_counts.csv --from-snowflake-csv sf_counts.csv

  # Dry-run:
  python3 scripts/reconcile.py -i input/table_inventory.xlsx --dry-run
""",
    )

    ap.add_argument("--input", "-i", default=str(ROOT / "input" / "table_inventory.xlsx"),
                    help="Path to inventory Excel/CSV (default: input/table_inventory.xlsx)")
    ap.add_argument("--threshold", type=float, default=1.0,
                    help="Allowed %% difference before flagging MISMATCH (default: 1)")

    # Oracle source
    ora = ap.add_argument_group("Oracle source")
    ora.add_argument("--oracle-dsn",
                     help="Oracle DSN: user/password@host:port/service_name")
    ora.add_argument("--from-oracle-csv",
                     help="CSV with Oracle counts (offline mode, columns: schema, table_name, row_count)")

    # Snowflake target
    sf = ap.add_argument_group("Snowflake target")
    sf.add_argument("--snowflake-account", help="Snowflake account (or env SNOWFLAKE_ACCOUNT)")
    sf.add_argument("--snowflake-user", help="Snowflake user (or env SNOWFLAKE_USER)")
    sf.add_argument("--snowflake-password", help="Snowflake password (or env SNOWFLAKE_PASSWORD)")
    sf.add_argument("--snowflake-database", help="Snowflake database (or env SNOWFLAKE_DATABASE)")
    sf.add_argument("--snowflake-warehouse", help="Snowflake warehouse (or env SNOWFLAKE_WAREHOUSE)")
    sf.add_argument("--from-snowflake-csv",
                    help="CSV with Snowflake counts (offline mode, columns: schema, table_name, total_count, active_count)")

    # Options
    ap.add_argument("--dry-run", action="store_true",
                    help="Just show which tables would be checked (no DB connections)")
    ap.add_argument("--output-dir", default=str(ROOT / "output" / "recon"),
                    help="Output directory for CSV results (default: output/recon/)")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="Enable debug logging")

    return ap


def main(argv: Optional[List[str]] = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Load inventory
    tables = read_inventory(args.input)
    enabled_tables = [t for t in tables if t.enabled]
    log.info("Inventory: %d tables (%d enabled)", len(tables), len(enabled_tables))

    # -----------------------------------------------------------------------
    # Dry-run: just show what would be checked
    # -----------------------------------------------------------------------
    if args.dry_run:
        print()
        print("=" * 70)
        print("DRY RUN — Tables that would be reconciled:")
        print("=" * 70)
        print(f"{'#':<5} {'Schema':<20} {'Table':<30} {'Group':<8}")
        print("-" * 70)
        for i, t in enumerate(sorted(enabled_tables, key=lambda x: (x.schema, x.name)), 1):
            print(f"{i:<5} {t.schema:<20} {t.name:<30} EXT{t.group_id:02d}")
        print("-" * 70)
        print(f"Total: {len(enabled_tables)} tables would be checked")
        print()
        return 0

    # -----------------------------------------------------------------------
    # Get Oracle counts
    # -----------------------------------------------------------------------
    if args.from_oracle_csv:
        oracle_counts = load_oracle_csv(args.from_oracle_csv)
    elif args.oracle_dsn:
        oracle_counts = get_oracle_counts(enabled_tables, args.oracle_dsn)
    else:
        log.error(
            "No Oracle source specified. Use --oracle-dsn or --from-oracle-csv.\n"
            "  Run with --dry-run to preview tables."
        )
        return 1

    # -----------------------------------------------------------------------
    # Get Snowflake counts
    # -----------------------------------------------------------------------
    if args.from_snowflake_csv:
        sf_total, sf_active = load_snowflake_csv(args.from_snowflake_csv)
    else:
        # Check that at least account is available (from args or env)
        sf_account = args.snowflake_account or os.environ.get("SNOWFLAKE_ACCOUNT")
        if not sf_account:
            log.error(
                "No Snowflake source specified. Use --from-snowflake-csv or set env vars:\n"
                "  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, "
                "SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE"
            )
            return 1
        sf_total, sf_active = get_snowflake_counts(
            enabled_tables,
            sf_account=args.snowflake_account,
            sf_user=args.snowflake_user,
            sf_password=args.snowflake_password,
            sf_database=args.snowflake_database,
            sf_warehouse=args.snowflake_warehouse,
        )

    # -----------------------------------------------------------------------
    # Compare and report
    # -----------------------------------------------------------------------
    results = compare_counts(
        enabled_tables, oracle_counts, sf_total, sf_active, args.threshold
    )
    matched, mismatched, missing = print_results(results, args.threshold)

    # Summary
    total = matched + mismatched + missing
    print()
    print(f"SUMMARY: {matched} matched, {mismatched} mismatched, {missing} missing in Snowflake")
    print(f"         ({total} tables checked, threshold={args.threshold}%)")

    # Write CSV
    out_path = write_csv(results, args.threshold, Path(args.output_dir))
    print(f"\nResults written to: {out_path}")

    if mismatched > 0 or missing > 0:
        print(f"\nRECON FAILED — {mismatched} mismatches, {missing} missing")
        return 1

    print("\nRECON PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

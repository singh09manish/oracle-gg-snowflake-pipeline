#!/usr/bin/env python3
"""
auto_group.py  — Intelligent GoldenGate extract group assignment CLI
====================================================================

Two modes:

  1. --from-csv   : Read the Oracle query output (already exported to CSV)
                    and assign group_ids without a live DB connection.

  2. --dsn        : Connect to Oracle directly, run sql/table_grouping_candidates.sql,
                    and produce the grouped inventory in one step.

Usage examples
--------------

# Mode 1 — CSV already exported (most common for RDS / restricted access)
python3 scripts/auto_group.py \\
    --from-csv /tmp/table_grouping_candidates.csv \\
    --output   input/table_inventory.xlsx \\
    --schemas  HR SALES FINANCE \\
    --max      75 --min 50

# Mode 2 — Direct Oracle connection
python3 scripts/auto_group.py \\
    --dsn      "oracle+oracledb://user:pass@host:1521/ORCL" \\
    --schemas  HR SALES FINANCE \\
    --output   input/table_inventory.xlsx \\
    --max      75 --min 50

# Preview only — print table counts without writing the file
python3 scripts/auto_group.py \\
    --from-csv /tmp/table_grouping_candidates.csv \\
    --schemas  HR SALES \\
    --dry-run

Output Excel columns
--------------------
  schema, table_name, target_schema, group_id,
  extract_name, pump_name, replicat_name,
  num_rows, segment_mb, dml_score, has_pk, needs_keycols, partitioned
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# Allow running from project root without installing the package
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.auto_grouper import auto_assign_groups

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SQL_FILE = PROJECT_ROOT / "sql" / "table_grouping_candidates.sql"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    df = _load_data(args)

    # Filter to requested schemas if provided
    if args.schemas:
        schemas_upper = [s.upper() for s in args.schemas]
        before = len(df)
        df = df[df["OWNER"].str.upper().isin(schemas_upper)].copy()
        log.info("Schema filter: %d → %d tables", before, len(df))

    if df.empty:
        log.error("No tables found after filtering. Check --schemas or the CSV.")
        sys.exit(1)

    # Assign groups
    result = auto_assign_groups(
        df,
        min_per_group=args.min,
        max_per_group=args.max,
        schema_affinity=not args.no_schema_affinity,
        target_groups=args.groups,
    )

    # Build inventory-style output
    inventory = _build_inventory(result, args.target_schema_map)

    # Print preview table
    _print_preview(inventory)

    if args.dry_run:
        log.info("--dry-run: skipping file write.")
        return

    # Write to Excel
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    inventory.to_excel(out_path, index=False, sheet_name="table_inventory")
    log.info("✓ Inventory written → %s  (%d tables)", out_path, len(inventory))
    log.info("")
    log.info("Next step:")
    log.info("  python3 -m app.main generate --input %s", out_path)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_data(args: argparse.Namespace) -> pd.DataFrame:
    if args.from_csv:
        log.info("Loading from CSV: %s", args.from_csv)
        df = pd.read_csv(
            args.from_csv,
            skipinitialspace=True,
            on_bad_lines="warn",
        )
        # Strip whitespace from column names and string values
        df.columns = [c.strip() for c in df.columns]
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.strip()
        log.info("Loaded %d rows from CSV", len(df))
        return df

    elif args.dsn:
        return _query_oracle(args.dsn)

    else:
        log.error("Provide either --from-csv or --dsn")
        sys.exit(1)


def _query_oracle(dsn: str) -> pd.DataFrame:
    """Run the SQL file against Oracle and return a DataFrame."""
    try:
        import sqlalchemy as sa
    except ImportError:
        log.error("sqlalchemy not installed. Run: pip install sqlalchemy oracledb")
        sys.exit(1)

    sql_text = SQL_FILE.read_text()
    # Strip SQLPlus directives (SET, SPOOL) — not valid for sqlalchemy
    query_lines = [
        ln for ln in sql_text.splitlines()
        if not ln.strip().upper().startswith(("SET ", "SPOOL ", "--"))
        and ln.strip()
    ]
    query = "\n".join(query_lines).rstrip(";")

    log.info("Connecting to Oracle: %s", dsn.split("@")[-1])  # hide credentials
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    log.info("Query returned %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Build final inventory DataFrame
# ---------------------------------------------------------------------------

def _build_inventory(
    df: pd.DataFrame,
    target_schema_map: dict[str, str],
) -> pd.DataFrame:
    """
    Convert the auto-grouper output into the canonical inventory format
    that app/inventory.py expects:
        schema, table_name, target_schema, group_id
    """
    col = lambda c: c.lower()  # columns are lowercased by auto_grouper

    inv = pd.DataFrame()
    inv["schema"]         = df[col("owner")]
    inv["table_name"]     = df[col("table_name")]
    # Default target_schema = same as source schema; override via --target-schema-map
    inv["target_schema"]  = df[col("owner")].apply(
        lambda s: target_schema_map.get(s.upper(), s.upper())
    )
    inv["group_id"]       = df[col("group_id")]
    inv["extract_name"]   = df[col("extract_name")]
    inv["pump_name"]      = df[col("pump_name")]
    inv["replicat_name"]  = df[col("replicat_name")]
    # Informational columns (helpful for review, not used by generator)
    inv["num_rows"]       = df[col("num_rows")]
    inv["segment_mb"]     = df[col("segment_mb")]
    inv["dml_score"]      = df[col("dml_score")]
    inv["has_pk"]         = df[col("has_pk")]
    inv["needs_keycols"]  = df[col("needs_keycols")]
    inv["partitioned"]    = df[col("partitioned")]

    return inv.sort_values(["group_id", "schema", "table_name"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Preview
# ---------------------------------------------------------------------------

def _print_preview(inventory: pd.DataFrame) -> None:
    summary = (
        inventory.groupby(["group_id", "extract_name"])
        .agg(
            tables        = ("table_name",    "count"),
            schemas       = ("schema",         lambda x: ", ".join(sorted(x.unique()))),
            total_dml     = ("dml_score",      "sum"),
            no_pk_count   = ("needs_keycols",  "sum"),
        )
        .reset_index()
    )

    print()
    print("=" * 80)
    print(f"  EXTRACT GROUP PLAN   ({len(inventory)} tables total)")
    print("=" * 80)
    print(f"  {'Extract':<10} {'Tables':>6}  {'DML Score':>12}  {'No-PK':>5}  Schemas")
    print("  " + "-" * 76)
    for _, row in summary.iterrows():
        flag = " ⚠ KEYCOLS needed" if row["no_pk_count"] > 0 else ""
        print(
            f"  {row['extract_name']:<10} {int(row['tables']):>6}  "
            f"{row['total_dml']:>12.0f}  {int(row['no_pk_count']):>5}  "
            f"{row['schemas'][:40]}{flag}"
        )
    print("=" * 80)
    print()


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Auto-assign GoldenGate extract group_ids from Oracle table metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    src = p.add_mutually_exclusive_group()
    src.add_argument("--from-csv", metavar="FILE",
                     help="CSV exported from sql/table_grouping_candidates.sql")
    src.add_argument("--dsn", metavar="DSN",
                     help="SQLAlchemy Oracle DSN: oracle+oracledb://user:pass@host:port/sid")

    p.add_argument("--schemas",  nargs="+", metavar="SCHEMA",
                   help="Filter to these Oracle schemas (case-insensitive)")
    p.add_argument("--output",   default="input/table_inventory.xlsx",
                   help="Output Excel file (default: input/table_inventory.xlsx)")
    p.add_argument("--min",      type=int, default=50,
                   help="Min tables per group — warning threshold (default: 50)")
    p.add_argument("--max",      type=int, default=75,
                   help="Max tables per group — hard limit (default: 75)")
    p.add_argument("--groups",   type=int, default=None,
                   help="Override number of groups (default: ceil(tables/max))")
    p.add_argument("--no-schema-affinity", action="store_true",
                   help="Disable schema affinity — assign purely by DML load balance")
    p.add_argument("--target-schema-map", metavar="SRC=TGT", nargs="+", default=[],
                   help="Map source schema to different Snowflake schema. "
                        "E.g. HR=HR_PROD SALES=SALES_PROD")
    p.add_argument("--dry-run",  action="store_true",
                   help="Print the plan but do not write any files")

    args = p.parse_args()

    # Parse target schema map
    tsm: dict[str, str] = {}
    for mapping in args.target_schema_map:
        if "=" not in mapping:
            p.error(f"Invalid --target-schema-map value: '{mapping}'. Use SRC=TGT format.")
        src_s, tgt_s = mapping.split("=", 1)
        tsm[src_s.upper()] = tgt_s.upper()
    args.target_schema_map = tsm

    return args


if __name__ == "__main__":
    main()

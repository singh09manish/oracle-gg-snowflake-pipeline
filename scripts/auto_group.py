#!/usr/bin/env python3
"""
auto_group.py  — Multi-dimensional GoldenGate extract group assignment CLI
===========================================================================

Assigns tables to extract groups using profile-aware load balancing:

  OLTP_HEAVY   — high churn, constant pressure (session tables, pricing)
  OLTP_LIGHT   — active OLTP but not heavyweight (preferences, small lookups)
  BATCH_HEAVY  — bulk loads, low churn, high volume (billing, audit logs)
  BATCH_LIGHT  — periodic batch, moderate volume (monthly staging)
  REFERENCE    — barely changes (country codes, config tables)

Usage examples
--------------

# From CSV (most common — export from Oracle first)
python3 scripts/auto_group.py \\
    --from-csv /tmp/table_grouping_candidates.csv \\
    --schemas HR SALES FINANCE \\
    --max 75 --min 50 \\
    --output input/table_inventory.xlsx \\
    --dry-run

# Direct Oracle connection
python3 scripts/auto_group.py \\
    --dsn "oracle+oracledb://user:pass@host:1521/ORCL" \\
    --schemas HR SALES FINANCE \\
    --output input/table_inventory.xlsx

# Show heavy tables only (for review before grouping)
python3 scripts/auto_group.py \\
    --from-csv /tmp/table_grouping_candidates.csv \\
    --show-heavy --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.auto_grouper import auto_assign_groups, TableProfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SQL_FILE = PROJECT_ROOT / "sql" / "table_grouping_candidates.sql"


def main() -> None:
    args = _parse_args()
    df = _load_data(args)

    # Filter to requested schemas
    if args.schemas:
        schemas_upper = [s.upper() for s in args.schemas]
        before = len(df)
        # Try multiple column name variants
        owner_col = None
        for c in df.columns:
            if c.strip().upper() in ("OWNER", "SCHEMA", "SCHEMA_NAME"):
                owner_col = c
                break
        if owner_col is None:
            log.error("Cannot find OWNER/SCHEMA column. Available: %s", list(df.columns))
            sys.exit(1)
        df = df[df[owner_col].str.strip().str.upper().isin(schemas_upper)].copy()
        log.info("Schema filter: %d -> %d tables", before, len(df))

    if df.empty:
        log.error("No tables found after filtering. Check --schemas or the CSV.")
        sys.exit(1)

    # Show heavy tables before grouping (for review)
    if args.show_heavy:
        _show_heavy_tables(df)
        if args.dry_run:
            return

    # Assign groups
    result = auto_assign_groups(
        df,
        min_per_group=args.min,
        max_per_group=args.max,
        schema_affinity=not args.no_schema_affinity,
        target_groups=args.groups,
        max_batch_heavy_per_group=args.max_batch_heavy,
        max_oltp_heavy_per_group=args.max_oltp_heavy,
    )

    inventory = _build_inventory(result, args.target_schema_map)
    _print_preview(inventory)

    if args.dry_run:
        log.info("--dry-run: skipping file write.")
        return

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    inventory.to_excel(out_path, index=False, sheet_name="table_inventory")
    log.info("Inventory written -> %s  (%d tables)", out_path, len(inventory))
    log.info("")
    log.info("Next step:")
    log.info("  ggctl generate")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_data(args: argparse.Namespace) -> pd.DataFrame:
    if args.from_csv:
        log.info("Loading from CSV: %s", args.from_csv)
        df = pd.read_csv(args.from_csv, skipinitialspace=True, on_bad_lines="warn")
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
    try:
        import sqlalchemy as sa
    except ImportError:
        log.error("sqlalchemy not installed. Run: pip install sqlalchemy oracledb")
        sys.exit(1)

    sql_text = SQL_FILE.read_text()
    query_lines = [
        ln for ln in sql_text.splitlines()
        if not ln.strip().upper().startswith(("SET ", "SPOOL ", "--"))
        and ln.strip()
    ]
    query = "\n".join(query_lines).rstrip(";")

    log.info("Connecting to Oracle: %s", dsn.split("@")[-1])
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    log.info("Query returned %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Show heavy tables (for review)
# ---------------------------------------------------------------------------

def _show_heavy_tables(df: pd.DataFrame) -> None:
    """Show tables that will be classified as heavy — useful for manual review."""
    df_copy = df.copy()
    df_copy.columns = [c.lower() for c in df_copy.columns]

    # Compute scores for display
    for col in ["inserts", "updates", "deletes", "avg_row_len", "num_rows",
                "lob_count", "col_count", "churn_rate", "byte_throughput"]:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors="coerce").fillna(0)

    if "byte_throughput" not in df_copy.columns or df_copy["byte_throughput"].sum() == 0:
        df_copy["byte_throughput"] = (
            df_copy.get("inserts", 0) * df_copy.get("avg_row_len", 100)
            + df_copy.get("updates", 0) * df_copy.get("avg_row_len", 100) * 2
            + df_copy.get("deletes", 0) * 64
        )

    heavy = df_copy.nlargest(20, "byte_throughput")

    print()
    print("=" * 100)
    print("  TOP 20 HEAVIEST TABLES (by byte throughput)")
    print("=" * 100)
    print(f"  {'Owner':<15} {'Table':<30} {'Rows':>10}  {'Inserts':>10}  {'Updates':>10}  "
          f"{'Deletes':>10}  {'Byte TP':>12}  {'Churn':>6}")
    print("  " + "-" * 96)
    for _, r in heavy.iterrows():
        print(
            f"  {str(r.get('owner','')):<15} {str(r.get('table_name','')):<30} "
            f"{int(r.get('num_rows',0)):>10}  {int(r.get('inserts',0)):>10}  "
            f"{int(r.get('updates',0)):>10}  {int(r.get('deletes',0)):>10}  "
            f"{r.get('byte_throughput',0):>12,.0f}  "
            f"{r.get('churn_rate',0):>6.2f}"
        )
    print("=" * 100)
    print()


# ---------------------------------------------------------------------------
# Build inventory
# ---------------------------------------------------------------------------

def _build_inventory(
    df: pd.DataFrame,
    target_schema_map: dict[str, str],
) -> pd.DataFrame:
    col = lambda c: c.lower()

    inv = pd.DataFrame()
    inv["schema"]         = df[col("owner")]
    inv["table_name"]     = df[col("table_name")]
    inv["target_schema"]  = df[col("owner")].apply(
        lambda s: target_schema_map.get(s.upper(), s.upper())
    )
    inv["group_id"]       = df[col("group_id")]
    inv["enabled"]        = "Y"
    inv["extract_name"]   = df[col("extract_name")]
    inv["pump_name"]      = df[col("pump_name")]
    inv["replicat_name"]  = df[col("replicat_name")]
    inv["profile"]        = df[col("profile_name")]
    # Scoring columns (informational — helps with debugging & rebalancing)
    inv["gg_extract_cost"] = df[col("gg_extract_cost")].round(0).astype(int)
    inv["byte_throughput"] = df[col("byte_throughput")].round(0).astype(int)
    inv["num_rows"]       = df[col("num_rows")]
    inv["avg_row_len"]    = df[col("avg_row_len")]
    inv["segment_mb"]     = df[col("segment_mb")]
    inv["churn_rate"]     = df[col("churn_rate")]
    inv["inserts"]        = df[col("inserts")]
    inv["updates"]        = df[col("updates")]
    inv["deletes"]        = df[col("deletes")]
    inv["col_count"]      = df[col("col_count")]
    inv["lob_count"]      = df[col("lob_count")]
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
            tables        = ("table_name",      "count"),
            schemas       = ("schema",           lambda x: ", ".join(sorted(x.unique()))),
            total_cost    = ("gg_extract_cost",  "sum"),
            total_byte_tp = ("byte_throughput",  "sum"),
            no_pk_count   = ("needs_keycols",    "sum"),
            oltp_heavy    = ("profile",          lambda x: (x == "OLTP_HEAVY").sum()),
            batch_heavy   = ("profile",          lambda x: (x == "BATCH_HEAVY").sum()),
            lob_tables    = ("lob_count",        lambda x: (x.astype(int) > 0).sum()),
        )
        .reset_index()
    )

    print()
    print("=" * 110)
    print(f"  EXTRACT GROUP PLAN   ({len(inventory)} tables -> {len(summary)} groups)")
    print("=" * 110)
    print(f"  {'Extract':<8} {'Tbl':>4}  {'GG Cost':>12}  {'Byte TP':>12}  "
          f"{'OH':>3} {'BH':>3} {'LOB':>3} {'NoPK':>4}  Schemas")
    print(f"  {'':8} {'':>4}  {'':>12}  {'':>12}  "
          f"--- --- --- ----")
    print("  " + "-" * 106)

    for _, row in summary.iterrows():
        flags = []
        if row["batch_heavy"] > 2:
            flags.append("BH!")
        if row["lob_tables"] > 0 and row["total_byte_tp"] > 10_000_000:
            flags.append("LOB-MIX!")
        flag_str = f"  !! {', '.join(flags)}" if flags else ""

        print(
            f"  {row['extract_name']:<8} {int(row['tables']):>4}  "
            f"{row['total_cost']:>12,.0f}  {row['total_byte_tp']:>12,.0f}  "
            f"{int(row['oltp_heavy']):>3} {int(row['batch_heavy']):>3} "
            f"{int(row['lob_tables']):>3} {int(row['no_pk_count']):>4}  "
            f"{row['schemas'][:35]}{flag_str}"
        )

    # Profile totals
    print("  " + "-" * 106)
    profile_counts = inventory["profile"].value_counts()
    profile_str = "  ".join(f"{k}: {v}" for k, v in profile_counts.items())
    print(f"  Profiles: {profile_str}")

    # Balance metric
    costs = summary["total_cost"].tolist()
    if costs:
        imbalance = (max(costs) - min(costs)) / max(max(costs), 1) * 100
        balance_label = "GOOD" if imbalance < 30 else "FAIR" if imbalance < 50 else "POOR"
        print(f"  Balance:  {imbalance:.0f}% variance ({balance_label})")

    print("=" * 110)
    print()
    print("  Legend: OH=OLTP_HEAVY  BH=BATCH_HEAVY  LOB=tables with LOB columns  NoPK=needs KEYCOLS")
    print()


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multi-dimensional GoldenGate extract group assignment.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    src = p.add_mutually_exclusive_group()
    src.add_argument("--from-csv", metavar="FILE",
                     help="CSV exported from sql/table_grouping_candidates.sql")
    src.add_argument("--dsn", metavar="DSN",
                     help="SQLAlchemy Oracle DSN")

    p.add_argument("--schemas",  nargs="+", metavar="SCHEMA",
                   help="Filter to these schemas (case-insensitive)")
    p.add_argument("--output",   default="input/table_inventory.xlsx",
                   help="Output Excel file (default: input/table_inventory.xlsx)")
    p.add_argument("--min",      type=int, default=50,
                   help="Min tables per group (warning threshold, default: 50)")
    p.add_argument("--max",      type=int, default=75,
                   help="Max tables per group (hard limit, default: 75)")
    p.add_argument("--groups",   type=int, default=None,
                   help="Override number of groups")
    p.add_argument("--max-batch-heavy", type=int, default=2,
                   help="Max BATCH_HEAVY tables per group (default: 2)")
    p.add_argument("--max-oltp-heavy",  type=int, default=5,
                   help="Max OLTP_HEAVY tables per group (default: 5)")
    p.add_argument("--no-schema-affinity", action="store_true",
                   help="Disable schema affinity")
    p.add_argument("--target-schema-map", metavar="SRC=TGT", nargs="+", default=[],
                   help="Map source to target schema. E.g. HR=HR_PROD")
    p.add_argument("--show-heavy", action="store_true",
                   help="Show top 20 heaviest tables before grouping")
    p.add_argument("--dry-run",  action="store_true",
                   help="Print plan without writing files")

    args = p.parse_args()

    tsm: dict[str, str] = {}
    for mapping in args.target_schema_map:
        if "=" not in mapping:
            p.error(f"Invalid --target-schema-map: '{mapping}'. Use SRC=TGT.")
        src_s, tgt_s = mapping.split("=", 1)
        tsm[src_s.upper()] = tgt_s.upper()
    args.target_schema_map = tsm

    return args


if __name__ == "__main__":
    main()

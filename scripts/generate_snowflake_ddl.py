#!/usr/bin/env python3
"""
generate_snowflake_ddl.py — CLI for Snowflake DDL generation
=============================================================================

Generate CREATE TABLE statements for Snowflake from Oracle column metadata.
Each table includes GoldenGate CDC metadata columns (OP_TYPE, OP_TS, etc.).

Input modes (pick one):
  --from-csv     CSV exported from sql/oracle_table_ddl.sql
  --dsn          Direct Oracle connection (SQLAlchemy DSN)

Filtering:
  --inventory    Only generate DDL for tables listed in the Excel inventory
  --schemas      Filter to specific schemas

Output:
  One .sql file per target schema + all_tables.sql combined file.

Usage examples
--------------
# From CSV (most common — export from Oracle first)
python3 scripts/generate_snowflake_ddl.py \\
    --from-csv /tmp/oracle_columns.csv \\
    --schemas HR SALES \\
    --output output/snowflake/ddl/

# With inventory filter (only tables in the Excel)
python3 scripts/generate_snowflake_ddl.py \\
    --from-csv /tmp/oracle_columns.csv \\
    --inventory input/table_inventory.xlsx \\
    --output output/snowflake/ddl/

# Direct Oracle connection
python3 scripts/generate_snowflake_ddl.py \\
    --dsn "oracle+oracledb://user:pass@host:1521/ORCL" \\
    --inventory input/table_inventory.xlsx

# Dry run — print DDL without writing files
python3 scripts/generate_snowflake_ddl.py \\
    --from-csv /tmp/oracle_columns.csv \\
    --schemas HR \\
    --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.ddl_generator import (
    ColumnInfo,
    generate_ddl,
    get_filter_tables_from_inventory,
    get_target_schema_map_from_inventory,
    read_columns_from_csv,
    write_ddl_files,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SQL_FILE = PROJECT_ROOT / "sql" / "oracle_table_ddl.sql"


def main() -> None:
    args = _parse_args()

    # -----------------------------------------------------------------
    # 1. Load column metadata
    # -----------------------------------------------------------------
    columns = _load_columns(args)

    # -----------------------------------------------------------------
    # 2. Load inventory (optional — for filtering + target schema map)
    # -----------------------------------------------------------------
    filter_schemas = None
    filter_tables = None
    target_schema_map = None

    if args.schemas:
        filter_schemas = {s.upper() for s in args.schemas}
        log.info("Schema filter: %s", ", ".join(sorted(filter_schemas)))

    if args.inventory:
        from app.inventory import read_inventory

        inv_path = Path(args.inventory)
        if not inv_path.exists():
            log.error("Inventory file not found: %s", inv_path)
            sys.exit(1)

        tables = read_inventory(inv_path)
        filter_tables = get_filter_tables_from_inventory(tables)
        target_schema_map = get_target_schema_map_from_inventory(tables)
        log.info(
            "Inventory loaded: %d enabled tables, target schemas: %s",
            len(filter_tables),
            ", ".join(f"{k}->{v}" for k, v in sorted(target_schema_map.items())),
        )

        # If --schemas also provided, intersect: only inventory tables in those schemas
        if filter_schemas:
            filter_tables = {
                fqn for fqn in filter_tables
                if fqn.split(".")[0] in filter_schemas
            }
            log.info(
                "After schema intersection: %d tables remain", len(filter_tables)
            )

    # -----------------------------------------------------------------
    # 3. Generate DDL
    # -----------------------------------------------------------------
    ddl_by_schema, warnings = generate_ddl(
        columns,
        filter_schemas=filter_schemas if not filter_tables else None,
        filter_tables=filter_tables,
        target_schema_map=target_schema_map,
    )

    # Print warnings
    if warnings:
        log.warning("Data type warnings (%d):", len(warnings))
        for w in warnings:
            log.warning("  %s", w)

    # -----------------------------------------------------------------
    # 4. Output
    # -----------------------------------------------------------------
    if args.dry_run:
        print()
        for schema, ddl_text in sorted(ddl_by_schema.items()):
            print(ddl_text)
            print()
        _print_summary(ddl_by_schema, warnings)
        log.info("--dry-run: no files written.")
        return

    output_dir = Path(args.output)
    files = write_ddl_files(ddl_by_schema, output_dir)

    _print_summary(ddl_by_schema, warnings)
    print()
    log.info("Files written:")
    for f in files:
        log.info("  %s", f)
    print()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_columns(args: argparse.Namespace) -> list[ColumnInfo]:
    """Load Oracle column metadata from CSV or direct Oracle connection."""

    if args.from_csv:
        log.info("Loading columns from CSV: %s", args.from_csv)
        return read_columns_from_csv(args.from_csv)

    elif args.dsn:
        return _query_oracle(args.dsn)

    else:
        log.error("Provide either --from-csv or --dsn")
        sys.exit(1)


def _query_oracle(dsn: str) -> list[ColumnInfo]:
    """Query Oracle directly using the SQL from oracle_table_ddl.sql."""
    try:
        import sqlalchemy as sa
    except ImportError:
        log.error("sqlalchemy not installed. Run: pip install sqlalchemy oracledb")
        sys.exit(1)

    # Read and clean the SQL file (strip SQL*Plus directives)
    sql_text = SQL_FILE.read_text()
    query_lines = [
        ln for ln in sql_text.splitlines()
        if not ln.strip().upper().startswith(("SET ", "SPOOL ", "--"))
        and ln.strip()
    ]
    query = "\n".join(query_lines).rstrip(";")

    log.info("Connecting to Oracle: %s", dsn.split("@")[-1] if "@" in dsn else dsn)
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        result = conn.execute(sa.text(query))
        rows = result.fetchall()
        col_names = list(result.keys())

    log.info("Query returned %d rows", len(rows))

    columns = []
    for row in rows:
        row_dict = dict(zip(col_names, row))
        columns.append(ColumnInfo(
            owner=str(row_dict.get("owner", "") or ""),
            table_name=str(row_dict.get("table_name", "") or ""),
            column_name=str(row_dict.get("column_name", "") or ""),
            data_type=str(row_dict.get("data_type", "") or ""),
            data_length=row_dict.get("data_length", 0),
            data_precision=row_dict.get("data_precision"),
            data_scale=row_dict.get("data_scale"),
            nullable=str(row_dict.get("nullable", "Y") or "Y"),
            column_id=row_dict.get("column_id", 0),
        ))

    return columns


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _print_summary(ddl_by_schema: dict, warnings: list) -> None:
    """Print a summary table of what was generated."""
    total_tables = 0
    print()
    print("=" * 60)
    print("  SNOWFLAKE DDL GENERATION SUMMARY")
    print("=" * 60)
    print(f"  {'Target Schema':<30} {'Tables':>8}")
    print("  " + "-" * 42)
    for schema, ddl_text in sorted(ddl_by_schema.items()):
        count = ddl_text.count("CREATE TABLE IF NOT EXISTS")
        total_tables += count
        print(f"  {schema:<30} {count:>8}")
    print("  " + "-" * 42)
    print(f"  {'TOTAL':<30} {total_tables:>8}")
    if warnings:
        print(f"\n  Warnings: {len(warnings)} (unmapped data types)")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate Snowflake CREATE TABLE DDL from Oracle metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 scripts/generate_snowflake_ddl.py --from-csv /tmp/oracle_columns.csv\n"
            "  python3 scripts/generate_snowflake_ddl.py --from-csv /tmp/oracle_columns.csv --inventory input/table_inventory.xlsx\n"
            "  python3 scripts/generate_snowflake_ddl.py --dsn 'oracle+oracledb://user:pass@host:1521/ORCL' --schemas HR\n"
        ),
    )

    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "--from-csv", metavar="FILE",
        help="CSV exported from sql/oracle_table_ddl.sql",
    )
    src.add_argument(
        "--dsn", metavar="DSN",
        help="SQLAlchemy Oracle DSN (e.g. oracle+oracledb://user:pass@host:1521/ORCL)",
    )

    p.add_argument(
        "--inventory", metavar="FILE",
        help="Only generate DDL for tables in this inventory Excel/CSV "
             "(uses target_schema mapping from inventory)",
    )
    p.add_argument(
        "--schemas", nargs="+", metavar="SCHEMA",
        help="Filter to these schemas (case-insensitive)",
    )
    p.add_argument(
        "--output", default="output/snowflake/ddl/",
        help="Output directory (default: output/snowflake/ddl/)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print DDL to stdout without writing files",
    )

    args = p.parse_args()

    if not args.from_csv and not args.dsn:
        p.error("Provide either --from-csv or --dsn")

    return args


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
CLI for the Oracle → GoldenGate → Snowflake pipeline generator.

Commands:
  generate   Read Excel inventory → generate all GG param files (zero manual work)
  validate   Pre-flight check: connect to Oracle, verify PKs + trandata
  report     Print group assignment report (no files written)

Examples:
  # Generate everything from an Excel file:
  python3 -m app.main generate --input input/table_inventory.xlsx

  # Validate Oracle readiness (PKs, supplemental logging):
  python3 -m app.main validate --input input/tables.xlsx

  # Just see the group report (no files written):
  python3 -m app.main report --input input/tables.xlsx
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler()],
    )


# -------------------------------------------------------------------
# Commands
# -------------------------------------------------------------------

def cmd_generate(args: argparse.Namespace) -> int:
    from app.generator import Generator
    from app.grouper import group_tables
    from app.inventory import read_inventory

    tables = read_inventory(args.input)
    groups = group_tables(
        tables,
        max_per_group=args.max_per_group,
    )
    gen = Generator(groups)
    manifest = gen.generate_all()

    print()
    print("=" * 100)
    print("PIPELINE GENERATION COMPLETE")
    print("=" * 100)
    for line in manifest.summary_lines():
        print(line)
    print()

    if manifest.warnings:
        return 1
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    from app.inventory import read_inventory
    from app.validator import OracleValidator

    tables = read_inventory(args.input)

    # Load env config
    from audit.config import load_env
    env = load_env()

    validator = OracleValidator(env)
    result = validator.validate(tables)

    print()
    print("=" * 80)
    print("ORACLE PRE-FLIGHT VALIDATION")
    print("=" * 80)
    for line in result.summary_lines():
        print(line)
    print()

    reports_dir = ROOT / "reports"
    validator.save_report(result, reports_dir)
    return 0 if result.passed else 1


def cmd_report(args: argparse.Namespace) -> int:
    from app.grouper import group_tables
    from app.inventory import read_inventory
    from app.models import PipelineManifest

    tables = read_inventory(args.input)
    groups = group_tables(
        tables,
        max_per_group=args.max_per_group,
    )

    manifest = PipelineManifest(
        total_tables=sum(g.table_count for g in groups),
        total_groups=len(groups),
        groups=groups,
    )

    print()
    print("=" * 100)
    print("GROUP ASSIGNMENT REPORT (dry-run — no files written)")
    print("=" * 100)
    for line in manifest.summary_lines():
        print(line)
    print()

    # Print per-group table list
    for g in groups:
        print(f"\n── {g.extract_name} / {g.pump_name} / {g.replicat_name} ──")
        print(f"   Extract trail: {g.extract_trail}*    Pump trail: {g.pump_trail}*")
        print(f"   Tables ({g.table_count}):")
        for t in g.tables:
            target = f" → {t.target_schema}.{t.name}" if t.target_schema != t.schema else ""
            print(f"     {t.fqn}{target}")
    return 0


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        prog="gg-pipeline",
        description="Oracle → GoldenGate → Snowflake pipeline generator",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    # --- generate ---
    p_gen = sub.add_parser("generate", help="Generate all GG parameter files from inventory")
    p_gen.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")
    p_gen.add_argument("--max-per-group", type=int, default=75,
                       help="Advisory max tables per group — warns if exceeded (default: 75)")

    # --- validate ---
    p_val = sub.add_parser("validate", help="Pre-flight Oracle checks (PKs, trandata)")
    p_val.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")

    # --- report ---
    p_rep = sub.add_parser("report", help="Print group assignment report (no files written)")
    p_rep.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")
    p_rep.add_argument("--max-per-group", type=int, default=75)

    args = parser.parse_args()
    _setup_logging(args.verbose)

    commands = {
        "generate": cmd_generate,
        "validate": cmd_validate,
        "report":   cmd_report,
    }
    sys.exit(commands[args.command](args))


if __name__ == "__main__":
    main()

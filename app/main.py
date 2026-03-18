#!/usr/bin/env python3
"""
CLI for the Oracle → GoldenGate → Snowflake pipeline generator.

Commands:
  generate   Read Excel inventory → generate all GG param files (zero manual work)
  validate   Pre-flight check: connect to Oracle, verify PKs + trandata
  report     Print group assignment report (no files written)
  disable    Disable a table in the inventory (exclude from .prm generation)
  enable     Re-enable a previously disabled table
  status     Show inventory status — enabled/disabled counts per group

Examples:
  # Generate everything from an Excel file:
  python3 -m app.main generate --input input/table_inventory.xlsx

  # Disable a failing table:
  python3 -m app.main disable --input input/table_inventory.xlsx \
      --table HR.EMPLOYEES --reason "OGG-01004: no supplemental logging"

  # Re-enable it after fix:
  python3 -m app.main enable --input input/table_inventory.xlsx --table HR.EMPLOYEES

  # Check overall status:
  python3 -m app.main status --input input/table_inventory.xlsx

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


def cmd_disable(args: argparse.Namespace) -> int:
    """Disable one or more tables in the inventory."""
    from app.inventory import read_inventory, write_inventory, toggle_table

    tables = read_inventory(args.input)

    errors = 0
    for fqn in args.table:
        try:
            toggle_table(tables, fqn, enable=False, reason=args.reason)
        except KeyError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            errors += 1

    if errors:
        return 1

    write_inventory(tables, args.input)

    # Show what's now disabled in this group
    disabled = [t for t in tables if not t.enabled]
    if disabled:
        print(f"\nCurrently disabled ({len(disabled)} tables):")
        for t in sorted(disabled, key=lambda x: (x.group_id, x.fqn)):
            print(f"  EXT{t.group_id:02d}  {t.fqn:<40s}  {t.disabled_reason}")

    print(f"\nIMPORTANT: Regenerate configs to apply:")
    print(f"  python3 -m app.main generate --input {args.input}")
    return 0


def cmd_enable(args: argparse.Namespace) -> int:
    """Re-enable one or more tables in the inventory."""
    from app.inventory import read_inventory, write_inventory, toggle_table

    tables = read_inventory(args.input)

    errors = 0
    for fqn in args.table:
        try:
            toggle_table(tables, fqn, enable=True)
        except KeyError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            errors += 1

    if errors:
        return 1

    write_inventory(tables, args.input)

    print(f"\nIMPORTANT: Regenerate configs to apply:")
    print(f"  python3 -m app.main generate --input {args.input}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    """Show inventory status — enabled/disabled counts per group."""
    from app.inventory import read_inventory

    tables = read_inventory(args.input)

    # Group by group_id
    from collections import defaultdict
    groups: dict[int, dict] = defaultdict(lambda: {"enabled": [], "disabled": []})
    for t in tables:
        key = "enabled" if t.enabled else "disabled"
        groups[t.group_id][key].append(t)

    total_enabled = sum(1 for t in tables if t.enabled)
    total_disabled = sum(1 for t in tables if not t.enabled)

    print()
    print("=" * 90)
    print(f"INVENTORY STATUS  —  {len(tables)} tables  |  "
          f"{total_enabled} enabled  |  {total_disabled} disabled")
    print("=" * 90)
    print(f"  {'Group':<8}  {'Extract':<10}  {'Enabled':>8}  {'Disabled':>9}  {'Total':>6}  Disabled Tables")
    print("  " + "-" * 84)

    for gid in sorted(groups):
        g = groups[gid]
        en = len(g["enabled"])
        dis = len(g["disabled"])
        dis_names = ", ".join(t.fqn for t in g["disabled"][:3])
        if len(g["disabled"]) > 3:
            dis_names += f" (+{len(g['disabled']) - 3} more)"
        print(f"  {gid:<8}  EXT{gid:02d}      {en:>8}  {dis:>9}  {en + dis:>6}  {dis_names}")

    # Detailed disabled table list
    disabled = [t for t in tables if not t.enabled]
    if disabled:
        print()
        print("  DISABLED TABLES:")
        print(f"  {'Table':<40s}  {'Group':<8}  {'Reason':<30s}  {'Since'}")
        print("  " + "-" * 84)
        for t in sorted(disabled, key=lambda x: (x.group_id, x.fqn)):
            print(f"  {t.fqn:<40s}  EXT{t.group_id:02d}    {t.disabled_reason:<30s}  {t.disabled_at}")

    print()
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

    # --- disable ---
    p_dis = sub.add_parser(
        "disable",
        help="Disable table(s) — exclude from .prm generation",
        description="Disable one or more tables in the inventory. "
                    "Disabled tables are excluded when you next run 'generate'.",
    )
    p_dis.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")
    p_dis.add_argument("--table", "-t", required=True, nargs="+",
                       help="Table FQN(s) to disable (SCHEMA.TABLE). Multiple allowed.")
    p_dis.add_argument("--reason", "-r", default="manually disabled",
                       help="Reason for disabling (stored in inventory)")

    # --- enable ---
    p_en = sub.add_parser(
        "enable",
        help="Re-enable previously disabled table(s)",
        description="Re-enable one or more tables. Run 'generate' after to update .prm files.",
    )
    p_en.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")
    p_en.add_argument("--table", "-t", required=True, nargs="+",
                      help="Table FQN(s) to enable (SCHEMA.TABLE)")

    # --- status ---
    p_st = sub.add_parser("status", help="Show inventory status — enabled/disabled counts per group")
    p_st.add_argument("--input", "-i", required=True, help="Path to .xlsx or .csv inventory")

    args = parser.parse_args()
    _setup_logging(args.verbose)

    commands = {
        "generate":  cmd_generate,
        "validate":  cmd_validate,
        "report":    cmd_report,
        "disable":   cmd_disable,
        "enable":    cmd_enable,
        "status":    cmd_status,
    }
    sys.exit(commands[args.command](args))


if __name__ == "__main__":
    main()

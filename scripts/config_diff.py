#!/usr/bin/env python3
"""
Config Drift Detection: compare inventory vs generated vs deployed GG configs.

Detects three categories of drift:
  1. Inventory vs Generated — Did you run 'ggctl generate' after changing inventory?
  2. Generated vs Deployed  — Did you run 'ggctl deploy' after generating?
  3. Full                   — Inventory -> Generated -> Deployed (all checks)

Usage:
  # Check inventory vs generated configs (no GG access needed):
  python3 scripts/config_diff.py --input input/table_inventory.xlsx --level inventory

  # Check generated vs deployed (requires GG access):
  python3 scripts/config_diff.py --input input/table_inventory.xlsx --level deployed \
      --gg19c-home /u01/app/oracle/product/19c/oggcore_1 \
      --gg21c-home /u01/app/oracle/product/21c/oggbd_1

  # Full check (all levels):
  python3 scripts/config_diff.py --input input/table_inventory.xlsx --level full \
      --gg19c-home /path/to/gg19c --gg21c-home /path/to/gg21c

Via ggctl:
  ggctl diff                         # defaults to --level inventory
  ggctl diff --level deployed
  ggctl diff --level full
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.inventory import read_inventory
from app.models import TableInfo

log = logging.getLogger("config_diff")

OUTPUT_DIR = ROOT / "output"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class DiffResult:
    """Collection of all detected drifts."""

    # Inventory vs Generated
    tables_in_inventory_not_generated: List[str] = field(default_factory=list)
    tables_in_generated_not_inventory: List[str] = field(default_factory=list)
    disabled_in_inventory_but_in_generated: List[str] = field(default_factory=list)
    groups_needing_regeneration: List[int] = field(default_factory=list)

    # Generated vs Deployed
    extracts_generated_not_deployed: List[str] = field(default_factory=list)
    pumps_generated_not_deployed: List[str] = field(default_factory=list)
    replicats_generated_not_deployed: List[str] = field(default_factory=list)
    orphan_extracts: List[str] = field(default_factory=list)
    orphan_pumps: List[str] = field(default_factory=list)
    orphan_replicats: List[str] = field(default_factory=list)

    # GG process status
    gg19c_processes: Dict[str, str] = field(default_factory=dict)
    gg21c_processes: Dict[str, str] = field(default_factory=dict)

    @property
    def has_diffs(self) -> bool:
        return bool(
            self.tables_in_inventory_not_generated
            or self.tables_in_generated_not_inventory
            or self.disabled_in_inventory_but_in_generated
            or self.groups_needing_regeneration
            or self.extracts_generated_not_deployed
            or self.pumps_generated_not_deployed
            or self.replicats_generated_not_deployed
            or self.orphan_extracts
            or self.orphan_pumps
            or self.orphan_replicats
        )


# ---------------------------------------------------------------------------
# Parse inventory: extract expected groups + tables
# ---------------------------------------------------------------------------

def get_inventory_state(
    tables: List[TableInfo],
) -> Tuple[Dict[int, Set[str]], Dict[int, Set[str]]]:
    """
    From the inventory, derive:
      enabled_groups:  {group_id: set of FQN} for enabled tables
      disabled_tables: {group_id: set of FQN} for disabled tables

    Returns (enabled_groups, disabled_groups).
    """
    enabled_groups: Dict[int, Set[str]] = {}
    disabled_groups: Dict[int, Set[str]] = {}

    for t in tables:
        target = enabled_groups if t.enabled else disabled_groups
        target.setdefault(t.group_id, set()).add(t.fqn)

    return enabled_groups, disabled_groups


# ---------------------------------------------------------------------------
# Parse generated .prm files: extract tables per group
# ---------------------------------------------------------------------------

def parse_prm_tables(prm_path: Path) -> Set[str]:
    """
    Parse a .prm file and extract TABLE/MAP statements to find which
    tables are included.

    Handles patterns like:
      TABLE HR.EMPLOYEES;
      MAP SCHEMA.TABLE, TARGET SCHEMA.TABLE;
    """
    tables: Set[str] = set()
    if not prm_path.exists():
        return tables

    content = prm_path.read_text()

    # GG 19c extract/pump: TABLE schema.table
    for match in re.finditer(
        r'^\s*TABLE\s+([A-Za-z_]\w*\.[A-Za-z_]\w*)',
        content, re.MULTILINE | re.IGNORECASE,
    ):
        tables.add(match.group(1).upper())

    # GG 21c replicat: MAP schema.table, TARGET schema.table
    for match in re.finditer(
        r'^\s*MAP\s+([A-Za-z_]\w*\.[A-Za-z_]\w*)',
        content, re.MULTILINE | re.IGNORECASE,
    ):
        tables.add(match.group(1).upper())

    return tables


def get_generated_state() -> Tuple[Dict[int, Set[str]], Set[str], Set[str], Set[str]]:
    """
    Scan output/ directory for generated .prm files and extract table lists.

    Returns:
      generated_groups: {group_id: set of FQN} from extract .prm files
      extract_names:    set of extract process names (EXT01, EXT02, ...)
      pump_names:       set of pump process names (PMP01, PMP02, ...)
      replicat_names:   set of replicat process names (REP01, REP02, ...)
    """
    generated_groups: Dict[int, Set[str]] = {}
    extract_names: Set[str] = set()
    pump_names: Set[str] = set()
    replicat_names: Set[str] = set()

    # GG 19c extracts and pumps
    gg19c_dir = OUTPUT_DIR / "gg19c" / "dirprm"
    if gg19c_dir.exists():
        for prm_file in sorted(gg19c_dir.glob("EXT*.prm")):
            name = prm_file.stem.upper()
            extract_names.add(name)
            # Parse group_id from name: EXT01 -> 1
            match = re.match(r'EXT(\d+)', name)
            if match:
                gid = int(match.group(1))
                tables = parse_prm_tables(prm_file)
                generated_groups[gid] = tables

        for prm_file in sorted(gg19c_dir.glob("PMP*.prm")):
            pump_names.add(prm_file.stem.upper())

    # GG 21c replicats
    gg21c_dir = OUTPUT_DIR / "gg21c" / "dirprm"
    if gg21c_dir.exists():
        for prm_file in sorted(gg21c_dir.glob("REP*.prm")):
            name = prm_file.stem.upper()
            replicat_names.add(name)
            # Also parse tables from replicat MAP statements
            match = re.match(r'REP(\d+)', name)
            if match:
                gid = int(match.group(1))
                tables = parse_prm_tables(prm_file)
                # Merge with extract tables if both exist
                if gid in generated_groups:
                    generated_groups[gid] |= tables
                else:
                    generated_groups[gid] = tables

    return generated_groups, extract_names, pump_names, replicat_names


# ---------------------------------------------------------------------------
# Parse GG deployed state: query GGSCI for running processes
# ---------------------------------------------------------------------------

def run_ggsci(gg_home: str, command: str) -> str:
    """
    Run a GGSCI command and return its output.
    Returns empty string on failure.
    """
    ggsci_path = os.path.join(gg_home, "ggsci")
    if not os.path.isfile(ggsci_path):
        log.warning("ggsci not found at: %s", ggsci_path)
        return ""

    try:
        result = subprocess.run(
            [ggsci_path],
            input=command + "\nEXIT\n",
            capture_output=True,
            text=True,
            timeout=30,
            cwd=gg_home,
            env={
                **os.environ,
                "LD_LIBRARY_PATH": os.path.join(gg_home, "lib") + ":" + os.environ.get("LD_LIBRARY_PATH", ""),
            },
        )
        return result.stdout
    except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError) as e:
        log.warning("Failed to run ggsci: %s", e)
        return ""


def parse_info_output(output: str) -> Dict[str, str]:
    """
    Parse GGSCI 'INFO EXTRACT *' or 'INFO REPLICAT *' output.

    Typical output format:
      EXTRACT    EXT01     Last Started 2024-01-15 10:30   Status RUNNING
      EXTRACT    PMP01     Last Started 2024-01-15 10:30   Status RUNNING
      REPLICAT   REP01     Last Started 2024-01-15 10:31   Status RUNNING

    Returns dict of {process_name: status}.
    """
    processes: Dict[str, str] = {}
    if not output:
        return processes

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue

        # Match: EXTRACT/REPLICAT  NAME  ... Status STATUS
        match = re.match(
            r'(EXTRACT|REPLICAT)\s+(\w+)\s+.*Status\s+(\w+)',
            line, re.IGNORECASE,
        )
        if match:
            proc_name = match.group(2).upper()
            status = match.group(3).upper()
            processes[proc_name] = status
            continue

        # Alternative format: EXTRACT/REPLICAT NAME ... RUNNING/STOPPED/ABENDED
        match = re.match(
            r'(EXTRACT|REPLICAT)\s+(\w+)\s+.*(RUNNING|STOPPED|ABENDED)',
            line, re.IGNORECASE,
        )
        if match:
            proc_name = match.group(2).upper()
            status = match.group(3).upper()
            processes[proc_name] = status

    return processes


def get_deployed_state(
    gg19c_home: Optional[str] = None,
    gg21c_home: Optional[str] = None,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Query GGSCI to find what processes are actually deployed and running.

    Returns:
      gg19c_processes: {process_name: status} for GG 19c
      gg21c_processes: {process_name: status} for GG 21c
    """
    gg19c_procs: Dict[str, str] = {}
    gg21c_procs: Dict[str, str] = {}

    if gg19c_home:
        log.info("Checking GG 19c deployed state: %s", gg19c_home)
        output = run_ggsci(gg19c_home, "INFO ALL")
        gg19c_procs = parse_info_output(output)
        if gg19c_procs:
            log.info("  Found %d processes in GG 19c", len(gg19c_procs))
        else:
            log.warning("  No processes found in GG 19c (or ggsci unavailable)")

    if gg21c_home:
        log.info("Checking GG 21c deployed state: %s", gg21c_home)
        output = run_ggsci(gg21c_home, "INFO ALL")
        gg21c_procs = parse_info_output(output)
        if gg21c_procs:
            log.info("  Found %d processes in GG 21c", len(gg21c_procs))
        else:
            log.warning("  No processes found in GG 21c (or ggsci unavailable)")

    return gg19c_procs, gg21c_procs


# ---------------------------------------------------------------------------
# Comparison: Inventory vs Generated
# ---------------------------------------------------------------------------

def diff_inventory_vs_generated(
    tables: List[TableInfo],
    result: DiffResult,
) -> None:
    """Compare what inventory says should exist vs what's actually generated."""
    enabled_groups, disabled_groups = get_inventory_state(tables)
    generated_groups, _, _, _ = get_generated_state()

    # All enabled FQNs from inventory
    inv_enabled_fqns: Set[str] = set()
    for fqns in enabled_groups.values():
        inv_enabled_fqns |= fqns

    # All disabled FQNs from inventory
    inv_disabled_fqns: Set[str] = set()
    for fqns in disabled_groups.values():
        inv_disabled_fqns |= fqns

    # All FQNs in generated .prm files
    gen_fqns: Set[str] = set()
    for fqns in generated_groups.values():
        gen_fqns |= fqns

    # Tables in inventory (enabled) but not in any generated .prm
    for fqn in sorted(inv_enabled_fqns - gen_fqns):
        result.tables_in_inventory_not_generated.append(fqn)

    # Tables in generated .prm but not in inventory (enabled)
    # Could be orphan tables from a previous inventory version
    for fqn in sorted(gen_fqns - inv_enabled_fqns):
        result.tables_in_generated_not_inventory.append(fqn)

    # Tables disabled in inventory but still present in generated .prm
    for fqn in sorted(inv_disabled_fqns & gen_fqns):
        result.disabled_in_inventory_but_in_generated.append(fqn)

    # Groups that need regeneration: any group with differences
    all_group_ids = sorted(set(enabled_groups.keys()) | set(generated_groups.keys()))
    for gid in all_group_ids:
        inv_tables = enabled_groups.get(gid, set())
        gen_tables = generated_groups.get(gid, set())
        if inv_tables != gen_tables:
            result.groups_needing_regeneration.append(gid)


# ---------------------------------------------------------------------------
# Comparison: Generated vs Deployed
# ---------------------------------------------------------------------------

def diff_generated_vs_deployed(
    gg19c_home: Optional[str],
    gg21c_home: Optional[str],
    result: DiffResult,
) -> None:
    """Compare what's been generated vs what's actually deployed in GG."""
    _, extract_names, pump_names, replicat_names = get_generated_state()
    gg19c_procs, gg21c_procs = get_deployed_state(gg19c_home, gg21c_home)

    result.gg19c_processes = gg19c_procs
    result.gg21c_processes = gg21c_procs

    # Deployed extract/pump names from GG 19c
    deployed_19c = set(gg19c_procs.keys())

    # Deployed replicat names from GG 21c
    deployed_21c = set(gg21c_procs.keys())

    # Extracts generated but not deployed
    for name in sorted(extract_names - deployed_19c):
        result.extracts_generated_not_deployed.append(name)

    # Pumps generated but not deployed
    for name in sorted(pump_names - deployed_19c):
        result.pumps_generated_not_deployed.append(name)

    # Replicats generated but not deployed
    for name in sorted(replicat_names - deployed_21c):
        result.replicats_generated_not_deployed.append(name)

    # Orphans: processes in GG but not in generated configs
    # Only flag EXT*/PMP*/REP* processes (ignore MGR etc.)
    for name in sorted(deployed_19c):
        if name.startswith("EXT") and name not in extract_names:
            result.orphan_extracts.append(name)
        elif name.startswith("PMP") and name not in pump_names:
            result.orphan_pumps.append(name)

    for name in sorted(deployed_21c):
        if name.startswith("REP") and name not in replicat_names:
            result.orphan_replicats.append(name)


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def print_diff(result: DiffResult, level: str) -> None:
    """Print a formatted diff report."""
    w = 80
    print()
    print("=" * w)
    print("CONFIG DRIFT DETECTION REPORT")
    print("=" * w)
    print(f"Level: {level.upper()}")
    print()

    has_any = False

    # --- Inventory vs Generated ---
    if level in ("inventory", "full"):
        print("-" * w)
        print("INVENTORY vs GENERATED")
        print("-" * w)

        if result.tables_in_inventory_not_generated:
            has_any = True
            print(f"\n  Tables in inventory but NOT in generated .prm files "
                  f"({len(result.tables_in_inventory_not_generated)}):")
            print("  (Action: run 'ggctl generate' to create .prm files)")
            for fqn in result.tables_in_inventory_not_generated:
                print(f"    + {fqn}")

        if result.tables_in_generated_not_inventory:
            has_any = True
            print(f"\n  Tables in generated .prm files but NOT in inventory "
                  f"({len(result.tables_in_generated_not_inventory)}):")
            print("  (These may be orphans from a previous inventory version)")
            for fqn in result.tables_in_generated_not_inventory:
                print(f"    - {fqn}")

        if result.disabled_in_inventory_but_in_generated:
            has_any = True
            print(f"\n  Tables DISABLED in inventory but still in generated .prm files "
                  f"({len(result.disabled_in_inventory_but_in_generated)}):")
            print("  (Action: run 'ggctl generate' to remove disabled tables)")
            for fqn in result.disabled_in_inventory_but_in_generated:
                print(f"    ! {fqn}")

        if result.groups_needing_regeneration:
            has_any = True
            print(f"\n  Groups needing regeneration ({len(result.groups_needing_regeneration)}):")
            for gid in result.groups_needing_regeneration:
                print(f"    * EXT{gid:02d} / PMP{gid:02d} / REP{gid:02d}")

        if not has_any and level == "inventory":
            print("\n  No drift detected. Inventory matches generated configs.")

    # --- Generated vs Deployed ---
    if level in ("deployed", "full"):
        print()
        print("-" * w)
        print("GENERATED vs DEPLOYED")
        print("-" * w)

        deployed_diff = False

        if result.extracts_generated_not_deployed:
            deployed_diff = True
            has_any = True
            print(f"\n  Extracts generated but NOT deployed in GG 19c "
                  f"({len(result.extracts_generated_not_deployed)}):")
            print("  (Action: run 'ggctl deploy 19c')")
            for name in result.extracts_generated_not_deployed:
                print(f"    + {name}")

        if result.pumps_generated_not_deployed:
            deployed_diff = True
            has_any = True
            print(f"\n  Pumps generated but NOT deployed in GG 19c "
                  f"({len(result.pumps_generated_not_deployed)}):")
            print("  (Action: run 'ggctl deploy 19c')")
            for name in result.pumps_generated_not_deployed:
                print(f"    + {name}")

        if result.replicats_generated_not_deployed:
            deployed_diff = True
            has_any = True
            print(f"\n  Replicats generated but NOT deployed in GG 21c "
                  f"({len(result.replicats_generated_not_deployed)}):")
            print("  (Action: run 'ggctl deploy 21c')")
            for name in result.replicats_generated_not_deployed:
                print(f"    + {name}")

        if result.orphan_extracts:
            deployed_diff = True
            has_any = True
            print(f"\n  ORPHAN extracts in GG 19c (deployed but not in inventory) "
                  f"({len(result.orphan_extracts)}):")
            print("  (These processes exist in GG but have no corresponding inventory entry)")
            for name in result.orphan_extracts:
                status = result.gg19c_processes.get(name, "UNKNOWN")
                print(f"    ? {name}  (status: {status})")

        if result.orphan_pumps:
            deployed_diff = True
            has_any = True
            print(f"\n  ORPHAN pumps in GG 19c (deployed but not in inventory) "
                  f"({len(result.orphan_pumps)}):")
            for name in result.orphan_pumps:
                status = result.gg19c_processes.get(name, "UNKNOWN")
                print(f"    ? {name}  (status: {status})")

        if result.orphan_replicats:
            deployed_diff = True
            has_any = True
            print(f"\n  ORPHAN replicats in GG 21c (deployed but not in inventory) "
                  f"({len(result.orphan_replicats)}):")
            for name in result.orphan_replicats:
                status = result.gg21c_processes.get(name, "UNKNOWN")
                print(f"    ? {name}  (status: {status})")

        if not deployed_diff:
            if result.gg19c_processes or result.gg21c_processes:
                print("\n  No drift detected. Generated configs match deployed state.")
            else:
                print("\n  Could not connect to GG. Skipping deployed check.")
                print("  (Provide --gg19c-home and --gg21c-home, or run on the GG host)")

        # Show deployed process status
        if result.gg19c_processes:
            print(f"\n  GG 19c deployed processes ({len(result.gg19c_processes)}):")
            for name, status in sorted(result.gg19c_processes.items()):
                indicator = "OK" if status == "RUNNING" else "!!"
                print(f"    [{indicator}] {name:<10} {status}")

        if result.gg21c_processes:
            print(f"\n  GG 21c deployed processes ({len(result.gg21c_processes)}):")
            for name, status in sorted(result.gg21c_processes.items()):
                indicator = "OK" if status == "RUNNING" else "!!"
                print(f"    [{indicator}] {name:<10} {status}")

    # --- Summary ---
    print()
    print("=" * w)
    if has_any:
        print("DRIFT DETECTED — see above for details and recommended actions.")
    else:
        print("NO DRIFT DETECTED — inventory, generated configs, and deployed state are in sync.")
    print("=" * w)
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="config_diff",
        description="Detect config drift between inventory, generated files, and deployed GG state.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Comparison levels:
  inventory  — Inventory vs generated .prm files (no GG access needed)
  deployed   — Generated .prm files vs deployed GG processes (requires GG access)
  full       — Both checks combined

Examples:
  # Quick check (no GG access):
  python3 scripts/config_diff.py -i input/table_inventory.xlsx --level inventory

  # Full check on GG host:
  python3 scripts/config_diff.py -i input/table_inventory.xlsx --level full \\
      --gg19c-home /u01/app/oracle/product/19c/oggcore_1 \\
      --gg21c-home /u01/app/oracle/product/21c/oggbd_1
""",
    )

    ap.add_argument("--input", "-i", default=str(ROOT / "input" / "table_inventory.xlsx"),
                    help="Path to inventory Excel/CSV (default: input/table_inventory.xlsx)")
    ap.add_argument("--level", choices=["inventory", "deployed", "full"], default="inventory",
                    help="Comparison level (default: inventory)")

    gg = ap.add_argument_group("GoldenGate homes (required for --level deployed/full)")
    gg.add_argument("--gg19c-home",
                    help="Path to GG 19c home directory (or env GG19C_HOME)")
    gg.add_argument("--gg21c-home",
                    help="Path to GG 21c home directory (or env GG21C_HOME)")

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
    enabled = [t for t in tables if t.enabled]
    disabled = [t for t in tables if not t.enabled]
    log.info("Inventory: %d tables (%d enabled, %d disabled)",
             len(tables), len(enabled), len(disabled))

    result = DiffResult()

    # --- Level: inventory ---
    if args.level in ("inventory", "full"):
        log.info("Checking: inventory vs generated configs...")
        diff_inventory_vs_generated(tables, result)

    # --- Level: deployed ---
    if args.level in ("deployed", "full"):
        gg19c = args.gg19c_home or os.environ.get("GG19C_HOME")
        gg21c = args.gg21c_home or os.environ.get("GG21C_HOME")

        if not gg19c and not gg21c:
            log.warning(
                "No GG home paths provided. Use --gg19c-home / --gg21c-home "
                "or set GG19C_HOME / GG21C_HOME env vars."
            )
        else:
            log.info("Checking: generated configs vs deployed GG processes...")
            diff_generated_vs_deployed(gg19c, gg21c, result)

    # --- Output ---
    print_diff(result, args.level)

    return 1 if result.has_diffs else 0


if __name__ == "__main__":
    sys.exit(main())

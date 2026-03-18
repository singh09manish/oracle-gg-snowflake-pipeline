#!/usr/bin/env python3
"""
schema_evolution.py — Schema Evolution Handler for Oracle-to-Snowflake Pipeline
================================================================================

Detects DDL changes between Oracle source column metadata and previously
generated Snowflake DDL, then produces safe ALTER TABLE statements and
warning reports for changes that need human review.

Change detection:
  - New columns added      -> generates ALTER TABLE ... ADD COLUMN
  - Columns dropped        -> WARNING only (dangerous, never auto-drops)
  - Data type changes      -> WARNING only (may need manual intervention)
  - Column renamed         -> WARNING only (cannot auto-detect with certainty)

Outputs:
  - output/snowflake/alter/alter_YYYYMMDD.sql   — safe ALTER statements
  - output/snowflake/alter/warnings_YYYYMMDD.txt — changes needing human review

Usage:
  # Compare two CSV snapshots:
  python3 scripts/schema_evolution.py \
      --current-csv /tmp/oracle_columns_new.csv \
      --previous-csv /tmp/oracle_columns_old.csv

  # Compare current CSV against previously generated Snowflake DDL:
  python3 scripts/schema_evolution.py \
      --current-csv /tmp/oracle_columns_new.csv

  # Filter to inventory tables only:
  python3 scripts/schema_evolution.py \
      --current-csv /tmp/oracle_columns_new.csv \
      --inventory input/table_inventory.xlsx

  # Dry-run (show what would change without writing files):
  python3 scripts/schema_evolution.py \
      --current-csv /tmp/oracle_columns_new.csv \
      --dry-run

  # Apply ALTER statements directly to Snowflake:
  python3 scripts/schema_evolution.py \
      --current-csv /tmp/oracle_columns_new.csv \
      --apply

The CSV format matches the output of sql/oracle_table_ddl.sql:
  owner, table_name, column_name, data_type, data_length,
  data_precision, data_scale, nullable, column_id
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("schema_evolution")


# ---------------------------------------------------------------------------
# Oracle-to-Snowflake type mapping
# ---------------------------------------------------------------------------

def oracle_to_snowflake_type(
    data_type: str,
    data_length: int,
    data_precision: Optional[int],
    data_scale: Optional[int],
) -> str:
    """
    Map an Oracle data type to its Snowflake equivalent.

    This mirrors the mapping used during initial DDL generation.
    """
    dt = data_type.upper().strip()

    # Numeric types
    if dt == "NUMBER":
        if data_precision is not None and data_scale is not None:
            if data_scale == 0:
                if data_precision <= 10:
                    return "INTEGER"
                elif data_precision <= 18:
                    return "BIGINT"
                else:
                    return f"NUMBER({data_precision},0)"
            else:
                return f"NUMBER({data_precision},{data_scale})"
        elif data_precision is not None:
            return f"NUMBER({data_precision})"
        else:
            return "NUMBER(38,10)"
    elif dt == "FLOAT":
        return "FLOAT"
    elif dt in ("BINARY_FLOAT",):
        return "FLOAT"
    elif dt in ("BINARY_DOUBLE",):
        return "DOUBLE"

    # String types
    elif dt == "VARCHAR2":
        return f"VARCHAR({data_length})"
    elif dt == "NVARCHAR2":
        return f"VARCHAR({data_length})"
    elif dt == "CHAR":
        return f"CHAR({data_length})"
    elif dt == "NCHAR":
        return f"CHAR({data_length})"
    elif dt in ("CLOB", "NCLOB", "LONG"):
        return "VARCHAR(16777216)"

    # Date/time types
    elif dt == "DATE":
        return "TIMESTAMP_NTZ"
    elif dt.startswith("TIMESTAMP"):
        if "TIME ZONE" in dt:
            return "TIMESTAMP_TZ"
        elif "LOCAL" in dt:
            return "TIMESTAMP_LTZ"
        else:
            return "TIMESTAMP_NTZ"

    # Binary types
    elif dt in ("BLOB", "RAW", "LONG RAW"):
        return "BINARY"
    elif dt == "BFILE":
        return "VARCHAR(255)"

    # Interval types
    elif dt.startswith("INTERVAL"):
        return "VARCHAR(100)"

    # ROWID
    elif dt in ("ROWID", "UROWID"):
        return "VARCHAR(200)"

    # Fallback
    else:
        return "VARCHAR(4000)"


# ---------------------------------------------------------------------------
# Column metadata model
# ---------------------------------------------------------------------------

class ColumnMeta:
    """Represents a single column's metadata from Oracle."""

    def __init__(
        self,
        owner: str,
        table_name: str,
        column_name: str,
        data_type: str,
        data_length: int = 0,
        data_precision: Optional[int] = None,
        data_scale: Optional[int] = None,
        nullable: str = "Y",
        column_id: int = 0,
    ):
        self.owner = owner.strip().upper()
        self.table_name = table_name.strip().upper()
        self.column_name = column_name.strip().upper()
        self.data_type = data_type.strip().upper()
        self.data_length = data_length
        self.data_precision = data_precision
        self.data_scale = data_scale
        self.nullable = nullable.strip().upper()
        self.column_id = column_id

    @property
    def fqn(self) -> str:
        """Fully qualified table name."""
        return f"{self.owner}.{self.table_name}"

    @property
    def snowflake_type(self) -> str:
        """Snowflake data type equivalent."""
        return oracle_to_snowflake_type(
            self.data_type, self.data_length,
            self.data_precision, self.data_scale,
        )

    @property
    def is_nullable(self) -> bool:
        return self.nullable == "Y"

    def type_signature(self) -> str:
        """
        A normalized string representing the column type for comparison.
        Used to detect data type changes.
        """
        parts = [self.data_type]
        if self.data_precision is not None:
            parts.append(str(self.data_precision))
        if self.data_scale is not None:
            parts.append(str(self.data_scale))
        if self.data_length:
            parts.append(str(self.data_length))
        return "|".join(parts)


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def read_oracle_csv(csv_path: str) -> Dict[str, Dict[str, ColumnMeta]]:
    """
    Read an Oracle column metadata CSV (output of oracle_table_ddl.sql).

    Returns: { "OWNER.TABLE": { "COLUMN_NAME": ColumnMeta, ... }, ... }
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    tables: Dict[str, Dict[str, ColumnMeta]] = {}

    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, skipinitialspace=True)

        # Normalize header names
        if reader.fieldnames:
            reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]

        for row in reader:
            # Clean values
            row = {k.strip().lower(): v.strip() for k, v in row.items() if k}

            owner = row.get("owner", "").upper()
            table_name = row.get("table_name", "").upper()
            column_name = row.get("column_name", "").upper()
            data_type = row.get("data_type", "").upper()

            if not all([owner, table_name, column_name, data_type]):
                continue

            # Parse numeric fields safely
            data_length = _safe_int(row.get("data_length", "0"), 0)
            data_precision = _safe_int_or_none(row.get("data_precision", ""))
            data_scale = _safe_int_or_none(row.get("data_scale", ""))
            nullable = row.get("nullable", "Y").upper()
            column_id = _safe_int(row.get("column_id", "0"), 0)

            col = ColumnMeta(
                owner=owner,
                table_name=table_name,
                column_name=column_name,
                data_type=data_type,
                data_length=data_length,
                data_precision=data_precision,
                data_scale=data_scale,
                nullable=nullable,
                column_id=column_id,
            )

            fqn = col.fqn
            if fqn not in tables:
                tables[fqn] = OrderedDict()
            tables[fqn][column_name] = col

    log.info("Loaded %d tables (%d total columns) from %s",
             len(tables), sum(len(cols) for cols in tables.values()), csv_path)
    return tables


def _safe_int(val: str, default: int = 0) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_int_or_none(val: str) -> Optional[int]:
    if not val or val.strip() == "" or val.strip().upper() == "NONE":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Parse previously generated Snowflake DDL files
# ---------------------------------------------------------------------------

def read_previous_ddl(ddl_dir: str) -> Dict[str, Dict[str, str]]:
    """
    Parse previously generated Snowflake CREATE TABLE statements from the
    output/snowflake/ddl/ directory.

    Returns: { "OWNER.TABLE": { "COLUMN_NAME": "SNOWFLAKE_TYPE", ... }, ... }
    """
    ddl_path = Path(ddl_dir)
    if not ddl_path.exists():
        log.warning("DDL directory not found: %s", ddl_dir)
        return {}

    tables: Dict[str, Dict[str, str]] = {}

    for sql_file in sorted(ddl_path.glob("*.sql")):
        content = sql_file.read_text()
        tables.update(_parse_create_statements(content))

    log.info("Loaded %d tables from previous DDL in %s", len(tables), ddl_dir)
    return tables


def _parse_create_statements(sql_text: str) -> Dict[str, Dict[str, str]]:
    """
    Extract table name and column definitions from CREATE TABLE statements.

    Returns: { "SCHEMA.TABLE": { "COL_NAME": "SF_TYPE", ... }, ... }
    """
    tables: Dict[str, Dict[str, str]] = {}

    # Match CREATE TABLE schema.table ( ... );
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(\w+)\.(\w+)\s*\((.*?)\)\s*;",
        re.IGNORECASE | re.DOTALL,
    )

    for m in pattern.finditer(sql_text):
        schema = m.group(1).upper()
        table = m.group(2).upper()
        body = m.group(3)
        fqn = f"{schema}.{table}"

        columns: Dict[str, str] = OrderedDict()
        for line in body.split(","):
            line = line.strip()
            if not line or line.upper().startswith("PRIMARY") or line.upper().startswith("CONSTRAINT"):
                continue
            parts = line.split(None, 2)
            if len(parts) >= 2:
                col_name = parts[0].strip().upper().strip('"')
                col_type = parts[1].strip().upper()
                # Merge type with precision if present (e.g., "NUMBER(10,2)")
                if len(parts) > 2 and parts[2].startswith("("):
                    col_type += " " + parts[2].split(",")[0].split(")")[0] + ")"
                columns[col_name] = col_type

        if columns:
            tables[fqn] = columns

    return tables


# ---------------------------------------------------------------------------
# Schema change detection
# ---------------------------------------------------------------------------

class SchemaChange:
    """Represents a detected schema change for a single table."""

    def __init__(self, table_fqn: str):
        self.table_fqn = table_fqn
        self.added_columns: List[ColumnMeta] = []
        self.dropped_columns: List[str] = []
        self.type_changes: List[Tuple[str, str, str]] = []  # (col, old_type, new_type)
        self.possible_renames: List[Tuple[str, str]] = []    # (old_col, new_col)

    @property
    def has_safe_changes(self) -> bool:
        return bool(self.added_columns)

    @property
    def has_warnings(self) -> bool:
        return bool(self.dropped_columns or self.type_changes or self.possible_renames)

    @property
    def has_any_changes(self) -> bool:
        return self.has_safe_changes or self.has_warnings


def detect_changes(
    current: Dict[str, Dict[str, ColumnMeta]],
    previous: Dict[str, Dict[str, ColumnMeta]],
    filter_tables: Optional[Set[str]] = None,
) -> List[SchemaChange]:
    """
    Compare current Oracle metadata against a previous snapshot and detect changes.

    Args:
        current:  Current column metadata (from --current-csv).
        previous: Previous column metadata (from --previous-csv or generated DDL).
        filter_tables: Optional set of FQNs to restrict comparison to.

    Returns a list of SchemaChange objects.
    """
    changes: List[SchemaChange] = []

    # Only look at tables that exist in both or existed previously
    all_tables = set(current.keys()) | set(previous.keys())
    if filter_tables:
        all_tables &= filter_tables

    for fqn in sorted(all_tables):
        cur_cols = current.get(fqn, {})
        prev_cols = previous.get(fqn, {})

        if not prev_cols:
            # Table is new — skip (not a schema change, it is a new table)
            continue
        if not cur_cols:
            # Table disappeared from source — unusual, skip
            continue

        change = SchemaChange(fqn)

        cur_names = set(cur_cols.keys())
        prev_names = set(prev_cols.keys())

        # New columns
        added = cur_names - prev_names
        for col_name in sorted(added):
            change.added_columns.append(cur_cols[col_name])

        # Dropped columns
        dropped = prev_names - cur_names
        for col_name in sorted(dropped):
            change.dropped_columns.append(col_name)

        # Type changes (columns present in both)
        common = cur_names & prev_names
        for col_name in sorted(common):
            cur_sig = cur_cols[col_name].type_signature()
            prev_sig = prev_cols[col_name].type_signature()
            if cur_sig != prev_sig:
                change.type_changes.append((
                    col_name,
                    prev_cols[col_name].data_type,
                    cur_cols[col_name].data_type,
                ))

        # Possible renames: heuristic — if exactly one column was dropped and
        # exactly one was added in the same position range, it might be a rename
        if len(added) == 1 and len(dropped) == 1:
            added_col = list(added)[0]
            dropped_col = list(dropped)[0]
            # Check if the new column has a similar column_id
            added_meta = cur_cols[added_col]
            # Simple heuristic: same data type suggests rename
            dropped_meta_sig = prev_cols[dropped_col].type_signature()
            added_meta_sig = added_meta.type_signature()
            if dropped_meta_sig == added_meta_sig:
                change.possible_renames.append((dropped_col, added_col))
                # Remove from added/dropped since we flagged as rename
                change.added_columns = [
                    c for c in change.added_columns if c.column_name != added_col
                ]
                change.dropped_columns = [
                    c for c in change.dropped_columns if c != dropped_col
                ]

        if change.has_any_changes:
            changes.append(change)

    return changes


def detect_changes_from_ddl(
    current: Dict[str, Dict[str, ColumnMeta]],
    previous_ddl: Dict[str, Dict[str, str]],
    filter_tables: Optional[Set[str]] = None,
) -> List[SchemaChange]:
    """
    Compare current Oracle metadata against previously generated Snowflake DDL.

    This is used when --previous-csv is not provided and we compare against
    the files in output/snowflake/ddl/.
    """
    changes: List[SchemaChange] = []

    all_tables = set(current.keys()) | set(previous_ddl.keys())
    if filter_tables:
        all_tables &= filter_tables

    for fqn in sorted(all_tables):
        cur_cols = current.get(fqn, {})
        prev_col_types = previous_ddl.get(fqn, {})

        if not prev_col_types or not cur_cols:
            continue

        change = SchemaChange(fqn)

        cur_names = set(cur_cols.keys())
        prev_names = set(prev_col_types.keys())

        # New columns
        for col_name in sorted(cur_names - prev_names):
            # Skip CDC metadata columns that only exist in Snowflake
            if col_name in ("OP_TYPE", "OP_TS", "OGG_POSITION", "IS_DELETED", "DELETED_TS"):
                continue
            change.added_columns.append(cur_cols[col_name])

        # Dropped columns
        for col_name in sorted(prev_names - cur_names):
            if col_name in ("OP_TYPE", "OP_TS", "OGG_POSITION", "IS_DELETED", "DELETED_TS"):
                continue
            change.dropped_columns.append(col_name)

        if change.has_any_changes:
            changes.append(change)

    return changes


# ---------------------------------------------------------------------------
# Output generators
# ---------------------------------------------------------------------------

def generate_alter_sql(
    changes: List[SchemaChange],
    target_schema_map: Optional[Dict[str, str]] = None,
) -> str:
    """
    Generate ALTER TABLE ... ADD COLUMN statements for safe changes (new columns only).

    Args:
        changes: List of detected schema changes.
        target_schema_map: Optional map { "SOURCE_SCHEMA.TABLE" -> "TARGET_SCHEMA" }.

    Returns SQL text with ALTER TABLE statements.
    """
    lines = [
        "-- =============================================================================",
        f"-- Schema Evolution: ALTER TABLE statements",
        f"-- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "-- Only ADD COLUMN operations (safe). Drops/renames require manual action.",
        "-- =============================================================================",
        "",
    ]

    alter_count = 0
    for change in changes:
        if not change.added_columns:
            continue

        # Determine target schema
        parts = change.table_fqn.split(".", 1)
        source_schema = parts[0]
        table_name = parts[1] if len(parts) > 1 else parts[0]
        target_schema = source_schema
        if target_schema_map and change.table_fqn in target_schema_map:
            target_schema = target_schema_map[change.table_fqn]

        lines.append(f"-- Table: {change.table_fqn}")
        lines.append(f"-- New columns: {len(change.added_columns)}")

        for col in change.added_columns:
            sf_type = col.snowflake_type
            nullable_str = "" if col.is_nullable else " NOT NULL"
            stmt = (
                f"ALTER TABLE {target_schema}.{table_name} "
                f"ADD COLUMN {col.column_name} {sf_type}{nullable_str};"
            )
            lines.append(stmt)
            alter_count += 1

        lines.append("")

    if alter_count == 0:
        lines.append("-- No safe ALTER statements to generate.")
        lines.append("")

    lines.append(f"-- Total ALTER statements: {alter_count}")
    return "\n".join(lines)


def generate_warnings_report(changes: List[SchemaChange]) -> str:
    """
    Generate a human-readable warnings report for unsafe changes.
    """
    lines = [
        "=" * 70,
        "Schema Evolution: Changes Requiring Human Review",
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "=" * 70,
        "",
    ]

    warning_count = 0

    for change in changes:
        if not change.has_warnings:
            continue

        lines.append(f"Table: {change.table_fqn}")
        lines.append("-" * 50)

        # Dropped columns
        if change.dropped_columns:
            lines.append("")
            lines.append("  DROPPED COLUMNS (will NOT auto-drop -- too dangerous):")
            for col in change.dropped_columns:
                lines.append(f"    - {col}")
                warning_count += 1
            lines.append("")
            lines.append("  ACTION REQUIRED: Verify these columns are no longer needed.")
            lines.append("  If confirmed, manually run:")
            for col in change.dropped_columns:
                parts = change.table_fqn.split(".", 1)
                lines.append(
                    f"    ALTER TABLE {parts[0]}.{parts[1]} DROP COLUMN {col};"
                )

        # Type changes
        if change.type_changes:
            lines.append("")
            lines.append("  DATA TYPE CHANGES (may need manual intervention):")
            for col_name, old_type, new_type in change.type_changes:
                lines.append(f"    - {col_name}: {old_type} -> {new_type}")
                warning_count += 1
            lines.append("")
            lines.append("  ACTION REQUIRED: Review type compatibility.")
            lines.append("  Snowflake may auto-cast, or you may need ALTER COLUMN ... SET DATA TYPE.")

        # Possible renames
        if change.possible_renames:
            lines.append("")
            lines.append("  POSSIBLE COLUMN RENAMES (cannot auto-detect with certainty):")
            for old_col, new_col in change.possible_renames:
                lines.append(f"    - {old_col} -> {new_col} (same data type)")
                warning_count += 1
            lines.append("")
            lines.append("  ACTION REQUIRED: Confirm if this is a rename or drop+add.")
            lines.append("  If rename, manually run:")
            for old_col, new_col in change.possible_renames:
                parts = change.table_fqn.split(".", 1)
                lines.append(
                    f"    ALTER TABLE {parts[0]}.{parts[1]} "
                    f"RENAME COLUMN {old_col} TO {new_col};"
                )

        lines.append("")

    if warning_count == 0:
        lines.append("No unsafe changes detected.")
    else:
        lines.append(f"Total warnings: {warning_count}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# PRM file updater (update COLMAP if column list affects it)
# ---------------------------------------------------------------------------

def check_prm_impact(
    changes: List[SchemaChange],
    prm_dir: str,
) -> List[str]:
    """
    Check if any schema changes affect existing .prm files (replicat MAP statements).

    Returns a list of warning messages about affected .prm files.
    """
    prm_path = Path(prm_dir)
    if not prm_path.exists():
        return []

    warnings = []
    affected_tables = {c.table_fqn for c in changes if c.has_any_changes}

    for prm_file in sorted(prm_path.glob("REP*.prm")):
        content = prm_file.read_text()
        for fqn in affected_tables:
            # Check if this table appears in the PRM file
            schema, table = fqn.split(".", 1)
            if f"{schema}.{table}" in content.upper():
                warnings.append(
                    f"  {prm_file.name} contains MAP for {fqn} "
                    f"-- may need regeneration (ggctl generate)"
                )

    return warnings


# ---------------------------------------------------------------------------
# Snowflake apply (optional)
# ---------------------------------------------------------------------------

def apply_alters_to_snowflake(
    alter_sql: str,
    config: dict,
) -> Tuple[int, int, List[str]]:
    """
    Execute ALTER TABLE statements against Snowflake.

    Returns (success_count, failure_count, error_messages).
    """
    sf_cfg = config.get("gg21c", {}).get("snowflake", {})
    account = sf_cfg.get("account", "")
    warehouse = sf_cfg.get("warehouse", "")
    database = sf_cfg.get("database", "")
    role = sf_cfg.get("role", "")

    if not account or account == "your_account":
        return 0, 0, ["Snowflake account not configured in pipeline.yaml"]

    try:
        import snowflake.connector
    except ImportError:
        return 0, 0, [
            "snowflake-connector-python not installed. "
            "Install with: pip install snowflake-connector-python"
        ]

    success = 0
    failure = 0
    errors: List[str] = []

    # Extract individual ALTER statements
    statements = [
        line.strip() for line in alter_sql.splitlines()
        if line.strip().upper().startswith("ALTER TABLE")
    ]

    if not statements:
        log.info("No ALTER statements to apply.")
        return 0, 0, []

    try:
        conn = snowflake.connector.connect(
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
            authenticator="externalbrowser",  # SSO auth
        )
        cur = conn.cursor()

        for stmt in statements:
            try:
                log.info("Executing: %s", stmt)
                cur.execute(stmt)
                success += 1
            except Exception as e:
                failure += 1
                errors.append(f"FAILED: {stmt}\n  Error: {e}")
                log.error("Failed: %s -- %s", stmt, e)

        cur.close()
        conn.close()
    except Exception as e:
        return 0, len(statements), [f"Snowflake connection failed: {e}"]

    return success, failure, errors


# ---------------------------------------------------------------------------
# Inventory filter
# ---------------------------------------------------------------------------

def load_inventory_tables(inventory_path: str) -> Set[str]:
    """
    Load table FQNs from the inventory file to filter schema evolution checks.
    """
    try:
        from app.inventory import read_inventory
        tables = read_inventory(inventory_path)
        fqns = {t.fqn for t in tables if t.enabled}
        log.info("Loaded %d enabled tables from inventory for filtering", len(fqns))
        return fqns
    except Exception as e:
        log.error("Failed to load inventory: %s", e)
        return set()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Detect schema evolution between Oracle and Snowflake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--current-csv", required=True,
        help="Path to current Oracle column metadata CSV (from oracle_table_ddl.sql)",
    )
    parser.add_argument(
        "--previous-csv", default=None,
        help="Path to previous Oracle column metadata CSV. "
             "If not provided, compares against output/snowflake/ddl/",
    )
    parser.add_argument(
        "--inventory", default=None,
        help="Path to table_inventory.xlsx — filter changes to inventory tables only",
    )
    parser.add_argument(
        "--config", default=str(PROJECT_ROOT / "config" / "pipeline.yaml"),
        help="Path to pipeline.yaml (for Snowflake connection when using --apply)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Apply safe ALTER statements directly to Snowflake",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would change without writing any files",
    )

    args = parser.parse_args()

    # Load current CSV
    log.info("Reading current column metadata...")
    current = read_oracle_csv(args.current_csv)

    # Load previous state
    filter_tables: Optional[Set[str]] = None
    if args.inventory:
        filter_tables = load_inventory_tables(args.inventory)

    if args.previous_csv:
        # Compare two CSV snapshots
        log.info("Reading previous column metadata...")
        previous = read_oracle_csv(args.previous_csv)
        changes = detect_changes(current, previous, filter_tables)
    else:
        # Compare against generated Snowflake DDL
        ddl_dir = str(PROJECT_ROOT / "output" / "snowflake" / "ddl")
        log.info("Reading previously generated Snowflake DDL from %s...", ddl_dir)
        previous_ddl = read_previous_ddl(ddl_dir)
        if previous_ddl:
            changes = detect_changes_from_ddl(current, previous_ddl, filter_tables)
        else:
            log.warning(
                "No previous DDL found. Cannot detect changes without a baseline. "
                "Use --previous-csv to provide an older Oracle CSV snapshot."
            )
            changes = []

    # Report results
    safe_count = sum(len(c.added_columns) for c in changes)
    warn_count = sum(
        len(c.dropped_columns) + len(c.type_changes) + len(c.possible_renames)
        for c in changes
    )
    total_tables = len(changes)

    log.info("=" * 60)
    log.info("Schema Evolution Summary")
    log.info("  Tables with changes:  %d", total_tables)
    log.info("  Safe changes (ADD):   %d", safe_count)
    log.info("  Warnings:             %d", warn_count)
    log.info("=" * 60)

    if not changes:
        log.info("No schema changes detected.")
        return

    # Generate outputs
    alter_sql = generate_alter_sql(changes)
    warnings_report = generate_warnings_report(changes)

    # Check PRM impact
    prm_dir = str(PROJECT_ROOT / "output" / "gg21c" / "dirprm")
    prm_warnings = check_prm_impact(changes, prm_dir)
    if prm_warnings:
        log.info("")
        log.info("Affected .prm files (may need regeneration):")
        for w in prm_warnings:
            log.info(w)

    if args.dry_run:
        log.info("")
        log.info("--- ALTER SQL (dry-run) ---")
        print(alter_sql)
        if warn_count > 0:
            log.info("")
            log.info("--- WARNINGS (dry-run) ---")
            print(warnings_report)
        return

    # Write output files
    today = datetime.utcnow().strftime("%Y%m%d")
    alter_dir = PROJECT_ROOT / "output" / "snowflake" / "alter"
    alter_dir.mkdir(parents=True, exist_ok=True)

    alter_path = alter_dir / f"alter_{today}.sql"
    alter_path.write_text(alter_sql)
    log.info("Wrote: %s", alter_path)

    if warn_count > 0:
        warnings_path = alter_dir / f"warnings_{today}.txt"
        warnings_path.write_text(warnings_report)
        log.info("Wrote: %s", warnings_path)

    # Apply to Snowflake if requested
    if args.apply and safe_count > 0:
        log.info("")
        log.info("Applying ALTER statements to Snowflake...")
        config = {}
        config_path = Path(args.config)
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f) or {}

        ok, fail, errs = apply_alters_to_snowflake(alter_sql, config)
        log.info("  Applied: %d  Failed: %d", ok, fail)
        for e in errs:
            log.error("  %s", e)
    elif args.apply and safe_count == 0:
        log.info("No safe ALTER statements to apply.")


if __name__ == "__main__":
    main()

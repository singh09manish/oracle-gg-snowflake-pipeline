"""
Snowflake DDL Generator — auto-generate CREATE TABLE statements from Oracle metadata.

Supports two input modes:
  1. List of TableInfo objects + Oracle column metadata (from direct query or CSV)
  2. CSV file exported from sql/oracle_table_ddl.sql

Every generated table includes GoldenGate CDC metadata columns appended
after the source columns (OP_TYPE, OP_TS, IS_DELETED).

Output: one .sql file per target_schema + a combined all_tables.sql file.
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column model
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """One column from Oracle DBA_TAB_COLUMNS."""

    owner: str
    table_name: str
    column_name: str
    data_type: str
    data_length: int = 0
    data_precision: Optional[int] = None
    data_scale: Optional[int] = None
    nullable: str = "Y"       # Y or N
    column_id: int = 0

    def __post_init__(self) -> None:
        self.owner = self.owner.strip().upper()
        self.table_name = self.table_name.strip().upper()
        self.column_name = self.column_name.strip().upper()
        self.data_type = self.data_type.strip().upper()
        self.nullable = self.nullable.strip().upper()
        self.data_length = int(self.data_length) if self.data_length else 0
        self.data_precision = _safe_int(self.data_precision)
        self.data_scale = _safe_int(self.data_scale)
        self.column_id = int(self.column_id) if self.column_id else 0


# ---------------------------------------------------------------------------
# GG CDC metadata columns (appended to every table)
# ---------------------------------------------------------------------------

_GG_CDC_COLUMNS = [
    ("OP_TYPE",      "VARCHAR(1)",       "-- I/U/D operation type"),
    ("OP_TS",        "TIMESTAMP_NTZ",    "-- operation timestamp from source"),
    ("IS_DELETED",   "NUMBER(1,0) DEFAULT 0", "-- soft-delete flag (1=deleted)"),
]


# ---------------------------------------------------------------------------
# Oracle → Snowflake type mapping
# ---------------------------------------------------------------------------

def map_oracle_to_snowflake(col: ColumnInfo) -> Tuple[str, Optional[str]]:
    """
    Map an Oracle data type to the equivalent Snowflake type.

    Returns:
        (snowflake_type, warning_or_none)
    """
    dt = col.data_type
    p = col.data_precision
    s = col.data_scale
    length = col.data_length

    # --- NUMBER variants ---
    if dt == "NUMBER":
        if p is not None and s is not None and s > 0:
            return f"NUMBER({p},{s})", None
        elif p is not None:
            return f"NUMBER({p},0)", None
        else:
            # Bare NUMBER with no precision — common in Oracle for flexible numerics
            return "NUMBER(38,10)", None

    # --- VARCHAR2 / NVARCHAR2 ---
    if dt in ("VARCHAR2", "NVARCHAR2"):
        return f"VARCHAR({length})", None

    # --- CHAR / NCHAR ---
    if dt in ("CHAR", "NCHAR"):
        return f"CHAR({length})", None

    # --- DATE ---
    if dt == "DATE":
        return "TIMESTAMP_NTZ", None

    # --- TIMESTAMP variants ---
    # DBA_TAB_COLUMNS stores these as e.g. "TIMESTAMP(6)" or
    # "TIMESTAMP(6) WITH TIME ZONE" or "TIMESTAMP(6) WITH LOCAL TIME ZONE"
    if dt.startswith("TIMESTAMP"):
        if "WITH LOCAL TIME ZONE" in dt:
            return "TIMESTAMP_LTZ", None
        elif "WITH TIME ZONE" in dt:
            return "TIMESTAMP_TZ", None
        else:
            return "TIMESTAMP_NTZ", None

    # --- LOB types ---
    if dt in ("CLOB", "NCLOB"):
        return "VARCHAR(16777216)", None

    if dt == "BLOB":
        return "BINARY", None

    # --- RAW ---
    if dt == "RAW":
        return f"BINARY({length})", None

    # --- LONG / LONG RAW ---
    if dt in ("LONG", "LONG RAW"):
        return "VARCHAR(16777216)", None

    # --- Float / Double ---
    if dt == "FLOAT":
        return "FLOAT", None

    if dt == "BINARY_FLOAT":
        return "FLOAT", None

    if dt == "BINARY_DOUBLE":
        return "DOUBLE", None

    # --- XMLTYPE ---
    if dt == "XMLTYPE":
        return "VARIANT", None

    # --- INTERVAL types ---
    if dt.startswith("INTERVAL YEAR"):
        return "VARCHAR(100)", None

    if dt.startswith("INTERVAL DAY"):
        return "VARCHAR(100)", None

    # --- ROWID / UROWID ---
    if dt in ("ROWID", "UROWID"):
        return "VARCHAR(200)", None

    # --- Fallback ---
    warning = (
        f"Unmapped Oracle type '{dt}' for {col.owner}.{col.table_name}.{col.column_name} "
        f"— defaulting to VARCHAR(16777216)"
    )
    return "VARCHAR(16777216)", warning


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def read_columns_from_csv(csv_path: str | Path) -> List[ColumnInfo]:
    """
    Read column metadata from a CSV exported by sql/oracle_table_ddl.sql.

    Handles both SQL*Plus CSV output (with extra whitespace) and clean CSVs.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    columns: List[ColumnInfo] = []
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, skipinitialspace=True)
        for row in reader:
            # Normalise keys (SQL*Plus sometimes adds spaces)
            cleaned = {k.strip().upper(): v.strip() if v else "" for k, v in row.items() if k}

            # Skip junk rows (SQL*Plus may include separator lines)
            owner = cleaned.get("OWNER", "")
            if not owner or owner.startswith("-") or owner.upper() == "OWNER":
                continue

            columns.append(ColumnInfo(
                owner=owner,
                table_name=cleaned.get("TABLE_NAME", ""),
                column_name=cleaned.get("COLUMN_NAME", ""),
                data_type=cleaned.get("DATA_TYPE", ""),
                data_length=cleaned.get("DATA_LENGTH", "0"),
                data_precision=cleaned.get("DATA_PRECISION", ""),
                data_scale=cleaned.get("DATA_SCALE", ""),
                nullable=cleaned.get("NULLABLE", "Y"),
                column_id=cleaned.get("COLUMN_ID", "0"),
            ))

    if not columns:
        raise ValueError(f"No column data found in {path}")

    log.info("Loaded %d columns from %s", len(columns), path)
    return columns


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

def generate_ddl(
    columns: List[ColumnInfo],
    *,
    filter_schemas: Optional[Set[str]] = None,
    filter_tables: Optional[Set[str]] = None,
    target_schema_map: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str]]:
    """
    Generate Snowflake CREATE TABLE statements from Oracle column metadata.

    Args:
        columns: list of ColumnInfo from Oracle
        filter_schemas: only include these schemas (uppercase). None = all.
        filter_tables: only include these FQNs (SCHEMA.TABLE). None = all.
        target_schema_map: {SOURCE_SCHEMA: TARGET_SCHEMA}. Unmapped schemas
                          keep their Oracle name as the Snowflake schema.

    Returns:
        (ddl_by_schema, warnings)
        ddl_by_schema: {target_schema: full_ddl_text}
        warnings: list of warning messages
    """
    target_schema_map = target_schema_map or {}
    warnings: List[str] = []

    # Group columns by (owner, table_name), preserving column_id order
    table_cols: Dict[str, List[ColumnInfo]] = defaultdict(list)
    for col in sorted(columns, key=lambda c: (c.owner, c.table_name, c.column_id)):
        fqn = f"{col.owner}.{col.table_name}"

        # Apply schema filter
        if filter_schemas and col.owner not in filter_schemas:
            continue
        # Apply table-level filter (from inventory)
        if filter_tables and fqn not in filter_tables:
            continue

        table_cols[fqn].append(col)

    if not table_cols:
        raise ValueError("No tables matched the filters. Check --schemas or --inventory.")

    # Generate DDL grouped by target schema
    ddl_by_schema: Dict[str, List[str]] = defaultdict(list)

    for fqn, cols in sorted(table_cols.items()):
        owner = cols[0].owner
        table_name = cols[0].table_name
        target_schema = target_schema_map.get(owner, owner)

        lines: List[str] = []
        lines.append(f"CREATE TABLE IF NOT EXISTS {target_schema}.{table_name} (")

        # Source columns
        col_defs: List[str] = []
        for col in cols:
            sf_type, warn = map_oracle_to_snowflake(col)
            if warn:
                warnings.append(warn)

            not_null = " NOT NULL" if col.nullable == "N" else ""
            col_defs.append(f"    {col.column_name:<40} {sf_type}{not_null}")

        # GG CDC metadata columns
        col_defs.append("")
        col_defs.append("    -- GoldenGate CDC metadata columns")
        for cdc_name, cdc_type, cdc_comment in _GG_CDC_COLUMNS:
            col_defs.append(f"    {cdc_name:<40} {cdc_type}{'':30} {cdc_comment}")

        lines.append(",\n".join(col_defs))
        lines.append(");")
        lines.append("")

        ddl_by_schema[target_schema].append("\n".join(lines))

    # Assemble into final output per schema
    result: Dict[str, str] = {}
    for schema, stmts in sorted(ddl_by_schema.items()):
        header = (
            f"-- =============================================================================\n"
            f"-- Snowflake DDL — Schema: {schema}\n"
            f"-- Generated by: oracle-gg-snowflake-pipeline / ddl_generator\n"
            f"-- Tables: {len(stmts)}\n"
            f"-- =============================================================================\n"
            f"\n"
            f"USE SCHEMA {schema};\n"
            f"\n"
        )
        result[schema] = header + "\n".join(stmts)

    table_count = len(table_cols)
    schema_count = len(result)
    log.info(
        "Generated DDL for %d tables across %d target schema(s): %s",
        table_count, schema_count, ", ".join(sorted(result.keys())),
    )

    return result, warnings


def write_ddl_files(
    ddl_by_schema: Dict[str, str],
    output_dir: str | Path,
) -> List[str]:
    """
    Write DDL files to disk:
      - One file per target schema: {SCHEMA}.sql
      - One combined file: all_tables.sql

    Returns list of file paths written.
    """
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    files_written: List[str] = []

    # Per-schema files
    for schema, ddl_text in sorted(ddl_by_schema.items()):
        file_path = out / f"{schema}.sql"
        file_path.write_text(ddl_text, encoding="utf-8")
        files_written.append(str(file_path))
        log.info("  Written: %s", file_path)

    # Combined file
    all_path = out / "all_tables.sql"
    combined_header = (
        "-- =============================================================================\n"
        "-- Snowflake DDL — All Tables (combined)\n"
        "-- Generated by: oracle-gg-snowflake-pipeline / ddl_generator\n"
        f"-- Schemas: {', '.join(sorted(ddl_by_schema.keys()))}\n"
        "-- =============================================================================\n"
        "\n"
    )
    all_ddl = combined_header + "\n\n".join(
        ddl_by_schema[s] for s in sorted(ddl_by_schema.keys())
    )
    all_path.write_text(all_ddl, encoding="utf-8")
    files_written.append(str(all_path))
    log.info("  Written: %s", all_path)

    return files_written


# ---------------------------------------------------------------------------
# Inventory integration
# ---------------------------------------------------------------------------

def get_target_schema_map_from_inventory(tables) -> Dict[str, str]:
    """
    Build a {source_schema: target_schema} map from TableInfo objects.

    If multiple tables in the same source schema map to different target
    schemas, the most common mapping wins (with a warning).
    """
    from collections import Counter

    schema_targets: Dict[str, Counter] = defaultdict(Counter)
    for t in tables:
        schema_targets[t.schema][t.target_schema] += 1

    mapping: Dict[str, str] = {}
    for src, counter in schema_targets.items():
        most_common = counter.most_common(1)[0][0]
        mapping[src] = most_common
        if len(counter) > 1:
            log.warning(
                "Schema %s maps to multiple target schemas: %s — using %s",
                src, dict(counter), most_common,
            )

    return mapping


def get_filter_tables_from_inventory(tables) -> Set[str]:
    """Return set of FQNs (SCHEMA.TABLE) from inventory for filtering."""
    return {t.fqn for t in tables if t.enabled}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(val) -> Optional[int]:
    """Convert to int, returning None for empty/null values."""
    if val is None or val == "" or str(val).strip() == "":
        return None
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return None

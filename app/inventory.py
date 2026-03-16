"""
Read the table inventory from Excel (.xlsx) or CSV.

Expected columns (case-insensitive):
  - schema       (required)  — Oracle source schema
  - table_name   (required)  — Oracle table name
  - target_schema (optional) — Snowflake target schema (defaults to source schema)

Any extra columns are silently ignored.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import List, Optional

from app.models import TableInfo

log = logging.getLogger(__name__)

# Column name aliases (lowercase) → canonical field
_SCHEMA_ALIASES = {"schema", "schema_name", "owner", "source_schema"}
_TABLE_ALIASES = {"table_name", "tablename", "table", "name", "object_name"}
_TARGET_ALIASES = {"target_schema", "target_schema_name", "tgt_schema", "snowflake_schema"}
_GROUP_ALIASES = {"group_id", "group", "extract_group"}


def read_inventory(path: str | Path) -> List[TableInfo]:
    """
    Read an Excel (.xlsx) or CSV file and return deduplicated TableInfo list.

    Raises ValueError if required columns are missing or file is empty.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Inventory file not found: {path}")

    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        rows = _read_excel(path)
    elif ext in (".csv", ".tsv"):
        rows = _read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}  (use .xlsx or .csv)")

    tables = _parse_rows(rows, str(path))
    tables = _deduplicate(tables)
    _validate(tables, str(path))
    return tables


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------

def _read_excel(path: Path) -> List[dict]:
    try:
        import openpyxl
    except ImportError:
        raise ImportError(
            "openpyxl is required to read .xlsx files.\n"
            "  pip install openpyxl"
        )

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    if ws is None:
        raise ValueError(f"No active sheet in {path}")

    rows_iter = ws.iter_rows(values_only=True)
    header_raw = next(rows_iter, None)
    if header_raw is None:
        raise ValueError(f"Empty spreadsheet: {path}")

    header = [str(c).strip().lower() if c else "" for c in header_raw]
    data: List[dict] = []
    for row in rows_iter:
        if all(c is None or str(c).strip() == "" for c in row):
            continue  # skip blank rows
        data.append({header[i]: (str(row[i]).strip() if row[i] is not None else "")
                      for i in range(min(len(header), len(row)))})
    wb.close()
    return data


def _read_csv(path: Path) -> List[dict]:
    delimiter = "\t" if path.suffix.lower() == ".tsv" else ","
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [
            {k.strip().lower(): v.strip() for k, v in row.items() if k}
            for row in reader
        ]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _resolve_col(row: dict, aliases: set) -> Optional[str]:
    """Find first matching alias in row keys, return value."""
    for key in row:
        if key in aliases:
            return row[key]
    return None


def _parse_rows(rows: List[dict], source: str) -> List[TableInfo]:
    if not rows:
        raise ValueError(f"No data rows found in {source}")

    # Validate that required columns exist
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())

    found_schema = all_keys & _SCHEMA_ALIASES
    found_table = all_keys & _TABLE_ALIASES
    found_group = all_keys & _GROUP_ALIASES
    if not found_schema:
        raise ValueError(
            f"Missing schema column in {source}.\n"
            f"  Expected one of: {_SCHEMA_ALIASES}\n"
            f"  Found columns: {sorted(all_keys)}"
        )
    if not found_table:
        raise ValueError(
            f"Missing table_name column in {source}.\n"
            f"  Expected one of: {_TABLE_ALIASES}\n"
            f"  Found columns: {sorted(all_keys)}"
        )
    if not found_group:
        raise ValueError(
            f"Missing group_id column in {source}.\n"
            f"  Expected one of: {_GROUP_ALIASES}\n"
            f"  Found columns: {sorted(all_keys)}"
        )

    tables: List[TableInfo] = []
    for i, row in enumerate(rows, start=2):  # row 2 = first data row in Excel
        schema = _resolve_col(row, _SCHEMA_ALIASES) or ""
        table = _resolve_col(row, _TABLE_ALIASES) or ""
        target = _resolve_col(row, _TARGET_ALIASES) or ""
        group_raw = _resolve_col(row, _GROUP_ALIASES) or ""

        if not schema or not table:
            log.warning("Row %d: skipping — empty schema or table_name", i)
            continue

        if not group_raw:
            raise ValueError(f"Row {i}: missing group_id for {schema}.{table}")
        try:
            group_id = int(group_raw)
        except ValueError:
            raise ValueError(f"Row {i}: group_id must be an integer, got '{group_raw}'")

        tables.append(TableInfo(schema=schema, name=table, group_id=group_id, target_schema=target))
    return tables


def _deduplicate(tables: List[TableInfo]) -> List[TableInfo]:
    seen: dict[str, TableInfo] = {}
    dupes = 0
    for t in tables:
        if t.fqn in seen:
            dupes += 1
            continue
        seen[t.fqn] = t
    if dupes:
        log.warning("Removed %d duplicate table entries", dupes)
    return list(seen.values())


def _validate(tables: List[TableInfo], source: str) -> None:
    if not tables:
        raise ValueError(f"No valid tables found in {source}")

    schemas = {t.schema for t in tables}
    log.info(
        "Loaded %d tables across %d schemas from %s: %s",
        len(tables), len(schemas), source, ", ".join(sorted(schemas)),
    )

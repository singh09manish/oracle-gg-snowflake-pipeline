"""
Domain models for the GoldenGate pipeline generator.

Everything flows from the Excel inventory:
  Excel → list[TableInfo] → list[ExtractGroup] → generated param files
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

@dataclass
class TableInfo:
    """One row from the Excel inventory."""

    schema: str
    name: str
    group_id: int = 0          # extract group assignment from inventory
    target_schema: str = ""    # defaults to source schema if blank
    pk_columns: Optional[str] = None  # populated by validator
    estimated_rows: Optional[int] = None
    has_lobs: bool = False
    has_trandata: bool = False  # populated by validator

    # --- Enable/Disable control ---
    enabled: bool = True               # False = excluded from .prm generation
    disabled_reason: str = ""          # why the table was disabled (user note or auto)
    disabled_at: str = ""              # ISO timestamp of when it was disabled

    def __post_init__(self) -> None:
        self.schema = self.schema.strip().upper()
        self.name = self.name.strip().upper()
        if not self.target_schema:
            self.target_schema = self.schema
        # Normalise enabled field from various Excel representations
        if isinstance(self.enabled, str):
            self.enabled = self.enabled.strip().upper() not in ("N", "NO", "FALSE", "0", "DISABLED")

    @property
    def fqn(self) -> str:
        """Fully qualified name: SCHEMA.TABLE"""
        return f"{self.schema}.{self.name}"

    @property
    def target_fqn(self) -> str:
        return f"{self.target_schema}.{self.name}"

    def __hash__(self) -> int:
        return hash(self.fqn)


# ---------------------------------------------------------------------------
# Extract Group (1 extract + 1 pump + 1 replicat + 1 props)
# ---------------------------------------------------------------------------

@dataclass
class ExtractGroup:
    """
    A balanced group of tables assigned to one extract chain.

    Each group produces:
      GG 19c:  EXTnn.prm   (integrated extract)
               PMPnn.prm   (PASSTHRU pump)
      GG 21c:  REPnn.prm   (BigData replicat)
               REPnn.props  (Snowflake JDBC handler properties)
    """

    group_id: int
    tables: List[TableInfo] = field(default_factory=list)

    # --- Derived names (8-char GG process name limit) ---

    @property
    def extract_name(self) -> str:
        return f"EXT{self.group_id:02d}"

    @property
    def pump_name(self) -> str:
        return f"PMP{self.group_id:02d}"

    @property
    def replicat_name(self) -> str:
        return f"REP{self.group_id:02d}"

    @property
    def extract_trail(self) -> str:
        """2-char trail prefix for primary extract trail."""
        return _trail_prefix("e", self.group_id)

    @property
    def pump_trail(self) -> str:
        """2-char trail prefix for pump (remote) trail."""
        return _trail_prefix("p", self.group_id)

    # --- Helpers ---

    @property
    def schemas(self) -> List[str]:
        return sorted({t.schema for t in self.tables})

    @property
    def target_schemas(self) -> List[str]:
        return sorted({t.target_schema for t in self.tables})

    @property
    def default_target_schema(self) -> str:
        """Most common target schema (used as JDBC default)."""
        counts: Dict[str, int] = {}
        for t in self.tables:
            counts[t.target_schema] = counts.get(t.target_schema, 0) + 1
        return max(counts, key=counts.get) if counts else ""

    @property
    def table_count(self) -> int:
        return len(self.tables)


# ---------------------------------------------------------------------------
# Pipeline manifest (returned after generation)
# ---------------------------------------------------------------------------

@dataclass
class PipelineManifest:
    """Summary produced after generate_all() — shows exactly what was built."""

    total_tables: int = 0
    total_groups: int = 0
    groups: List[ExtractGroup] = field(default_factory=list)
    files_written: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def summary_lines(self) -> List[str]:
        lines = [
            f"Tables:  {self.total_tables}",
            f"Groups:  {self.total_groups}",
            "",
            f"{'Group':<10} {'Extract':<10} {'Pump':<10} {'Replicat':<10} "
            f"{'Ext Trail':<12} {'Pump Trail':<12} {'Tables':<8} {'Schemas'}",
            "-" * 100,
        ]
        for g in self.groups:
            lines.append(
                f"{g.group_id:<10} {g.extract_name:<10} {g.pump_name:<10} {g.replicat_name:<10} "
                f"{g.extract_trail + '*':<12} {g.pump_trail + '*':<12} {g.table_count:<8} "
                f"{', '.join(g.schemas)}"
            )
        if self.warnings:
            lines += ["", "WARNINGS:"]
            for w in self.warnings:
                lines.append(f"  ⚠  {w}")
        lines += ["", f"Files written: {len(self.files_written)}"]
        return lines


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _trail_prefix(base: str, group_id: int) -> str:
    """
    2-char trail prefix.  group 1→e1, 9→e9, 10→ea, 35→ez.
    Supports up to 35 groups (500 tables / 15 per group = 34 groups max).
    """
    if group_id <= 9:
        return f"{base}{group_id}"
    return f"{base}{chr(ord('a') + group_id - 10)}"

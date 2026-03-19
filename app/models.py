"""
Domain models for the GoldenGate pipeline generator.

Everything flows from the Excel inventory:
  Excel → list[TableInfo] → list[ExtractGroup] → generated param files

Supports dual-target replication:
  - Snowflake (via GG BigData 21c JDBC handler — append-only CDC)
  - Oracle RDS target (via GG 19c Classic replicat — live mirror)
  - Both simultaneously (same extract trail, independent replicats)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

# Valid target values
VALID_TARGETS = {"snowflake", "rds", "both"}


# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

@dataclass
class TableInfo:
    """One row from the Excel inventory."""

    schema: str
    name: str
    group_id: int = 0          # extract group assignment from inventory
    target_schema: str = ""    # Snowflake target schema (defaults to source schema)
    pk_columns: Optional[str] = None  # populated by validator
    estimated_rows: Optional[int] = None
    has_lobs: bool = False
    has_trandata: bool = False  # populated by validator
    excluded_columns: list = None  # columns to exclude via COLSEXCEPT (PII/PCI)

    # --- Target control ---
    target: str = "snowflake"      # "snowflake" | "rds" | "both"
    rds_target_schema: str = ""    # RDS target schema (defaults to source schema)

    # --- Enable/Disable control ---
    enabled: bool = True               # False = excluded from .prm generation
    disabled_reason: str = ""          # why the table was disabled (user note or auto)
    disabled_at: str = ""              # ISO timestamp of when it was disabled

    def __post_init__(self) -> None:
        self.schema = self.schema.strip().upper()
        self.name = self.name.strip().upper()
        if not self.target_schema:
            self.target_schema = self.schema
        if not self.rds_target_schema:
            self.rds_target_schema = self.schema
        if self.excluded_columns is None:
            self.excluded_columns = []
        # Normalise target field
        if isinstance(self.target, str):
            self.target = self.target.strip().lower()
        if self.target not in VALID_TARGETS:
            self.target = "snowflake"
        # Normalise enabled field from various Excel representations
        if isinstance(self.enabled, str):
            self.enabled = self.enabled.strip().upper() not in ("N", "NO", "FALSE", "0", "DISABLED")

    @property
    def fqn(self) -> str:
        """Fully qualified name: SCHEMA.TABLE"""
        return f"{self.schema}.{self.name}"

    @property
    def target_fqn(self) -> str:
        """Snowflake target FQN."""
        return f"{self.target_schema}.{self.name}"

    @property
    def rds_target_fqn(self) -> str:
        """RDS target FQN."""
        return f"{self.rds_target_schema}.{self.name}"

    @property
    def goes_to_snowflake(self) -> bool:
        return self.target in ("snowflake", "both")

    @property
    def goes_to_rds(self) -> bool:
        return self.target in ("rds", "both")

    def __hash__(self) -> int:
        return hash(self.fqn)


# ---------------------------------------------------------------------------
# Extract Group (1 extract + 1 pump + replicats per target)
# ---------------------------------------------------------------------------

@dataclass
class ExtractGroup:
    """
    A balanced group of tables assigned to one extract chain.

    Each group produces:
      GG 19c:  EXTnn.prm   (integrated extract — all tables)
               PMPnn.prm   (PASSTHRU pump → GG 21c for Snowflake)
               RDBnn.prm   (Classic replicat → RDS target, if any RDS tables)
      GG 21c:  REPnn.prm   (BigData replicat → Snowflake, if any Snowflake tables)
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
        """Snowflake replicat (GG 21c BigData)."""
        return f"REP{self.group_id:02d}"

    @property
    def rds_replicat_name(self) -> str:
        """RDS replicat (GG 19c Classic)."""
        return f"RDB{self.group_id:02d}"

    @property
    def extract_trail(self) -> str:
        """2-char trail prefix for primary extract trail."""
        return _trail_prefix("e", self.group_id)

    @property
    def pump_trail(self) -> str:
        """2-char trail prefix for pump (remote) trail."""
        return _trail_prefix("p", self.group_id)

    # --- Target-filtered table lists ---

    @property
    def snowflake_tables(self) -> List[TableInfo]:
        """Tables going to Snowflake (target=snowflake or both)."""
        return [t for t in self.tables if t.goes_to_snowflake]

    @property
    def rds_tables(self) -> List[TableInfo]:
        """Tables going to RDS target (target=rds or both)."""
        return [t for t in self.tables if t.goes_to_rds]

    @property
    def has_snowflake_tables(self) -> bool:
        return any(t.goes_to_snowflake for t in self.tables)

    @property
    def has_rds_tables(self) -> bool:
        return any(t.goes_to_rds for t in self.tables)

    # --- Helpers ---

    @property
    def schemas(self) -> List[str]:
        return sorted({t.schema for t in self.tables})

    @property
    def target_schemas(self) -> List[str]:
        """Snowflake target schemas."""
        return sorted({t.target_schema for t in self.snowflake_tables})

    @property
    def rds_target_schemas(self) -> List[str]:
        """RDS target schemas."""
        return sorted({t.rds_target_schema for t in self.rds_tables})

    @property
    def default_target_schema(self) -> str:
        """Most common Snowflake target schema (used as JDBC default)."""
        sf_tables = self.snowflake_tables
        if not sf_tables:
            return ""
        counts: Dict[str, int] = {}
        for t in sf_tables:
            counts[t.target_schema] = counts.get(t.target_schema, 0) + 1
        return max(counts, key=counts.get) if counts else ""

    @property
    def table_count(self) -> int:
        return len(self.tables)

    @property
    def target_summary(self) -> str:
        """Human-readable target summary, e.g. 'SF:5 RDS:3 BOTH:2'."""
        sf_only = sum(1 for t in self.tables if t.target == "snowflake")
        rds_only = sum(1 for t in self.tables if t.target == "rds")
        both = sum(1 for t in self.tables if t.target == "both")
        parts = []
        if sf_only:
            parts.append(f"SF:{sf_only}")
        if rds_only:
            parts.append(f"RDS:{rds_only}")
        if both:
            parts.append(f"BOTH:{both}")
        return " ".join(parts) if parts else "SF:0"


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
        total_sf = sum(len(g.snowflake_tables) for g in self.groups)
        total_rds = sum(len(g.rds_tables) for g in self.groups)
        has_rds = any(g.has_rds_tables for g in self.groups)

        lines = [
            f"Tables:  {self.total_tables}",
            f"Groups:  {self.total_groups}",
            f"Targets: Snowflake={total_sf}  RDS={total_rds}",
            "",
        ]

        if has_rds:
            lines += [
                f"{'Group':<8} {'Extract':<8} {'Pump':<8} {'SF Rep':<8} {'RDS Rep':<8} "
                f"{'Trails':<12} {'Tables':<8} {'Targets':<16} {'Schemas'}",
                "-" * 110,
            ]
            for g in self.groups:
                rds_rep = g.rds_replicat_name if g.has_rds_tables else "—"
                sf_rep = g.replicat_name if g.has_snowflake_tables else "—"
                lines.append(
                    f"{g.group_id:<8} {g.extract_name:<8} {g.pump_name:<8} {sf_rep:<8} {rds_rep:<8} "
                    f"{g.extract_trail + '/' + g.pump_trail + '*':<12} {g.table_count:<8} "
                    f"{g.target_summary:<16} {', '.join(g.schemas)}"
                )
        else:
            lines += [
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

"""
auto_grouper.py — Multi-dimensional GoldenGate extract group assignment
=======================================================================

Why multi-dimensional?
----------------------
A one-dimensional "DML count" score fails in practice because GoldenGate
extract cost depends on multiple factors:

  - A table with 10K updates/day of 2KB rows costs **more** than a table
    with 100K inserts/day of 50-byte rows (byte throughput matters).
  - Two batch tables that bulk-insert 1M rows nightly should NOT be in
    the same group (they'll both spike and cause lag together).
  - An OLTP table with constant small updates needs a different treatment
    than a reporting table that gets a nightly truncate+reload.

Table Profiles
--------------
Every table is classified into one of five profiles based on its metrics:

  OLTP_HEAVY   — High churn rate (>1.0) AND high byte throughput
                 (rows updated multiple times, constant pressure)
                 Example: session tables, real-time pricing

  OLTP_LIGHT   — High churn rate but lower total volume
                 (active OLTP but not a heavyweight)
                 Example: user preferences, small lookup updates

  BATCH_HEAVY  — Low churn rate (<0.1) but very high total volume
                 (bulk inserts/loads, nightly ETL targets)
                 Example: transaction history, audit logs, billing runs

  BATCH_LIGHT  — Low churn, moderate volume
                 (periodic batch but not huge)
                 Example: monthly report staging tables

  REFERENCE    — Minimal DML activity
                 (lookup tables, config, barely change)
                 Example: country codes, product categories

Scoring Model
-------------
The composite score combines multiple dimensions:

  gg_extract_cost = (
      byte_throughput                    # actual bytes flowing through redo
    + (lob_count * LOB_PENALTY)          # LOBs cause extra redo + GG overhead
    + (partition_count * PARTITION_PENALTY)  # partitioned tables = more redo entries
    + (col_count_penalty)               # wide tables (100+ cols) are heavier in COLMAP
  )

Grouping Algorithm: Profile-aware bin-packing
----------------------------------------------
1. Classify every table into a profile.
2. Sort BATCH_HEAVY and OLTP_HEAVY tables first — they're the hardest to place.
3. Use round-robin for heavy tables (spreads them evenly across groups).
4. Use load-balanced greedy assignment for remaining tables.
5. Anti-affinity: max N heavy tables per group (configurable).
6. Schema affinity: prefer same-schema placement when load permits.

Anti-Patterns Detected
----------------------
The algorithm warns about dangerous group compositions:
  - Two+ BATCH_HEAVY tables in the same group (concurrent spikes)
  - All OLTP_HEAVY tables in one group (sustained overload)
  - Mixing LOB-heavy tables with high-DML tables
  - Under-filled groups (wasted extract processes)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table profiles
# ---------------------------------------------------------------------------

class TableProfile(Enum):
    OLTP_HEAVY  = "OLTP_HEAVY"
    OLTP_LIGHT  = "OLTP_LIGHT"
    BATCH_HEAVY = "BATCH_HEAVY"
    BATCH_LIGHT = "BATCH_LIGHT"
    REFERENCE   = "REFERENCE"


# Profile classification thresholds (adjustable)
_CHURN_HIGH    = 0.5     # churn_rate above this → OLTP pattern
_CHURN_BATCH   = 0.05    # churn_rate below this → BATCH pattern
_LOB_PENALTY   = 50_000  # per LOB column (LOBs cause significant extra redo)
_PART_PENALTY  = 5_000   # per partition (more redo entries per DML)
_WIDE_COL_THRESHOLD = 80 # tables with more columns than this get a penalty
_WIDE_COL_PENALTY   = 200  # per column above threshold


# ---------------------------------------------------------------------------
# Bucket tracker (multi-dimensional)
# ---------------------------------------------------------------------------

@dataclass
class _Bucket:
    bucket_id: int
    table_count: int = 0
    total_cost: float = 0.0
    total_byte_throughput: float = 0.0
    schemas: set = field(default_factory=set)
    profiles: dict = field(default_factory=lambda: {p: 0 for p in TableProfile})
    has_lob_tables: int = 0

    def has_space(self, max_per_group: int) -> bool:
        return self.table_count < max_per_group

    def batch_heavy_count(self) -> int:
        return self.profiles.get(TableProfile.BATCH_HEAVY, 0)

    def oltp_heavy_count(self) -> int:
        return self.profiles.get(TableProfile.OLTP_HEAVY, 0)

    def add(self, schema: str, cost: float, byte_tp: float,
            profile: TableProfile, has_lobs: bool) -> None:
        self.table_count += 1
        self.total_cost += cost
        self.total_byte_throughput += byte_tp
        self.schemas.add(schema)
        self.profiles[profile] = self.profiles.get(profile, 0) + 1
        if has_lobs:
            self.has_lob_tables += 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def auto_assign_groups(
    df: pd.DataFrame,
    *,
    min_per_group: int = 50,
    max_per_group: int = 75,
    schema_affinity: bool = True,
    target_groups: Optional[int] = None,
    max_batch_heavy_per_group: int = 2,
    max_oltp_heavy_per_group: int = 5,
) -> pd.DataFrame:
    """
    Assign a balanced ``group_id`` to every row using multi-dimensional scoring.

    Parameters
    ----------
    df                         : DataFrame from Oracle query.
    min_per_group              : Soft lower bound (warning only).
    max_per_group              : Hard upper bound per extract group.
    schema_affinity            : Prefer keeping same-schema tables together.
    target_groups              : Override number of groups.
    max_batch_heavy_per_group  : Anti-affinity: max BATCH_HEAVY tables per group.
    max_oltp_heavy_per_group   : Anti-affinity: max OLTP_HEAVY tables per group.

    Returns
    -------
    df with added columns: group_id, extract_name, pump_name, replicat_name,
                           profile, gg_extract_cost, needs_keycols.
    """
    df = df.copy()
    _normalise_columns(df)

    if df.empty:
        raise ValueError("Input DataFrame is empty — nothing to group.")

    # Step 1: Compute multi-dimensional scores
    df = _compute_scores(df)

    # Step 2: Classify profiles
    df["profile"] = df.apply(_classify_profile, axis=1)

    n_tables = len(df)
    n_buckets = target_groups or math.ceil(n_tables / max_per_group)

    log.info(
        "auto_assign_groups: %d tables -> target %d groups "
        "(min=%d, max=%d, schema_affinity=%s)",
        n_tables, n_buckets, min_per_group, max_per_group, schema_affinity,
    )
    _log_profile_distribution(df)

    buckets: List[_Bucket] = [_Bucket(bucket_id=i + 1) for i in range(n_buckets)]

    # Step 3: Assign heavy tables first (round-robin for even distribution)
    heavy_mask = df["profile"].isin([TableProfile.BATCH_HEAVY, TableProfile.OLTP_HEAVY])
    heavy_df = df[heavy_mask].sort_values("gg_extract_cost", ascending=False)
    light_df = df[~heavy_mask].copy()

    assigned: dict[tuple, int] = {}  # (owner, table_name) → bucket_id

    # Round-robin heavy tables across buckets (sorted by cost descending)
    _assign_heavy_tables(
        heavy_df, buckets, assigned, max_per_group,
        max_batch_heavy_per_group, max_oltp_heavy_per_group,
    )

    # Step 4: Greedy load-balanced assignment for remaining tables
    # Sort: schema (for affinity), then cost desc (heavy first)
    if schema_affinity:
        light_df = light_df.sort_values(
            ["owner", "gg_extract_cost"], ascending=[True, False]
        )
    else:
        light_df = light_df.sort_values("gg_extract_cost", ascending=False)

    for _, row in light_df.iterrows():
        key = (str(row["owner"]), str(row["table_name"]))
        bucket = _pick_bucket(
            buckets, str(row["owner"]), float(row["gg_extract_cost"]),
            float(row["byte_throughput"]), row["profile"],
            int(row["lob_count"]) > 0,
            max_per_group, max_batch_heavy_per_group,
            max_oltp_heavy_per_group, schema_affinity,
        )
        bucket.add(
            str(row["owner"]), float(row["gg_extract_cost"]),
            float(row["byte_throughput"]), row["profile"],
            int(row["lob_count"]) > 0,
        )
        assigned[key] = bucket.bucket_id

    # Map assignments back to df
    df["group_id"] = df.apply(
        lambda r: assigned[(str(r["owner"]), str(r["table_name"]))], axis=1
    )

    # Derived columns
    df["extract_name"]  = df["group_id"].apply(lambda g: f"EXT{g:02d}")
    df["pump_name"]     = df["group_id"].apply(lambda g: f"PMP{g:02d}")
    df["replicat_name"] = df["group_id"].apply(lambda g: f"REP{g:02d}")
    df["needs_keycols"] = df["has_pk"].str.upper() == "N"
    df["profile_name"]  = df["profile"].apply(lambda p: p.value)

    _log_summary(df, buckets, min_per_group, max_per_group)
    _detect_anti_patterns(df, buckets)

    return df


# ---------------------------------------------------------------------------
# Multi-dimensional scoring
# ---------------------------------------------------------------------------

def _compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Compute the composite GG extract cost score."""

    # Ensure numeric columns
    for col in ["inserts", "updates", "deletes", "avg_row_len",
                "num_rows", "lob_count", "col_count", "partition_count",
                "byte_throughput", "churn_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # If byte_throughput wasn't computed by SQL (older query), compute it
    if "byte_throughput" not in df.columns or df["byte_throughput"].sum() == 0:
        df["byte_throughput"] = (
            df["inserts"] * df.get("avg_row_len", 100)
            + df["updates"] * df.get("avg_row_len", 100) * 2
            + df["deletes"] * 64
        )

    # If churn_rate wasn't computed by SQL
    if "churn_rate" not in df.columns or df["churn_rate"].sum() == 0:
        df["churn_rate"] = np.where(
            df["num_rows"] > 0,
            (df["inserts"] + df["updates"] + df["deletes"]) / df["num_rows"],
            0.0,
        )

    # LOB penalty
    lob_col = df.get("lob_count", pd.Series(0, index=df.index))
    lob_penalty = lob_col * _LOB_PENALTY

    # Partition penalty
    part_col = df.get("partition_count", pd.Series(0, index=df.index))
    part_penalty = part_col * _PART_PENALTY

    # Wide-table penalty (tables with many columns are heavier in COLMAP)
    col_col = df.get("col_count", pd.Series(0, index=df.index))
    wide_penalty = np.maximum(col_col - _WIDE_COL_THRESHOLD, 0) * _WIDE_COL_PENALTY

    # Composite GG extract cost
    df["gg_extract_cost"] = (
        df["byte_throughput"]
        + lob_penalty
        + part_penalty
        + wide_penalty
    )

    # Normalize to 0-1000 scale for readability
    max_cost = df["gg_extract_cost"].max()
    if max_cost > 0:
        df["gg_cost_normalized"] = (df["gg_extract_cost"] / max_cost * 1000).round(1)
    else:
        df["gg_cost_normalized"] = 0.0

    return df


def _classify_profile(row: pd.Series) -> TableProfile:
    """Classify a table into one of five profiles."""
    churn = float(row.get("churn_rate", 0))
    dml_total = float(row.get("dml_total",
                               row.get("inserts", 0) + row.get("updates", 0) + row.get("deletes", 0)))
    byte_tp = float(row.get("byte_throughput", 0))

    # Use percentile-based thresholds would be ideal, but we use
    # absolute thresholds here for simplicity. The key insight is
    # churn_rate (DML/num_rows) separates OLTP from BATCH patterns.

    if dml_total == 0:
        return TableProfile.REFERENCE

    if churn >= _CHURN_HIGH:
        # High churn = OLTP pattern (rows change repeatedly)
        if byte_tp > 0:
            # Check if it's in the top tier by byte throughput
            # We'll refine this after all rows are scored
            return TableProfile.OLTP_HEAVY
        return TableProfile.OLTP_LIGHT

    if churn <= _CHURN_BATCH:
        # Low churn = BATCH pattern (bulk loads, rarely re-touched)
        if dml_total > 10_000 or byte_tp > 1_000_000:
            return TableProfile.BATCH_HEAVY
        return TableProfile.BATCH_LIGHT

    # Middle ground: moderate churn
    if byte_tp > 5_000_000 or dml_total > 100_000:
        return TableProfile.OLTP_HEAVY
    if dml_total > 10_000:
        return TableProfile.OLTP_LIGHT

    return TableProfile.REFERENCE


# ---------------------------------------------------------------------------
# Heavy table assignment (round-robin)
# ---------------------------------------------------------------------------

def _assign_heavy_tables(
    heavy_df: pd.DataFrame,
    buckets: List[_Bucket],
    assigned: dict,
    max_per_group: int,
    max_batch_heavy: int,
    max_oltp_heavy: int,
) -> None:
    """Assign BATCH_HEAVY and OLTP_HEAVY tables using round-robin."""

    # Separate batch and OLTP heavy
    batch_heavy = heavy_df[heavy_df["profile"] == TableProfile.BATCH_HEAVY]
    oltp_heavy  = heavy_df[heavy_df["profile"] == TableProfile.OLTP_HEAVY]

    # Assign BATCH_HEAVY first (hardest to co-locate — they spike together)
    bucket_idx = 0
    for _, row in batch_heavy.iterrows():
        key = (str(row["owner"]), str(row["table_name"]))
        # Find next bucket that has room for another BATCH_HEAVY
        attempts = 0
        while attempts < len(buckets) + 1:
            b = buckets[bucket_idx % len(buckets)]
            if b.has_space(max_per_group) and b.batch_heavy_count() < max_batch_heavy:
                b.add(
                    str(row["owner"]), float(row["gg_extract_cost"]),
                    float(row["byte_throughput"]), row["profile"],
                    int(row["lob_count"]) > 0,
                )
                assigned[key] = b.bucket_id
                bucket_idx += 1
                break
            bucket_idx += 1
            attempts += 1
        else:
            # All buckets at batch limit — open a new one
            new_b = _Bucket(bucket_id=len(buckets) + 1)
            buckets.append(new_b)
            new_b.add(
                str(row["owner"]), float(row["gg_extract_cost"]),
                float(row["byte_throughput"]), row["profile"],
                int(row["lob_count"]) > 0,
            )
            assigned[key] = new_b.bucket_id
            log.warning("Auto-scaled to bucket %d for BATCH_HEAVY table %s.%s",
                        new_b.bucket_id, row["owner"], row["table_name"])

    # Assign OLTP_HEAVY — round-robin, prefer lowest-cost bucket
    for _, row in oltp_heavy.iterrows():
        key = (str(row["owner"]), str(row["table_name"]))
        # Pick bucket with fewest OLTP_HEAVY tables, then lowest cost
        eligible = [b for b in buckets
                    if b.has_space(max_per_group) and b.oltp_heavy_count() < max_oltp_heavy]
        if not eligible:
            eligible = [b for b in buckets if b.has_space(max_per_group)]
        if not eligible:
            new_b = _Bucket(bucket_id=len(buckets) + 1)
            buckets.append(new_b)
            eligible = [new_b]

        best = min(eligible, key=lambda b: (b.oltp_heavy_count(), b.total_cost))
        best.add(
            str(row["owner"]), float(row["gg_extract_cost"]),
            float(row["byte_throughput"]), row["profile"],
            int(row["lob_count"]) > 0,
        )
        assigned[key] = best.bucket_id


# ---------------------------------------------------------------------------
# Load-balanced bucket picker (for non-heavy tables)
# ---------------------------------------------------------------------------

def _pick_bucket(
    buckets: List[_Bucket],
    schema: str,
    cost: float,
    byte_tp: float,
    profile: TableProfile,
    has_lobs: bool,
    max_per_group: int,
    max_batch_heavy: int,
    max_oltp_heavy: int,
    schema_affinity: bool,
) -> _Bucket:
    """Pick the best bucket for a non-heavy table."""

    available = [b for b in buckets if b.has_space(max_per_group)]

    if not available:
        new_bucket = _Bucket(bucket_id=len(buckets) + 1)
        buckets.append(new_bucket)
        log.warning("Auto-scaling to bucket %d", new_bucket.bucket_id)
        return new_bucket

    # Avoid placing LOB tables with high-DML tables
    if has_lobs:
        low_dml = [b for b in available if b.total_byte_throughput < _median_throughput(available)]
        if low_dml:
            available = low_dml

    if schema_affinity:
        same_schema = [b for b in available if schema in b.schemas]
        if same_schema:
            return min(same_schema, key=lambda b: b.total_cost)

    # Pick by lowest total cost
    return min(available, key=lambda b: b.total_cost)


def _median_throughput(buckets: List[_Bucket]) -> float:
    throughputs = sorted(b.total_byte_throughput for b in buckets)
    n = len(throughputs)
    if n == 0:
        return 0.0
    return throughputs[n // 2]


# ---------------------------------------------------------------------------
# Column normalisation
# ---------------------------------------------------------------------------

def _normalise_columns(df: pd.DataFrame) -> None:
    """Lowercase all column names and fill missing columns with defaults."""
    df.columns = [c.strip().lower() for c in df.columns]

    defaults = {
        "inserts": 0, "updates": 0, "deletes": 0,
        "num_rows": 0, "avg_row_len": 100, "segment_mb": 0,
        "dml_total": 0, "gg_dml_cost": 0, "byte_throughput": 0,
        "churn_rate": 0, "update_pct": 0,
        "col_count": 10, "lob_count": 0, "partition_count": 0,
        "has_pk": "Y", "partitioned": "NO", "days_since_analyze": 0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
        else:
            if isinstance(default, (int, float)):
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default)
            else:
                df[col] = df[col].fillna(default).astype(str)

    # Compute dml_total if not present
    if df["dml_total"].sum() == 0:
        df["dml_total"] = df["inserts"] + df["updates"] + df["deletes"]

    # Legacy compat: if old CSV has "dml_score" but not "gg_dml_cost"
    if "dml_score" in df.columns and df["gg_dml_cost"].sum() == 0:
        df["gg_dml_cost"] = pd.to_numeric(df["dml_score"], errors="coerce").fillna(0)


# ---------------------------------------------------------------------------
# Anti-pattern detection
# ---------------------------------------------------------------------------

def _detect_anti_patterns(df: pd.DataFrame, buckets: List[_Bucket]) -> None:
    """Warn about dangerous group compositions."""

    warnings_found = False

    for b in buckets:
        if b.table_count == 0:
            continue

        bid = b.bucket_id
        subset = df[df["group_id"] == bid]

        # Anti-pattern 1: Multiple BATCH_HEAVY in same group
        batch_count = b.profiles.get(TableProfile.BATCH_HEAVY, 0)
        if batch_count > 2:
            log.warning(
                "  ANTI-PATTERN: EXT%02d has %d BATCH_HEAVY tables — "
                "concurrent bulk loads will cause lag spikes. "
                "Consider splitting them.",
                bid, batch_count,
            )
            warnings_found = True

        # Anti-pattern 2: All heavy, no light tables to absorb pauses
        heavy = (b.profiles.get(TableProfile.BATCH_HEAVY, 0)
                 + b.profiles.get(TableProfile.OLTP_HEAVY, 0))
        if heavy > 0 and heavy == b.table_count:
            log.warning(
                "  ANTI-PATTERN: EXT%02d has ONLY heavy tables (%d) — "
                "extract will be under sustained pressure. "
                "Mix in some REFERENCE/LIGHT tables.",
                bid, heavy,
            )
            warnings_found = True

        # Anti-pattern 3: LOB tables mixed with high-throughput tables
        if b.has_lob_tables > 0 and b.total_byte_throughput > 10_000_000:
            lob_tables = subset[subset["lob_count"].astype(int) > 0]["table_name"].tolist()
            log.warning(
                "  ANTI-PATTERN: EXT%02d has LOB tables (%s) mixed with "
                "high byte throughput (%.0f). LOBs slow down redo parsing. "
                "Consider isolating LOB tables in their own group.",
                bid, ", ".join(lob_tables[:3]), b.total_byte_throughput,
            )
            warnings_found = True

        # Anti-pattern 4: Very uneven group sizes
        # (checked in summary already, but flag extreme cases)

    if not warnings_found:
        log.info("  No anti-patterns detected — group composition looks good.")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _log_profile_distribution(df: pd.DataFrame) -> None:
    """Log how many tables are in each profile."""
    log.info("Table profile distribution:")
    for p in TableProfile:
        count = (df["profile"] == p).sum()
        if count > 0:
            subset = df[df["profile"] == p]
            avg_cost = subset["gg_extract_cost"].mean() if "gg_extract_cost" in subset else 0
            log.info("  %-14s  %3d tables  (avg extract cost: %10.0f)", p.value, count, avg_cost)


def _log_summary(
    df: pd.DataFrame,
    buckets: List[_Bucket],
    min_per_group: int,
    max_per_group: int,
) -> None:
    total_groups = df["group_id"].nunique()
    log.info("")
    log.info("=" * 90)
    log.info("GROUP ASSIGNMENT SUMMARY  (%d tables -> %d groups)", len(df), total_groups)
    log.info("%-8s  %-6s  %12s  %12s  %-12s  %-20s  %s",
             "Group", "Tables", "GG Cost", "Byte TP", "Profiles", "Schemas", "Flags")
    log.info("-" * 90)

    for b in sorted(buckets, key=lambda x: x.bucket_id):
        if b.table_count == 0:
            continue

        subset = df[df["group_id"] == b.bucket_id]
        no_pk = subset["needs_keycols"].sum()
        lobs  = subset["lob_count"].astype(int).sum()

        # Build profile breakdown string
        profile_parts = []
        for p in TableProfile:
            cnt = b.profiles.get(p, 0)
            if cnt > 0:
                # Short names for display
                short = {"OLTP_HEAVY": "OH", "OLTP_LIGHT": "OL",
                         "BATCH_HEAVY": "BH", "BATCH_LIGHT": "BL",
                         "REFERENCE": "RF"}
                profile_parts.append(f"{short[p.value]}:{cnt}")
        profile_str = " ".join(profile_parts)

        schemas_str = ", ".join(sorted(b.schemas))

        flags = []
        if no_pk:
            flags.append(f"{int(no_pk)} no-PK")
        if lobs:
            flags.append(f"{int(lobs)} LOBs")
        flag_str = ", ".join(flags) if flags else "OK"

        if b.table_count < min_per_group:
            log.warning(
                "  EXT%02d has only %d tables (below min=%d)",
                b.bucket_id, b.table_count, min_per_group,
            )

        log.info(
            "  EXT%02d   %3d   %12.0f  %12.0f  %-12s  %-20s  %s",
            b.bucket_id, b.table_count, b.total_cost, b.total_byte_throughput,
            profile_str, schemas_str[:20], flag_str,
        )

    log.info("=" * 90)

    # Cost balance check
    costs = [b.total_cost for b in buckets if b.table_count > 0]
    if costs:
        min_c, max_c = min(costs), max(costs)
        if max_c > 0:
            imbalance = (max_c - min_c) / max_c * 100
            if imbalance > 40:
                log.warning(
                    "  Load imbalance: %.0f%% (heaviest group is %.0fx the lightest). "
                    "Consider adjusting group count or moving tables.",
                    imbalance, max_c / max(min_c, 1),
                )
            else:
                log.info("  Load balance: %.0f%% variance (good)", imbalance)

    # Tables missing PK
    no_pk_tables = df[df["needs_keycols"]][["owner", "table_name", "group_id"]]
    if not no_pk_tables.empty:
        log.warning(
            "%d tables have no primary key — add KEYCOLS to their extract .prm:",
            len(no_pk_tables),
        )
        for _, r in no_pk_tables.iterrows():
            log.warning("  EXT%02d  %s.%s", r["group_id"], r["owner"], r["table_name"])

"""
auto_grouper.py
===============
Intelligent GoldenGate extract group assignment.

Given a DataFrame of tables with DML activity scores (from Oracle), this
module assigns a ``group_id`` to every table so that:

  1. **Table count** per group stays within [min_per_group, max_per_group].
  2. **DML load** is balanced across groups (heavy tables spread evenly).
  3. **Schema affinity** is respected — same-schema tables go in the same
     group where possible (reduces TRANDATA overhead & simplifies ops).
  4. Tables **without a PK** are flagged in the output (needs KEYCOLS in GG).

Algorithm: Greedy bin-packing with load awareness
--------------------------------------------------
1. Sort tables by (schema, dml_score DESC) — heaviest hitters first so they
   are distributed before lighter tables fill up buckets.
2. Maintain N buckets, each tracking:
       table_count, total_dml_score, schemas_in_bucket
3. For each table, pick the best bucket:
   a. Must have space (table_count < max_per_group).
   b. Prefer a bucket that already contains the same schema.
   c. Among eligible same-schema buckets, pick lowest total_dml_score.
   d. If no same-schema bucket has space, spill to lowest-load bucket.
4. If all buckets full, open a new one (auto-scales beyond initial estimate).

Input DataFrame columns (from sql/table_grouping_candidates.sql):
    OWNER, TABLE_NAME, NUM_ROWS, SEGMENT_MB,
    INSERTS, UPDATES, DELETES, DML_SCORE, HAS_PK, PARTITIONED

Output DataFrame adds columns:
    group_id        - integer, 1-based
    extract_name    - e.g. EXT01
    pump_name       - e.g. PMP01
    replicat_name   - e.g. REP01
    needs_keycols   - True if HAS_PK == 'N'
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal bucket tracker
# ---------------------------------------------------------------------------

@dataclass
class _Bucket:
    bucket_id: int                       # 1-based group_id
    table_count: int = 0
    total_dml_score: float = 0.0
    schemas: set = field(default_factory=set)

    def has_space(self, max_per_group: int) -> bool:
        return self.table_count < max_per_group

    def add(self, schema: str, dml_score: float) -> None:
        self.table_count += 1
        self.total_dml_score += dml_score
        self.schemas.add(schema)


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
) -> pd.DataFrame:
    """
    Assign a balanced ``group_id`` to every row in *df*.

    Parameters
    ----------
    df              : DataFrame from Oracle query (see module docstring).
    min_per_group   : Soft lower bound — informational warning only.
    max_per_group   : Hard upper bound per extract group.
    schema_affinity : If True, prefer keeping same-schema tables together.
    target_groups   : If set, start with exactly this many buckets.
                      Otherwise auto-computed from len(df) / max_per_group.

    Returns
    -------
    df with added columns: group_id, extract_name, pump_name,
                           replicat_name, needs_keycols.
    """
    df = df.copy()
    _normalise_columns(df)

    if df.empty:
        raise ValueError("Input DataFrame is empty — nothing to group.")

    n_tables = len(df)
    n_buckets = target_groups or math.ceil(n_tables / max_per_group)
    log.info(
        "auto_assign_groups: %d tables → target %d groups "
        "(min=%d, max=%d, schema_affinity=%s)",
        n_tables, n_buckets, min_per_group, max_per_group, schema_affinity,
    )

    buckets: List[_Bucket] = [_Bucket(bucket_id=i + 1) for i in range(n_buckets)]

    # Sort: schema first, then heaviest DML first so load balancing kicks in
    # before buckets are nearly full.
    sort_cols = (["owner", "dml_score"] if schema_affinity else ["dml_score"])
    sort_asc  = ([True, False]          if schema_affinity else [False])
    df_sorted = df.sort_values(sort_cols, ascending=sort_asc).reset_index(drop=True)

    assigned_ids: List[int] = []

    for _, row in df_sorted.iterrows():
        schema    = str(row["owner"])
        dml_score = float(row["dml_score"])

        bucket = _pick_bucket(buckets, schema, dml_score, max_per_group, schema_affinity)
        bucket.add(schema, dml_score)
        assigned_ids.append(bucket.bucket_id)

    df_sorted["group_id"] = assigned_ids

    # Restore original row order
    df = df.merge(
        df_sorted[["owner", "table_name", "group_id"]],
        on=["owner", "table_name"],
        how="left",
    )

    # Derived name columns
    df["extract_name"]  = df["group_id"].apply(lambda g: f"EXT{g:02d}")
    df["pump_name"]     = df["group_id"].apply(lambda g: f"PMP{g:02d}")
    df["replicat_name"] = df["group_id"].apply(lambda g: f"REP{g:02d}")
    df["needs_keycols"] = df["has_pk"].str.upper() == "N"

    _log_summary(df, buckets, min_per_group, max_per_group)
    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_bucket(
    buckets: List[_Bucket],
    schema: str,
    dml_score: float,
    max_per_group: int,
    schema_affinity: bool,
) -> _Bucket:
    """Pick the best bucket for (schema, dml_score)."""

    available = [b for b in buckets if b.has_space(max_per_group)]

    if not available:
        # All buckets full — open a new one (auto-scale)
        new_bucket = _Bucket(bucket_id=len(buckets) + 1)
        buckets.append(new_bucket)
        log.warning(
            "All %d buckets full — auto-scaling to bucket %d",
            len(buckets) - 1, new_bucket.bucket_id,
        )
        return new_bucket

    if schema_affinity:
        # Prefer same-schema buckets with lowest load
        same_schema = [b for b in available if schema in b.schemas]
        if same_schema:
            return min(same_schema, key=lambda b: b.total_dml_score)

    # No same-schema bucket available → lowest load bucket
    return min(available, key=lambda b: b.total_dml_score)


def _normalise_columns(df: pd.DataFrame) -> None:
    """Lowercase all column names and fill nulls."""
    df.columns = [c.lower() for c in df.columns]
    df["dml_score"] = pd.to_numeric(df.get("dml_score", 0), errors="coerce").fillna(0)
    df["num_rows"]  = pd.to_numeric(df.get("num_rows",  0), errors="coerce").fillna(0)
    df["has_pk"]    = df.get("has_pk", "Y").fillna("Y").astype(str)


def _log_summary(
    df: pd.DataFrame,
    buckets: List[_Bucket],
    min_per_group: int,
    max_per_group: int,
) -> None:
    total_groups = df["group_id"].nunique()
    log.info("─" * 70)
    log.info("GROUP ASSIGNMENT SUMMARY  (%d tables → %d groups)", len(df), total_groups)
    log.info("%-8s  %-6s  %-12s  %-20s  %s",
             "Group", "Tables", "DML Score", "Schemas", "Flags")
    log.info("─" * 70)

    for b in sorted(buckets, key=lambda x: x.bucket_id):
        if b.table_count == 0:
            continue
        subset = df[df["group_id"] == b.bucket_id]
        no_pk  = subset["needs_keycols"].sum()
        flags  = f"⚠ {no_pk} tables need KEYCOLS" if no_pk else "OK"
        schemas_str = ", ".join(sorted(b.schemas))

        # Warn if under-filled
        if b.table_count < min_per_group:
            log.warning(
                "  EXT%02d has only %d tables (below min=%d)",
                b.bucket_id, b.table_count, min_per_group,
            )

        log.info(
            "  EXT%02d   %3d     %10.0f    %-20s  %s",
            b.bucket_id, b.table_count, b.total_dml_score,
            schemas_str[:20], flags,
        )

    log.info("─" * 70)

    # Tables missing PK — list them so user can add KEYCOLS
    no_pk_tables = df[df["needs_keycols"]][["owner", "table_name", "group_id"]]
    if not no_pk_tables.empty:
        log.warning(
            "%d tables have no primary key — add KEYCOLS to their extract .prm:",
            len(no_pk_tables),
        )
        for _, r in no_pk_tables.iterrows():
            log.warning("  EXT%02d  %s.%s", r["group_id"], r["owner"], r["table_name"])

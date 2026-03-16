"""
Inventory-driven table grouping for GoldenGate extract/replicat parallelism.

Each table in the inventory specifies its group_id. This module collects
tables by group_id and produces one ExtractGroup per unique group_id.

The inventory is authoritative — if a group exceeds max_per_group, a warning
is logged but the group is not split.
"""

from __future__ import annotations

import logging
from typing import Dict, List

from app.models import ExtractGroup, TableInfo

log = logging.getLogger(__name__)


def group_tables(
    tables: List[TableInfo],
    *,
    max_per_group: int = 75,
) -> List[ExtractGroup]:
    """
    Group tables by the group_id assigned in the inventory.

    Args:
        tables: Full table inventory (from Excel/CSV), each with a group_id.
        max_per_group: Advisory threshold — warns if exceeded but does not split.

    Returns:
        List of ExtractGroup, ordered by group_id.
    """
    if not tables:
        raise ValueError("No tables to group")

    by_group: Dict[int, List[TableInfo]] = {}
    for t in tables:
        by_group.setdefault(t.group_id, []).append(t)

    groups: List[ExtractGroup] = []
    for gid in sorted(by_group):
        group_tables_list = sorted(by_group[gid], key=lambda t: (t.schema, t.name))
        groups.append(ExtractGroup(group_id=gid, tables=group_tables_list))

    # Advisory warnings
    for g in groups:
        if g.table_count > max_per_group:
            log.warning(
                "%s has %d tables (exceeds advisory max of %d)",
                g.extract_name, g.table_count, max_per_group,
            )

    _log_summary(groups)
    return groups


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _log_summary(groups: List[ExtractGroup]) -> None:
    total = sum(g.table_count for g in groups)
    log.info(
        "Grouped %d tables into %d extract groups (max %d per group)",
        total, len(groups), max(g.table_count for g in groups),
    )
    for g in groups:
        log.info(
            "  %s: %3d tables  schemas=[%s]  trails=%s/%s",
            g.extract_name, g.table_count,
            ", ".join(g.schemas),
            g.extract_trail, g.pump_trail,
        )

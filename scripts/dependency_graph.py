#!/usr/bin/env python3
"""
GG Process Dependency Graph — Visualize extract -> pump -> replicat chains.

Reads the table inventory (input/table_inventory.xlsx) and pipeline.yaml
to generate a visual dependency graph showing the full data flow from
Oracle RDS through GoldenGate 19c/21c to Snowflake.

Output formats:
    --format text   ASCII art dependency graph in terminal (default)
    --format dot    Graphviz DOT file (output/graphs/pipeline.dot)
    --format html   Self-contained HTML with SVG (output/graphs/pipeline.html)

Usage:
    python3 scripts/dependency_graph.py
    python3 scripts/dependency_graph.py --format dot
    python3 scripts/dependency_graph.py --format html
    python3 scripts/dependency_graph.py --format text --no-color

    # Via ggctl
    ggctl graph [--format text|dot|html]
"""

from __future__ import annotations

import argparse
import html as html_lib
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.yaml"
INVENTORY_FILE = PROJECT_ROOT / "input" / "table_inventory.xlsx"
OUTPUT_DIR = PROJECT_ROOT / "output" / "graphs"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger("dependency_graph")

# ---------------------------------------------------------------------------
# Add project root to path so we can import app modules
# ---------------------------------------------------------------------------
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load pipeline.yaml configuration."""
    path = Path(config_path) if config_path else CONFIG_FILE
    if not path.exists():
        log.error("Config file not found: %s", path)
        sys.exit(1)
    with open(path) as f:
        return yaml.safe_load(f)


def load_groups(inventory_path: Optional[str] = None):
    """Load inventory and build extract groups."""
    from app.inventory import read_inventory
    from app.grouper import group_tables

    path = Path(inventory_path) if inventory_path else INVENTORY_FILE
    if not path.exists():
        log.error("Inventory file not found: %s", path)
        sys.exit(1)

    tables = read_inventory(path)
    config = load_config()
    max_tables = config.get("grouping", {}).get("max_tables_per_group", 75)
    groups = group_tables(tables, max_per_group=max_tables)
    return groups


# ---------------------------------------------------------------------------
# Graph data structure
# ---------------------------------------------------------------------------

class PipelineNode:
    """A node in the pipeline dependency graph."""

    def __init__(
        self,
        node_type: str,
        name: str,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.node_type = node_type   # "source", "extract", "pump", "replicat", "target"
        self.name = name
        self.details = details or {}


class PipelineEdge:
    """A directed edge between two pipeline nodes."""

    def __init__(self, source: str, target: str, label: str = ""):
        self.source = source
        self.target = target
        self.label = label


class PipelineGraph:
    """The full pipeline dependency graph."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.nodes: List[PipelineNode] = []
        self.edges: List[PipelineEdge] = []

        # Config shortcuts
        self.source_alias = config.get("source", {}).get("alias", "source")
        self.gg19c = config.get("gg19c", {})
        self.gg21c = config.get("gg21c", {})
        self.encryption = self.gg19c.get("trail", {}).get("encryption", {})
        self.enc_enabled = self.encryption.get("enabled", False)
        self.enc_algo = self.encryption.get("algorithm", "AES256")
        self.mgr_port_19c = self.gg19c.get("manager", {}).get("port", 7809)
        self.mgr_port_21c = self.gg21c.get("manager", {}).get("port", 7999)
        self.pump_target_port = self.gg19c.get("pump", {}).get("target_manager_port", 7909)
        self.encrypt_rmthost = self.gg19c.get("pump", {}).get("encrypt_rmthost", False)
        self.sf_config = self.gg21c.get("snowflake", {})

    def build(self, groups) -> None:
        """Build the graph from extract groups."""
        # Source node
        self.nodes.append(PipelineNode(
            "source", f"Oracle RDS ({self.source_alias})",
            {"alias": self.source_alias},
        ))

        # Target node
        sf_db = self.sf_config.get("database", "SNOWFLAKE")
        self.nodes.append(PipelineNode(
            "target", f"Snowflake ({sf_db})",
            {"database": sf_db, "account": self.sf_config.get("account", "")},
        ))

        for g in groups:
            # Extract
            schemas_str = ", ".join(g.schemas)
            self.nodes.append(PipelineNode(
                "extract", g.extract_name,
                {
                    "table_count": g.table_count,
                    "schemas": g.schemas,
                    "schemas_str": schemas_str,
                    "trail": g.extract_trail,
                    "encryption": f"[{self.enc_algo}]" if self.enc_enabled else "",
                },
            ))

            # Pump
            self.nodes.append(PipelineNode(
                "pump", g.pump_name,
                {
                    "trail": g.pump_trail,
                    "target_port": self.pump_target_port,
                    "encrypt_rmthost": self.encrypt_rmthost,
                },
            ))

            # Replicat
            target_schemas = ", ".join(g.target_schemas)
            self.nodes.append(PipelineNode(
                "replicat", g.replicat_name,
                {
                    "target_schemas": g.target_schemas,
                    "target_schemas_str": target_schemas,
                    "table_count": g.table_count,
                },
            ))

            # Edges: Source -> Extract
            self.edges.append(PipelineEdge(
                f"Oracle RDS ({self.source_alias})", g.extract_name,
                label="integrated tranlog",
            ))

            # Extract -> Pump (via local trail)
            enc_label = f" [{self.enc_algo}]" if self.enc_enabled else ""
            self.edges.append(PipelineEdge(
                g.extract_name, g.pump_name,
                label=f"trail: {g.extract_trail}*{enc_label}",
            ))

            # Pump -> Replicat (via remote trail)
            transit_label = "ENCRYPT" if self.encrypt_rmthost else ""
            self.edges.append(PipelineEdge(
                g.pump_name, g.replicat_name,
                label=f"trail: {g.pump_trail}* port {self.pump_target_port}"
                      + (f" [{transit_label}]" if transit_label else ""),
            ))

            # Replicat -> Snowflake
            self.edges.append(PipelineEdge(
                g.replicat_name, f"Snowflake ({sf_db})",
                label=f"JDBC/TLS ({target_schemas})",
            ))


# ---------------------------------------------------------------------------
# Text (ASCII art) formatter
# ---------------------------------------------------------------------------

# ANSI color codes
COLORS = {
    "reset":   "\033[0m",
    "bold":    "\033[1m",
    "cyan":    "\033[0;36m",
    "green":   "\033[0;32m",
    "yellow":  "\033[1;33m",
    "blue":    "\033[0;34m",
    "magenta": "\033[0;35m",
    "red":     "\033[0;31m",
    "dim":     "\033[2m",
}


def c(text: str, color: str, use_color: bool = True) -> str:
    """Wrap text in ANSI color codes."""
    if not use_color or color not in COLORS:
        return text
    return f"{COLORS[color]}{text}{COLORS['reset']}"


def format_text(graph: PipelineGraph, groups, use_color: bool = True) -> str:
    """Render the pipeline as ASCII art."""
    lines: List[str] = []

    source_alias = graph.source_alias
    enc_enabled = graph.enc_enabled
    enc_algo = graph.enc_algo
    pump_port = graph.pump_target_port
    encrypt_rmthost = graph.encrypt_rmthost
    sf_db = graph.sf_config.get("database", "SNOWFLAKE")

    # Header
    lines.append("")
    lines.append(c("Oracle GoldenGate Pipeline Dependency Graph", "bold", use_color))
    lines.append(c("=" * 60, "dim", use_color))
    lines.append("")

    # Source
    lines.append(c(f"Oracle RDS ({source_alias})", "cyan", use_color))

    for i, g in enumerate(groups):
        is_last = (i == len(groups) - 1)
        connector = "  |" if not is_last else "  |"
        branch = "+-" if not is_last else "`-"

        schemas_str = ", ".join(g.schemas)
        target_schemas_str = ", ".join(g.target_schemas)
        enc_tag = f" [{enc_algo}]" if enc_enabled else ""
        transit_tag = "ENCRYPT" if encrypt_rmthost else ""

        lines.append(f"  |")
        lines.append(
            f"  {branch}-- "
            + c(g.extract_name, "green", use_color)
            + c(f" ({g.table_count} tables: {schemas_str})", "dim", use_color)
        )

        prefix = "  |     " if not is_last else "        "

        lines.append(f"{prefix}|")
        lines.append(f"{prefix}| trail: {g.extract_trail}*{enc_tag}")
        lines.append(f"{prefix}v")

        transit_str = f" --{transit_tag}-->" if transit_tag else " ---------->"
        lines.append(
            f"{prefix}"
            + c(g.pump_name, "yellow", use_color)
            + c(f"{transit_str} port {pump_port}", "dim", use_color)
        )
        lines.append(f"{prefix}|")
        lines.append(f"{prefix}| trail: {g.pump_trail}*")
        lines.append(f"{prefix}v")
        lines.append(
            f"{prefix}"
            + c(g.replicat_name, "magenta", use_color)
            + c(f" --JDBC/TLS--> Snowflake ({target_schemas_str})", "dim", use_color)
        )

    lines.append("")
    lines.append(c("=" * 60, "dim", use_color))

    # Summary
    total_tables = sum(g.table_count for g in groups)
    all_schemas = sorted({s for g in groups for s in g.schemas})
    lines.append(
        c(f"  {len(groups)} groups | {total_tables} tables | "
          f"{len(all_schemas)} schemas | Target: {sf_db}", "bold", use_color)
    )
    lines.append(
        c(f"  Encryption at rest: {'AES256' if enc_enabled else 'disabled'} | "
          f"In transit: {'OGG_AES256_GCM' if encrypt_rmthost else 'disabled'}", "dim", use_color)
    )
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# DOT (Graphviz) formatter
# ---------------------------------------------------------------------------

def format_dot(graph: PipelineGraph, groups) -> str:
    """Render the pipeline as a Graphviz DOT file."""
    lines: List[str] = []

    lines.append("digraph pipeline {")
    lines.append("    rankdir=TB;")
    lines.append("    fontname=\"Helvetica\";")
    lines.append("    node [fontname=\"Helvetica\", fontsize=11];")
    lines.append("    edge [fontname=\"Helvetica\", fontsize=9];")
    lines.append("")

    # Source node
    source_id = "oracle_rds"
    source_label = f"Oracle RDS\\n({graph.source_alias})"
    lines.append(f'    {source_id} [label="{source_label}", shape=cylinder, '
                 f'style=filled, fillcolor="#FFE0B2"];')

    # Target node
    sf_db = graph.sf_config.get("database", "SNOWFLAKE")
    target_id = "snowflake"
    target_label = f"Snowflake\\n({sf_db})"
    lines.append(f'    {target_id} [label="{target_label}", shape=cylinder, '
                 f'style=filled, fillcolor="#B3E5FC"];')
    lines.append("")

    for g in groups:
        ext_id = g.extract_name.lower()
        pmp_id = g.pump_name.lower()
        rep_id = g.replicat_name.lower()
        trail_ext_id = f"trail_{g.extract_trail}"
        trail_pmp_id = f"trail_{g.pump_trail}"

        schemas_str = ", ".join(g.schemas)
        target_schemas_str = ", ".join(g.target_schemas)
        enc_tag = f"\\n[{graph.enc_algo}]" if graph.enc_enabled else ""

        # Extract node
        lines.append(f'    {ext_id} [label="{g.extract_name}\\n'
                     f'{g.table_count} tables\\n({schemas_str})", '
                     f'shape=box, style=filled, fillcolor="#C8E6C9"];')

        # Local trail
        lines.append(f'    {trail_ext_id} [label="{g.extract_trail}*{enc_tag}", '
                     f'shape=note, style=filled, fillcolor="#FFF9C4"];')

        # Pump node
        lines.append(f'    {pmp_id} [label="{g.pump_name}", '
                     f'shape=box, style=filled, fillcolor="#FFECB3"];')

        # Remote trail
        lines.append(f'    {trail_pmp_id} [label="{g.pump_trail}*", '
                     f'shape=note, style=filled, fillcolor="#FFF9C4"];')

        # Replicat node
        lines.append(f'    {rep_id} [label="{g.replicat_name}\\n'
                     f'({target_schemas_str})", '
                     f'shape=box, style=filled, fillcolor="#E1BEE7"];')

        # Edges
        lines.append(f'    {source_id} -> {ext_id} [label="tranlog"];')
        lines.append(f'    {ext_id} -> {trail_ext_id};')
        lines.append(f'    {trail_ext_id} -> {pmp_id};')

        transit_label = "ENCRYPT" if graph.encrypt_rmthost else ""
        port_label = f"port {graph.pump_target_port}"
        edge_label = f"{transit_label}\\n{port_label}" if transit_label else port_label
        lines.append(f'    {pmp_id} -> {trail_pmp_id} [label="{edge_label}"];')

        lines.append(f'    {trail_pmp_id} -> {rep_id};')
        lines.append(f'    {rep_id} -> {target_id} [label="JDBC/TLS"];')
        lines.append("")

    lines.append("}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML (inline SVG) formatter
# ---------------------------------------------------------------------------

def format_html(graph: PipelineGraph, groups) -> str:
    """Render the pipeline as a self-contained HTML page with inline SVG."""

    sf_db = html_lib.escape(graph.sf_config.get("database", "SNOWFLAKE"))
    source_alias = html_lib.escape(graph.source_alias)
    enc_enabled = graph.enc_enabled
    enc_algo = html_lib.escape(graph.enc_algo) if enc_enabled else ""
    pump_port = graph.pump_target_port
    encrypt_rmthost = graph.encrypt_rmthost

    # Layout constants
    margin_x = 40
    col_width = 180
    node_h = 50
    node_w = 160
    group_spacing = 280
    row_spacing = 80
    header_y = 40
    source_y = 90
    start_y = 160

    num_groups = len(groups)
    total_width = max(margin_x * 2 + num_groups * group_spacing, 600)
    # 6 rows: source, extract, trail_ext, pump, trail_pmp, replicat, target
    total_height = start_y + 7 * row_spacing + 80

    svg_parts: List[str] = []

    # --- Defs (arrow marker, gradients) ---
    svg_parts.append("""
    <defs>
      <marker id="arrow" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
        <polygon points="0 0, 10 3.5, 0 7" fill="#555"/>
      </marker>
      <linearGradient id="sourceGrad" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" style="stop-color:#FF9800;stop-opacity:0.9"/>
        <stop offset="100%" style="stop-color:#F57C00;stop-opacity:0.9"/>
      </linearGradient>
      <linearGradient id="targetGrad" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" style="stop-color:#03A9F4;stop-opacity:0.9"/>
        <stop offset="100%" style="stop-color:#0288D1;stop-opacity:0.9"/>
      </linearGradient>
      <linearGradient id="extGrad" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" style="stop-color:#66BB6A;stop-opacity:0.9"/>
        <stop offset="100%" style="stop-color:#43A047;stop-opacity:0.9"/>
      </linearGradient>
      <linearGradient id="pmpGrad" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" style="stop-color:#FFA726;stop-opacity:0.9"/>
        <stop offset="100%" style="stop-color:#FB8C00;stop-opacity:0.9"/>
      </linearGradient>
      <linearGradient id="repGrad" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" style="stop-color:#AB47BC;stop-opacity:0.9"/>
        <stop offset="100%" style="stop-color:#8E24AA;stop-opacity:0.9"/>
      </linearGradient>
      <filter id="shadow">
        <feDropShadow dx="1" dy="1" stdDeviation="2" flood-opacity="0.15"/>
      </filter>
    </defs>
    """)

    def rect_node(x, y, w, h, fill, text_lines, rx=8):
        """Draw a rounded rect with centered text lines."""
        parts = []
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
                     f'fill="{fill}" filter="url(#shadow)" stroke="#ccc" stroke-width="0.5"/>')
        line_h = 16
        total_text_h = len(text_lines) * line_h
        text_start_y = y + (h - total_text_h) / 2 + 12
        for i, line in enumerate(text_lines):
            escaped = html_lib.escape(line)
            font_size = "12" if i > 0 else "13"
            font_weight = "bold" if i == 0 else "normal"
            color = "#fff" if fill.startswith("url") else "#333"
            parts.append(
                f'<text x="{x + w / 2}" y="{text_start_y + i * line_h}" '
                f'text-anchor="middle" font-size="{font_size}" '
                f'font-weight="{font_weight}" fill="{color}">{escaped}</text>'
            )
        return "\n".join(parts)

    def trail_node(x, y, w, h, label):
        """Draw a trail file node (parallelogram-like shape)."""
        # Small note shape
        parts = []
        points = f"{x},{y} {x + w},{y} {x + w},{y + h - 8} {x + w - 8},{y + h} {x},{y + h}"
        parts.append(f'<polygon points="{points}" fill="#FFF9C4" stroke="#F9A825" '
                     f'stroke-width="1" filter="url(#shadow)"/>')
        # Fold corner
        parts.append(f'<polygon points="{x + w - 8},{y + h} {x + w},{y + h - 8} {x + w - 8},{y + h - 8}" '
                     f'fill="#F9A825" opacity="0.3"/>')
        escaped = html_lib.escape(label)
        parts.append(f'<text x="{x + w / 2}" y="{y + h / 2 + 4}" text-anchor="middle" '
                     f'font-size="11" fill="#333">{escaped}</text>')
        return "\n".join(parts)

    def arrow(x1, y1, x2, y2, label=""):
        """Draw an arrow with optional label."""
        parts = []
        parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
                     f'stroke="#555" stroke-width="1.5" marker-end="url(#arrow)"/>')
        if label:
            mid_x = (x1 + x2) / 2
            mid_y = (y1 + y2) / 2
            escaped = html_lib.escape(label)
            parts.append(f'<text x="{mid_x + 5}" y="{mid_y}" font-size="9" '
                         f'fill="#777">{escaped}</text>')
        return "\n".join(parts)

    # --- Source node (centered) ---
    source_w = 200
    source_x = total_width / 2 - source_w / 2
    svg_parts.append(rect_node(source_x, source_y, source_w, node_h,
                               "url(#sourceGrad)",
                               [f"Oracle RDS ({source_alias})"]))

    # --- Per-group columns ---
    for i, g in enumerate(groups):
        cx = margin_x + i * group_spacing + group_spacing / 2
        nw = node_w
        nx = cx - nw / 2

        schemas_str = ", ".join(g.schemas)
        target_schemas_str = ", ".join(g.target_schemas)
        enc_tag = f" [{enc_algo}]" if enc_enabled else ""
        transit = "ENCRYPT" if encrypt_rmthost else ""

        # Row positions
        row0 = start_y                     # extract
        row1 = start_y + row_spacing       # extract trail
        row2 = start_y + 2 * row_spacing   # pump
        row3 = start_y + 3 * row_spacing   # pump trail
        row4 = start_y + 4 * row_spacing   # replicat

        # Arrow: source -> extract
        svg_parts.append(arrow(total_width / 2, source_y + node_h, cx, row0, ""))

        # Extract
        svg_parts.append(rect_node(nx, row0, nw, node_h, "url(#extGrad)",
                                   [g.extract_name, f"{g.table_count} tables: {schemas_str}"]))

        # Arrow: extract -> trail
        svg_parts.append(arrow(cx, row0 + node_h, cx, row1, ""))

        # Extract trail
        tw = 120
        th = 30
        svg_parts.append(trail_node(cx - tw / 2, row1, tw, th,
                                     f"{g.extract_trail}*{enc_tag}"))

        # Arrow: trail -> pump
        svg_parts.append(arrow(cx, row1 + th, cx, row2, ""))

        # Pump
        pump_detail = f"port {pump_port}"
        if transit:
            pump_detail = f"{transit} -> {pump_detail}"
        svg_parts.append(rect_node(nx, row2, nw, node_h, "url(#pmpGrad)",
                                   [g.pump_name, pump_detail]))

        # Arrow: pump -> pump trail
        svg_parts.append(arrow(cx, row2 + node_h, cx, row3, ""))

        # Pump trail
        svg_parts.append(trail_node(cx - tw / 2, row3, tw, th, f"{g.pump_trail}*"))

        # Arrow: pump trail -> replicat
        svg_parts.append(arrow(cx, row3 + th, cx, row4, ""))

        # Replicat
        svg_parts.append(rect_node(nx, row4, nw, node_h, "url(#repGrad)",
                                   [g.replicat_name, f"JDBC/TLS -> {target_schemas_str}"]))

    # --- Target node (centered, at the bottom) ---
    target_y = start_y + 5 * row_spacing
    target_w = 200
    target_x = total_width / 2 - target_w / 2
    svg_parts.append(rect_node(target_x, target_y, target_w, node_h,
                               "url(#targetGrad)",
                               [f"Snowflake ({sf_db})"]))

    # Arrows: replicat -> target
    for i, g in enumerate(groups):
        cx = margin_x + i * group_spacing + group_spacing / 2
        rep_y = start_y + 4 * row_spacing + node_h
        svg_parts.append(arrow(cx, rep_y, total_width / 2, target_y, ""))

    # --- Summary footer ---
    total_tables = sum(g.table_count for g in groups)
    all_schemas = sorted({s for g in groups for s in g.schemas})
    footer_y = target_y + node_h + 30
    summary = (f"{len(groups)} groups | {total_tables} tables | "
               f"{len(all_schemas)} schemas")
    svg_parts.append(
        f'<text x="{total_width / 2}" y="{footer_y}" text-anchor="middle" '
        f'font-size="12" fill="#666">{html_lib.escape(summary)}</text>'
    )

    total_height = footer_y + 30

    # Build HTML
    svg_content = "\n".join(svg_parts)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>GoldenGate Pipeline Dependency Graph</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: #f5f5f5;
    margin: 0;
    padding: 20px;
    display: flex;
    flex-direction: column;
    align-items: center;
  }}
  h1 {{
    color: #333;
    font-size: 1.4em;
    margin-bottom: 5px;
  }}
  .subtitle {{
    color: #777;
    font-size: 0.9em;
    margin-bottom: 20px;
  }}
  .graph-container {{
    background: #fff;
    border-radius: 12px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    padding: 20px;
    overflow-x: auto;
  }}
  svg {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  }}
  .legend {{
    display: flex;
    gap: 20px;
    margin-top: 15px;
    font-size: 0.85em;
    color: #666;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .legend-color {{
    width: 14px;
    height: 14px;
    border-radius: 3px;
  }}
</style>
</head>
<body>
<h1>Oracle GoldenGate Pipeline Dependency Graph</h1>
<div class="subtitle">Oracle RDS -> GG 19c -> GG 21c -> Snowflake</div>
<div class="graph-container">
  <svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="{total_height}" viewBox="0 0 {total_width} {total_height}">
    {svg_content}
  </svg>
</div>
<div class="legend">
  <div class="legend-item"><div class="legend-color" style="background:#FF9800;"></div> Source (Oracle RDS)</div>
  <div class="legend-item"><div class="legend-color" style="background:#66BB6A;"></div> Extract</div>
  <div class="legend-item"><div class="legend-color" style="background:#FFA726;"></div> Pump</div>
  <div class="legend-item"><div class="legend-color" style="background:#AB47BC;"></div> Replicat</div>
  <div class="legend-item"><div class="legend-color" style="background:#03A9F4;"></div> Target (Snowflake)</div>
  <div class="legend-item"><div class="legend-color" style="background:#FFF9C4; border: 1px solid #F9A825;"></div> Trail Files</div>
</div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="GG Process Dependency Graph — Visualize extract/pump/replicat chains",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           ASCII art in terminal
  %(prog)s --format dot              Graphviz DOT file
  %(prog)s --format html             Self-contained HTML with SVG
  %(prog)s --format text --no-color  Plain text (no ANSI colors)
        """,
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "dot", "html"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colors in text output",
    )
    parser.add_argument(
        "--input", "-i",
        type=str,
        default=None,
        help="Path to table_inventory.xlsx (default: input/table_inventory.xlsx)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline.yaml (default: config/pipeline.yaml)",
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        default=None,
        help="Output file path (default: stdout for text, output/graphs/ for dot/html)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def main() -> None:
    """Entry point."""
    args = parse_args()

    # Logging setup — suppress app module logs unless verbose
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Override config path if provided
    global CONFIG_FILE
    if args.config:
        CONFIG_FILE = Path(args.config)

    config = load_config(args.config)
    groups = load_groups(args.input)

    # Build graph
    graph = PipelineGraph(config)
    graph.build(groups)

    # Render
    if args.format == "text":
        use_color = not args.no_color and sys.stdout.isatty()
        output = format_text(graph, groups, use_color=use_color)
        if args.output:
            Path(args.output).write_text(output)
            print(f"Written to {args.output}")
        else:
            print(output)

    elif args.format == "dot":
        output = format_dot(graph, groups)
        out_path = Path(args.output) if args.output else OUTPUT_DIR / "pipeline.dot"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output)
        print(f"DOT graph written to {out_path}")
        print(f"Render with: dot -Tpng {out_path} -o {out_path.with_suffix('.png')}")

    elif args.format == "html":
        output = format_html(graph, groups)
        out_path = Path(args.output) if args.output else OUTPUT_DIR / "pipeline.html"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output)
        print(f"HTML graph written to {out_path}")
        print(f"Open in browser: open {out_path}")


if __name__ == "__main__":
    main()

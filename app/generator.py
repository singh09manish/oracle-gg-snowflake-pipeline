"""
Generate all GoldenGate parameter files + GGSCI obeyfiles from ExtractGroups.

Uses Jinja2 templates from templates/ → writes to output/.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from app.models import ExtractGroup, PipelineManifest

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_DIR = ROOT / "templates"
OUTPUT_DIR = ROOT / "output"
CONFIG_DIR = ROOT / "config"


class Generator:
    """Renders all GG parameter files from pipeline.yaml + ExtractGroups."""

    def __init__(self, groups: List[ExtractGroup]) -> None:
        self.groups = groups
        self.cfg = self._load_config()
        self.env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.manifest = PipelineManifest(
            total_tables=sum(g.table_count for g in groups),
            total_groups=len(groups),
            groups=groups,
        )

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def generate_all(self) -> PipelineManifest:
        """Generate everything. Returns a manifest of what was written."""
        log.info("Generating files for %d groups (%d tables)...",
                 len(self.groups), self.manifest.total_tables)

        self._gen_gg19c_manager()
        self._gen_gg21c_manager()

        for g in self.groups:
            self._gen_extract(g)
            self._gen_pump(g)
            self._gen_replicat(g)
            self._gen_snowflake_props(g)

        self._gen_gg19c_ggsci()
        self._gen_gg21c_ggsci()

        log.info("Generated %d files.", len(self.manifest.files_written))
        return self.manifest

    # -------------------------------------------------------------------
    # Config
    # -------------------------------------------------------------------

    def _load_config(self) -> Dict[str, Any]:
        path = CONFIG_DIR / "pipeline.yaml"
        if not path.exists():
            raise FileNotFoundError(f"Config not found: {path}")
        with open(path) as f:
            return yaml.safe_load(f)

    # -------------------------------------------------------------------
    # Write helper
    # -------------------------------------------------------------------

    def _write(self, rel_path: str, content: str) -> None:
        out = OUTPUT_DIR / rel_path
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(content.rstrip() + "\n")
        self.manifest.files_written.append(rel_path)
        log.debug("  wrote %s", rel_path)

    def _render(self, template_path: str, ctx: Dict[str, Any]) -> str:
        tmpl = self.env.get_template(template_path)
        return tmpl.render(**ctx)

    # -------------------------------------------------------------------
    # Common context
    # -------------------------------------------------------------------

    def _gg19c_ctx(self, group: ExtractGroup) -> Dict[str, Any]:
        c = self.cfg
        trail = c["gg19c"]["trail"]
        enc = trail.get("encryption", {})
        pump = c["gg19c"]["pump"]
        return {
            "group":              group,
            "source_alias":       c["source"]["alias"],
            "trail_dir":          trail.get("dir", "./dirdat"),
            # Encryption — trail files at rest
            "encryption_enabled": enc.get("enabled", False),
            "encryption_algorithm": enc.get("algorithm", "AES256"),
            "encryption_keyname":   enc.get("keyname", "src_trail_key"),
            # Pump — data in transit
            "target_host":        pump.get("target_host", "localhost"),
            "target_mgr_port":    pump.get("target_manager_port", 7909),
            "target_trail_dir":   pump.get("target_trail_dir", "./dirdat"),
            "encrypt_rmthost":    pump.get("encrypt_rmthost", False),
        }

    def _gg21c_ctx(self, group: ExtractGroup) -> Dict[str, Any]:
        c = self.cfg
        sf = c["gg21c"]["snowflake"]
        auth = sf.get("auth", {})
        auth_method = auth.get("method", "password")
        return {
            "group":              group,
            "grouptransops":      c["gg21c"].get("replicat", {}).get("grouptransops", 1000),
            "maxtransops":        c["gg21c"].get("replicat", {}).get("maxtransops", 1000),
            "snowflake_account":  sf["account"],
            "snowflake_warehouse": sf["warehouse"],
            "snowflake_database": sf["database"],
            "snowflake_role":     sf.get("role", ""),
            "gg21c_home":         c["gg21c"]["home"],
            "snowflake_jdbc_jar": sf.get("jdbc_jar", "snowflake-jdbc-3.14.4.jar"),
            # Authentication
            "snowflake_auth_method":           auth_method,
            "snowflake_user":                  auth.get("user", ""),
            "snowflake_private_key_file":      auth.get("private_key_file", ""),
            "snowflake_private_key_passphrase": auth.get("private_key_passphrase", ""),
        }

    # -------------------------------------------------------------------
    # GG 19c — Manager
    # -------------------------------------------------------------------

    def _gen_gg19c_manager(self) -> None:
        ctx = {"manager": self.cfg["gg19c"]["manager"]}
        content = self._render("gg19c/mgr.prm.j2", ctx)
        self._write("gg19c/dirprm/mgr.prm", content)

    # -------------------------------------------------------------------
    # GG 19c — Extract
    # -------------------------------------------------------------------

    def _gen_extract(self, group: ExtractGroup) -> None:
        ctx = self._gg19c_ctx(group)
        content = self._render("gg19c/extract.prm.j2", ctx)
        self._write(f"gg19c/dirprm/{group.extract_name}.prm", content)

    # -------------------------------------------------------------------
    # GG 19c — Pump
    # -------------------------------------------------------------------

    def _gen_pump(self, group: ExtractGroup) -> None:
        ctx = self._gg19c_ctx(group)
        content = self._render("gg19c/pump.prm.j2", ctx)
        self._write(f"gg19c/dirprm/{group.pump_name}.prm", content)

    # -------------------------------------------------------------------
    # GG 21c — Manager
    # -------------------------------------------------------------------

    def _gen_gg21c_manager(self) -> None:
        ctx = {"manager": self.cfg["gg21c"]["manager"]}
        content = self._render("gg21c/mgr.prm.j2", ctx)
        self._write("gg21c/dirprm/mgr.prm", content)

    # -------------------------------------------------------------------
    # GG 21c — Replicat
    # -------------------------------------------------------------------

    def _gen_replicat(self, group: ExtractGroup) -> None:
        ctx = self._gg21c_ctx(group)
        content = self._render("gg21c/replicat.prm.j2", ctx)
        self._write(f"gg21c/dirprm/{group.replicat_name}.prm", content)

    # -------------------------------------------------------------------
    # GG 21c — Snowflake handler properties
    # -------------------------------------------------------------------

    def _gen_snowflake_props(self, group: ExtractGroup) -> None:
        ctx = self._gg21c_ctx(group)
        content = self._render("gg21c/snowflake.props.j2", ctx)
        self._write(f"gg21c/dirprm/{group.replicat_name}.properties", content)

    # -------------------------------------------------------------------
    # GGSCI obeyfiles — GG 19c
    # -------------------------------------------------------------------

    def _gen_gg19c_ggsci(self) -> None:
        c = self.cfg
        alias = c["source"]["alias"]
        trail = c["gg19c"]["trail"]
        pump = c["gg19c"]["pump"]

        # 01: ADD TRANDATA per table (not SCHEMATRANDATA — explicit is safer)
        lines = [
            "-- Step 1: Enable supplemental logging per table",
            "-- This is more precise than SCHEMATRANDATA and avoids overhead on excluded tables.",
            f"DBLOGIN USERIDALIAS {alias}",
            "",
        ]
        for g in self.groups:
            lines.append(f"-- Group {g.extract_name} ({g.table_count} tables)")
            for t in g.tables:
                lines.append(f"ADD TRANDATA {t.schema}.{t.name}, COLS (*)")
            lines.append("")
        self._write_oby("gg19c/ggsci/01_trandata.oby", lines)

        # 02: ADD EXTRACT + REGISTER
        lines = [
            "-- Step 2: Add integrated extracts and register with Oracle logmining server",
            f"DBLOGIN USERIDALIAS {alias}",
            "",
        ]
        for g in self.groups:
            lines += [
                f"ADD EXTRACT {g.extract_name}, INTEGRATED TRANLOG, BEGIN NOW",
                f"ADD EXTTRAIL {trail['dir']}/{g.extract_trail}, EXTRACT {g.extract_name}",
                f"REGISTER EXTRACT {g.extract_name} DATABASE",
                "",
            ]
        self._write_oby("gg19c/ggsci/02_add_extracts.oby", lines)

        # 03: ADD PUMP
        lines = ["-- Step 3: Add data pump extracts (pass-through to GG BigData 21c)", ""]
        for g in self.groups:
            lines += [
                f"ADD EXTRACT {g.pump_name}, EXTTRAILSOURCE {trail['dir']}/{g.extract_trail}",
                f"ADD RMTTRAIL {pump['target_trail_dir']}/{g.pump_trail}, EXTRACT {g.pump_name}",
                "",
            ]
        self._write_oby("gg19c/ggsci/03_add_pumps.oby", lines)

        # 04: START ALL
        lines = ["-- Step 4: Start all GG 19c processes", ""]
        for g in self.groups:
            lines.append(f"START EXTRACT {g.extract_name}")
        lines.append("")
        for g in self.groups:
            lines.append(f"START EXTRACT {g.pump_name}")
        lines += ["", "INFO ALL"]
        self._write_oby("gg19c/ggsci/04_start_all.oby", lines)

        # 05: STATUS
        lines = ["-- Step 5: Check GG 19c status", ""]
        for g in self.groups:
            lines.append(f"INFO EXTRACT {g.extract_name}, DETAIL")
        lines.append("")
        for g in self.groups:
            lines.append(f"INFO EXTRACT {g.pump_name}, DETAIL")
        lines += ["", "INFO ALL", "LAG EXTRACT *"]
        self._write_oby("gg19c/ggsci/05_status.oby", lines)

    # -------------------------------------------------------------------
    # GGSCI obeyfiles — GG 21c
    # -------------------------------------------------------------------

    def _gen_gg21c_ggsci(self) -> None:
        t_dir = self.cfg["gg21c"].get("trail", {}).get("dir", "./dirdat")

        # 01: ADD REPLICAT
        lines = [
            "-- Step 1: Add BigData Replicats (Snowflake via JDBC handler)",
            "-- Note: BigData Replicat uses Java handler checkpointing, not CHECKPOINTTABLE.",
            "",
        ]
        for g in self.groups:
            lines += [
                f"ADD REPLICAT {g.replicat_name}, EXTTRAIL {t_dir}/{g.pump_trail}",
                "",
            ]
        self._write_oby("gg21c/ggsci/01_add_replicats.oby", lines)

        # 02: START ALL
        lines = ["-- Step 2: Start all GG 21c Replicats", ""]
        for g in self.groups:
            lines.append(f"START REPLICAT {g.replicat_name}")
        lines += ["", "INFO ALL"]
        self._write_oby("gg21c/ggsci/02_start_all.oby", lines)

        # 03: STATUS
        lines = ["-- Step 3: Check GG 21c status", ""]
        for g in self.groups:
            lines.append(f"INFO REPLICAT {g.replicat_name}, DETAIL")
        lines += ["", "INFO ALL", "LAG REPLICAT *"]
        self._write_oby("gg21c/ggsci/03_status.oby", lines)

    def _write_oby(self, rel_path: str, lines: List[str]) -> None:
        self._write(rel_path, "\n".join(lines))

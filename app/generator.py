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

        has_rds = self._has_rds_targets()
        has_sf = self._has_snowflake_targets()

        self._gen_gg19c_manager()

        for g in self.groups:
            self._gen_extract(g)

            # Snowflake pump + replicat (only for groups with Snowflake-bound tables)
            if g.has_snowflake_tables:
                self._gen_pump(g)

            # RDS Classic replicat (only for groups with RDS-bound tables)
            if g.has_rds_tables and has_rds:
                self._gen_rds_replicat(g)

        if has_sf:
            self._gen_gg21c_manager()
            for g in self.groups:
                if g.has_snowflake_tables:
                    self._gen_replicat(g)
                    self._gen_snowflake_props(g)

        self._gen_gg19c_ggsci()
        if has_sf:
            self._gen_gg21c_ggsci()
        self._gen_heartbeat_setup()
        self._gen_scn_helper()

        log.info("Generated %d files.", len(self.manifest.files_written))
        return self.manifest

    def _has_rds_targets(self) -> bool:
        """Check if any group has RDS-targeted tables and target_rds is configured."""
        rds_cfg = self.cfg.get("target_rds", {})
        if not rds_cfg.get("enabled", False):
            rds_tables = sum(len(g.rds_tables) for g in self.groups)
            if rds_tables > 0:
                log.warning(
                    "%d tables target RDS but target_rds.enabled=false in pipeline.yaml. "
                    "Set target_rds.enabled: true to generate RDS replicat configs.",
                    rds_tables,
                )
            return False
        return any(g.has_rds_tables for g in self.groups)

    def _has_snowflake_targets(self) -> bool:
        return any(g.has_snowflake_tables for g in self.groups)

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

    def _rds_replicat_ctx(self, group: ExtractGroup) -> Dict[str, Any]:
        c = self.cfg
        trail = c["gg19c"]["trail"]
        enc = trail.get("encryption", {})
        rds = c.get("target_rds", {})
        rep_cfg = rds.get("replicat", {})
        return {
            "group":                  group,
            "target_rds_alias":       rds.get("alias", "rds_target"),
            # Encryption — decrypt extract trail
            "encryption_enabled":     enc.get("enabled", False),
            "encryption_algorithm":   enc.get("algorithm", "AES256"),
            # Replicat tuning
            "batch_sql":              rep_cfg.get("batch_sql", True),
            "batch_sql_buffer_size":  rep_cfg.get("batch_sql_buffer_size", 50000),
            "grouptransops":          rep_cfg.get("grouptransops", 1000),
            "maxtransops":            rep_cfg.get("maxtransops", 1000),
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
    # GG 19c — RDS Classic Replicat
    # -------------------------------------------------------------------

    def _gen_rds_replicat(self, group: ExtractGroup) -> None:
        ctx = self._rds_replicat_ctx(group)
        content = self._render("gg19c/rds_replicat.prm.j2", ctx)
        self._write(f"gg19c/dirprm/{group.rds_replicat_name}.prm", content)

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
        # Pass only Snowflake-targeted tables to the template
        ctx["sf_tables"] = group.snowflake_tables
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
        has_rds = self._has_rds_targets()
        has_sf = self._has_snowflake_targets()
        rds_groups = [g for g in self.groups if g.has_rds_tables] if has_rds else []

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

        # 02: ADD EXTRACT + REGISTER (supports BEGIN NOW or BEGIN AT SCN)
        scn_cfg = c.get("scn", {})
        default_begin = "BEGIN NOW"  # default: start from current position

        lines = [
            "-- Step 2: Add integrated extracts and register with Oracle logmining server",
            "-- To start from a specific SCN, edit pipeline.yaml → scn.extracts.EXTnn",
            "-- Or use: ggctl start-from-scn EXT01 12345678",
            f"DBLOGIN USERIDALIAS {alias}",
            "",
        ]
        for g in self.groups:
            # Check if a specific SCN is configured for this extract
            scn_value = scn_cfg.get("extracts", {}).get(g.extract_name, "")
            if scn_value:
                begin_clause = f"BEGIN AT SCN {scn_value}"
            else:
                begin_clause = default_begin

            lines += [
                f"ADD EXTRACT {g.extract_name}, INTEGRATED TRANLOG, {begin_clause}",
                f"ADD EXTTRAIL {trail['dir']}/{g.extract_trail}, EXTRACT {g.extract_name}",
                f"REGISTER EXTRACT {g.extract_name} DATABASE",
                "",
            ]
        self._write_oby("gg19c/ggsci/02_add_extracts.oby", lines)

        # 03: ADD PUMP (only for Snowflake-bound groups)
        if has_sf:
            sf_groups = [g for g in self.groups if g.has_snowflake_tables]
            lines = ["-- Step 3: Add data pump extracts (pass-through to GG BigData 21c)", ""]
            for g in sf_groups:
                lines += [
                    f"ADD EXTRACT {g.pump_name}, EXTTRAILSOURCE {trail['dir']}/{g.extract_trail}",
                    f"ADD RMTTRAIL {pump['target_trail_dir']}/{g.pump_trail}, EXTRACT {g.pump_name}",
                    "",
                ]
            self._write_oby("gg19c/ggsci/03_add_pumps.oby", lines)

        # 07: ADD RDS REPLICATS (only for RDS-bound groups)
        if rds_groups:
            rds_alias = c.get("target_rds", {}).get("alias", "rds_target")
            lines = [
                "-- Step 7: Add Classic replicats for RDS target (live mirror)",
                f"-- These replicats read from the extract trail and apply to target RDS via {rds_alias}",
                f"DBLOGIN USERIDALIAS {rds_alias}",
                "",
            ]
            for g in rds_groups:
                lines += [
                    f"ADD REPLICAT {g.rds_replicat_name}, EXTTRAIL {trail['dir']}/{g.extract_trail}",
                    "",
                ]
            self._write_oby("gg19c/ggsci/07_add_rds_replicats.oby", lines)

        # 04: START ALL
        lines = ["-- Step 4: Start all GG 19c processes", ""]
        for g in self.groups:
            lines.append(f"START EXTRACT {g.extract_name}")
        lines.append("")
        if has_sf:
            for g in self.groups:
                if g.has_snowflake_tables:
                    lines.append(f"START EXTRACT {g.pump_name}")
            lines.append("")
        if rds_groups:
            lines.append("-- RDS Classic Replicats")
            for g in rds_groups:
                lines.append(f"START REPLICAT {g.rds_replicat_name}")
            lines.append("")
        lines.append("INFO ALL")
        self._write_oby("gg19c/ggsci/04_start_all.oby", lines)

        # 05: STATUS
        lines = ["-- Step 5: Check GG 19c status", ""]
        for g in self.groups:
            lines.append(f"INFO EXTRACT {g.extract_name}, DETAIL")
        lines.append("")
        if has_sf:
            for g in self.groups:
                if g.has_snowflake_tables:
                    lines.append(f"INFO EXTRACT {g.pump_name}, DETAIL")
            lines.append("")
        if rds_groups:
            lines.append("-- RDS Classic Replicats")
            for g in rds_groups:
                lines.append(f"INFO REPLICAT {g.rds_replicat_name}, DETAIL")
            lines.append("")
        lines += ["INFO ALL", "LAG EXTRACT *"]
        if rds_groups:
            lines.append("LAG REPLICAT *")
        self._write_oby("gg19c/ggsci/05_status.oby", lines)

    # -------------------------------------------------------------------
    # GGSCI obeyfiles — GG 21c
    # -------------------------------------------------------------------

    def _gen_gg21c_ggsci(self) -> None:
        t_dir = self.cfg["gg21c"].get("trail", {}).get("dir", "./dirdat")
        sf_groups = [g for g in self.groups if g.has_snowflake_tables]

        if not sf_groups:
            return

        # 01: ADD REPLICAT
        lines = [
            "-- Step 1: Add BigData Replicats (Snowflake via JDBC handler)",
            "-- Note: BigData Replicat uses Java handler checkpointing, not CHECKPOINTTABLE.",
            "",
        ]
        for g in sf_groups:
            lines += [
                f"ADD REPLICAT {g.replicat_name}, EXTTRAIL {t_dir}/{g.pump_trail}",
                "",
            ]
        self._write_oby("gg21c/ggsci/01_add_replicats.oby", lines)

        # 02: START ALL
        lines = ["-- Step 2: Start all GG 21c Snowflake Replicats", ""]
        for g in sf_groups:
            lines.append(f"START REPLICAT {g.replicat_name}")
        lines += ["", "INFO ALL"]
        self._write_oby("gg21c/ggsci/02_start_all.oby", lines)

        # 03: STATUS
        lines = ["-- Step 3: Check GG 21c status", ""]
        for g in sf_groups:
            lines.append(f"INFO REPLICAT {g.replicat_name}, DETAIL")
        lines += ["", "INFO ALL", "LAG REPLICAT *"]
        self._write_oby("gg21c/ggsci/03_status.oby", lines)

    # -------------------------------------------------------------------
    # Heartbeat table — end-to-end lag monitoring
    # -------------------------------------------------------------------

    def _gen_heartbeat_setup(self) -> None:
        """Generate heartbeat table setup scripts for GG 19c and 21c."""
        c = self.cfg
        hb = c.get("heartbeat", {})
        if not hb.get("enabled", True):
            return

        alias = c["source"]["alias"]
        hb_schema = hb.get("schema", c["source"]["alias"].upper())
        hb_frequency = hb.get("frequency_seconds", 60)
        hb_retain_hours = hb.get("retain_hours", 24)
        hb_purge_frequency = hb.get("purge_frequency_minutes", 10)

        # GG 19c: Setup heartbeat table
        lines = [
            "-- Heartbeat Table Setup (GG 19c)",
            "-- Creates GG_HEARTBEAT + GG_HEARTBEAT_HISTORY tables in Oracle",
            "-- The extract automatically writes a heartbeat row every N seconds.",
            "-- The replicat reads it on the target to calculate true end-to-end lag.",
            "",
            f"DBLOGIN USERIDALIAS {alias}",
            "",
            "-- Add heartbeat table (creates table + scheduled job)",
            f"ADD HEARTBEATTABLE, UPDATE FREQUENCY {hb_frequency}, "
            f"PURGE FREQUENCY {hb_purge_frequency}, "
            f"RETAIN {hb_retain_hours} HOURS",
            "",
            "-- Verify heartbeat table was created",
            "INFO HEARTBEATTABLE",
            "",
        ]
        self._write_oby("gg19c/ggsci/00_heartbeat_setup.oby", lines)

        # GG 21c: Heartbeat monitoring query (for Snowflake)
        snowflake_hb_ddl = [
            "-- Heartbeat Monitoring Table (Snowflake)",
            "-- This table receives heartbeat rows from GG replicat.",
            "-- Query it to measure true end-to-end lag:",
            "--",
            "--   SELECT",
            "--     INCOMING_HEARTBEAT_TS,",
            "--     CURRENT_TIMESTAMP() AS snowflake_ts,",
            "--     TIMESTAMPDIFF('SECOND', INCOMING_HEARTBEAT_TS, CURRENT_TIMESTAMP()) AS lag_seconds",
            "--   FROM GG_HEARTBEAT",
            "--   ORDER BY INCOMING_HEARTBEAT_TS DESC LIMIT 1;",
            "--",
            "-- Automated lag alert query:",
            "--   SELECT CASE",
            "--     WHEN TIMESTAMPDIFF('SECOND', MAX(INCOMING_HEARTBEAT_TS), CURRENT_TIMESTAMP()) > 300",
            "--     THEN 'CRITICAL: GG lag > 5 minutes'",
            "--     WHEN TIMESTAMPDIFF('SECOND', MAX(INCOMING_HEARTBEAT_TS), CURRENT_TIMESTAMP()) > 60",
            "--     THEN 'WARNING: GG lag > 1 minute'",
            "--     ELSE 'OK'",
            "--   END AS lag_status",
            "--   FROM GG_HEARTBEAT;",
            "",
            "CREATE TABLE IF NOT EXISTS GG_HEARTBEAT (",
            "    LOCAL_CSN                  NUMBER,",
            "    LOCAL_CSN_COMMIT_TS        TIMESTAMP_NTZ,",
            "    INCOMING_EXTRACT           VARCHAR(50),",
            "    INCOMING_ROUTING_PATH      VARCHAR(500),",
            "    INCOMING_COMMIT_TS         TIMESTAMP_NTZ,",
            "    INCOMING_HEARTBEAT_TS      TIMESTAMP_NTZ,",
            "    INCOMING_LAG               NUMBER,",
            "    OP_TYPE                    VARCHAR(1),",
            "    OP_TS                      TIMESTAMP_NTZ,",
            "    IS_DELETED                 NUMBER(1,0) DEFAULT 0",
            ");",
            "",
            "CREATE TABLE IF NOT EXISTS GG_HEARTBEAT_HISTORY (",
            "    LOCAL_CSN                  NUMBER,",
            "    LOCAL_CSN_COMMIT_TS        TIMESTAMP_NTZ,",
            "    INCOMING_EXTRACT           VARCHAR(50),",
            "    INCOMING_ROUTING_PATH      VARCHAR(500),",
            "    INCOMING_COMMIT_TS         TIMESTAMP_NTZ,",
            "    INCOMING_HEARTBEAT_TS      TIMESTAMP_NTZ,",
            "    INCOMING_LAG               NUMBER,",
            "    OP_TYPE                    VARCHAR(1),",
            "    OP_TS                      TIMESTAMP_NTZ,",
            "    IS_DELETED                 NUMBER(1,0) DEFAULT 0",
            ");",
        ]
        self._write("gg21c/heartbeat/snowflake_heartbeat_ddl.sql",
                     "\n".join(snowflake_hb_ddl))

    # -------------------------------------------------------------------
    # SCN helper — start extract/replicat from specific SCN
    # -------------------------------------------------------------------

    def _gen_scn_helper(self) -> None:
        """Generate SCN helper scripts for disaster recovery scenarios."""
        c = self.cfg
        alias = c["source"]["alias"]
        trail = c["gg19c"]["trail"]
        t_dir = self.cfg["gg21c"].get("trail", {}).get("dir", "./dirdat")

        # Oracle query to find current SCN
        scn_query = [
            "-- =============================================================================",
            "-- get_current_scn.sql",
            "-- ",
            "-- Run in Oracle to get the current System Change Number (SCN).",
            "-- Use this SCN to start/restart an extract from a known point.",
            "-- =============================================================================",
            "",
            "-- Current SCN",
            "SELECT CURRENT_SCN FROM V$DATABASE;",
            "",
            "-- Find SCN at a specific point in time (useful for recovery)",
            "-- Replace the timestamp with your desired recovery point:",
            "SELECT TIMESTAMP_TO_SCN(TO_TIMESTAMP('2026-03-18 10:00:00', 'YYYY-MM-DD HH24:MI:SS'))",
            "       AS recovery_scn",
            "FROM DUAL;",
            "",
            "-- Find SCN from GG extract checkpoint (last processed position)",
            "-- Run in GGSCI: INFO EXTRACT EXTnn, SHOWCH",
            "",
            "-- LogMiner checkpoint SCN (what the extract has read up to)",
            "SELECT * FROM DBA_LOGMNR_SESSION WHERE SESSION_NAME LIKE 'OGG%';",
        ]
        self._write("sql/get_current_scn.sql", "\n".join(scn_query))

        # GG 19c: SCN-based add extract (per group)
        lines = [
            "-- =============================================================================",
            "-- Start extracts from a specific SCN (disaster recovery)",
            "-- =============================================================================",
            "--",
            "-- USAGE:",
            "--   1. Find the recovery SCN:",
            "--      sqlplus> @sql/get_current_scn.sql",
            "--",
            "--   2. Edit the SCN values below",
            "--",
            "--   3. Run this obeyfile:",
            "--      ggsci> OBEY gg19c/ggsci/06_add_extracts_from_scn.oby",
            "--",
            "--   Or use ggctl:",
            "--      ggctl start-from-scn EXT01 12345678",
            "-- =============================================================================",
            "",
            f"DBLOGIN USERIDALIAS {alias}",
            "",
            "-- !! EDIT THESE SCN VALUES BEFORE RUNNING !!",
            "",
        ]
        for g in self.groups:
            lines += [
                f"-- {g.extract_name}: Replace <SCN> with actual SCN value",
                f"-- DELETE EXTRACT {g.extract_name}",
                f"-- UNREGISTER EXTRACT {g.extract_name} DATABASE",
                f"-- ADD EXTRACT {g.extract_name}, INTEGRATED TRANLOG, SCN <SCN>",
                f"-- ADD EXTTRAIL {trail['dir']}/{g.extract_trail}, EXTRACT {g.extract_name}",
                f"-- REGISTER EXTRACT {g.extract_name} DATABASE",
                f"-- START EXTRACT {g.extract_name}",
                "",
            ]
        self._write_oby("gg19c/ggsci/06_add_extracts_from_scn.oby", lines)

        # GG 21c: SCN-based replicat positioning
        lines = [
            "-- =============================================================================",
            "-- Position replicat at specific trail file + RBA (disaster recovery)",
            "-- =============================================================================",
            "--",
            "-- USAGE:",
            "--   1. Find the trail file and RBA from the last good checkpoint:",
            "--      ggsci> INFO REPLICAT REPnn, DETAIL",
            "--",
            "--   2. Edit the trail file path and RBA below",
            "--",
            "--   3. Run this obeyfile:",
            "--      ggsci> OBEY gg21c/ggsci/04_position_replicat.oby",
            "-- =============================================================================",
            "",
        ]
        for g in self.groups:
            lines += [
                f"-- {g.replicat_name}: Replace <trail_file> and <rba> with actual values",
                f"-- STOP REPLICAT {g.replicat_name}",
                f"-- ALTER REPLICAT {g.replicat_name}, EXTSEQNO <trail_seq>, EXTRBA <rba>",
                f"-- START REPLICAT {g.replicat_name}",
                "",
            ]
        self._write_oby("gg21c/ggsci/04_position_replicat.oby", lines)

    def _write_oby(self, rel_path: str, lines: List[str]) -> None:
        self._write(rel_path, "\n".join(lines))

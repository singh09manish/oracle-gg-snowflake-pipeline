"""
Oracle pre-flight validation for GoldenGate pipeline.

Checks BEFORE deploying:
  1. Oracle RDS connectivity
  2. Every table in the inventory exists
  3. Every table has a primary key (required for MERGE/upsert on Snowflake)
  4. Supplemental logging status per table
  5. LOB column detection (affects FETCHOPTIONS tuning)

Usage:
  python3 -m app.main validate --input input/table_inventory.xlsx
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.models import TableInfo

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of pre-flight validation."""
    tables_checked: int = 0
    tables_ok: int = 0
    missing_tables: List[str] = field(default_factory=list)
    no_pk_tables: List[str] = field(default_factory=list)
    no_trandata: List[str] = field(default_factory=list)
    lob_tables: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.missing_tables and not self.no_pk_tables and not self.errors

    def summary_lines(self) -> List[str]:
        lines = [
            f"Tables checked:     {self.tables_checked}",
            f"Tables OK:          {self.tables_ok}",
            f"Missing in Oracle:  {len(self.missing_tables)}",
            f"No primary key:     {len(self.no_pk_tables)}",
            f"No TRANDATA:        {len(self.no_trandata)}",
            f"Has LOB columns:    {len(self.lob_tables)}",
            f"Errors:             {len(self.errors)}",
            "",
            f"RESULT: {'PASSED' if self.passed else 'FAILED'}",
        ]
        if self.missing_tables:
            lines += ["", "MISSING TABLES (do not exist in Oracle):"]
            for t in self.missing_tables[:20]:
                lines.append(f"  - {t}")
            if len(self.missing_tables) > 20:
                lines.append(f"  ... and {len(self.missing_tables) - 20} more")

        if self.no_pk_tables:
            lines += ["", "TABLES WITHOUT PRIMARY KEY (MERGE/upsert will fail):"]
            for t in self.no_pk_tables[:20]:
                lines.append(f"  - {t}")
            if len(self.no_pk_tables) > 20:
                lines.append(f"  ... and {len(self.no_pk_tables) - 20} more")

        if self.no_trandata:
            lines += ["", "TABLES WITHOUT SUPPLEMENTAL LOGGING (run 01_trandata.oby first):"]
            for t in self.no_trandata[:20]:
                lines.append(f"  - {t}")

        if self.lob_tables:
            lines += ["", f"TABLES WITH LOB COLUMNS ({len(self.lob_tables)} — FETCHOPTIONS enabled):"]
            for t in self.lob_tables[:10]:
                lines.append(f"  - {t}")

        return lines


class OracleValidator:
    """Validates Oracle RDS readiness for GoldenGate extract."""

    def __init__(self, env: Dict[str, Any]) -> None:
        self.env = env
        self._conn = None

    def validate(self, tables: List[TableInfo]) -> ValidationResult:
        result = ValidationResult()
        try:
            self._connect()
            for t in tables:
                result.tables_checked += 1
                self._check_table(t, result)
            result.tables_ok = (
                result.tables_checked
                - len(result.missing_tables)
                - len(result.no_pk_tables)
                - len(result.errors)
            )
        except Exception as e:
            result.errors.append(f"Connection failed: {e}")
            log.error("Oracle connection failed: %s", e)
        finally:
            self._close()
        return result

    def _connect(self) -> None:
        import oracledb
        rds = self.env["oracle_rds"]
        dsn = oracledb.makedsn(
            rds["host"], int(rds.get("port", 1521)),
            service_name=rds["service_name"],
        )
        self._conn = oracledb.connect(
            user=rds["user"], password=rds["password"], dsn=dsn,
        )
        log.info("Connected to Oracle RDS: %s", rds["host"])

    def _close(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

    def _check_table(self, t: TableInfo, result: ValidationResult) -> None:
        cur = self._conn.cursor()
        try:
            # 1. Table exists?
            cur.execute(
                "SELECT COUNT(*) FROM all_tables WHERE owner = :o AND table_name = :t",
                o=t.schema, t=t.name,
            )
            if cur.fetchone()[0] == 0:
                result.missing_tables.append(t.fqn)
                return

            # 2. Primary key?
            cur.execute(
                "SELECT COUNT(*) FROM all_constraints "
                "WHERE owner = :o AND table_name = :t AND constraint_type = 'P'",
                o=t.schema, t=t.name,
            )
            if cur.fetchone()[0] == 0:
                result.no_pk_tables.append(t.fqn)

            # 3. Get PK columns (enrich the TableInfo)
            cur.execute(
                "SELECT LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) "
                "FROM all_cons_columns cc "
                "JOIN all_constraints c ON c.owner = cc.owner "
                "  AND c.constraint_name = cc.constraint_name "
                "WHERE c.owner = :o AND c.table_name = :t AND c.constraint_type = 'P'",
                o=t.schema, t=t.name,
            )
            row = cur.fetchone()
            if row and row[0]:
                t.pk_columns = row[0]

            # 4. LOB columns?
            cur.execute(
                "SELECT COUNT(*) FROM all_tab_columns "
                "WHERE owner = :o AND table_name = :t "
                "AND data_type IN ('CLOB','BLOB','NCLOB','LONG','LONG RAW')",
                o=t.schema, t=t.name,
            )
            if cur.fetchone()[0] > 0:
                t.has_lobs = True
                result.lob_tables.append(t.fqn)

            # 5. Supplemental logging (TRANDATA)?
            cur.execute(
                "SELECT COUNT(*) FROM all_log_groups "
                "WHERE owner = :o AND table_name = :t",
                o=t.schema, t=t.name,
            )
            if cur.fetchone()[0] == 0:
                result.no_trandata.append(t.fqn)
            else:
                t.has_trandata = True

        except Exception as e:
            result.errors.append(f"{t.fqn}: {e}")
            log.warning("Error checking %s: %s", t.fqn, e)
        finally:
            cur.close()

    def save_report(self, result: ValidationResult, reports_dir: Path) -> Path:
        reports_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out = reports_dir / f"validation_{ts}.json"
        data = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "tables_checked": result.tables_checked,
            "tables_ok": result.tables_ok,
            "missing_tables": result.missing_tables,
            "no_pk_tables": result.no_pk_tables,
            "no_trandata": result.no_trandata,
            "lob_tables": result.lob_tables,
            "errors": result.errors,
            "passed": result.passed,
        }
        out.write_text(json.dumps(data, indent=2))
        log.info("Validation report: %s", out)
        return out

#!/usr/bin/env python3
"""
event_watcher.py — Real-time GoldenGate ABEND detection & notification
========================================================================

Watches ggserr.log for ABEND events, identifies the failing table from
report files, sends notifications (Slack, email, PagerDuty, webhook),
and optionally auto-disables the failing table in the inventory.

Architecture:
  ggserr.log → tail -F → regex match → parse report → identify table
  → notify (Slack/email/PD) → optionally auto-disable in inventory

Usage:
  # Watch GG 19c logs (extract/pump ABEND detection):
  python3 scripts/event_watcher.py \
      --gg-home /u01/app/oracle/product/19c/oggcore_1 \
      --inventory input/table_inventory.xlsx

  # Watch GG 21c logs (replicat ABEND detection):
  python3 scripts/event_watcher.py \
      --gg-home /u01/app/oracle/product/21c/oggbd_1 \
      --inventory input/table_inventory.xlsx

  # Enable auto-disable (failing table auto-excluded from next generate):
  python3 scripts/event_watcher.py \
      --gg-home /u01/app/oracle/product/19c/oggcore_1 \
      --inventory input/table_inventory.xlsx \
      --auto-disable

  # Run as a systemd service or cron (see docs below)

Notification channels are read from config/pipeline.yaml (monitoring.alerting).

Systemd service setup:
  sudo cp scripts/gg-event-watcher.service /etc/systemd/system/
  sudo systemctl enable --now gg-event-watcher
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

# Allow running from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("event_watcher")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regex patterns for ggserr.log event detection
RE_ABEND = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T[\d:.]+Z?)\s+"
    r"(?:ERROR|CRITICAL)\s+OGG-\d+\s+.*"
    r"(?:EXTRACT|REPLICAT)\s+(?P<process>\w+)\s+.*"
    r"(?:ABEND|ABENDED|PROCESS ABENDING)",
    re.IGNORECASE,
)

# Simpler fallback — matches "PROCESS ABENDING" or "abended" lines
RE_ABEND_SIMPLE = re.compile(
    r"(ABEND|PROCESS ABENDING|abended)",
    re.IGNORECASE,
)

# Extract process name from ggserr.log line
RE_PROCESS_NAME = re.compile(
    r"(?:EXTRACT|REPLICAT|PUMP)\s+(\w+)",
    re.IGNORECASE,
)

# OGG error codes in report files
RE_OGG_ERROR = re.compile(r"(OGG-\d+)")

# Table name from error context (e.g., "TABLE HR.EMPLOYEES" or "SCHEMA.TABLE")
RE_TABLE_IN_ERROR = re.compile(
    r"(?:TABLE|MAP)\s+(\w+\.\w+)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Event model
# ---------------------------------------------------------------------------

class GGEvent:
    """Represents a detected GoldenGate event."""

    def __init__(
        self,
        event_type: str,          # ABEND, WARNING, LAG_CRITICAL
        process_name: str,        # EXT01, PMP01, REP01
        process_type: str,        # EXTRACT, PUMP, REPLICAT
        timestamp: str,
        error_code: str = "",     # OGG-01004
        error_message: str = "",
        failing_table: str = "",  # SCHEMA.TABLE (if identified)
        report_excerpt: str = "", # last N lines of report file
        gg_home: str = "",
    ):
        self.event_type = event_type
        self.process_name = process_name
        self.process_type = process_type
        self.timestamp = timestamp
        self.error_code = error_code
        self.error_message = error_message
        self.failing_table = failing_table
        self.report_excerpt = report_excerpt
        self.gg_home = gg_home

    def to_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "process_name": self.process_name,
            "process_type": self.process_type,
            "timestamp": self.timestamp,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "failing_table": self.failing_table,
            "gg_home": self.gg_home,
        }

    def summary(self) -> str:
        lines = [
            f"GoldenGate {self.event_type} Detected",
            f"  Process:   {self.process_name} ({self.process_type})",
            f"  Timestamp: {self.timestamp}",
        ]
        if self.error_code:
            lines.append(f"  Error:     {self.error_code}")
        if self.error_message:
            lines.append(f"  Message:   {self.error_message}")
        if self.failing_table:
            lines.append(f"  Table:     {self.failing_table}")
        if self.report_excerpt:
            lines.append(f"  Report excerpt:")
            for l in self.report_excerpt.splitlines()[-10:]:
                lines.append(f"    {l}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Log watcher (tail -F with event detection)
# ---------------------------------------------------------------------------

class LogWatcher:
    """Watches ggserr.log for ABEND and error events."""

    def __init__(self, gg_home: str):
        self.gg_home = Path(gg_home)
        self.ggserr_path = self.gg_home / "ggserr.log"
        self.dirrpt = self.gg_home / "dirrpt"

        if not self.ggserr_path.exists():
            raise FileNotFoundError(f"ggserr.log not found: {self.ggserr_path}")

    def watch(self):
        """
        Generator that yields GGEvent objects as they occur.
        Uses tail -F to follow the log file in real time.
        """
        log.info("Watching: %s", self.ggserr_path)
        log.info("Report dir: %s", self.dirrpt)

        # Start from end of file (only watch new events)
        proc = subprocess.Popen(
            ["tail", "-F", str(self.ggserr_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,  # line-buffered
        )

        try:
            for line in proc.stdout:
                line = line.strip()
                if not line:
                    continue

                # Check for ABEND
                if RE_ABEND_SIMPLE.search(line):
                    event = self._parse_abend(line)
                    if event:
                        yield event
        except KeyboardInterrupt:
            log.info("Watcher stopped by user.")
        finally:
            proc.terminate()
            proc.wait()

    def _parse_abend(self, line: str) -> Optional[GGEvent]:
        """Parse an ABEND line and enrich from report file."""
        # Extract process name
        m = RE_PROCESS_NAME.search(line)
        if not m:
            log.warning("ABEND detected but could not parse process name: %s", line)
            return None

        process_name = m.group(1).upper()
        process_type = self._infer_process_type(process_name)
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Read the report file for details
        report_path = self.dirrpt / f"{process_name}.rpt"
        error_code = ""
        error_message = ""
        failing_table = ""
        report_excerpt = ""

        if report_path.exists():
            report_excerpt, error_code, error_message, failing_table = \
                self._parse_report(report_path)

        return GGEvent(
            event_type="ABEND",
            process_name=process_name,
            process_type=process_type,
            timestamp=timestamp,
            error_code=error_code,
            error_message=error_message,
            failing_table=failing_table,
            report_excerpt=report_excerpt,
            gg_home=str(self.gg_home),
        )

    def _parse_report(self, report_path: Path):
        """
        Read the last 50 lines of a report file to extract:
        - OGG error code
        - Error message
        - Failing table name
        """
        try:
            result = subprocess.run(
                ["tail", "-50", str(report_path)],
                capture_output=True, text=True, timeout=5,
            )
            excerpt = result.stdout
        except Exception:
            return "", "", "", ""

        # Find OGG error code
        error_code = ""
        error_codes = RE_OGG_ERROR.findall(excerpt)
        if error_codes:
            error_code = error_codes[-1]  # last error is usually the cause

        # Find error message (line containing the OGG error code)
        error_message = ""
        for line in reversed(excerpt.splitlines()):
            if error_code and error_code in line:
                error_message = line.strip()
                break
            elif "ERROR" in line.upper() or "ABEND" in line.upper():
                error_message = line.strip()
                break

        # Find failing table
        failing_table = ""
        table_matches = RE_TABLE_IN_ERROR.findall(excerpt)
        if table_matches:
            failing_table = table_matches[-1].upper()

        return excerpt, error_code, error_message, failing_table

    @staticmethod
    def _infer_process_type(name: str) -> str:
        name_upper = name.upper()
        if name_upper.startswith("EXT"):
            return "EXTRACT"
        elif name_upper.startswith("PMP"):
            return "PUMP"
        elif name_upper.startswith("REP"):
            return "REPLICAT"
        return "UNKNOWN"


# ---------------------------------------------------------------------------
# Notifiers
# ---------------------------------------------------------------------------

class Notifier:
    """Send alerts via configured channels (from pipeline.yaml)."""

    def __init__(self, config: dict):
        self.cfg = config.get("monitoring", {}).get("alerting", {})
        self.log_file = self.cfg.get("log_file", "/var/log/gg-monitor/alerts.log")

    def notify(self, event: GGEvent) -> None:
        """Send notification through all enabled channels."""
        message = event.summary()
        subject = f"GG {event.event_type}: {event.process_name}"

        # Always log to file
        self._log_to_file(event, message)

        # Slack
        if self.cfg.get("slack", {}).get("enabled"):
            self._notify_slack(event, message)

        # Email
        if self.cfg.get("email", {}).get("enabled"):
            self._notify_email(subject, message)

        # PagerDuty
        if self.cfg.get("pagerduty", {}).get("enabled"):
            self._notify_pagerduty(event)

        # Generic webhook
        if self.cfg.get("webhook", {}).get("enabled"):
            self._notify_webhook(event)

    def _log_to_file(self, event: GGEvent, message: str) -> None:
        try:
            log_dir = Path(self.log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            with open(self.log_file, "a") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{event.timestamp}] {event.event_type}\n")
                f.write(message)
                f.write(f"\n{'='*60}\n")
        except Exception as e:
            log.error("Failed to write alert log: %s", e)

    def _notify_slack(self, event: GGEvent, message: str) -> None:
        slack_cfg = self.cfg["slack"]
        webhook_url = slack_cfg["webhook_url"]
        channel = slack_cfg.get("channel", "#gg-alerts")

        # Build Slack block message
        color = "#FF0000" if event.event_type == "ABEND" else "#FFA500"
        payload = {
            "channel": channel,
            "attachments": [{
                "color": color,
                "title": f"GoldenGate {event.event_type}: {event.process_name}",
                "fields": [
                    {"title": "Process", "value": f"{event.process_name} ({event.process_type})", "short": True},
                    {"title": "Error", "value": event.error_code or "N/A", "short": True},
                    {"title": "Failing Table", "value": event.failing_table or "N/A", "short": True},
                    {"title": "Timestamp", "value": event.timestamp, "short": True},
                ],
                "text": event.error_message or message,
                "footer": f"GG Home: {event.gg_home}",
            }],
        }

        try:
            import urllib.request
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            log.info("Slack notification sent for %s", event.process_name)
        except Exception as e:
            log.error("Slack notification failed: %s", e)

    def _notify_email(self, subject: str, body: str) -> None:
        email_cfg = self.cfg["email"]
        recipients = email_cfg["recipients"]
        from_addr = email_cfg.get("from", "gg-monitor@localhost")

        try:
            proc = subprocess.run(
                ["mail", "-s", subject, "-r", from_addr, recipients],
                input=body, text=True, timeout=30,
                capture_output=True,
            )
            if proc.returncode == 0:
                log.info("Email sent to %s", recipients)
            else:
                log.error("Email failed: %s", proc.stderr)
        except Exception as e:
            log.error("Email notification failed: %s", e)

    def _notify_pagerduty(self, event: GGEvent) -> None:
        pd_cfg = self.cfg["pagerduty"]
        routing_key = pd_cfg["routing_key"]

        severity = "critical" if event.event_type == "ABEND" else "warning"
        payload = {
            "routing_key": routing_key,
            "event_action": "trigger",
            "payload": {
                "summary": f"GG {event.event_type}: {event.process_name} — {event.error_code}",
                "severity": severity,
                "source": event.gg_home,
                "component": event.process_name,
                "custom_details": event.to_dict(),
            },
        }

        try:
            import urllib.request
            req = urllib.request.Request(
                "https://events.pagerduty.com/v2/enqueue",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            log.info("PagerDuty alert triggered for %s", event.process_name)
        except Exception as e:
            log.error("PagerDuty notification failed: %s", e)

    def _notify_webhook(self, event: GGEvent) -> None:
        wh_cfg = self.cfg["webhook"]
        url = wh_cfg["url"]
        method = wh_cfg.get("method", "POST").upper()

        try:
            import urllib.request
            data = json.dumps(event.to_dict()).encode("utf-8")
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Content-Type", "application/json")
            # Add custom headers
            extra_headers = wh_cfg.get("headers", "")
            if extra_headers and ":" in extra_headers:
                for hdr in extra_headers.split(","):
                    if ":" in hdr:
                        k, v = hdr.split(":", 1)
                        req.add_header(k.strip(), v.strip())
            urllib.request.urlopen(req, timeout=10)
            log.info("Webhook notification sent to %s", url)
        except Exception as e:
            log.error("Webhook notification failed: %s", e)


# ---------------------------------------------------------------------------
# Auto-disable (optional)
# ---------------------------------------------------------------------------

def auto_disable_table(
    inventory_path: str,
    fqn: str,
    reason: str,
) -> bool:
    """
    Disable a failing table in the inventory Excel file.
    Returns True if the table was found and disabled.
    """
    try:
        from app.inventory import read_inventory, write_inventory, toggle_table

        tables = read_inventory(inventory_path)
        toggle_table(tables, fqn, enable=False, reason=reason)
        write_inventory(tables, inventory_path)

        log.info("AUTO-DISABLED %s in %s — reason: %s", fqn, inventory_path, reason)
        log.info("Run 'python3 -m app.main generate --input %s' to regenerate configs.", inventory_path)
        return True
    except KeyError:
        log.warning("Table %s not found in inventory — cannot auto-disable.", fqn)
        return False
    except Exception as e:
        log.error("Auto-disable failed for %s: %s", fqn, e)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Real-time GoldenGate ABEND watcher with notifications",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--gg-home", required=True,
                        help="GoldenGate home directory (contains ggserr.log)")
    parser.add_argument("--config", default=str(PROJECT_ROOT / "config" / "pipeline.yaml"),
                        help="Path to pipeline.yaml for notification config")
    parser.add_argument("--inventory", default=str(PROJECT_ROOT / "input" / "table_inventory.xlsx"),
                        help="Path to table inventory (for auto-disable)")
    parser.add_argument("--auto-disable", action="store_true",
                        help="Automatically disable failing tables in inventory")
    parser.add_argument("--auto-regenerate", action="store_true",
                        help="Automatically regenerate .prm files after auto-disable "
                             "(implies --auto-disable)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print events but don't notify or auto-disable")

    args = parser.parse_args()

    if args.auto_regenerate:
        args.auto_disable = True

    # Load config
    config = {}
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    else:
        log.warning("Config not found: %s — notifications will only go to log file.", args.config)

    notifier = Notifier(config)
    watcher = LogWatcher(args.gg_home)

    log.info("=" * 60)
    log.info("GoldenGate Event Watcher started")
    log.info("  GG Home:        %s", args.gg_home)
    log.info("  Inventory:      %s", args.inventory)
    log.info("  Auto-disable:   %s", args.auto_disable)
    log.info("  Auto-regenerate:%s", args.auto_regenerate)
    log.info("  Dry-run:        %s", args.dry_run)
    log.info("=" * 60)

    for event in watcher.watch():
        log.info("EVENT DETECTED:\n%s", event.summary())

        if args.dry_run:
            log.info("[DRY-RUN] Would notify and potentially disable %s", event.failing_table)
            continue

        # Send notifications
        notifier.notify(event)

        # Auto-disable failing table
        if args.auto_disable and event.failing_table:
            reason = f"Auto-disabled: {event.event_type} in {event.process_name} — {event.error_code}"
            disabled = auto_disable_table(args.inventory, event.failing_table, reason)

            if disabled and args.auto_regenerate:
                log.info("Auto-regenerating .prm files...")
                try:
                    result = subprocess.run(
                        [sys.executable, "-m", "app.main", "generate",
                         "--input", args.inventory],
                        capture_output=True, text=True, timeout=60,
                        cwd=str(PROJECT_ROOT),
                    )
                    if result.returncode == 0:
                        log.info("Configs regenerated successfully.")
                        log.info("IMPORTANT: Deploy new configs and restart %s manually.",
                                 event.process_name)
                    else:
                        log.error("Config regeneration failed:\n%s", result.stderr)
                except Exception as e:
                    log.error("Auto-regenerate failed: %s", e)


if __name__ == "__main__":
    main()

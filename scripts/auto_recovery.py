#!/usr/bin/env python3
"""
auto_recovery.py — Automated ABEND Recovery Daemon for GoldenGate
=================================================================

Watches ggserr.log on GG 19c and/or GG 21c, classifies errors into
TRANSIENT (auto-restart safe), PERMANENT (needs human), or UNKNOWN
(notify without restart), and takes appropriate recovery action.

Recovery strategy:
  TRANSIENT:
    - Exponential backoff (30s, 60s, 120s, 240s) with configurable retries.
    - Restart process via GGSCI (START EXTRACT/REPLICAT).
    - Escalate to PERMANENT after max retries exceeded.

  PERMANENT:
    - Parse report file to identify failing table.
    - Optionally auto-disable the table in inventory (--auto-disable).
    - Optionally auto-regenerate configs (--auto-regenerate).
    - Auto-restart the process with the failing table removed.
    - Send notification with full error context.

  UNKNOWN:
    - Send notification but do NOT auto-restart.

Usage:
  # Watch both GG 19c and GG 21c (default):
  python3 scripts/auto_recovery.py --watch-all

  # Watch only GG 19c:
  python3 scripts/auto_recovery.py --watch-19c-only

  # Watch only GG 21c:
  python3 scripts/auto_recovery.py --watch-21c-only

  # Full auto-recovery (disable failing table + regenerate + restart):
  python3 scripts/auto_recovery.py --watch-all --auto-disable --auto-regenerate

  # Custom retry settings:
  python3 scripts/auto_recovery.py --watch-all --max-retries 3 --base-backoff 15

Reads GG homes and notification config from config/pipeline.yaml.
Writes structured recovery logs to /var/log/gg-monitor/recovery.log.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("auto_recovery")


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------

class ErrorClass(Enum):
    """Classification of a GoldenGate error for recovery decisions."""
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"
    UNKNOWN = "UNKNOWN"


# Transient errors: safe to auto-restart with backoff
TRANSIENT_ERRORS: Dict[str, str] = {
    "OGG-00868": "TCP/IP connection timeout",
    "OGG-01234": "extract process stopped",
    "OGG-02075": "manager port conflict",
    "SQLException: Connection refused": "Snowflake connection refused",
    "Warehouse .* is suspended": "Snowflake warehouse suspended",
    "429": "Snowflake rate limited",
    "OGG-00446": "TCP/IP error: connection reset",
    "OGG-01031": "process restart required",
    "OGG-02089": "extract lag exceeded threshold",
    "ORA-03113": "end-of-file on communication channel",
    "ORA-03114": "not connected to Oracle",
    "ORA-12170": "TNS connect timeout",
    "ORA-12541": "TNS no listener",
    "ORA-12543": "TNS destination host unreachable",
}

# Permanent errors: needs human intervention
PERMANENT_ERRORS: Dict[str, str] = {
    "OGG-01004": "table not found in source",
    "OGG-01028": "missing primary key",
    "OGG-01163": "DDL not supported",
    "OGG-02091": "DDL change detected",
    "OGG-01194": "credential expired",
    "OGG-01168": "column not found",
    "OGG-01027": "table definition mismatch",
    "OGG-01161": "bad column mapping",
    "OGG-06439": "handler configuration error",
}


def classify_error(error_text: str) -> Tuple[ErrorClass, str]:
    """
    Classify an error line or report excerpt into TRANSIENT, PERMANENT, or UNKNOWN.

    Returns (ErrorClass, description).
    """
    # Check permanent errors first (more specific, takes priority)
    for pattern, description in PERMANENT_ERRORS.items():
        if re.search(re.escape(pattern), error_text, re.IGNORECASE):
            return ErrorClass.PERMANENT, f"{pattern}: {description}"

    # Check transient errors
    for pattern, description in TRANSIENT_ERRORS.items():
        # Some patterns use regex metacharacters (e.g., "Warehouse .* is suspended")
        try:
            if re.search(pattern, error_text, re.IGNORECASE):
                return ErrorClass.TRANSIENT, f"{pattern}: {description}"
        except re.error:
            # Fall back to literal match if regex fails
            if pattern.lower() in error_text.lower():
                return ErrorClass.TRANSIENT, f"{pattern}: {description}"

    return ErrorClass.UNKNOWN, "Unrecognized error pattern"


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

RE_ABEND = re.compile(
    r"(ABEND|PROCESS ABENDING|abended|STOPPED)",
    re.IGNORECASE,
)

RE_PROCESS_NAME = re.compile(
    r"(?:EXTRACT|REPLICAT|PUMP)\s+(\w+)",
    re.IGNORECASE,
)

RE_OGG_ERROR = re.compile(r"(OGG-\d+)")

RE_TABLE_IN_ERROR = re.compile(
    r"(?:TABLE|MAP)\s+(\w+\.\w+)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Recovery log (structured output)
# ---------------------------------------------------------------------------

class RecoveryLogger:
    """Writes structured JSON-line entries to the recovery log file."""

    def __init__(self, log_path: str = "/var/log/gg-monitor/recovery.log"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        event_type: str,
        process_name: str,
        error_class: str,
        error_pattern: str,
        action_taken: str,
        attempt: int = 0,
        max_retries: int = 0,
        failing_table: str = "",
        gg_home: str = "",
        success: bool = False,
        details: str = "",
    ) -> None:
        """Append a structured log entry."""
        entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "event_type": event_type,
            "process_name": process_name,
            "error_class": error_class,
            "error_pattern": error_pattern,
            "action_taken": action_taken,
            "attempt": attempt,
            "max_retries": max_retries,
            "failing_table": failing_table,
            "gg_home": gg_home,
            "success": success,
            "details": details,
        }
        try:
            with open(self.log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            log.error("Failed to write recovery log: %s", e)


# ---------------------------------------------------------------------------
# GGSCI command runner
# ---------------------------------------------------------------------------

def run_ggsci_command(gg_home: str, command: str, timeout: int = 30) -> Tuple[bool, str]:
    """
    Execute a GGSCI command by piping it to the ggsci binary.

    Returns (success: bool, output: str).
    """
    ggsci_path = Path(gg_home) / "ggsci"
    if not ggsci_path.exists():
        return False, f"ggsci not found at {ggsci_path}"

    try:
        env = os.environ.copy()
        env["LD_LIBRARY_PATH"] = f"{gg_home}/lib:{env.get('LD_LIBRARY_PATH', '')}"

        result = subprocess.run(
            [str(ggsci_path)],
            input=command + "\nEXIT\n",
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=gg_home,
            env=env,
        )
        output = result.stdout + result.stderr
        # Check for success indicators
        success = result.returncode == 0 and "ERROR" not in output.upper()
        return success, output.strip()
    except subprocess.TimeoutExpired:
        return False, f"GGSCI command timed out after {timeout}s"
    except Exception as e:
        return False, f"GGSCI execution failed: {e}"


def restart_process(gg_home: str, process_name: str) -> Tuple[bool, str]:
    """
    Restart a GG process (extract or replicat) via GGSCI.

    Determines the correct START command based on the process name prefix.
    """
    name_upper = process_name.upper()
    if name_upper.startswith("EXT") or name_upper.startswith("PMP"):
        cmd = f"START EXTRACT {name_upper}"
    elif name_upper.startswith("REP"):
        cmd = f"START REPLICAT {name_upper}"
    else:
        return False, f"Cannot determine process type for: {name_upper}"

    log.info("Executing GGSCI: %s (home: %s)", cmd, gg_home)
    return run_ggsci_command(gg_home, cmd)


# ---------------------------------------------------------------------------
# Report file parser
# ---------------------------------------------------------------------------

def parse_report_file(gg_home: str, process_name: str) -> Dict[str, str]:
    """
    Parse the report file for a process to extract error details.

    Returns a dict with keys: excerpt, error_code, error_message, failing_table.
    """
    report_path = Path(gg_home) / "dirrpt" / f"{process_name.upper()}.rpt"
    result = {
        "excerpt": "",
        "error_code": "",
        "error_message": "",
        "failing_table": "",
    }

    if not report_path.exists():
        log.warning("Report file not found: %s", report_path)
        return result

    try:
        proc = subprocess.run(
            ["tail", "-80", str(report_path)],
            capture_output=True, text=True, timeout=5,
        )
        excerpt = proc.stdout
        result["excerpt"] = excerpt
    except Exception:
        return result

    # Extract the last OGG error code
    codes = RE_OGG_ERROR.findall(excerpt)
    if codes:
        result["error_code"] = codes[-1]

    # Find the error message line
    for line in reversed(excerpt.splitlines()):
        if result["error_code"] and result["error_code"] in line:
            result["error_message"] = line.strip()
            break
        elif "ERROR" in line.upper() or "ABEND" in line.upper():
            result["error_message"] = line.strip()
            break

    # Find the failing table
    table_matches = RE_TABLE_IN_ERROR.findall(excerpt)
    if table_matches:
        result["failing_table"] = table_matches[-1].upper()

    return result


# ---------------------------------------------------------------------------
# Notifier (reuses the pattern from event_watcher.py)
# ---------------------------------------------------------------------------

class RecoveryNotifier:
    """Send recovery notifications through configured channels."""

    def __init__(self, config: dict):
        self.cfg = config.get("monitoring", {}).get("alerting", {})
        self.log_file = self.cfg.get("log_file", "/var/log/gg-monitor/alerts.log")

    def notify(
        self,
        subject: str,
        message: str,
        severity: str = "critical",
    ) -> None:
        """Send notification through all enabled channels."""
        # Always log to file
        self._log_to_file(subject, message)

        # Slack
        if self.cfg.get("slack", {}).get("enabled"):
            self._notify_slack(subject, message, severity)

        # Email
        if self.cfg.get("email", {}).get("enabled"):
            self._notify_email(subject, message)

        # PagerDuty
        if self.cfg.get("pagerduty", {}).get("enabled"):
            self._notify_pagerduty(subject, message, severity)

        # Generic webhook
        if self.cfg.get("webhook", {}).get("enabled"):
            self._notify_webhook(subject, message)

    def _log_to_file(self, subject: str, message: str) -> None:
        try:
            log_dir = Path(self.log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            with open(self.log_file, "a") as f:
                f.write(f"\n{'='*60}\n")
                f.write(f"[{ts}] {subject}\n")
                f.write(message)
                f.write(f"\n{'='*60}\n")
        except Exception as e:
            log.error("Failed to write alert log: %s", e)

    def _notify_slack(self, subject: str, message: str, severity: str) -> None:
        slack_cfg = self.cfg["slack"]
        webhook_url = slack_cfg["webhook_url"]
        color = "#FF0000" if severity == "critical" else "#FFA500"
        payload = {
            "channel": slack_cfg.get("channel", "#gg-alerts"),
            "attachments": [{
                "color": color,
                "title": subject,
                "text": message[:3000],  # Slack limit
                "footer": "GG Auto-Recovery Daemon",
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
            log.info("Slack notification sent: %s", subject)
        except Exception as e:
            log.error("Slack notification failed: %s", e)

    def _notify_email(self, subject: str, body: str) -> None:
        email_cfg = self.cfg["email"]
        try:
            subprocess.run(
                ["mail", "-s", subject, "-r",
                 email_cfg.get("from", "gg-monitor@localhost"),
                 email_cfg["recipients"]],
                input=body, text=True, timeout=30,
                capture_output=True,
            )
            log.info("Email sent: %s", subject)
        except Exception as e:
            log.error("Email notification failed: %s", e)

    def _notify_pagerduty(self, subject: str, message: str, severity: str) -> None:
        pd_cfg = self.cfg["pagerduty"]
        payload = {
            "routing_key": pd_cfg["routing_key"],
            "event_action": "trigger",
            "payload": {
                "summary": subject,
                "severity": severity,
                "source": "gg-auto-recovery",
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
            log.info("PagerDuty alert: %s", subject)
        except Exception as e:
            log.error("PagerDuty notification failed: %s", e)

    def _notify_webhook(self, subject: str, message: str) -> None:
        wh_cfg = self.cfg["webhook"]
        try:
            import urllib.request
            payload = {"subject": subject, "message": message}
            req = urllib.request.Request(
                wh_cfg["url"],
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method=wh_cfg.get("method", "POST"),
            )
            urllib.request.urlopen(req, timeout=10)
            log.info("Webhook notification sent: %s", subject)
        except Exception as e:
            log.error("Webhook notification failed: %s", e)


# ---------------------------------------------------------------------------
# Auto-disable + regenerate helpers
# ---------------------------------------------------------------------------

def auto_disable_table(inventory_path: str, fqn: str, reason: str) -> bool:
    """Disable a failing table in the inventory Excel file."""
    try:
        from app.inventory import read_inventory, write_inventory, toggle_table
        tables = read_inventory(inventory_path)
        toggle_table(tables, fqn, enable=False, reason=reason)
        write_inventory(tables, inventory_path)
        log.info("AUTO-DISABLED %s in %s", fqn, inventory_path)
        return True
    except KeyError:
        log.warning("Table %s not found in inventory", fqn)
        return False
    except Exception as e:
        log.error("Auto-disable failed for %s: %s", fqn, e)
        return False


def auto_regenerate_configs(inventory_path: str) -> bool:
    """Regenerate all .prm files from the updated inventory."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "app.main", "generate",
             "--input", inventory_path],
            capture_output=True, text=True, timeout=60,
            cwd=str(PROJECT_ROOT),
        )
        if result.returncode == 0:
            log.info("Configs regenerated successfully.")
            return True
        else:
            log.error("Config regeneration failed:\n%s", result.stderr)
            return False
    except Exception as e:
        log.error("Auto-regenerate failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Retry tracker (per-process backoff state)
# ---------------------------------------------------------------------------

class RetryTracker:
    """
    Tracks retry attempts per process with exponential backoff.

    Default backoff sequence: 30s, 60s, 120s, 240s (exponential, base * 2^n).
    """

    def __init__(self, max_retries: int = 5, base_backoff: float = 30.0):
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self._attempts: Dict[str, int] = {}

    def get_attempt(self, process_name: str) -> int:
        """Get the current attempt count for a process (0 = first attempt)."""
        return self._attempts.get(process_name, 0)

    def increment(self, process_name: str) -> int:
        """Increment attempt count and return the new value."""
        current = self._attempts.get(process_name, 0) + 1
        self._attempts[process_name] = current
        return current

    def reset(self, process_name: str) -> None:
        """Reset attempt counter after a successful restart."""
        self._attempts.pop(process_name, None)

    def exceeded(self, process_name: str) -> bool:
        """Check whether the process has exceeded max retries."""
        return self._attempts.get(process_name, 0) >= self.max_retries

    def backoff_seconds(self, process_name: str) -> float:
        """Calculate backoff delay for the current attempt (exponential)."""
        attempt = self._attempts.get(process_name, 0)
        # Exponential: base * 2^attempt, capped at 240s
        delay = self.base_backoff * (2 ** attempt)
        return min(delay, 240.0)


# ---------------------------------------------------------------------------
# Log watcher thread
# ---------------------------------------------------------------------------

class GGLogWatcher(threading.Thread):
    """
    Watches a single ggserr.log file in a background thread.

    Calls the provided callback for each detected ABEND event with:
      callback(gg_home, process_name, line)
    """

    def __init__(
        self,
        gg_home: str,
        label: str,
        callback,
    ):
        super().__init__(daemon=True, name=f"watcher-{label}")
        self.gg_home = gg_home
        self.label = label
        self.callback = callback
        self.ggserr_path = Path(gg_home) / "ggserr.log"
        self._stop_event = threading.Event()

    def run(self) -> None:
        if not self.ggserr_path.exists():
            log.error("[%s] ggserr.log not found: %s", self.label, self.ggserr_path)
            return

        log.info("[%s] Watching: %s", self.label, self.ggserr_path)
        proc = subprocess.Popen(
            ["tail", "-F", str(self.ggserr_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )

        try:
            for line in proc.stdout:
                if self._stop_event.is_set():
                    break
                line = line.strip()
                if not line:
                    continue
                if RE_ABEND.search(line):
                    self.callback(self.gg_home, line)
        except Exception as e:
            log.error("[%s] Watcher error: %s", self.label, e)
        finally:
            proc.terminate()
            proc.wait()

    def stop(self) -> None:
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Recovery daemon
# ---------------------------------------------------------------------------

class RecoveryDaemon:
    """
    Main auto-recovery daemon. Watches one or more GG log files,
    classifies errors, and takes appropriate recovery action.
    """

    def __init__(
        self,
        config: dict,
        inventory_path: str,
        max_retries: int = 5,
        base_backoff: float = 30.0,
        auto_disable: bool = False,
        auto_regenerate: bool = False,
        dry_run: bool = False,
    ):
        self.config = config
        self.inventory_path = inventory_path
        self.auto_disable = auto_disable
        self.auto_regenerate = auto_regenerate
        self.dry_run = dry_run

        self.tracker = RetryTracker(max_retries=max_retries, base_backoff=base_backoff)
        self.notifier = RecoveryNotifier(config)
        self.recovery_log = RecoveryLogger()
        self.watchers: List[GGLogWatcher] = []

        # Lock to serialize recovery actions (avoid concurrent restarts)
        self._lock = threading.Lock()

    def add_watcher(self, gg_home: str, label: str) -> None:
        """Register a GG home directory to watch."""
        watcher = GGLogWatcher(gg_home, label, self._on_abend)
        self.watchers.append(watcher)

    def start(self) -> None:
        """Start all watchers and block until interrupted."""
        if not self.watchers:
            log.error("No watchers configured. Nothing to watch.")
            return

        log.info("=" * 60)
        log.info("GoldenGate Auto-Recovery Daemon started")
        log.info("  Watchers:        %d", len(self.watchers))
        log.info("  Max retries:     %d", self.tracker.max_retries)
        log.info("  Base backoff:    %.0fs", self.tracker.base_backoff)
        log.info("  Auto-disable:    %s", self.auto_disable)
        log.info("  Auto-regenerate: %s", self.auto_regenerate)
        log.info("  Dry-run:         %s", self.dry_run)
        log.info("  Recovery log:    %s", self.recovery_log.log_path)
        log.info("=" * 60)

        for w in self.watchers:
            w.start()
            log.info("  Started watcher: %s (%s)", w.label, w.gg_home)

        try:
            # Block main thread until interrupted
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Shutdown requested.")
            for w in self.watchers:
                w.stop()
            for w in self.watchers:
                w.join(timeout=5)
            log.info("All watchers stopped.")

    def _on_abend(self, gg_home: str, line: str) -> None:
        """
        Callback invoked when an ABEND is detected in a ggserr.log.

        Runs in the watcher thread but serialized via _lock.
        """
        with self._lock:
            self._handle_abend(gg_home, line)

    def _handle_abend(self, gg_home: str, line: str) -> None:
        """Core recovery logic: classify, decide action, execute."""
        # Extract process name
        m = RE_PROCESS_NAME.search(line)
        if not m:
            log.warning("ABEND detected but could not parse process name: %s", line)
            return

        process_name = m.group(1).upper()
        log.info("ABEND detected: process=%s, gg_home=%s", process_name, gg_home)

        # Parse report file for error details
        report = parse_report_file(gg_home, process_name)
        error_code = report["error_code"]
        error_message = report["error_message"]
        failing_table = report["failing_table"]

        # Build full error text for classification
        error_text = f"{line}\n{report['excerpt']}\n{error_code} {error_message}"

        # Classify the error
        error_class, description = classify_error(error_text)
        log.info(
            "  Classification: %s — %s",
            error_class.value, description,
        )
        log.info("  Error code:     %s", error_code or "(none)")
        log.info("  Failing table:  %s", failing_table or "(none)")

        # ---- TRANSIENT: auto-restart with backoff ----
        if error_class == ErrorClass.TRANSIENT:
            self._handle_transient(
                gg_home, process_name, description, error_code, failing_table,
            )

        # ---- PERMANENT: disable table + notify ----
        elif error_class == ErrorClass.PERMANENT:
            self._handle_permanent(
                gg_home, process_name, description, error_code,
                error_message, failing_table, report["excerpt"],
            )

        # ---- UNKNOWN: notify only ----
        else:
            self._handle_unknown(
                gg_home, process_name, description, error_code,
                error_message, failing_table, report["excerpt"],
            )

    # -----------------------------------------------------------------------
    # TRANSIENT recovery
    # -----------------------------------------------------------------------

    def _handle_transient(
        self,
        gg_home: str,
        process_name: str,
        description: str,
        error_code: str,
        failing_table: str,
    ) -> None:
        """Handle a transient error: backoff + restart, escalate if exhausted."""

        if self.tracker.exceeded(process_name):
            log.warning(
                "Max retries (%d) exceeded for %s — escalating to PERMANENT.",
                self.tracker.max_retries, process_name,
            )
            self.recovery_log.write(
                event_type="ESCALATED",
                process_name=process_name,
                error_class="TRANSIENT->PERMANENT",
                error_pattern=description,
                action_taken="escalated_to_permanent",
                attempt=self.tracker.get_attempt(process_name),
                max_retries=self.tracker.max_retries,
                failing_table=failing_table,
                gg_home=gg_home,
            )
            self._handle_permanent(
                gg_home, process_name,
                f"ESCALATED (retries exhausted): {description}",
                error_code, description, failing_table, "",
            )
            self.tracker.reset(process_name)
            return

        attempt = self.tracker.increment(process_name)
        backoff = self.tracker.backoff_seconds(process_name)

        log.info(
            "  TRANSIENT recovery: attempt %d/%d, backoff %.0fs",
            attempt, self.tracker.max_retries, backoff,
        )

        self.recovery_log.write(
            event_type="TRANSIENT_RETRY",
            process_name=process_name,
            error_class="TRANSIENT",
            error_pattern=description,
            action_taken=f"waiting_{backoff:.0f}s_then_restart",
            attempt=attempt,
            max_retries=self.tracker.max_retries,
            failing_table=failing_table,
            gg_home=gg_home,
        )

        if self.dry_run:
            log.info("  [DRY-RUN] Would wait %.0fs then restart %s", backoff, process_name)
            return

        # Wait with backoff
        log.info("  Waiting %.0fs before restart attempt...", backoff)
        time.sleep(backoff)

        # Attempt restart
        success, output = restart_process(gg_home, process_name)

        self.recovery_log.write(
            event_type="RESTART_ATTEMPT",
            process_name=process_name,
            error_class="TRANSIENT",
            error_pattern=description,
            action_taken="restart",
            attempt=attempt,
            max_retries=self.tracker.max_retries,
            failing_table=failing_table,
            gg_home=gg_home,
            success=success,
            details=output[:500],
        )

        if success:
            log.info("  Restart SUCCEEDED for %s (attempt %d)", process_name, attempt)
            self.tracker.reset(process_name)
        else:
            log.warning(
                "  Restart FAILED for %s (attempt %d): %s",
                process_name, attempt, output[:200],
            )

    # -----------------------------------------------------------------------
    # PERMANENT recovery
    # -----------------------------------------------------------------------

    def _handle_permanent(
        self,
        gg_home: str,
        process_name: str,
        description: str,
        error_code: str,
        error_message: str,
        failing_table: str,
        report_excerpt: str,
    ) -> None:
        """Handle a permanent error: disable table, regenerate, restart, notify."""

        log.warning("PERMANENT error for %s: %s", process_name, description)

        # Build notification message
        msg_lines = [
            f"Process:       {process_name}",
            f"Error Class:   PERMANENT",
            f"Error Code:    {error_code or 'N/A'}",
            f"Description:   {description}",
            f"Failing Table: {failing_table or 'N/A'}",
            f"GG Home:       {gg_home}",
        ]

        table_disabled = False
        configs_regenerated = False

        # Auto-disable the failing table
        if self.auto_disable and failing_table:
            if self.dry_run:
                log.info("  [DRY-RUN] Would disable %s in inventory", failing_table)
                msg_lines.append(f"Action:        [DRY-RUN] Would disable {failing_table}")
            else:
                reason = f"Auto-recovery: {error_code} {description}"
                table_disabled = auto_disable_table(
                    self.inventory_path, failing_table, reason,
                )
                if table_disabled:
                    msg_lines.append(f"Action:        DISABLED {failing_table} in inventory")
                else:
                    msg_lines.append(f"Action:        Failed to disable {failing_table}")

        # Auto-regenerate configs
        if self.auto_regenerate and table_disabled:
            if self.dry_run:
                log.info("  [DRY-RUN] Would regenerate configs")
                msg_lines.append(f"Action:        [DRY-RUN] Would regenerate configs")
            else:
                configs_regenerated = auto_regenerate_configs(self.inventory_path)
                if configs_regenerated:
                    msg_lines.append("Action:        Configs regenerated")
                else:
                    msg_lines.append("Action:        Config regeneration FAILED")

        # Auto-restart with failing table removed
        if configs_regenerated and not self.dry_run:
            log.info("  Restarting %s with updated configs...", process_name)
            success, output = restart_process(gg_home, process_name)
            if success:
                msg_lines.append(f"Action:        Restarted {process_name} (table removed)")
                log.info("  Restarted %s successfully", process_name)
            else:
                msg_lines.append(f"Action:        Restart FAILED: {output[:200]}")
                log.error("  Restart failed: %s", output[:200])

        # If we did not auto-handle, just advise
        if not self.auto_disable:
            msg_lines.append("Action:        NONE (--auto-disable not set)")
            msg_lines.append(f"Suggested:     ggctl disable -t {failing_table}")
            msg_lines.append("               ggctl generate && ggctl deploy all")

        if report_excerpt:
            msg_lines.append("")
            msg_lines.append("Report excerpt (last lines):")
            for rline in report_excerpt.splitlines()[-15:]:
                msg_lines.append(f"  {rline}")

        message = "\n".join(msg_lines)

        # Log and notify
        self.recovery_log.write(
            event_type="PERMANENT_ERROR",
            process_name=process_name,
            error_class="PERMANENT",
            error_pattern=description,
            action_taken="disabled_and_restarted" if table_disabled else "notified_only",
            failing_table=failing_table,
            gg_home=gg_home,
            success=table_disabled,
            details=error_message,
        )

        subject = f"GG PERMANENT ERROR: {process_name} — {error_code or description}"
        if not self.dry_run:
            self.notifier.notify(subject, message, severity="critical")
        else:
            log.info("[DRY-RUN] Would send notification:\n%s", message)

    # -----------------------------------------------------------------------
    # UNKNOWN recovery
    # -----------------------------------------------------------------------

    def _handle_unknown(
        self,
        gg_home: str,
        process_name: str,
        description: str,
        error_code: str,
        error_message: str,
        failing_table: str,
        report_excerpt: str,
    ) -> None:
        """Handle an unknown error: notify but do NOT auto-restart."""

        log.warning("UNKNOWN error for %s: %s", process_name, description)

        msg_lines = [
            f"Process:       {process_name}",
            f"Error Class:   UNKNOWN (not auto-restarting)",
            f"Error Code:    {error_code or 'N/A'}",
            f"Description:   {description}",
            f"Failing Table: {failing_table or 'N/A'}",
            f"GG Home:       {gg_home}",
            "",
            "This error is not in the known TRANSIENT or PERMANENT lists.",
            "Manual investigation is required. No automatic restart was attempted.",
        ]

        if report_excerpt:
            msg_lines.append("")
            msg_lines.append("Report excerpt (last lines):")
            for rline in report_excerpt.splitlines()[-15:]:
                msg_lines.append(f"  {rline}")

        message = "\n".join(msg_lines)

        self.recovery_log.write(
            event_type="UNKNOWN_ERROR",
            process_name=process_name,
            error_class="UNKNOWN",
            error_pattern=description,
            action_taken="notified_only",
            failing_table=failing_table,
            gg_home=gg_home,
            details=error_message,
        )

        subject = f"GG UNKNOWN ERROR: {process_name} — {error_code or 'unclassified'}"
        if not self.dry_run:
            self.notifier.notify(subject, message, severity="warning")
        else:
            log.info("[DRY-RUN] Would send notification:\n%s", message)


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    """Load pipeline.yaml configuration."""
    path = Path(config_path)
    if not path.exists():
        log.warning("Config not found: %s — using defaults", config_path)
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def get_gg_homes(config: dict) -> Tuple[str, str]:
    """Extract GG 19c and 21c home directories from pipeline.yaml."""
    gg19c_home = config.get("gg19c", {}).get(
        "home", "/u01/app/oracle/product/19c/oggcore_1"
    )
    gg21c_home = config.get("gg21c", {}).get(
        "home", "/u01/app/oracle/product/21c/oggbd_1"
    )
    return gg19c_home, gg21c_home


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Automated ABEND recovery daemon for GoldenGate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Watch mode (mutually exclusive)
    watch_group = parser.add_mutually_exclusive_group()
    watch_group.add_argument(
        "--watch-all", action="store_true", default=True,
        help="Watch both GG 19c and GG 21c (default)",
    )
    watch_group.add_argument(
        "--watch-19c-only", action="store_true",
        help="Watch only GG 19c (extract/pump)",
    )
    watch_group.add_argument(
        "--watch-21c-only", action="store_true",
        help="Watch only GG 21c (replicat)",
    )

    # Recovery options
    parser.add_argument(
        "--auto-disable", action="store_true",
        help="Auto-disable failing tables in the inventory on PERMANENT errors",
    )
    parser.add_argument(
        "--auto-regenerate", action="store_true",
        help="Auto-regenerate .prm files after disabling a table (implies --auto-disable)",
    )
    parser.add_argument(
        "--max-retries", type=int, default=5,
        help="Max restart attempts for TRANSIENT errors before escalating (default: 5)",
    )
    parser.add_argument(
        "--base-backoff", type=float, default=30.0,
        help="Base backoff in seconds for TRANSIENT retries (default: 30). "
             "Actual delay: base * 2^attempt, capped at 240s",
    )

    # Paths
    parser.add_argument(
        "--config", default=str(PROJECT_ROOT / "config" / "pipeline.yaml"),
        help="Path to pipeline.yaml",
    )
    parser.add_argument(
        "--inventory", default=str(PROJECT_ROOT / "input" / "table_inventory.xlsx"),
        help="Path to table inventory Excel file",
    )

    # Debug
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log actions but do not execute restarts, disables, or notifications",
    )

    args = parser.parse_args()

    # --auto-regenerate implies --auto-disable
    if args.auto_regenerate:
        args.auto_disable = True

    # Load configuration
    config = load_config(args.config)
    gg19c_home, gg21c_home = get_gg_homes(config)

    # Create the daemon
    daemon = RecoveryDaemon(
        config=config,
        inventory_path=args.inventory,
        max_retries=args.max_retries,
        base_backoff=args.base_backoff,
        auto_disable=args.auto_disable,
        auto_regenerate=args.auto_regenerate,
        dry_run=args.dry_run,
    )

    # Register watchers based on watch mode
    if args.watch_19c_only:
        daemon.add_watcher(gg19c_home, "GG-19c")
    elif args.watch_21c_only:
        daemon.add_watcher(gg21c_home, "GG-21c")
    else:
        # --watch-all (default)
        daemon.add_watcher(gg19c_home, "GG-19c")
        daemon.add_watcher(gg21c_home, "GG-21c")

    # Start the daemon (blocks until Ctrl+C)
    daemon.start()


if __name__ == "__main__":
    main()

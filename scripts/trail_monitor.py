#!/usr/bin/env python3
"""
Trail File Size Monitoring for Oracle GoldenGate Pipeline.

A leading indicator tool that alerts when trail files grow faster than
downstream processes can consume them, predicting lag before it becomes
critical.

Monitors trail file directories for both GG 19c (extract/pump) and
GG 21c (replicat), tracking growth and consumption rates, backlog,
and disk usage.

Usage:
    # Run once (for cron jobs)
    python3 scripts/trail_monitor.py --once

    # Continuous watch mode (refreshes every 30s)
    python3 scripts/trail_monitor.py --watch --interval 30

    # JSON output for integration
    python3 scripts/trail_monitor.py --once --json

    # Via ggctl
    ggctl trails [--once|--watch] [--interval N] [--json]

Reads configuration from config/pipeline.yaml.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import signal
import smtplib
import subprocess
import sys
import time
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_ROOT / "config" / "pipeline.yaml"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log = logging.getLogger("trail_monitor")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class TrailFile:
    """Represents a single GoldenGate trail file."""
    path: str
    name: str
    size_bytes: int
    mtime: float          # last-modified epoch time
    sequence: int          # parsed sequence number from filename

    @property
    def size_mb(self) -> float:
        return self.size_bytes / (1024 * 1024)

    @property
    def age_seconds(self) -> float:
        return time.time() - self.mtime


@dataclass
class TrailPrefix:
    """All trail files sharing a common 2-char prefix (e.g. e1, p2)."""
    prefix: str
    files: List[TrailFile] = field(default_factory=list)

    @property
    def file_count(self) -> int:
        return len(self.files)

    @property
    def total_size_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def total_size_mb(self) -> float:
        return self.total_size_bytes / (1024 * 1024)

    @property
    def total_size_gb(self) -> float:
        return self.total_size_bytes / (1024 ** 3)

    @property
    def newest_sequence(self) -> int:
        return max((f.sequence for f in self.files), default=0)

    @property
    def oldest_sequence(self) -> int:
        return min((f.sequence for f in self.files), default=0)

    @property
    def newest_mtime(self) -> float:
        return max((f.mtime for f in self.files), default=0)

    @property
    def oldest_mtime(self) -> float:
        return min((f.mtime for f in self.files), default=0)


@dataclass
class TrailDirectoryReport:
    """Report for a single trail directory (e.g. GG 19c dirdat)."""
    label: str                     # "GG 19c" or "GG 21c"
    directory: str
    exists: bool = True
    prefixes: List[TrailPrefix] = field(default_factory=list)

    # Disk usage
    disk_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    disk_free_gb: float = 0.0
    disk_usage_pct: float = 0.0

    # Aggregate trail stats
    total_trail_files: int = 0
    total_trail_size_mb: float = 0.0
    total_trail_size_gb: float = 0.0

    # Rates (calculated from snapshots)
    growth_rate_mb_min: float = 0.0
    consumption_rate_mb_min: float = 0.0

    # Predictions
    time_to_fill_disk_hours: Optional[float] = None
    time_to_catchup_hours: Optional[float] = None


@dataclass
class MonitorReport:
    """Complete monitoring report across all trail directories."""
    timestamp: str
    reports: List[TrailDirectoryReport] = field(default_factory=list)
    alerts: List[str] = field(default_factory=list)
    thresholds: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Trail file parser
# ---------------------------------------------------------------------------

# GoldenGate trail filenames: <2-char-prefix><9-digit-sequence>
# e.g. e1000000001, p2000000042
TRAIL_PATTERN = re.compile(r'^([a-zA-Z][a-zA-Z0-9])(\d{9})$')


def parse_trail_files(directory: str) -> List[TrailFile]:
    """Scan a directory for GoldenGate trail files and parse their names."""
    trail_dir = Path(directory)
    if not trail_dir.exists():
        return []

    trails: List[TrailFile] = []
    for entry in trail_dir.iterdir():
        if not entry.is_file():
            continue
        match = TRAIL_PATTERN.match(entry.name)
        if not match:
            continue
        try:
            stat = entry.stat()
            trails.append(TrailFile(
                path=str(entry),
                name=entry.name,
                size_bytes=stat.st_size,
                mtime=stat.st_mtime,
                sequence=int(match.group(2)),
            ))
        except OSError:
            continue

    return sorted(trails, key=lambda t: (t.name[:2], t.sequence))


def group_by_prefix(trails: List[TrailFile]) -> List[TrailPrefix]:
    """Group trail files by their 2-character prefix."""
    by_prefix: Dict[str, List[TrailFile]] = {}
    for t in trails:
        prefix = t.name[:2]
        by_prefix.setdefault(prefix, []).append(t)

    prefixes: List[TrailPrefix] = []
    for prefix in sorted(by_prefix):
        files = sorted(by_prefix[prefix], key=lambda f: f.sequence)
        prefixes.append(TrailPrefix(prefix=prefix, files=files))

    return prefixes


# ---------------------------------------------------------------------------
# Disk usage
# ---------------------------------------------------------------------------

def get_disk_usage(path: str) -> Tuple[float, float, float, float]:
    """
    Returns (total_gb, used_gb, free_gb, usage_pct) for the partition
    containing the given path.
    """
    try:
        usage = shutil.disk_usage(path)
        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        free_gb = usage.free / (1024 ** 3)
        usage_pct = (usage.used / usage.total * 100) if usage.total > 0 else 0
        return total_gb, used_gb, free_gb, usage_pct
    except OSError:
        return 0.0, 0.0, 0.0, 0.0


# ---------------------------------------------------------------------------
# Rate calculation (snapshot-based)
# ---------------------------------------------------------------------------

class RateTracker:
    """Tracks trail file sizes across snapshots to compute growth/consumption rates."""

    def __init__(self) -> None:
        # keyed by directory label -> {prefix -> (timestamp, total_bytes, newest_seq)}
        self._snapshots: Dict[str, Dict[str, Tuple[float, int, int]]] = {}

    def update(self, label: str, prefixes: List[TrailPrefix]) -> Tuple[float, float]:
        """
        Record current state and compute rates.

        Returns (growth_rate_mb_min, consumption_rate_mb_min).
        Growth rate: how fast total trail size is increasing.
        Consumption rate: estimated from trail files being deleted (sequence advancement).
        """
        now = time.time()
        current: Dict[str, Tuple[float, int, int]] = {}
        for p in prefixes:
            current[p.prefix] = (now, p.total_size_bytes, p.newest_sequence)

        prev = self._snapshots.get(label)
        self._snapshots[label] = current

        if not prev:
            return 0.0, 0.0

        total_growth_bytes = 0
        total_consumed_bytes = 0
        min_elapsed = float('inf')

        for prefix, (cur_time, cur_bytes, cur_seq) in current.items():
            if prefix in prev:
                prev_time, prev_bytes, prev_seq = prev[prefix]
                elapsed = cur_time - prev_time
                if elapsed > 0:
                    min_elapsed = min(min_elapsed, elapsed)
                    # Growth: new bytes added
                    if cur_bytes > prev_bytes:
                        total_growth_bytes += cur_bytes - prev_bytes
                    # Consumption: if oldest files were removed, estimate consumed bytes
                    # by the decrease in total size when new files are also being added
                    if cur_bytes < prev_bytes and cur_seq >= prev_seq:
                        total_consumed_bytes += prev_bytes - cur_bytes

        if min_elapsed == float('inf') or min_elapsed < 1:
            return 0.0, 0.0

        elapsed_min = min_elapsed / 60.0
        growth_rate = (total_growth_bytes / (1024 * 1024)) / elapsed_min if elapsed_min > 0 else 0
        consumption_rate = (total_consumed_bytes / (1024 * 1024)) / elapsed_min if elapsed_min > 0 else 0

        return growth_rate, consumption_rate


# ---------------------------------------------------------------------------
# Alerting (reuses pipeline.yaml alerting config)
# ---------------------------------------------------------------------------

def send_alerts(config: Dict[str, Any], alerts: List[str]) -> None:
    """Send alerts via configured channels (Slack, email, PagerDuty, webhook)."""
    if not alerts:
        return

    monitoring = config.get("monitoring", {})
    alerting = monitoring.get("alerting", {})
    message = "\n".join(alerts)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Always log
    log_file = alerting.get("log_file", "/var/log/gg-monitor/alerts.log")
    _log_alert(log_file, timestamp, message)

    # Slack
    slack = alerting.get("slack", {})
    if slack.get("enabled"):
        _send_slack(slack, timestamp, message)

    # Email
    email = alerting.get("email", {})
    if email.get("enabled"):
        _send_email(email, timestamp, message)

    # PagerDuty
    pd = alerting.get("pagerduty", {})
    if pd.get("enabled"):
        _send_pagerduty(pd, timestamp, message)

    # Generic webhook
    wh = alerting.get("webhook", {})
    if wh.get("enabled"):
        _send_webhook(wh, timestamp, message)


def _log_alert(log_file: str, timestamp: str, message: str) -> None:
    """Write alert to log file."""
    try:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        with open(log_file, "a") as f:
            f.write(f"[{timestamp}] TRAIL_MONITOR\n{message}\n{'='*60}\n")
    except OSError as e:
        log.warning("Could not write to alert log %s: %s", log_file, e)


def _send_slack(slack: Dict[str, Any], timestamp: str, message: str) -> None:
    """Send alert to Slack via incoming webhook."""
    try:
        payload = json.dumps({
            "channel": slack.get("channel", "#gg-alerts"),
            "username": "Trail Monitor",
            "icon_emoji": ":floppy_disk:",
            "text": f"*Trail Monitor Alert* ({timestamp})\n```\n{message}\n```",
        }).encode("utf-8")
        req = urllib.request.Request(
            slack["webhook_url"],
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        log.info("Slack alert sent")
    except Exception as e:
        log.warning("Slack alert failed: %s", e)


def _send_email(email_cfg: Dict[str, Any], timestamp: str, message: str) -> None:
    """Send alert via email using local sendmail/mailx."""
    try:
        subject = f"[Trail Monitor] Alert - {timestamp}"
        recipients = email_cfg.get("recipients", "")
        from_addr = email_cfg.get("from", "gg-monitor@localhost")

        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = recipients

        cmd = ["sendmail", "-t"]
        proc = subprocess.run(cmd, input=msg.as_string(), capture_output=True, text=True)
        if proc.returncode == 0:
            log.info("Email alert sent to %s", recipients)
        else:
            log.warning("Email alert failed: %s", proc.stderr)
    except Exception as e:
        log.warning("Email alert failed: %s", e)


def _send_pagerduty(pd: Dict[str, Any], timestamp: str, message: str) -> None:
    """Send alert via PagerDuty Events API v2."""
    try:
        payload = json.dumps({
            "routing_key": pd["routing_key"],
            "event_action": "trigger",
            "payload": {
                "summary": f"Trail Monitor: disk or growth rate threshold exceeded",
                "source": "trail_monitor",
                "severity": "warning",
                "timestamp": timestamp,
                "custom_details": {"message": message},
            },
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://events.pagerduty.com/v2/enqueue",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        log.info("PagerDuty alert sent")
    except Exception as e:
        log.warning("PagerDuty alert failed: %s", e)


def _send_webhook(wh: Dict[str, Any], timestamp: str, message: str) -> None:
    """Send alert via generic webhook."""
    try:
        payload = json.dumps({
            "source": "trail_monitor",
            "timestamp": timestamp,
            "message": message,
        }).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        # Parse extra headers from config
        extra = wh.get("headers", "")
        if extra:
            for part in extra.split(","):
                if ":" in part:
                    k, v = part.split(":", 1)
                    headers[k.strip()] = v.strip()

        req = urllib.request.Request(
            wh["url"],
            data=payload,
            headers=headers,
            method=wh.get("method", "POST"),
        )
        urllib.request.urlopen(req, timeout=10)
        log.info("Webhook alert sent")
    except Exception as e:
        log.warning("Webhook alert failed: %s", e)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_directory_report(
    label: str,
    gg_home: str,
    trail_dir_rel: str,
    rate_tracker: RateTracker,
) -> TrailDirectoryReport:
    """Build a monitoring report for a single trail directory."""

    # Resolve trail directory (relative to GG home or absolute)
    if trail_dir_rel.startswith("/"):
        trail_dir = trail_dir_rel
    elif trail_dir_rel.startswith("./"):
        trail_dir = os.path.join(gg_home, trail_dir_rel[2:])
    else:
        trail_dir = os.path.join(gg_home, trail_dir_rel)

    report = TrailDirectoryReport(label=label, directory=trail_dir)

    if not os.path.isdir(trail_dir):
        report.exists = False
        return report

    # Parse trail files
    trails = parse_trail_files(trail_dir)
    report.prefixes = group_by_prefix(trails)

    # Aggregate stats
    report.total_trail_files = sum(p.file_count for p in report.prefixes)
    report.total_trail_size_mb = sum(p.total_size_mb for p in report.prefixes)
    report.total_trail_size_gb = sum(p.total_size_gb for p in report.prefixes)

    # Disk usage
    total_gb, used_gb, free_gb, usage_pct = get_disk_usage(trail_dir)
    report.disk_total_gb = total_gb
    report.disk_used_gb = used_gb
    report.disk_free_gb = free_gb
    report.disk_usage_pct = usage_pct

    # Growth/consumption rates
    growth, consumption = rate_tracker.update(label, report.prefixes)
    report.growth_rate_mb_min = growth
    report.consumption_rate_mb_min = consumption

    # Time to fill disk (at current growth rate)
    if report.growth_rate_mb_min > 0 and report.disk_free_gb > 0:
        free_mb = report.disk_free_gb * 1024
        minutes_to_fill = free_mb / report.growth_rate_mb_min
        report.time_to_fill_disk_hours = minutes_to_fill / 60.0
    else:
        report.time_to_fill_disk_hours = None

    # Time for replicat to catch up
    net_growth = report.growth_rate_mb_min - report.consumption_rate_mb_min
    if net_growth > 0 and report.total_trail_size_mb > 0:
        # Backlog growing: no catchup possible at current rates
        report.time_to_catchup_hours = None
    elif report.consumption_rate_mb_min > report.growth_rate_mb_min and report.total_trail_size_mb > 0:
        net_drain = report.consumption_rate_mb_min - report.growth_rate_mb_min
        minutes_to_catchup = report.total_trail_size_mb / net_drain
        report.time_to_catchup_hours = minutes_to_catchup / 60.0
    else:
        report.time_to_catchup_hours = None

    return report


def build_full_report(config: Dict[str, Any], rate_tracker: RateTracker) -> MonitorReport:
    """Build a monitoring report across all configured trail directories."""

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    monitoring = config.get("monitoring", {})

    thresholds = {
        "trail_disk_warn_gb": monitoring.get("trail_disk_warn_gb", 50),
        "trail_growth_rate_warn_mb_min": monitoring.get("trail_growth_rate_warn_mb_min", 100),
        "disk_usage_warn_pct": monitoring.get("disk_usage_warn_pct", 80),
    }

    report = MonitorReport(timestamp=timestamp, thresholds=thresholds)

    # GG 19c trails
    gg19c = config.get("gg19c", {})
    gg19c_home = gg19c.get("home", "")
    gg19c_trail_dir = gg19c.get("trail", {}).get("dir", "./dirdat")
    if gg19c_home:
        dir_report = build_directory_report("GG 19c", gg19c_home, gg19c_trail_dir, rate_tracker)
        report.reports.append(dir_report)

    # GG 21c trails
    gg21c = config.get("gg21c", {})
    gg21c_home = gg21c.get("home", "")
    gg21c_trail_dir = gg21c.get("trail", {}).get("dir", "./dirdat")
    if gg21c_home:
        dir_report = build_directory_report("GG 21c", gg21c_home, gg21c_trail_dir, rate_tracker)
        report.reports.append(dir_report)

    # Check thresholds and generate alerts
    report.alerts = _check_thresholds(report, thresholds)

    return report


def _check_thresholds(report: MonitorReport, thresholds: Dict[str, Any]) -> List[str]:
    """Check thresholds and return list of alert messages."""
    alerts: List[str] = []

    trail_disk_warn_gb = thresholds.get("trail_disk_warn_gb", 50)
    growth_rate_warn = thresholds.get("trail_growth_rate_warn_mb_min", 100)
    disk_usage_warn = thresholds.get("disk_usage_warn_pct", 80)

    for dr in report.reports:
        if not dr.exists:
            alerts.append(f"WARN: {dr.label} trail directory does not exist: {dr.directory}")
            continue

        # Trail size exceeds threshold
        if dr.total_trail_size_gb > trail_disk_warn_gb:
            alerts.append(
                f"WARN: {dr.label} trail files total {dr.total_trail_size_gb:.1f} GB "
                f"(threshold: {trail_disk_warn_gb} GB)"
            )

        # Growth rate exceeds threshold
        if dr.growth_rate_mb_min > growth_rate_warn:
            alerts.append(
                f"WARN: {dr.label} trail growth rate {dr.growth_rate_mb_min:.1f} MB/min "
                f"(threshold: {growth_rate_warn} MB/min)"
            )

        # Disk usage exceeds threshold
        if dr.disk_usage_pct > disk_usage_warn:
            alerts.append(
                f"CRITICAL: {dr.label} disk usage {dr.disk_usage_pct:.1f}% "
                f"(threshold: {disk_usage_warn}%)"
            )

        # Time to fill disk is dangerously low (< 4 hours)
        if dr.time_to_fill_disk_hours is not None and dr.time_to_fill_disk_hours < 4.0:
            alerts.append(
                f"CRITICAL: {dr.label} disk will fill in {dr.time_to_fill_disk_hours:.1f} hours "
                f"at current growth rate ({dr.growth_rate_mb_min:.1f} MB/min)"
            )

    return alerts


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def format_text(report: MonitorReport) -> str:
    """Format the report as human-readable text for terminal display."""
    lines: List[str] = []

    lines.append("=" * 70)
    lines.append("  Trail File Monitor")
    lines.append(f"  {report.timestamp}")
    lines.append("=" * 70)

    for dr in report.reports:
        lines.append("")
        lines.append(f"  {dr.label}")
        lines.append(f"  Directory: {dr.directory}")
        lines.append("-" * 70)

        if not dr.exists:
            lines.append("  [Directory does not exist]")
            continue

        # Disk usage
        lines.append(f"  Disk: {dr.disk_used_gb:.1f} / {dr.disk_total_gb:.1f} GB "
                      f"({dr.disk_usage_pct:.1f}% used, {dr.disk_free_gb:.1f} GB free)")

        # Trail file summary
        lines.append(f"  Trail files: {dr.total_trail_files} files, "
                      f"{dr.total_trail_size_mb:.1f} MB ({dr.total_trail_size_gb:.2f} GB)")

        # Rates
        if dr.growth_rate_mb_min > 0 or dr.consumption_rate_mb_min > 0:
            lines.append(f"  Growth rate:      {dr.growth_rate_mb_min:>8.1f} MB/min")
            lines.append(f"  Consumption rate: {dr.consumption_rate_mb_min:>8.1f} MB/min")

            net = dr.growth_rate_mb_min - dr.consumption_rate_mb_min
            status = "GROWING" if net > 0 else "DRAINING" if net < 0 else "STABLE"
            lines.append(f"  Net rate:         {net:>8.1f} MB/min  [{status}]")

        # Predictions
        if dr.time_to_fill_disk_hours is not None:
            if dr.time_to_fill_disk_hours < 24:
                lines.append(f"  Time to fill disk:  {dr.time_to_fill_disk_hours:.1f} hours")
            else:
                lines.append(f"  Time to fill disk:  {dr.time_to_fill_disk_hours / 24:.1f} days")

        if dr.time_to_catchup_hours is not None:
            if dr.time_to_catchup_hours < 24:
                lines.append(f"  Est. catchup time:  {dr.time_to_catchup_hours:.1f} hours")
            else:
                lines.append(f"  Est. catchup time:  {dr.time_to_catchup_hours / 24:.1f} days")

        # Per-prefix breakdown
        if dr.prefixes:
            lines.append("")
            lines.append(f"  {'Prefix':<8} {'Files':>6} {'Size MB':>10} {'Newest Seq':>12} "
                          f"{'Oldest Seq':>12} {'Backlog':>8}")
            lines.append(f"  {'-'*8} {'-'*6} {'-'*10} {'-'*12} {'-'*12} {'-'*8}")
            for p in dr.prefixes:
                backlog = p.newest_sequence - p.oldest_sequence
                lines.append(
                    f"  {p.prefix + '*':<8} {p.file_count:>6} {p.total_size_mb:>10.1f} "
                    f"{p.newest_sequence:>12} {p.oldest_sequence:>12} {backlog:>8}"
                )

    # Thresholds
    lines.append("")
    lines.append("-" * 70)
    lines.append("  Thresholds:")
    lines.append(f"    Trail disk warn:       {report.thresholds.get('trail_disk_warn_gb', 50)} GB")
    lines.append(f"    Growth rate warn:      {report.thresholds.get('trail_growth_rate_warn_mb_min', 100)} MB/min")
    lines.append(f"    Disk usage warn:       {report.thresholds.get('disk_usage_warn_pct', 80)}%")

    # Alerts
    if report.alerts:
        lines.append("")
        lines.append("  ALERTS:")
        for alert in report.alerts:
            lines.append(f"    >> {alert}")
    else:
        lines.append("")
        lines.append("  Status: OK (no thresholds exceeded)")

    lines.append("=" * 70)
    return "\n".join(lines)


def format_json(report: MonitorReport) -> str:
    """Format the report as JSON for integration with other tools."""
    data: Dict[str, Any] = {
        "timestamp": report.timestamp,
        "thresholds": report.thresholds,
        "alerts": report.alerts,
        "directories": [],
    }

    for dr in report.reports:
        dir_data: Dict[str, Any] = {
            "label": dr.label,
            "directory": dr.directory,
            "exists": dr.exists,
            "disk": {
                "total_gb": round(dr.disk_total_gb, 2),
                "used_gb": round(dr.disk_used_gb, 2),
                "free_gb": round(dr.disk_free_gb, 2),
                "usage_pct": round(dr.disk_usage_pct, 1),
            },
            "trails": {
                "file_count": dr.total_trail_files,
                "total_size_mb": round(dr.total_trail_size_mb, 2),
                "total_size_gb": round(dr.total_trail_size_gb, 4),
            },
            "rates": {
                "growth_mb_min": round(dr.growth_rate_mb_min, 2),
                "consumption_mb_min": round(dr.consumption_rate_mb_min, 2),
                "net_mb_min": round(dr.growth_rate_mb_min - dr.consumption_rate_mb_min, 2),
            },
            "predictions": {
                "time_to_fill_disk_hours": (
                    round(dr.time_to_fill_disk_hours, 2) if dr.time_to_fill_disk_hours else None
                ),
                "time_to_catchup_hours": (
                    round(dr.time_to_catchup_hours, 2) if dr.time_to_catchup_hours else None
                ),
            },
            "prefixes": [],
        }

        for p in dr.prefixes:
            dir_data["prefixes"].append({
                "prefix": p.prefix,
                "file_count": p.file_count,
                "total_size_mb": round(p.total_size_mb, 2),
                "newest_sequence": p.newest_sequence,
                "oldest_sequence": p.oldest_sequence,
                "backlog": p.newest_sequence - p.oldest_sequence,
            })

        data["directories"].append(dir_data)

    return json.dumps(data, indent=2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_config() -> Dict[str, Any]:
    """Load pipeline.yaml configuration."""
    config_path = CONFIG_FILE
    # Allow override via environment variable
    env_config = os.environ.get("PIPELINE_CONFIG")
    if env_config:
        config_path = Path(env_config)

    if not config_path.exists():
        log.error("Config file not found: %s", config_path)
        sys.exit(1)

    with open(config_path) as f:
        return yaml.safe_load(f)


def run_once(config: Dict[str, Any], rate_tracker: RateTracker, as_json: bool = False) -> MonitorReport:
    """Run a single monitoring pass."""
    report = build_full_report(config, rate_tracker)

    if as_json:
        print(format_json(report))
    else:
        print(format_text(report))

    # Send alerts if any thresholds exceeded
    if report.alerts:
        send_alerts(config, report.alerts)

    return report


def run_watch(config: Dict[str, Any], interval: int = 30, as_json: bool = False) -> None:
    """Run continuous monitoring with periodic refresh."""
    rate_tracker = RateTracker()

    # Handle graceful shutdown
    running = True

    def signal_handler(sig, frame):
        nonlocal running
        running = False
        print("\nStopping trail monitor...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"Trail monitor starting (refresh every {interval}s, Ctrl+C to stop)")
    print()

    while running:
        # Clear screen for watch mode (not in JSON mode)
        if not as_json:
            os.system('clear' if os.name != 'nt' else 'cls')

        run_once(config, rate_tracker, as_json=as_json)

        # Wait for next interval, checking for shutdown
        for _ in range(interval):
            if not running:
                break
            time.sleep(1)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Trail File Size Monitor for Oracle GoldenGate Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --once                    Run once and exit (for cron)
  %(prog)s --watch --interval 30     Continuous monitoring every 30s
  %(prog)s --once --json             JSON output for integration
  %(prog)s --watch --json            Continuous JSON output
        """,
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (suitable for cron jobs)",
    )
    mode.add_argument(
        "--watch",
        action="store_true",
        help="Continuous monitoring with periodic refresh",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        metavar="SECONDS",
        help="Refresh interval for watch mode (default: 30)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON (for integration with other tools)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline.yaml (default: config/pipeline.yaml)",
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

    # Logging setup
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Config override
    if args.config:
        os.environ["PIPELINE_CONFIG"] = args.config

    config = load_config()
    rate_tracker = RateTracker()

    if args.once:
        report = run_once(config, rate_tracker, as_json=args.json)
        # Exit with non-zero if alerts fired
        if report.alerts:
            sys.exit(1)
    elif args.watch:
        run_watch(config, interval=args.interval, as_json=args.json)


if __name__ == "__main__":
    main()

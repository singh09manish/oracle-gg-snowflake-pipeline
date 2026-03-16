#!/usr/bin/env bash
# =================================================================
# GoldenGate Pipeline Monitor with Alerting
#
# Checks all GG processes for:
#   1. ABENDED / STOPPED processes
#   2. Replication lag exceeding thresholds
#   3. Trail disk usage exceeding thresholds
#   4. GG error log (ggserr.log) for recent errors
#   5. Java handler errors (replicat/Snowflake side)
#
# Sends alerts via: Email, Slack, PagerDuty, Webhook, or log file.
#
# Designed for cron (every 5 min):
#   */5 * * * *  /path/to/scripts/monitor.sh >> /var/log/gg-monitor/monitor.log 2>&1
#
# Env vars:
#   GG19C_HOME, GG21C_HOME     — GG installation directories
#   LAG_WARN_SEC                — lag warning threshold (default: 300s)
#   LAG_CRITICAL_SEC            — lag critical threshold (default: 1800s)
#   TRAIL_DISK_WARN_GB          — disk usage warning (default: 50GB)
#   ALERT_EMAIL_ENABLED         — "true" to send email
#   ALERT_EMAIL_TO              — recipient email(s)
#   ALERT_SLACK_ENABLED         — "true" to send Slack
#   ALERT_SLACK_WEBHOOK         — Slack incoming webhook URL
#   ALERT_PAGERDUTY_ENABLED     — "true" to send PagerDuty
#   ALERT_PAGERDUTY_KEY         — PagerDuty routing key
#   ALERT_WEBHOOK_ENABLED       — "true" to send generic webhook
#   ALERT_WEBHOOK_URL           — webhook URL
#   ALERT_LOG_FILE              — log file for all alerts (default: /var/log/gg-monitor/alerts.log)
#
# Usage:
#   ./scripts/monitor.sh [--quiet]  (--quiet suppresses stdout, only alerts)
# =================================================================

set -euo pipefail

# --- Configuration ---
GG19C_HOME="${GG19C_HOME:-/u01/app/oracle/product/19c/oggcore_1}"
GG21C_HOME="${GG21C_HOME:-/u01/app/oracle/product/21c/oggbd_1}"
LAG_WARN_SEC="${LAG_WARN_SEC:-300}"
LAG_CRITICAL_SEC="${LAG_CRITICAL_SEC:-1800}"
TRAIL_DISK_WARN_GB="${TRAIL_DISK_WARN_GB:-50}"

# Alerting channels
ALERT_EMAIL_ENABLED="${ALERT_EMAIL_ENABLED:-false}"
ALERT_EMAIL_TO="${ALERT_EMAIL_TO:-}"
ALERT_EMAIL_FROM="${ALERT_EMAIL_FROM:-gg-monitor@$(hostname)}"
ALERT_SLACK_ENABLED="${ALERT_SLACK_ENABLED:-false}"
ALERT_SLACK_WEBHOOK="${ALERT_SLACK_WEBHOOK:-}"
ALERT_PAGERDUTY_ENABLED="${ALERT_PAGERDUTY_ENABLED:-false}"
ALERT_PAGERDUTY_KEY="${ALERT_PAGERDUTY_KEY:-}"
ALERT_WEBHOOK_ENABLED="${ALERT_WEBHOOK_ENABLED:-false}"
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_LOG_FILE="${ALERT_LOG_FILE:-/var/log/gg-monitor/alerts.log}"

QUIET=0
for arg in "$@"; do [[ "$arg" == "--quiet" ]] && QUIET=1; done

HOSTNAME=$(hostname -s 2>/dev/null || hostname)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Collect all issues
declare -a ISSUES=()
declare -a DETAILS=()
SEVERITY="ok"  # ok, warn, critical

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { [[ "$QUIET" -eq 0 ]] && echo "[$(ts)] [MONITOR] $*" || true; }

add_issue() {
    local sev="$1" msg="$2"
    ISSUES+=("[$sev] $msg")
    # Escalate severity
    if [[ "$sev" == "CRITICAL" ]]; then
        SEVERITY="critical"
    elif [[ "$sev" == "WARN" && "$SEVERITY" != "critical" ]]; then
        SEVERITY="warn"
    fi
}

# =================================================================
# CHECK 1: Process Status (RUNNING / ABENDED / STOPPED)
# =================================================================
check_process() {
    local ggsci="$1" type="$2" name="$3"
    if [[ ! -x "$ggsci" ]]; then
        add_issue "WARN" "$name: ggsci not found at $ggsci"
        return
    fi
    local out
    out=$("$ggsci" -e "info ${type} ${name}" 2>/dev/null || echo "CONNECT_FAILED")

    if echo "$out" | grep -q "RUNNING"; then
        log "  OK   $name RUNNING"
    elif echo "$out" | grep -q "ABENDED"; then
        add_issue "CRITICAL" "$name ABENDED — requires immediate attention"
        # Grab last 5 lines from report for context
        local rpt="$(dirname "$ggsci")/../dirrpt/${name}.rpt"
        if [[ -f "$rpt" ]]; then
            local ctx
            ctx=$(tail -10 "$rpt" | grep -i "error\|abend\|OGG-" | head -5)
            [[ -n "$ctx" ]] && DETAILS+=("$name report: $ctx")
        fi
    elif echo "$out" | grep -q "STOPPED"; then
        add_issue "CRITICAL" "$name STOPPED — not processing data"
    elif echo "$out" | grep -q "CONNECT_FAILED"; then
        add_issue "WARN" "$name: cannot connect to GG manager"
    else
        add_issue "WARN" "$name status UNKNOWN"
    fi
}

# =================================================================
# CHECK 2: Replication Lag
# =================================================================
check_lag() {
    local ggsci="$1" type="$2" label="$3"
    if [[ ! -x "$ggsci" ]]; then return; fi

    local out
    out=$("$ggsci" -e "lag ${type} *" 2>/dev/null || true)

    echo "$out" | grep -oP '(\w+)\s+.*?(\d+):(\d+):(\d+)' | while IFS= read -r line; do
        local proc_name lag_h lag_m lag_s lag_total
        proc_name=$(echo "$line" | awk '{print $1}')
        lag_h=$(echo "$line" | grep -oP '\d+:\d+:\d+' | head -1 | cut -d: -f1)
        lag_m=$(echo "$line" | grep -oP '\d+:\d+:\d+' | head -1 | cut -d: -f2)
        lag_s=$(echo "$line" | grep -oP '\d+:\d+:\d+' | head -1 | cut -d: -f3)
        lag_total=$(( ${lag_h:-0}*3600 + ${lag_m:-0}*60 + ${lag_s:-0} ))

        if [[ "$lag_total" -gt "$LAG_CRITICAL_SEC" ]]; then
            add_issue "CRITICAL" "$label $proc_name lag ${lag_h}:${lag_m}:${lag_s} (>${LAG_CRITICAL_SEC}s threshold)"
        elif [[ "$lag_total" -gt "$LAG_WARN_SEC" ]]; then
            add_issue "WARN" "$label $proc_name lag ${lag_h}:${lag_m}:${lag_s} (>${LAG_WARN_SEC}s threshold)"
        else
            log "  OK   $label $proc_name lag ${lag_h}:${lag_m}:${lag_s}"
        fi
    done
}

# =================================================================
# CHECK 3: Trail Disk Usage
# =================================================================
check_disk() {
    local dir="$1" label="$2"
    if [[ ! -d "$dir" ]]; then return; fi

    local usage_mb usage_gb count
    usage_mb=$(du -sm "$dir" 2>/dev/null | awk '{print $1}')
    usage_gb=$((usage_mb / 1024))
    count=$(find "$dir" -maxdepth 1 -type f 2>/dev/null | wc -l | tr -d ' ')

    log "  DISK $label: ${usage_gb}GB (${count} files)"

    if [[ "$usage_gb" -gt "$TRAIL_DISK_WARN_GB" ]]; then
        add_issue "WARN" "$label trail disk ${usage_gb}GB exceeds ${TRAIL_DISK_WARN_GB}GB threshold ($count files)"
    fi
}

# =================================================================
# CHECK 4: Recent errors in ggserr.log
# =================================================================
check_error_log() {
    local gg_home="$1" label="$2"
    local errlog="$gg_home/ggserr.log"
    if [[ ! -f "$errlog" ]]; then return; fi

    # Check for errors in last 10 minutes
    local recent_errors
    recent_errors=$(find "$errlog" -mmin -10 -exec grep -c "ERROR\|ABEND\|FATAL" {} \; 2>/dev/null || echo "0")

    if [[ "${recent_errors:-0}" -gt 0 ]]; then
        local last_err
        last_err=$(tail -50 "$errlog" | grep -i "ERROR\|ABEND\|FATAL" | tail -3)
        add_issue "WARN" "$label ggserr.log has $recent_errors recent errors"
        [[ -n "$last_err" ]] && DETAILS+=("$label ggserr.log: $last_err")
    fi
}

# =================================================================
# CHECK 5: Java handler errors (GG 21c replicat side)
# =================================================================
check_java_errors() {
    local gg_home="$1"
    local dirrpt="$gg_home/dirrpt"
    if [[ ! -d "$dirrpt" ]]; then return; fi

    # Check all *_javaue.log files for recent errors
    for jlog in "$dirrpt"/*_javaue.log; do
        [[ -f "$jlog" ]] || continue
        local basename_j
        basename_j=$(basename "$jlog" _javaue.log)

        # Only check if modified in last 10 minutes
        if [[ $(find "$jlog" -mmin -10 2>/dev/null | wc -l) -gt 0 ]]; then
            local java_errors
            java_errors=$(tail -100 "$jlog" | grep -c "Exception\|SQLException\|ERROR\|FATAL" 2>/dev/null || echo "0")
            if [[ "${java_errors:-0}" -gt 0 ]]; then
                local last_java_err
                last_java_err=$(tail -100 "$jlog" | grep -i "Exception\|SQLException\|ERROR" | tail -2)
                add_issue "WARN" "$basename_j Java handler has $java_errors recent errors"
                [[ -n "$last_java_err" ]] && DETAILS+=("$basename_j java: $last_java_err")
            fi
        fi
    done
}

# =================================================================
# ALERTING FUNCTIONS
# =================================================================

send_alert_email() {
    local subject="$1" body="$2"
    if [[ "$ALERT_EMAIL_ENABLED" != "true" || -z "$ALERT_EMAIL_TO" ]]; then return; fi

    echo "$body" | mail -s "$subject" -r "$ALERT_EMAIL_FROM" "$ALERT_EMAIL_TO" 2>/dev/null || \
    echo "$body" | mailx -s "$subject" "$ALERT_EMAIL_TO" 2>/dev/null || \
    log "WARN: Failed to send email alert"
}

send_alert_slack() {
    local subject="$1" body="$2"
    if [[ "$ALERT_SLACK_ENABLED" != "true" || -z "$ALERT_SLACK_WEBHOOK" ]]; then return; fi

    local color="warning"
    [[ "$SEVERITY" == "critical" ]] && color="danger"

    local payload
    payload=$(cat <<EOSLACK
{
  "attachments": [{
    "color": "$color",
    "title": "$subject",
    "text": $(echo "$body" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo "\"$body\""),
    "footer": "GG Monitor | $HOSTNAME",
    "ts": $(date +%s)
  }]
}
EOSLACK
)
    curl -s -X POST -H 'Content-Type: application/json' \
        -d "$payload" "$ALERT_SLACK_WEBHOOK" >/dev/null 2>&1 || \
    log "WARN: Failed to send Slack alert"
}

send_alert_pagerduty() {
    local subject="$1" body="$2"
    if [[ "$ALERT_PAGERDUTY_ENABLED" != "true" || -z "$ALERT_PAGERDUTY_KEY" ]]; then return; fi

    local pd_severity="warning"
    [[ "$SEVERITY" == "critical" ]] && pd_severity="critical"

    local payload
    payload=$(cat <<EOPD
{
  "routing_key": "$ALERT_PAGERDUTY_KEY",
  "event_action": "trigger",
  "payload": {
    "summary": "$subject",
    "source": "$HOSTNAME",
    "severity": "$pd_severity",
    "custom_details": {
      "body": $(echo "$body" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo "\"$body\"")
    }
  }
}
EOPD
)
    curl -s -X POST -H 'Content-Type: application/json' \
        -d "$payload" "https://events.pagerduty.com/v2/enqueue" >/dev/null 2>&1 || \
    log "WARN: Failed to send PagerDuty alert"
}

send_alert_webhook() {
    local subject="$1" body="$2"
    if [[ "$ALERT_WEBHOOK_ENABLED" != "true" || -z "$ALERT_WEBHOOK_URL" ]]; then return; fi

    local payload
    payload=$(cat <<EOWH
{
  "severity": "$SEVERITY",
  "host": "$HOSTNAME",
  "title": "$subject",
  "message": $(echo "$body" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo "\"$body\""),
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOWH
)
    curl -s -X POST -H 'Content-Type: application/json' \
        -d "$payload" "$ALERT_WEBHOOK_URL" >/dev/null 2>&1 || \
    log "WARN: Failed to send webhook alert"
}

send_alert_log() {
    local subject="$1" body="$2"
    local log_dir
    log_dir=$(dirname "$ALERT_LOG_FILE")
    mkdir -p "$log_dir" 2>/dev/null || true

    cat >> "$ALERT_LOG_FILE" <<EOF
========================================
$(ts) | $SEVERITY | $HOSTNAME
$subject
----------------------------------------
$body
========================================

EOF
}

send_alerts() {
    local subject="$1" body="$2"

    # Always log
    send_alert_log "$subject" "$body"

    # Conditional channels
    send_alert_email "$subject" "$body"
    send_alert_slack "$subject" "$body"
    send_alert_pagerduty "$subject" "$body"
    send_alert_webhook "$subject" "$body"
}

# =================================================================
# MAIN
# =================================================================

log "=== GG Pipeline Monitor Started ==="

# Auto-detect process names from generated output
EXTRACTS=$(ls "$PROJECT_ROOT/output/gg19c/dirprm/EXT"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)
PUMPS=$(ls "$PROJECT_ROOT/output/gg19c/dirprm/PMP"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)
REPLICATS=$(ls "$PROJECT_ROOT/output/gg21c/dirprm/REP"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)

# --- Check 1: Process status ---
log "── Process Status ──"
for e in $EXTRACTS; do check_process "$GG19C_HOME/ggsci" "extract" "$e"; done
for p in $PUMPS; do check_process "$GG19C_HOME/ggsci" "extract" "$p"; done
for r in $REPLICATS; do check_process "$GG21C_HOME/ggsci" "replicat" "$r"; done

# --- Check 2: Lag ---
log "── Lag Check ──"
check_lag "$GG19C_HOME/ggsci" "extract" "Extract"
check_lag "$GG21C_HOME/ggsci" "replicat" "Replicat"

# --- Check 3: Disk ---
log "── Trail Disk ──"
check_disk "$GG19C_HOME/dirdat" "GG19c"
check_disk "$GG21C_HOME/dirdat" "GG21c"

# --- Check 4: GG error logs ---
log "── Error Logs ──"
check_error_log "$GG19C_HOME" "GG19c"
check_error_log "$GG21C_HOME" "GG21c"

# --- Check 5: Java handler errors ---
log "── Java Handler ──"
check_java_errors "$GG21C_HOME"

# =================================================================
# ALERT if issues found
# =================================================================
if [[ ${#ISSUES[@]} -gt 0 ]]; then
    SUBJECT="[GG-${SEVERITY^^}] ${#ISSUES[@]} issue(s) on $HOSTNAME"

    BODY="GoldenGate Pipeline Monitor — $(ts)
Host: $HOSTNAME

ISSUES:
$(printf '  • %s\n' "${ISSUES[@]}")
"

    if [[ ${#DETAILS[@]} -gt 0 ]]; then
        BODY+="
DETAILS:
$(printf '  %s\n' "${DETAILS[@]}")
"
    fi

    BODY+="
---
GG 19c Home: $GG19C_HOME
GG 21c Home: $GG21C_HOME
Thresholds: lag_warn=${LAG_WARN_SEC}s lag_critical=${LAG_CRITICAL_SEC}s disk_warn=${TRAIL_DISK_WARN_GB}GB"

    log ""
    log "ALERT: $SUBJECT"
    send_alerts "$SUBJECT" "$BODY"
    log "=== Monitor Complete: ${#ISSUES[@]} issues ==="
    exit 1
else
    log ""
    log "=== Monitor Complete: ALL OK ==="
    exit 0
fi

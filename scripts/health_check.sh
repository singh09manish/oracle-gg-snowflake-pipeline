#!/usr/bin/env bash
# Unified health check for all GG processes + trail disk usage.
# Designed for cron (every 5 min). Exits non-zero on any issue.
#
# Usage:
#   ./scripts/health_check.sh
#
# Env vars (all have defaults from pipeline.yaml paths):
#   GG19C_HOME  — GG 19c installation directory
#   GG21C_HOME  — GG 21c BigData installation directory

set -euo pipefail

GG19C_HOME="${GG19C_HOME:-/u01/app/oracle/product/19c/oggcore_1}"
GG21C_HOME="${GG21C_HOME:-/u01/app/oracle/product/21c/oggbd_1}"
TRAIL_WARN_GB="${TRAIL_WARN_GB:-50}"
fail=0

ts() { date '+%Y-%m-%d %H:%M:%S'; }

echo "━━━ GG Pipeline Health Check  $(ts) ━━━"

# --- Discover process names from generated output ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Auto-detect extract/pump/replicat names from generated .prm files
EXTRACTS=$(ls "$PROJECT_ROOT/output/gg19c/dirprm/EXT"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)
PUMPS=$(ls "$PROJECT_ROOT/output/gg19c/dirprm/PMP"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)
REPLICATS=$(ls "$PROJECT_ROOT/output/gg21c/dirprm/REP"*.prm 2>/dev/null | xargs -I{} basename {} .prm || true)

check() {
    local ggsci="$1" type="$2" name="$3"
    local out
    out=$("$ggsci" -e "info ${type} ${name}" 2>/dev/null || true)
    if echo "$out" | grep -q "RUNNING"; then
        printf "  ✓  %-10s RUNNING\n" "$name"
    elif echo "$out" | grep -q "STOPPED"; then
        printf "  ✗  %-10s STOPPED\n" "$name" >&2; fail=1
    elif echo "$out" | grep -q "ABENDED"; then
        printf "  ✗  %-10s ABENDED\n" "$name" >&2; fail=1
    else
        printf "  ?  %-10s UNKNOWN\n" "$name" >&2; fail=1
    fi
}

check_lag() {
    local ggsci="$1" type="$2"
    local out
    out=$("$ggsci" -e "lag ${type} *" 2>/dev/null || true)
    echo "$out" | while IFS= read -r line; do
        if echo "$line" | grep -qP '\d+:\d+:\d+'; then
            # Parse lag time (HH:MM:SS)
            lag_seconds=$(echo "$line" | grep -oP '\d+:\d+:\d+' | head -1 | awk -F: '{print $1*3600+$2*60+$3}')
            if [ "${lag_seconds:-0}" -gt 300 ]; then
                echo "  ⚠  LAG WARNING: $line" >&2; fail=1
            fi
        fi
    done
    echo "$out" | grep -i "lag\|at\|second\|minute\|hour" || true
}

echo ""
echo "── GG 19c Extracts ($GG19C_HOME) ──"
for e in $EXTRACTS; do check "$GG19C_HOME/ggsci" "extract" "$e"; done

echo ""
echo "── GG 19c Pumps ──"
for p in $PUMPS; do check "$GG19C_HOME/ggsci" "extract" "$p"; done

echo ""
echo "── GG 21c Replicats ($GG21C_HOME) ──"
for r in $REPLICATS; do check "$GG21C_HOME/ggsci" "replicat" "$r"; done

echo ""
echo "── Lag Status ──"
check_lag "$GG19C_HOME/ggsci" "extract"
check_lag "$GG21C_HOME/ggsci" "replicat"

echo ""
echo "── Trail Disk Usage ──"
for dir in "$GG19C_HOME/dirdat" "$GG21C_HOME/dirdat"; do
    if [ -d "$dir" ]; then
        usage_mb=$(du -sm "$dir" 2>/dev/null | awk '{print $1}')
        usage_gb=$((usage_mb / 1024))
        count=$(ls "$dir" 2>/dev/null | wc -l | tr -d ' ')
        printf "  %-40s %5d MB (%d GB)  %d files\n" "$dir" "$usage_mb" "$usage_gb" "$count"
        if [ "$usage_gb" -gt "$TRAIL_WARN_GB" ]; then
            echo "  ⚠  TRAIL DISK WARNING: $dir exceeds ${TRAIL_WARN_GB} GB" >&2
            fail=1
        fi
    fi
done

echo ""
if [ "$fail" -eq 0 ]; then
    echo "Result: ALL OK"
else
    echo "Result: ISSUES DETECTED — investigate above" >&2
fi

exit $fail

#!/usr/bin/env bash
# =============================================================================
# diagnose_lag.sh — GoldenGate Pipeline Lag Diagnostic
# =============================================================================
#
# Checks the entire data path: Extract -> Pump -> Replicat
# to pinpoint where data flow is stuck.
#
# Environment variables:
#   GG19C_HOME  — Path to GG 19c installation (extract/pump)
#   GG21C_HOME  — Path to GG 21c BigData installation (replicat)
#
# These default to values from config/pipeline.yaml if not set.
#
# Usage:
#   GG19C_HOME=/path/to/19c GG21C_HOME=/path/to/21c bash scripts/diagnose_lag.sh
#   ggctl diagnose
# =============================================================================

set -uo pipefail

# ---------------------------------------------------------------------------
# Resolve GG homes from pipeline.yaml if not provided
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_HOME="${PIPELINE_HOME:-$(dirname "$SCRIPT_DIR")}"
CONFIG_FILE="$PIPELINE_HOME/config/pipeline.yaml"

if [ -z "${GG19C_HOME:-}" ] || [ -z "${GG21C_HOME:-}" ]; then
    if command -v python3 &>/dev/null && [ -f "$CONFIG_FILE" ]; then
        if [ -z "${GG19C_HOME:-}" ]; then
            GG19C_HOME=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg19c', {}).get('home', '/u01/app/oracle/product/19c/oggcore_1'))
" 2>/dev/null || echo "/u01/app/oracle/product/19c/oggcore_1")
        fi
        if [ -z "${GG21C_HOME:-}" ]; then
            GG21C_HOME=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg21c', {}).get('home', '/u01/app/oracle/product/21c/oggbd_1'))
" 2>/dev/null || echo "/u01/app/oracle/product/21c/oggbd_1")
        fi
    else
        GG19C_HOME="${GG19C_HOME:-/u01/app/oracle/product/19c/oggcore_1}"
        GG21C_HOME="${GG21C_HOME:-/u01/app/oracle/product/21c/oggbd_1}"
    fi
fi

# Read ports from pipeline.yaml
GG19C_MGR_PORT=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg19c', {}).get('manager', {}).get('port', 7809))
" 2>/dev/null || echo "7809")

GG21C_MGR_PORT=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg21c', {}).get('manager', {}).get('port', 7999))
" 2>/dev/null || echo "7999")

PUMP_TARGET_HOST=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg19c', {}).get('pump', {}).get('target_host', 'localhost'))
" 2>/dev/null || echo "localhost")

PUMP_TARGET_PORT=$(python3 -c "
import yaml
with open('$CONFIG_FILE') as f:
    c = yaml.safe_load(f)
print(c.get('gg19c', {}).get('pump', {}).get('target_manager_port', 7909))
" 2>/dev/null || echo "7909")

# ---------------------------------------------------------------------------
# Colors and counters
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASS=0
WARN=0
FAIL=0

ok()   { echo -e "  ${GREEN}[OK]${NC}   $1"; ((PASS++)); }
warn() { echo -e "  ${YELLOW}[WARN]${NC} $1"; ((WARN++)); }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; ((FAIL++)); }
info() { echo -e "  ${CYAN}[INFO]${NC} $1"; }
section() { echo -e "\n${BOLD}── $1${NC}"; }

# ---------------------------------------------------------------------------
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  GoldenGate Pipeline — Lag Diagnostic${NC}"
echo -e "  GG 19c: $GG19C_HOME"
echo -e "  GG 21c: $GG21C_HOME"
echo -e "  Time:   $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# ==========================================================================
# 1. Manager Processes
# ==========================================================================
section "1. Manager Processes"

# Check GG 19c manager
if [ -f "$GG19C_HOME/ggsci" ]; then
    mgr19_out=$(echo "INFO MGR" | "$GG19C_HOME/ggsci" 2>/dev/null)
    if echo "$mgr19_out" | grep -qi "running"; then
        ok "GG 19c Manager is RUNNING (port $GG19C_MGR_PORT)"
    else
        fail "GG 19c Manager is NOT running — extracts and pumps cannot start"
        info "Fix: cd $GG19C_HOME && ./ggsci <<< 'START MGR'"
    fi
else
    fail "GG 19c ggsci not found at $GG19C_HOME/ggsci"
fi

# Check GG 21c manager
if [ -f "$GG21C_HOME/ggsci" ]; then
    mgr21_out=$(echo "INFO MGR" | "$GG21C_HOME/ggsci" 2>/dev/null)
    if echo "$mgr21_out" | grep -qi "running"; then
        ok "GG 21c Manager is RUNNING (port $GG21C_MGR_PORT)"
    else
        fail "GG 21c Manager is NOT running — replicats cannot start"
        info "Fix: cd $GG21C_HOME && ./ggsci <<< 'START MGR'"
    fi
else
    fail "GG 21c ggsci not found at $GG21C_HOME/ggsci"
fi

# ==========================================================================
# 2. Extract Status
# ==========================================================================
section "2. Extract Processes (GG 19c)"

if [ -f "$GG19C_HOME/ggsci" ]; then
    extract_out=$(echo "INFO ALL" | "$GG19C_HOME/ggsci" 2>/dev/null)
    extract_lines=$(echo "$extract_out" | grep -i "EXTRACT" || true)

    if [ -z "$extract_lines" ]; then
        warn "No extract processes found in GG 19c"
        info "Have you added and started the extracts? Run: ggctl deploy 19c"
    else
        while IFS= read -r line; do
            name=$(echo "$line" | awk '{print $3}')
            if echo "$line" | grep -qi "RUNNING"; then
                ok "Extract $name is RUNNING"
            elif echo "$line" | grep -qi "ABENDED"; then
                fail "Extract $name has ABENDED — data is NOT flowing"
                info "Fix: Check report — echo 'VIEW REPORT $name' | $GG19C_HOME/ggsci"
                info "Fix: Restart — echo 'START EXTRACT $name' | $GG19C_HOME/ggsci"
            elif echo "$line" | grep -qi "STOPPED"; then
                warn "Extract $name is STOPPED"
                info "Fix: echo 'START EXTRACT $name' | $GG19C_HOME/ggsci"
            fi
        done <<< "$extract_lines"
    fi

    # Check lag
    lag_out=$(echo "LAG EXTRACT *" | "$GG19C_HOME/ggsci" 2>/dev/null || true)
    lag_lines=$(echo "$lag_out" | grep -i "checkpoint" || true)
    if [ -n "$lag_lines" ]; then
        info "Extract lag info:"
        echo "$lag_lines" | while IFS= read -r l; do
            echo "         $l"
        done
    fi
fi

# ==========================================================================
# 3. Pump Status — is pump reading from the correct trail?
# ==========================================================================
section "3. Pump Processes (GG 19c -> 21c)"

if [ -f "$GG19C_HOME/ggsci" ]; then
    pump_lines=$(echo "$extract_out" | grep -iE "EXTRACT.*PUMP|PUMP" | grep -iv "^GGSCI\|^Oracle\|^Version" || true)

    # Also check for pump-type extracts (pumps show as EXTRACT type)
    # Get all extract names and check their params for RMTHOST
    all_names=$(echo "$extract_out" | grep -i "EXTRACT" | awk '{print $3}' || true)

    for pname in $all_names; do
        param_file="$GG19C_HOME/dirprm/$(echo "$pname" | tr '[:upper:]' '[:lower:]').prm"
        if [ -f "$param_file" ] && grep -qi "RMTHOST\|RMTTRAIL" "$param_file" 2>/dev/null; then
            info "Pump $pname config:"
            rmthost=$(grep -i "RMTHOST" "$param_file" 2>/dev/null | head -1 || true)
            rmttrail=$(grep -i "RMTTRAIL" "$param_file" 2>/dev/null | head -1 || true)
            exttrail=$(grep -i "EXTTRAIL" "$param_file" 2>/dev/null | head -1 || true)
            if [ -n "$rmthost" ]; then
                echo "         $rmthost"
            fi
            if [ -n "$rmttrail" ]; then
                echo "         $rmttrail"
            fi
            if [ -n "$exttrail" ]; then
                echo "         Reading from: $exttrail"
            fi
        fi
    done
fi

# ==========================================================================
# 4. Trail File Checks
# ==========================================================================
section "4. Trail Files"

# GG 19c trails (extract writes here, pump reads from here)
gg19_trail_dir="$GG19C_HOME/dirdat"
if [ -d "$gg19_trail_dir" ]; then
    trail_count=$(find "$gg19_trail_dir" -maxdepth 1 -type f -name "??*" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$trail_count" -gt 0 ]; then
        newest=$(find "$gg19_trail_dir" -maxdepth 1 -type f -name "??*" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 || \
                 ls -t "$gg19_trail_dir"/??* 2>/dev/null | head -1)

        # macOS compatible timestamp check
        if [ -n "$newest" ]; then
            newest_file=$(echo "$newest" | awk '{print $NF}')
            if [ -z "$newest_file" ]; then
                newest_file="$newest"
            fi
            newest_mtime=$(stat -f '%m' "$newest_file" 2>/dev/null || stat -c '%Y' "$newest_file" 2>/dev/null || echo "0")
            now=$(date +%s)
            age_minutes=$(( (now - newest_mtime) / 60 ))

            if [ "$age_minutes" -gt 30 ]; then
                warn "19c trails: $trail_count files — newest is ${age_minutes}min old (stale?)"
                info "If extract is RUNNING but trail is stale, no DML is happening on source tables"
            elif [ "$age_minutes" -gt 5 ]; then
                ok "19c trails: $trail_count files — newest is ${age_minutes}min old"
            else
                ok "19c trails: $trail_count files — actively being written (${age_minutes}min ago)"
            fi
        fi
    else
        warn "19c trail dir exists but no trail files found in $gg19_trail_dir"
        info "Extract may not have started capturing yet"
    fi
else
    warn "19c trail directory not found: $gg19_trail_dir"
fi

# GG 21c trails (pump writes here, replicat reads from here)
gg21_trail_dir="$GG21C_HOME/dirdat"
if [ -d "$gg21_trail_dir" ]; then
    trail_count=$(find "$gg21_trail_dir" -maxdepth 1 -type f -name "??*" 2>/dev/null | wc -l | tr -d ' ')
    if [ "$trail_count" -gt 0 ]; then
        newest=$(ls -t "$gg21_trail_dir"/??* 2>/dev/null | head -1)
        if [ -n "$newest" ]; then
            newest_mtime=$(stat -f '%m' "$newest" 2>/dev/null || stat -c '%Y' "$newest" 2>/dev/null || echo "0")
            now=$(date +%s)
            age_minutes=$(( (now - newest_mtime) / 60 ))

            if [ "$age_minutes" -gt 30 ]; then
                warn "21c trails: $trail_count files — newest is ${age_minutes}min old (pump not sending data?)"
                info "Check pump status and network connectivity"
            elif [ "$age_minutes" -gt 5 ]; then
                ok "21c trails: $trail_count files — newest is ${age_minutes}min old"
            else
                ok "21c trails: $trail_count files — actively being written (${age_minutes}min ago)"
            fi
        fi
    else
        warn "21c trail dir exists but no trail files found in $gg21_trail_dir"
        info "Pump may not be sending data to the 21c side yet"
    fi
else
    warn "21c trail directory not found: $gg21_trail_dir"
fi

# ==========================================================================
# 5. Replicat Status
# ==========================================================================
section "5. Replicat Processes (GG 21c)"

if [ -f "$GG21C_HOME/ggsci" ]; then
    rep_out=$(echo "INFO ALL" | "$GG21C_HOME/ggsci" 2>/dev/null)
    rep_lines=$(echo "$rep_out" | grep -i "REPLICAT" || true)

    if [ -z "$rep_lines" ]; then
        warn "No replicat processes found in GG 21c"
        info "Have you added and started the replicats? Run: ggctl deploy 21c"
    else
        while IFS= read -r line; do
            name=$(echo "$line" | awk '{print $3}')
            if echo "$line" | grep -qi "RUNNING"; then
                ok "Replicat $name is RUNNING"
            elif echo "$line" | grep -qi "ABENDED"; then
                fail "Replicat $name has ABENDED — data is NOT reaching Snowflake"
                info "Fix: Check report — echo 'VIEW REPORT $name' | $GG21C_HOME/ggsci"
                info "Fix: Restart — echo 'START REPLICAT $name' | $GG21C_HOME/ggsci"
            elif echo "$line" | grep -qi "STOPPED"; then
                warn "Replicat $name is STOPPED"
                info "Fix: echo 'START REPLICAT $name' | $GG21C_HOME/ggsci"
            fi
        done <<< "$rep_lines"
    fi

    # Check replicat reads from correct trail
    for prmfile in "$GG21C_HOME"/dirprm/*.prm; do
        [ -f "$prmfile" ] || continue
        if grep -qi "REPLICAT" "$prmfile" 2>/dev/null; then
            rep_name=$(basename "$prmfile" .prm)
            trail_ref=$(grep -i "EXTTRAIL\|RMTTRAIL" "$prmfile" 2>/dev/null | head -1 || true)
            if [ -n "$trail_ref" ]; then
                info "Replicat $rep_name reads from: $trail_ref"
            fi
        fi
    done

    # Replicat lag
    rep_lag=$(echo "LAG REPLICAT *" | "$GG21C_HOME/ggsci" 2>/dev/null || true)
    rep_lag_lines=$(echo "$rep_lag" | grep -i "checkpoint" || true)
    if [ -n "$rep_lag_lines" ]; then
        info "Replicat lag info:"
        echo "$rep_lag_lines" | while IFS= read -r l; do
            echo "         $l"
        done
    fi
fi

# ==========================================================================
# 6. Disk Space
# ==========================================================================
section "6. Disk Space"

for dir in "$GG19C_HOME/dirdat" "$GG21C_HOME/dirdat"; do
    if [ -d "$dir" ]; then
        usage=$(df -h "$dir" 2>/dev/null | tail -1)
        pct=$(echo "$usage" | awk '{print $(NF-1)}' | tr -d '%')
        mount=$(echo "$usage" | awk '{print $NF}')
        if [ -n "$pct" ] && [ "$pct" -gt 90 ] 2>/dev/null; then
            fail "Disk ${pct}% full on $mount (trail dir: $dir)"
            info "Full disk causes extract/pump to ABEND. Archive or purge old trails."
        elif [ -n "$pct" ] && [ "$pct" -gt 75 ] 2>/dev/null; then
            warn "Disk ${pct}% full on $mount (trail dir: $dir)"
        else
            ok "Disk ${pct:-?}% used on $mount (trail dir: $dir)"
        fi
    fi
done

# ==========================================================================
# 7. ENCKEYS Consistency
# ==========================================================================
section "7. Encryption Key Consistency"

enckeys_19="$GG19C_HOME/dirprm/ENCKEYS"
enckeys_21="$GG21C_HOME/dirprm/ENCKEYS"

# Check alternative locations too
[ ! -f "$enckeys_19" ] && enckeys_19="$GG19C_HOME/ENCKEYS"
[ ! -f "$enckeys_21" ] && enckeys_21="$GG21C_HOME/ENCKEYS"

if [ -f "$enckeys_19" ] && [ -f "$enckeys_21" ]; then
    # Compare key names (not values — just ensure same keys exist)
    keys_19=$(grep -v '^#\|^$' "$enckeys_19" 2>/dev/null | awk '{print $1}' | sort)
    keys_21=$(grep -v '^#\|^$' "$enckeys_21" 2>/dev/null | awk '{print $1}' | sort)

    if [ "$keys_19" = "$keys_21" ]; then
        # Also check if actual values match
        hash_19=$(md5sum "$enckeys_19" 2>/dev/null | awk '{print $1}' || md5 -q "$enckeys_19" 2>/dev/null || echo "?")
        hash_21=$(md5sum "$enckeys_21" 2>/dev/null | awk '{print $1}' || md5 -q "$enckeys_21" 2>/dev/null || echo "?")
        if [ "$hash_19" = "$hash_21" ]; then
            ok "ENCKEYS files are identical on 19c and 21c"
        else
            warn "ENCKEYS have same key names but different content — values may differ"
            info "Ensure the encryption key values match between $enckeys_19 and $enckeys_21"
        fi
    else
        fail "ENCKEYS mismatch between 19c and 21c — pump cannot decrypt/encrypt correctly"
        info "19c keys: $keys_19"
        info "21c keys: $keys_21"
        info "Fix: Copy ENCKEYS from 19c to 21c (or ensure both have the same key entries)"
    fi
elif [ -f "$enckeys_19" ] && [ ! -f "$enckeys_21" ]; then
    fail "ENCKEYS exists on 19c but NOT on 21c — encrypted trails cannot be read"
    info "Fix: cp $enckeys_19 $enckeys_21"
elif [ ! -f "$enckeys_19" ] && [ -f "$enckeys_21" ]; then
    warn "ENCKEYS exists on 21c but NOT on 19c — may be fine if encryption is disabled"
else
    info "No ENCKEYS files found (encryption may be disabled)"
fi

# ==========================================================================
# 8. Network Connectivity (19c -> 21c)
# ==========================================================================
section "8. Network Connectivity (19c -> 21c)"

# Test connectivity to 21c manager port (pump connects here)
if command -v nc &>/dev/null; then
    if nc -z -w 3 "$PUMP_TARGET_HOST" "$PUMP_TARGET_PORT" 2>/dev/null; then
        ok "Can reach $PUMP_TARGET_HOST:$PUMP_TARGET_PORT (pump target manager port)"
    else
        fail "Cannot reach $PUMP_TARGET_HOST:$PUMP_TARGET_PORT — pump cannot send data to 21c"
        info "Fix: Ensure GG 21c manager is running on port $PUMP_TARGET_PORT"
        info "Fix: Check firewall rules if 19c and 21c are on different hosts"
    fi
elif command -v bash &>/dev/null; then
    # Fallback: use bash /dev/tcp
    if (echo >/dev/tcp/"$PUMP_TARGET_HOST"/"$PUMP_TARGET_PORT") 2>/dev/null; then
        ok "Can reach $PUMP_TARGET_HOST:$PUMP_TARGET_PORT (pump target manager port)"
    else
        fail "Cannot reach $PUMP_TARGET_HOST:$PUMP_TARGET_PORT — pump cannot send data to 21c"
        info "Fix: Ensure GG 21c manager is running on port $PUMP_TARGET_PORT"
    fi
else
    warn "Cannot test connectivity (nc not available)"
fi

# Also check the 21c manager port (may differ from pump target)
if [ "$GG21C_MGR_PORT" != "$PUMP_TARGET_PORT" ]; then
    if command -v nc &>/dev/null; then
        if nc -z -w 3 "$PUMP_TARGET_HOST" "$GG21C_MGR_PORT" 2>/dev/null; then
            ok "Can reach $PUMP_TARGET_HOST:$GG21C_MGR_PORT (21c manager port)"
        else
            warn "Cannot reach $PUMP_TARGET_HOST:$GG21C_MGR_PORT (21c manager port)"
        fi
    fi
fi

# ==========================================================================
# 9. Recent Errors in ggserr.log
# ==========================================================================
section "9. Recent Errors (last 30 minutes)"

for label_home in "19c:$GG19C_HOME" "21c:$GG21C_HOME"; do
    label="${label_home%%:*}"
    home="${label_home##*:}"
    logfile="$home/ggserr.log"

    if [ -f "$logfile" ]; then
        # Get errors from last 30 minutes
        cutoff=$(date -d '30 minutes ago' '+%Y-%m-%dT%H:%M' 2>/dev/null || \
                 date -v-30M '+%Y-%m-%dT%H:%M' 2>/dev/null || echo "")

        if [ -n "$cutoff" ]; then
            recent_errors=$(awk -v cutoff="$cutoff" '
                /^[0-9]{4}-[0-9]{2}-[0-9]{2}T/ {
                    ts = substr($0, 1, 16)
                    if (ts >= cutoff) recent = 1; else recent = 0
                }
                recent && /ERROR|ABEND|OGG-/ { print }
            ' "$logfile" 2>/dev/null | tail -15)
        else
            # Fallback: just get last errors
            recent_errors=$(grep -iE "ERROR|ABEND|OGG-" "$logfile" 2>/dev/null | tail -10)
        fi

        if [ -n "$recent_errors" ]; then
            warn "GG $label ggserr.log has recent errors:"
            echo "$recent_errors" | while IFS= read -r eline; do
                echo -e "         ${RED}$eline${NC}"
            done
        else
            ok "GG $label ggserr.log — no recent errors"
        fi
    else
        info "GG $label ggserr.log not found at $logfile"
    fi
done

# ==========================================================================
# Summary
# ==========================================================================
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BOLD}  Summary${NC}"
echo -e "  ${GREEN}PASS: $PASS${NC}  |  ${YELLOW}WARN: $WARN${NC}  |  ${RED}FAIL: $FAIL${NC}"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo -e "  ${RED}Action required:${NC} Fix the FAIL items above to restore data flow."
    echo -e "  Common flow: Extract -> (19c trail) -> Pump -> (21c trail) -> Replicat -> Snowflake"
    echo -e "  Data stops at the first broken link in this chain."
elif [ "$WARN" -gt 0 ]; then
    echo ""
    echo -e "  ${YELLOW}Review warnings above. Pipeline may be functional but needs attention.${NC}"
else
    echo ""
    echo -e "  ${GREEN}All checks passed. Pipeline appears healthy.${NC}"
fi

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

exit $FAIL

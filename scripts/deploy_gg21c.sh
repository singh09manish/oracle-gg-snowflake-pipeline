#!/usr/bin/env bash
# Deploy GoldenGate BigData 21c (Replicat → Snowflake) from generated output/.
#
# Prerequisites:
#   - source ~/gg_environments.sh 21
#   - python3 -m app.main generate --input input/table_inventory.xlsx  (already run)
#   - Snowflake JDBC jar placed in $GG_HOME/dirprm/
#     Download: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/
#   - Set env vars: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#   - GG 19c pump trails must be flowing (deploy_gg19c.sh completed)
#
# Usage:
#   SNOWFLAKE_USER=gg_user SNOWFLAKE_PASSWORD=xxx ./scripts/deploy_gg21c.sh [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GG_HOME="${GG_HOME:-/oracle/product/21c/oggbd_1}"
DRY_RUN=0
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=1; done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [GG21c] $*"; }

# --- Pre-flight checks ---
if [ ! -f "$GG_HOME/ggsci" ]; then
    log "ERROR: $GG_HOME/ggsci not found. Set GG_HOME correctly."
    exit 1
fi

GEN_DIR="$PROJECT_ROOT/output/gg21c"
if [ ! -d "$GEN_DIR/dirprm" ]; then
    log "ERROR: $GEN_DIR/dirprm not found. Run 'python3 -m app.main generate' first."
    exit 1
fi

if [ -z "${SNOWFLAKE_USER:-}" ] || [ -z "${SNOWFLAKE_PASSWORD:-}" ]; then
    log "ERROR: SNOWFLAKE_USER and SNOWFLAKE_PASSWORD must be set."
    exit 2
fi

# Check Snowflake JDBC jar exists
JDBC_JAR=$(grep -oP 'snowflake-jdbc-[^ :]+\.jar' "$GEN_DIR/dirprm/"*.props 2>/dev/null | head -1)
if [ -n "$JDBC_JAR" ] && [ ! -f "$GG_HOME/dirprm/$JDBC_JAR" ]; then
    log "WARNING: Snowflake JDBC jar not found at $GG_HOME/dirprm/$JDBC_JAR"
    log "  Download from: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/"
    if [ "$DRY_RUN" -eq 0 ]; then
        log "ERROR: Cannot proceed without JDBC jar. Exiting."
        exit 1
    fi
fi

run_ggsci() {
    local oby="$1"
    if [ "$DRY_RUN" -eq 1 ]; then
        log "[DRY-RUN] ggsci < $(basename "$oby")"
        cat "$oby"; echo "---"
    else
        log "Running: ggsci < $(basename "$oby")"
        "$GG_HOME/ggsci" < "$oby"
    fi
}

DIRPRM="$GG_HOME/dirprm"

log "Copying .prm and .props files → $DIRPRM/"
if [ "$DRY_RUN" -eq 1 ]; then
    log "[DRY-RUN] cp $GEN_DIR/dirprm/*.{prm,props} $DIRPRM/"
    ls -la "$GEN_DIR/dirprm/"*.{prm,props} 2>/dev/null
else
    cp "$GEN_DIR/dirprm/"*.prm "$DIRPRM/"
    cp "$GEN_DIR/dirprm/"*.props "$DIRPRM/"
    COUNT=$(ls "$GEN_DIR/dirprm/"*.prm "$GEN_DIR/dirprm/"*.props 2>/dev/null | wc -l | tr -d ' ')
    log "Copied $COUNT files (.prm + .props)"
fi

log "--- Step 1: ADD REPLICATS ---"
run_ggsci "$GEN_DIR/ggsci/01_add_replicats.oby"

log "--- Step 2: START ALL ---"
run_ggsci "$GEN_DIR/ggsci/02_start_all.oby"

log "--- Step 3: STATUS ---"
run_ggsci "$GEN_DIR/ggsci/03_status.oby"

log "GG 21c deployment complete."

#!/usr/bin/env bash
# Deploy GoldenGate 19c (Extract + Pump) from generated output/.
#
# Prerequisites:
#   - source ~/gg_environments.sh 19
#   - python3 -m app.main generate --input input/table_inventory.xlsx  (already run)
#   - ENCKEYS file provisioned for AES256 trail encryption (in $GG_HOME)
#   - Credential store configured:  ggsci> ADD CREDENTIALSTORE
#                                   ggsci> ALTER CREDENTIALSTORE ADD USER ... ALIAS rdsadmin
#
# Usage:
#   ./scripts/deploy_gg19c.sh [--dry-run]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
GG_HOME="${GG_HOME:-/oracle/product/19c/oggcore_1}"
DRY_RUN=0
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=1; done

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [GG19c] $*"; }

# --- Pre-flight checks ---
if [ ! -f "$GG_HOME/ggsci" ]; then
    log "ERROR: $GG_HOME/ggsci not found. Set GG_HOME correctly."
    exit 1
fi

GEN_DIR="$PROJECT_ROOT/output/gg19c"
if [ ! -d "$GEN_DIR/dirprm" ]; then
    log "ERROR: $GEN_DIR/dirprm not found. Run 'python3 -m app.main generate' first."
    exit 1
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

log "Copying .prm files → $DIRPRM/"
if [ "$DRY_RUN" -eq 1 ]; then
    log "[DRY-RUN] cp $GEN_DIR/dirprm/*.prm $DIRPRM/"
    ls -la "$GEN_DIR/dirprm/"*.prm 2>/dev/null
else
    cp "$GEN_DIR/dirprm/"*.prm "$DIRPRM/"
    log "Copied $(ls "$GEN_DIR/dirprm/"*.prm | wc -l | tr -d ' ') .prm files"
fi

log "--- Step 1: ADD TRANDATA ---"
run_ggsci "$GEN_DIR/ggsci/01_trandata.oby"

log "--- Step 2: ADD EXTRACTS ---"
run_ggsci "$GEN_DIR/ggsci/02_add_extracts.oby"

log "--- Step 3: ADD PUMPS ---"
run_ggsci "$GEN_DIR/ggsci/03_add_pumps.oby"

log "--- Step 4: START ALL ---"
run_ggsci "$GEN_DIR/ggsci/04_start_all.oby"

log "--- Step 5: STATUS ---"
run_ggsci "$GEN_DIR/ggsci/05_status.oby"

log "GG 19c deployment complete."

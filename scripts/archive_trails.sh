#!/usr/bin/env bash
# =================================================================
# Archive GoldenGate trail files before manager purges them.
#
# This script copies trail files older than MIN_AGE_HOURS to an
# archive directory (local/NFS) or S3. Manager PURGEOLDEXTRACTS
# handles deletion — this script only archives.
#
# Designed for cron:
#   0 */6 * * *  /path/to/scripts/archive_trails.sh >> /var/log/gg-monitor/archive.log 2>&1
#
# Env vars (all have defaults from pipeline.yaml):
#   GG19C_HOME    — GG 19c installation directory
#   GG21C_HOME    — GG 21c BigData installation directory
#   ARCHIVE_DIR   — local/NFS archive directory
#   MIN_AGE_HOURS — minimum age in hours before archiving (default: 24)
#   S3_ENABLED    — set "true" to archive to S3 instead
#   S3_BUCKET     — S3 bucket name
#   S3_PREFIX     — S3 key prefix (e.g., "gg-trails")
#   S3_REGION     — AWS region
#
# Usage:
#   ./scripts/archive_trails.sh [--dry-run]
# =================================================================

set -euo pipefail

# --- Defaults ---
GG19C_HOME="${GG19C_HOME:-/u01/app/oracle/product/19c/oggcore_1}"
GG21C_HOME="${GG21C_HOME:-/u01/app/oracle/product/21c/oggbd_1}"
ARCHIVE_DIR="${ARCHIVE_DIR:-/mnt/archive/trails}"
MIN_AGE_HOURS="${MIN_AGE_HOURS:-24}"
S3_ENABLED="${S3_ENABLED:-false}"
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-gg-trails}"
S3_REGION="${S3_REGION:-us-east-1}"
DRY_RUN=0
for arg in "$@"; do [[ "$arg" == "--dry-run" ]] && DRY_RUN=1; done

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] [ARCHIVE] $*"; }

# --- Pre-flight ---
if [[ "$S3_ENABLED" == "true" ]]; then
    if ! command -v aws &>/dev/null; then
        log "ERROR: aws CLI not found. Install it or set S3_ENABLED=false."
        exit 1
    fi
    if [[ -z "$S3_BUCKET" ]]; then
        log "ERROR: S3_BUCKET must be set when S3_ENABLED=true."
        exit 1
    fi
    log "Archive target: s3://$S3_BUCKET/$S3_PREFIX/"
else
    log "Archive target: $ARCHIVE_DIR/"
    mkdir -p "$ARCHIVE_DIR/gg19c" "$ARCHIVE_DIR/gg21c"
fi

# --- Archive function ---
archive_trails() {
    local src_dir="$1"
    local label="$2"       # gg19c or gg21c
    local archived=0
    local skipped=0
    local errors=0

    if [[ ! -d "$src_dir" ]]; then
        log "SKIP: $src_dir does not exist"
        return
    fi

    log "Scanning $src_dir (min age: ${MIN_AGE_HOURS}h)..."

    # Find trail files older than MIN_AGE_HOURS
    # Trail files are typically 2-char prefix + sequence (e1000000001, p2000000003, etc.)
    while IFS= read -r trail_file; do
        local basename_f
        basename_f=$(basename "$trail_file")

        if [[ "$S3_ENABLED" == "true" ]]; then
            local s3_key="$S3_PREFIX/$label/$basename_f"
            if [[ "$DRY_RUN" -eq 1 ]]; then
                log "[DRY-RUN] aws s3 cp $trail_file s3://$S3_BUCKET/$s3_key"
            else
                if aws s3 cp "$trail_file" "s3://$S3_BUCKET/$s3_key" \
                    --region "$S3_REGION" \
                    --storage-class STANDARD_IA \
                    --quiet 2>/dev/null; then
                    ((archived++))
                else
                    log "ERROR: Failed to upload $trail_file to S3"
                    ((errors++))
                fi
            fi
        else
            local dest="$ARCHIVE_DIR/$label/$basename_f"
            if [[ -f "$dest" ]]; then
                # Already archived — skip
                ((skipped++))
                continue
            fi
            if [[ "$DRY_RUN" -eq 1 ]]; then
                log "[DRY-RUN] cp $trail_file → $dest"
            else
                if cp -p "$trail_file" "$dest" 2>/dev/null; then
                    ((archived++))
                else
                    log "ERROR: Failed to copy $trail_file"
                    ((errors++))
                fi
            fi
        fi
    done < <(find "$src_dir" -maxdepth 1 -type f -mmin +$((MIN_AGE_HOURS * 60)) \
                 \( -name "e*" -o -name "p*" -o -name "r*" \) 2>/dev/null | sort)

    log "$label: archived=$archived skipped=$skipped errors=$errors"
}

# --- Run ---
log "=== Trail Archival Started ==="

archive_trails "$GG19C_HOME/dirdat" "gg19c"
archive_trails "$GG21C_HOME/dirdat" "gg21c"

# --- Report disk usage ---
if [[ "$S3_ENABLED" != "true" ]] && [[ -d "$ARCHIVE_DIR" ]]; then
    archive_size=$(du -sh "$ARCHIVE_DIR" 2>/dev/null | awk '{print $1}')
    log "Archive size: $archive_size ($ARCHIVE_DIR)"
fi

log "=== Trail Archival Complete ==="

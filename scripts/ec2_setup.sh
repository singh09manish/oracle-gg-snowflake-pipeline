#!/usr/bin/env bash
# =============================================================================
# ec2_setup.sh — One-time setup for the GG pipeline project on an EC2 instance
# =============================================================================
#
# Run as: the oracle user (or whoever owns the GG installation)
#
# What it does:
#   1. Installs Python 3.11+ and pip (if not present)
#   2. Clones the repo (or pulls latest)
#   3. Creates virtualenv + installs dependencies
#   4. Creates directory structure (logs, archive, etc.)
#   5. Installs ggctl CLI to /usr/local/bin
#   6. Sets up environment profiles for multi-env support
#   7. Optionally installs systemd services (event watcher)
#
# Usage:
#   # First time setup
#   curl -sL https://raw.githubusercontent.com/singh09manish/oracle-gg-snowflake-pipeline/main/scripts/ec2_setup.sh | bash
#
#   # Or after cloning:
#   ./scripts/ec2_setup.sh
#
#   # With custom install dir:
#   PIPELINE_HOME=/opt/gg-pipeline ./scripts/ec2_setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PIPELINE_HOME="${PIPELINE_HOME:-/home/$(whoami)/oracle-gg-snowflake-pipeline}"
REPO_URL="https://github.com/singh09manish/oracle-gg-snowflake-pipeline.git"
VENV_DIR="$PIPELINE_HOME/.venv"
LOG_DIR="/var/log/gg-monitor"
ARCHIVE_DIR="/mnt/archive/trails"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SETUP] $*"; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Step 1: Python 3.11+
# ---------------------------------------------------------------------------

log "Step 1: Checking Python..."

if command -v python3.11 &>/dev/null; then
    PYTHON=python3.11
elif command -v python3 &>/dev/null; then
    PY_VER=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
    if [[ "$(echo "$PY_VER >= 3.9" | bc 2>/dev/null || echo 0)" == "1" ]] || python3 -c 'import sys; exit(0 if sys.version_info >= (3,9) else 1)' 2>/dev/null; then
        PYTHON=python3
    else
        warn "Python $PY_VER found — need 3.9+. Installing..."
        if command -v dnf &>/dev/null; then
            sudo dnf install -y python3.11 python3.11-pip 2>/dev/null || \
            sudo dnf install -y python39 python39-pip 2>/dev/null || \
            err "Could not install Python 3.9+. Install manually."
            PYTHON=python3.11
        elif command -v yum &>/dev/null; then
            sudo yum install -y python3 python3-pip
            PYTHON=python3
        else
            err "No package manager found. Install Python 3.9+ manually."
        fi
    fi
else
    log "Python 3 not found. Installing..."
    if command -v dnf &>/dev/null; then
        sudo dnf install -y python3.11 python3.11-pip
        PYTHON=python3.11
    elif command -v yum &>/dev/null; then
        sudo yum install -y python3 python3-pip
        PYTHON=python3
    else
        err "No package manager found. Install Python 3.9+ manually."
    fi
fi

log "Using Python: $($PYTHON --version)"

# ---------------------------------------------------------------------------
# Step 2: Clone or update repo
# ---------------------------------------------------------------------------

log "Step 2: Setting up project at $PIPELINE_HOME..."

if [ -d "$PIPELINE_HOME/.git" ]; then
    log "Repo exists — pulling latest..."
    cd "$PIPELINE_HOME"
    git pull origin main
else
    log "Cloning repo..."
    git clone "$REPO_URL" "$PIPELINE_HOME"
    cd "$PIPELINE_HOME"
fi

# ---------------------------------------------------------------------------
# Step 3: Virtual environment + dependencies
# ---------------------------------------------------------------------------

log "Step 3: Setting up Python virtualenv..."

if [ ! -d "$VENV_DIR" ]; then
    $PYTHON -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r requirements.txt -q

log "Installed $(pip list --format=columns | wc -l) packages"

# ---------------------------------------------------------------------------
# Step 4: Directory structure
# ---------------------------------------------------------------------------

log "Step 4: Creating directories..."

mkdir -p "$PIPELINE_HOME/input"
mkdir -p "$PIPELINE_HOME/output"
mkdir -p "$PIPELINE_HOME/envs"

# Log directory (may need sudo)
if [ ! -d "$LOG_DIR" ]; then
    sudo mkdir -p "$LOG_DIR" 2>/dev/null || mkdir -p "$HOME/gg-monitor-logs"
    sudo chown "$(whoami)" "$LOG_DIR" 2>/dev/null || true
    log "Log directory: $LOG_DIR"
fi

# Archive directory (may need sudo or NFS mount)
if [ ! -d "$ARCHIVE_DIR" ]; then
    sudo mkdir -p "$ARCHIVE_DIR" 2>/dev/null || mkdir -p "$HOME/trail-archive"
    sudo chown "$(whoami)" "$ARCHIVE_DIR" 2>/dev/null || true
    log "Archive directory: $ARCHIVE_DIR"
fi

# ---------------------------------------------------------------------------
# Step 5: Install ggctl CLI
# ---------------------------------------------------------------------------

log "Step 5: Installing ggctl CLI..."

chmod +x "$PIPELINE_HOME/ggctl"

# Symlink to /usr/local/bin (needs sudo)
if [ -w /usr/local/bin ] || sudo test -w /usr/local/bin 2>/dev/null; then
    sudo ln -sf "$PIPELINE_HOME/ggctl" /usr/local/bin/ggctl
    log "ggctl installed → /usr/local/bin/ggctl"
else
    # Fallback: add to PATH via profile
    PROFILE="$HOME/.bashrc"
    if ! grep -q "PIPELINE_HOME" "$PROFILE" 2>/dev/null; then
        cat >> "$PROFILE" <<PROFILE_EOF

# GoldenGate Pipeline CLI
export PIPELINE_HOME="$PIPELINE_HOME"
export PATH="\$PIPELINE_HOME:\$PATH"
PROFILE_EOF
        log "Added \$PIPELINE_HOME to PATH in $PROFILE"
        log "Run: source $PROFILE"
    fi
fi

# ---------------------------------------------------------------------------
# Step 6: Create default environment profile (if none exists)
# ---------------------------------------------------------------------------

log "Step 6: Setting up environment profiles..."

if [ ! -f "$PIPELINE_HOME/envs/default.env" ]; then
    cat > "$PIPELINE_HOME/envs/default.env" <<'ENV_EOF'
# =============================================================================
# GoldenGate Pipeline Environment Profile
# =============================================================================
# Copy this file for each environment:
#   cp envs/default.env envs/dev.env
#   cp envs/default.env envs/prod.env
#
# Activate with: ggctl env use dev
# =============================================================================

# Environment name (used in logging and alerts)
GG_ENV_NAME="default"

# GoldenGate 19c home (Extract + Pump)
GG19C_HOME="/u01/app/oracle/product/19c/oggcore_1"

# GoldenGate 21c BigData home (Replicat → Snowflake)
GG21C_HOME="/u01/app/oracle/product/21c/oggbd_1"

# Java home for GG 21c
JAVA_HOME="/usr/lib/jvm/java-11-openjdk-11.0.25.0.9-2.el8.x86_64"

# Oracle source alias (GG credential store)
GG_SOURCE_ALIAS="rdsadmin"

# Snowflake connection (used by audit scripts)
SNOWFLAKE_ACCOUNT="your_account"
SNOWFLAKE_WAREHOUSE="YOUR_WH"
SNOWFLAKE_DATABASE="YOUR_DB"
SNOWFLAKE_ROLE="GG_ROLE"
# SNOWFLAKE_USER and SNOWFLAKE_PASSWORD should be set via AWS Secrets Manager
# or exported manually — never store passwords in this file.

# Pipeline config file (environment-specific overrides)
PIPELINE_CONFIG="config/pipeline.yaml"

# Table inventory file
TABLE_INVENTORY="input/table_inventory.xlsx"
ENV_EOF
    log "Created default.env — copy and edit for each environment"
fi

# ---------------------------------------------------------------------------
# Step 7: Verify GG installations (optional)
# ---------------------------------------------------------------------------

log "Step 7: Verifying GoldenGate installations..."

check_gg() {
    local label="$1" home="$2"
    if [ -f "$home/ggsci" ]; then
        log "  $label: $home — OK"
    elif [ -d "$home" ]; then
        warn "  $label: $home — directory exists but ggsci not found"
    else
        warn "  $label: $home — NOT FOUND (update envs/*.env)"
    fi
}

# Source default env to check paths
if [ -f "$PIPELINE_HOME/envs/default.env" ]; then
    source "$PIPELINE_HOME/envs/default.env"
    check_gg "GG 19c" "${GG19C_HOME:-/u01/app/oracle/product/19c/oggcore_1}"
    check_gg "GG 21c" "${GG21C_HOME:-/u01/app/oracle/product/21c/oggbd_1}"
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

log ""
log "============================================="
log "  EC2 SETUP COMPLETE"
log "============================================="
log ""
log "Next steps:"
log "  1. Edit your environment profile:"
log "     vi $PIPELINE_HOME/envs/default.env"
log ""
log "  2. For multiple environments, copy the profile:"
log "     cp envs/default.env envs/dev.env"
log "     cp envs/default.env envs/prod.env"
log ""
log "  3. Activate an environment:"
log "     ggctl env use dev"
log ""
log "  4. Place your table inventory:"
log "     scp table_inventory.xlsx ec2-user@this-host:$PIPELINE_HOME/input/"
log ""
log "  5. Generate configs and deploy:"
log "     ggctl generate"
log "     ggctl deploy 19c --dry-run"
log "     ggctl deploy 19c"
log "     ggctl deploy 21c"
log ""
log "  6. Start monitoring:"
log "     ggctl watch"
log ""
log "  7. Check status anytime:"
log "     ggctl status"
log ""

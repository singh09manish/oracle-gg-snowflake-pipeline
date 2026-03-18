#!/usr/bin/env bash
# =============================================================================
# ec2_setup.sh — One-time setup for the GG pipeline project on an EC2 instance
# =============================================================================
#
# Run as: the oracle user (or whoever owns the GG installation)
#
# What it does:
#   1. Installs Python 3.9+ and pip (if not present)
#   2. Clones the repo (or pulls latest if already cloned)
#   3. Creates virtualenv + installs dependencies
#   4. Creates directory structure (logs, archive)
#   5. Installs ggctl CLI to /usr/local/bin
#   6. Verifies GG 19c/21c installations
#
# Usage:
#   # After SCP'ing the project to EC2:
#   cd /home/oracle/oracle-gg-snowflake-pipeline
#   ./scripts/ec2_setup.sh
#
#   # Or one-liner from GitHub:
#   git clone https://github.com/singh09manish/oracle-gg-snowflake-pipeline.git
#   cd oracle-gg-snowflake-pipeline && ./scripts/ec2_setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIPELINE_HOME="${PIPELINE_HOME:-$(cd "$SCRIPT_DIR/.." && pwd)}"
VENV_DIR="$PIPELINE_HOME/.venv"
LOG_DIR="/var/log/gg-monitor"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [SETUP] $*"; }
warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [WARN]  $*" >&2; }
err()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Step 1: Python 3.9+
# ---------------------------------------------------------------------------

log "Step 1: Checking Python..."

PYTHON=""
for py in python3.11 python3.9 python3; do
    if command -v "$py" &>/dev/null; then
        ver=$($py -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        if $py -c 'import sys; exit(0 if sys.version_info >= (3,9) else 1)' 2>/dev/null; then
            PYTHON="$py"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    log "Python 3.9+ not found. Installing..."
    if command -v dnf &>/dev/null; then
        sudo dnf install -y python3.11 python3.11-pip 2>/dev/null || \
        sudo dnf install -y python39 python39-pip 2>/dev/null || \
        err "Could not install Python. Install Python 3.9+ manually."
        PYTHON="$(command -v python3.11 || command -v python3.9 || command -v python3)"
    elif command -v yum &>/dev/null; then
        sudo yum install -y python3 python3-pip
        PYTHON="python3"
    elif command -v apt-get &>/dev/null; then
        sudo apt-get update && sudo apt-get install -y python3 python3-pip python3-venv
        PYTHON="python3"
    else
        err "No package manager found. Install Python 3.9+ manually."
    fi
fi

log "Using Python: $($PYTHON --version)"

# ---------------------------------------------------------------------------
# Step 2: Virtual environment + dependencies
# ---------------------------------------------------------------------------

log "Step 2: Setting up Python virtualenv..."

if [ ! -d "$VENV_DIR" ]; then
    $PYTHON -m venv "$VENV_DIR"
    log "Created virtualenv at $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r "$PIPELINE_HOME/requirements.txt" -q

log "Installed $(pip list --format=columns | wc -l) packages"

# ---------------------------------------------------------------------------
# Step 3: Directory structure
# ---------------------------------------------------------------------------

log "Step 3: Creating directories..."

mkdir -p "$PIPELINE_HOME/input"
mkdir -p "$PIPELINE_HOME/output"

# Log directory
if [ ! -d "$LOG_DIR" ]; then
    sudo mkdir -p "$LOG_DIR" 2>/dev/null && sudo chown "$(whoami)" "$LOG_DIR" 2>/dev/null || {
        LOG_DIR="$PIPELINE_HOME/logs"
        mkdir -p "$LOG_DIR"
    }
fi
log "Log directory: $LOG_DIR"

# ---------------------------------------------------------------------------
# Step 4: Install ggctl CLI
# ---------------------------------------------------------------------------

log "Step 4: Installing ggctl CLI..."

chmod +x "$PIPELINE_HOME/ggctl"
chmod +x "$PIPELINE_HOME/scripts/"*.sh 2>/dev/null || true

# Symlink to /usr/local/bin
if sudo ln -sf "$PIPELINE_HOME/ggctl" /usr/local/bin/ggctl 2>/dev/null; then
    log "ggctl installed → /usr/local/bin/ggctl"
else
    # Fallback: add project to PATH
    PROFILE="$HOME/.bashrc"
    if ! grep -q "PIPELINE_HOME" "$PROFILE" 2>/dev/null; then
        cat >> "$PROFILE" <<PROFILE_EOF

# GoldenGate Pipeline CLI
export PIPELINE_HOME="$PIPELINE_HOME"
export PATH="\$PIPELINE_HOME:\$PATH"
PROFILE_EOF
        log "Added PIPELINE_HOME to PATH in $PROFILE"
        log "Run: source $PROFILE"
    fi
fi

# ---------------------------------------------------------------------------
# Step 5: Verify GG installations
# ---------------------------------------------------------------------------

log "Step 5: Verifying GoldenGate installations..."

_activate_venv() { source "$VENV_DIR/bin/activate"; }
_activate_venv

# Parse GG homes from pipeline.yaml
GG19C_HOME=$($PYTHON -c "
import yaml
with open('$PIPELINE_HOME/config/pipeline.yaml') as f:
    c = yaml.safe_load(f)
print(c.get('gg19c', {}).get('home', '/u01/app/oracle/product/19c/oggcore_1'))
" 2>/dev/null || echo "/u01/app/oracle/product/19c/oggcore_1")

GG21C_HOME=$($PYTHON -c "
import yaml
with open('$PIPELINE_HOME/config/pipeline.yaml') as f:
    c = yaml.safe_load(f)
print(c.get('gg21c', {}).get('home', '/u01/app/oracle/product/21c/oggbd_1'))
" 2>/dev/null || echo "/u01/app/oracle/product/21c/oggbd_1")

check_gg() {
    local label="$1" home="$2"
    if [ -f "$home/ggsci" ]; then
        log "  $label: $home — OK"
    elif [ -d "$home" ]; then
        warn "  $label: $home — directory exists but ggsci not found"
    else
        warn "  $label: $home — NOT FOUND (update config/pipeline.yaml)"
    fi
}

check_gg "GG 19c" "$GG19C_HOME"
check_gg "GG 21c" "$GG21C_HOME"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

log ""
log "============================================="
log "  EC2 SETUP COMPLETE"
log "============================================="
log ""
log "Next steps:"
log ""
log "  1. Edit config/pipeline.yaml with this instance's GG paths"
log "     and Snowflake connection details:"
log "     vi $PIPELINE_HOME/config/pipeline.yaml"
log ""
log "  2. Place your table inventory:"
log "     cp /path/to/table_inventory.xlsx $PIPELINE_HOME/input/"
log ""
log "  3. Generate configs:"
log "     ggctl generate"
log ""
log "  4. Preview deployment:"
log "     ggctl deploy all --dry-run"
log ""
log "  5. Deploy:"
log "     ggctl deploy all"
log ""
log "  6. Check status:"
log "     ggctl status"
log ""
log "  For all commands: ggctl help"
log ""

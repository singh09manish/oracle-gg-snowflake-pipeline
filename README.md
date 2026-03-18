# Oracle GoldenGate → Snowflake CDC Pipeline

**Inventory-driven, zero-manual-work CDC pipeline generator with a unified CLI (`ggctl`).**

Oracle RDS → GG 19c Classic (Extract + Pump) → GG BigData 21c (Replicat via JDBC) → Snowflake.
All on a single EC2 instance (RHEL 8). Deploy the same code to any environment by editing one config file.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     EC2 Instance (RHEL 8)                               │
│                                                                          │
│   table_inventory.xlsx                                                   │
│   (schema, table_name, group_id, enabled)                                │
│              │                                                           │
│       ggctl generate                                                     │
│              │                                                           │
│   ┌──────────┼──────────────┐                                            │
│   ▼          ▼              ▼                                            │
│  GG 19c    GG 21c       GGSCI                                           │
│  params    params       obeyfiles                                        │
│   │          │                                                           │
│   ▼          ▼                                                           │
│  GG 19c Classic        GG 21c BigData                                    │
│  (port 7809)           (port 7999)                                       │
│   │                        │                                             │
│  Extract ──► Pump ──────► Replicat ──► Snowflake                         │
│  (redo)    (PASSTHRU)    (JDBC)       (append-only)                      │
│  AES256     AES256        TLS                                            │
│  trail      in-transit    JDBC                                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow (per extract group)
```
Oracle RDS  ──►  EXT01 (Integrated Extract)
                   │  ENCRYPTTRAIL AES256
                   ▼
                 ./dirdat/e1*  (local trail)
                   │
                 PMP01 (Data Pump, PASSTHRU)
                   │  ENCRYPT RMTHOST AES256
                   ▼
                 ./dirdat/p1*  (remote trail on GG 21c)
                   │
                 REP01 (BigData Replicat)
                   │  INSERTALLRECORDS + COLMAP
                   │  (OP_TYPE, OP_TS, IS_DELETED, etc.)
                   ▼
                 Snowflake (TLS/JDBC)
                   └── Append-only staging tables
                       └── Downstream: MERGE / SCD Type 2
```

---

## Table of Contents

- [Quick Start (EC2 Deployment)](#quick-start-ec2-deployment)
- [Project Structure](#project-structure)
- [The `ggctl` CLI](#the-ggctl-cli)
- [Table Inventory](#table-inventory)
- [Auto-Grouping 500+ Tables](#auto-grouping-500-tables)
- [Enable/Disable Tables](#enabledisable-tables)
- [Monitoring & Alerting](#monitoring--alerting)
- [Trail File Archival](#trail-file-archival)
- [Deploying to Multiple EC2 Instances](#deploying-to-multiple-ec2-instances)
- [Process Naming Convention](#process-naming-convention)
- [Design Decisions](#design-decisions)
- [Troubleshooting](#troubleshooting)

---

## Quick Start (EC2 Deployment)

### Prerequisites

- EC2 instance with RHEL 8 (or Amazon Linux 2)
- Oracle GoldenGate 19c Classic installed (for Extract + Pump)
- Oracle GoldenGate 21c BigData installed (for Replicat → Snowflake)
- Python 3.9+
- Network access to Oracle RDS and Snowflake
- ENCKEYS file provisioned for AES256 trail encryption
- GG credential store configured:
  ```
  ggsci> ADD CREDENTIALSTORE
  ggsci> ALTER CREDENTIALSTORE ADD USER ogg_user PASSWORD xxx ALIAS rdsadmin
  ```

### Step 1: Get the code onto your EC2

```bash
# Option A: SCP from your local machine
scp -r oracle-gg-snowflake-pipeline/ ec2-user@your-ec2:/home/oracle/

# Option B: Clone directly on EC2
git clone https://github.com/singh09manish/oracle-gg-snowflake-pipeline.git
cd oracle-gg-snowflake-pipeline
```

### Step 2: Run setup

```bash
./scripts/ec2_setup.sh
```

This installs Python, creates a virtualenv, installs dependencies, and makes `ggctl` available system-wide.

### Step 3: Edit config for this EC2

```bash
vi config/pipeline.yaml
```

Update these values for your environment:
```yaml
source:
  alias: "rdsadmin"            # your GG credential store alias

gg19c:
  home: /u01/app/oracle/product/19c/oggcore_1    # your GG 19c path

gg21c:
  home: /u01/app/oracle/product/21c/oggbd_1      # your GG 21c path
  snowflake:
    account: "xy12345.us-east-1"
    warehouse: "YOUR_WH"
    database: "YOUR_DB"
    role: "GG_ROLE"
```

### Step 4: Place your table inventory

```bash
cp /path/to/table_inventory.xlsx input/table_inventory.xlsx
```

See [Table Inventory](#table-inventory) for the required format.

### Step 5: Generate configs

```bash
ggctl generate
```

This reads the inventory and produces all `.prm` files, `.properties` files, and GGSCI obeyfiles in `output/`.

### Step 6: Deploy (dry-run first!)

```bash
# Preview what will happen
ggctl deploy all --dry-run

# Deploy GG 19c (extract + pump)
ggctl deploy 19c

# Deploy GG 21c (replicat → Snowflake)
ggctl deploy 21c

# Or deploy both at once
ggctl deploy all
```

### Step 7: Verify

```bash
# Check all processes
ggctl status

# Check lag
ggctl lag

# Open GGSCI directly
ggctl use 19c    # switch to GG 19c context
ggctl ggsci      # opens GGSCI for GG 19c

ggctl use 21c    # switch to GG 21c context
ggctl ggsci      # opens GGSCI for GG 21c
```

### Step 8: Start monitoring

```bash
# Real-time ABEND watcher (runs in foreground, Ctrl+C to stop)
ggctl watch

# With auto-disable (automatically excludes failing tables)
ggctl watch --auto-disable

# Or run as a systemd service
sudo cp scripts/gg-event-watcher.service /etc/systemd/system/
sudo systemctl enable --now gg-event-watcher
```

---

## Project Structure

```
oracle-gg-snowflake-pipeline/
├── ggctl                           ← Unified CLI (bash wrapper)
├── config/
│   └── pipeline.yaml               ← All infrastructure settings (GG paths, ports, Snowflake)
├── input/
│   └── table_inventory.xlsx         ← YOUR table inventory (source of truth)
├── app/
│   ├── main.py                      ← CLI: generate, validate, report, disable, enable, status
│   ├── inventory.py                 ← Reads/writes Excel/CSV inventory
│   ├── grouper.py                   ← Groups tables by group_id, filters disabled
│   ├── generator.py                 ← Renders Jinja2 templates → output/
│   ├── auto_grouper.py              ← Intelligent bin-packing for 500+ tables
│   ├── validator.py                 ← Oracle pre-flight (PKs, TRANDATA, LOBs)
│   └── models.py                    ← TableInfo, ExtractGroup, PipelineManifest
├── templates/
│   ├── gg19c/                       ← Jinja2: extract.prm, pump.prm, mgr.prm
│   └── gg21c/                       ← Jinja2: replicat.prm, snowflake.props, mgr.prm
├── output/                          ← Generated files (gitignored, recreate with ggctl generate)
│   ├── gg19c/dirprm/                ← EXT01.prm, PMP01.prm, mgr.prm
│   ├── gg19c/ggsci/                 ← 01_trandata.oby through 05_status.oby
│   ├── gg21c/dirprm/                ← REP01.prm, REP01.properties, mgr.prm
│   └── gg21c/ggsci/                 ← 01_add_replicats.oby through 03_status.oby
├── scripts/
│   ├── ec2_setup.sh                 ← One-time EC2 setup (Python, venv, ggctl)
│   ├── deploy_gg19c.sh              ← Deploy extract + pump (GGSCI obeyfiles)
│   ├── deploy_gg21c.sh              ← Deploy replicat (GGSCI obeyfiles)
│   ├── auto_group.py                ← CLI for auto-grouping tables by DML activity
│   ├── event_watcher.py             ← Real-time ABEND watcher + notifications
│   ├── gg-event-watcher.service     ← Systemd service for event watcher
│   ├── monitor.sh                   ← Periodic health check (cron)
│   ├── archive_trails.sh            ← Trail file archival (local or S3)
│   └── health_check.sh              ← Basic health check
├── sql/
│   └── table_grouping_candidates.sql ← Oracle query for DML activity scores
├── audit/
│   ├── control_totals.py            ← Oracle source row counts
│   ├── snowflake_totals.py          ← Snowflake target row counts
│   └── recon.py                     ← Source vs target reconciliation
└── requirements.txt
```

---

## The `ggctl` CLI

`ggctl` is the single entry point for all operations. It reads GG paths from `config/pipeline.yaml` — no environment files needed.

```bash
# GoldenGate context switching (19c ↔ 21c)
ggctl use 19c                     # Switch to GG 19c (extract/pump)
ggctl use 21c                     # Switch to GG 21c (replicat/snowflake)
ggctl ggsci                       # Open GGSCI in current context
ggctl info                        # Show current config + GG paths

# Pipeline operations
ggctl generate                    # Generate all .prm files from inventory
ggctl deploy 19c [--dry-run]      # Deploy extract + pump
ggctl deploy 21c [--dry-run]      # Deploy replicat
ggctl deploy all [--dry-run]      # Deploy everything

# Table control
ggctl disable -t SCHEMA.TABLE [-r "reason"]
ggctl enable  -t SCHEMA.TABLE
ggctl tables                      # Show enabled/disabled status

# Monitoring
ggctl status                      # All GG processes + lag
ggctl watch [--auto-disable]      # Real-time ABEND watcher
ggctl health                      # Run health check
ggctl lag                         # Show lag only
ggctl report                      # Group assignment report

# Maintenance
ggctl archive                     # Archive old trail files
ggctl update                      # Pull latest code from GitHub
ggctl setup                       # Re-run EC2 setup
```

### GG Context Switching

Both GG 19c and GG 21c run on the same EC2. Use `ggctl use` to switch which one `ggsci` connects to:

```bash
ggctl use 19c        # sets context to GG 19c
ggctl ggsci          # opens: /u01/.../19c/oggcore_1/ggsci
# → INFO ALL, LAG EXTRACT *, etc.

ggctl use 21c        # sets context to GG 21c
ggctl ggsci          # opens: /u01/.../21c/oggbd_1/ggsci
# → INFO ALL, LAG REPLICAT *, etc.
```

The context persists across terminal sessions (stored in `.ggctl_state`).

---

## Table Inventory

The Excel/CSV file is the **single source of truth** for the entire pipeline. Every extract, pump, and replicat is generated from it.

### Required columns

| Column | Required | Description |
|--------|----------|-------------|
| `schema` | Yes | Oracle source schema (e.g., `HR`, `SALES`) |
| `table_name` | Yes | Oracle table name (e.g., `EMPLOYEES`) |
| `group_id` | Yes | Integer — maps to EXT01/PMP01/REP01 (group 1), EXT02/PMP02/REP02 (group 2), etc. |
| `target_schema` | No | Snowflake target schema. Defaults to source schema if blank |
| `enabled` | No | `Y` or `N`. Defaults to `Y`. Disabled tables are excluded from `.prm` generation |
| `disabled_reason` | No | Why the table was disabled (auto-populated by event watcher) |
| `disabled_at` | No | ISO timestamp of when it was disabled |

### Example

| schema | table_name | group_id | target_schema | enabled |
|--------|-----------|----------|---------------|---------|
| HR | EMPLOYEES | 1 | HR_PROD | Y |
| HR | DEPARTMENTS | 1 | HR_PROD | Y |
| SALES | ORDERS | 2 | SALES_PROD | Y |
| SALES | BAD_TABLE | 2 | SALES_PROD | N |

### Column aliases

The reader is flexible — these column names all work:

- Schema: `schema`, `schema_name`, `owner`, `source_schema`
- Table: `table_name`, `tablename`, `table`, `name`, `object_name`
- Group: `group_id`, `group`, `extract_group`
- Enabled: `enabled`, `active`, `status`

---

## Auto-Grouping 500+ Tables

When you have hundreds of tables to split across extracts (50–75 per group), use the auto-grouper:

### Step 1: Run the Oracle query

```sql
-- On your Oracle RDS (as DBA or OGG user)
@sql/table_grouping_candidates.sql
-- Exports → /tmp/table_grouping_candidates.csv
```

This pulls DML activity from `dba_tab_modifications` — inserts, updates (weighted 2x — expensive for GG), deletes — plus segment sizes and PK checks.

### Step 2: Run the auto-grouper

```bash
python3 scripts/auto_group.py \
    --from-csv /tmp/table_grouping_candidates.csv \
    --schemas HR SALES FINANCE ORDERS \
    --max 75 --min 50 \
    --output input/table_inventory.xlsx \
    --dry-run   # preview first, remove to write
```

### What the algorithm does

1. **Sorts tables** by schema + DML activity (heaviest first)
2. **Bin-packs** into groups balancing both table count and DML load
3. **Keeps same-schema tables together** (reduces TRANDATA overhead)
4. **Flags tables without PKs** — you'll need `KEYCOLS` in the extract `.prm`
5. **Auto-scales** — opens new groups if all buckets are full

Output preview:
```
════════════════════════════════════════════════
  EXTRACT GROUP PLAN   (500 tables total)
════════════════════════════════════════════════
  Extract    Tables     DML Score  No-PK  Schemas
  ──────────────────────────────────────────────
  EXT01          73       982,400      0  FINANCE, HR
  EXT02          74       978,100      2  SALES
  EXT03          75       975,300      0  ORDERS
  ...
════════════════════════════════════════════════
```

---

## Enable/Disable Tables

When a table causes issues (missing PK, DDL change, permission error), disable it without touching anyone else:

```bash
# Disable a failing table
ggctl disable -t HR.EMPLOYEES -r "OGG-01004: no supplemental logging"

# Disable multiple tables
ggctl disable -t HR.EMPLOYEES SALES.ORDERS -r "extract EXT01 ABEND"

# Check status
ggctl tables

# Re-enable after fix
ggctl enable -t HR.EMPLOYEES

# IMPORTANT: regenerate configs after any change
ggctl generate
```

### What happens when you disable a table

1. The table's `enabled` column is set to `N` in the inventory Excel
2. The row is highlighted red in Excel for easy visual identification
3. `disabled_reason` and `disabled_at` are recorded
4. On next `ggctl generate`, the table is excluded from all `.prm` files
5. The extract group continues to work with the remaining tables
6. **No data loss** — GG checkpoint-based recovery handles restart

---

## Monitoring & Alerting

### Quick status checks

```bash
ggctl status    # all processes + lag (both 19c and 21c)
ggctl lag       # lag only
ggctl health    # full health check
```

### Real-time ABEND Event Watcher

The event watcher tails `ggserr.log` and reacts to ABENDs in real time:

```bash
# Watch GG 19c (switch context first)
ggctl use 19c
ggctl watch

# Watch with auto-disable (failing table auto-excluded)
ggctl watch --auto-disable

# Watch with auto-disable AND auto-regenerate configs
ggctl watch --auto-disable --auto-regenerate
```

**What happens on ABEND:**
1. Detects ABEND in `ggserr.log` (real-time `tail -F`)
2. Parses `dirrpt/EXT01.rpt` to identify the OGG error code + failing table
3. Sends notifications via Slack, email, PagerDuty, or webhook
4. (With `--auto-disable`) Disables the failing table in inventory
5. (With `--auto-regenerate`) Regenerates `.prm` files automatically
6. You restart the extract — checkpoint recovery means **zero data loss**

### Configure notifications

Edit `config/pipeline.yaml`:

```yaml
monitoring:
  alerting:
    slack:
      enabled: true
      webhook_url: "https://hooks.slack.com/services/T00/B00/xxx"
      channel: "#gg-alerts"

    email:
      enabled: true
      recipients: "oncall@yourcompany.com"

    pagerduty:
      enabled: true
      routing_key: "your-integration-key"
```

### Run as a systemd service

```bash
sudo cp scripts/gg-event-watcher.service /etc/systemd/system/
# Edit the service file to set correct GG_HOME path
sudo vi /etc/systemd/system/gg-event-watcher.service
sudo systemctl daemon-reload
sudo systemctl enable --now gg-event-watcher

# Check logs
journalctl -u gg-event-watcher -f
```

---

## Trail File Archival

Trail files accumulate on disk. The manager purges old ones automatically, but you can archive before purge:

```bash
ggctl archive
```

Configure in `pipeline.yaml`:
```yaml
archive:
  enabled: true
  archive_dir: "/mnt/archive/trails"
  min_age_hours: 24
  s3:
    enabled: true              # archive to S3 instead of local
    bucket: "your-bucket"
    prefix: "gg-trails"
```

---

## Deploying to Multiple EC2 Instances

Each AWS environment (dev, staging, prod) has its own EC2 with its own GoldenGate installation. The deployment is the same code — only `pipeline.yaml` differs.

### Deployment workflow

```
Your laptop                    EC2 (Dev)              EC2 (Prod)
─────────                      ─────────              ──────────
git clone                      scp project/           scp project/
edit inventory                 edit pipeline.yaml     edit pipeline.yaml
                               ./scripts/ec2_setup.sh ./scripts/ec2_setup.sh
                               ggctl generate         ggctl generate
                               ggctl deploy all       ggctl deploy all
```

### Step-by-step for each EC2

```bash
# 1. Copy project to EC2
scp -r oracle-gg-snowflake-pipeline/ ec2-user@dev-ec2:/home/oracle/

# 2. SSH into EC2
ssh ec2-user@dev-ec2

# 3. Run setup
cd oracle-gg-snowflake-pipeline
./scripts/ec2_setup.sh

# 4. Edit config for this environment
vi config/pipeline.yaml
# → Update: gg19c.home, gg21c.home, snowflake.account/database/warehouse

# 5. Copy your table inventory
# (same inventory across envs, or different per env)
cp /path/to/table_inventory.xlsx input/

# 6. Generate + deploy
ggctl generate
ggctl deploy all --dry-run    # preview
ggctl deploy all              # deploy

# 7. Verify
ggctl status
```

### Updating code across EC2s

```bash
# If using git on EC2:
ggctl update    # pulls latest from GitHub

# If using SCP:
# Re-SCP from local → EC2, then:
ggctl generate
ggctl deploy all --dry-run
```

---

## Process Naming Convention

| Group | Extract | Pump | Replicat | Ext Trail | Pump Trail |
|-------|---------|------|----------|-----------|------------|
| 1 | EXT01 | PMP01 | REP01 | e1* | p1* |
| 2 | EXT02 | PMP02 | REP02 | e2* | p2* |
| ... | ... | ... | ... | ... | ... |
| 9 | EXT09 | PMP09 | REP09 | e9* | p9* |
| 10 | EXT10 | PMP10 | REP10 | ea* | pa* |

Supports up to 35 groups (groups 10+ use hex: `ea`, `eb`, ... `ez`).

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Inventory is the single source of truth** | `group_id` + `enabled` columns control all extract/replicat creation |
| **INSERTALLRECORDS + COLMAP** | Append-only staging in Snowflake → downstream MERGE/SCD handles dedup |
| **ADD TRANDATA per table** (not SCHEMATRANDATA) | Precise supplemental logging — no overhead on excluded tables |
| **AES256 encryption end-to-end** | ENCRYPTTRAIL (at rest) + ENCRYPT RMTHOST (in transit) + TLS JDBC (to Snowflake) |
| **PASSTHRU pump** | No transformation in transit — just moves data from 19c trail to 21c trail |
| **No initial/one-time load** | Pure CDC only — initial loads handled separately (e.g., AWS DMS or direct Snowflake COPY) |
| **Checkpoint-based recovery** | GG checkpoints are durable — restart after ABEND picks up exactly where it left off, zero data loss |
| **TOKEN columns** | OP_TYPE, OP_TS, COMMIT_TS written as extra columns for downstream audit |
| **Passwords never in config** | Use env vars, AWS Secrets Manager, or GG credential store |

---

## Troubleshooting

### Extract ABENDED

```bash
# 1. Check what happened
ggctl use 19c
ggctl ggsci
> VIEW REPORT EXT01       # scroll to bottom for error
> INFO EXTRACT EXT01, DETAIL

# 2. Check ggserr.log
tail -200 /u01/.../19c/oggcore_1/ggserr.log | grep -A5 "ERROR"

# 3. Common fixes:
#    OGG-01004: ADD TRANDATA for the table
#    OGG-01028: Table has no PK → add KEYCOLS to extract .prm
#    OGG-02091: DDL change → regenerate + redeploy

# 4. Disable the failing table if needed
ggctl disable -t SCHEMA.TABLE -r "OGG-01004"
ggctl generate
ggctl deploy 19c

# 5. Restart (resumes from checkpoint — no data loss)
ggctl ggsci
> START EXTRACT EXT01
```

### Replicat ABENDED (Snowflake side)

```bash
ggctl use 21c
ggctl ggsci
> VIEW REPORT REP01

# Also check Java handler log:
tail -100 /u01/.../21c/oggbd_1/dirrpt/REP01_javaue.log

# Common issues:
# - Table doesn't exist in Snowflake → CREATE TABLE
# - Column mismatch → ALTER TABLE
# - Connection timeout → check Snowflake credentials
```

### High lag

```bash
ggctl lag

# If extract lag is high:
# - Too many tables per group → reduce to 50-75
# - DDL changes causing slow queries → check Oracle AWR

# If replicat lag is high:
# - Snowflake warehouse too small → increase warehouse size
# - GROUPTRANSOPS too low → increase in pipeline.yaml
```

---

## License

MIT

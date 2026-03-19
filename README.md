# Oracle GoldenGate CDC Pipeline Generator

**Inventory-driven, dual-target CDC pipeline: Oracle RDS → GG 19c → Snowflake + Oracle RDS Target.**

Zero manual GGSCI work. One Excel/CSV inventory controls everything — which tables replicate, where they go (Snowflake, RDS, or both), which columns to exclude, and which tables are enabled or disabled.

All on a single EC2 instance (RHEL 8). Deploy the same code to any environment by editing one config file.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            EC2 Instance (RHEL 8)                                 │
│                                                                                  │
│   table_inventory.xlsx                                                           │
│   (schema, table, group_id, target, enabled)                                     │
│              │                                                                   │
│       ggctl generate                                                             │
│              │                                                                   │
│   ┌──────────┼─────────────────────────────────┐                                 │
│   ▼          ▼                ▼                 ▼                                 │
│  GG 19c   GG 21c          GG 19c            GGSCI                                │
│  EXT/PMP  REP/props       RDB replicat      obeyfiles                            │
│   │          │                │                                                  │
│   ▼          ▼                ▼                                                  │
│  GG 19c Classic            GG 21c BigData                                        │
│  (port 7809)               (port 7999)                                           │
│   │                            │                                                 │
│  Extract ──┬── Pump ────────► Snowflake Replicat ──► Snowflake (JDBC)            │
│  (redo)    │  (PASSTHRU)     (INSERTALLRECORDS)     (append-only CDC)            │
│  AES256    │                                                                     │
│            └── RDS Replicat ──────────────────────► Target Oracle RDS             │
│               (Classic, live mirror)                (standard replication)        │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow (per extract group)

```
Oracle RDS (source)
    │
    ▼
EXT01 (Integrated Extract — captures ALL tables in group)
    │  ENCRYPTTRAIL AES256
    ▼
./dirdat/e1*  (local encrypted trail)
    │
    ├───► PMP01 (Data Pump, PASSTHRU → GG 21c)
    │       │  ENCRYPT RMTHOST AES256
    │       ▼
    │     ./dirdat/p1*  (remote trail on GG 21c)
    │       │
    │       ▼
    │     REP01 (BigData Replicat → Snowflake)
    │       │  INSERTALLRECORDS + COLMAP (OP_TYPE, OP_TS, IS_DELETED)
    │       ▼
    │     Snowflake (TLS/JDBC)
    │       └── Append-only staging → downstream MERGE / SCD Type 2
    │
    └───► RDB01 (Classic Replicat → Target RDS)
            │  Standard MAP (live mirror, not INSERTALLRECORDS)
            │  COLSEXCEPT for PII/PCI columns
            ▼
          Target Oracle RDS
            └── Current-state replica tables
```

**Key:** The extract trail (`e1`) is shared — both the Snowflake pump and the RDS replicat read from it independently. Tables with `target=both` appear in both replicats. Tables with `target=snowflake` only appear in REP01. Tables with `target=rds` only appear in RDB01.

---

## Table of Contents

- [Quick Start](#quick-start)
- [Table Inventory](#table-inventory)
- [Dual-Target Replication](#dual-target-replication)
- [The `ggctl` CLI](#the-ggctl-cli)
- [Auto-Grouping 500+ Tables](#auto-grouping-500-tables)
- [Enable/Disable Tables](#enabledisable-tables)
- [PII/PCI Column Exclusion (COLSEXCEPT)](#piipci-column-exclusion)
- [SCN-Based Disaster Recovery](#scn-based-disaster-recovery)
- [Heartbeat & Lag Monitoring](#heartbeat--lag-monitoring)
- [Monitoring & Alerting](#monitoring--alerting)
- [Trail File Archival](#trail-file-archival)
- [Deploying to Multiple EC2 Instances](#deploying-to-multiple-ec2-instances)
- [Process Naming Convention](#process-naming-convention)
- [Project Structure](#project-structure)
- [Design Decisions](#design-decisions)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites

- EC2 instance running RHEL 8 (or Amazon Linux 2)
- Oracle GoldenGate 19c Classic installed (`/u01/app/oracle/product/19c/oggcore_1`)
- Oracle GoldenGate 21c BigData installed (`/u01/app/oracle/product/21c/oggbd_1`)
- Python 3.9+
- Network access to source Oracle RDS and Snowflake
- GG credential store configured:
  ```
  ggsci> ADD CREDENTIALSTORE
  ggsci> ALTER CREDENTIALSTORE ADD USER ogg_user PASSWORD xxx ALIAS rdsadmin
  ```
- ENCKEYS file provisioned for AES256 trail encryption

### Step 1: Get the code

```bash
# Option A: SCP from your local machine
scp -r oracle-gg-snowflake-pipeline/ ec2-user@your-ec2:/home/oracle/

# Option B: Clone directly on EC2
git clone https://github.com/singh09manish/oracle-gg-snowflake-pipeline.git
cd oracle-gg-snowflake-pipeline
```

### Step 2: Run EC2 setup

```bash
./scripts/ec2_setup.sh
```

Installs Python, creates a virtualenv, installs dependencies, and symlinks `ggctl` for system-wide access.

### Step 3: Configure for your environment

```bash
vi config/pipeline.yaml
```

Key settings to update:
```yaml
source:
  alias: "rdsadmin"                    # your GG credential store alias

gg19c:
  home: /u01/app/oracle/product/19c/oggcore_1

gg21c:
  home: /u01/app/oracle/product/21c/oggbd_1
  snowflake:
    account: "xy12345.us-east-1"
    warehouse: "YOUR_WH"
    database: "YOUR_DB"
    role: "GG_ROLE"
    auth:
      method: "keypair"                # "password" or "keypair"
      private_key_file: "/home/oracle/.snowflake/rsa_key.p8"

# Enable RDS target replication (optional)
target_rds:
  enabled: true                        # set true if you have RDS-targeted tables
  alias: "rds_target"                  # credential store alias for target RDS
```

### Step 4: Place your inventory

```bash
cp /path/to/table_inventory.xlsx input/table_inventory.xlsx
```

### Step 5: Generate + Deploy

```bash
# Generate all configs from inventory
ggctl generate

# Preview deployment
ggctl deploy all --dry-run

# Deploy GG 19c (extract + pump + RDS replicat)
ggctl deploy 19c

# Deploy GG 21c (Snowflake replicat)
ggctl deploy 21c

# Verify
ggctl status
ggctl lag
```

---

## Table Inventory

The Excel/CSV file is the **single source of truth** for the entire pipeline. Every extract, pump, and replicat is generated from it.

### Columns

| Column | Required | Default | Description |
|--------|----------|---------|-------------|
| `schema` | Yes | — | Oracle source schema (`HR`, `SALES`) |
| `table_name` | Yes | — | Oracle table name (`EMPLOYEES`) |
| `group_id` | Yes | — | Extract group (1 → EXT01/PMP01/REP01/RDB01) |
| `target_schema` | No | source schema | Snowflake target schema |
| `target` | No | `snowflake` | `snowflake`, `rds`, or `both` |
| `rds_target_schema` | No | source schema | RDS target schema |
| `enabled` | No | `Y` | `Y`/`N` — disabled tables excluded from all .prm files |
| `disabled_reason` | No | — | Why the table was disabled |
| `disabled_at` | No | — | ISO timestamp of when disabled |
| `excluded_columns` | No | — | Comma-separated columns to exclude (PII/PCI) |

### Example inventory

| schema | table_name | group_id | target_schema | target | rds_target_schema | enabled | excluded_columns |
|--------|-----------|----------|---------------|--------|-------------------|---------|-----------------|
| HR | EMPLOYEES | 1 | HR_PROD | both | HR_MIRROR | Y | SSN,TAX_ID |
| HR | DEPARTMENTS | 1 | HR_PROD | both | HR_MIRROR | Y | |
| HR | JOBS | 1 | HR_PROD | snowflake | | Y | |
| HR | COUNTRIES | 1 | HR_PROD | rds | HR_MIRROR | Y | |
| SALES | ORDERS | 2 | SALES_PROD | snowflake | | Y | |
| SALES | BAD_TABLE | 2 | SALES_PROD | snowflake | | N | |

### What this produces

- **EXT01**: Captures ALL 4 HR tables from Oracle redo logs
- **PMP01**: Pumps trails to GG 21c for Snowflake
- **REP01** (Snowflake): MAP for EMPLOYEES, DEPARTMENTS, JOBS (not COUNTRIES — it's RDS-only)
- **RDB01** (RDS): MAP for EMPLOYEES, DEPARTMENTS, COUNTRIES (not JOBS — it's Snowflake-only)
- **BAD_TABLE**: Excluded from everything (disabled)
- **SSN, TAX_ID**: Excluded via COLSEXCEPT on EMPLOYEES in both targets

### Column aliases

The inventory reader is flexible — these column names all work:

- Schema: `schema`, `schema_name`, `owner`, `source_schema`
- Table: `table_name`, `tablename`, `table`, `name`, `object_name`
- Group: `group_id`, `group`, `extract_group`
- Target: `target`, `target_type`, `replication_target`, `destination`
- RDS schema: `rds_target_schema`, `rds_schema`, `oracle_target_schema`
- Enabled: `enabled`, `active`, `status`
- Excluded cols: `excluded_columns`, `colsexcept`, `pii_columns`

---

## Dual-Target Replication

### How it works

The `target` column in the inventory controls where each table is replicated:

| target value | Snowflake (REP) | RDS (RDB) | Description |
|-------------|-----------------|-----------|-------------|
| `snowflake` | Yes | No | CDC to Snowflake only (default) |
| `rds` | No | Yes | Live mirror to target Oracle RDS only |
| `both` | Yes | Yes | Replicated to both targets simultaneously |

### Architecture per group

For a group with mixed targets:
- **Extract** (EXT01) captures ALL enabled tables regardless of target
- **Pump** (PMP01) is only created if the group has Snowflake-targeted tables
- **Snowflake Replicat** (REP01, GG 21c BigData) — only MAP statements for `snowflake`/`both` tables, using INSERTALLRECORDS + COLMAP
- **RDS Replicat** (RDB01, GG 19c Classic) — only MAP statements for `rds`/`both` tables, using standard replication (live mirror)

### Enabling RDS target

1. Set `target_rds.enabled: true` in `config/pipeline.yaml`
2. Configure the credential store alias:
   ```
   ggsci> ALTER CREDENTIALSTORE ADD USER target_user PASSWORD xxx ALIAS rds_target DOMAIN OracleGoldenGate
   ```
3. Set `target` column in your inventory to `rds` or `both` for applicable tables
4. Run `ggctl generate && ggctl deploy 19c`

### Key differences between targets

| Aspect | Snowflake (REP) | RDS (RDB) |
|--------|----------------|-----------|
| GG version | 21c BigData | 19c Classic |
| Replicat type | Java JDBC handler | Standard SQL apply |
| Mode | INSERTALLRECORDS (append-only CDC) | Standard replication (live mirror) |
| COLMAP | OP_TYPE, OP_TS, IS_DELETED added | No COLMAP — direct column mapping |
| Connection | JDBC URL + properties file | USERIDALIAS (credential store) |
| Trail source | Pump trail (p1*) in GG 21c | Extract trail (e1*) in GG 19c |

---

## The `ggctl` CLI

Single entry point for all pipeline operations. Reads all paths from `config/pipeline.yaml`.

```bash
# Context switching
ggctl use 19c                           # GG 19c (extract/pump/rds replicat)
ggctl use 21c                           # GG 21c (snowflake replicat)
ggctl ggsci                             # Open GGSCI in current context
ggctl info                              # Show config, targets, encryption status

# Pipeline
ggctl generate                          # Generate all .prm from inventory
ggctl deploy 19c [--dry-run]            # Deploy extract + pump + rds replicat
ggctl deploy 21c [--dry-run]            # Deploy snowflake replicat
ggctl deploy all [--dry-run]            # Deploy everything
ggctl ddl [options]                     # Generate Snowflake CREATE TABLE DDL

# Table control
ggctl disable -t SCHEMA.TABLE [-r "reason"]
ggctl enable  -t SCHEMA.TABLE
ggctl tables                            # Show enabled/disabled per group

# Monitoring
ggctl status                            # All processes + lag (both GG instances)
ggctl watch [--auto-disable]            # Real-time ABEND watcher
ggctl health                            # Full health check
ggctl lag                               # Lag only
ggctl diagnose                          # 9-area lag diagnostic
ggctl trails [--once|--watch]           # Trail file size monitor
ggctl graph [--format text|dot|html]    # Pipeline dependency graph

# Reconciliation & drift
ggctl recon [options]                   # Oracle vs Snowflake row counts
ggctl diff [--level deployed|full]      # Config drift detection

# Disaster recovery
ggctl start-from-scn EXT01 12345678    # Restart extract from specific SCN
ggctl heartbeat setup                   # Create heartbeat table in Oracle
ggctl heartbeat status                  # Check heartbeat table

# Recovery & maintenance
ggctl recover                           # Auto-recovery daemon
ggctl schema-check --current-csv FILE   # Detect schema drift + generate ALTERs
ggctl archive                           # Archive old trail files
ggctl update                            # Pull latest code from GitHub
ggctl setup                             # Re-run EC2 setup
```

---

## Auto-Grouping 500+ Tables

When you have hundreds of tables, use multi-dimensional scoring to assign balanced extract groups:

### Step 1: Run the Oracle profiling query

```sql
@sql/table_grouping_candidates.sql
-- Exports: inserts, updates, deletes, avg_row_len, lob_count, partition_count, etc.
```

### Step 2: Auto-assign groups

```bash
python3 scripts/auto_group.py \
    --from-csv /tmp/table_grouping_candidates.csv \
    --schemas HR SALES FINANCE \
    --max 75 --min 50 \
    --output input/table_inventory.xlsx \
    --dry-run
```

### Scoring algorithm

Each table gets a composite **GG extract cost**:
```
cost = byte_throughput
     + (lob_count x 50,000)
     + (partition_count x 5,000)
     + max(col_count - 80, 0) x 200
```

Tables are classified into profiles (OLTP_HEAVY, OLTP_LIGHT, BATCH_HEAVY, BATCH_LIGHT, REFERENCE) and assigned with anti-affinity rules (max 2 BATCH_HEAVY per group, max 5 OLTP_HEAVY per group).

---

## Enable/Disable Tables

```bash
# Disable a failing table
ggctl disable -t HR.EMPLOYEES -r "OGG-01004: no supplemental logging"

# Disable multiple tables
ggctl disable -t HR.EMPLOYEES SALES.ORDERS -r "extract ABEND"

# Check status
ggctl tables

# Re-enable after fix
ggctl enable -t HR.EMPLOYEES

# IMPORTANT: regenerate + redeploy after changes
ggctl generate
ggctl deploy 19c
```

When disabled:
1. Table row turns red in inventory Excel
2. Excluded from all `.prm` files on next `ggctl generate`
3. Other tables in the group are unaffected
4. GG checkpoint-based recovery handles restart — **zero data loss**

---

## PII/PCI Column Exclusion

Use the `excluded_columns` field in the inventory to prevent sensitive columns from being replicated:

```csv
HR,EMPLOYEES,1,HR_PROD,both,HR_MIRROR,Y,,,"SSN,TAX_ID"
```

This generates `COLSEXCEPT` in both Snowflake and RDS replicat MAP statements:
```
MAP HR.EMPLOYEES, TARGET HR_PROD.EMPLOYEES, INSERTALLRECORDS, &
COLSEXCEPT (SSN, TAX_ID), &
COLMAP ( ... );
```

The columns are still captured by the extract (for other potential consumers) but excluded at the replicat level.

---

## SCN-Based Disaster Recovery

### Start extract from a specific SCN

```bash
# Find the recovery SCN
sqlplus> SELECT CURRENT_SCN FROM V$DATABASE;
sqlplus> @sql/get_current_scn.sql

# Restart extract from that SCN (interactive — confirms before executing)
ggctl start-from-scn EXT01 12345678
```

This safely: STOP → DELETE → UNREGISTER → ADD AT SCN → REGISTER → START.

### Configure SCN in pipeline.yaml

```yaml
scn:
  extracts:
    EXT01: 12345678
    EXT02: 12345679
```

Then `ggctl generate && ggctl deploy 19c` — the obeyfiles will use `BEGIN AT SCN` instead of `BEGIN NOW`.

---

## Heartbeat & Lag Monitoring

### Setup

```bash
# Create heartbeat table in Oracle
ggctl heartbeat setup

# Create monitoring tables in Snowflake
snowsql -f output/gg21c/heartbeat/snowflake_heartbeat_ddl.sql
```

### Check lag

```sql
-- In Snowflake: true end-to-end lag
SELECT
    INCOMING_HEARTBEAT_TS,
    CURRENT_TIMESTAMP() AS snowflake_ts,
    TIMESTAMPDIFF('SECOND', INCOMING_HEARTBEAT_TS, CURRENT_TIMESTAMP()) AS lag_seconds
FROM GG_HEARTBEAT
ORDER BY INCOMING_HEARTBEAT_TS DESC LIMIT 1;
```

---

## Monitoring & Alerting

### Quick checks

```bash
ggctl status     # all processes + lag (GG 19c + GG 21c)
ggctl lag         # lag only
ggctl health      # full health check
ggctl diagnose    # 9-area lag diagnostic
```

### Real-time ABEND watcher

```bash
ggctl watch                              # basic: detect + notify
ggctl watch --auto-disable               # auto-exclude failing tables
ggctl watch --auto-disable --auto-regenerate  # full auto-recovery
```

### Configure notifications

```yaml
# config/pipeline.yaml
monitoring:
  alerting:
    slack:
      enabled: true
      webhook_url: "https://hooks.slack.com/services/T00/B00/xxx"
    email:
      enabled: true
      recipients: "oncall@yourcompany.com"
    pagerduty:
      enabled: true
      routing_key: "your-integration-key"
```

### Run as systemd service

```bash
sudo cp scripts/gg-event-watcher.service /etc/systemd/system/
sudo systemctl enable --now gg-event-watcher
```

---

## Trail File Archival

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
    enabled: true
    bucket: "your-bucket"
    prefix: "gg-trails"
```

---

## Deploying to Multiple EC2 Instances

Each environment (dev, staging, prod) gets its own EC2. Same code, different `pipeline.yaml`.

```bash
# 1. Copy to EC2
scp -r oracle-gg-snowflake-pipeline/ ec2-user@prod-ec2:/home/oracle/

# 2. On EC2: setup + configure
./scripts/ec2_setup.sh
vi config/pipeline.yaml   # update GG homes, Snowflake, RDS target

# 3. Generate + deploy
ggctl generate
ggctl deploy all --dry-run
ggctl deploy all
ggctl status
```

---

## Process Naming Convention

| Group | Extract | Pump | Snowflake Rep | RDS Rep | Ext Trail | Pump Trail |
|-------|---------|------|--------------|---------|-----------|------------|
| 1 | EXT01 | PMP01 | REP01 | RDB01 | e1* | p1* |
| 2 | EXT02 | PMP02 | REP02 | RDB02 | e2* | p2* |
| 9 | EXT09 | PMP09 | REP09 | RDB09 | e9* | p9* |
| 10 | EXT10 | PMP10 | REP10 | RDB10 | ea* | pa* |

Supports up to 35 parallel extract chains (groups 10+ use hex: `ea`–`ez`).

RDS replicat (`RDBnn`) reads from the extract trail (`en*`) directly — no separate pump required. The Snowflake replicat (`REPnn`) reads from the pump trail (`pn*`).

---

## Project Structure

```
oracle-gg-snowflake-pipeline/
├── ggctl                              # Unified CLI (bash) — single entry point
├── config/
│   ├── pipeline.yaml                  # Infrastructure: GG homes, Snowflake, RDS target, encryption
│   └── env.example.yaml               # Credential template (Oracle + Snowflake)
├── input/
│   └── table_inventory.xlsx            # YOUR inventory (source of truth)
├── app/
│   ├── main.py                         # CLI: generate, validate, report, disable, enable, status
│   ├── models.py                       # TableInfo, ExtractGroup (target routing, naming)
│   ├── inventory.py                    # Excel/CSV reader/writer, column aliases, toggle
│   ├── grouper.py                      # Groups tables by group_id, filters disabled
│   ├── generator.py                    # Jinja2 renderer → output/ (all .prm, .props, .oby)
│   ├── auto_grouper.py                 # Multi-dimensional scoring for 500+ tables
│   ├── ddl_generator.py                # Oracle → Snowflake DDL translation
│   └── validator.py                    # Oracle pre-flight (PKs, TRANDATA, LOBs)
├── templates/
│   ├── gg19c/
│   │   ├── extract.prm.j2              # Integrated extract (all tables in group)
│   │   ├── pump.prm.j2                 # PASSTHRU pump → GG 21c
│   │   ├── rds_replicat.prm.j2         # Classic replicat → RDS target (live mirror)
│   │   └── mgr.prm.j2                  # GG 19c Manager
│   └── gg21c/
│       ├── replicat.prm.j2             # BigData replicat → Snowflake (INSERTALLRECORDS)
│       ├── snowflake.props.j2           # JDBC handler properties
│       └── mgr.prm.j2                  # GG 21c Manager
├── output/                             # Generated files (recreate with ggctl generate)
│   ├── gg19c/dirprm/                   # EXT01.prm, PMP01.prm, RDB01.prm, mgr.prm
│   ├── gg19c/ggsci/                    # 01_trandata → 07_add_rds_replicats.oby
│   ├── gg21c/dirprm/                   # REP01.prm, REP01.properties, mgr.prm
│   └── gg21c/ggsci/                    # 01_add_replicats → 03_status.oby
├── scripts/
│   ├── ec2_setup.sh                    # One-time EC2 bootstrap
│   ├── deploy_gg19c.sh                 # Deploy extract + pump + RDS replicat
│   ├── deploy_gg21c.sh                 # Deploy Snowflake replicat
│   ├── auto_group.py                   # CLI for auto-grouping
│   ├── event_watcher.py                # Real-time ABEND watcher + notifications
│   ├── auto_recovery.py                # Daemon: detect, disable, regenerate, restart
│   ├── reconcile.py                    # Oracle vs Snowflake row count comparison
│   ├── config_diff.py                  # Config drift detection (3 levels)
│   ├── trail_monitor.py                # Trail size + disk fill prediction
│   ├── schema_evolution.py             # Schema drift → Snowflake ALTER generation
│   ├── dependency_graph.py             # ASCII/DOT/HTML pipeline visualization
│   ├── diagnose_lag.sh                 # 9-area lag diagnostic
│   ├── health_check.sh                 # Process health + disk + lag thresholds
│   ├── archive_trails.sh               # Trail archival (local or S3)
│   └── gg-event-watcher.service        # Systemd unit
├── sql/
│   ├── table_grouping_candidates.sql   # Oracle DML profiling query
│   ├── oracle_table_ddl.sql            # Column metadata for DDL generation
│   └── get_current_scn.sql             # SCN queries for disaster recovery
├── audit/
│   ├── control_totals.py               # Oracle row counts
│   ├── snowflake_totals.py             # Snowflake row counts
│   └── recon.py                        # Reconciliation logic
└── tests/
    └── test_inventory.csv              # Sample: 22 tables, 3 groups, dual-target, disabled, COLSEXCEPT
```

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Inventory is the single source of truth** | `group_id`, `target`, `enabled`, `excluded_columns` — all control comes from one spreadsheet |
| **Dual-target from shared extract** | One extract trail feeds both Snowflake (via pump → BigData replicat) and RDS (via Classic replicat). No data duplication at the source |
| **INSERTALLRECORDS for Snowflake** | Append-only staging preserves full CDC history. Downstream MERGE/SCD handles dedup |
| **Standard replication for RDS** | Live mirror — current-state tables. No CDC metadata columns needed |
| **COLSEXCEPT for PII/PCI** | Sensitive columns excluded at replicat level in both targets. Extract still captures full row for other potential consumers |
| **AES256 encryption end-to-end** | ENCRYPTTRAIL (at rest) + ENCRYPT RMTHOST (in transit) + TLS JDBC (Snowflake) |
| **No second pump for RDS** | RDS replicat reads extract trail directly (local to GG 19c). Simpler, fewer moving parts |
| **ADD TRANDATA per table** (not SCHEMATRANDATA) | Precise supplemental logging — no overhead on non-pipeline tables |
| **PASSTHRU pump** | No transformation in transit — just moves trail data from GG 19c to GG 21c |
| **No initial/one-time load** | Pure CDC only. Initial loads via AWS DMS or Snowflake COPY |
| **Checkpoint-based recovery** | Restart after ABEND picks up exactly where it left off — zero data loss |
| **No secrets in generated files** | USERIDALIAS (credential store) for Oracle. Env vars or keypair for Snowflake |

---

## Troubleshooting

### Extract ABENDED

```bash
ggctl use 19c && ggctl ggsci
> VIEW REPORT EXT01
> INFO EXTRACT EXT01, DETAIL

# Common fixes:
# OGG-01004 → ADD TRANDATA for the table
# OGG-01028 → Table has no PK → add KEYCOLS
# DDL change → ggctl disable -t TABLE && ggctl generate && ggctl deploy 19c
```

### Snowflake Replicat ABENDED

```bash
ggctl use 21c && ggctl ggsci
> VIEW REPORT REP01
# Also: tail /u01/.../21c/oggbd_1/dirrpt/REP01_javaue.log

# Common: table not in Snowflake → run DDL
# Column mismatch → ggctl schema-check
```

### RDS Replicat ABENDED

```bash
ggctl use 19c && ggctl ggsci
> VIEW REPORT RDB01
> INFO REPLICAT RDB01, DETAIL

# Common: target table doesn't exist, column mismatch, PK violation
```

### High lag

```bash
ggctl lag
ggctl diagnose    # full 9-area diagnostic

# Extract lag → too many tables per group, or long-running transactions
# Replicat lag → increase GROUPTRANSOPS, scale Snowflake warehouse
```

---

## License

MIT

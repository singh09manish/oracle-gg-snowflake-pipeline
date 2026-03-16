# Oracle RDS → GoldenGate → Snowflake Pipeline

**Inventory-driven, zero-manual-work CDC pipeline generator.**

Oracle RDS (3 schemas, ~500 tables) → GG 19c Classic (Extract + Pump) → GG BigData 21c (Replicat via JDBC) → Snowflake.  All on a single EC2 (RHEL 8).

## Architecture

```
                 Excel inventory (schema, table_name, group_id)
                           │
                 python3 -m app.main generate
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                  ▼
  GG 19c params      GG 21c params      GGSCI obeyfiles
  EXTnn.prm          REPnn.prm          01_trandata.oby
  PMPnn.prm          REPnn.props        02_add_extracts.oby
  mgr.prm            mgr.prm            ...
         │                 │
         ▼                 ▼
  GG 19c (port 7809)       GG 21c BigData (port 7909)
  /oracle/product/19c/     /oracle/product/21c/oggbd_1
  oggcore_1
         │                        │
  Extract x N  ──► Pump x N  ──► Replicat x N ──► Snowflake
  (redo logs)     (PASSTHRU)     (JDBC handler)    (MERGE/upsert)
  AES256 trail    e1→p1          REP01.props
```

## How it works

1. **You provide an Excel/CSV** with columns: `schema`, `table_name`, `group_id`
2. **The `group_id` column controls** which extract chain each table belongs to (EXT01/PMP01/REP01, etc.)
3. **Generates everything**: extract, pump, replicat, Snowflake handler, GGSCI obeyfiles
4. **Deploy scripts** copy files and run GGSCI — zero manual GGSCI commands

## Repo layout

```
oracle-gg-snowflake-pipeline/
├── input/
│   └── table_inventory.xlsx      ← YOUR Excel (schema, table_name, group_id)
├── config/
│   ├── pipeline.yaml             ← infrastructure settings (ports, paths, Snowflake)
│   └── env.example.yaml          ← credentials template
├── app/
│   ├── main.py                   ← CLI: generate / validate / report
│   ├── inventory.py              ← reads Excel/CSV
│   ├── grouper.py                ← groups tables by inventory group_id
│   ├── generator.py              ← renders Jinja2 templates → output/
│   ├── validator.py              ← Oracle pre-flight (PKs, TRANDATA, LOBs)
│   └── models.py                 ← dataclasses
├── templates/
│   ├── gg19c/                    ← Jinja2 templates for extract/pump/mgr
│   └── gg21c/                    ← Jinja2 templates for replicat/snowflake.props/mgr
├── output/                       ← generated files (gitignored)
│   ├── gg19c/dirprm/             ← mgr.prm, EXT01-N.prm, PMP01-N.prm
│   ├── gg19c/ggsci/              ← obeyfiles 01-05
│   ├── gg21c/dirprm/             ← mgr.prm, REP01-N.prm, REP01-N.props
│   └── gg21c/ggsci/              ← obeyfiles 01-03
├── scripts/
│   ├── deploy_gg19c.sh           ← deploy GG 19c (copy + GGSCI)
│   ├── deploy_gg21c.sh           ← deploy GG 21c (copy + GGSCI)
│   └── health_check.sh           ← unified health check (cron every 5 min)
├── audit/
│   ├── control_totals.py         ← Oracle source row counts
│   ├── snowflake_totals.py       ← Snowflake target row counts
│   └── recon.py                  ← compare source vs target
└── requirements.txt
```

## Quick start

### 1. Install

```bash
pip3 install -r requirements.txt
```

### 2. Configure

```bash
cp config/env.example.yaml config/env.yaml
# Edit: oracle_rds.host, snowflake.account/warehouse/database
# Edit config/pipeline.yaml if you need to change GG paths/ports
```

### 3. Place your inventory

Put your table inventory at `input/table_inventory.xlsx` (or `.csv`) with columns:

| schema | table_name | group_id | target_schema (optional) |
|--------|-----------|----------|--------------------------|
| SCHEMA1 | CUSTOMERS | 1 | |
| SCHEMA1 | ORDERS | 1 | |
| SCHEMA2 | ACCOUNTS | 2 | |
| SCHEMA3 | EMPLOYEES | 3 | |

- `group_id` (required) — integer that maps to EXT01/PMP01/REP01 (group 1), EXT02/PMP02/REP02 (group 2), etc.
- `target_schema` (optional) — Snowflake target schema; defaults to source schema if blank.
- Also supports column aliases: `owner`, `schema_name` for schema; `tablename`, `object_name` for table_name; `group`, `extract_group` for group_id.

### 4. Generate everything

```bash
# Generate all GG param files:
python3 -m app.main generate --input input/table_inventory.xlsx

# See group assignments without writing files:
python3 -m app.main report --input input/table_inventory.xlsx
```

### 5. Validate Oracle readiness (optional)

```bash
# Pre-flight: check PKs, supplemental logging, LOBs
python3 -m app.main validate --input input/table_inventory.xlsx
```

### 6. Deploy

```bash
# Place Snowflake JDBC jar
cp snowflake-jdbc-3.14.4.jar /oracle/product/21c/oggbd_1/dirprm/

# Deploy GG 19c (dry-run first):
source ~/gg_environments.sh 19
./scripts/deploy_gg19c.sh --dry-run
./scripts/deploy_gg19c.sh

# Deploy GG 21c:
source ~/gg_environments.sh 21
SNOWFLAKE_USER=gg_user SNOWFLAKE_PASSWORD=xxx ./scripts/deploy_gg21c.sh --dry-run
SNOWFLAKE_USER=gg_user SNOWFLAKE_PASSWORD=xxx ./scripts/deploy_gg21c.sh
```

### 7. Monitor

```bash
# Health check (add to cron every 5 min):
./scripts/health_check.sh

# Nightly audit:
# 02:00 — Oracle counts
# 03:00 — Snowflake counts
# 04:00 — Reconciliation
```

## Process naming

| Group | Extract | Pump | Replicat | Ext Trail | Pump Trail |
|-------|---------|------|----------|-----------|------------|
| 1 | EXT01 | PMP01 | REP01 | e1* | p1* |
| 2 | EXT02 | PMP02 | REP02 | e2* | p2* |
| ... | ... | ... | ... | ... | ... |
| 9 | EXT09 | PMP09 | REP09 | e9* | p9* |
| 10 | EXT10 | PMP10 | REP10 | ea* | pa* |

## Key design decisions

- **Inventory is the single source of truth** — the `group_id` column controls all extract/replicat creation
- **ADD TRANDATA per table** (not SCHEMATRANDATA) — precise supplemental logging, no overhead on excluded tables
- **ENCRYPTTRAIL AES256** — all trail files encrypted at rest
- **DISCARDFILE** on every extract and replicat — captures failed rows instead of abending
- **GROUPTRANSOPS 5000** — batched Snowflake commits (lower overhead than row-by-row)
- **Auto-tuned SGA** — generator increases max_sga_size based on tables per group
- **Connection resilience** — Snowflake JDBC handler has 30s connect timeout, 300s query timeout

-- =============================================================================
-- table_grouping_candidates.sql
--
-- Purpose : Pull table metadata + DML activity from Oracle so the Python
--           auto-grouper can assign balanced GoldenGate extract group_ids.
--
-- Run as  : A user with SELECT on DBA_TABLES, DBA_TAB_MODIFICATIONS,
--           DBA_SEGMENTS (i.e. the OGG capture user or a DBA).
--
-- Usage   :
--   sqlplus system/pass@rds @sql/table_grouping_candidates.sql
--   -- OR save output to CSV and pass to: python3 scripts/auto_group.py --from-csv
--
-- Output columns:
--   OWNER          - Oracle schema
--   TABLE_NAME     - Table name
--   NUM_ROWS       - Last gathered row count (may be stale)
--   SEGMENT_MB     - Physical segment size in MB
--   INSERTS        - Inserts since last DBMS_STATS gather
--   UPDATES        - Updates since last DBMS_STATS gather (cost x2 in GG)
--   DELETES        - Deletes since last DBMS_STATS gather
--   DML_SCORE      - Weighted DML load score (used by auto-grouper)
--   HAS_PK         - Y/N — tables without PK need KEYCOLS in GG param
--   PARTITIONED    - YES/NO — partitioned tables are heavier for extract
-- =============================================================================

SET LINESIZE 200
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET COLSEP ','
SET TRIMSPOOL ON
SPOOL /tmp/table_grouping_candidates.csv

SELECT
    t.owner                                                               AS owner,
    t.table_name                                                          AS table_name,
    NVL(t.num_rows, 0)                                                   AS num_rows,
    NVL(ROUND(s.bytes / 1048576, 2), 0)                                  AS segment_mb,
    NVL(m.inserts, 0)                                                    AS inserts,
    NVL(m.updates, 0)                                                    AS updates,
    NVL(m.deletes, 0)                                                    AS deletes,
    -- DML score: updates are 2x expensive for GG (before/after image capture)
    NVL(m.inserts, 0) + (NVL(m.updates, 0) * 2) + NVL(m.deletes, 0)    AS dml_score,
    CASE
        WHEN pk.constraint_name IS NOT NULL THEN 'Y'
        ELSE 'N'
    END                                                                   AS has_pk,
    t.partitioned                                                         AS partitioned
FROM
    dba_tables t
    -- DML activity since last statistics gather
    LEFT JOIN dba_tab_modifications m
           ON m.table_owner = t.owner
          AND m.table_name  = t.table_name
          AND m.partition_name IS NULL          -- whole-table rows only
    -- Physical segment size
    LEFT JOIN dba_segments s
           ON s.owner        = t.owner
          AND s.segment_name = t.table_name
          AND s.segment_type = 'TABLE'
    -- Primary key check
    LEFT JOIN (
        SELECT c.owner, c.table_name, c.constraint_name
        FROM   dba_constraints c
        WHERE  c.constraint_type = 'P'
    ) pk ON pk.owner = t.owner AND pk.table_name = t.table_name
WHERE
    t.owner NOT IN (
        -- Exclude Oracle system schemas
        'SYS','SYSTEM','DBSNMP','SYSMAN','OUTLN','MDSYS','ORDSYS','EXFSYS',
        'DMSYS','WMSYS','CTXSYS','ANONYMOUS','XDB','ORDDATA','ORDPLUGINS',
        'SI_INFORMTN_SCHEMA','OLAPSYS','MDDATA','APEX_030200','APEX_PUBLIC_USER',
        'FLOWS_FILES','OWBSYS','OWBSYS_AUDIT','AURORA$ORB$UNAUTHENTICATED',
        'LBACSYS','OJVMSYS','GSMADMIN_INTERNAL','APPQOSSYS','GSMCATUSER',
        'GSMUSER','DIP','AUDSYS','RDSADMIN'
    )
    AND t.temporary  = 'N'           -- exclude temporary tables
    AND t.iot_type  IS NULL          -- exclude IOT tables (GG limitation)
    AND t.nested     = 'NO'          -- exclude nested tables
    -- Uncomment and edit to filter to specific schemas:
    -- AND t.owner IN ('HR', 'SALES', 'FINANCE', 'ORDERS')
ORDER BY
    t.owner,
    dml_score DESC,
    t.num_rows DESC,
    t.table_name
;

SPOOL OFF

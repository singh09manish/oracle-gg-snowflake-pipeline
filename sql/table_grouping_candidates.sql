-- =============================================================================
-- table_grouping_candidates.sql
--
-- Purpose : Pull table metadata + DML activity + row width + column count
--           from Oracle so the Python auto-grouper can assign balanced
--           GoldenGate extract group_ids using multi-dimensional scoring.
--
-- Scoring dimensions captured:
--   1. DML volume        — raw inserts/updates/deletes since last stats gather
--   2. Row width         — avg_row_len bytes (wide rows = more redo per DML)
--   3. Byte throughput   — estimated bytes/sec flowing through GG extract
--   4. Churn rate        — DML / num_rows (OLTP vs. batch pattern detection)
--   5. Column count      — more columns = heavier COLMAP in replicat
--   6. LOB presence      — LOB columns cause extra redo + GG overhead
--   7. Partition count   — partitioned tables generate more redo entries
--
-- Run as  : A user with SELECT on DBA_TABLES, DBA_TAB_MODIFICATIONS,
--           DBA_SEGMENTS, DBA_TAB_COLUMNS, DBA_CONSTRAINTS, DBA_TAB_PARTITIONS.
--
-- Usage   :
--   sqlplus system/pass@rds @sql/table_grouping_candidates.sql
--   -- Exports → /tmp/table_grouping_candidates.csv
--   -- Then:  python3 scripts/auto_group.py --from-csv /tmp/table_grouping_candidates.csv
-- =============================================================================

SET LINESIZE 300
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET COLSEP ','
SET TRIMSPOOL ON
SPOOL /tmp/table_grouping_candidates.csv

SELECT
    t.owner                                                              AS owner,
    t.table_name                                                         AS table_name,
    NVL(t.num_rows, 0)                                                   AS num_rows,
    NVL(t.avg_row_len, 100)                                              AS avg_row_len,
    NVL(ROUND(s.bytes / 1048576, 2), 0)                                  AS segment_mb,

    -- Raw DML counts (since last DBMS_STATS gather)
    NVL(m.inserts, 0)                                                    AS inserts,
    NVL(m.updates, 0)                                                    AS updates,
    NVL(m.deletes, 0)                                                    AS deletes,

    -- Total DML volume
    NVL(m.inserts, 0) + NVL(m.updates, 0) + NVL(m.deletes, 0)           AS dml_total,

    -- GG-weighted DML cost (updates 2x: before+after image capture)
    NVL(m.inserts, 0) + (NVL(m.updates, 0) * 2) + NVL(m.deletes, 0)    AS gg_dml_cost,

    -- Byte throughput estimate (DML * avg_row_len)
    -- This is what actually flows through the extract → trail → replicat
    ROUND(
        (NVL(m.inserts, 0) * NVL(t.avg_row_len, 100))                   -- inserts: full row
      + (NVL(m.updates, 0) * NVL(t.avg_row_len, 100) * 2)               -- updates: before + after
      + (NVL(m.deletes, 0) * 64)                                         -- deletes: just the key
    )                                                                     AS byte_throughput,

    -- Churn rate: DML / num_rows (how many times each row changes)
    -- High churn (>1.0) = OLTP (rows updated repeatedly)
    -- Low churn (<0.01) = BATCH (bulk inserts, rarely touched)
    ROUND(
        CASE WHEN NVL(t.num_rows, 0) > 0
             THEN (NVL(m.inserts, 0) + NVL(m.updates, 0) + NVL(m.deletes, 0))
                  / t.num_rows
             ELSE 0
        END, 4
    )                                                                     AS churn_rate,

    -- Update ratio: what % of DML is updates (expensive for GG)
    ROUND(
        CASE WHEN (NVL(m.inserts, 0) + NVL(m.updates, 0) + NVL(m.deletes, 0)) > 0
             THEN NVL(m.updates, 0) * 100.0
                  / (NVL(m.inserts, 0) + NVL(m.updates, 0) + NVL(m.deletes, 0))
             ELSE 0
        END, 1
    )                                                                     AS update_pct,

    -- Column count (more columns = heavier COLMAP in replicat)
    NVL(cols.col_count, 0)                                               AS col_count,

    -- LOB column count (LOBs cause extra redo + GG overhead)
    NVL(cols.lob_count, 0)                                               AS lob_count,

    -- Partition count (0 = not partitioned)
    NVL(parts.part_count, 0)                                             AS partition_count,

    -- Primary key check
    CASE
        WHEN pk.constraint_name IS NOT NULL THEN 'Y'
        ELSE 'N'
    END                                                                   AS has_pk,

    t.partitioned                                                         AS partitioned,

    -- Days since last stats gather (staleness indicator)
    ROUND(SYSDATE - NVL(t.last_analyzed, SYSDATE - 365))                 AS days_since_analyze

FROM
    dba_tables t

    -- DML activity since last statistics gather
    LEFT JOIN dba_tab_modifications m
           ON m.table_owner = t.owner
          AND m.table_name  = t.table_name
          AND m.partition_name IS NULL

    -- Physical segment size
    LEFT JOIN dba_segments s
           ON s.owner        = t.owner
          AND s.segment_name = t.table_name
          AND s.segment_type = 'TABLE'

    -- Column counts (total + LOB)
    LEFT JOIN (
        SELECT
            owner,
            table_name,
            COUNT(*)                                                      AS col_count,
            SUM(CASE WHEN data_type IN ('BLOB','CLOB','NCLOB','LONG','LONG RAW')
                     THEN 1 ELSE 0 END)                                   AS lob_count
        FROM   dba_tab_columns
        GROUP BY owner, table_name
    ) cols ON cols.owner = t.owner AND cols.table_name = t.table_name

    -- Partition count
    LEFT JOIN (
        SELECT table_owner, table_name, COUNT(*) AS part_count
        FROM   dba_tab_partitions
        GROUP BY table_owner, table_name
    ) parts ON parts.table_owner = t.owner AND parts.table_name = t.table_name

    -- Primary key check
    LEFT JOIN (
        SELECT c.owner, c.table_name, c.constraint_name
        FROM   dba_constraints c
        WHERE  c.constraint_type = 'P'
    ) pk ON pk.owner = t.owner AND pk.table_name = t.table_name

WHERE
    t.owner NOT IN (
        'SYS','SYSTEM','DBSNMP','SYSMAN','OUTLN','MDSYS','ORDSYS','EXFSYS',
        'DMSYS','WMSYS','CTXSYS','ANONYMOUS','XDB','ORDDATA','ORDPLUGINS',
        'SI_INFORMTN_SCHEMA','OLAPSYS','MDDATA','APEX_030200','APEX_PUBLIC_USER',
        'FLOWS_FILES','OWBSYS','OWBSYS_AUDIT','AURORA$ORB$UNAUTHENTICATED',
        'LBACSYS','OJVMSYS','GSMADMIN_INTERNAL','APPQOSSYS','GSMCATUSER',
        'GSMUSER','DIP','AUDSYS','RDSADMIN'
    )
    AND t.temporary  = 'N'
    AND t.iot_type  IS NULL
    AND t.nested     = 'NO'
    -- Uncomment and edit to filter to specific schemas:
    -- AND t.owner IN ('HR', 'SALES', 'FINANCE', 'ORDERS')
ORDER BY
    t.owner,
    byte_throughput DESC,
    t.num_rows DESC,
    t.table_name
;

SPOOL OFF

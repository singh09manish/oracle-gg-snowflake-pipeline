-- =============================================================================
-- oracle_row_counts.sql
--
-- Purpose:  Count rows per table across all configured schemas in Oracle.
--           Used by the reconciliation dashboard (ggctl recon) to compare
--           Oracle source counts against Snowflake target counts.
--
-- Output:   schema, table_name, row_count, method, elapsed_seconds
--
-- Methods:
--   EXACT    — SELECT COUNT(*) (default, most accurate)
--   SAMPLE   — SELECT COUNT(*) FROM table SAMPLE(n) * scale_factor
--              (faster for very large tables, approximate)
--   STATS    — DBA_TABLES.NUM_ROWS (fastest, but may be stale)
--
-- Usage:
--   -- Exact counts for all tables in specified schemas:
--   sqlplus user/pass@host:1521/service @sql/oracle_row_counts.sql
--
--   -- For offline reconciliation, spool to CSV:
--   SET COLSEP ','
--   SPOOL /tmp/oracle_row_counts.csv
--   @sql/oracle_row_counts.sql
--   SPOOL OFF
--
-- Customize:
--   - Edit the schema filter in the WHERE clause below
--   - For large tables (>100M rows), switch to SAMPLE method by uncommenting
--     the alternate query at the bottom of this file
--
-- Run as:  A user with SELECT ANY TABLE privilege, or SELECT on the
--          specific schemas being replicated.
-- =============================================================================

SET LINESIZE 200
SET PAGESIZE 50000
SET FEEDBACK OFF
SET HEADING ON
SET COLSEP ','
SET TRIMSPOOL ON
SET TIMING OFF
SET SERVEROUTPUT ON SIZE UNLIMITED

-- ---------------------------------------------------------------------------
-- Option 1: Exact counts (default — most accurate for reconciliation)
-- ---------------------------------------------------------------------------
-- This uses a PL/SQL block to iterate over tables and count rows dynamically.
-- It handles quoted identifiers and reports timing per table.

DECLARE
    v_count     NUMBER;
    v_start     TIMESTAMP;
    v_elapsed   NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('schema,table_name,row_count,method,elapsed_seconds');

    FOR rec IN (
        SELECT t.owner, t.table_name
        FROM   dba_tables t
        WHERE  t.owner NOT IN (
            'SYS','SYSTEM','DBSNMP','SYSMAN','OUTLN','MDSYS','ORDSYS','EXFSYS',
            'DMSYS','WMSYS','CTXSYS','ANONYMOUS','XDB','ORDDATA','ORDPLUGINS',
            'SI_INFORMTN_SCHEMA','OLAPSYS','MDDATA','APEX_030200',
            'APEX_PUBLIC_USER','FLOWS_FILES','OWBSYS','OWBSYS_AUDIT',
            'LBACSYS','OJVMSYS','GSMADMIN_INTERNAL','APPQOSSYS','GSMCATUSER',
            'GSMUSER','DIP','AUDSYS','RDSADMIN'
        )
        AND t.temporary = 'N'
        AND t.iot_type IS NULL
        AND t.nested   = 'NO'
        -- Uncomment and edit to filter to specific schemas:
        -- AND t.owner IN ('HR', 'SALES', 'FINANCE', 'ORDERS')
        ORDER BY t.owner, t.table_name
    )
    LOOP
        v_start := SYSTIMESTAMP;
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM "' || rec.owner || '"."' || rec.table_name || '"'
                INTO v_count;
            v_elapsed := EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start));
            DBMS_OUTPUT.PUT_LINE(
                rec.owner || ',' ||
                rec.table_name || ',' ||
                v_count || ',' ||
                'EXACT,' ||
                ROUND(v_elapsed, 2)
            );
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE(
                    rec.owner || ',' ||
                    rec.table_name || ',' ||
                    '-1,' ||
                    'ERROR: ' || REPLACE(SQLERRM, ',', ';') || ',' ||
                    '0'
                );
        END;
    END LOOP;
END;
/

-- ---------------------------------------------------------------------------
-- Option 2: SAMPLE-based approximate counts (for very large tables)
-- ---------------------------------------------------------------------------
-- Uncomment this block if you need faster counts for tables with >100M rows.
-- SAMPLE(1) samples ~1% of blocks and scales up. Accuracy is typically within 5%.
--
-- DECLARE
--     v_count     NUMBER;
--     v_num_rows  NUMBER;
-- BEGIN
--     DBMS_OUTPUT.PUT_LINE('schema,table_name,row_count,method,sample_pct');
--     FOR rec IN (
--         SELECT t.owner, t.table_name, NVL(t.num_rows, 0) AS num_rows
--         FROM   dba_tables t
--         WHERE  t.owner IN ('HR', 'SALES', 'FINANCE')
--           AND  t.temporary = 'N'
--         ORDER BY t.owner, t.table_name
--     )
--     LOOP
--         IF rec.num_rows > 100000000 THEN
--             -- Large table: use 1% SAMPLE and scale
--             EXECUTE IMMEDIATE
--                 'SELECT COUNT(*) * 100 FROM "' || rec.owner || '"."' ||
--                 rec.table_name || '" SAMPLE(1)'
--                 INTO v_count;
--             DBMS_OUTPUT.PUT_LINE(
--                 rec.owner || ',' || rec.table_name || ',' ||
--                 v_count || ',SAMPLE,1'
--             );
--         ELSE
--             -- Normal table: exact count
--             EXECUTE IMMEDIATE
--                 'SELECT COUNT(*) FROM "' || rec.owner || '"."' ||
--                 rec.table_name || '"'
--                 INTO v_count;
--             DBMS_OUTPUT.PUT_LINE(
--                 rec.owner || ',' || rec.table_name || ',' ||
--                 v_count || ',EXACT,100'
--             );
--         END IF;
--     END LOOP;
-- END;
-- /

-- ---------------------------------------------------------------------------
-- Option 3: Statistics-based counts (fastest, but may be stale)
-- ---------------------------------------------------------------------------
-- Uses DBA_TABLES.NUM_ROWS which reflects the last DBMS_STATS gather.
-- Good for a quick ballpark but not accurate enough for reconciliation.
--
-- SELECT
--     owner          AS schema,
--     table_name,
--     NVL(num_rows, 0) AS row_count,
--     'STATS'        AS method,
--     ROUND(SYSDATE - NVL(last_analyzed, SYSDATE - 365)) AS days_stale
-- FROM   dba_tables
-- WHERE  owner IN ('HR', 'SALES', 'FINANCE')
--   AND  temporary = 'N'
-- ORDER BY owner, table_name;

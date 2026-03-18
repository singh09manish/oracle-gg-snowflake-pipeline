-- =============================================================================
-- oracle_table_ddl.sql
--
-- Purpose : Pull column-level metadata from Oracle for generating Snowflake
--           CREATE TABLE DDL statements.
--
-- Columns returned:
--   owner, table_name, column_name, data_type, data_length,
--   data_precision, data_scale, nullable, column_id
--
-- Run as  : A user with SELECT on DBA_TAB_COLUMNS, DBA_TABLES.
--
-- Usage   :
--   sqlplus system/pass@rds @sql/oracle_table_ddl.sql
--   -- Exports → /tmp/oracle_columns.csv
--   -- Then:  python3 scripts/generate_snowflake_ddl.py --from-csv /tmp/oracle_columns.csv
-- =============================================================================

SET LINESIZE 500
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING ON
SET COLSEP ','
SET TRIMSPOOL ON
SPOOL /tmp/oracle_columns.csv

SELECT
    c.owner                                                AS owner,
    c.table_name                                           AS table_name,
    c.column_name                                          AS column_name,
    c.data_type                                            AS data_type,
    NVL(c.data_length, 0)                                  AS data_length,
    c.data_precision                                       AS data_precision,
    c.data_scale                                           AS data_scale,
    c.nullable                                             AS nullable,
    c.column_id                                            AS column_id
FROM
    dba_tab_columns c
    INNER JOIN dba_tables t
           ON t.owner      = c.owner
          AND t.table_name = c.table_name
WHERE
    c.owner NOT IN (
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
    -- AND c.owner IN ('HR', 'SALES', 'FINANCE', 'ORDERS')
ORDER BY
    c.owner,
    c.table_name,
    c.column_id
;

SPOOL OFF

-- =============================================================================
-- snowflake_row_counts.sql
--
-- Purpose:  Count rows per table in Snowflake for reconciliation against
--           Oracle source counts (used by ggctl recon).
--
-- Context:  GoldenGate BigData replicat uses INSERTALLRECORDS mode, meaning
--           every DML operation (INSERT, UPDATE, DELETE) is appended as a new
--           row. Snowflake tables include metadata columns:
--             - OP_TYPE       : I (insert), U (update), D (delete)
--             - OP_TS         : operation timestamp from Oracle redo
--             - IS_DELETED    : 0 (active) or 1 (soft-deleted by GG)
--             - CURRENT_TS    : when the row was loaded into Snowflake
--
-- Output:   schema, table_name, total_count, active_count, deleted_count,
--           insert_count, update_count, delete_count
--
-- Usage:
--   -- Run in Snowflake worksheet or SnowSQL:
--   snowsql -a <account> -u <user> -d <database> -w <warehouse> \
--       -f sql/snowflake_row_counts.sql -o output_format=csv > snowflake_counts.csv
--
--   -- Or for offline reconciliation:
--   snowsql -a <account> -u <user> -d <database> -w <warehouse> \
--       -f sql/snowflake_row_counts.sql -o output_format=csv \
--       -o header=true > /tmp/snowflake_row_counts.csv
--
-- Customize:
--   - Edit the schema filter in the WHERE clause
--   - Add the database name prefix if querying cross-database
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Query 1: Row counts per table (total, active, deleted)
-- ---------------------------------------------------------------------------
-- This is the primary query for reconciliation. It counts:
--   total_count   : all rows in the table (every CDC operation)
--   active_count  : rows where IS_DELETED = 0 (current active state)
--   deleted_count : rows where IS_DELETED = 1 (soft-deleted)
--
-- For reconciliation against Oracle:
--   Oracle COUNT(*) should match Snowflake active_count
--   (assuming Oracle shows current state and GG has caught up)

SELECT
    TABLE_SCHEMA                                          AS schema,
    TABLE_NAME                                            AS table_name,
    ROW_COUNT                                             AS total_count
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_TYPE = 'BASE TABLE'
    AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    -- Uncomment and edit to filter to specific schemas:
    -- AND TABLE_SCHEMA IN ('HR', 'SALES', 'FINANCE', 'ORDERS')
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME
;


-- ---------------------------------------------------------------------------
-- Query 2: Detailed counts with IS_DELETED breakdown
-- ---------------------------------------------------------------------------
-- Run this per-table or generate dynamically for tables that have IS_DELETED.
-- Since Snowflake INFORMATION_SCHEMA does not track IS_DELETED,
-- this requires querying each table individually.
--
-- Example for a single table:
--
-- SELECT
--     'HR'                                                AS schema,
--     'EMPLOYEES'                                         AS table_name,
--     COUNT(*)                                            AS total_count,
--     COUNT(CASE WHEN "IS_DELETED" = 0 THEN 1 END)       AS active_count,
--     COUNT(CASE WHEN "IS_DELETED" = 1 THEN 1 END)       AS deleted_count,
--     COUNT(CASE WHEN "OP_TYPE" = 'I' THEN 1 END)        AS insert_count,
--     COUNT(CASE WHEN "OP_TYPE" = 'U' THEN 1 END)        AS update_count,
--     COUNT(CASE WHEN "OP_TYPE" = 'D' THEN 1 END)        AS delete_count
-- FROM "HR"."EMPLOYEES"
-- ;


-- ---------------------------------------------------------------------------
-- Query 3: Generate dynamic SQL for all tables with IS_DELETED
-- ---------------------------------------------------------------------------
-- This generates UNION ALL statements for all tables in specified schemas.
-- Copy the output and run it as a separate query.
--
-- SELECT
--     'SELECT ''' || TABLE_SCHEMA || ''' AS schema, ''' ||
--     TABLE_NAME || ''' AS table_name, ' ||
--     'COUNT(*) AS total_count, ' ||
--     'COUNT(CASE WHEN "IS_DELETED" = 0 THEN 1 END) AS active_count, ' ||
--     'COUNT(CASE WHEN "IS_DELETED" = 1 THEN 1 END) AS deleted_count ' ||
--     'FROM "' || TABLE_SCHEMA || '"."' || TABLE_NAME || '" UNION ALL'
--     AS generated_sql
-- FROM INFORMATION_SCHEMA.TABLES
-- WHERE TABLE_TYPE = 'BASE TABLE'
--   AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
-- ORDER BY TABLE_SCHEMA, TABLE_NAME
-- ;
-- -- NOTE: Remove the trailing "UNION ALL" from the last row before running.

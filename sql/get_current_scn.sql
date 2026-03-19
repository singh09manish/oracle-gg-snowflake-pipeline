-- =============================================================================
-- get_current_scn.sql
-- 
-- Run in Oracle to get the current System Change Number (SCN).
-- Use this SCN to start/restart an extract from a known point.
-- =============================================================================

-- Current SCN
SELECT CURRENT_SCN FROM V$DATABASE;

-- Find SCN at a specific point in time (useful for recovery)
-- Replace the timestamp with your desired recovery point:
SELECT TIMESTAMP_TO_SCN(TO_TIMESTAMP('2026-03-18 10:00:00', 'YYYY-MM-DD HH24:MI:SS'))
       AS recovery_scn
FROM DUAL;

-- Find SCN from GG extract checkpoint (last processed position)
-- Run in GGSCI: INFO EXTRACT EXTnn, SHOWCH

-- LogMiner checkpoint SCN (what the extract has read up to)
SELECT * FROM DBA_LOGMNR_SESSION WHERE SESSION_NAME LIKE 'OGG%';

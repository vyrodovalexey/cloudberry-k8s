-- =============================================================================
-- TASK-U019 text-format shape probes (run WITHOUT ON_ERROR_STOP — probes must
-- tolerate failure; every outcome is evidence).
-- =============================================================================
-- Probes, for s3:text and hdfs:text:
--   P1 read-textfmt : read the FORMAT 'TEXT' (delimiter ',')-written data with
--                     FORMAT 'TEXT' (delimiter ',') — the 2.1.0 phase7f shape.
--   P2 bridge-write : WRITE through FORMAT 'CUSTOM' (FORMATTER=
--                     'pxfwritable_export') — the clause shape the operator's
--                     export path emits for ALL profiles (TASK-U027 open
--                     question D-1d: does a writable TEXT export round-trip
--                     live?). Writes to a separate *_bridge path, then reads
--                     it back with FORMAT 'CSV' and records raw byte shape.
--
-- Companion to phase7f_external_tables_u019.sql (which proves the 7 main
-- round-trips). Run after it:
--   psql -U gpadmin -d postgres -v rows=175000 -f this.sql
-- =============================================================================

\timing on

\if :{?rows}
\else
  \set rows 175000
\endif

SET search_path TO u019_ext, public;

-- ---------------------------------------------------------------------------
-- P1 (s3:text): read TEXT-delimiter-written data using FORMAT 'TEXT'
-- ---------------------------------------------------------------------------
\echo '>>> P1a s3:text read via FORMAT ''TEXT'' (delimiter '',''):'
DROP FOREIGN TABLE IF EXISTS u019_ext.rext_s3_text_textfmt;
CREATE EXTERNAL TABLE u019_ext.rext_s3_text_textfmt (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_text?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'TEXT' (delimiter ',');
SELECT count(*) AS rows_via_textfmt, sum(id) AS sum_id FROM u019_ext.rext_s3_text_textfmt;

\echo '>>> P1b hdfs:text read via FORMAT ''TEXT'' (delimiter '',''):'
DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_text_textfmt;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_text_textfmt (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_text?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'TEXT' (delimiter ',');
SELECT count(*) AS rows_via_textfmt, sum(id) AS sum_id FROM u019_ext.rext_hdfs_text_textfmt;

-- ---------------------------------------------------------------------------
-- P2 (s3:text): bridge-formatter WRITE (pxfwritable_export) — D-1d probe
-- ---------------------------------------------------------------------------
\echo '>>> P2a s3:text WRITABLE via pxfwritable_export (bridge) — write:'
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_s3_text_bridge;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_s3_text_bridge (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_text_bridge?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_s3_text_bridge SELECT * FROM u019_ext.staging;

\echo '>>> P2a s3:text bridge — read back with FORMAT ''CSV'':'
DROP FOREIGN TABLE IF EXISTS u019_ext.rext_s3_text_bridge;
CREATE EXTERNAL TABLE u019_ext.rext_s3_text_bridge (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_text_bridge?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'CSV';
SELECT count(*) AS rows_bridge_readback, sum(id) AS sum_id FROM u019_ext.rext_s3_text_bridge;

-- ---------------------------------------------------------------------------
-- P2 (hdfs:text): bridge-formatter WRITE (pxfwritable_export) — D-1d probe
-- ---------------------------------------------------------------------------
\echo '>>> P2b hdfs:text WRITABLE via pxfwritable_export (bridge) — write:'
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_hdfs_text_bridge;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_hdfs_text_bridge (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_text_bridge?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_hdfs_text_bridge SELECT * FROM u019_ext.staging;

\echo '>>> P2b hdfs:text bridge — read back with FORMAT ''CSV'':'
DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_text_bridge;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_text_bridge (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_text_bridge?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'CSV';
SELECT count(*) AS rows_bridge_readback, sum(id) AS sum_id FROM u019_ext.rext_hdfs_text_bridge;

\echo '>>> TASK-U019 text probes complete (inspect object bytes separately for bridge paths).'

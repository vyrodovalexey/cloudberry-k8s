-- =============================================================================
-- TASK-U019 (phase 7f convention) — 7 external WRITABLE tables round-trip
-- =============================================================================
-- For each of the 7 sources below, in schema u019_ext of the target DB:
--   1. (Re)builds a ~10MB staging table u019_ext.staging (deterministic).
--   2. CREATE WRITABLE EXTERNAL TABLE wext_<src> over a fresh u019 path.
--   3. INSERTs the staging table through it (writes ~10MB via PXF).
--   4. CREATE EXTERNAL TABLE rext_<src> over the SAME location, reads it back
--      into u019_ext.readback, and proves FULL fidelity with EXCEPT ALL in
--      both directions (missing/extra must be 0), plus aggregate fingerprints
--      and boundary spot rows.
--   5. Records one row per source in u019_ext.u019_results.
--
-- Sources (PROFILE / SERVER / backend path):
--   s3:text             s3-datalake     cloudberry-data/u019/s3_text
--   s3:parquet          s3-datalake     cloudberry-data/u019/s3_parquet
--   s3:avro             s3-datalake     cloudberry-data/u019/s3_avro
--   hdfs:text           hadoop-cluster  /u019/hdfs_text
--   hdfs:parquet        hadoop-cluster  /u019/hdfs_parquet
--   hdfs:avro           hadoop-cluster  /u019/hdfs_avro
--   hdfs:SequenceFile   hadoop-cluster  /u019/hdfs_sequencefile (DATA-SCHEMA=
--                       Phase7fRecord — test/cases/Phase7fRecord.java must be
--                       deployed to /pxf-base/lib on every segment-primary PXF
--                       sidecar and PXF restarted; see phase7f-external-tables.sh)
--
-- U019-specific deltas vs phase7f_external_tables.sql (2.1.0-verified fixture):
--   * Dedicated schema u019_ext (does not touch mydb's U017 tables, nor
--     postgres.public.pxf_loaded).
--   * READABLE text tables use FORMAT 'CSV' — the EXACT clause shape the
--     operator's fixed PXF read path emits on 2.2.0 (builder.PXFReadFormatClause,
--     TASK-U027 DEF-1). Running this script is the first LIVE confirmation of
--     that DDL shape against s3:text / hdfs:text data on Cloudberry 2.2.0.
--   * Full-fidelity EXCEPT ALL checks (both directions), aggregate
--     fingerprints and boundary spot rows — not just count(*).
--   * Write-side format quirks and the bridge-formatter text probes live in
--     phase7f_u019_text_probe.sql (run separately, tolerates errors).
--
-- Run inside the coordinator pod (postgres DB has the pxf 2.2 extension):
--   psql -v ON_ERROR_STOP=1 -U gpadmin -d postgres -v rows=175000 -f this.sql
-- (~10MB text / source at 175k rows; parquet/avro/sequencefile compress.)
-- =============================================================================

\set ON_ERROR_STOP on
\timing on

\if :{?rows}
\else
  \set rows 175000
\endif
\set mid :rows / 2

CREATE SCHEMA IF NOT EXISTS u019_ext;
SET search_path TO u019_ext, public;

-- ---------------------------------------------------------------------------
-- Staging table (deterministic — same shape as phase7f so Phase7fRecord maps).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS u019_ext.staging CASCADE;
CREATE TABLE u019_ext.staging (
    id      int             NOT NULL,
    name    text            NOT NULL,
    amount  bigint          NOT NULL,
    price   float8          NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO u019_ext.staging (id, name, amount, price)
SELECT g,
       'customer_' || md5(g::text),            -- ~40 chars
       (g % 1000000)::bigint,
       ((g % 100000)::float8) / 100.0
FROM generate_series(1, :rows) AS g;

ANALYZE u019_ext.staging;

DROP TABLE IF EXISTS u019_ext.readback CASCADE;
CREATE TABLE u019_ext.readback (LIKE u019_ext.staging) DISTRIBUTED BY (id);

DROP TABLE IF EXISTS u019_ext.u019_results;
CREATE TABLE u019_ext.u019_results (
    source text, kind text,
    rows_written bigint, rows_read bigint, missing bigint, extra bigint
) DISTRIBUTED BY (source);

\echo '>>> staging rows / heap size:'
SELECT count(*) AS staging_rows FROM u019_ext.staging;
SELECT pg_size_pretty(pg_total_relation_size('u019_ext.staging')) AS staging_size;

-- ===========================================================================
-- 1) s3:text  (writable FORMAT 'TEXT' delimiter ','; readable FORMAT 'CSV'
--    — DEF-1 fixed clause shape; phase7f convention on the write side)
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_s3_text;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_s3_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_text?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'TEXT' (delimiter ',');
INSERT INTO u019_ext.wext_s3_text SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_s3_text;
CREATE EXTERNAL TABLE u019_ext.rext_s3_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_text?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'CSV';
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_s3_text;

INSERT INTO u019_ext.u019_results
SELECT 's3:text', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> s3:text fingerprints (write-side staging vs read-back):'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 's3:text', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> s3:text boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_s3_text WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging   WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 2) s3:parquet
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_s3_parquet;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_s3_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_parquet?PROFILE=s3:parquet&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_s3_parquet SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_s3_parquet;
CREATE EXTERNAL TABLE u019_ext.rext_s3_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_parquet?PROFILE=s3:parquet&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_s3_parquet;

INSERT INTO u019_ext.u019_results
SELECT 's3:parquet', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> s3:parquet fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 's3:parquet', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> s3:parquet boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_s3_parquet WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging     WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 3) s3:avro
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_s3_avro;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_s3_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_avro?PROFILE=s3:avro&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_s3_avro SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_s3_avro;
CREATE EXTERNAL TABLE u019_ext.rext_s3_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/u019/s3_avro?PROFILE=s3:avro&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_s3_avro;

INSERT INTO u019_ext.u019_results
SELECT 's3:avro', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> s3:avro fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 's3:avro', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> s3:avro boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_s3_avro WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging    WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 4) hdfs:text (readable FORMAT 'CSV' — DEF-1 fixed clause shape)
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_hdfs_text;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_hdfs_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_text?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'TEXT' (delimiter ',');
INSERT INTO u019_ext.wext_hdfs_text SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_text;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_text?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'CSV';
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_hdfs_text;

INSERT INTO u019_ext.u019_results
SELECT 'hdfs:text', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> hdfs:text fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 'hdfs:text', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> hdfs:text boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_hdfs_text WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging       WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 5) hdfs:parquet
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_hdfs_parquet;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_hdfs_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_parquet?PROFILE=hdfs:parquet&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_hdfs_parquet SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_parquet;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_parquet?PROFILE=hdfs:parquet&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_hdfs_parquet;

INSERT INTO u019_ext.u019_results
SELECT 'hdfs:parquet', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> hdfs:parquet fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 'hdfs:parquet', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> hdfs:parquet boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_hdfs_parquet WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging          WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 6) hdfs:avro
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_hdfs_avro;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_hdfs_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_avro?PROFILE=hdfs:avro&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_hdfs_avro SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_avro;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_avro?PROFILE=hdfs:avro&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_hdfs_avro;

INSERT INTO u019_ext.u019_results
SELECT 'hdfs:avro', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> hdfs:avro fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 'hdfs:avro', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> hdfs:avro boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_hdfs_avro WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging        WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- 7) hdfs:SequenceFile (custom Writable DATA-SCHEMA = Phase7fRecord)
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS u019_ext.wext_hdfs_sequencefile;
CREATE WRITABLE EXTERNAL TABLE u019_ext.wext_hdfs_sequencefile (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_sequencefile?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster&DATA-SCHEMA=Phase7fRecord')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO u019_ext.wext_hdfs_sequencefile SELECT * FROM u019_ext.staging;

DROP FOREIGN TABLE IF EXISTS u019_ext.rext_hdfs_sequencefile;
CREATE EXTERNAL TABLE u019_ext.rext_hdfs_sequencefile (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://u019/hdfs_sequencefile?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster&DATA-SCHEMA=Phase7fRecord')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
TRUNCATE u019_ext.readback;
INSERT INTO u019_ext.readback SELECT * FROM u019_ext.rext_hdfs_sequencefile;

INSERT INTO u019_ext.u019_results
SELECT 'hdfs:SequenceFile', 'main',
       (SELECT count(*) FROM u019_ext.staging),
       (SELECT count(*) FROM u019_ext.readback),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.staging EXCEPT ALL SELECT * FROM u019_ext.readback) d),
       (SELECT count(*) FROM (SELECT * FROM u019_ext.readback EXCEPT ALL SELECT * FROM u019_ext.staging) d);

\echo '>>> hdfs:SequenceFile fingerprints:'
SELECT 'staging' AS side, count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.staging
UNION ALL
SELECT 'hdfs:SequenceFile', count(*), sum(id), sum(amount), sum(round(price::numeric,2)) FROM u019_ext.readback;
\echo '>>> hdfs:SequenceFile boundary spot rows (read-back vs staging):'
SELECT * FROM u019_ext.rext_hdfs_sequencefile WHERE id IN (1, :mid, :rows) ORDER BY id;
SELECT * FROM u019_ext.staging                WHERE id IN (1, :mid, :rows) ORDER BY id;

-- ===========================================================================
-- Verdict
-- ===========================================================================
\echo '>>> U019 per-source results (kind=main is the acceptance row):'
SELECT source, kind, rows_written, rows_read, missing, extra,
       (rows_read = rows_written)              AS count_ok,
       (missing = 0 AND extra = 0)             AS fidelity_ok
FROM u019_ext.u019_results ORDER BY source, kind;

\echo '>>> U019 verdict:'
SELECT CASE WHEN bool_and(rows_read = rows_written AND missing = 0 AND extra = 0)
            THEN 'ALL SOURCES PASS' ELSE 'AT LEAST ONE SOURCE FAILED' END AS verdict
FROM u019_ext.u019_results WHERE kind = 'main';

\echo '>>> TASK-U019 SQL complete.'

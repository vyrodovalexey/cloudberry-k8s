-- =============================================================================
-- Phase 7f — PXF external WRITABLE/READABLE tables acceptance (data round-trip)
-- =============================================================================
-- For each of the 7 sources below this script:
--   1. (Re)builds a ~10MB internal staging table `p7f_staging`.
--   2. Creates a WRITABLE external table wext_<src> over a fresh phase7f path.
--   3. INSERTs the staging table through it (writes ~10MB to S3/HDFS via PXF).
--   4. Creates a READABLE external table rext_<src> over the SAME location and
--      runs SELECT count(*) + a sample SELECT to prove the data round-trips.
--
-- Sources (PROFILE / SERVER / location):
--   s3:text             s3-datalake    cloudberry-data/phase7f/s3_text
--   s3:parquet          s3-datalake    cloudberry-data/phase7f/s3_parquet
--   s3:avro             s3-datalake    cloudberry-data/phase7f/s3_avro
--   hdfs:text           hadoop-cluster /phase7f/hdfs_text
--   hdfs:parquet        hadoop-cluster /phase7f/hdfs_parquet
--   hdfs:avro           hadoop-cluster /phase7f/hdfs_avro
--   hdfs:SequenceFile   hadoop-cluster /phase7f/hdfs_sequencefile
--
-- VERIFIED-IN-ENV NOTES (source of truth = the live acceptance-test cluster):
--   * s3 sources use bucket `cloudberry-data` (path-style MinIO) + SERVER
--     s3-datalake; hdfs sources use a leading HDFS path + SERVER hadoop-cluster.
--   * *:text  -> FORMAT 'TEXT' (delimiter ',') for both write and read.
--   * *:parquet / *:avro -> FORMAT 'CUSTOM' with formatter='pxfwritable_export'
--     (write) and formatter='pxfwritable_import' (read). No explicit schema
--     needed: PXF derives parquet/avro schema from the table column definitions.
--   * hdfs:SequenceFile REQUIRES a user-supplied Java Writable class passed via
--     the DATA-SCHEMA option. We ship `Phase7fRecord` (test/cases/Phase7fRecord.java),
--     compiled for Java 11 and dropped into /pxf-base/lib on every segment-primary
--     PXF sidecar (see phase7f-external-tables.sh::deploy_seqfile_class).
--   * IMPORTANT: directory names must NOT start with '_' or '.' — Hadoop's
--     FileInputFormat.hiddenFileFilter silently skips such paths, which makes a
--     readable external table fail with "Input path does not exist" even though
--     the writer created the files. All phase7f paths use plain names.
--
-- Run inside the coordinator pod via:  psql -U gpadmin -d <db> -f this.sql
-- The driver script sets :rows so the staging table lands near ~10MB.
-- =============================================================================

\set ON_ERROR_STOP on

-- Default row count (driver overrides with -v rows=...). 175k rows of the staging
-- schema below is ~20MB heap and lands ~10MB per source in the TEXT encoding
-- (parquet/avro/sequencefile compress to the ~5-8MB band), which satisfies the
-- "~10MB per source" acceptance target.
\if :{?rows}
\else
  \set rows 175000
\endif

-- ---------------------------------------------------------------------------
-- Staging table (~10MB). Column layout intentionally matches every external
-- table and the Phase7fRecord SequenceFile schema: (id, name, amount, price).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS p7f_staging;
CREATE TABLE p7f_staging (
    id      int             NOT NULL,
    name    text            NOT NULL,
    amount  bigint          NOT NULL,
    price   float8          NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO p7f_staging (id, name, amount, price)
SELECT g,
       'customer_' || md5(g::text),            -- ~40 chars
       (g % 1000000)::bigint,
       ((g % 100000)::float8) / 100.0
FROM generate_series(1, :rows) AS g;

ANALYZE p7f_staging;

\echo '>>> staging row count / heap size:'
SELECT count(*) AS staging_rows FROM p7f_staging;
SELECT pg_size_pretty(pg_total_relation_size('p7f_staging')) AS staging_size;

-- ===========================================================================
-- 1) s3:text
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_s3_text;
CREATE WRITABLE EXTERNAL TABLE wext_s3_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_text?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'TEXT' (delimiter ',');
INSERT INTO wext_s3_text SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_s3_text;
CREATE EXTERNAL TABLE rext_s3_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_text?PROFILE=s3:text&SERVER=s3-datalake')
  FORMAT 'TEXT' (delimiter ',');
\echo '>>> s3:text round-trip count + sample:'
SELECT count(*) AS rows FROM rext_s3_text;
SELECT * FROM rext_s3_text ORDER BY id LIMIT 3;

-- ===========================================================================
-- 2) s3:parquet
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_s3_parquet;
CREATE WRITABLE EXTERNAL TABLE wext_s3_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_parquet?PROFILE=s3:parquet&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO wext_s3_parquet SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_s3_parquet;
CREATE EXTERNAL TABLE rext_s3_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_parquet?PROFILE=s3:parquet&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
\echo '>>> s3:parquet round-trip count + sample:'
SELECT count(*) AS rows FROM rext_s3_parquet;
SELECT * FROM rext_s3_parquet ORDER BY id LIMIT 3;

-- ===========================================================================
-- 3) s3:avro
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_s3_avro;
CREATE WRITABLE EXTERNAL TABLE wext_s3_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_avro?PROFILE=s3:avro&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO wext_s3_avro SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_s3_avro;
CREATE EXTERNAL TABLE rext_s3_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://cloudberry-data/phase7f/s3_avro?PROFILE=s3:avro&SERVER=s3-datalake')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
\echo '>>> s3:avro round-trip count + sample:'
SELECT count(*) AS rows FROM rext_s3_avro;
SELECT * FROM rext_s3_avro ORDER BY id LIMIT 3;

-- ===========================================================================
-- 4) hdfs:text
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_hdfs_text;
CREATE WRITABLE EXTERNAL TABLE wext_hdfs_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_text?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'TEXT' (delimiter ',');
INSERT INTO wext_hdfs_text SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_hdfs_text;
CREATE EXTERNAL TABLE rext_hdfs_text (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_text?PROFILE=hdfs:text&SERVER=hadoop-cluster')
  FORMAT 'TEXT' (delimiter ',');
\echo '>>> hdfs:text round-trip count + sample:'
SELECT count(*) AS rows FROM rext_hdfs_text;
SELECT * FROM rext_hdfs_text ORDER BY id LIMIT 3;

-- ===========================================================================
-- 5) hdfs:parquet
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_hdfs_parquet;
CREATE WRITABLE EXTERNAL TABLE wext_hdfs_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_parquet?PROFILE=hdfs:parquet&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO wext_hdfs_parquet SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_hdfs_parquet;
CREATE EXTERNAL TABLE rext_hdfs_parquet (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_parquet?PROFILE=hdfs:parquet&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
\echo '>>> hdfs:parquet round-trip count + sample:'
SELECT count(*) AS rows FROM rext_hdfs_parquet;
SELECT * FROM rext_hdfs_parquet ORDER BY id LIMIT 3;

-- ===========================================================================
-- 6) hdfs:avro
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_hdfs_avro;
CREATE WRITABLE EXTERNAL TABLE wext_hdfs_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_avro?PROFILE=hdfs:avro&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO wext_hdfs_avro SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_hdfs_avro;
CREATE EXTERNAL TABLE rext_hdfs_avro (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_avro?PROFILE=hdfs:avro&SERVER=hadoop-cluster')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
\echo '>>> hdfs:avro round-trip count + sample:'
SELECT count(*) AS rows FROM rext_hdfs_avro;
SELECT * FROM rext_hdfs_avro ORDER BY id LIMIT 3;

-- ===========================================================================
-- 7) hdfs:SequenceFile (custom Writable DATA-SCHEMA = Phase7fRecord)
-- ===========================================================================
DROP FOREIGN TABLE IF EXISTS wext_hdfs_sequencefile;
CREATE WRITABLE EXTERNAL TABLE wext_hdfs_sequencefile (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_sequencefile?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster&DATA-SCHEMA=Phase7fRecord')
  FORMAT 'CUSTOM' (formatter='pxfwritable_export');
INSERT INTO wext_hdfs_sequencefile SELECT * FROM p7f_staging;

DROP FOREIGN TABLE IF EXISTS rext_hdfs_sequencefile;
CREATE EXTERNAL TABLE rext_hdfs_sequencefile (id int, name text, amount bigint, price float8)
  LOCATION ('pxf://phase7f/hdfs_sequencefile?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster&DATA-SCHEMA=Phase7fRecord')
  FORMAT 'CUSTOM' (formatter='pxfwritable_import');
\echo '>>> hdfs:SequenceFile round-trip count + sample:'
SELECT count(*) AS rows FROM rext_hdfs_sequencefile;
SELECT * FROM rext_hdfs_sequencefile ORDER BY id LIMIT 3;

\echo '>>> Phase 7f SQL complete.'

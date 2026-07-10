-- =============================================================================
-- OKD4 scenario step 8 — generate ~100MB of s3:parquet into greenplum-test.
-- =============================================================================
-- Builds a staging table sized so its parquet encoding is ~100MB, then writes
-- it to MinIO (greenplum-test/parquet/) through a WRITABLE PXF s3:parquet
-- external table. A READABLE external table over the same path is created so
-- step 9 can read it back.
--
-- Caller passes -v rows=<N> and -v server=<pxf-server-name>.
-- =============================================================================
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pxf;

-- Staging table (~100MB target; row ~= 220 bytes uncompressed, parquet ~half).
DROP EXTERNAL TABLE IF EXISTS okd4_parquet_write;
DROP EXTERNAL TABLE IF EXISTS okd4_parquet_read;
DROP TABLE IF EXISTS okd4_parquet_staging;

CREATE TABLE okd4_parquet_staging (
    id        bigint,
    category  text,
    amount    numeric(12,2),
    ts        timestamp,
    payload   text
) DISTRIBUTED BY (id);

INSERT INTO okd4_parquet_staging (id, category, amount, ts, payload)
SELECT g,
       (ARRAY['alpha','beta','gamma','delta','epsilon'])[(g % 5) + 1],
       ((g % 1000000)::numeric / 100),
       TIMESTAMP '2024-01-01 00:00:00' + (g % 86400) * INTERVAL '1 second',
       -- High-entropy payload (concatenated md5 hashes) so parquet+snappy does
       -- not over-compress and the on-disk size scales predictably (~40B/row).
       md5(g::text) || md5((g*7)::text) || md5((g*13)::text)
FROM generate_series(1, :rows) AS g;

ANALYZE okd4_parquet_staging;

-- Writable PXF s3:parquet external table -> writes to greenplum-test/parquet/.
-- __SERVER__ / __BUCKET__ are substituted by the step script before send.
CREATE WRITABLE EXTERNAL TABLE okd4_parquet_write (
    id bigint, category text, amount numeric(12,2), ts timestamp, payload text
)
LOCATION ('pxf://__BUCKET__/parquet?PROFILE=s3:parquet&SERVER=__SERVER__')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

INSERT INTO okd4_parquet_write SELECT * FROM okd4_parquet_staging;

-- Readable PXF s3:parquet external table over the same path (for step 9).
CREATE EXTERNAL TABLE okd4_parquet_read (
    id bigint, category text, amount numeric(12,2), ts timestamp, payload text
)
LOCATION ('pxf://__BUCKET__/parquet?PROFILE=s3:parquet&SERVER=__SERVER__')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

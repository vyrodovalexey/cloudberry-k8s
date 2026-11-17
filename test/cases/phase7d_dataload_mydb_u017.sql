-- =============================================================================
-- TASK-U017 - Data-load acceptance dataset for Apache Cloudberry (MPP) on K8s
-- Adapted from phase7d_dataload_mydb.sql (same schema and conventions), with:
--   * volumes scaled so the on-disk total is guaranteed >= 100 MB
--     (pg_database_size('mydb')), and
--   * fully deterministic values (no now()/random()) so re-runs and
--     backup/restore verification (TASK-U018) produce identical numbers.
--
-- Target database: mydb
--
-- Schema (Cloudberry DISTRIBUTED BY clauses for MPP even distribution):
--   customers  (id, name, email, created_at, region)         DISTRIBUTED BY (id)
--   orders     (id, customer_id, amount, status, order_date) DISTRIBUTED BY (id)
--   line_items (id, order_id, product, qty, price)           DISTRIBUTED BY (id)
--
-- Volumes and closed-form expectations (verify with
--   test/cases/phase7d_dataload_verify_u017.sql):
--   customers   80,000 rows   8 regions x exactly 10,000 customers
--   orders     480,000 rows   every customer has exactly 6 orders;
--                             5 statuses x exactly 96,000 orders
--   line_items 960,000 rows   every order has exactly 2 line items,
--                             hence every region has exactly 120,000 line items
--                             sum(qty) = 5,280,000 exactly
--
-- Idempotency: safe to re-run. Tables are dropped and recreated, then
-- regenerated with identical contents (pure generate_series arithmetic - no
-- randomness, no clock dependency). Re-running leaves the same dataset.
--
-- Run inside the coordinator pod as gpadmin against mydb (documented access
-- path from TASK-U015; psql over the local socket needs no password):
--   kubectl exec -i -n cloudberry-test acceptance-test-coordinator-0 \
--     -c cloudberry -- /usr/local/cloudberry-db/bin/psql \
--     -v ON_ERROR_STOP=1 -U gpadmin -d mydb \
--     < test/cases/phase7d_dataload_mydb_u017.sql
--
-- First-time database bootstrap (outside this file, once):
--   kubectl exec -n cloudberry-test acceptance-test-coordinator-0 -c cloudberry -- \
--     /usr/local/cloudberry-db/bin/psql -U gpadmin -d postgres \
--     -c "SELECT 1 FROM pg_database WHERE datname='mydb'" \
--     -c "CREATE DATABASE mydb"            -- only if the check returned 0 rows
--   kubectl exec -n cloudberry-test acceptance-test-coordinator-0 -c cloudberry -- \
--     /usr/local/cloudberry-db/bin/psql -U gpadmin -d mydb \
--     -c "CREATE EXTENSION IF NOT EXISTS gp_toolkit"   -- skew views (U015 note)
-- =============================================================================

-- Drop children first (FK-free schema, but keep the template's order).
DROP TABLE IF EXISTS line_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

-- ---------------------------------------------------------------------------
-- customers: 80,000 rows. Distribute by id (PK-like, high cardinality).
-- ---------------------------------------------------------------------------
CREATE TABLE customers (
    id          bigint      NOT NULL,
    name        text        NOT NULL,
    email       text        NOT NULL,
    created_at  timestamptz NOT NULL,
    region      text        NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO customers (id, name, email, created_at, region)
SELECT g,
       'customer_' || md5(g::text),
       'user_' || g::text || '@example.com',
       timestamptz '2026-01-01 00:00:00+00' - (g % 1000) * interval '1 day',
       (ARRAY['us-east','us-west','eu-central','eu-west',
              'ap-south','ap-northeast','sa-east','af-south'])[(g % 8) + 1]
FROM generate_series(1, 80000) AS g;

-- ---------------------------------------------------------------------------
-- orders: 480,000 rows - every customer has exactly 6 orders (480k = 6 x 80k).
-- ---------------------------------------------------------------------------
CREATE TABLE orders (
    id           bigint        NOT NULL,
    customer_id  bigint        NOT NULL,
    amount       numeric(12,2) NOT NULL,
    status       text          NOT NULL,
    order_date   date          NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO orders (id, customer_id, amount, status, order_date)
SELECT g,
       (g % 80000) + 1,
       ((g % 100000)::numeric / 100) + 1.00,
       (ARRAY['pending','paid','shipped','delivered','cancelled'])[(g % 5) + 1],
       (DATE '2023-01-01' + ((g % 730) || ' days')::interval)::date
FROM generate_series(1, 480000) AS g;

-- ---------------------------------------------------------------------------
-- line_items: 960,000 rows - every order has exactly 2 line items (960k = 2 x
-- 480k). This is the bulk table that pushes the database past 100 MB.
-- ---------------------------------------------------------------------------
CREATE TABLE line_items (
    id        bigint        NOT NULL,
    order_id  bigint        NOT NULL,
    product   text          NOT NULL,
    qty       integer       NOT NULL,
    price     numeric(10,2) NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO line_items (id, order_id, product, qty, price)
SELECT g,
       (g % 480000) + 1,
       'product_' || left(md5(g::text), 12),
       (g % 10) + 1,
       ((g % 50000)::numeric / 100) + 0.99
FROM generate_series(1, 960000) AS g;

-- ---------------------------------------------------------------------------
-- Indexes on the join / filter columns:
--   * customers_email_idx    - btree on a HIGH-CARDINALITY (unique) column
--   * orders_order_date_idx  - btree on the date column
--   * the remaining three cover join columns (template conventions).
-- ---------------------------------------------------------------------------
CREATE INDEX customers_region_idx    ON customers  (region);
CREATE INDEX customers_email_idx     ON customers  (email);
CREATE INDEX orders_customer_id_idx  ON orders     (customer_id);
CREATE INDEX orders_order_date_idx   ON orders     (order_date);
CREATE INDEX line_items_order_id_idx ON line_items (order_id);

-- ---------------------------------------------------------------------------
-- Refresh planner statistics (needed so the planner prefers index scans).
-- ---------------------------------------------------------------------------
ANALYZE customers;
ANALYZE orders;
ANALYZE line_items;

-- =============================================================================
-- Phase 7d - Data-load acceptance for Apache Cloudberry (MPP) on Kubernetes
-- Target database: mydb  (~100MB across a few tables with indexes)
--
-- Schema (with Cloudberry DISTRIBUTED BY clauses for MPP even distribution):
--   customers  (id, name, email, created_at, region)        DISTRIBUTED BY (id)
--   orders     (id, customer_id, amount, status, order_date) DISTRIBUTED BY (id)
--   line_items (id, order_id, product, qty, price)           DISTRIBUTED BY (id)
--
-- Row volumes are tuned so total on-disk size lands in the 90-150MB band.
-- Run inside the coordinator pod via psql -U gpadmin -d mydb.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- customers: ~80k rows. Distribute by id (PK-like, high cardinality).
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS line_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id          bigint      NOT NULL,
    name        text        NOT NULL,
    email       text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    region      text        NOT NULL
) DISTRIBUTED BY (id);

INSERT INTO customers (id, name, email, created_at, region)
SELECT g,
       'customer_' || md5(g::text),
       'user_' || g::text || '@example.com',
       now() - ((g % 1000) || ' days')::interval,
       (ARRAY['us-east','us-west','eu-central','eu-west',
              'ap-south','ap-northeast','sa-east','af-south'])[(g % 8) + 1]
FROM generate_series(1, 80000) AS g;

-- ---------------------------------------------------------------------------
-- orders: ~240k rows (each customer has ~3 orders). Distribute by id.
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
FROM generate_series(1, 240000) AS g;

-- ---------------------------------------------------------------------------
-- line_items: ~520k rows (each order has ~2-3 line items). Distribute by id.
-- This is the bulk table that pushes the database into the ~100MB range.
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
       (g % 240000) + 1,
       'product_' || left(md5(g::text), 12),
       (g % 10) + 1,
       ((g % 50000)::numeric / 100) + 0.99
FROM generate_series(1, 520000) AS g;

-- ---------------------------------------------------------------------------
-- Indexes on the join / filter columns.
-- ---------------------------------------------------------------------------
CREATE INDEX customers_region_idx   ON customers  (region);
CREATE INDEX orders_customer_id_idx ON orders     (customer_id);
CREATE INDEX orders_order_date_idx  ON orders     (order_date);
CREATE INDEX line_items_order_id_idx ON line_items (order_id);

-- ---------------------------------------------------------------------------
-- Refresh planner statistics.
-- ---------------------------------------------------------------------------
ANALYZE customers;
ANALYZE orders;
ANALYZE line_items;

-- =============================================================================
-- OKD4 scenario step 2 — generate ~1000MB of realistic data in mydb.
-- =============================================================================
-- Three MPP tables (customers, orders, lineitem) with realistic columns,
-- indexes, and bulk generate_series inserts sized to ~1GB total on-disk.
--
-- Row sizing (heap, uncompressed):
--   customers : :cust_rows  rows  (~150 bytes/row of payload)
--   orders    : :order_rows rows  (~120 bytes/row)
--   lineitem  : :line_rows  rows  (~180 bytes/row  -> the bulk of the volume)
--
-- The caller passes -v cust_rows=.. -v order_rows=.. -v line_rows=.. so the
-- volume is tuned from the shell (NO hardcoded absolute sizes here).
-- =============================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------------
-- customers
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS lineitem;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    c_custkey    bigint      NOT NULL,
    c_name       text        NOT NULL,
    c_address    text        NOT NULL,
    c_nationkey  int         NOT NULL,
    c_phone      text        NOT NULL,
    c_acctbal    numeric(12,2) NOT NULL,
    c_mktsegment text        NOT NULL,
    c_comment    text        NOT NULL,
    c_created    timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED BY (c_custkey);

INSERT INTO customers
      (c_custkey, c_name, c_address, c_nationkey, c_phone,
       c_acctbal, c_mktsegment, c_comment)
SELECT g,
       'Customer#' || lpad(g::text, 9, '0'),
       'addr-' || md5(g::text) || '-' || repeat('a', 40),
       (g % 25),
       to_char((g % 900000000) + 100000000, 'FM000-000-0000'),
       ((g % 1000000)::numeric / 100) - 5000,
       (ARRAY['BUILDING','AUTOMOBILE','MACHINERY','HOUSEHOLD','FURNITURE'])[(g % 5) + 1],
       'cust comment ' || md5((g * 7)::text)
FROM generate_series(1, :cust_rows) AS g;

-- ---------------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------------
CREATE TABLE orders (
    o_orderkey    bigint      NOT NULL,
    o_custkey     bigint      NOT NULL,
    o_orderstatus char(1)     NOT NULL,
    o_totalprice  numeric(12,2) NOT NULL,
    o_orderdate   date        NOT NULL,
    o_orderpriority text      NOT NULL,
    o_clerk       text        NOT NULL,
    o_comment     text        NOT NULL
) DISTRIBUTED BY (o_orderkey);

INSERT INTO orders
      (o_orderkey, o_custkey, o_orderstatus, o_totalprice,
       o_orderdate, o_orderpriority, o_clerk, o_comment)
SELECT g,
       (g % :cust_rows) + 1,
       (ARRAY['O','F','P'])[(g % 3) + 1],
       ((g % 5000000)::numeric / 100),
       DATE '2020-01-01' + ((g % 1460)),
       (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[(g % 5) + 1],
       'Clerk#' || lpad((g % 1000)::text, 9, '0'),
       'order comment ' || md5((g * 3)::text)
FROM generate_series(1, :order_rows) AS g;

-- ---------------------------------------------------------------------------
-- lineitem (the bulk table)
-- ---------------------------------------------------------------------------
CREATE TABLE lineitem (
    l_orderkey    bigint      NOT NULL,
    l_partkey     bigint      NOT NULL,
    l_suppkey     bigint      NOT NULL,
    l_linenumber  int         NOT NULL,
    l_quantity    numeric(10,2) NOT NULL,
    l_extendedprice numeric(12,2) NOT NULL,
    l_discount    numeric(4,2) NOT NULL,
    l_tax         numeric(4,2) NOT NULL,
    l_returnflag  char(1)     NOT NULL,
    l_linestatus  char(1)     NOT NULL,
    l_shipdate    date        NOT NULL,
    l_shipmode    text        NOT NULL,
    l_comment     text        NOT NULL
) DISTRIBUTED BY (l_orderkey);

INSERT INTO lineitem
      (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
       l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
       l_shipdate, l_shipmode, l_comment)
SELECT (g % :order_rows) + 1,
       (g % 200000) + 1,
       (g % 10000) + 1,
       (g % 7) + 1,
       ((g % 50) + 1)::numeric,
       ((g % 8000000)::numeric / 100),
       ((g % 10)::numeric / 100),
       ((g % 8)::numeric / 100),
       (ARRAY['A','N','R'])[(g % 3) + 1],
       (ARRAY['O','F'])[(g % 2) + 1],
       DATE '2020-01-01' + ((g % 1460)),
       (ARRAY['TRUCK','MAIL','SHIP','RAIL','AIR','FOB','REG AIR'])[(g % 7) + 1],
       'line comment ' || md5((g * 11)::text) || '-' || repeat('z', 20)
FROM generate_series(1, :line_rows) AS g;

-- ---------------------------------------------------------------------------
-- Indexes (exercise index-using queries)
-- ---------------------------------------------------------------------------
CREATE INDEX customers_mktsegment_idx ON customers (c_mktsegment);
CREATE INDEX orders_custkey_idx       ON orders (o_custkey);
CREATE INDEX orders_orderdate_idx     ON orders (o_orderdate);
CREATE INDEX lineitem_orderkey_idx    ON lineitem (l_orderkey);
CREATE INDEX lineitem_shipdate_idx    ON lineitem (l_shipdate);

ANALYZE customers;
ANALYZE orders;
ANALYZE lineitem;

-- =============================================================================
-- TASK-U017 - Verification SELECTs for the mydb dataset loaded by
--   test/cases/phase7d_dataload_mydb_u017.sql
--
-- The generator is deterministic (pure generate_series arithmetic), so every
-- expectation below is closed-form. These same numbers are the restore-
-- verification baseline for TASK-U018 and the dataset spec for TASK-U023.
--
-- Expected values (generator contract):
--   row counts:   customers 80,000 | orders 480,000 | line_items 960,000
--   sum(id):      customers 3,200,040,000 | orders 115,200,240,000
--                 line_items 460,800,480,000        (n(n+1)/2)
--   sum(qty):     line_items 5,280,000              (960,000 + 96,000*45)
--   regions:      8 x exactly 10,000 customers
--   statuses:     5 x exactly 96,000 orders
--   fan-out:      every customer exactly 6 orders; every order exactly 2
--                 line items; every region exactly 120,000 line items
--   size:         pg_database_size('mydb') >= 104,857,600 bytes (100 MiB)
--   distribution: rows on both primary segments (gp_segment_id 0 and 1)
--
-- Run inside the coordinator pod as gpadmin against mydb:
--   kubectl exec -i -n cloudberry-test acceptance-test-coordinator-0 \
--     -c cloudberry -- /usr/local/cloudberry-db/bin/psql \
--     -v ON_ERROR_STOP=1 -U gpadmin -d mydb \
--     < test/cases/phase7d_dataload_verify_u017.sql
-- =============================================================================

\timing off

\echo '== 1. row counts vs generated counts (exact match required) =='
SELECT 'customers'  AS table_name,  80000 AS expected_rows,
       count(*)     AS actual_rows,  count(*) =  80000 AS ok FROM customers
UNION ALL
SELECT 'orders',              480000, count(*), count(*) = 480000 FROM orders
UNION ALL
SELECT 'line_items',          960000, count(*), count(*) = 960000 FROM line_items;

\echo '== 2. integrity: distinct ids, id bounds, sum(id), sum(qty) =='
SELECT 'customers' AS table_name, count(*) AS rows, count(DISTINCT id) AS distinct_ids,
       min(id) AS min_id, max(id) AS max_id, sum(id) AS sum_id,
       sum(id) =  3200040000 AS sum_id_ok
FROM customers
UNION ALL
SELECT 'orders',     count(*), count(DISTINCT id), min(id), max(id), sum(id),
       sum(id) = 115200240000
FROM orders
UNION ALL
SELECT 'line_items', count(*), count(DISTINCT id), min(id), max(id), sum(id),
       sum(id) = 460800480000
FROM line_items;

\echo '== 2b. line_items sum(qty) (expected 5280000) =='
SELECT sum(qty) AS sum_qty, sum(qty) = 5280000 AS ok FROM line_items;

\echo '== 3. database size (must be >= 100 MiB = 104857600 bytes) =='
SELECT pg_size_pretty(pg_database_size('mydb')) AS mydb_size,
       pg_database_size('mydb')                 AS mydb_bytes,
       pg_database_size('mydb') >= 104857600    AS ge_100mib;

\echo '== 4. per-table total size (heap + indexes) =='
SELECT relname AS table_name,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
       pg_total_relation_size(relid)                 AS total_bytes
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

\echo '== 5. indexes present, definitions and sizes =='
\echo '== (pg_indexes is coordinator-local; pg_stat_user_indexes is not =='
\echo '==  dispatch-safe from the coordinator in Cloudberry)             =='
SELECT tablename AS table_name, indexname AS index_name,
       pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
       indexdef
FROM pg_indexes
WHERE tablename IN ('customers','orders','line_items')
ORDER BY tablename, indexname;

\echo '== 6. aggregate: orders per status (expected 96000 x 5) =='
SELECT status, count(*) AS orders, count(*) = 96000 AS ok
FROM orders GROUP BY status ORDER BY status;

\echo '== 7. aggregate: customers per region (expected 10000 x 8) =='
SELECT region, count(*) AS customers, count(*) = 10000 AS ok
FROM customers GROUP BY region ORDER BY region;

\echo '== 8. join fan-out: orders per customer (expected exactly 6 for all) =='
SELECT count(*)   AS customers_with_orders,
       min(c)     AS min_orders_per_customer,
       max(c)     AS max_orders_per_customer,
       min(c) = 6 AND max(c) = 6 AS ok
FROM (SELECT customer_id, count(*) AS c FROM orders GROUP BY customer_id) t;

\echo '== 9. join fan-out: line items per order (expected exactly 2 for all) =='
SELECT count(*)   AS orders_with_items,
       min(c)     AS min_items_per_order,
       max(c)     AS max_items_per_order,
       min(c) = 2 AND max(c) = 2 AS ok
FROM (SELECT order_id, count(*) AS c FROM line_items GROUP BY order_id) t;

\echo '== 10. 3-way join + aggregation: line items and revenue per region =='
\echo '==      (expected line_items = 120000 per region, 960000 total)      =='
SELECT c.region,
       count(*)                     AS line_items,
       count(*) = 120000            AS items_ok,
       round(sum(li.qty * li.price), 2) AS line_revenue
FROM customers c
JOIN orders o      ON o.customer_id = c.id
JOIN line_items li ON li.order_id   = o.id
GROUP BY c.region
ORDER BY line_revenue DESC;

\echo '== 10b. 3-way join total rows (expected 960000) =='
SELECT count(*) AS joined_rows, count(*) = 960000 AS ok
FROM customers c
JOIN orders o      ON o.customer_id = c.id
JOIN line_items li ON li.order_id   = o.id;

\echo '== 11. EXPLAIN: equality on indexed join column orders.customer_id =='
EXPLAIN SELECT id, customer_id, amount
FROM orders
WHERE customer_id = 12345;

\echo '== 12. EXPLAIN: equality on high-cardinality index customers.email =='
EXPLAIN SELECT id, name, created_at
FROM customers
WHERE email = 'user_4242@example.com';

\echo '== 13. EXPLAIN: range on indexed date column orders.order_date =='
EXPLAIN SELECT id, customer_id, order_date
FROM orders
WHERE order_date BETWEEN DATE '2023-05-01' AND DATE '2023-05-07';

\echo '== 14. EXPLAIN: equality on indexed join column line_items.order_id =='
EXPLAIN SELECT id, product, qty, price
FROM line_items
WHERE order_id = 99999;

\echo '== 15. segment distribution / skew (both primary segments required) =='
\echo '== customers =='
SELECT gp_segment_id, count(*) AS rows FROM customers  GROUP BY gp_segment_id ORDER BY gp_segment_id;
\echo '== orders =='
SELECT gp_segment_id, count(*) AS rows FROM orders     GROUP BY gp_segment_id ORDER BY gp_segment_id;
\echo '== line_items =='
SELECT gp_segment_id, count(*) AS rows FROM line_items GROUP BY gp_segment_id ORDER BY gp_segment_id;

\echo '== 16. gp_toolkit skew coefficients (2.2.0 column names; extension =='
\echo '==      installed in mydb per U015 note)                        =='
SELECT skcrelname AS table_name, skccoeff AS skew_coefficient
FROM gp_toolkit.gp_skew_coefficients
WHERE skcrelname IN ('customers','orders','line_items')
ORDER BY skcrelname;

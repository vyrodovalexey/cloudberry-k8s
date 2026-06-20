-- =============================================================================
-- Phase 7d - Verification SELECTs for mydb data-load acceptance.
-- Run inside the coordinator pod via psql -U gpadmin -d mydb.
-- =============================================================================

\echo '== row counts per table =='
SELECT 'customers'  AS table, count(*) AS rows FROM customers
UNION ALL SELECT 'orders',     count(*) FROM orders
UNION ALL SELECT 'line_items', count(*) FROM line_items;

\echo '== database size =='
SELECT pg_size_pretty(pg_database_size('mydb')) AS mydb_size,
       pg_database_size('mydb')                 AS mydb_bytes;

\echo '== per-table total size (incl. indexes) =='
SELECT relname AS table,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_catalog.pg_statio_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

\echo '== join + aggregation: top regions by total order amount =='
SELECT c.region,
       count(DISTINCT o.id)        AS orders,
       round(sum(o.amount), 2)     AS total_amount
FROM customers c
JOIN orders o ON o.customer_id = c.id
GROUP BY c.region
ORDER BY total_amount DESC;

\echo '== three-way join + aggregation (customers/orders/line_items) =='
SELECT c.region,
       round(sum(li.qty * li.price), 2) AS line_revenue
FROM customers c
JOIN orders o      ON o.customer_id = c.id
JOIN line_items li ON li.order_id   = o.id
GROUP BY c.region
ORDER BY line_revenue DESC;

\echo '== EXPLAIN: filtered query that should use an index =='
EXPLAIN SELECT id, customer_id, amount
FROM orders
WHERE customer_id = 12345;

\echo '== EXPLAIN: filtered query on indexed line_items.order_id =='
EXPLAIN SELECT id, product, qty, price
FROM line_items
WHERE order_id = 999;

\echo '== segment distribution / skew check (orders) =='
SELECT gp_segment_id, count(*) AS rows
FROM orders
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

\echo '== segment distribution / skew check (line_items) =='
SELECT gp_segment_id, count(*) AS rows
FROM line_items
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

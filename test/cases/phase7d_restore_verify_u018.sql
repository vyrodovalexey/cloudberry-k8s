-- =============================================================================
-- TASK-U018 - Post-restore verification for a database restored from the
--   TASK-U017 mydb backup (gpbackup/gprestore round-trip via MinIO S3).
--
-- Contract source: U017-report_2026-11-15_045943.md §9 (restore baseline).
-- Every check is a closed-form expected value captured from the U017 load
-- (deterministic dataset - no now()/random()) so the comparison is exact.
--
-- Usage (run inside the coordinator pod as gpadmin against the RESTORED db,
-- e.g. mydb_restored; the db name only matters for the size check below):
--   kubectl exec -i -n cloudberry-test acceptance-test-coordinator-0 -c cloudberry -- \
--     /usr/local/cloudberry-db/bin/psql -v ON_ERROR_STOP=1 -U gpadmin -d mydb_restored \
--     < test/cases/phase7d_restore_verify_u018.sql
--
-- Exit-detection: every section has an ok/f column; the suite is green when
-- all of them are t and the revenue fingerprint sums to 1314280100.00.
-- =============================================================================

\timing off

\echo '== R1. row counts vs U017 baseline (exact) =='
SELECT 'customers'  AS table_name,  80000 AS expected_rows,
       count(*)     AS actual_rows,  count(*) =  80000 AS ok FROM customers
UNION ALL
SELECT 'orders',              480000, count(*), count(*) = 480000 FROM orders
UNION ALL
SELECT 'line_items',          960000, count(*), count(*) = 960000 FROM line_items;

\echo '== R2. integrity fingerprints: distinct ids, bounds, sum(id) =='
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

\echo '== R2b. line_items sum(qty) (baseline 5280000) =='
SELECT sum(qty) AS sum_qty, sum(qty) = 5280000 AS ok FROM line_items;

\echo '== R3. restored database size (informational; counts/sums are the contract) =='
SELECT current_database() AS db,
       pg_size_pretty(pg_database_size(current_database())) AS db_size,
       pg_database_size(current_database())                 AS db_bytes;

\echo '== R4. indexes present (U017 baseline: 5 btrees) =='
SELECT tablename AS table_name, indexname AS index_name,
       pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
       indexdef
FROM pg_indexes
WHERE schemaname = 'public' AND tablename IN ('customers','orders','line_items')
ORDER BY tablename, indexname;

\echo '== R4b. index count check (baseline 5) =='
SELECT count(*) AS index_count, count(*) = 5 AS ok
FROM pg_indexes
WHERE schemaname = 'public' AND tablename IN ('customers','orders','line_items');

\echo '== R5. aggregates: orders per status (baseline 96000 x 5) =='
SELECT status, count(*) AS orders,
       count(*) = 96000 AS ok
FROM orders GROUP BY status ORDER BY status;

\echo '== R5b. aggregates: customers per region (baseline 10000 x 8) =='
SELECT region, count(*) AS customers,
       count(*) = 10000 AS ok
FROM customers GROUP BY region ORDER BY region;

\echo '== R6. join fan-out: orders per customer (baseline exactly 6) =='
SELECT min(c) AS min_orders_per_customer, max(c) AS max_orders_per_customer,
       min(c) = 6 AND max(c) = 6 AS ok
FROM (SELECT customer_id, count(*) AS c FROM orders GROUP BY customer_id) t;

\echo '== R6b. join fan-out: line items per order (baseline exactly 2) =='
SELECT min(c) AS min_items_per_order, max(c) AS max_items_per_order,
       min(c) = 2 AND max(c) = 2 AS ok
FROM (SELECT order_id, count(*) AS c FROM line_items GROUP BY order_id) t;

\echo '== R7. 3-way join + revenue fingerprint per region =='
\echo '== (baseline: 120000 items per region, 960000 total, values below) =='
SELECT c.region, count(*) AS line_items, count(*) = 120000 AS items_ok,
       sum(li.qty * li.price) AS line_revenue
FROM line_items li
JOIN orders o      ON o.id = li.order_id
JOIN customers c   ON c.id = o.customer_id
GROUP BY c.region ORDER BY c.region;

\echo '== R7b. 3-way join total rows + total revenue (baseline 960000 / 1314280100.00) =='
SELECT count(*) AS joined_rows, count(*) = 960000 AS rows_ok,
       sum(li.qty * li.price) AS total_revenue,
       sum(li.qty * li.price) = 1314280100.00 AS revenue_ok
FROM line_items li
JOIN orders o      ON o.id = li.order_id
JOIN customers c   ON c.id = o.customer_id;

\echo '== R8. segment distribution (both primary segments, ~50/50) =='
SELECT 'customers' AS table_name, gp_segment_id, count(*) AS rows
FROM customers GROUP BY gp_segment_id
UNION ALL
SELECT 'orders', gp_segment_id, count(*) FROM orders GROUP BY gp_segment_id
UNION ALL
SELECT 'line_items', gp_segment_id, count(*) FROM line_items GROUP BY gp_segment_id
ORDER BY 1, 2;

\echo '== R9. gp_toolkit extension survived the restore (U017 note) =='
SELECT count(*) >= 1 AS gp_toolkit_present
FROM pg_extension WHERE extname = 'gp_toolkit';

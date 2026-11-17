-- =============================================================================
-- U023 — mydb DB-query benchmark (PTC-220-05), Cloudberry 2.2.0 campaign
-- =============================================================================
-- READ-ONLY. Five queries in the exact SHAPES of the 2026-07-10/12 baselines
-- (Test Case C), adapted to the U017 dataset contract (customers 80k,
-- orders 480k, line_items 960k; .opencode/output/U017-report §9).
-- mydb must remain byte-identical at handoff — no DML/DDL here.
--
-- Run via (U015 access path; psql is NOT on PATH in the image):
--   kubectl exec -n cloudberry-test acceptance-test-coordinator-0 -c cloudberry -- \
--     /usr/local/cloudberry-db/bin/psql -U gpadmin -d mydb -f - < this file
--
-- Verified closed forms (U017 §6 + generator): C1=480000; C2=5 rows × exactly
-- 96000 each; C3=8 rows × exactly 60000 orders/region (10,000 customers/region
-- × 6 orders/customer — U017-verified fan-out), region_revenue sums checked at
-- runtime for internal consistency (Σorder_cnt=480000); C4=480000 windowed
-- rows; C5 checked at runtime for internal consistency only (amount generator
-- is ((id % 100000)/100)+1.00, not a linear ramp — no simple closed form).
-- =============================================================================

\timing on

\echo '=== C1: COUNT(*) on orders (full scan, 480k rows) ==='
SELECT COUNT(*) AS total_orders FROM orders;

\echo '=== C2: GROUP BY status + AVG(amount) (480k) ==='
SELECT status, COUNT(*) AS cnt, AVG(amount) AS avg_amount
FROM orders GROUP BY status ORDER BY status;

\echo '=== C3: JOIN customers×orders + GROUP BY region ==='
SELECT c.region, COUNT(*) AS order_cnt, SUM(o.amount) AS region_revenue
FROM orders o JOIN customers c ON c.id = o.customer_id
GROUP BY c.region ORDER BY c.region;

\echo '=== C4: Window function (running total + rank, 480k) ==='
SELECT COUNT(*) AS windowed FROM (
  SELECT id,
         SUM(amount) OVER (PARTITION BY customer_id ORDER BY id) AS running_total,
         RANK()       OVER (ORDER BY amount DESC)                 AS rnk
  FROM orders
) t;

\echo '=== C5: Subquery — above-average orders ==='
SELECT COUNT(*) AS above_avg_cnt, AVG(amount) AS above_avg_avg FROM orders
WHERE amount > (SELECT AVG(amount) FROM orders);

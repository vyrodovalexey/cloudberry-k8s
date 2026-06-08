-- =============================================================================
-- Cloudberry Performance Test - UPDATE Queries
-- =============================================================================
-- Three UPDATE queries of increasing complexity for benchmarking.
-- Each query is wrapped in a transaction and rolled back to preserve data.

-- Query 1: Simple update - mark old pending orders as cancelled.
\echo 'Query 1: Simple update - cancel old pending orders'
\timing on
BEGIN;
UPDATE orders
SET status = 'cancelled'
WHERE status = 'pending'
  AND order_date < '2023-06-01';
ROLLBACK;
\timing off

-- Query 2: Conditional update - adjust prices based on quantity tiers.
\echo 'Query 2: Conditional update - quantity-based price adjustment'
\timing on
BEGIN;
UPDATE orders
SET price = CASE
    WHEN quantity >= 10 THEN price * 0.90
    WHEN quantity >= 5  THEN price * 0.95
    ELSE price
END
WHERE status IN ('pending', 'processing')
  AND order_date >= '2025-01-01';
ROLLBACK;
\timing off

-- Query 3: Update with subquery - flag high-value customer orders.
\echo 'Query 3: Update with subquery - flag high-value customer orders'
\timing on
BEGIN;
UPDATE orders
SET notes = 'VIP-customer-order'
WHERE customer_id IN (
    SELECT customer_id
    FROM orders
    GROUP BY customer_id
    HAVING SUM(quantity * price) > 10000
)
AND status = 'delivered';
ROLLBACK;
\timing off

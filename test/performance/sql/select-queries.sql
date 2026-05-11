-- =============================================================================
-- Cloudberry Performance Test - SELECT Queries
-- =============================================================================
-- Five queries of increasing complexity for benchmarking.

-- Query 1: Simple sequential scan - count all orders.
\echo 'Query 1: Simple scan - count all orders'
\timing on
SELECT COUNT(*) AS total_orders FROM orders;
\timing off

-- Query 2: Filtered scan - orders by status with date range.
\echo 'Query 2: Filtered scan - orders by status and date range'
\timing on
SELECT status, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM orders
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
GROUP BY status
ORDER BY cnt DESC;
\timing off

-- Query 3: Aggregation - monthly revenue summary.
\echo 'Query 3: Aggregation - monthly revenue summary'
\timing on
SELECT
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*)                        AS order_count,
    SUM(quantity * price)           AS total_revenue,
    AVG(quantity * price)           AS avg_order_value,
    MAX(quantity * price)           AS max_order_value
FROM orders
WHERE status NOT IN ('cancelled', 'refunded')
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
\timing off

-- Query 4: Subquery - customers with above-average order value.
\echo 'Query 4: Subquery - customers with above-average order value'
\timing on
SELECT customer_id, order_count, avg_value
FROM (
    SELECT
        customer_id,
        COUNT(*)          AS order_count,
        AVG(price)        AS avg_value
    FROM orders
    GROUP BY customer_id
) sub
WHERE avg_value > (SELECT AVG(price) FROM orders)
ORDER BY avg_value DESC
LIMIT 100;
\timing off

-- Query 5: Window function - running total and rank per customer.
\echo 'Query 5: Window function - running total and rank per customer'
\timing on
SELECT *
FROM (
    SELECT
        id,
        customer_id,
        order_date,
        price,
        SUM(price) OVER (
            PARTITION BY customer_id ORDER BY order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_total,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY price DESC
        ) AS price_rank
    FROM orders
    WHERE status = 'delivered'
) ranked
WHERE price_rank <= 3
ORDER BY customer_id, price_rank
LIMIT 200;
\timing off

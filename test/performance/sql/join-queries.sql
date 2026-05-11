-- =============================================================================
-- Cloudberry Performance Test - JOIN Queries
-- =============================================================================
-- Three JOIN queries of increasing complexity for benchmarking.

-- Query 1: Inner join - orders with customer details.
\echo 'Query 1: Inner join - orders with customer details'
\timing on
SELECT
    o.id          AS order_id,
    c.first_name,
    c.last_name,
    c.city,
    o.product_id,
    o.quantity,
    o.price,
    o.status
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.order_date >= '2025-01-01'
ORDER BY o.order_date DESC
LIMIT 500;
\timing off

-- Query 2: Left join with aggregation - customer order summary.
\echo 'Query 2: Left join with aggregation - customer order summary'
\timing on
SELECT
    c.id          AS customer_id,
    c.first_name,
    c.last_name,
    c.state,
    COALESCE(agg.order_count, 0)    AS order_count,
    COALESCE(agg.total_spent, 0)    AS total_spent,
    COALESCE(agg.avg_order, 0)      AS avg_order
FROM customers c
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*)                AS order_count,
        SUM(quantity * price)   AS total_spent,
        AVG(quantity * price)   AS avg_order
    FROM orders
    WHERE status NOT IN ('cancelled', 'refunded')
    GROUP BY customer_id
) agg ON c.id = agg.customer_id
ORDER BY total_spent DESC
LIMIT 200;
\timing off

-- Query 3: Multi-table join with analytics - top customers by state.
\echo 'Query 3: Multi-table join with analytics - top customers by state'
\timing on
SELECT
    c.state,
    COUNT(DISTINCT c.id)            AS customer_count,
    COUNT(o.id)                     AS order_count,
    SUM(o.quantity * o.price)       AS total_revenue,
    AVG(o.quantity * o.price)       AS avg_order_value,
    SUM(CASE WHEN o.status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count,
    ROUND(
        SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END)::NUMERIC /
        NULLIF(COUNT(o.id), 0) * 100, 2
    ) AS cancel_rate_pct
FROM customers c
INNER JOIN orders o ON c.id = o.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.state
ORDER BY total_revenue DESC;
\timing off

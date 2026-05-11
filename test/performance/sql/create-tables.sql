-- =============================================================================
-- Cloudberry Performance Test - Table Definitions
-- =============================================================================

-- Drop tables if they exist for clean re-runs.
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Customers table (~50,000 rows, ~5MB)
CREATE TABLE customers (
    id          INTEGER PRIMARY KEY,
    first_name  VARCHAR(50)  NOT NULL,
    last_name   VARCHAR(50)  NOT NULL,
    email       VARCHAR(120) NOT NULL,
    city        VARCHAR(50),
    state       VARCHAR(10),
    country     VARCHAR(10),
    created_at  DATE
);

-- Orders table (~1,000,000 rows, ~95MB)
CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    price       NUMERIC(10,2) NOT NULL,
    order_date  TIMESTAMP NOT NULL,
    status      VARCHAR(20) NOT NULL,
    notes       TEXT
);

-- Create indexes for query performance testing.
CREATE INDEX idx_orders_customer_id ON orders (customer_id);
CREATE INDEX idx_orders_order_date  ON orders (order_date);
CREATE INDEX idx_orders_status      ON orders (status);
CREATE INDEX idx_customers_email    ON customers (email);
CREATE INDEX idx_customers_state    ON customers (state);

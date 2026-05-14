-- Scenario 7: Load Data for Subsequent Scenarios
-- Creates tables with different distribution strategies and loads test data

-- Connect to mydb
\c mydb

-- ============================================================
-- Hash-distributed tables (simulated via COMMENT)
-- In Cloudberry/GP: DISTRIBUTED BY (id)
-- ============================================================

-- customers table already exists from Scenario 6, add distribution comment
COMMENT ON TABLE customers IS 'distribution=hash, key=id';

-- orders table already exists from Scenario 6, add distribution comment  
COMMENT ON TABLE orders IS 'distribution=hash, key=customer_id';

-- Add more data to make skew measurable
-- Insert skewed data: 80% of orders go to 20% of customers (Pareto)
INSERT INTO orders (customer_id, amount, status)
SELECT 
    CASE WHEN random() < 0.8 
        THEN (random() * 19999 + 1)::int          -- 80% to first 20K customers
        ELSE (random() * 79999 + 20001)::int       -- 20% to remaining 80K
    END,
    (random() * 5000 + 1)::numeric(10,2),
    CASE (random() * 4)::int 
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'cancelled'
        ELSE 'returned'
    END
FROM generate_series(1, 500000);

-- ============================================================
-- Randomly distributed table
-- In Cloudberry/GP: DISTRIBUTED RANDOMLY
-- ============================================================

CREATE TABLE IF NOT EXISTS logs (
    id BIGSERIAL PRIMARY KEY,
    log_time TIMESTAMP NOT NULL DEFAULT NOW(),
    level VARCHAR(10) NOT NULL,
    source VARCHAR(100) NOT NULL,
    message TEXT NOT NULL,
    metadata JSONB
);

COMMENT ON TABLE logs IS 'distribution=random';

CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(log_time);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_source ON logs(source);

-- Insert ~200K log entries
INSERT INTO logs (log_time, level, source, message, metadata)
SELECT 
    NOW() - (random() * interval '30 days'),
    CASE (random() * 4)::int
        WHEN 0 THEN 'DEBUG'
        WHEN 1 THEN 'INFO'
        WHEN 2 THEN 'WARN'
        WHEN 3 THEN 'ERROR'
        ELSE 'FATAL'
    END,
    'service-' || (random() * 20)::int,
    'Log message ' || i || ': ' || md5(random()::text),
    jsonb_build_object(
        'request_id', md5(random()::text),
        'duration_ms', (random() * 5000)::int,
        'status_code', CASE (random() * 5)::int
            WHEN 0 THEN 200 WHEN 1 THEN 201 WHEN 2 THEN 400
            WHEN 3 THEN 404 ELSE 500 END
    )
FROM generate_series(1, 200000) AS i;

-- ============================================================
-- Exclusion table (should be skipped during rebalance)
-- ============================================================

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMP NOT NULL DEFAULT NOW(),
    user_name VARCHAR(100) NOT NULL,
    action VARCHAR(50) NOT NULL,
    resource_type VARCHAR(50) NOT NULL,
    resource_id VARCHAR(200),
    details JSONB,
    ip_address INET
);

COMMENT ON TABLE audit_log IS 'distribution=hash, key=id, exclude_from_rebalance=true';

CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_log(event_time);
CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_name);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);

-- Insert ~100K audit entries
INSERT INTO audit_log (event_time, user_name, action, resource_type, resource_id, details, ip_address)
SELECT 
    NOW() - (random() * interval '90 days'),
    CASE (random() * 4)::int
        WHEN 0 THEN 'gpadmin' WHEN 1 THEN 'analyst'
        WHEN 2 THEN 'app_user' ELSE 'system'
    END,
    CASE (random() * 5)::int
        WHEN 0 THEN 'CREATE' WHEN 1 THEN 'READ' WHEN 2 THEN 'UPDATE'
        WHEN 3 THEN 'DELETE' ELSE 'LOGIN'
    END,
    CASE (random() * 3)::int
        WHEN 0 THEN 'table' WHEN 1 THEN 'role' WHEN 2 THEN 'database' ELSE 'schema'
    END,
    'resource-' || (random() * 1000)::int,
    jsonb_build_object('old_value', md5(random()::text), 'new_value', md5(random()::text)),
    ('10.' || (random()*255)::int || '.' || (random()*255)::int || '.' || (random()*255)::int)::inet
FROM generate_series(1, 100000) AS i;

-- ============================================================
-- Temp-pattern table (matches temp_* pattern for exclusion)
-- ============================================================

CREATE TABLE IF NOT EXISTS temp_staging (
    id SERIAL PRIMARY KEY,
    batch_id VARCHAR(50) NOT NULL,
    raw_data JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

COMMENT ON TABLE temp_staging IS 'distribution=hash, key=id, temporary_staging=true';

CREATE INDEX IF NOT EXISTS idx_temp_staging_batch ON temp_staging(batch_id);
CREATE INDEX IF NOT EXISTS idx_temp_staging_processed ON temp_staging(processed);

-- Insert ~50K staging records
INSERT INTO temp_staging (batch_id, raw_data, processed, created_at, processed_at)
SELECT 
    'batch-' || (i / 1000)::int,
    jsonb_build_object(
        'source', 'import-' || (random() * 5)::int,
        'record_num', i,
        'payload', md5(random()::text),
        'size_bytes', (random() * 10000)::int
    ),
    random() < 0.7,  -- 70% processed
    NOW() - (random() * interval '7 days'),
    CASE WHEN random() < 0.7 THEN NOW() - (random() * interval '6 days') ELSE NULL END
FROM generate_series(1, 50000) AS i;

-- ============================================================
-- Grant permissions
-- ============================================================

GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO analyst;

-- ============================================================
-- Run ANALYZE to update statistics
-- ============================================================

ANALYZE customers;
ANALYZE orders;
ANALYZE logs;
ANALYZE audit_log;
ANALYZE temp_staging;

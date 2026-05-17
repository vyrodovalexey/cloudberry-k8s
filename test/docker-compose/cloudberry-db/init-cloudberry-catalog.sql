-- =============================================================================
-- Cloudberry Database Catalog Simulation for PostgreSQL 16
-- =============================================================================
-- This script creates the Cloudberry/Greenplum-specific catalog tables,
-- views, functions, and schemas that the cloudberry-operator expects.
-- It allows testing the operator's FTS probe, failover, segment management,
-- and resource group logic against a real PostgreSQL database.
-- =============================================================================

-- Create the gpadmin superuser role (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'gpadmin') THEN
        CREATE ROLE gpadmin WITH LOGIN SUPERUSER CREATEDB CREATEROLE PASSWORD 'changeme';
    END IF;
END
$$;

-- =============================================================================
-- gp_segment_configuration — core catalog table
-- =============================================================================
-- Stores the topology of the Cloudberry/Greenplum cluster.
-- content = -1 for coordinator, >= 0 for segments
-- role: 'p' = primary, 'm' = mirror
-- preferred_role: original role assignment
-- mode: 's' = synced, 'r' = resync/catchup, 'n' = not synced
-- status: 'u' = up, 'd' = down
-- =============================================================================
CREATE TABLE IF NOT EXISTS gp_segment_configuration (
    dbid            INTEGER PRIMARY KEY,
    content         INTEGER NOT NULL,
    role            CHAR(1) NOT NULL CHECK (role IN ('p', 'm')),
    preferred_role  CHAR(1) NOT NULL CHECK (preferred_role IN ('p', 'm')),
    mode            CHAR(1) NOT NULL DEFAULT 's' CHECK (mode IN ('s', 'r', 'n')),
    status          CHAR(1) NOT NULL DEFAULT 'u' CHECK (status IN ('u', 'd')),
    port            INTEGER NOT NULL DEFAULT 5432,
    hostname        TEXT NOT NULL,
    address         TEXT NOT NULL,
    datadir         TEXT NOT NULL DEFAULT '/data/pgdata'
);

-- Insert coordinator (content = -1)
INSERT INTO gp_segment_configuration (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
VALUES
    (1, -1, 'p', 'p', 's', 'u', 5432, 'coordinator', 'coordinator', '/data/pgdata')
ON CONFLICT (dbid) DO NOTHING;

-- Insert standby coordinator (content = -1, role = 'm')
INSERT INTO gp_segment_configuration (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
VALUES
    (8, -1, 'm', 'm', 's', 'u', 5432, 'standby', 'standby', '/data/pgdata')
ON CONFLICT (dbid) DO NOTHING;

-- Insert 4 primary segments (content 0-3)
INSERT INTO gp_segment_configuration (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
VALUES
    (2, 0, 'p', 'p', 's', 'u', 5432, 'segment-primary-0', 'segment-primary-0', '/data/pgdata'),
    (3, 1, 'p', 'p', 's', 'u', 5432, 'segment-primary-1', 'segment-primary-1', '/data/pgdata'),
    (4, 2, 'p', 'p', 's', 'u', 5432, 'segment-primary-2', 'segment-primary-2', '/data/pgdata'),
    (5, 3, 'p', 'p', 's', 'u', 5432, 'segment-primary-3', 'segment-primary-3', '/data/pgdata')
ON CONFLICT (dbid) DO NOTHING;

-- Insert 4 mirror segments (content 0-3)
INSERT INTO gp_segment_configuration (dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir)
VALUES
    (9,  0, 'm', 'm', 's', 'u', 5432, 'segment-mirror-0', 'segment-mirror-0', '/data/pgdata'),
    (10, 1, 'm', 'm', 's', 'u', 5432, 'segment-mirror-1', 'segment-mirror-1', '/data/pgdata'),
    (11, 2, 'm', 'm', 's', 'u', 5432, 'segment-mirror-2', 'segment-mirror-2', '/data/pgdata'),
    (12, 3, 'm', 'm', 's', 'u', 5432, 'segment-mirror-3', 'segment-mirror-3', '/data/pgdata')
ON CONFLICT (dbid) DO NOTHING;

-- =============================================================================
-- gp_request_fts_probe_scan() — FTS probe trigger function
-- =============================================================================
-- In real Cloudberry, this triggers the Fault Tolerance Service to scan
-- all segments. Here we simulate it by checking connectivity metadata.
-- =============================================================================
CREATE OR REPLACE FUNCTION gp_request_fts_probe_scan()
RETURNS BOOLEAN AS $$
BEGIN
    -- Simulate FTS probe: update timestamps, check segment health
    RAISE NOTICE 'FTS probe scan triggered (simulated)';
    
    -- In simulation, we just return true indicating probe completed
    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- gp_toolkit schema — Cloudberry management toolkit
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS gp_toolkit;

-- gp_resgroup_status view — resource group monitoring
CREATE TABLE IF NOT EXISTS gp_toolkit.gp_resgroup_status (
    rsgname         TEXT PRIMARY KEY,
    num_running     INTEGER DEFAULT 0,
    num_queueing    INTEGER DEFAULT 0,
    cpu_usage       NUMERIC(10,4) DEFAULT 0.0,
    memory_usage    NUMERIC(10,4) DEFAULT 0.0
);

-- Insert default resource groups (matching Cloudberry defaults)
INSERT INTO gp_toolkit.gp_resgroup_status (rsgname, num_running, num_queueing, cpu_usage, memory_usage)
VALUES
    ('admin_group', 0, 0, 0.0, 0.0),
    ('default_group', 0, 0, 0.0, 0.0),
    ('system_group', 0, 0, 0.0, 0.0)
ON CONFLICT (rsgname) DO NOTHING;

-- gp_skew_coefficients view — data distribution skew analysis
CREATE OR REPLACE VIEW gp_toolkit.gp_skew_coefficients AS
SELECT
    c.oid AS skcoid,
    n.nspname AS skcnamespace,
    c.relname AS skcrelname,
    0.0::NUMERIC AS skccoeff
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit');

-- =============================================================================
-- Helper functions for segment management
-- =============================================================================

-- Simulate gp_segment_id (normally a system column in Cloudberry)
CREATE OR REPLACE FUNCTION gp_segment_id()
RETURNS INTEGER AS $$
BEGIN
    RETURN -1;  -- coordinator
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Simulate pg_wal_lsn_diff for replication lag (already exists in PG16)
-- pg_current_wal_lsn() already exists in PG16

-- =============================================================================
-- Utility functions for the operator
-- =============================================================================

-- Function to simulate marking a segment as down (for testing failover)
CREATE OR REPLACE FUNCTION gp_mark_segment_down(seg_dbid INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE gp_segment_configuration
    SET status = 'd', mode = 'n'
    WHERE dbid = seg_dbid;
    RAISE NOTICE 'Segment dbid=% marked as down', seg_dbid;
END;
$$ LANGUAGE plpgsql;

-- Function to simulate marking a segment as up (for testing recovery)
CREATE OR REPLACE FUNCTION gp_mark_segment_up(seg_dbid INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE gp_segment_configuration
    SET status = 'u', mode = 's'
    WHERE dbid = seg_dbid;
    RAISE NOTICE 'Segment dbid=% marked as up', seg_dbid;
END;
$$ LANGUAGE plpgsql;

-- Function to update segment hostnames dynamically (used during cluster init)
CREATE OR REPLACE FUNCTION gp_update_segment_host(
    seg_content INTEGER,
    seg_role CHAR(1),
    new_hostname TEXT,
    new_address TEXT
)
RETURNS VOID AS $$
BEGIN
    UPDATE gp_segment_configuration
    SET hostname = new_hostname, address = new_address
    WHERE content = seg_content AND role = seg_role;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Grant permissions to gpadmin
-- =============================================================================
GRANT ALL ON TABLE gp_segment_configuration TO gpadmin;
GRANT ALL ON SCHEMA gp_toolkit TO gpadmin;
GRANT ALL ON ALL TABLES IN SCHEMA gp_toolkit TO gpadmin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO gpadmin;

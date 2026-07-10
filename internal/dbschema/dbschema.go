// Package dbschema holds shared database schema DDL constants used by both
// the operator's db client (internal/db) and the standalone
// cloudberry-query-exporter binary. It is a const-only leaf package (no
// dependencies) so the exporter does not pull in the operator's pgx pool
// stack and no import cycles are possible (T19/D3).
package dbschema

// QueryHistoryDDL creates the cloudberry_query_history table and its indexes.
// It is executed with CREATE ... IF NOT EXISTS semantics by two independent
// writers (the operator db client and the query exporter), so the definition
// MUST stay a single source of truth — schema drift between the two would
// corrupt inserts silently.
const QueryHistoryDDL = `
CREATE TABLE IF NOT EXISTS cloudberry_query_history (
    id               BIGSERIAL PRIMARY KEY,
    query_id         TEXT NOT NULL,
    pid              INTEGER NOT NULL,
    username         TEXT NOT NULL,
    database_name    TEXT NOT NULL,
    query_text       TEXT NOT NULL,
    query_start      TIMESTAMPTZ NOT NULL,
    query_end        TIMESTAMPTZ NOT NULL,
    duration_ms      DOUBLE PRECISION NOT NULL,
    state            TEXT NOT NULL,
    rows_affected    BIGINT DEFAULT 0,
    cpu_time_ms      DOUBLE PRECISION DEFAULT 0,
    memory_bytes     BIGINT DEFAULT 0,
    spill_bytes      BIGINT DEFAULT 0,
    disk_read_bytes  BIGINT DEFAULT 0,
    disk_write_bytes BIGINT DEFAULT 0,
    wait_events      TEXT DEFAULT '',
    resource_group   TEXT DEFAULT '',
    explain_plan     TEXT DEFAULT '',
    error_message    TEXT DEFAULT '',
    created_at       TIMESTAMPTZ DEFAULT NOW()
) DISTRIBUTED BY (id);

CREATE INDEX IF NOT EXISTS idx_query_history_start ON cloudberry_query_history (query_start);
CREATE INDEX IF NOT EXISTS idx_query_history_user ON cloudberry_query_history (username);
CREATE INDEX IF NOT EXISTS idx_query_history_db ON cloudberry_query_history (database_name);
`

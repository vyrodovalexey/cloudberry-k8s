// Package main contains the history collector for the cloudberry-query-exporter.
// It periodically snapshots completed queries from pg_stat_activity and writes
// them to the cloudberry_query_history table.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// historyCollector tracks active queries and detects completed ones by comparing
// PID sets between collection cycles.
type historyCollector struct {
	logger             *slog.Logger
	lastSeenPIDs       map[int32]*sessionSnapshot
	planCollection     bool
	slowQueryThreshold time.Duration
}

// sessionSnapshot holds the state of a session from the previous collection cycle.
type sessionSnapshot struct {
	PID           int32
	Username      string
	Database      string
	QueryText     string
	QueryStart    time.Time
	State         string
	WaitEventType string
	ResourceGroup string
}

// newHistoryCollector creates a new historyCollector.
func newHistoryCollector(logger *slog.Logger, planCollection bool, slowQueryThreshold time.Duration) *historyCollector {
	return &historyCollector{
		logger:             logger.With("component", "history-collector"),
		lastSeenPIDs:       make(map[int32]*sessionSnapshot),
		planCollection:     planCollection,
		slowQueryThreshold: slowQueryThreshold,
	}
}

// queryHistoryDDL is the DDL for creating the query history table.
// Duplicated from internal/db/query_history.go because the exporter uses
// a raw pgx.Conn rather than the db.Client interface.
const historyTableDDL = `
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

// SQL query to snapshot current sessions from pg_stat_activity.
const snapshotSessionsSQL = `SELECT pid, COALESCE(usename, ''), COALESCE(datname, ''),
	COALESCE(query, ''), COALESCE(query_start, now()),
	COALESCE(state, ''), COALESCE(wait_event_type, '')
	FROM pg_stat_activity
	WHERE backend_type = 'client backend'
	AND pid != pg_backend_pid()
	AND usename IS NOT NULL`

// SQL for inserting a history entry.
const insertHistorySQL = `INSERT INTO cloudberry_query_history
	(query_id, pid, username, database_name, query_text, query_start, query_end,
	 duration_ms, state, wait_events, resource_group)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`

// SQL for inserting a history entry with an EXPLAIN plan.
const insertHistoryWithPlanSQL = `INSERT INTO cloudberry_query_history
	(query_id, pid, username, database_name, query_text, query_start, query_end,
	 duration_ms, state, wait_events, resource_group, explain_plan)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

// ensureTable creates the history table if it doesn't exist.
func (hc *historyCollector) ensureTable(ctx context.Context, conn *pgx.Conn) error {
	if conn == nil {
		return fmt.Errorf("no database connection available")
	}
	if _, err := conn.Exec(ctx, historyTableDDL); err != nil {
		return fmt.Errorf("ensuring query history table: %w", err)
	}
	hc.logger.Info("query history table ensured")
	return nil
}

// collectHistory performs a single history collection cycle.
// It snapshots current sessions, compares with the previous cycle to detect
// completed queries, and inserts them into the history table.
func (hc *historyCollector) collectHistory(ctx context.Context, conn *pgx.Conn) {
	if conn == nil {
		return
	}

	currentPIDs, ok := hc.snapshotSessions(ctx, conn)
	if !ok {
		return
	}

	// Detect completed queries: PIDs that were in lastSeenPIDs but not in currentPIDs.
	now := time.Now()
	var insertedCount int
	for pid, prevSession := range hc.lastSeenPIDs {
		if _, stillActive := currentPIDs[pid]; stillActive {
			continue
		}
		if hc.recordCompletedQuery(ctx, conn, pid, prevSession, now) {
			insertedCount++
		}
	}

	if insertedCount > 0 {
		hc.logger.Info("query history entries inserted", "count", insertedCount)
	}

	// Update lastSeenPIDs for the next cycle.
	hc.lastSeenPIDs = currentPIDs
}

// snapshotSessions queries the current sessions and returns them keyed by PID.
// The boolean is false when the snapshot could not be obtained.
func (hc *historyCollector) snapshotSessions(ctx context.Context, conn *pgx.Conn) (map[int32]*sessionSnapshot, bool) {
	queryCtx, cancel := context.WithTimeout(ctx, collectorQueryTimeout)
	defer cancel()

	rows, err := conn.Query(queryCtx, snapshotSessionsSQL)
	if err != nil {
		hc.logger.Warn("failed to snapshot sessions for history collection", "error", err)
		return nil, false
	}
	defer rows.Close()

	currentPIDs := make(map[int32]*sessionSnapshot)
	for rows.Next() {
		var s sessionSnapshot
		if scanErr := rows.Scan(
			&s.PID, &s.Username, &s.Database,
			&s.QueryText, &s.QueryStart, &s.State, &s.WaitEventType,
		); scanErr != nil {
			hc.logger.Warn("failed to scan session snapshot row", "error", scanErr)
			continue
		}
		currentPIDs[s.PID] = &s
	}

	if rowErr := rows.Err(); rowErr != nil {
		hc.logger.Warn("error iterating session snapshot rows", "error", rowErr)
		return nil, false
	}

	return currentPIDs, true
}

// recordCompletedQuery inserts a single completed query into the history table.
// It returns true when an entry was inserted.
func (hc *historyCollector) recordCompletedQuery(
	ctx context.Context, conn *pgx.Conn, pid int32, prevSession *sessionSnapshot, now time.Time,
) bool {
	// Only record queries that were in 'active' state (not idle sessions).
	if prevSession.State != "active" {
		return false
	}

	// Skip empty queries.
	if prevSession.QueryText == "" {
		return false
	}

	durationMs := now.Sub(prevSession.QueryStart).Seconds() * 1000
	queryID := fmt.Sprintf("q-%d-%d", pid, prevSession.QueryStart.UnixNano())

	// Optionally collect EXPLAIN plan for slow queries.
	var explainPlan string
	if hc.planCollection && durationMs > hc.slowQueryThreshold.Seconds()*1000 {
		explainPlan = hc.collectExplainPlan(ctx, conn, prevSession.QueryText)
	}

	insertErr := hc.insertHistoryEntry(ctx, conn, queryID, pid, prevSession, now, durationMs, explainPlan)
	if insertErr != nil {
		hc.logger.Warn("failed to insert query history entry",
			"pid", pid, "queryId", queryID, "error", insertErr)
		return false
	}

	return true
}

// insertHistoryEntry writes a completed query row, with or without an EXPLAIN plan.
func (hc *historyCollector) insertHistoryEntry(
	ctx context.Context, conn *pgx.Conn, queryID string, pid int32,
	prevSession *sessionSnapshot, now time.Time, durationMs float64, explainPlan string,
) error {
	if explainPlan != "" {
		_, err := conn.Exec(ctx, insertHistoryWithPlanSQL,
			queryID, pid, prevSession.Username, prevSession.Database,
			prevSession.QueryText, prevSession.QueryStart, now,
			durationMs, "completed", prevSession.WaitEventType,
			prevSession.ResourceGroup, explainPlan,
		)
		return err
	}

	_, err := conn.Exec(ctx, insertHistorySQL,
		queryID, pid, prevSession.Username, prevSession.Database,
		prevSession.QueryText, prevSession.QueryStart, now,
		durationMs, "completed", prevSession.WaitEventType,
		prevSession.ResourceGroup,
	)
	return err
}

// collectExplainPlan attempts to collect an EXPLAIN plan for a query.
// It uses a short timeout to prevent blocking and skips DDL/utility commands.
func (hc *historyCollector) collectExplainPlan(ctx context.Context, conn *pgx.Conn, queryText string) string {
	// Skip DDL, COPY, and utility commands.
	upperQuery := strings.ToUpper(strings.TrimSpace(queryText))
	skipPrefixes := []string{
		"CREATE", "ALTER", "DROP", "COPY", "GRANT", "REVOKE",
		"SET", "RESET", "VACUUM", "ANALYZE", "REINDEX",
	}
	for _, prefix := range skipPrefixes {
		if strings.HasPrefix(upperQuery, prefix) {
			return ""
		}
	}

	// Use a short timeout for EXPLAIN to prevent blocking.
	explainCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	explainQuery := fmt.Sprintf("EXPLAIN (FORMAT TEXT) %s", queryText)
	rows, err := conn.Query(explainCtx, explainQuery)
	if err != nil {
		hc.logger.Debug("failed to collect EXPLAIN plan", "error", err)
		return ""
	}
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		if scanErr := rows.Scan(&line); scanErr != nil {
			hc.logger.Debug("failed to scan EXPLAIN plan row", "error", scanErr)
			return ""
		}
		planLines = append(planLines, line)
	}

	if rowErr := rows.Err(); rowErr != nil {
		hc.logger.Debug("error iterating EXPLAIN plan rows", "error", rowErr)
		return ""
	}

	return strings.Join(planLines, "\n")
}

// cleanupHistory deletes entries older than the retention period.
func (hc *historyCollector) cleanupHistory(ctx context.Context, conn *pgx.Conn, retention time.Duration) {
	if conn == nil {
		return
	}

	cutoff := time.Now().Add(-retention)
	result, err := conn.Exec(ctx, "DELETE FROM cloudberry_query_history WHERE created_at < $1", cutoff)
	if err != nil {
		hc.logger.Warn("failed to cleanup query history", "error", err)
		return
	}

	deleted := result.RowsAffected()
	if deleted > 0 {
		hc.logger.Info("query history cleanup completed",
			"deleted", deleted, "retention", retention.String())
	}
}

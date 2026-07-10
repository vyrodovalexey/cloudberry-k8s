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
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/dbschema"
)

// History pipeline stage label values for
// cloudberry_query_exporter_history_errors_total (bounded enum, E2).
const (
	historyStageSnapshot = "snapshot"
	historyStageInsert   = "insert"
	historyStageExplain  = "explain"
)

// historyInsertTimeout bounds each history INSERT round-trip.
// cloudberry_query_history is a DISTRIBUTED table: the INSERT is dispatched to
// a segment over the Motion/Interconnect layer, so a degraded interconnect
// would otherwise wedge the shared connection (and the whole collect loop)
// until gp_interconnect_setup_timeout. Aligned with collectorQueryTimeout so
// every statement the loop issues shares one budget.
const historyInsertTimeout = collectorQueryTimeout

// historyCleanupTimeout bounds the hourly retention DELETE on the same
// distributed table. It is deliberately more generous than the insert budget:
// a month of history rows may be deleted in one pass, but the cleanup must
// still never wedge the collect loop indefinitely — an aborted cleanup simply
// retries on the next tick.
const historyCleanupTimeout = 60 * time.Second

// historyCollector tracks active queries and detects completed ones by comparing
// PID sets between collection cycles.
type historyCollector struct {
	logger             *slog.Logger
	lastSeenPIDs       map[int32]*sessionSnapshot
	planCollection     bool
	slowQueryThreshold time.Duration
	// historyErrors counts failures per pipeline stage
	// (snapshot|insert|explain). Incremented ONLY on a real failure — never
	// on skips or empty results (honest metrics). Nil-safe for tests that do
	// not wire metrics.
	historyErrors *prometheus.CounterVec
	// insertTimeout / cleanupTimeout bound the statements touching the
	// distributed cloudberry_query_history table. They are struct fields (not
	// the consts directly) as a per-instance testability seam (E-2): tests
	// shrink them to drive the deadline paths without multi-second sleeps;
	// production always uses the defaults set in newHistoryCollector.
	insertTimeout  time.Duration
	cleanupTimeout time.Duration
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
}

// newHistoryCollector creates a new historyCollector. historyErrors is the
// per-stage error counter (may be nil in tests that do not assert metrics).
func newHistoryCollector(
	logger *slog.Logger,
	planCollection bool,
	slowQueryThreshold time.Duration,
	historyErrors *prometheus.CounterVec,
) *historyCollector {
	return &historyCollector{
		logger:             logger.With("component", "history-collector"),
		lastSeenPIDs:       make(map[int32]*sessionSnapshot),
		planCollection:     planCollection,
		slowQueryThreshold: slowQueryThreshold,
		historyErrors:      historyErrors,
		insertTimeout:      historyInsertTimeout,
		cleanupTimeout:     historyCleanupTimeout,
	}
}

// recordError increments the per-stage history error counter (nil-safe).
func (hc *historyCollector) recordError(stage string) {
	if hc.historyErrors != nil {
		hc.historyErrors.WithLabelValues(stage).Inc()
	}
}

// historyTableDDL is the DDL for creating the query history table. The
// definition lives in the shared const-only internal/dbschema package so the
// exporter and the operator db client cannot drift (T19/D3).
const historyTableDDL = dbschema.QueryHistoryDDL

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
		hc.recordError(historyStageSnapshot)
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
			hc.recordError(historyStageSnapshot)
			hc.logger.Warn("failed to scan session snapshot row", "error", scanErr)
			continue
		}
		currentPIDs[s.PID] = &s
	}

	if rowErr := rows.Err(); rowErr != nil {
		hc.recordError(historyStageSnapshot)
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
		hc.recordError(historyStageInsert)
		hc.logger.Warn("failed to insert query history entry",
			"pid", pid, "queryId", queryID, "error", insertErr)
		return false
	}

	return true
}

// insertHistoryEntry writes a completed query row, with or without an EXPLAIN plan.
//
// The Exec is bounded by insertTimeout (same client-side deadline pattern as
// snapshotSessions): cloudberry_query_history is a distributed table, so a
// degraded interconnect turns the dispatched INSERT into an indefinite hang —
// the deadline makes it fail fast instead, and the caller surfaces the failure
// as cloudberry_query_exporter_history_errors_total{stage="insert"}.
//
// resource_group is bound as an explicit ” literal: pg_stat_activity's
// rsgname is not part of the snapshot query, so there is no honest value to
// record — an always-empty struct field previously pretended otherwise
// (T18/D2, option (a); populating from rsgname is a recorded follow-up).
func (hc *historyCollector) insertHistoryEntry(
	ctx context.Context, conn *pgx.Conn, queryID string, pid int32,
	prevSession *sessionSnapshot, now time.Time, durationMs float64, explainPlan string,
) error {
	insertCtx, cancel := context.WithTimeout(ctx, hc.insertTimeout)
	defer cancel()

	const resourceGroup = ""
	if explainPlan != "" {
		_, err := conn.Exec(insertCtx, insertHistoryWithPlanSQL,
			queryID, pid, prevSession.Username, prevSession.Database,
			prevSession.QueryText, prevSession.QueryStart, now,
			durationMs, "completed", prevSession.WaitEventType,
			resourceGroup, explainPlan,
		)
		return err
	}

	_, err := conn.Exec(insertCtx, insertHistorySQL,
		queryID, pid, prevSession.Username, prevSession.Database,
		prevSession.QueryText, prevSession.QueryStart, now,
		durationMs, "completed", prevSession.WaitEventType,
		resourceGroup,
	)
	return err
}

// explainTimeout bounds both the client-side EXPLAIN round-trip and the
// server-side statement_timeout inside the sandbox transaction, keeping the
// two budgets aligned.
const explainTimeout = 2 * time.Second

// stripLeadingSQLComments removes leading whitespace, line comments (--) and
// (nested) block comments (/* */) from the query head, so a comment prefix
// cannot smuggle a non-SELECT statement past the allowlist
// (e.g. "/*x*/DROP TABLE t"). An unterminated block comment yields "" (never
// allowlisted).
func stripLeadingSQLComments(q string) string {
	for {
		q = strings.TrimLeft(q, " \t\r\n")
		switch {
		case strings.HasPrefix(q, "--"):
			idx := strings.IndexByte(q, '\n')
			if idx < 0 {
				return ""
			}
			q = q[idx+1:]
		case strings.HasPrefix(q, "/*"):
			depth, i := 1, 2
			for i < len(q) && depth > 0 {
				switch {
				case strings.HasPrefix(q[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(q[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
			}
			if depth > 0 {
				return ""
			}
			q = q[i:]
		default:
			return q
		}
	}
}

// hasKeywordPrefix reports whether the upper-cased statement starts with the
// keyword followed by a non-identifier character (or end of input), so
// "SELECTx" does not match "SELECT".
func hasKeywordPrefix(upper, keyword string) bool {
	if !strings.HasPrefix(upper, keyword) {
		return false
	}
	if len(upper) == len(keyword) {
		return true
	}
	c := upper[len(keyword)]
	isIdent := (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
	return !isIdent
}

// isExplainSafe reports whether the query is ALLOWLISTED for EXPLAIN: after
// stripping leading comments, only SELECT and WITH statements qualify.
// Everything else (DML, DDL, utility, unknown) is rejected — a blocklist is
// bypassable, an allowlist is not.
//
// Multi-statement hardening (F-4.8c): a top-level ';' followed by further
// statement content is ALSO rejected. Under the operator-built DSN pgx's
// extended protocol refuses multi-statement text, but a user-supplied DSN
// with default_query_exec_mode=simple_protocol would happily run
// "SELECT 1; ROLLBACK; DROP ..." — where the embedded ROLLBACK would END the
// read-only sandbox and let the trailing statement run OUTSIDE it. Rejecting
// the separator here keeps the allowlist protocol-independent.
func isExplainSafe(queryText string) bool {
	head := strings.ToUpper(stripLeadingSQLComments(queryText))
	if !hasKeywordPrefix(head, "SELECT") && !hasKeywordPrefix(head, "WITH") {
		return false
	}
	return !hasTopLevelStatementSeparator(queryText)
}

// hasTopLevelStatementSeparator reports whether the query contains a
// semicolon OUTSIDE single-quoted string literals, double-quoted identifiers
// and comments that is followed by further statement content — i.e. a real
// multi-statement separator. A trailing ';' followed only by whitespace,
// comments or more semicolons is NOT a separator: pg_stat_activity routinely
// keeps the trailing semicolon of interactively typed queries.
//
// The scanner understands the SQL doubled-quote escape (” / "") and nested
// block comments. It intentionally does NOT understand dollar-quoting or
// E” backslash escapes: a ';' inside those constructs may be seen as
// top-level and reject the query — the fail-SAFE direction for an allowlist
// (the plan is skipped; nothing unsafe can slip through).
func hasTopLevelStatementSeparator(q string) bool {
	sawSeparator := false
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		switch {
		case c == '-' && i+1 < n && q[i+1] == '-':
			// Line comment: never statement content; skip to end of line.
			next, atEnd := skipSQLLineComment(q, i)
			if atEnd {
				return false // comment runs to end of input
			}
			i = next
		case c == '/' && i+1 < n && q[i+1] == '*':
			// (Nested) block comment: never statement content. An
			// unterminated comment consumes the rest of the input (the
			// allowlist head check already rejects comment-only heads).
			i = skipSQLBlockComment(q, i)
		case isSQLSpace(c):
			i++
		case c == ';':
			sawSeparator = true
			i++
		case sawSeparator:
			// Any non-comment, non-whitespace token after a top-level
			// semicolon is a second statement.
			return true
		case c == '\'' || c == '"':
			i = skipSQLQuoted(q, i, c)
		default:
			i++
		}
	}
	return false
}

// isSQLSpace reports whether c is SQL whitespace.
func isSQLSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v'
}

// skipSQLLineComment advances past a "--" line comment starting at q[start].
// It returns the index just after the terminating newline, or (len(q), true)
// when the comment runs to the end of the input.
func skipSQLLineComment(q string, start int) (next int, atEnd bool) {
	idx := strings.IndexByte(q[start:], '\n')
	if idx < 0 {
		return len(q), true
	}
	return start + idx + 1, false
}

// skipSQLBlockComment advances past a (nested) "/* */" block comment starting
// at q[start]. An unterminated comment consumes the rest of the input.
func skipSQLBlockComment(q string, start int) int {
	depth, i, n := 1, start+2, len(q)
	for i < n && depth > 0 {
		switch {
		case strings.HasPrefix(q[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(q[i:], "*/"):
			depth--
			i += 2
		default:
			i++
		}
	}
	return i
}

// skipSQLQuoted advances past a quoted region starting at q[start] == quote,
// honoring the SQL doubled-quote escape (” inside literals, "" inside
// quoted identifiers). It returns the index just after the closing quote, or
// len(q) when the region is unterminated.
func skipSQLQuoted(q string, start int, quote byte) int {
	i, n := start+1, len(q)
	for i < n {
		if q[i] != quote {
			i++
			continue
		}
		if i+1 < n && q[i+1] == quote {
			i += 2 // doubled quote: escaped, still inside
			continue
		}
		return i + 1
	}
	return n
}

// collectExplainPlan attempts to collect an EXPLAIN plan for a completed
// query. The raw query text came from pg_stat_activity (arbitrary user SQL),
// so it is (1) allowlisted to SELECT/WITH after comment stripping and
// (2) re-planned only inside a sandbox: BEGIN READ ONLY + SET LOCAL
// statement_timeout, with ROLLBACK on ALL paths so the shared connection is
// never left inside a transaction. pgx's extended protocol additionally
// refuses multi-statement text; the read-only transaction is the backstop.
func (hc *historyCollector) collectExplainPlan(ctx context.Context, conn *pgx.Conn, queryText string) string {
	if !isExplainSafe(queryText) {
		// Not an error: non-allowlisted statements are silently skipped.
		return ""
	}

	explainCtx, cancel := context.WithTimeout(ctx, explainTimeout)
	defer cancel()

	if _, err := conn.Exec(explainCtx, "BEGIN READ ONLY"); err != nil {
		hc.recordError(historyStageExplain)
		hc.logger.Debug("failed to begin EXPLAIN sandbox transaction", "error", err)
		return ""
	}
	// ROLLBACK must run on every path below (success, error, timeout) or the
	// shared conn stays poisoned inside the transaction. It uses its own
	// deadline derived from the ORIGINAL ctx (WithoutCancel) because
	// explainCtx may already be expired when the EXPLAIN timed out.
	defer func() {
		rbCtx, rbCancel := context.WithTimeout(context.WithoutCancel(ctx), explainTimeout)
		defer rbCancel()
		if _, err := conn.Exec(rbCtx, "ROLLBACK"); err != nil {
			hc.logger.Debug("failed to roll back EXPLAIN sandbox transaction", "error", err)
		}
	}()

	// SET LOCAL is scoped to the sandbox transaction and aligned with the
	// client-side explainCtx budget.
	timeoutSQL := fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", explainTimeout.Milliseconds())
	if _, err := conn.Exec(explainCtx, timeoutSQL); err != nil {
		hc.recordError(historyStageExplain)
		hc.logger.Debug("failed to set EXPLAIN statement timeout", "error", err)
		return ""
	}

	return hc.runExplainQuery(explainCtx, conn, queryText)
}

// runExplainQuery executes the EXPLAIN inside the already-open sandbox
// transaction and joins the plan lines.
func (hc *historyCollector) runExplainQuery(ctx context.Context, conn *pgx.Conn, queryText string) string {
	rows, err := conn.Query(ctx, "EXPLAIN (FORMAT TEXT) "+queryText)
	if err != nil {
		hc.recordError(historyStageExplain)
		hc.logger.Debug("failed to collect EXPLAIN plan", "error", err)
		return ""
	}
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		if scanErr := rows.Scan(&line); scanErr != nil {
			hc.recordError(historyStageExplain)
			hc.logger.Debug("failed to scan EXPLAIN plan row", "error", scanErr)
			return ""
		}
		planLines = append(planLines, line)
	}

	if rowErr := rows.Err(); rowErr != nil {
		hc.recordError(historyStageExplain)
		hc.logger.Debug("error iterating EXPLAIN plan rows", "error", rowErr)
		return ""
	}

	return strings.Join(planLines, "\n")
}

// cleanupHistory deletes entries older than the retention period. The DELETE
// targets the same distributed table as the insert path, so it is bounded by
// cleanupTimeout for the same reason: a degraded interconnect must abort the
// statement instead of wedging the collect loop; the next hourly tick retries.
func (hc *historyCollector) cleanupHistory(ctx context.Context, conn *pgx.Conn, retention time.Duration) {
	if conn == nil {
		return
	}

	cleanupCtx, cancel := context.WithTimeout(ctx, hc.cleanupTimeout)
	defer cancel()

	cutoff := time.Now().Add(-retention)
	result, err := conn.Exec(cleanupCtx, "DELETE FROM cloudberry_query_history WHERE created_at < $1", cutoff)
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

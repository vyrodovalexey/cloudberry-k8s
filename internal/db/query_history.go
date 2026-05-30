// Package db provides query history storage and retrieval for the cloudberry operator.
package db

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// QueryHistoryEntry represents a completed query stored in the history table.
type QueryHistoryEntry struct {
	ID             int64     `json:"id"`
	QueryID        string    `json:"queryId"`
	PID            int32     `json:"pid"`
	Username       string    `json:"username"`
	DatabaseName   string    `json:"databaseName"`
	QueryText      string    `json:"queryText"`
	QueryStart     time.Time `json:"queryStart"`
	QueryEnd       time.Time `json:"queryEnd"`
	DurationMs     float64   `json:"durationMs"`
	State          string    `json:"state"`
	RowsAffected   int64     `json:"rowsAffected"`
	CPUTimeMs      float64   `json:"cpuTimeMs"`
	MemoryBytes    int64     `json:"memoryBytes"`
	SpillBytes     int64     `json:"spillBytes"`
	DiskReadBytes  int64     `json:"diskReadBytes"`
	DiskWriteBytes int64     `json:"diskWriteBytes"`
	WaitEvents     string    `json:"waitEvents"`
	ResourceGroup  string    `json:"resourceGroup"`
	ExplainPlan    string    `json:"explainPlan,omitempty"`
	ErrorMessage   string    `json:"errorMessage,omitempty"`
	CreatedAt      time.Time `json:"createdAt"`
}

// QueryHistoryFilter defines search/filter criteria for query history.
type QueryHistoryFilter struct {
	Pattern       string    // regex or wildcard search pattern
	PatternType   string    // "regex" or "wildcard"
	Username      string    // filter by username
	Database      string    // filter by database name
	ResourceGroup string    // filter by resource group
	State         string    // filter by state (completed, canceled, error)
	MinDuration   float64   // minimum duration in ms
	Since         time.Time // start of time range
	Until         time.Time // end of time range
	Limit         int       // pagination limit
	Offset        int       // pagination offset
}

// QueryHistoryResult contains paginated query history results.
type QueryHistoryResult struct {
	Entries []QueryHistoryEntry `json:"entries"`
	Total   int                 `json:"total"`
	Limit   int                 `json:"limit"`
	Offset  int                 `json:"offset"`
}

// Default and maximum pagination limits.
const (
	defaultHistoryLimit = 50
	maxHistoryLimit     = 100
)

// queryHistoryDDL is the DDL for creating the query history table and indexes.
const queryHistoryDDL = `
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

// queryHistoryColumns is the ordered list of columns for SELECT queries.
const queryHistoryColumns = `id, query_id, pid, username, database_name, query_text,
	query_start, query_end, duration_ms, state, rows_affected, cpu_time_ms,
	memory_bytes, spill_bytes, disk_read_bytes, disk_write_bytes,
	wait_events, resource_group, explain_plan, error_message, created_at`

// csvHeader is the CSV header for query history exports.
var csvHeader = []string{
	"query_id", "username", "database", "query_text",
	"start_time", "end_time", "duration_ms", "rows_affected",
	"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
}

// EnsureQueryHistoryTable creates the query history table and indexes if they don't exist.
func (c *pgxClient) EnsureQueryHistoryTable(ctx context.Context) error {
	if _, err := c.pool.Exec(ctx, queryHistoryDDL); err != nil {
		return fmt.Errorf("ensuring query history table: %w", err)
	}
	c.logger.Info("query history table ensured")
	return nil
}

// InsertQueryHistory inserts a single query history entry into the table.
func (c *pgxClient) InsertQueryHistory(ctx context.Context, entry *QueryHistoryEntry) error {
	query := `INSERT INTO cloudberry_query_history
		(query_id, pid, username, database_name, query_text, query_start, query_end,
		 duration_ms, state, rows_affected, cpu_time_ms, memory_bytes, spill_bytes,
		 disk_read_bytes, disk_write_bytes, wait_events, resource_group, explain_plan,
		 error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`

	if _, err := c.pool.Exec(ctx, query,
		entry.QueryID, entry.PID, entry.Username, entry.DatabaseName,
		entry.QueryText, entry.QueryStart, entry.QueryEnd,
		entry.DurationMs, entry.State, entry.RowsAffected,
		entry.CPUTimeMs, entry.MemoryBytes, entry.SpillBytes,
		entry.DiskReadBytes, entry.DiskWriteBytes,
		entry.WaitEvents, entry.ResourceGroup, entry.ExplainPlan,
		entry.ErrorMessage,
	); err != nil {
		return fmt.Errorf("inserting query history entry: %w", err)
	}

	if c.recorder != nil {
		c.recorder.RecordQueryHistoryInsert(c.metricsCluster, c.metricsNamespace)
	}

	c.logger.Debug("query history entry inserted",
		"queryId", entry.QueryID, "pid", entry.PID, "state", entry.State)
	return nil
}

// GetQueryHistory searches query history with filters and pagination.
// Returns the matching entries, total count, and any error.
func (c *pgxClient) GetQueryHistory(ctx context.Context, filter QueryHistoryFilter) ([]QueryHistoryEntry, int, error) {
	// Normalize pagination.
	limit, offset := normalizePagination(filter.Limit, filter.Offset)

	// Build WHERE clause with parameterized conditions.
	conditions, args, err := buildHistoryWhereClause(filter, c.logger)
	if err != nil {
		return nil, 0, err
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	// Count total matching entries.
	countQuery := "SELECT COUNT(*) FROM cloudberry_query_history" + whereClause
	var total int
	if scanErr := c.pool.QueryRow(ctx, countQuery, args...).Scan(&total); scanErr != nil {
		return nil, 0, fmt.Errorf("counting query history entries: %w", scanErr)
	}

	// Fetch paginated results.
	nextParam := len(args) + 1
	dataQuery := fmt.Sprintf("SELECT %s FROM cloudberry_query_history%s ORDER BY query_start DESC LIMIT $%d OFFSET $%d",
		queryHistoryColumns, whereClause, nextParam, nextParam+1)
	args = append(args, limit, offset)

	rows, queryErr := c.pool.Query(ctx, dataQuery, args...)
	if queryErr != nil {
		return nil, 0, fmt.Errorf("querying query history: %w", queryErr)
	}
	defer rows.Close()

	var entries []QueryHistoryEntry
	for rows.Next() {
		var e QueryHistoryEntry
		if scanErr := rows.Scan(
			&e.ID, &e.QueryID, &e.PID, &e.Username, &e.DatabaseName,
			&e.QueryText, &e.QueryStart, &e.QueryEnd, &e.DurationMs,
			&e.State, &e.RowsAffected, &e.CPUTimeMs, &e.MemoryBytes,
			&e.SpillBytes, &e.DiskReadBytes, &e.DiskWriteBytes,
			&e.WaitEvents, &e.ResourceGroup, &e.ExplainPlan,
			&e.ErrorMessage, &e.CreatedAt,
		); scanErr != nil {
			return nil, 0, fmt.Errorf("scanning query history row: %w", scanErr)
		}
		entries = append(entries, e)
	}

	if rowErr := rows.Err(); rowErr != nil {
		return nil, 0, fmt.Errorf("iterating query history rows: %w", rowErr)
	}

	c.logger.Info("query history retrieved",
		"total", total, "returned", len(entries), "limit", limit, "offset", offset)
	return entries, total, nil
}

// GetQueryHistoryDetail returns detailed information for a specific historical query.
func (c *pgxClient) GetQueryHistoryDetail(ctx context.Context, queryID string) (*QueryHistoryEntry, error) {
	query := fmt.Sprintf("SELECT %s FROM cloudberry_query_history WHERE query_id = $1", queryHistoryColumns)

	var e QueryHistoryEntry
	err := c.pool.QueryRow(ctx, query, queryID).Scan(
		&e.ID, &e.QueryID, &e.PID, &e.Username, &e.DatabaseName,
		&e.QueryText, &e.QueryStart, &e.QueryEnd, &e.DurationMs,
		&e.State, &e.RowsAffected, &e.CPUTimeMs, &e.MemoryBytes,
		&e.SpillBytes, &e.DiskReadBytes, &e.DiskWriteBytes,
		&e.WaitEvents, &e.ResourceGroup, &e.ExplainPlan,
		&e.ErrorMessage, &e.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("query %s not found: %w", queryID, err)
	}

	c.logger.Info("query history detail retrieved", "queryId", queryID)
	return &e, nil
}

// ExportQueryHistoryCSV writes query history matching the filter as CSV to the writer.
// It streams rows directly to the writer to avoid buffering the entire result set in memory.
func (c *pgxClient) ExportQueryHistoryCSV(ctx context.Context, filter QueryHistoryFilter, w io.Writer) error {
	// Build WHERE clause (no LIMIT/OFFSET for export — export all matching).
	conditions, args, err := buildHistoryWhereClause(filter, c.logger)
	if err != nil {
		return err
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = " WHERE " + strings.Join(conditions, " AND ")
	}

	dataQuery := fmt.Sprintf("SELECT %s FROM cloudberry_query_history%s ORDER BY query_start DESC",
		queryHistoryColumns, whereClause)

	rows, queryErr := c.pool.Query(ctx, dataQuery, args...)
	if queryErr != nil {
		return fmt.Errorf("querying query history for export: %w", queryErr)
	}
	defer rows.Close()

	csvWriter := csv.NewWriter(w)

	// Write CSV header.
	if headerErr := csvWriter.Write(csvHeader); headerErr != nil {
		return fmt.Errorf("writing CSV header: %w", headerErr)
	}

	// Stream rows.
	var rowCount int
	for rows.Next() {
		var e QueryHistoryEntry
		if scanErr := rows.Scan(
			&e.ID, &e.QueryID, &e.PID, &e.Username, &e.DatabaseName,
			&e.QueryText, &e.QueryStart, &e.QueryEnd, &e.DurationMs,
			&e.State, &e.RowsAffected, &e.CPUTimeMs, &e.MemoryBytes,
			&e.SpillBytes, &e.DiskReadBytes, &e.DiskWriteBytes,
			&e.WaitEvents, &e.ResourceGroup, &e.ExplainPlan,
			&e.ErrorMessage, &e.CreatedAt,
		); scanErr != nil {
			return fmt.Errorf("scanning query history row for export: %w", scanErr)
		}

		record := []string{
			e.QueryID,
			e.Username,
			e.DatabaseName,
			e.QueryText,
			e.QueryStart.Format(time.RFC3339),
			e.QueryEnd.Format(time.RFC3339),
			strconv.FormatFloat(e.DurationMs, 'f', 2, 64),
			strconv.FormatInt(e.RowsAffected, 10),
			strconv.FormatFloat(e.CPUTimeMs, 'f', 2, 64),
			strconv.FormatInt(e.MemoryBytes, 10),
			strconv.FormatInt(e.SpillBytes, 10),
			e.State,
		}

		if writeErr := csvWriter.Write(record); writeErr != nil {
			return fmt.Errorf("writing CSV row: %w", writeErr)
		}
		rowCount++
	}

	if rowErr := rows.Err(); rowErr != nil {
		return fmt.Errorf("iterating query history rows for export: %w", rowErr)
	}

	csvWriter.Flush()
	if flushErr := csvWriter.Error(); flushErr != nil {
		return fmt.Errorf("flushing CSV writer: %w", flushErr)
	}

	c.logger.Info("query history CSV export completed", "rows", rowCount)
	return nil
}

// CleanupQueryHistory deletes query history entries older than the retention period.
// Returns the number of deleted rows.
func (c *pgxClient) CleanupQueryHistory(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)

	result, err := c.pool.Exec(ctx, "DELETE FROM cloudberry_query_history WHERE created_at < $1", cutoff)
	if err != nil {
		return 0, fmt.Errorf("cleaning up query history: %w", err)
	}

	deleted := result.RowsAffected()
	if c.recorder != nil {
		c.recorder.RecordQueryHistoryRetentionCleanup(c.metricsCluster, c.metricsNamespace, deleted)
		c.recordQueryHistorySize(ctx)
	}
	c.logger.Info("query history cleanup completed",
		"deleted", deleted, "retention", retention.String(), "cutoff", cutoff)
	return deleted, nil
}

// recordQueryHistorySize computes the current on-disk size of the query history
// table and records it via the metrics recorder. It is nil-safe and logs (but
// does not return) any error so that metrics collection never fails the caller.
func (c *pgxClient) recordQueryHistorySize(ctx context.Context) {
	if c.recorder == nil {
		return
	}
	var sizeBytes int64
	const sizeQuery = "SELECT pg_total_relation_size('cloudberry_query_history')"
	if err := c.pool.QueryRow(ctx, sizeQuery).Scan(&sizeBytes); err != nil {
		c.logger.Debug("failed to compute query history size", "error", err)
		return
	}
	c.recorder.SetQueryHistorySizeBytes(c.metricsCluster, c.metricsNamespace, float64(sizeBytes))
}

// buildHistoryWhereClause constructs parameterized WHERE conditions from a filter.
// Returns the conditions slice, args slice, and any validation error.
func buildHistoryWhereClause(
	filter QueryHistoryFilter, logger *slog.Logger,
) (conditions []string, args []interface{}, err error) {
	paramIdx := 1

	// Pattern filter (regex or wildcard).
	if filter.Pattern != "" {
		patternType := filter.PatternType
		if patternType == "" {
			patternType = "regex"
		}

		switch patternType {
		case "regex":
			// Validate regex pattern before sending to DB to prevent ReDoS.
			if _, compileErr := regexp.Compile(filter.Pattern); compileErr != nil {
				return nil, nil, fmt.Errorf("invalid regex pattern %q: %w", filter.Pattern, compileErr)
			}
			conditions = append(conditions, fmt.Sprintf("query_text ~ $%d", paramIdx))
			args = append(args, filter.Pattern)
			paramIdx++
		case "wildcard":
			// Convert wildcard to SQL LIKE pattern: * → %, ? → _
			likePattern := convertWildcardToLike(filter.Pattern)
			conditions = append(conditions, fmt.Sprintf("query_text LIKE $%d", paramIdx))
			args = append(args, likePattern)
			paramIdx++
		default:
			logger.Warn("unknown pattern type, defaulting to regex", "patternType", patternType)
			if _, compileErr := regexp.Compile(filter.Pattern); compileErr != nil {
				return nil, nil, fmt.Errorf("invalid regex pattern %q: %w", filter.Pattern, compileErr)
			}
			conditions = append(conditions, fmt.Sprintf("query_text ~ $%d", paramIdx))
			args = append(args, filter.Pattern)
			paramIdx++
		}
	}

	// Username filter.
	if filter.Username != "" {
		conditions = append(conditions, fmt.Sprintf("username = $%d", paramIdx))
		args = append(args, filter.Username)
		paramIdx++
	}

	// Database filter.
	if filter.Database != "" {
		conditions = append(conditions, fmt.Sprintf("database_name = $%d", paramIdx))
		args = append(args, filter.Database)
		paramIdx++
	}

	// Resource group filter.
	if filter.ResourceGroup != "" {
		conditions = append(conditions, fmt.Sprintf("resource_group = $%d", paramIdx))
		args = append(args, filter.ResourceGroup)
		paramIdx++
	}

	// State filter.
	if filter.State != "" {
		conditions = append(conditions, fmt.Sprintf("state = $%d", paramIdx))
		args = append(args, filter.State)
		paramIdx++
	}

	// Minimum duration filter.
	if filter.MinDuration > 0 {
		conditions = append(conditions, fmt.Sprintf("duration_ms >= $%d", paramIdx))
		args = append(args, filter.MinDuration)
		paramIdx++
	}

	// Time range filter.
	if !filter.Since.IsZero() {
		conditions = append(conditions, fmt.Sprintf("query_start >= $%d", paramIdx))
		args = append(args, filter.Since)
		paramIdx++
	}
	if !filter.Until.IsZero() {
		conditions = append(conditions, fmt.Sprintf("query_start <= $%d", paramIdx))
		args = append(args, filter.Until)
		paramIdx++ //nolint:ineffassign // paramIdx kept for consistency
	}

	return conditions, args, nil
}

// convertWildcardToLike converts a wildcard pattern to a SQL LIKE pattern.
// * → %, ? → _, and escapes existing % and _ characters.
func convertWildcardToLike(pattern string) string {
	var b strings.Builder
	b.Grow(len(pattern) + 10)
	for _, ch := range pattern {
		switch ch {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%':
			b.WriteString("\\%")
		case '_':
			b.WriteString("\\_")
		default:
			b.WriteRune(ch)
		}
	}
	return b.String()
}

// normalizePagination applies default and maximum limits to pagination parameters.
func normalizePagination(limit, offset int) (normLimit, normOffset int) {
	normLimit = limit
	normOffset = offset
	if normLimit <= 0 {
		normLimit = defaultHistoryLimit
	}
	if normLimit > maxHistoryLimit {
		normLimit = maxHistoryLimit
	}
	if normOffset < 0 {
		normOffset = 0
	}
	return normLimit, normOffset
}

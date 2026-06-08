package db

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// ============================================================================
// capturingRecorder is a metrics.Recorder that records query-history metric
// invocations. It embeds metrics.NoopRecorder so only the relevant methods
// are overridden.
// ============================================================================

type capturingRecorder struct {
	*metrics.NoopRecorder

	mu sync.Mutex

	inserts        int
	insertCluster  string
	insertNS       string
	cleanups       int
	cleanupDeleted int64
	cleanupCluster string
	cleanupNS      string
	sizeSets       int
	sizeBytes      float64
	sizeCluster    string
	sizeNS         string
}

func newCapturingRecorder() *capturingRecorder {
	return &capturingRecorder{NoopRecorder: &metrics.NoopRecorder{}}
}

func (c *capturingRecorder) RecordQueryHistoryInsert(cluster, namespace string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.inserts++
	c.insertCluster = cluster
	c.insertNS = namespace
}

func (c *capturingRecorder) RecordQueryHistoryRetentionCleanup(cluster, namespace string, deleted int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups++
	c.cleanupDeleted = deleted
	c.cleanupCluster = cluster
	c.cleanupNS = namespace
}

func (c *capturingRecorder) SetQueryHistorySizeBytes(cluster, namespace string, b float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sizeSets++
	c.sizeBytes = b
	c.sizeCluster = cluster
	c.sizeNS = namespace
}

// historyRowFields are the typed field descriptors for a query history row in
// the order produced by queryHistoryColumns.
func historyRowFields() []fieldDesc {
	return []fieldDesc{
		int8Field("id"),
		textField("query_id"),
		int4Field("pid"),
		textField("username"),
		textField("database_name"),
		textField("query_text"),
		{name: "query_start", oid: 1184}, // timestamptz
		{name: "query_end", oid: 1184},
		float8Field("duration_ms"),
		textField("state"),
		int8Field("rows_affected"),
		float8Field("cpu_time_ms"),
		int8Field("memory_bytes"),
		int8Field("spill_bytes"),
		int8Field("disk_read_bytes"),
		int8Field("disk_write_bytes"),
		textField("wait_events"),
		textField("resource_group"),
		textField("explain_plan"),
		textField("error_message"),
		{name: "created_at", oid: 1184},
	}
}

// historyRowValues returns one row of string values matching historyRowFields.
func historyRowValues() []string {
	return []string{
		"1", "q-1", "123", "analyst", "analytics",
		"SELECT 1", "2025-01-01 00:00:00+00", "2025-01-01 00:00:01+00",
		"1000", "completed", "10", "500", "1024", "0", "2048", "0",
		"", "default", "", "", "2025-01-01 00:00:02+00",
	}
}

// ============================================================================
// EnsureQueryHistoryTable
// ============================================================================

func TestPgxClient_EnsureQueryHistoryTable_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("CREATE TABLE")
	})
	defer cleanup()

	err := client.EnsureQueryHistoryTable(context.Background())
	assert.NoError(t, err)
}

func TestPgxClient_EnsureQueryHistoryTable_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("permission denied")
	})
	defer cleanup()

	err := client.EnsureQueryHistoryTable(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensuring query history table")
}

// ============================================================================
// InsertQueryHistory
// ============================================================================

func TestPgxClient_InsertQueryHistory_Success_NoRecorder(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	entry := &QueryHistoryEntry{
		QueryID:  "q-1",
		PID:      123,
		Username: "analyst",
		State:    "completed",
	}
	err := client.InsertQueryHistory(context.Background(), entry)
	assert.NoError(t, err)
}

func TestPgxClient_InsertQueryHistory_Success_RecordsMetric(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	rec := newCapturingRecorder()
	client.SetRecorder(rec, "my-cluster", "my-ns")

	entry := &QueryHistoryEntry{QueryID: "q-1", State: "completed"}
	err := client.InsertQueryHistory(context.Background(), entry)
	require.NoError(t, err)

	assert.Equal(t, 1, rec.inserts)
	assert.Equal(t, "my-cluster", rec.insertCluster)
	assert.Equal(t, "my-ns", rec.insertNS)
}

func TestPgxClient_InsertQueryHistory_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("constraint violation")
	})
	defer cleanup()

	rec := newCapturingRecorder()
	client.SetRecorder(rec, "c", "n")

	err := client.InsertQueryHistory(context.Background(), &QueryHistoryEntry{QueryID: "q-1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inserting query history entry")
	// Metric should NOT be recorded on error.
	assert.Equal(t, 0, rec.inserts)
}

// ============================================================================
// GetQueryHistory
// ============================================================================

func TestPgxClient_GetQueryHistory_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "COUNT(*)") {
			return singleRowResponseTyped([]fieldDesc{int8Field("count")}, []string{"1"})
		}
		return multiRowResponseTyped(historyRowFields(), [][]string{historyRowValues()})
	})
	defer cleanup()

	entries, total, err := client.GetQueryHistory(context.Background(), QueryHistoryFilter{})
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, entries, 1)
	assert.Equal(t, "q-1", entries[0].QueryID)
	assert.Equal(t, "analyst", entries[0].Username)
	assert.Equal(t, "completed", entries[0].State)
}

func TestPgxClient_GetQueryHistory_WithFilters(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "COUNT(*)") {
			return singleRowResponseTyped([]fieldDesc{int8Field("count")}, []string{"0"})
		}
		buf := mustEncode(buildRowDesc(historyRowFields()))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	filter := QueryHistoryFilter{
		Username: "analyst",
		State:    "completed",
		Limit:    10,
		Offset:   0,
	}
	entries, total, err := client.GetQueryHistory(context.Background(), filter)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, entries)
}

func TestPgxClient_GetQueryHistory_InvalidRegex(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("SELECT 0")
	})
	defer cleanup()

	filter := QueryHistoryFilter{Pattern: "[invalid", PatternType: "regex"}
	_, _, err := client.GetQueryHistory(context.Background(), filter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex pattern")
}

func TestPgxClient_GetQueryHistory_CountError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("count failed")
	})
	defer cleanup()

	_, _, err := client.GetQueryHistory(context.Background(), QueryHistoryFilter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "counting query history entries")
}

func TestPgxClient_GetQueryHistory_QueryError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "COUNT(*)") {
			return singleRowResponseTyped([]fieldDesc{int8Field("count")}, []string{"5"})
		}
		return errorResponseMsg("data query failed")
	})
	defer cleanup()

	_, _, err := client.GetQueryHistory(context.Background(), QueryHistoryFilter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying query history")
}

func TestPgxClient_GetQueryHistory_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "COUNT(*)") {
			return singleRowResponseTyped([]fieldDesc{int8Field("count")}, []string{"1"})
		}
		// Return a row with the wrong column count to trigger a scan error.
		return multiRowResponseTyped(
			[]fieldDesc{int8Field("id"), textField("query_id")},
			[][]string{{"1", "q-1"}},
		)
	})
	defer cleanup()

	_, _, err := client.GetQueryHistory(context.Background(), QueryHistoryFilter{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning query history row")
}

// ============================================================================
// GetQueryHistoryDetail
// ============================================================================

func TestPgxClient_GetQueryHistoryDetail_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped(historyRowFields(), historyRowValues())
	})
	defer cleanup()

	entry, err := client.GetQueryHistoryDetail(context.Background(), "q-1")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, "q-1", entry.QueryID)
	assert.Equal(t, int32(123), entry.PID)
}

func TestPgxClient_GetQueryHistoryDetail_NotFound(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("no rows")
	})
	defer cleanup()

	_, err := client.GetQueryHistoryDetail(context.Background(), "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// ============================================================================
// ExportQueryHistoryCSV
// ============================================================================

func TestPgxClient_ExportQueryHistoryCSV_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return multiRowResponseTyped(historyRowFields(), [][]string{historyRowValues()})
	})
	defer cleanup()

	var buf bytes.Buffer
	err := client.ExportQueryHistoryCSV(context.Background(), QueryHistoryFilter{}, &buf)
	require.NoError(t, err)

	out := buf.String()
	// Header.
	assert.Contains(t, out, "query_id,username,database,query_text")
	// Data row.
	assert.Contains(t, out, "q-1")
	assert.Contains(t, out, "analyst")
}

func TestPgxClient_ExportQueryHistoryCSV_Empty(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		buf := mustEncode(buildRowDesc(historyRowFields()))
		buf = append(buf, mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})...)
		return buf
	})
	defer cleanup()

	var buf bytes.Buffer
	err := client.ExportQueryHistoryCSV(context.Background(), QueryHistoryFilter{}, &buf)
	require.NoError(t, err)
	// Only the header should be present.
	assert.Contains(t, buf.String(), "query_id,username,database")
}

func TestPgxClient_ExportQueryHistoryCSV_InvalidRegex(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("SELECT 0")
	})
	defer cleanup()

	var buf bytes.Buffer
	err := client.ExportQueryHistoryCSV(context.Background(),
		QueryHistoryFilter{Pattern: "(unclosed", PatternType: "regex"}, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex pattern")
}

func TestPgxClient_ExportQueryHistoryCSV_QueryError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("query failed")
	})
	defer cleanup()

	var buf bytes.Buffer
	err := client.ExportQueryHistoryCSV(context.Background(), QueryHistoryFilter{}, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying query history for export")
}

func TestPgxClient_ExportQueryHistoryCSV_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return multiRowResponseTyped(
			[]fieldDesc{int8Field("id"), textField("query_id")},
			[][]string{{"1", "q-1"}},
		)
	})
	defer cleanup()

	var buf bytes.Buffer
	err := client.ExportQueryHistoryCSV(context.Background(), QueryHistoryFilter{}, &buf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning query history row for export")
}

// failingWriter fails after allowing a fixed number of successful writes.
type failingWriter struct {
	allow int
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.allow <= 0 {
		return 0, assertWriteErr
	}
	f.allow--
	return len(p), nil
}

var assertWriteErr = &writeError{}

type writeError struct{}

func (*writeError) Error() string { return "write failed" }

func TestPgxClient_ExportQueryHistoryCSV_HeaderWriteError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return multiRowResponseTyped(historyRowFields(), [][]string{historyRowValues()})
	})
	defer cleanup()

	// Writer fails immediately, so writing the CSV header fails.
	w := &failingWriter{allow: 0}
	err := client.ExportQueryHistoryCSV(context.Background(), QueryHistoryFilter{}, w)
	require.Error(t, err)
	// Header write or flush error path.
	assert.True(t,
		strings.Contains(err.Error(), "writing CSV header") ||
			strings.Contains(err.Error(), "writing CSV row") ||
			strings.Contains(err.Error(), "flushing CSV writer"),
		"expected CSV write/flush error, got: %v", err)
}

// ============================================================================
// CleanupQueryHistory
// ============================================================================

func TestPgxClient_CleanupQueryHistory_Success_NoRecorder(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("DELETE 5")
	})
	defer cleanup()

	deleted, err := client.CleanupQueryHistory(context.Background(), 24*time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(5), deleted)
}

func TestPgxClient_CleanupQueryHistory_Success_RecordsMetrics(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if strings.Contains(query, "pg_total_relation_size") {
			return singleRowResponseTyped([]fieldDesc{int8Field("size")}, []string{"4096"})
		}
		return execResponse("DELETE 3")
	})
	defer cleanup()

	rec := newCapturingRecorder()
	client.SetRecorder(rec, "c1", "ns1")

	deleted, err := client.CleanupQueryHistory(context.Background(), time.Hour)
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	assert.Equal(t, 1, rec.cleanups)
	assert.Equal(t, int64(3), rec.cleanupDeleted)
	assert.Equal(t, "c1", rec.cleanupCluster)
	assert.Equal(t, "ns1", rec.cleanupNS)
	// recordQueryHistorySize is invoked, which sets the size gauge.
	assert.Equal(t, 1, rec.sizeSets)
	assert.Equal(t, float64(4096), rec.sizeBytes)
}

func TestPgxClient_CleanupQueryHistory_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("delete failed")
	})
	defer cleanup()

	_, err := client.CleanupQueryHistory(context.Background(), time.Hour)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleaning up query history")
}

// ============================================================================
// recordQueryHistorySize
// ============================================================================

func TestPgxClient_RecordQueryHistorySize_NilRecorder(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped([]fieldDesc{int8Field("size")}, []string{"4096"})
	})
	defer cleanup()

	// No recorder configured: must be a no-op and must not panic.
	client.recordQueryHistorySize(context.Background())
}

func TestPgxClient_RecordQueryHistorySize_Success(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped([]fieldDesc{int8Field("size")}, []string{"8192"})
	})
	defer cleanup()

	rec := newCapturingRecorder()
	client.SetRecorder(rec, "c", "n")

	client.recordQueryHistorySize(context.Background())
	assert.Equal(t, 1, rec.sizeSets)
	assert.Equal(t, float64(8192), rec.sizeBytes)
	assert.Equal(t, "c", rec.sizeCluster)
	assert.Equal(t, "n", rec.sizeNS)
}

func TestPgxClient_RecordQueryHistorySize_QueryError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("size query failed")
	})
	defer cleanup()

	rec := newCapturingRecorder()
	client.SetRecorder(rec, "c", "n")

	// Error is logged and swallowed: the gauge must not be set.
	client.recordQueryHistorySize(context.Background())
	assert.Equal(t, 0, rec.sizeSets)
}

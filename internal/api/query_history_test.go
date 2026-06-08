package api

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// queryHistoryMockDBClient extends mockDBClient with configurable query history methods.
type queryHistoryMockDBClient struct {
	mockDBClient

	// Query history function fields.
	getQueryHistoryFunc       func(ctx context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error)
	getQueryHistoryDetailFunc func(ctx context.Context, queryID string) (*db.QueryHistoryEntry, error)
	exportQueryHistoryCSVFunc func(ctx context.Context, filter db.QueryHistoryFilter, w io.Writer) error
}

func (m *queryHistoryMockDBClient) GetQueryHistory(ctx context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	if m.getQueryHistoryFunc != nil {
		return m.getQueryHistoryFunc(ctx, filter)
	}
	return []db.QueryHistoryEntry{}, 0, nil
}

func (m *queryHistoryMockDBClient) GetQueryHistoryDetail(ctx context.Context, queryID string) (*db.QueryHistoryEntry, error) {
	if m.getQueryHistoryDetailFunc != nil {
		return m.getQueryHistoryDetailFunc(ctx, queryID)
	}
	return nil, fmt.Errorf("query %s not found", queryID)
}

func (m *queryHistoryMockDBClient) ExportQueryHistoryCSV(ctx context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
	if m.exportQueryHistoryCSVFunc != nil {
		return m.exportQueryHistoryCSVFunc(ctx, filter, w)
	}
	return nil
}

// newQueryHistoryTestServer creates a test server with a configurable query history mock.
func newQueryHistoryTestServer(dbClient db.Client, clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	factory := &mockDBFactory{client: dbClient}
	return NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0)
}

// ============================================================================
// handleGetQueryHistory Tests
// ============================================================================

func TestHandleGetQueryHistory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	now := time.Now()

	entries := []db.QueryHistoryEntry{
		{
			QueryID:      "q-1",
			Username:     "analyst",
			DatabaseName: "mydb",
			QueryText:    "SELECT * FROM orders",
			QueryStart:   now.Add(-time.Minute),
			QueryEnd:     now,
			DurationMs:   60000,
			State:        "completed",
		},
		{
			QueryID:      "q-2",
			Username:     "admin",
			DatabaseName: "mydb",
			QueryText:    "UPDATE users SET active = true",
			QueryStart:   now.Add(-2 * time.Minute),
			QueryEnd:     now.Add(-time.Minute),
			DurationMs:   30000,
			State:        "completed",
		},
	}

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			return entries, 2, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/queries/history?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(2), resp["total"])
	assert.Equal(t, float64(50), resp["limit"])
	assert.Equal(t, float64(0), resp["offset"])

	items, ok := resp["items"].([]interface{})
	require.True(t, ok)
	assert.Len(t, items, 2)
}

func TestHandleGetQueryHistory_EmptyResult(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			return nil, 0, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/queries/history?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(0), resp["total"])

	items, ok := resp["items"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, items)
}

func TestHandleGetQueryHistory_WithFilters(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	var capturedFilter db.QueryHistoryFilter
	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			capturedFilter = filter
			return []db.QueryHistoryEntry{}, 0, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&pattern=SELECT.*&patternType=regex&user=analyst&database=mydb&state=completed&limit=10&offset=5",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "SELECT.*", capturedFilter.Pattern)
	assert.Equal(t, "regex", capturedFilter.PatternType)
	assert.Equal(t, "analyst", capturedFilter.Username)
	assert.Equal(t, "mydb", capturedFilter.Database)
	assert.Equal(t, "completed", capturedFilter.State)
	assert.Equal(t, 10, capturedFilter.Limit)
	assert.Equal(t, 5, capturedFilter.Offset)
}

func TestHandleGetQueryHistory_Pagination(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			return []db.QueryHistoryEntry{{QueryID: "q-1"}}, 100, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&limit=10&offset=20",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(100), resp["total"])
	assert.Equal(t, float64(10), resp["limit"])
	assert.Equal(t, float64(20), resp["offset"])
}

func TestHandleGetQueryHistory_InvalidRegex(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&pattern=[invalid&patternType=regex",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	// Error response has nested structure: {"error": {"code": "...", "message": "..."}}
	errorObj, ok := resp["error"].(map[string]interface{})
	require.True(t, ok, "response should have 'error' field")
	assert.Contains(t, errorObj["message"].(string), "invalid regex pattern")
}

func TestHandleGetQueryHistory_InvalidLimit(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&limit=abc",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleGetQueryHistory_InvalidOffset(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&offset=-1",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleGetQueryHistory_ClusterNotFound(t *testing.T) {
	// No clusters registered.
	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/queries/history?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetQueryHistory_DBNotAvailable(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	// Server with DB factory that returns error.
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleGetQueryHistory_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	// Server without DB factory.
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(0), resp["total"])
	assert.Equal(t, msgDBNotAvailable, resp["message"])
}

func TestHandleGetQueryHistory_WithSinceAsDuration(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	var capturedFilter db.QueryHistoryFilter
	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			capturedFilter = filter
			return []db.QueryHistoryEntry{}, 0, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&since=24h",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	// Since should be approximately 24 hours ago.
	assert.False(t, capturedFilter.Since.IsZero())
	assert.WithinDuration(t, time.Now().Add(-24*time.Hour), capturedFilter.Since, 5*time.Second)
}

func TestHandleGetQueryHistory_WithSinceAsRFC3339(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	var capturedFilter db.QueryHistoryFilter
	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			capturedFilter = filter
			return []db.QueryHistoryEntry{}, 0, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	sinceTime := "2026-05-28T00:00:00Z"
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&since="+sinceTime,
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	expected, _ := time.Parse(time.RFC3339, sinceTime)
	assert.Equal(t, expected, capturedFilter.Since)
}

func TestHandleGetQueryHistory_WithMinDuration(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	var capturedFilter db.QueryHistoryFilter
	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			capturedFilter = filter
			return []db.QueryHistoryEntry{}, 0, nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default&minDuration=1000.5",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1000.5, capturedFilter.MinDuration)
}

func TestHandleGetQueryHistory_DBError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			return nil, 0, fmt.Errorf("database error")
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistory(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ============================================================================
// handleGetQueryHistoryDetail Tests
// ============================================================================

func TestHandleGetQueryHistoryDetail_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	now := time.Now()

	entry := &db.QueryHistoryEntry{
		QueryID:      "q-1234-5678",
		PID:          12345,
		Username:     "analyst",
		DatabaseName: "analytics",
		QueryText:    "SELECT * FROM orders WHERE amount > 100",
		QueryStart:   now.Add(-time.Minute),
		QueryEnd:     now,
		DurationMs:   60000,
		State:        "completed",
		RowsAffected: 1500,
		CPUTimeMs:    45000,
		MemoryBytes:  1073741824,
		ExplainPlan:  "Seq Scan on orders\n  Filter: (amount > 100)",
	}

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			if queryID == "q-1234-5678" {
				return entry, nil
			}
			return nil, fmt.Errorf("query %s not found", queryID)
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history/q-1234-5678?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("qid", "q-1234-5678")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp db.QueryHistoryEntry
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "q-1234-5678", resp.QueryID)
	assert.Equal(t, "analyst", resp.Username)
	assert.Equal(t, float64(60000), resp.DurationMs)
	assert.Contains(t, resp.ExplainPlan, "Seq Scan")
}

func TestHandleGetQueryHistoryDetail_NotFound(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			return nil, fmt.Errorf("query %s not found", queryID)
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history/nonexistent?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("qid", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	// Error response has nested structure: {"error": {"code": "...", "message": "..."}}
	errorObj, ok := resp["error"].(map[string]interface{})
	require.True(t, ok, "response should have 'error' field")
	assert.Contains(t, errorObj["message"].(string), "not found")
}

func TestHandleGetQueryHistoryDetail_EmptyQID(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history/?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("qid", "")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleGetQueryHistoryDetail_ClusterNotFound(t *testing.T) {
	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/queries/history/q-1?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("qid", "q-1")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetQueryHistoryDetail_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	// Server without DB factory.
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history/q-1?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("qid", "q-1")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleGetQueryHistoryDetail_DBConnectionError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history/q-1?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("qid", "q-1")
	rec := httptest.NewRecorder()
	s.handleGetQueryHistoryDetail(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

// ============================================================================
// handleExportQueryHistory Tests
// ============================================================================

func TestHandleExportQueryHistory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	now := time.Now()

	dbClient := &queryHistoryMockDBClient{
		exportQueryHistoryCSVFunc: func(_ context.Context, _ db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id", "username", "database", "query_text",
				"start_time", "end_time", "duration_ms", "rows_affected",
				"cpu_time_ms", "memory_bytes", "spill_bytes", "state"})
			_ = csvWriter.Write([]string{
				"q-1", "analyst", "mydb", "SELECT 1",
				now.Format(time.RFC3339), now.Format(time.RFC3339),
				"100.00", "10", "50.00", "1024", "0", "completed",
			})
			csvWriter.Flush()
			return nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/csv", rec.Header().Get("Content-Type"))
	assert.Equal(t, `attachment; filename="query-history.csv"`, rec.Header().Get("Content-Disposition"))

	// Verify CSV content.
	body := rec.Body.String()
	assert.Contains(t, body, "query_id")
	assert.Contains(t, body, "q-1")
}

func TestHandleExportQueryHistory_ContentType(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		exportQueryHistoryCSVFunc: func(_ context.Context, _ db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id"})
			csvWriter.Flush()
			return nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/csv", rec.Header().Get("Content-Type"))
}

func TestHandleExportQueryHistory_ContentDisposition(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		exportQueryHistoryCSVFunc: func(_ context.Context, _ db.QueryHistoryFilter, w io.Writer) error {
			return nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, `attachment; filename="query-history.csv"`, rec.Header().Get("Content-Disposition"))
}

func TestHandleExportQueryHistory_EmptyResult(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		exportQueryHistoryCSVFunc: func(_ context.Context, _ db.QueryHistoryFilter, w io.Writer) error {
			// Write only header, no data rows.
			csvWriter := csv.NewWriter(w)
			_ = csvWriter.Write([]string{"query_id", "username", "database", "query_text",
				"start_time", "end_time", "duration_ms", "rows_affected",
				"cpu_time_ms", "memory_bytes", "spill_bytes", "state"})
			csvWriter.Flush()
			return nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Parse CSV — should have header only.
	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, records, 1) // Header only.
	assert.Equal(t, "query_id", records[0][0])
}

func TestHandleExportQueryHistory_WithFilters(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	var capturedFilter db.QueryHistoryFilter
	dbClient := &queryHistoryMockDBClient{
		exportQueryHistoryCSVFunc: func(_ context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
			capturedFilter = filter
			return nil
		},
	}

	s := newQueryHistoryTestServer(dbClient, cluster)

	body := `{"pattern":"SELECT.*","patternType":"regex","user":"analyst","database":"mydb","since":"2026-05-28T00:00:00Z"}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "SELECT.*", capturedFilter.Pattern)
	assert.Equal(t, "regex", capturedFilter.PatternType)
	assert.Equal(t, "analyst", capturedFilter.Username)
	assert.Equal(t, "mydb", capturedFilter.Database)
	assert.False(t, capturedFilter.Since.IsZero())
}

func TestHandleExportQueryHistory_ClusterNotFound(t *testing.T) {
	dbClient := &queryHistoryMockDBClient{}
	s := newQueryHistoryTestServer(dbClient)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleExportQueryHistory_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	// Server without DB factory.
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleExportQueryHistory_DBConnectionError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/history/export?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleExportQueryHistory(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

// ============================================================================
// parseSinceTime Tests
// ============================================================================

func TestParseSinceTime_RFC3339(t *testing.T) {
	result := parseSinceTime("2026-05-28T00:00:00Z")
	expected := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, result)
}

func TestParseSinceTime_GoDuration(t *testing.T) {
	result := parseSinceTime("24h")
	assert.WithinDuration(t, time.Now().Add(-24*time.Hour), result, 5*time.Second)
}

func TestParseSinceTime_ShortDuration(t *testing.T) {
	result := parseSinceTime("30m")
	assert.WithinDuration(t, time.Now().Add(-30*time.Minute), result, 5*time.Second)
}

func TestParseSinceTime_Invalid(t *testing.T) {
	result := parseSinceTime("invalid-time")
	assert.True(t, result.IsZero())
}

func TestParseSinceTime_Empty(t *testing.T) {
	result := parseSinceTime("")
	assert.True(t, result.IsZero())
}

// ============================================================================
// parseQueryHistoryFilter Tests
// ============================================================================

func TestParseQueryHistoryFilter_AllParams(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet,
		"/test?pattern=SELECT.*&patternType=wildcard&user=admin&database=prod&resourceGroup=analytics&state=error&limit=25&offset=10&minDuration=500&since=2026-05-28T00:00:00Z&until=2026-05-29T00:00:00Z",
		nil)

	filter, err := parseQueryHistoryFilter(req)
	require.NoError(t, err)

	assert.Equal(t, "SELECT.*", filter.Pattern)
	assert.Equal(t, "wildcard", filter.PatternType)
	assert.Equal(t, "admin", filter.Username)
	assert.Equal(t, "prod", filter.Database)
	assert.Equal(t, "analytics", filter.ResourceGroup)
	assert.Equal(t, "error", filter.State)
	assert.Equal(t, 25, filter.Limit)
	assert.Equal(t, 10, filter.Offset)
	assert.Equal(t, 500.0, filter.MinDuration)
	assert.False(t, filter.Since.IsZero())
	assert.False(t, filter.Until.IsZero())
}

func TestParseQueryHistoryFilter_InvalidUntil(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test?until=not-a-date", nil)

	_, err := parseQueryHistoryFilter(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid until parameter")
}

func TestParseQueryHistoryFilter_NegativeLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test?limit=-5", nil)

	_, err := parseQueryHistoryFilter(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid limit parameter")
}

func TestParseQueryHistoryFilter_NegativeOffset(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test?offset=-1", nil)

	_, err := parseQueryHistoryFilter(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid offset parameter")
}

func TestParseQueryHistoryFilter_InvalidMinDuration(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test?minDuration=abc", nil)

	_, err := parseQueryHistoryFilter(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid minDuration parameter")
}

func TestParseQueryHistoryFilter_NegativeMinDuration(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/test?minDuration=-100", nil)

	_, err := parseQueryHistoryFilter(req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid minDuration parameter")
}

// ============================================================================
// Integration-style route tests
// ============================================================================

func TestQueryHistoryRoutes_WithAuth(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	dbClient := &queryHistoryMockDBClient{
		getQueryHistoryFunc: func(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			return []db.QueryHistoryEntry{}, 0, nil
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{client: dbClient}

	// Create server WITH auth middleware — operator user.
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "operator", Permission: auth.PermissionOperatorBasic},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := NewServer(k8sClient, mw, factory, &metrics.NoopRecorder{}, nil, 0)
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/history?namespace=default", nil)
	req.SetBasicAuth("operator", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// OperatorBasic should be allowed.
	assert.Equal(t, http.StatusOK, rec.Code)
}

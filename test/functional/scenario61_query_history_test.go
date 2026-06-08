//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 61: Query History API
// ============================================================================
//
// This scenario verifies the Query History API endpoints:
//   - 61a: Browse history with charts (list completed queries, verify duration/resource fields)
//   - 61b: Advanced search (regex pattern, wildcard, combined filters)
//   - 61c: Export to CSV (correct headers, data rows, content type)
//   - 61d: Historical query details (execution metrics, saved plan)
//
// ============================================================================

const (
	scenario61APIPrefix = "/api/v1alpha1"
	scenario61Cluster   = "test-cluster"
	scenario61User      = "admin"
	scenario61Pass      = "admin-pass"
)

// sampleHistoryEntries returns a set of realistic query history entries for testing.
func sampleHistoryEntries() []db.QueryHistoryEntry {
	now := time.Now()
	return []db.QueryHistoryEntry{
		{
			ID:             1,
			QueryID:        "q-100-1000",
			PID:            100,
			Username:       "analyst",
			DatabaseName:   "mydb",
			QueryText:      "SELECT * FROM orders WHERE total > 1000",
			QueryStart:     now.Add(-2 * time.Hour),
			QueryEnd:       now.Add(-2*time.Hour + 30*time.Second),
			DurationMs:     30000,
			State:          "completed",
			RowsAffected:   150,
			CPUTimeMs:      12500,
			MemoryBytes:    67108864,
			SpillBytes:     0,
			DiskReadBytes:  1048576,
			DiskWriteBytes: 0,
			WaitEvents:     "",
			ResourceGroup:  "analytics",
			ExplainPlan:    "Seq Scan on orders\n  Filter: (total > 1000)\n  Rows Removed by Filter: 500",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-2 * time.Hour),
		},
		{
			ID:             2,
			QueryID:        "q-101-2000",
			PID:            101,
			Username:       "analyst",
			DatabaseName:   "analytics_db",
			QueryText:      "SELECT count(*) FROM events GROUP BY event_type",
			QueryStart:     now.Add(-1 * time.Hour),
			QueryEnd:       now.Add(-1*time.Hour + 5*time.Second),
			DurationMs:     5000,
			State:          "completed",
			RowsAffected:   25,
			CPUTimeMs:      3200,
			MemoryBytes:    16777216,
			SpillBytes:     4194304,
			DiskReadBytes:  524288,
			DiskWriteBytes: 0,
			WaitEvents:     "IO:DataFileRead",
			ResourceGroup:  "analytics",
			ExplainPlan:    "",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-1 * time.Hour),
		},
		{
			ID:             3,
			QueryID:        "q-102-3000",
			PID:            102,
			Username:       "etl_service",
			DatabaseName:   "warehouse",
			QueryText:      "INSERT INTO staging SELECT * FROM raw_data",
			QueryStart:     now.Add(-30 * time.Minute),
			QueryEnd:       now.Add(-30*time.Minute + 120*time.Second),
			DurationMs:     120000,
			State:          "completed",
			RowsAffected:   50000,
			CPUTimeMs:      45000,
			MemoryBytes:    268435456,
			SpillBytes:     134217728,
			DiskReadBytes:  10485760,
			DiskWriteBytes: 52428800,
			WaitEvents:     "IO:DataFileWrite",
			ResourceGroup:  "etl",
			ExplainPlan:    "",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-30 * time.Minute),
		},
	}
}

// Scenario61QueryHistorySuite tests the Query History API.
type Scenario61QueryHistorySuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
}

func TestFunctional_Scenario61(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario61QueryHistorySuite))
}

func (s *Scenario61QueryHistorySuite) SetupTest() {
	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario61Cluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringEnabled(true).
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	entries := sampleHistoryEntries()

	// Create mock DB client with query history functions.
	mockClient := &testutil.MockDBClient{
		GetQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
			// Apply filters to sample entries.
			var filtered []db.QueryHistoryEntry
			for _, e := range entries {
				if filter.Username != "" && e.Username != filter.Username {
					continue
				}
				if filter.Database != "" && e.DatabaseName != filter.Database {
					continue
				}
				if filter.ResourceGroup != "" && e.ResourceGroup != filter.ResourceGroup {
					continue
				}
				if filter.Pattern != "" && filter.PatternType == "regex" {
					if !strings.Contains(e.QueryText, "orders") && filter.Pattern == "SELECT.*FROM orders" {
						continue
					}
				}
				if filter.Pattern != "" && filter.PatternType == "wildcard" {
					// Simple wildcard simulation: "SELECT *" matches queries starting with SELECT.
					if !strings.HasPrefix(e.QueryText, "SELECT") {
						continue
					}
				}
				if !filter.Since.IsZero() && e.QueryStart.Before(filter.Since) {
					continue
				}
				if !filter.Until.IsZero() && e.QueryStart.After(filter.Until) {
					continue
				}
				filtered = append(filtered, e)
			}

			total := len(filtered)

			// Apply pagination.
			limit := filter.Limit
			if limit <= 0 {
				limit = 50
			}
			if limit > 100 {
				limit = 100
			}
			offset := filter.Offset
			if offset < 0 {
				offset = 0
			}
			if offset > len(filtered) {
				filtered = nil
			} else {
				end := offset + limit
				if end > len(filtered) {
					end = len(filtered)
				}
				filtered = filtered[offset:end]
			}

			if filtered == nil {
				filtered = []db.QueryHistoryEntry{}
			}

			return filtered, total, nil
		},
		GetQueryHistoryDetailFunc: func(_ context.Context, queryID string) (*db.QueryHistoryEntry, error) {
			for _, e := range entries {
				if e.QueryID == queryID {
					return &e, nil
				}
			}
			return nil, fmt.Errorf("query %s not found", queryID)
		},
		ExportQueryHistoryCSVFunc: func(_ context.Context, filter db.QueryHistoryFilter, w io.Writer) error {
			csvWriter := csv.NewWriter(w)

			// Write header.
			header := []string{
				"query_id", "username", "database", "query_text",
				"start_time", "end_time", "duration_ms", "rows_affected",
				"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
			}
			if err := csvWriter.Write(header); err != nil {
				return err
			}

			// Filter and write rows.
			for _, e := range entries {
				if filter.Username != "" && e.Username != filter.Username {
					continue
				}
				if filter.Database != "" && e.DatabaseName != filter.Database {
					continue
				}
				record := []string{
					e.QueryID,
					e.Username,
					e.DatabaseName,
					e.QueryText,
					e.QueryStart.Format(time.RFC3339),
					e.QueryEnd.Format(time.RFC3339),
					fmt.Sprintf("%.2f", e.DurationMs),
					fmt.Sprintf("%d", e.RowsAffected),
					fmt.Sprintf("%.2f", e.CPUTimeMs),
					fmt.Sprintf("%d", e.MemoryBytes),
					fmt.Sprintf("%d", e.SpillBytes),
					e.State,
				}
				if err := csvWriter.Write(record); err != nil {
					return err
				}
			}

			csvWriter.Flush()
			return csvWriter.Error()
		},
	}
	mockFactory := &testutil.MockDBClientFactory{Client: mockClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario61User, scenario61Pass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario61QueryHistorySuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// doRequest creates and executes an HTTP request with basic auth credentials.
func (s *Scenario61QueryHistorySuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario61User, scenario61Pass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// doRequestNoAuth creates and executes an HTTP request without auth credentials.
func (s *Scenario61QueryHistorySuite) doRequestNoAuth(method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario61QueryHistorySuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// historyBasePath returns the base path for query history API calls.
func historyBasePath() string {
	return scenario61APIPrefix + "/clusters/" + scenario61Cluster + "/queries/history?namespace=default"
}

// historyExportPath returns the path for query history export API calls.
func historyExportPath() string {
	return scenario61APIPrefix + "/clusters/" + scenario61Cluster + "/queries/history/export?namespace=default"
}

// historyDetailPath returns the path for query history detail API calls.
func historyDetailPath(qid string) string {
	return scenario61APIPrefix + "/clusters/" + scenario61Cluster + "/queries/history/" + qid + "?namespace=default"
}

// ============================================================================
// 61a: Browse History with Charts
// ============================================================================

// TestFunctional_Scenario61a_BrowseHistory verifies that the query history
// endpoint returns completed queries with duration and resource usage fields.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61a_BrowseHistory() {
	rec := s.doRequest(http.MethodGet, historyBasePath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"GET /queries/history should return 200 OK")

	resp := s.decodeJSON(rec)

	// Verify pagination metadata.
	assert.Equal(s.T(), float64(3), resp["total"],
		"total should be 3 for all sample entries")
	assert.NotNil(s.T(), resp["items"], "response should contain 'items' array")
	assert.NotNil(s.T(), resp["limit"], "response should contain 'limit'")
	assert.NotNil(s.T(), resp["offset"], "response should contain 'offset'")

	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok, "items should be an array")
	require.Len(s.T(), items, 3, "should return all 3 entries")

	// Verify each entry has duration and resource usage fields.
	for i, item := range items {
		entry, ok := item.(map[string]interface{})
		require.True(s.T(), ok, "each item should be an object")

		assert.NotNil(s.T(), entry["durationMs"],
			"entry %d should have durationMs field", i)
		assert.NotNil(s.T(), entry["cpuTimeMs"],
			"entry %d should have cpuTimeMs field", i)
		assert.NotNil(s.T(), entry["memoryBytes"],
			"entry %d should have memoryBytes field", i)
		assert.NotNil(s.T(), entry["spillBytes"],
			"entry %d should have spillBytes field", i)
		assert.NotNil(s.T(), entry["diskReadBytes"],
			"entry %d should have diskReadBytes field", i)
		assert.NotNil(s.T(), entry["diskWriteBytes"],
			"entry %d should have diskWriteBytes field", i)
		assert.NotNil(s.T(), entry["queryId"],
			"entry %d should have queryId field", i)
		assert.NotNil(s.T(), entry["state"],
			"entry %d should have state field", i)
	}
}

// TestFunctional_Scenario61a_BrowseHistory_EmptyResult verifies that the query
// history endpoint returns an empty result when no entries match.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61a_BrowseHistory_EmptyResult() {
	// Use a filter that matches no entries.
	path := historyBasePath() + "&user=nonexistent_user"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(0), resp["total"],
		"total should be 0 for no matching entries")

	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok, "items should be an array")
	assert.Empty(s.T(), items, "items should be empty")
}

// TestFunctional_Scenario61a_BrowseHistory_Pagination verifies that pagination
// parameters (limit, offset) are correctly applied.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61a_BrowseHistory_Pagination() {
	s.Run("page1_limit2", func() {
		path := historyBasePath() + "&limit=2&offset=0"
		rec := s.doRequest(http.MethodGet, path, nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(3), resp["total"],
			"total should be 3 regardless of pagination")
		assert.Equal(s.T(), float64(2), resp["limit"])
		assert.Equal(s.T(), float64(0), resp["offset"])

		items, ok := resp["items"].([]interface{})
		require.True(s.T(), ok)
		assert.Len(s.T(), items, 2, "page 1 should return 2 entries")
	})

	s.Run("page2_limit2", func() {
		path := historyBasePath() + "&limit=2&offset=2"
		rec := s.doRequest(http.MethodGet, path, nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(3), resp["total"])

		items, ok := resp["items"].([]interface{})
		require.True(s.T(), ok)
		assert.Len(s.T(), items, 1, "page 2 should return 1 remaining entry")
	})

	s.Run("beyond_end", func() {
		path := historyBasePath() + "&limit=10&offset=100"
		rec := s.doRequest(http.MethodGet, path, nil)
		assert.Equal(s.T(), http.StatusOK, rec.Code)

		resp := s.decodeJSON(rec)
		assert.Equal(s.T(), float64(3), resp["total"])

		items, ok := resp["items"].([]interface{})
		require.True(s.T(), ok)
		assert.Empty(s.T(), items, "offset beyond total should return empty items")
	})
}

// ============================================================================
// 61b: Advanced Search
// ============================================================================

// TestFunctional_Scenario61b_RegexSearch verifies that regex pattern search
// returns matching queries.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_RegexSearch() {
	path := historyBasePath() + "&pattern=SELECT.*FROM+orders&patternType=regex"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(items), 1,
		"regex search for 'SELECT.*FROM orders' should return at least 1 match")

	// Verify the matched entry contains "orders" in query text.
	for _, item := range items {
		entry := item.(map[string]interface{})
		queryText, _ := entry["queryText"].(string)
		assert.Contains(s.T(), queryText, "orders",
			"matched entry should contain 'orders' in query text")
	}
}

// TestFunctional_Scenario61b_WildcardSearch verifies that wildcard pattern search
// returns matching queries.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_WildcardSearch() {
	path := historyBasePath() + "&pattern=SELECT+*&patternType=wildcard"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(items), 1,
		"wildcard search for 'SELECT *' should return at least 1 match")

	// Verify all matched entries start with SELECT.
	for _, item := range items {
		entry := item.(map[string]interface{})
		queryText, _ := entry["queryText"].(string)
		assert.True(s.T(), strings.HasPrefix(queryText, "SELECT"),
			"wildcard 'SELECT *' should match queries starting with SELECT, got: %s", queryText)
	}
}

// TestFunctional_Scenario61b_FilterByUser verifies filtering by username.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_FilterByUser() {
	path := historyBasePath() + "&user=analyst"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(2), resp["total"],
		"analyst has 2 queries in sample data")

	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "analyst", entry["username"],
			"all entries should belong to 'analyst'")
	}
}

// TestFunctional_Scenario61b_FilterByDatabase verifies filtering by database name.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_FilterByDatabase() {
	path := historyBasePath() + "&database=mydb"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), resp["total"],
		"only 1 query in 'mydb' database")

	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "mydb", entry["databaseName"],
			"all entries should be from 'mydb' database")
	}
}

// TestFunctional_Scenario61b_FilterByTimeRange verifies filtering by time range.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_FilterByTimeRange() {
	// Use "since=90m" to get queries from the last 90 minutes.
	path := historyBasePath() + "&since=90m"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	// Entries 2 and 3 are within the last 90 minutes (1h and 30m ago).
	total, _ := resp["total"].(float64)
	assert.GreaterOrEqual(s.T(), int(total), 1,
		"since=90m should return at least 1 recent entry")
}

// TestFunctional_Scenario61b_FilterByResourceGroup verifies filtering by resource group.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_FilterByResourceGroup() {
	path := historyBasePath() + "&resourceGroup=analytics"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)

	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "analytics", entry["resourceGroup"],
			"all entries should be in 'analytics' resource group")
	}
}

// TestFunctional_Scenario61b_CombinedFilters verifies that multiple filters
// are AND-combined.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_CombinedFilters() {
	path := historyBasePath() + "&user=analyst&database=mydb"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), float64(1), resp["total"],
		"combined user=analyst AND database=mydb should return 1 entry")

	if len(items) > 0 {
		entry := items[0].(map[string]interface{})
		assert.Equal(s.T(), "analyst", entry["username"])
		assert.Equal(s.T(), "mydb", entry["databaseName"])
	}
}

// TestFunctional_Scenario61b_InvalidRegex verifies that an invalid regex pattern
// returns 400 Bad Request.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61b_InvalidRegex() {
	path := historyBasePath() + "&pattern=[invalid&patternType=regex"
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"invalid regex should return 400 Bad Request")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"],
		"error code should be INVALID_REQUEST")
}

// ============================================================================
// 61c: Export to CSV
// ============================================================================

// TestFunctional_Scenario61c_ExportCSV verifies that the export endpoint
// generates valid CSV with header and data rows.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61c_ExportCSV() {
	rec := s.doRequest(http.MethodPost, historyExportPath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"POST /queries/history/export should return 200 OK")

	// Parse CSV.
	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	require.NoError(s.T(), err, "response body should be valid CSV")

	// Verify header + data rows.
	require.GreaterOrEqual(s.T(), len(records), 2,
		"CSV should have at least header + 1 data row")

	// Verify header columns.
	expectedHeader := []string{
		"query_id", "username", "database", "query_text",
		"start_time", "end_time", "duration_ms", "rows_affected",
		"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
	}
	assert.Equal(s.T(), expectedHeader, records[0],
		"CSV header should match specification")

	// Verify data rows count (3 sample entries).
	assert.Equal(s.T(), 3, len(records)-1,
		"CSV should have 3 data rows for 3 sample entries")
}

// TestFunctional_Scenario61c_ExportCSV_Headers verifies that the CSV export
// has the correct column headers.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61c_ExportCSV_Headers() {
	rec := s.doRequest(http.MethodPost, historyExportPath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	header, err := reader.Read()
	require.NoError(s.T(), err, "should be able to read CSV header")

	assert.Contains(s.T(), header, "query_id", "header should contain query_id")
	assert.Contains(s.T(), header, "username", "header should contain username")
	assert.Contains(s.T(), header, "database", "header should contain database")
	assert.Contains(s.T(), header, "query_text", "header should contain query_text")
	assert.Contains(s.T(), header, "start_time", "header should contain start_time")
	assert.Contains(s.T(), header, "end_time", "header should contain end_time")
	assert.Contains(s.T(), header, "duration_ms", "header should contain duration_ms")
	assert.Contains(s.T(), header, "rows_affected", "header should contain rows_affected")
	assert.Contains(s.T(), header, "cpu_time_ms", "header should contain cpu_time_ms")
	assert.Contains(s.T(), header, "memory_bytes", "header should contain memory_bytes")
	assert.Contains(s.T(), header, "spill_bytes", "header should contain spill_bytes")
	assert.Contains(s.T(), header, "state", "header should contain state")
}

// TestFunctional_Scenario61c_ExportCSV_ContentType verifies that the export
// response has the correct Content-Type header.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61c_ExportCSV_ContentType() {
	rec := s.doRequest(http.MethodPost, historyExportPath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	contentType := rec.Header().Get("Content-Type")
	assert.Equal(s.T(), "text/csv", contentType,
		"Content-Type should be text/csv")

	contentDisposition := rec.Header().Get("Content-Disposition")
	assert.Contains(s.T(), contentDisposition, "query-history.csv",
		"Content-Disposition should contain filename 'query-history.csv'")
}

// TestFunctional_Scenario61c_ExportCSV_WithFilters verifies that the export
// endpoint applies filter criteria from the request body.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61c_ExportCSV_WithFilters() {
	body, err := json.Marshal(map[string]string{
		"user": "analyst",
	})
	require.NoError(s.T(), err)

	rec := s.doRequest(http.MethodPost, historyExportPath(), body)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, csvErr := reader.ReadAll()
	require.NoError(s.T(), csvErr)

	// Header + 2 analyst entries.
	assert.Equal(s.T(), 3, len(records),
		"CSV with user=analyst filter should have header + 2 data rows")

	// Verify all data rows belong to analyst.
	for _, record := range records[1:] {
		assert.Equal(s.T(), "analyst", record[1],
			"all exported rows should belong to 'analyst'")
	}
}

// ============================================================================
// 61d: Historical Query Details
// ============================================================================

// TestFunctional_Scenario61d_HistoricalDetail verifies that the query history
// detail endpoint returns full execution metrics for a known query.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61d_HistoricalDetail() {
	path := historyDetailPath("q-100-1000")
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"GET /queries/history/{qid} should return 200 OK for existing query")

	resp := s.decodeJSON(rec)

	// Verify execution metrics.
	assert.Equal(s.T(), "q-100-1000", resp["queryId"],
		"response should contain the correct queryId")
	assert.Equal(s.T(), "completed", resp["state"],
		"response should contain the query state")
	assert.Equal(s.T(), "analyst", resp["username"],
		"response should contain the username")
	assert.Equal(s.T(), "mydb", resp["databaseName"],
		"response should contain the database name")

	// Verify duration and resource metrics.
	assert.Equal(s.T(), float64(30000), resp["durationMs"],
		"response should contain durationMs")
	assert.Equal(s.T(), float64(12500), resp["cpuTimeMs"],
		"response should contain cpuTimeMs")
	assert.Equal(s.T(), float64(67108864), resp["memoryBytes"],
		"response should contain memoryBytes")
	assert.Equal(s.T(), float64(0), resp["spillBytes"],
		"response should contain spillBytes")
	assert.Equal(s.T(), float64(1048576), resp["diskReadBytes"],
		"response should contain diskReadBytes")
	assert.Equal(s.T(), float64(0), resp["diskWriteBytes"],
		"response should contain diskWriteBytes")
	assert.Equal(s.T(), float64(150), resp["rowsAffected"],
		"response should contain rowsAffected")
}

// TestFunctional_Scenario61d_HistoricalDetail_WithPlan verifies that the query
// history detail includes the saved EXPLAIN plan when available.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61d_HistoricalDetail_WithPlan() {
	path := historyDetailPath("q-100-1000")
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)

	explainPlan, ok := resp["explainPlan"].(string)
	require.True(s.T(), ok, "response should contain 'explainPlan' field")
	assert.NotEmpty(s.T(), explainPlan,
		"explainPlan should not be empty for query with plan collection")
	assert.Contains(s.T(), explainPlan, "Seq Scan",
		"explainPlan should contain plan details")
}

// TestFunctional_Scenario61d_HistoricalDetail_NotFound verifies that the query
// history detail endpoint returns 404 for an unknown query ID.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61d_HistoricalDetail_NotFound() {
	path := historyDetailPath("q-nonexistent")
	rec := s.doRequest(http.MethodGet, path, nil)
	assert.Equal(s.T(), http.StatusNotFound, rec.Code,
		"GET /queries/history/{qid} should return 404 for unknown query")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "QUERY_NOT_FOUND", errObj["code"],
		"error code should be QUERY_NOT_FOUND")
}

// ============================================================================
// Cross-cutting: Authentication
// ============================================================================

// TestFunctional_Scenario61_Unauthenticated verifies that unauthenticated
// requests to all query history endpoints are rejected with 401.
func (s *Scenario61QueryHistorySuite) TestFunctional_Scenario61_Unauthenticated() {
	s.Run("browse_history", func() {
		rec := s.doRequestNoAuth(http.MethodGet, historyBasePath())
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated GET /queries/history should return 401")
	})

	s.Run("history_detail", func() {
		rec := s.doRequestNoAuth(http.MethodGet, historyDetailPath("q-100-1000"))
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated GET /queries/history/{qid} should return 401")
	})

	s.Run("export_csv", func() {
		rec := s.doRequestNoAuth(http.MethodPost, historyExportPath())
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
			"unauthenticated POST /queries/history/export should return 401")
	})
}

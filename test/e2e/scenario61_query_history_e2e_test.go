//go:build e2e

package e2e

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
// Scenario 61: Query History API (E2E)
// ============================================================================
//
// This E2E scenario tests the full user journeys for the Query History API:
//   1. User browses query history, sees completed queries with duration/resource
//      metrics, and paginates through results
//   2. User searches with regex pattern, then wildcard, then filters by
//      user+database+time range
//   3. User exports query history to CSV, verifies file has correct columns
//      and data
//   4. User views historical query details including execution metrics and
//      saved EXPLAIN plan
//   5. Unauthenticated user is rejected, authenticated user can access all
//      endpoints
//   6. User handles various error conditions: invalid regex, non-existent
//      query, DB unavailable
//
// The test uses a mock DB client to simulate query history responses and
// verifies the complete request/response cycle through the API server with auth.
//
// ============================================================================

const (
	scenario61E2ECluster = "e2e-cluster"
	scenario61E2EUser    = "admin"
	scenario61E2EPass    = "admin-pass"
	scenario61E2EPrefix  = "/api/v1alpha1"
)

// e2eHistoryEntries returns realistic query history entries for E2E testing.
func e2eHistoryEntries() []db.QueryHistoryEntry {
	now := time.Now()
	return []db.QueryHistoryEntry{
		{
			ID:             1,
			QueryID:        "q-5001-1000",
			PID:            5001,
			Username:       "analyst",
			DatabaseName:   "warehouse",
			QueryText:      "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 1000",
			QueryStart:     now.Add(-3 * time.Hour),
			QueryEnd:       now.Add(-3*time.Hour + 45*time.Second),
			DurationMs:     45000,
			State:          "completed",
			RowsAffected:   2500,
			CPUTimeMs:      18000,
			MemoryBytes:    134217728,
			SpillBytes:     0,
			DiskReadBytes:  4194304,
			DiskWriteBytes: 0,
			WaitEvents:     "",
			ResourceGroup:  "analytics",
			ExplainPlan:    "Hash Join\n  Hash Cond: (o.customer_id = c.id)\n  -> Seq Scan on orders o\n       Filter: (total > 1000)\n  -> Hash\n       -> Seq Scan on customers c",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-3 * time.Hour),
		},
		{
			ID:             2,
			QueryID:        "q-5002-2000",
			PID:            5002,
			Username:       "etl_service",
			DatabaseName:   "warehouse",
			QueryText:      "INSERT INTO fact_sales SELECT * FROM staging_sales WHERE sale_date >= '2026-05-01'",
			QueryStart:     now.Add(-2 * time.Hour),
			QueryEnd:       now.Add(-2*time.Hour + 180*time.Second),
			DurationMs:     180000,
			State:          "completed",
			RowsAffected:   100000,
			CPUTimeMs:      72000,
			MemoryBytes:    536870912,
			SpillBytes:     268435456,
			DiskReadBytes:  20971520,
			DiskWriteBytes: 104857600,
			WaitEvents:     "IO:DataFileWrite",
			ResourceGroup:  "etl",
			ExplainPlan:    "",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-2 * time.Hour),
		},
		{
			ID:             3,
			QueryID:        "q-5003-3000",
			PID:            5003,
			Username:       "analyst",
			DatabaseName:   "analytics",
			QueryText:      "SELECT event_type, count(*) FROM events GROUP BY event_type ORDER BY count(*) DESC LIMIT 10",
			QueryStart:     now.Add(-1 * time.Hour),
			QueryEnd:       now.Add(-1*time.Hour + 8*time.Second),
			DurationMs:     8000,
			State:          "completed",
			RowsAffected:   10,
			CPUTimeMs:      5500,
			MemoryBytes:    33554432,
			SpillBytes:     0,
			DiskReadBytes:  2097152,
			DiskWriteBytes: 0,
			WaitEvents:     "",
			ResourceGroup:  "analytics",
			ExplainPlan:    "Limit\n  -> Sort\n       Sort Key: (count(*)) DESC\n       -> HashAggregate\n            Group Key: event_type\n            -> Seq Scan on events",
			ErrorMessage:   "",
			CreatedAt:      now.Add(-1 * time.Hour),
		},
		{
			ID:             4,
			QueryID:        "q-5004-4000",
			PID:            5004,
			Username:       "analyst",
			DatabaseName:   "warehouse",
			QueryText:      "UPDATE inventory SET qty = qty - 1 WHERE product_id = 42",
			QueryStart:     now.Add(-30 * time.Minute),
			QueryEnd:       now.Add(-30*time.Minute + 500*time.Millisecond),
			DurationMs:     500,
			State:          "error",
			RowsAffected:   0,
			CPUTimeMs:      100,
			MemoryBytes:    1048576,
			SpillBytes:     0,
			DiskReadBytes:  4096,
			DiskWriteBytes: 0,
			WaitEvents:     "Lock:transactionid",
			ResourceGroup:  "analytics",
			ExplainPlan:    "",
			ErrorMessage:   "ERROR: deadlock detected",
			CreatedAt:      now.Add(-30 * time.Minute),
		},
	}
}

// Scenario61QueryHistoryE2ESuite tests the Query History API user journeys.
type Scenario61QueryHistoryE2ESuite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	logBuf  *bytes.Buffer
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
}

func TestE2E_Scenario61(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario61QueryHistoryE2ESuite))
}

func (s *Scenario61QueryHistoryE2ESuite) SetupTest() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 60*time.Second)

	s.logBuf = &bytes.Buffer{}
	s.logger = slog.New(slog.NewTextHandler(s.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	cluster := testutil.NewClusterBuilder(scenario61E2ECluster, "default").
		WithFinalizer().
		WithStatusReady().
		WithBasicAuth(true, "gpadmin").
		WithQueryMonitoringEnabled(true).
		Build()

	k8sEnv := testutil.NewTestK8sEnv(cluster)

	entries := e2eHistoryEntries()

	// Create mock DB client simulating realistic query history responses.
	mockClient := &testutil.MockDBClient{
		GetQueryHistoryFunc: func(_ context.Context, filter db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
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
				if filter.Pattern != "" {
					patternType := filter.PatternType
					if patternType == "" {
						patternType = "regex"
					}
					switch patternType {
					case "regex":
						// Simple simulation: check if pattern keywords appear in query.
						if strings.Contains(filter.Pattern, "orders") && !strings.Contains(e.QueryText, "orders") {
							continue
						}
						if strings.Contains(filter.Pattern, "events") && !strings.Contains(e.QueryText, "events") {
							continue
						}
					case "wildcard":
						if strings.HasPrefix(filter.Pattern, "SELECT") && !strings.HasPrefix(e.QueryText, "SELECT") {
							continue
						}
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

			header := []string{
				"query_id", "username", "database", "query_text",
				"start_time", "end_time", "duration_ms", "rows_affected",
				"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
			}
			if err := csvWriter.Write(header); err != nil {
				return err
			}

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
	store.SetCredentials(scenario61E2EUser, scenario61E2EPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, s.logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, s.logger, &metrics.NoopRecorder{})

	const highRateLimit = 1000
	s.server = api.NewServer(k8sEnv.Client, authMW, mockFactory, &metrics.NoopRecorder{}, s.logger, highRateLimit)
	s.handler = s.server.Handler()
}

func (s *Scenario61QueryHistoryE2ESuite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// doRequest creates and executes an authenticated HTTP request.
func (s *Scenario61QueryHistoryE2ESuite) doRequest(method, path string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.SetBasicAuth(scenario61E2EUser, scenario61E2EPass)

	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decodeJSON decodes the response body into a map.
func (s *Scenario61QueryHistoryE2ESuite) decodeJSON(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// e2eHistoryBasePath returns the base path for E2E query history API calls.
func e2eHistoryBasePath() string {
	return scenario61E2EPrefix + "/clusters/" + scenario61E2ECluster + "/queries/history?namespace=default"
}

// e2eHistoryExportPath returns the path for E2E query history export API calls.
func e2eHistoryExportPath() string {
	return scenario61E2EPrefix + "/clusters/" + scenario61E2ECluster + "/queries/history/export?namespace=default"
}

// e2eHistoryDetailPath returns the path for E2E query history detail API calls.
func e2eHistoryDetailPath(qid string) string {
	return scenario61E2EPrefix + "/clusters/" + scenario61E2ECluster + "/queries/history/" + qid + "?namespace=default"
}

// ============================================================================
// E2E Journey: Browse History (61a)
// ============================================================================

// TestE2E_Scenario61a_BrowseHistoryJourney tests the complete user journey for
// browsing query history: list all → verify metrics → paginate → filter.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61a_BrowseHistoryJourney() {
	// Step 1: User browses all query history.
	s.T().Log("Step 1: Browse all query history")
	rec := s.doRequest(http.MethodGet, e2eHistoryBasePath(), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code, "browsing history should succeed")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), float64(4), resp["total"], "should return all 4 entries")

	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok, "response should contain 'items' array")
	require.Len(s.T(), items, 4, "should have 4 items")

	// Step 2: User verifies each entry has duration and resource metrics.
	s.T().Log("Step 2: Verify duration and resource metrics")
	for i, item := range items {
		entry := item.(map[string]interface{})
		assert.NotNil(s.T(), entry["durationMs"], "entry %d should have durationMs", i)
		assert.NotNil(s.T(), entry["cpuTimeMs"], "entry %d should have cpuTimeMs", i)
		assert.NotNil(s.T(), entry["memoryBytes"], "entry %d should have memoryBytes", i)
		assert.NotNil(s.T(), entry["spillBytes"], "entry %d should have spillBytes", i)
		assert.NotNil(s.T(), entry["diskReadBytes"], "entry %d should have diskReadBytes", i)
		assert.NotNil(s.T(), entry["diskWriteBytes"], "entry %d should have diskWriteBytes", i)
		assert.NotNil(s.T(), entry["rowsAffected"], "entry %d should have rowsAffected", i)
		assert.NotNil(s.T(), entry["queryId"], "entry %d should have queryId", i)
		assert.NotNil(s.T(), entry["state"], "entry %d should have state", i)
		assert.NotNil(s.T(), entry["username"], "entry %d should have username", i)
		assert.NotNil(s.T(), entry["databaseName"], "entry %d should have databaseName", i)
	}

	// Step 3: User paginates through results.
	s.T().Log("Step 3: Paginate through results")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&limit=2&offset=0", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), float64(4), resp["total"], "total should remain 4")
	items, _ = resp["items"].([]interface{})
	assert.Len(s.T(), items, 2, "page 1 should have 2 items")

	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&limit=2&offset=2", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, _ = resp["items"].([]interface{})
	assert.Len(s.T(), items, 2, "page 2 should have 2 items")

	// Step 4: User filters by database.
	s.T().Log("Step 4: Filter by database")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&database=warehouse", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, _ = resp["items"].([]interface{})
	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "warehouse", entry["databaseName"],
			"filtered results should only contain warehouse queries")
	}
}

// ============================================================================
// E2E Journey: Advanced Search (61b)
// ============================================================================

// TestE2E_Scenario61b_AdvancedSearchJourney tests the complete user journey for
// advanced search: regex → wildcard → user filter → combined filters.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61b_AdvancedSearchJourney() {
	// Step 1: User searches with regex pattern.
	s.T().Log("Step 1: Search with regex pattern")
	rec := s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&pattern=SELECT.*FROM+orders&patternType=regex", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp := s.decodeJSON(rec)
	items, ok := resp["items"].([]interface{})
	require.True(s.T(), ok)
	assert.GreaterOrEqual(s.T(), len(items), 1,
		"regex search for orders should return at least 1 match")

	for _, item := range items {
		entry := item.(map[string]interface{})
		queryText, _ := entry["queryText"].(string)
		assert.Contains(s.T(), queryText, "orders",
			"regex match should contain 'orders'")
	}

	// Step 2: User searches with wildcard pattern.
	s.T().Log("Step 2: Search with wildcard pattern")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&pattern=SELECT+*&patternType=wildcard", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, _ = resp["items"].([]interface{})
	assert.GreaterOrEqual(s.T(), len(items), 1,
		"wildcard search for SELECT should return at least 1 match")

	for _, item := range items {
		entry := item.(map[string]interface{})
		queryText, _ := entry["queryText"].(string)
		assert.True(s.T(), strings.HasPrefix(queryText, "SELECT"),
			"wildcard match should start with SELECT")
	}

	// Step 3: User filters by username.
	s.T().Log("Step 3: Filter by username")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&user=analyst", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, _ = resp["items"].([]interface{})
	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "analyst", entry["username"],
			"user filter should only return analyst's queries")
	}

	// Step 4: User combines multiple filters.
	s.T().Log("Step 4: Combined filters (user + database)")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&user=analyst&database=warehouse", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	items, _ = resp["items"].([]interface{})
	for _, item := range items {
		entry := item.(map[string]interface{})
		assert.Equal(s.T(), "analyst", entry["username"])
		assert.Equal(s.T(), "warehouse", entry["databaseName"])
	}

	// Step 5: User filters by time range.
	s.T().Log("Step 5: Filter by time range (since=90m)")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&since=90m", nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	total, _ := resp["total"].(float64)
	assert.GreaterOrEqual(s.T(), int(total), 1,
		"since=90m should return at least 1 recent entry")
}

// ============================================================================
// E2E Journey: Export CSV (61c)
// ============================================================================

// TestE2E_Scenario61c_ExportCSVJourney tests the complete user journey for
// exporting query history to CSV: export all → verify format → export with filters.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61c_ExportCSVJourney() {
	// Step 1: User exports all query history.
	s.T().Log("Step 1: Export all query history to CSV")
	rec := s.doRequest(http.MethodPost, e2eHistoryExportPath(), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code, "export should succeed")

	// Step 2: Verify Content-Type and Content-Disposition headers.
	s.T().Log("Step 2: Verify response headers")
	assert.Equal(s.T(), "text/csv", rec.Header().Get("Content-Type"),
		"Content-Type should be text/csv")
	assert.Contains(s.T(), rec.Header().Get("Content-Disposition"), "query-history.csv",
		"Content-Disposition should contain filename")

	// Step 3: Verify CSV format.
	s.T().Log("Step 3: Verify CSV format")
	reader := csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err := reader.ReadAll()
	require.NoError(s.T(), err, "response should be valid CSV")

	// Header + 4 data rows.
	require.Equal(s.T(), 5, len(records),
		"CSV should have header + 4 data rows")

	// Verify header.
	expectedHeader := []string{
		"query_id", "username", "database", "query_text",
		"start_time", "end_time", "duration_ms", "rows_affected",
		"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
	}
	assert.Equal(s.T(), expectedHeader, records[0],
		"CSV header should match specification")

	// Step 4: Verify data rows have correct number of columns.
	s.T().Log("Step 4: Verify data row structure")
	for i, record := range records[1:] {
		assert.Len(s.T(), record, 12,
			"data row %d should have 12 columns", i)
		// Verify time format is RFC3339.
		_, parseErr := time.Parse(time.RFC3339, record[4])
		assert.NoError(s.T(), parseErr,
			"start_time in row %d should be RFC3339 format", i)
		_, parseErr = time.Parse(time.RFC3339, record[5])
		assert.NoError(s.T(), parseErr,
			"end_time in row %d should be RFC3339 format", i)
	}

	// Step 5: User exports with filter.
	s.T().Log("Step 5: Export with user filter")
	body, _ := json.Marshal(map[string]string{"user": "etl_service"})
	rec = s.doRequest(http.MethodPost, e2eHistoryExportPath(), body)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	reader = csv.NewReader(strings.NewReader(rec.Body.String()))
	records, err = reader.ReadAll()
	require.NoError(s.T(), err)

	// Header + 1 etl_service entry.
	assert.Equal(s.T(), 2, len(records),
		"filtered CSV should have header + 1 data row for etl_service")
	if len(records) > 1 {
		assert.Equal(s.T(), "etl_service", records[1][1],
			"filtered row should belong to etl_service")
	}
}

// ============================================================================
// E2E Journey: Historical Details (61d)
// ============================================================================

// TestE2E_Scenario61d_HistoricalDetailJourney tests the complete user journey for
// viewing historical query details: view with plan → view without plan → not found.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61d_HistoricalDetailJourney() {
	// Step 1: User views a query with EXPLAIN plan.
	s.T().Log("Step 1: View query detail with EXPLAIN plan")
	rec := s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-5001-1000"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code, "detail should succeed")

	resp := s.decodeJSON(rec)
	assert.Equal(s.T(), "q-5001-1000", resp["queryId"])
	assert.Equal(s.T(), "completed", resp["state"])
	assert.Equal(s.T(), "analyst", resp["username"])
	assert.Equal(s.T(), "warehouse", resp["databaseName"])

	// Verify execution metrics.
	assert.Equal(s.T(), float64(45000), resp["durationMs"])
	assert.Equal(s.T(), float64(18000), resp["cpuTimeMs"])
	assert.Equal(s.T(), float64(134217728), resp["memoryBytes"])
	assert.Equal(s.T(), float64(0), resp["spillBytes"])
	assert.Equal(s.T(), float64(4194304), resp["diskReadBytes"])
	assert.Equal(s.T(), float64(0), resp["diskWriteBytes"])
	assert.Equal(s.T(), float64(2500), resp["rowsAffected"])

	// Verify EXPLAIN plan.
	explainPlan, ok := resp["explainPlan"].(string)
	require.True(s.T(), ok, "should have explainPlan field")
	assert.NotEmpty(s.T(), explainPlan, "explainPlan should not be empty")
	assert.Contains(s.T(), explainPlan, "Hash Join",
		"plan should contain Hash Join")
	assert.Contains(s.T(), explainPlan, "Seq Scan on orders",
		"plan should contain Seq Scan on orders")

	// Step 2: User views a query without EXPLAIN plan.
	s.T().Log("Step 2: View query detail without EXPLAIN plan")
	rec = s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-5002-2000"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "q-5002-2000", resp["queryId"])
	assert.Equal(s.T(), "etl_service", resp["username"])
	assert.Equal(s.T(), float64(180000), resp["durationMs"])
	assert.Equal(s.T(), float64(100000), resp["rowsAffected"])

	// explainPlan should be empty or omitted.
	plan, _ := resp["explainPlan"].(string)
	assert.Empty(s.T(), plan, "query without plan collection should have empty explainPlan")

	// Step 3: User views a query with error.
	s.T().Log("Step 3: View query detail with error")
	rec = s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-5004-4000"), nil)
	require.Equal(s.T(), http.StatusOK, rec.Code)

	resp = s.decodeJSON(rec)
	assert.Equal(s.T(), "error", resp["state"])
	errorMsg, _ := resp["errorMessage"].(string)
	assert.Contains(s.T(), errorMsg, "deadlock",
		"error query should have error message about deadlock")

	// Step 4: User tries to view a non-existent query.
	s.T().Log("Step 4: View non-existent query")
	rec = s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-nonexistent"), nil)
	require.Equal(s.T(), http.StatusNotFound, rec.Code,
		"non-existent query should return 404")

	resp = s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain 'error' object")
	assert.Equal(s.T(), "QUERY_NOT_FOUND", errObj["code"])
}

// ============================================================================
// E2E Journey: Authentication (cross-cutting)
// ============================================================================

// TestE2E_Scenario61_AuthenticationJourney verifies that unauthenticated users
// are rejected and authenticated users can access all query history endpoints.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61_AuthenticationJourney() {
	// Step 1: Unauthenticated user tries to browse history.
	s.T().Log("Step 1: Unauthenticated browse history")
	req := httptest.NewRequest(http.MethodGet, e2eHistoryBasePath(), nil)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated browse should return 401")

	// Step 2: Unauthenticated user tries to view detail.
	s.T().Log("Step 2: Unauthenticated view detail")
	req = httptest.NewRequest(http.MethodGet, e2eHistoryDetailPath("q-5001-1000"), nil)
	rec = httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated detail should return 401")

	// Step 3: Unauthenticated user tries to export.
	s.T().Log("Step 3: Unauthenticated export")
	req = httptest.NewRequest(http.MethodPost, e2eHistoryExportPath(), nil)
	rec = httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	assert.Equal(s.T(), http.StatusUnauthorized, rec.Code,
		"unauthenticated export should return 401")

	// Step 4: Authenticated user can access all endpoints.
	s.T().Log("Step 4: Authenticated user accesses all endpoints")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"authenticated browse should succeed")

	rec = s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-5001-1000"), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"authenticated detail should succeed")

	rec = s.doRequest(http.MethodPost, e2eHistoryExportPath(), nil)
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"authenticated export should succeed")
}

// ============================================================================
// E2E Journey: Error Handling
// ============================================================================

// TestE2E_Scenario61_ErrorHandlingJourney verifies that the API handles various
// error conditions gracefully: invalid regex, non-existent query, invalid params.
func (s *Scenario61QueryHistoryE2ESuite) TestE2E_Scenario61_ErrorHandlingJourney() {
	// Step 1: Invalid regex pattern.
	s.T().Log("Step 1: Invalid regex pattern")
	rec := s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&pattern=[invalid&patternType=regex", nil)
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"invalid regex should return 400")

	resp := s.decodeJSON(rec)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "INVALID_REQUEST", errObj["code"])

	// Step 2: Non-existent query ID.
	s.T().Log("Step 2: Non-existent query ID")
	rec = s.doRequest(http.MethodGet, e2eHistoryDetailPath("q-does-not-exist"), nil)
	require.Equal(s.T(), http.StatusNotFound, rec.Code,
		"non-existent query should return 404")

	resp = s.decodeJSON(rec)
	errObj, ok = resp["error"].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "QUERY_NOT_FOUND", errObj["code"])

	// Step 3: Invalid limit parameter.
	s.T().Log("Step 3: Invalid limit parameter")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&limit=abc", nil)
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"invalid limit should return 400")

	// Step 4: Invalid offset parameter.
	s.T().Log("Step 4: Invalid offset parameter")
	rec = s.doRequest(http.MethodGet, e2eHistoryBasePath()+"&offset=-1", nil)
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"negative offset should return 400")

	// Step 5: Non-existent cluster.
	s.T().Log("Step 5: Non-existent cluster")
	nonExistentPath := scenario61E2EPrefix + "/clusters/nonexistent-cluster/queries/history?namespace=default"
	rec = s.doRequest(http.MethodGet, nonExistentPath, nil)
	require.Equal(s.T(), http.StatusNotFound, rec.Code,
		"non-existent cluster should return 404")
}

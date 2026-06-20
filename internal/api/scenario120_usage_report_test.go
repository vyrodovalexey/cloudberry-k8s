package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
)

// ============================================================================
// Scenario 120 — Usage Reporting (C.13) API handler tests. httptest + a mock DB
// client whose usageReport entries carry the per-table breakdown (db.TableUsage)
// proves the enriched C.11 content flows through P.6's JSON envelope; the
// disabled soft-gate yields an honest unavailable payload; the ?month= query
// param threads through; and a missing cluster is a 404.
// ============================================================================

// scenario120Cluster builds a running cluster with the usage report enabled so
// the P.6 gated endpoint takes its enabled branch.
func scenario120Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	return cluster
}

// scenario120UsageReport returns canned usage entries whose connected database
// carries a non-empty per-table breakdown (orders/lineitem), exercising the
// C.11 enriched content path.
func scenario120UsageReport() []db.UsageReportEntry {
	return []db.UsageReportEntry{
		{
			Month: "2026-05", Database: "testdb",
			SizeBytes: 3221225472, SizeHuman: "3 GB", Connections: 10,
			Tables: []db.TableUsage{
				{Schema: "public", Table: "orders", SizeBytes: 2147483648, SizeHuman: "2 GB"},
				{Schema: "public", Table: "lineitem", SizeBytes: 1073741824, SizeHuman: "1 GB"},
			},
		},
		{
			Month: "2026-05", Database: "postgres",
			SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 2,
		},
	}
}

// 120-C13-api-enabled — usageReport.enabled → 200 with usageReportEnabled true
// and entries whose connected-db entry carries the per-table breakdown.
func TestScenario120_API_Enabled(t *testing.T) {
	// Arrange
	cluster := scenario120Cluster()
	dbClient := &mockDBClient{usageReport: scenario120UsageReport()}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2026-05", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetUsageReport(rr, req)

	// Assert: enabled envelope with enriched per-table content.
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)
	assert.Equal(t, true, resp["usageReportEnabled"])
	assert.Equal(t, "2026-05", resp["month"])
	assert.Equal(t, float64(2), resp["total"])

	entries, ok := resp["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)

	first, ok := entries[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "testdb", first["database"])

	tables, ok := first["tables"].([]interface{})
	require.True(t, ok, "the connected-db entry must carry a per-table breakdown")
	require.Len(t, tables, 2)
	table0, ok := tables[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "public", table0["schema"])
	assert.Equal(t, "orders", table0["table"])
	assert.Equal(t, float64(2147483648), table0["sizeBytes"])
	assert.Equal(t, "2 GB", table0["sizeHuman"])

	// The non-connected database has no per-table breakdown (omitempty → absent).
	second, ok := entries[1].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "postgres", second["database"])
	_, hasTables := second["tables"]
	assert.False(t, hasTables, "non-connected entry must omit the empty tables array")
}

// 120-DISABLED-unavailable — usageReport disabled/nil → 200 with
// usageReportEnabled false and empty entries (the unavailable contract, a SOFT
// 200 gate, NOT a 400); the DB is never queried.
func TestScenario120_API_DisabledUnavailable(t *testing.T) {
	tests := []struct {
		name    string
		storage *cbv1alpha1.StorageManagementSpec
	}{
		{
			name:    "usageReport nil",
			storage: &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true},
		},
		{
			name: "usageReport disabled",
			storage: &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring: true,
				UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: false},
			},
		},
		{
			name:    "storage nil",
			storage: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: usable data exists, but the disabled gate must short-circuit.
			cluster := newTestCluster("test-cluster", "default")
			cluster.Spec.Storage = tt.storage
			dbClient := &mockDBClient{usageReport: scenario120UsageReport()}
			s := newTestServerWithDB(dbClient, cluster)

			req := httptest.NewRequest(http.MethodGet,
				apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2026-05", nil)
			req.SetPathValue("name", "test-cluster")
			rr := httptest.NewRecorder()

			// Act
			s.handleGetUsageReport(rr, req)

			// Assert: 200 honest empty + flag false (NOT a 400).
			require.Equal(t, http.StatusOK, rr.Code)
			resp := decodeJSON(t, rr)
			assert.Equal(t, false, resp["usageReportEnabled"])
			assert.Equal(t, float64(0), resp["total"])
			entries, ok := resp["entries"].([]interface{})
			require.True(t, ok)
			assert.Empty(t, entries)
			assert.Zero(t, dbClient.closeCalls,
				"disabled gate must not open a DB client")
		})
	}
}

// 120-MONTH-param — ?month=2026-05 threads through the handler: the response
// month echoes the query param (the handler reads r.URL.Query().Get("month")
// and passes it to GetUsageReport). A request without ?month= echoes empty.
func TestScenario120_API_MonthParam(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantMonth string
	}{
		{"explicit month", "?namespace=default&month=2026-05", "2026-05"},
		{"no month", "?namespace=default", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			cluster := scenario120Cluster()
			dbClient := &mockDBClient{usageReport: scenario120UsageReport()}
			s := newTestServerWithDB(dbClient, cluster)

			req := httptest.NewRequest(http.MethodGet,
				apiPrefix+"/clusters/test-cluster/storage/usage-report"+tt.query, nil)
			req.SetPathValue("name", "test-cluster")
			rr := httptest.NewRecorder()

			// Act
			s.handleGetUsageReport(rr, req)

			// Assert: the envelope month reflects the ?month= query param.
			require.Equal(t, http.StatusOK, rr.Code)
			resp := decodeJSON(t, rr)
			assert.Equal(t, tt.wantMonth, resp["month"])
			assert.Equal(t, true, resp["usageReportEnabled"])
		})
	}
}

// 120-NOTFOUND — a missing cluster yields a 404, checked BEFORE any DB call.
func TestScenario120_API_NotFound(t *testing.T) {
	// Arrange: a DB-backed server with NO clusters seeded.
	dbClient := &mockDBClient{usageReport: scenario120UsageReport()}
	s := newTestServerWithDB(dbClient)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/missing/storage/usage-report?namespace=default&month=2026-05", nil)
	req.SetPathValue("name", "missing")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetUsageReport(rr, req)

	// Assert
	require.Equal(t, http.StatusNotFound, rr.Code)
	assert.Zero(t, dbClient.closeCalls, "404 must be returned before any DB call")
}

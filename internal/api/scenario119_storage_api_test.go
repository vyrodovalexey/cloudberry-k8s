package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// ============================================================================
// Scenario 119 — Storage API handler tests (P.1–P.6). httptest + mockDBClient
// injected with known data exercises the real-shape (non-stub) responses, the
// best-effort / non-fatal contract (DB error → 200 honest-empty, never 500),
// the cluster-not-found path, and the route-permission registration.
// ============================================================================

// decodeJSON decodes the recorder body into a generic map for assertions.
func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// scenario119Cluster builds a running cluster with storage management enabled
// (recommendation scan + usage report) so the gated endpoints take their
// enabled branches.
func scenario119Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.DiskUsagePercent = 73
	cluster.Status.RecommendationCount = 9
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:  true,
			Schedule: "0 3 * * 0",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	return cluster
}

// ---------------------------------------------------------------------------
// 119a-P1 — GET /storage/disk-usage returns the status-sourced percent plus the
// per-DB usage and the per-segment breakdown.
// ---------------------------------------------------------------------------

func TestScenario119_P1_DiskUsage(t *testing.T) {
	// Arrange
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		diskUsage: []db.DiskUsage{
			{Database: "postgres", SizeBytes: 4096, SizeHuman: "4 KB"},
		},
		storageDiskUsage: []db.DiskUsageInfo{
			{Tablespace: "pg_default", SizeBytes: 8192, SizeHuman: "8 KB", UsagePercent: 73},
		},
	}
	rec := &countingRecorder{}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/disk-usage?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetDiskUsage(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)

	// P.1 == status invariant: diskUsagePercent sourced ONLY from status.
	assert.Equal(t, float64(73), resp["diskUsagePercent"])

	usage, ok := resp["diskUsage"].([]interface{})
	require.True(t, ok, "diskUsage must be present")
	require.Len(t, usage, 1)
	first, ok := usage[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "postgres", first["database"])

	breakdown, ok := resp["diskUsageBySegment"].([]interface{})
	require.True(t, ok, "diskUsageBySegment must be present")
	require.Len(t, breakdown, 1)
	seg, ok := breakdown[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "pg_default", seg["tablespace"])
}

// ---------------------------------------------------------------------------
// 119b-P2 — GET /storage/tables reflects the mock GetTables rows (size, bloat,
// skew, rowCount) with total == len.
// ---------------------------------------------------------------------------

func TestScenario119_P2_ListTables(t *testing.T) {
	// Arrange
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		tables: []db.TableStorageInfo{
			{
				Schema: "public", Table: "events",
				SizeBytes: 2147483648, SizeHuman: "2 GB",
				BloatPercent: 55, SkewPercent: 42, RowCount: 5000000,
			},
			{
				Schema: "public", Table: "users",
				SizeBytes: 1073741824, SizeHuman: "1 GB",
				BloatPercent: 10, SkewPercent: 7, RowCount: 1000000,
			},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/tables?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleListTables(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)
	assert.Equal(t, float64(2), resp["total"])

	tables, ok := resp["tables"].([]interface{})
	require.True(t, ok)
	require.Len(t, tables, 2)

	events, ok := tables[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "public", events["schema"])
	assert.Equal(t, "events", events["table"])
	assert.Equal(t, float64(2147483648), events["sizeBytes"])
	assert.Equal(t, "2 GB", events["sizeHuman"])
	assert.Equal(t, float64(55), events["bloatPercent"])
	assert.Equal(t, float64(42), events["skewPercent"])
	assert.Equal(t, float64(5000000), events["rowCount"])

	// The Close() defer must fire exactly once per request.
	assert.Equal(t, 1, dbClient.closeCalls)
}

// ---------------------------------------------------------------------------
// 119c-P3 — GET /storage/tables/{schema}/{table} reflects the mock
// GetTableDetails (size, bloat, skew, indexSizes).
// ---------------------------------------------------------------------------

func TestScenario119_P3_TableDetail(t *testing.T) {
	// Arrange
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		tableDetail: &db.TableDetail{
			Schema: "public", Table: "users",
			SizeBytes: 2147483648, SizeHuman: "2 GB",
			RowCount: 50000000, BloatPercent: 18, SkewPercent: 37,
			LastVacuum: "2025-01-01", LastAnalyze: "2025-01-02",
			IndexSizes: []db.IndexSizeInfo{
				{Name: "users_pkey", SizeBytes: 1048576, SizeHuman: "1 MB"},
				{Name: "users_email_idx", SizeBytes: 524288, SizeHuman: "512 kB"},
			},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/tables/public/users?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("schema", "public")
	req.SetPathValue("table", "users")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetTableDetail(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)
	assert.Equal(t, "public", resp["schema"])
	assert.Equal(t, "users", resp["table"])
	assert.Equal(t, float64(2147483648), resp["sizeBytes"])
	assert.Equal(t, float64(50000000), resp["rowCount"])
	assert.Equal(t, float64(18), resp["bloatPercent"])
	assert.Equal(t, float64(37), resp["skewPercent"])

	indexes, ok := resp["indexSizes"].([]interface{})
	require.True(t, ok, "indexSizes must be present")
	require.Len(t, indexes, 2)
	pkey, ok := indexes[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "users_pkey", pkey["name"])
	assert.Equal(t, float64(1048576), pkey["sizeBytes"])
}

// ---------------------------------------------------------------------------
// 119d-P4 — GET /storage/recommendations reflects the four threshold-aware
// Get* mocks (type + target) with recommendationCount == live len.
// ---------------------------------------------------------------------------

func TestScenario119_P4_Recommendations(t *testing.T) {
	// Arrange: one recommendation per type across the four fetchers.
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		bloatRecs: []db.Recommendation{
			{Type: "bloat", Schema: "public", Table: "events",
				Value: 55, Ratio: 55, Severity: "critical", Description: "bloated"},
		},
		skewRecs: []db.Recommendation{
			{Type: "skew", Schema: "public", Table: "users",
				Value: 42, Ratio: 42, Severity: "warning", Description: "skewed"},
		},
		ageRecs: []db.Recommendation{
			{Type: "age", Schema: "public", Table: "old_table",
				Value: 150000000, Severity: "warning", Description: "old"},
		},
		indexBloatRecs: []db.Recommendation{
			{Type: "index_bloat", Schema: "public", Table: "users",
				Value: 65, Ratio: 65, Severity: "critical", Description: "index bloated"},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/recommendations?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleListRecommendations(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)

	// recommendationCount is the LIVE total (4), NOT the cached status (9).
	assert.Equal(t, float64(4), resp["recommendationCount"])
	assert.Equal(t, float64(4), resp["total"])

	recs, ok := resp["recommendations"].([]interface{})
	require.True(t, ok)
	require.Len(t, recs, 4)

	// Verify type + target ("schema.table") across the combined list.
	gotTypes := map[string]string{} // type -> target
	for _, r := range recs {
		entry, ok := r.(map[string]interface{})
		require.True(t, ok)
		gotTypes[entry["type"].(string)] = entry["target"].(string)
	}
	assert.Equal(t, "public.events", gotTypes["bloat"])
	assert.Equal(t, "public.users", gotTypes["skew"])
	assert.Equal(t, "public.old_table", gotTypes["age"])
	assert.Equal(t, "public.users", gotTypes["index_bloat"])
}

// 119d-P4-dberr — DB unavailable: recommendations empty, recommendationCount
// falls back to the cached Status.RecommendationCount, HTTP 200 (NOT 500).
func TestScenario119_P4_Recommendations_DBUnavailableFallsBackToStatus(t *testing.T) {
	// Arrange: no DB factory at all → not reachable.
	cluster := scenario119Cluster()
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/recommendations?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleListRecommendations(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)
	assert.Equal(t, float64(9), resp["recommendationCount"],
		"count must fall back to Status.RecommendationCount when DB unreachable")
	assert.Equal(t, float64(0), resp["total"])
	recs, ok := resp["recommendations"].([]interface{})
	require.True(t, ok)
	assert.Empty(t, recs)
}

// ---------------------------------------------------------------------------
// 119e-P5 — POST /storage/recommendations/scan: enabled → 202 + scan-duration
// count advances; disabled → 400 RECOMMENDATION_SCAN_NOT_ENABLED.
// ---------------------------------------------------------------------------

func TestScenario119_P5_ScanEnabled_DurationAdvances(t *testing.T) {
	// Arrange
	cluster := scenario119Cluster()
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		bloatRecs: []db.Recommendation{{Type: "bloat"}},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	newScanReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		return req
	}

	// Act: first POST.
	rr1 := httptest.NewRecorder()
	s.handleTriggerRecommendationScan(rr1, newScanReq())
	// Act: second POST.
	rr2 := httptest.NewRecorder()
	s.handleTriggerRecommendationScan(rr2, newScanReq())

	// Assert: 202 each time; the duration histogram _count advances per POST.
	assert.Equal(t, http.StatusAccepted, rr1.Code)
	assert.Equal(t, http.StatusAccepted, rr2.Code)
	assert.Equal(t, 2, rec.scanDurationCalls,
		"ObserveRecommendationScanDuration must advance once per POST")
}

func TestScenario119_P5_ScanDisabled_Returns400(t *testing.T) {
	// Arrange: storage spec present but recommendationScan absent.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	rec := &countingRecorder{}
	factory := &mockDBFactory{client: &mockDBClient{}}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleTriggerRecommendationScan(rr, req)

	// Assert
	require.Equal(t, http.StatusBadRequest, rr.Code)
	resp := decodeJSON(t, rr)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "RECOMMENDATION_SCAN_NOT_ENABLED", errObj["code"])
	assert.Zero(t, rec.scanDurationCalls, "no scan must run when disabled")
}

// ---------------------------------------------------------------------------
// 119f-P6 — GET /storage/usage-report: enabled → entries + usageReportEnabled
// true; disabled → empty + usageReportEnabled false.
// ---------------------------------------------------------------------------

func TestScenario119_P6_UsageReportEnabled(t *testing.T) {
	// Arrange
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		usageReport: []db.UsageReportEntry{
			{Month: "2025-01", Database: "testdb", SizeBytes: 1073741824, SizeHuman: "1 GB", Connections: 10},
			{Month: "2025-01", Database: "postgres", SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 2},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2025-01", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetUsageReport(rr, req)

	// Assert
	require.Equal(t, http.StatusOK, rr.Code)
	resp := decodeJSON(t, rr)
	assert.Equal(t, "2025-01", resp["month"])
	assert.Equal(t, true, resp["usageReportEnabled"])
	assert.Equal(t, float64(2), resp["total"])

	entries, ok := resp["entries"].([]interface{})
	require.True(t, ok)
	require.Len(t, entries, 2)
	first, ok := entries[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "testdb", first["database"])
}

func TestScenario119_P6_UsageReportDisabled(t *testing.T) {
	// Arrange: usageReport absent → disabled soft-gate.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	// Even with usable usage data, the disabled gate must short-circuit the query.
	dbClient := &mockDBClient{
		usageReport: []db.UsageReportEntry{{Month: "2025-01", Database: "testdb"}},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2025-01", nil)
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
	// The DB must NOT be queried when disabled.
	assert.Zero(t, dbClient.closeCalls, "disabled gate must not open a DB client")
}

// ---------------------------------------------------------------------------
// 119-NOTFOUND — each storage endpoint returns 404 for a missing cluster,
// checked BEFORE any DB call.
// ---------------------------------------------------------------------------

func TestScenario119_NotFound_AllEndpoints(t *testing.T) {
	// Arrange: a DB-backed server with NO clusters seeded.
	dbClient := &mockDBClient{
		tables:      []db.TableStorageInfo{{Schema: "public", Table: "x"}},
		tableDetail: &db.TableDetail{Schema: "public", Table: "x"},
		usageReport: []db.UsageReportEntry{{Database: "x"}},
	}
	s := newTestServerWithDB(dbClient)

	cases := []struct {
		name    string
		method  string
		path    string
		handler func(http.ResponseWriter, *http.Request)
		setup   func(*http.Request)
	}{
		{
			name:    "P1 disk-usage",
			method:  http.MethodGet,
			path:    "/storage/disk-usage",
			handler: s.handleGetDiskUsage,
		},
		{
			name:    "P2 tables",
			method:  http.MethodGet,
			path:    "/storage/tables",
			handler: s.handleListTables,
		},
		{
			name:    "P3 table-detail",
			method:  http.MethodGet,
			path:    "/storage/tables/public/users",
			handler: s.handleGetTableDetail,
			setup: func(r *http.Request) {
				r.SetPathValue("schema", "public")
				r.SetPathValue("table", "users")
			},
		},
		{
			name:    "P4 recommendations",
			method:  http.MethodGet,
			path:    "/storage/recommendations",
			handler: s.handleListRecommendations,
		},
		{
			name:    "P5 scan",
			method:  http.MethodPost,
			path:    "/storage/recommendations/scan",
			handler: s.handleTriggerRecommendationScan,
		},
		{
			name:    "P6 usage-report",
			method:  http.MethodGet,
			path:    "/storage/usage-report",
			handler: s.handleGetUsageReport,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method,
				apiPrefix+"/clusters/nonexistent"+tc.path+"?namespace=default", nil)
			req.SetPathValue("name", "nonexistent")
			if tc.setup != nil {
				tc.setup(req)
			}
			rr := httptest.NewRecorder()

			tc.handler(rr, req)

			assert.Equal(t, http.StatusNotFound, rr.Code)
		})
	}

	// The 404 must be returned BEFORE any DB call (no client opened).
	assert.Zero(t, dbClient.closeCalls,
		"cluster-not-found must short-circuit before any DB access")
}

// ---------------------------------------------------------------------------
// 119-DBERR-nonfatal — DB query errors on P.2/P.3/P.6 yield honest-empty 200,
// never a 500.
// ---------------------------------------------------------------------------

func TestScenario119_DBError_NonFatal(t *testing.T) {
	cluster := scenario119Cluster()

	t.Run("P2 tables query error -> empty 200", func(t *testing.T) {
		dbClient := &mockDBClient{tablesErr: fmt.Errorf("tables query failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/tables?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rr := httptest.NewRecorder()

		s.handleListTables(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeJSON(t, rr)
		assert.Equal(t, float64(0), resp["total"])
		tables, ok := resp["tables"].([]interface{})
		require.True(t, ok)
		assert.Empty(t, tables)
	})

	t.Run("P3 detail query error -> minimal 200", func(t *testing.T) {
		dbClient := &mockDBClient{tableDetailErr: fmt.Errorf("detail query failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/tables/public/users?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("schema", "public")
		req.SetPathValue("table", "users")
		rr := httptest.NewRecorder()

		s.handleGetTableDetail(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeJSON(t, rr)
		// Honest minimal fallback shape: only schema + table.
		assert.Equal(t, "public", resp["schema"])
		assert.Equal(t, "users", resp["table"])
		assert.Nil(t, resp["sizeBytes"])
	})

	t.Run("P6 usage-report query error -> empty 200 with flag true", func(t *testing.T) {
		dbClient := &mockDBClient{usageReportErr: fmt.Errorf("usage report query failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2025-01", nil)
		req.SetPathValue("name", "test-cluster")
		rr := httptest.NewRecorder()

		s.handleGetUsageReport(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeJSON(t, rr)
		assert.Equal(t, true, resp["usageReportEnabled"])
		assert.Equal(t, float64(0), resp["total"])
		entries, ok := resp["entries"].([]interface{})
		require.True(t, ok)
		assert.Empty(t, entries)
	})

	t.Run("P1 breakdown query error -> empty breakdown 200", func(t *testing.T) {
		dbClient := &mockDBClient{
			diskUsageErr:        fmt.Errorf("disk usage query failed"),
			storageDiskUsageErr: fmt.Errorf("breakdown query failed"),
		}
		rec := &countingRecorder{}
		factory := &mockDBFactory{client: dbClient}
		s := newTestServerWithDBAndMetrics(factory, rec, cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/disk-usage?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rr := httptest.NewRecorder()

		s.handleGetDiskUsage(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeJSON(t, rr)
		// percent still from status; both breakdowns honestly empty.
		assert.Equal(t, float64(73), resp["diskUsagePercent"])
		usage, ok := resp["diskUsage"].([]interface{})
		require.True(t, ok)
		assert.Empty(t, usage)
		seg, ok := resp["diskUsageBySegment"].([]interface{})
		require.True(t, ok)
		assert.Empty(t, seg)
	})

	t.Run("P2 NewClient error -> empty 200", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/tables?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rr := httptest.NewRecorder()

		s.handleListTables(rr, req)

		require.Equal(t, http.StatusOK, rr.Code)
		resp := decodeJSON(t, rr)
		assert.Equal(t, float64(0), resp["total"])
	})
}

// ---------------------------------------------------------------------------
// 119-AUTH — the storage routes are registered with the right permission:
// reads require PermissionBasic, POST scan requires PermissionOperator. Drive
// the full mux through the auth middleware (mirroring the existing
// permission-model tests).
// ---------------------------------------------------------------------------

func TestScenario119_Auth_RoutePermissions(t *testing.T) {
	cluster := scenario119Cluster()

	// guestServer builds a server whose only identity is SelfOnly (below Basic),
	// so PermissionBasic reads and PermissionOperator writes are both forbidden.
	guestServer := func() *Server {
		scheme := newTestScheme()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithRuntimeObjects(cluster).Build()
		factory := &mockDBFactory{client: &mockDBClient{}}
		basicProvider := &mockAuthProvider{
			identity: &auth.Identity{Username: "guest", Permission: auth.PermissionSelfOnly},
		}
		mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
		return trackServer(NewServer(k8sClient, mw, factory, &metrics.NoopRecorder{}, nil, 0))
	}

	// operatorServer builds a server whose identity is an Operator (>= Basic but
	// used to confirm reads succeed and writes succeed).
	operatorServer := func() *Server {
		scheme := newTestScheme()
		k8sClient := fake.NewClientBuilder().WithScheme(scheme).
			WithRuntimeObjects(cluster).Build()
		factory := &mockDBFactory{client: &mockDBClient{}}
		basicProvider := &mockAuthProvider{
			identity: &auth.Identity{Username: "operator", Permission: auth.PermissionOperator},
		}
		mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
		return trackServer(NewServer(k8sClient, mw, factory, &metrics.NoopRecorder{}, nil, 0))
	}

	t.Run("read endpoints require at least Basic (Guest forbidden)", func(t *testing.T) {
		s := guestServer()
		handler := s.Handler()
		reads := []struct {
			method string
			path   string
		}{
			{http.MethodGet, "/storage/disk-usage"},
			{http.MethodGet, "/storage/tables"},
			{http.MethodGet, "/storage/tables/public/users"},
			{http.MethodGet, "/storage/recommendations"},
			{http.MethodGet, "/storage/usage-report"},
		}
		for _, r := range reads {
			req := httptest.NewRequest(r.method,
				apiPrefix+"/clusters/test-cluster"+r.path+"?namespace=default", nil)
			req.SetBasicAuth("guest", "pass")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusForbidden, rec.Code,
				"%s %s must require >= Basic", r.method, r.path)
		}
	})

	t.Run("read endpoints succeed with Operator", func(t *testing.T) {
		s := operatorServer()
		handler := s.Handler()
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/storage/tables?namespace=default", nil)
		req.SetBasicAuth("operator", "pass")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("POST scan requires Operator (Guest forbidden)", func(t *testing.T) {
		s := guestServer()
		handler := s.Handler()
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetBasicAuth("guest", "pass")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("POST scan succeeds with Operator", func(t *testing.T) {
		s := operatorServer()
		handler := s.Handler()
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetBasicAuth("operator", "pass")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})
}

// ---------------------------------------------------------------------------
// collector unit coverage — exercise the best-effort collectors directly for
// the nil-dbFactory branches not reachable through the DB-backed handlers.
// ---------------------------------------------------------------------------

func TestScenario119_Collectors_NilDBFactory(t *testing.T) {
	cluster := scenario119Cluster()
	s := newTestServer(cluster) // no DB factory

	ctx := context.Background()

	assert.Empty(t, s.collectTables(ctx, cluster))
	assert.Nil(t, s.collectTableDetail(ctx, cluster, "public", "users"))
	assert.Empty(t, s.collectStorageBreakdown(ctx, cluster))
	assert.Empty(t, s.collectUsageReport(ctx, cluster, "2025-01"))
	recs, reachable := s.collectRecommendations(ctx, cluster)
	assert.Empty(t, recs)
	assert.False(t, reachable)
}

func TestScenario119_Collectors_NewClientError(t *testing.T) {
	cluster := scenario119Cluster()
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	ctx := context.Background()

	assert.Empty(t, s.collectTables(ctx, cluster))
	assert.Nil(t, s.collectTableDetail(ctx, cluster, "public", "users"))
	assert.Empty(t, s.collectStorageBreakdown(ctx, cluster))
	assert.Empty(t, s.collectUsageReport(ctx, cluster, "2025-01"))
	recs, reachable := s.collectRecommendations(ctx, cluster)
	assert.Empty(t, recs)
	assert.False(t, reachable)
}

// collectRecommendations: per-fetch error is skipped but the DB stays reachable.
func TestScenario119_CollectRecommendations_FetchErrorSkips(t *testing.T) {
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{
		bloatRecsErr: fmt.Errorf("bloat failed"),
		skewRecs:     []db.Recommendation{{Type: "skew", Schema: "public", Table: "t"}},
	}
	s := newTestServerWithDB(dbClient, cluster)

	recs, reachable := s.collectRecommendations(context.Background(), cluster)

	assert.True(t, reachable, "DB stays reachable when only a fetch errors")
	require.Len(t, recs, 1)
	assert.Equal(t, "skew", recs[0].Type)
	assert.Equal(t, 1, dbClient.closeCalls)
}

// collectStorageBreakdown / collectTables / collectUsageReport return empty (not
// nil) when the mock returns a nil slice with no error.
func TestScenario119_Collectors_NilSliceNormalized(t *testing.T) {
	cluster := scenario119Cluster()
	dbClient := &mockDBClient{} // all nil slices, no errors
	s := newTestServerWithDB(dbClient, cluster)

	ctx := context.Background()
	assert.NotNil(t, s.collectTables(ctx, cluster))
	assert.Empty(t, s.collectTables(ctx, cluster))
	assert.NotNil(t, s.collectStorageBreakdown(ctx, cluster))
	assert.Empty(t, s.collectStorageBreakdown(ctx, cluster))
	assert.NotNil(t, s.collectUsageReport(ctx, cluster, "2025-01"))
	assert.Empty(t, s.collectUsageReport(ctx, cluster, "2025-01"))
}

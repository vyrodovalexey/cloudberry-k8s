//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 119: All Storage API Endpoints (P.1–P.6) — functional
// ============================================================================
//
// This suite black-boxes the FULL spec.storage REST surface through the REAL
// api.Server HTTP router + auth/RBAC middleware over a fake k8s cluster store +
// an injected dbFactory returning known data + a spy metrics recorder —
// infra-free, no live cluster. Unlike the internal/api package UNIT tests (which
// call the handlers directly), every request here travels the WHOLE server: mux
// routing -> auth middleware -> withPermission RBAC gate -> handler -> the
// best-effort DB collector.
//
//   - 119a-P1-F: GET disk-usage 200; diskUsagePercent == status.diskUsagePercent
//     (M.1==S.1) + non-empty diskUsage + the per-tablespace breakdown.
//   - 119b-P2-F: GET tables 200 with the real per-table rows.
//   - 119c-P3-F: GET tables/{schema}/{table} 200 with the real detail + indexSizes.
//   - 119d-P4-F: GET recommendations 200 with the real four-type list +
//     recommendationCount == live len.
//   - 119e-P5-F: POST scan 202 (enabled) + the duration metric _count advances
//     per POST; 400 RECOMMENDATION_SCAN_NOT_ENABLED (disabled).
//   - 119f-P6-F: GET usage-report 200 entries + flag true (enabled) / empty +
//     flag false (disabled soft-gate).
//   - 119-NOTFOUND: each endpoint 404 for a missing cluster.
//   - 119-AUTH: reads require Basic, scan requires Operator (403 below tier).
//   - 119-DBERR-nonfatal: a DB error yields an honest empty 200, never a 500.
//   - 119-CONTROL: a healthy DB populates all six endpoints with no error.
//
// It is catalog-honest via a coverage test that keeps the -F matrix from
// silently dropping a rule. The live proof is the KUBECONFIG/SCENARIO119_LIVE-
// gated Scenario 119 integration/e2e Part B.
// ============================================================================

const (
	scenario119Namespace = "cloudberry-test"
	scenario119Cluster   = "scenario119-storage"
	scenario119Prefix    = "/api/v1alpha1"

	scenario119BasicUser = "s119basic"
	scenario119BasicPass = "s119basicpass"
	scenario119OperUser  = "s119oper"
	scenario119OperPass  = "s119operpass"
)

// scenario119MetricsRecorder embeds NoopRecorder and counts the
// ObserveRecommendationScanDuration invocations so the suite can assert the P.5
// duration _count advances exactly once per POST.
type scenario119MetricsRecorder struct {
	metrics.NoopRecorder
	scanDurationCalls int32
}

func (m *scenario119MetricsRecorder) ObserveRecommendationScanDuration(
	_, _ string, _ time.Duration,
) {
	atomic.AddInt32(&m.scanDurationCalls, 1)
}

func (m *scenario119MetricsRecorder) durationCalls() int32 {
	return atomic.LoadInt32(&m.scanDurationCalls)
}

// scenario119MockDBClient extends testutil.MockDBClient with the three storage
// methods the shared mock does not expose a Func hook for (GetTableDetails,
// GetUsageReport, GetStorageDiskUsage). It carries the data + error each method
// should surface so the functional layer can drive REAL response shapes through
// the full router without touching the shared testutil mock.
type scenario119MockDBClient struct {
	testutil.MockDBClient

	storageDiskUsage    []db.DiskUsageInfo
	storageDiskUsageErr error

	tableDetail    *db.TableDetail
	tableDetailErr error

	usageReport    []db.UsageReportEntry
	usageReportErr error
}

func (m *scenario119MockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	if m.storageDiskUsageErr != nil {
		return nil, m.storageDiskUsageErr
	}
	return m.storageDiskUsage, nil
}

func (m *scenario119MockDBClient) GetTableDetails(
	_ context.Context, schema, table string,
) (*db.TableDetail, error) {
	if m.tableDetailErr != nil {
		return nil, m.tableDetailErr
	}
	if m.tableDetail != nil {
		return m.tableDetail, nil
	}
	return &db.TableDetail{Schema: schema, Table: table}, nil
}

func (m *scenario119MockDBClient) GetUsageReport(
	_ context.Context, _ string,
) ([]db.UsageReportEntry, error) {
	if m.usageReportErr != nil {
		return nil, m.usageReportErr
	}
	return m.usageReport, nil
}

// scenario119Factory is a db.DBClientFactory returning the supplied client (or
// an error), mirroring testutil.MockDBClientFactory but typed for the local
// scenario119MockDBClient.
type scenario119Factory struct {
	client db.Client
	err    error
}

func (f *scenario119Factory) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// Scenario119Suite drives the full storage REST surface through the real router
// over a fake cluster store + an injected dbFactory + a spy metrics recorder.
type Scenario119Suite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	client  client.Client
	metrics *scenario119MetricsRecorder
	ctx     context.Context
}

func TestFunctional_Scenario119(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario119Suite))
}

func (s *Scenario119Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario119Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// scenario119StorageCluster builds a running cluster with storage management
// enabled (disk monitoring + recommendation scan + usage report) + a known
// status so the gated endpoints take their enabled branches and the P.1
// percent==status invariant has a non-zero value to pin.
func scenario119StorageCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario119Cluster, scenario119Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
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

// scenario119DisabledCluster builds a running cluster whose recommendationScan
// and usageReport are absent (the disabled gates).
func scenario119DisabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario119Cluster, scenario119Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Status.DiskUsagePercent = 50
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	return cluster
}

// scenario119Populated returns a mock DB client carrying realistic data across
// every storage endpoint so the CONTROL + per-endpoint happy rows surface REAL
// shapes.
func scenario119Populated() *scenario119MockDBClient {
	m := &scenario119MockDBClient{
		storageDiskUsage: []db.DiskUsageInfo{
			{Tablespace: "pg_default", SizeBytes: 8192, SizeHuman: "8 KB", UsagePercent: 73},
		},
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
		usageReport: []db.UsageReportEntry{
			{Month: "2025-01", Database: "testdb", SizeBytes: 1073741824, SizeHuman: "1 GB", Connections: 10},
			{Month: "2025-01", Database: "postgres", SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 2},
		},
	}
	m.GetDiskUsageFunc = func(_ context.Context, _ string) ([]db.DiskUsage, error) {
		return []db.DiskUsage{{Database: "postgres", SizeBytes: 4096, SizeHuman: "4 KB"}}, nil
	}
	m.GetTablesFunc = func(_ context.Context) ([]db.TableStorageInfo, error) {
		return []db.TableStorageInfo{
			{Schema: "public", Table: "events", SizeBytes: 2147483648, SizeHuman: "2 GB",
				BloatPercent: 55, SkewPercent: 42, RowCount: 5000000},
			{Schema: "public", Table: "users", SizeBytes: 1073741824, SizeHuman: "1 GB",
				BloatPercent: 10, SkewPercent: 7, RowCount: 1000000},
		}, nil
	}
	m.GetBloatRecommendationsFunc = func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		return []db.Recommendation{{Type: "bloat", Schema: "public", Table: "events",
			Value: 55, Ratio: 55, Severity: "critical", Description: "bloated"}}, nil
	}
	m.GetSkewRecommendationsFunc = func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		return []db.Recommendation{{Type: "skew", Schema: "public", Table: "users",
			Value: 42, Ratio: 42, Severity: "warning", Description: "skewed"}}, nil
	}
	m.GetAgeRecommendationsFunc = func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		return []db.Recommendation{{Type: "age", Schema: "public", Table: "old_table",
			Value: 150000000, Severity: "warning", Description: "old"}}, nil
	}
	m.GetIndexBloatRecommendationsFunc = func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
		return []db.Recommendation{{Type: "index_bloat", Schema: "public", Table: "users",
			Value: 65, Ratio: 65, Severity: "critical", Description: "index bloated"}}, nil
	}
	return m
}

// boot builds the API server (real router + auth/RBAC + the injected dbFactory)
// over a fake client seeded with the cluster, plus the spy metrics recorder. The
// credential store carries a Basic + Operator user.
func (s *Scenario119Suite) boot(cluster *cbv1alpha1.CloudberryCluster, factory db.DBClientFactory) {
	env := testutil.NewTestK8sEnv(cluster)
	s.client = env.Client
	s.metrics = &scenario119MetricsRecorder{}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario119BasicUser, scenario119BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario119OperUser, scenario119OperPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, s.metrics, env.Logger, 0)
	s.handler = s.server.Handler()
}

// scenario119URL builds a full storage URL for the given suffix path.
func scenario119URL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario119Prefix + "/clusters/" + scenario119Cluster +
		suffix + sep + "namespace=" + scenario119Namespace
}

// scenario119URLFor builds a full storage URL for an arbitrary cluster name.
func scenario119URLFor(clusterName, suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario119Prefix + "/clusters/" + clusterName +
		suffix + sep + "namespace=" + scenario119Namespace
}

// do issues a request through the FULL handler with the given basic-auth identity.
func (s *Scenario119Suite) do(user, pass, method, url string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, url, strings.NewReader(""))
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// scenario119Decode JSON-decodes a recorder body into a generic map.
func scenario119Decode(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	return resp
}

// ============================================================================
// 119a-P1-F — GET /storage/disk-usage
// ============================================================================

func (s *Scenario119Suite) TestP1DiskUsage() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet, scenario119URL("/storage/disk-usage"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)

	// P.1 == status invariant: percent sourced ONLY from status.
	assert.Equal(s.T(), float64(73), resp["diskUsagePercent"],
		"119a-P1-F: diskUsagePercent must equal status.diskUsagePercent")

	usage, ok := resp["diskUsage"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), usage, 1)
	first, ok := usage[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "postgres", first["database"])

	breakdown, ok := resp["diskUsageBySegment"].([]interface{})
	require.True(s.T(), ok, "119a-P1-F: diskUsageBySegment must be present")
	require.Len(s.T(), breakdown, 1)
	seg, ok := breakdown[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "pg_default", seg["tablespace"])
}

// ============================================================================
// 119b-P2-F — GET /storage/tables
// ============================================================================

func (s *Scenario119Suite) TestP2ListTables() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet, scenario119URL("/storage/tables"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)
	assert.Equal(s.T(), float64(2), resp["total"])

	tables, ok := resp["tables"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), tables, 2)
	events, ok := tables[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "public", events["schema"])
	assert.Equal(s.T(), "events", events["table"])
	assert.Equal(s.T(), float64(2147483648), events["sizeBytes"])
	assert.Equal(s.T(), "2 GB", events["sizeHuman"])
	assert.Equal(s.T(), float64(55), events["bloatPercent"])
	assert.Equal(s.T(), float64(42), events["skewPercent"])
	assert.Equal(s.T(), float64(5000000), events["rowCount"])
}

// ============================================================================
// 119c-P3-F — GET /storage/tables/{schema}/{table}
// ============================================================================

func (s *Scenario119Suite) TestP3TableDetail() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/tables/public/users"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)
	assert.Equal(s.T(), "public", resp["schema"])
	assert.Equal(s.T(), "users", resp["table"])
	assert.Equal(s.T(), float64(2147483648), resp["sizeBytes"])
	assert.Equal(s.T(), float64(50000000), resp["rowCount"])
	assert.Equal(s.T(), float64(18), resp["bloatPercent"])
	assert.Equal(s.T(), float64(37), resp["skewPercent"])

	indexes, ok := resp["indexSizes"].([]interface{})
	require.True(s.T(), ok, "119c-P3-F: indexSizes must be present")
	require.Len(s.T(), indexes, 2)
	pkey, ok := indexes[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "users_pkey", pkey["name"])
	assert.Equal(s.T(), float64(1048576), pkey["sizeBytes"])
}

// ============================================================================
// 119d-P4-F — GET /storage/recommendations
// ============================================================================

func (s *Scenario119Suite) TestP4Recommendations() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/recommendations"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)

	// recommendationCount is the LIVE total (4), NOT the cached status (9).
	assert.Equal(s.T(), float64(4), resp["recommendationCount"])
	assert.Equal(s.T(), float64(4), resp["total"])

	recs, ok := resp["recommendations"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), recs, 4)

	gotTypes := map[string]string{} // type -> target
	for _, r := range recs {
		entry, ok := r.(map[string]interface{})
		require.True(s.T(), ok)
		gotTypes[entry["type"].(string)] = entry["target"].(string)
	}
	assert.Equal(s.T(), "public.events", gotTypes["bloat"])
	assert.Equal(s.T(), "public.users", gotTypes["skew"])
	assert.Equal(s.T(), "public.old_table", gotTypes["age"])
	assert.Equal(s.T(), "public.users", gotTypes["index_bloat"])
}

// TestP4RecommendationsNoDBFallsBackToStatus covers 119d-P4-F honesty: with no
// DB factory the count falls back to the cached status and the list is empty
// (200, not 500).
func (s *Scenario119Suite) TestP4RecommendationsNoDBFallsBackToStatus() {
	s.boot(scenario119StorageCluster(), nil)

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/recommendations"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)
	assert.Equal(s.T(), float64(9), resp["recommendationCount"],
		"count must fall back to status.recommendationCount when the DB is unreachable")
	assert.Equal(s.T(), float64(0), resp["total"])
}

// ============================================================================
// 119e-P5-F — POST /storage/recommendations/scan
// ============================================================================

func (s *Scenario119Suite) TestP5ScanEnabledDurationAdvances() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rr1 := s.do(scenario119OperUser, scenario119OperPass, http.MethodPost,
		scenario119URL("/storage/recommendations/scan"))
	rr2 := s.do(scenario119OperUser, scenario119OperPass, http.MethodPost,
		scenario119URL("/storage/recommendations/scan"))

	assert.Equal(s.T(), http.StatusAccepted, rr1.Code)
	assert.Equal(s.T(), http.StatusAccepted, rr2.Code)
	// The scan-duration histogram _count advances exactly once per POST.
	assert.Equal(s.T(), int32(2), s.metrics.durationCalls(),
		"119e-P5-F: ObserveRecommendationScanDuration must advance once per POST")

	resp := scenario119Decode(rr1)
	assert.Equal(s.T(), "scan initiated", resp["status"])
	assert.Equal(s.T(), scenario119Cluster, resp["cluster"])
}

func (s *Scenario119Suite) TestP5ScanDisabledReturns400() {
	s.boot(scenario119DisabledCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119OperUser, scenario119OperPass, http.MethodPost,
		scenario119URL("/storage/recommendations/scan"))
	require.Equal(s.T(), http.StatusBadRequest, rec.Code)
	assert.Contains(s.T(), rec.Body.String(), cases.Scenario119ScanNotEnabledCode)
	assert.Equal(s.T(), int32(0), s.metrics.durationCalls(),
		"119e-P5-F: no scan must run when disabled")
}

// ============================================================================
// 119f-P6-F — GET /storage/usage-report
// ============================================================================

func (s *Scenario119Suite) TestP6UsageReportEnabled() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/usage-report?month=2025-01"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)
	assert.Equal(s.T(), "2025-01", resp["month"])
	assert.Equal(s.T(), true, resp["usageReportEnabled"])
	assert.Equal(s.T(), float64(2), resp["total"])

	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), entries, 2)
	first, ok := entries[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "testdb", first["database"])
}

func (s *Scenario119Suite) TestP6UsageReportDisabled() {
	// Even with usable usage data, the disabled gate must short-circuit the query.
	s.boot(scenario119DisabledCluster(), &scenario119Factory{client: scenario119Populated()})

	rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/usage-report?month=2025-01"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario119Decode(rec)
	assert.Equal(s.T(), false, resp["usageReportEnabled"])
	assert.Equal(s.T(), float64(0), resp["total"])
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), entries)
}

// ============================================================================
// 119-NOTFOUND — each endpoint 404 for a missing cluster
// ============================================================================

func (s *Scenario119Suite) TestNotFoundAllEndpoints() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	endpoints := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/storage/disk-usage"},
		{http.MethodGet, "/storage/tables"},
		{http.MethodGet, "/storage/tables/public/users"},
		{http.MethodGet, "/storage/recommendations"},
		{http.MethodPost, "/storage/recommendations/scan"},
		{http.MethodGet, "/storage/usage-report"},
	}
	for _, ep := range endpoints {
		s.Run(ep.method+" "+ep.path, func() {
			url := scenario119URLFor("nonexistent", ep.path)
			// scan requires Operator; reads require Basic — both must still 404
			// (the cluster lookup precedes the DB call), so use Operator across
			// the board to isolate the NotFound contract.
			rec := s.do(scenario119OperUser, scenario119OperPass, ep.method, url)
			assert.Equal(s.T(), http.StatusNotFound, rec.Code,
				"119-NOTFOUND: %s %s must be 404 for a missing cluster", ep.method, ep.path)
			assert.Contains(s.T(), rec.Body.String(), cases.Scenario119NotFoundCode)
		})
	}
}

// ============================================================================
// 119-AUTH — reads require Basic, scan requires Operator
// ============================================================================

func (s *Scenario119Suite) TestAuthRoutePermissions() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	reads := []string{
		"/storage/disk-usage",
		"/storage/tables",
		"/storage/tables/public/users",
		"/storage/recommendations",
		"/storage/usage-report",
	}

	s.Run("missing creds -> 401", func() {
		req := httptest.NewRequest(http.MethodGet, scenario119URL("/storage/tables"), nil)
		rec := httptest.NewRecorder()
		s.handler.ServeHTTP(rec, req)
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
	})

	s.Run("reads succeed with Basic", func() {
		for _, path := range reads {
			rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet, scenario119URL(path))
			assert.Equalf(s.T(), http.StatusOK, rec.Code, "%s must succeed with Basic", path)
		}
	})

	s.Run("POST scan forbidden for Basic (requires Operator)", func() {
		rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodPost,
			scenario119URL("/storage/recommendations/scan"))
		assert.Equal(s.T(), http.StatusForbidden, rec.Code)
	})

	s.Run("POST scan succeeds with Operator", func() {
		rec := s.do(scenario119OperUser, scenario119OperPass, http.MethodPost,
			scenario119URL("/storage/recommendations/scan"))
		assert.Equal(s.T(), http.StatusAccepted, rec.Code)
	})
}

// ============================================================================
// 119-DBERR-nonfatal — a DB error yields an honest empty 200, never a 500
// ============================================================================

func (s *Scenario119Suite) TestDBErrorNonFatal() {
	s.Run("P2 tables query error -> empty 200", func() {
		m := &scenario119MockDBClient{}
		m.GetTablesFunc = func(_ context.Context) ([]db.TableStorageInfo, error) {
			return nil, fmt.Errorf("tables query failed")
		}
		s.boot(scenario119StorageCluster(), &scenario119Factory{client: m})

		rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
			scenario119URL("/storage/tables"))
		require.Equal(s.T(), http.StatusOK, rec.Code)
		resp := scenario119Decode(rec)
		assert.Equal(s.T(), float64(0), resp["total"])
	})

	s.Run("P3 detail query error -> minimal 200", func() {
		m := &scenario119MockDBClient{tableDetailErr: fmt.Errorf("detail query failed")}
		s.boot(scenario119StorageCluster(), &scenario119Factory{client: m})

		rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
			scenario119URL("/storage/tables/public/users"))
		require.Equal(s.T(), http.StatusOK, rec.Code)
		resp := scenario119Decode(rec)
		assert.Equal(s.T(), "public", resp["schema"])
		assert.Equal(s.T(), "users", resp["table"])
		assert.Nil(s.T(), resp["sizeBytes"])
	})

	s.Run("P6 usage-report query error -> empty 200 flag true", func() {
		m := &scenario119MockDBClient{usageReportErr: fmt.Errorf("usage report query failed")}
		s.boot(scenario119StorageCluster(), &scenario119Factory{client: m})

		rec := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
			scenario119URL("/storage/usage-report?month=2025-01"))
		require.Equal(s.T(), http.StatusOK, rec.Code)
		resp := scenario119Decode(rec)
		assert.Equal(s.T(), true, resp["usageReportEnabled"])
		assert.Equal(s.T(), float64(0), resp["total"])
	})

	s.Run("P1/P2 NewClient error -> empty 200", func() {
		s.boot(scenario119StorageCluster(), &scenario119Factory{err: fmt.Errorf("connection refused")})

		du := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
			scenario119URL("/storage/disk-usage"))
		require.Equal(s.T(), http.StatusOK, du.Code)
		duResp := scenario119Decode(du)
		assert.Equal(s.T(), float64(73), duResp["diskUsagePercent"])
		seg, ok := duResp["diskUsageBySegment"].([]interface{})
		require.True(s.T(), ok)
		assert.Empty(s.T(), seg)

		tbl := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
			scenario119URL("/storage/tables"))
		require.Equal(s.T(), http.StatusOK, tbl.Code)
		assert.Equal(s.T(), float64(0), scenario119Decode(tbl)["total"])
	})
}

// ============================================================================
// 119-CONTROL — a healthy DB populates all six endpoints with no error
// ============================================================================

func (s *Scenario119Suite) TestControlAllEndpointsPopulated() {
	s.boot(scenario119StorageCluster(), &scenario119Factory{client: scenario119Populated()})

	// P.1
	du := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet, scenario119URL("/storage/disk-usage"))
	require.Equal(s.T(), http.StatusOK, du.Code)
	assert.NotEmpty(s.T(), scenario119Decode(du)["diskUsageBySegment"])

	// P.2
	tbl := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet, scenario119URL("/storage/tables"))
	require.Equal(s.T(), http.StatusOK, tbl.Code)
	assert.Equal(s.T(), float64(2), scenario119Decode(tbl)["total"])

	// P.3
	detail := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/tables/public/users"))
	require.Equal(s.T(), http.StatusOK, detail.Code)
	assert.NotEmpty(s.T(), scenario119Decode(detail)["indexSizes"])

	// P.4
	recs := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/recommendations"))
	require.Equal(s.T(), http.StatusOK, recs.Code)
	assert.Equal(s.T(), float64(4), scenario119Decode(recs)["total"])

	// P.5
	scan := s.do(scenario119OperUser, scenario119OperPass, http.MethodPost,
		scenario119URL("/storage/recommendations/scan"))
	require.Equal(s.T(), http.StatusAccepted, scan.Code)

	// P.6
	usage := s.do(scenario119BasicUser, scenario119BasicPass, http.MethodGet,
		scenario119URL("/storage/usage-report?month=2025-01"))
	require.Equal(s.T(), http.StatusOK, usage.Code)
	assert.Equal(s.T(), float64(2), scenario119Decode(usage)["total"])
}

// ============================================================================
// Catalog-honest cross-check
// ============================================================================

// TestCatalogCoversFunctionalRows asserts every functional (-F) catalog row is
// honest (a known Req family + a non-empty Gate/Assert/Description) so the
// matrix cannot silently drop a rule, and every P.1–P.6 family is covered.
func (s *Scenario119Suite) TestCatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"P.1": true, "P.2": true, "P.3": true, "P.4": true, "P.5": true, "P.6": true,
		"NOTFOUND": true, "AUTH": true, "DBERR": true, "CONTROL": true, "PERSIST": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario119Cases() {
		if c.Layer != cases.Scenario119LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Assert, "%s must carry an Assert token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req], "functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario119 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Assert)
	}
	for _, req := range []string{"P.1", "P.2", "P.3", "P.4", "P.5", "P.6"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover endpoint family %s", req)
	}
}

//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
// Scenario 120: Usage Reporting (C.11, C.13) — functional
// ============================================================================
//
// This suite black-boxes the P.6 usage-report surface through the REAL
// api.Server HTTP router + auth/RBAC middleware over a fake k8s cluster store +
// an injected dbFactory whose GetUsageReport returns entries carrying the C.11
// per-table breakdown (db.TableUsage) — infra-free, no live cluster. Every
// request travels the WHOLE server: mux routing -> auth middleware ->
// withPermission RBAC gate -> handler -> the best-effort DB collector.
//
//   - 120-C11-generate-F / 120-C13-api-F: GET usage-report 200; the connected-db
//     entry carries the per-table breakdown; total==len(entries); the per-db AND
//     per-table content surface through P.6 (the C.11 "generate" content + the
//     C.13 "retrieve via API" contract, same enabled happy path).
//   - 120-MONTH-param-F: GET …?month=2026-05 -> envelope month=="2026-05".
//   - 120-DISABLED-unavailable-F: usageReport disabled -> 200
//     usageReportEnabled:false + empty entries (the SOFT gate, NOT a 400).
//   - 120-CONTROL: a healthy fast DB stub + enabled CR -> the populated REAL
//     enriched per-db + per-table shape with no error.
//   - NotFound: a missing cluster yields 404 before any DB call.
//   - DBERR-nonfatal: a usage-report query error yields an honest empty 200 with
//     usageReportEnabled:true, never a 500.
//
// It is catalog-honest via a coverage test that keeps the -F matrix from
// silently dropping a rule. The live proof is the KUBECONFIG/SCENARIO120_LIVE-
// gated Scenario 120 integration/e2e Part B.
// ============================================================================

const (
	scenario120Namespace = "cloudberry-test"
	scenario120Cluster   = "scenario120-usage"
	scenario120Prefix    = "/api/v1alpha1"

	scenario120BasicUser = "s120basic"
	scenario120BasicPass = "s120basicpass"
	scenario120OperUser  = "s120oper"
	scenario120OperPass  = "s120operpass"
)

// scenario120MockDBClient extends testutil.MockDBClient with a configurable
// GetUsageReport returning entries whose connected database carries the per-table
// breakdown (db.TableUsage). It carries the data + error the method should
// surface so the functional layer can drive the REAL enriched response shape
// through the full router without touching the shared testutil mock.
type scenario120MockDBClient struct {
	testutil.MockDBClient

	usageReport    []db.UsageReportEntry
	usageReportErr error
}

func (m *scenario120MockDBClient) GetUsageReport(
	_ context.Context, month string,
) ([]db.UsageReportEntry, error) {
	if m.usageReportErr != nil {
		return nil, m.usageReportErr
	}
	// Stamp the requested month as the scope label on each entry (mirroring the
	// production on-demand model) so the -F month-param assertion can observe it
	// threaded through GetUsageReport(ctx, month).
	out := make([]db.UsageReportEntry, len(m.usageReport))
	for i, e := range m.usageReport {
		e.Month = month
		out[i] = e
	}
	return out, nil
}

// scenario120Factory is a db.DBClientFactory returning the supplied client (or
// an error), mirroring testutil.MockDBClientFactory but typed for the local
// scenario120MockDBClient.
type scenario120Factory struct {
	client db.Client
	err    error
}

func (f *scenario120Factory) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.client, nil
}

// Scenario120Suite drives the P.6 usage-report surface through the real router
// over a fake cluster store + an injected dbFactory.
type Scenario120Suite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	client  client.Client
	ctx     context.Context
}

func TestFunctional_Scenario120(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario120Suite))
}

func (s *Scenario120Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario120Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// scenario120EnabledCluster builds a running cluster with usageReport enabled so
// the P.6 gated endpoint takes its enabled branch.
func scenario120EnabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario120Cluster, scenario120Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: true},
	}
	return cluster
}

// scenario120DisabledCluster builds a running cluster whose usageReport is
// absent (the disabled gate).
func scenario120DisabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario120Cluster, scenario120Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	return cluster
}

// scenario120Populated returns a mock DB client whose usage-report entries carry
// the per-table breakdown on the connected ("testdb") entry — the enriched C.11
// content the functional rows surface through P.6.
func scenario120Populated() *scenario120MockDBClient {
	return &scenario120MockDBClient{
		usageReport: []db.UsageReportEntry{
			{
				Database: "testdb", SizeBytes: 3221225472, SizeHuman: "3 GB", Connections: 10,
				Tables: []db.TableUsage{
					{Schema: "public", Table: "orders", SizeBytes: 2147483648, SizeHuman: "2 GB"},
					{Schema: "public", Table: "lineitem", SizeBytes: 1073741824, SizeHuman: "1 GB"},
				},
			},
			{Database: "postgres", SizeBytes: 8388608, SizeHuman: "8 MB", Connections: 2},
		},
	}
}

// boot builds the API server (real router + auth/RBAC + the injected dbFactory)
// over a fake client seeded with the cluster. The credential store carries a
// Basic + Operator user.
func (s *Scenario120Suite) boot(cluster *cbv1alpha1.CloudberryCluster, factory db.DBClientFactory) {
	env := testutil.NewTestK8sEnv(cluster)
	s.client = env.Client

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario120BasicUser, scenario120BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario120OperUser, scenario120OperPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, env.Logger, 0)
	s.handler = s.server.Handler()
}

// scenario120URL builds a full usage-report URL for the given suffix path.
func scenario120URL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario120Prefix + "/clusters/" + scenario120Cluster +
		suffix + sep + "namespace=" + scenario120Namespace
}

// scenario120URLFor builds a full usage-report URL for an arbitrary cluster name.
func scenario120URLFor(clusterName, suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario120Prefix + "/clusters/" + clusterName +
		suffix + sep + "namespace=" + scenario120Namespace
}

// do issues a request through the FULL handler with the given basic-auth identity.
func (s *Scenario120Suite) do(user, pass, method, url string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, url, strings.NewReader(""))
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// scenario120Decode JSON-decodes a recorder body into a generic map.
func scenario120Decode(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	return resp
}

// ============================================================================
// 120-C11-generate-F / 120-C13-api-F — GET /storage/usage-report (enabled)
// ============================================================================

// TestEnabledGenerateAndRetrieve drives the enabled happy path: the report is
// GENERATED with per-database AND per-table content (C.11) and RETRIEVED via the
// API (C.13). The connected-db entry carries the per-table breakdown; the
// non-connected entry omits the empty tables array.
func (s *Scenario120Suite) TestEnabledGenerateAndRetrieve() {
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: scenario120Populated()})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URL("/storage/usage-report?month=2026-05"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario120Decode(rec)

	assert.Equal(s.T(), true, resp["usageReportEnabled"],
		"120-C13-api-F: an enabled CR must report usageReportEnabled:true")
	assert.Equal(s.T(), "2026-05", resp["month"])
	assert.Equal(s.T(), float64(2), resp["total"], "total must equal len(entries)")

	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), entries, 2)

	// Per-database content on the connected entry.
	first, ok := entries[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "testdb", first["database"])
	assert.Equal(s.T(), float64(3221225472), first["sizeBytes"])
	assert.Equal(s.T(), float64(10), first["connections"])

	// Per-table breakdown on the connected entry (C.11 content), size-desc.
	tables, ok := first["tables"].([]interface{})
	require.True(s.T(), ok, "120-C11-generate-F: the connected-db entry must carry tables[]")
	require.Len(s.T(), tables, 2)
	t0, ok := tables[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "public", t0["schema"])
	assert.Equal(s.T(), "orders", t0["table"])
	assert.Equal(s.T(), float64(2147483648), t0["sizeBytes"])
	assert.Equal(s.T(), "2 GB", t0["sizeHuman"])

	// The non-connected database omits the empty tables array (omitempty).
	second, ok := entries[1].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "postgres", second["database"])
	_, hasTables := second["tables"]
	assert.False(s.T(), hasTables, "the non-connected entry must omit the empty tables array")
}

// ============================================================================
// 120-MONTH-param-F — GET …?month=2026-05 -> envelope month=="2026-05"
// ============================================================================

func (s *Scenario120Suite) TestMonthParamThreads() {
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: scenario120Populated()})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URL("/storage/usage-report?month=2026-05"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario120Decode(rec)
	assert.Equal(s.T(), "2026-05", resp["month"],
		"120-MONTH-param-F: the ?month= query param must echo in envelope.month")

	// The month is also threaded into GetUsageReport(ctx, month): every entry
	// carries the scope label.
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	for _, e := range entries {
		entry, ok := e.(map[string]interface{})
		require.True(s.T(), ok)
		assert.Equal(s.T(), "2026-05", entry["month"],
			"the month must thread through to each entry's scope label")
	}
}

// ============================================================================
// 120-DISABLED-unavailable-F — usageReport disabled -> 200 unavailable empty
// ============================================================================

func (s *Scenario120Suite) TestDisabledUnavailable() {
	// Even with usable usage data, the disabled gate must short-circuit the query.
	s.boot(scenario120DisabledCluster(), &scenario120Factory{client: scenario120Populated()})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URL("/storage/usage-report?month=2026-05"))
	require.Equal(s.T(), http.StatusOK, rec.Code,
		"120-DISABLED-unavailable-F: a disabled READ is a SOFT 200 gate, NOT a 400")
	resp := scenario120Decode(rec)
	assert.Equal(s.T(), false, resp["usageReportEnabled"])
	assert.Equal(s.T(), float64(0), resp["total"])
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), entries)
}

// ============================================================================
// 120-CONTROL — a healthy DB populates the enriched shape with no error
// ============================================================================

func (s *Scenario120Suite) TestControlPopulatedEnrichedShape() {
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: scenario120Populated()})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URL("/storage/usage-report?month=2026-05"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario120Decode(rec)
	assert.Equal(s.T(), true, resp["usageReportEnabled"])
	assert.Equal(s.T(), float64(2), resp["total"])
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), entries, 2)
	first, ok := entries[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.NotEmpty(s.T(), first["tables"],
		"120-CONTROL: the healthy enabled path must surface the per-table breakdown")
}

// ============================================================================
// 120-NOTFOUND — a missing cluster yields 404 before any DB call
// ============================================================================

func (s *Scenario120Suite) TestNotFoundMissingCluster() {
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: scenario120Populated()})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URLFor("nonexistent", "/storage/usage-report"))
	assert.Equal(s.T(), http.StatusNotFound, rec.Code,
		"120-NOTFOUND: a missing cluster must be 404")
	assert.Contains(s.T(), rec.Body.String(), cases.Scenario120NotFoundCode)
}

// ============================================================================
// 120-DBERR-nonfatal — a usage-report query error -> honest empty 200 flag true
// ============================================================================

func (s *Scenario120Suite) TestDBErrorNonFatal() {
	m := &scenario120MockDBClient{usageReportErr: fmt.Errorf("usage report query failed")}
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: m})

	rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
		scenario120URL("/storage/usage-report?month=2026-05"))
	require.Equal(s.T(), http.StatusOK, rec.Code,
		"a DB error must yield an honest empty 200, never a 500")
	resp := scenario120Decode(rec)
	assert.Equal(s.T(), true, resp["usageReportEnabled"])
	assert.Equal(s.T(), float64(0), resp["total"])
}

// ============================================================================
// 120-AUTH — the usage-report read requires PermissionBasic
// ============================================================================

func (s *Scenario120Suite) TestAuthRequiresBasic() {
	s.boot(scenario120EnabledCluster(), &scenario120Factory{client: scenario120Populated()})

	s.Run("missing creds -> 401", func() {
		req := httptest.NewRequest(http.MethodGet, scenario120URL("/storage/usage-report"), nil)
		rec := httptest.NewRecorder()
		s.handler.ServeHTTP(rec, req)
		assert.Equal(s.T(), http.StatusUnauthorized, rec.Code)
	})

	s.Run("read succeeds with Basic", func() {
		rec := s.do(scenario120BasicUser, scenario120BasicPass, http.MethodGet,
			scenario120URL("/storage/usage-report"))
		assert.Equal(s.T(), http.StatusOK, rec.Code)
	})
}

// ============================================================================
// Catalog-honest cross-check
// ============================================================================

// TestCatalogCoversFunctionalRows asserts every functional (-F) catalog row is
// honest (a known Req family + a non-empty Gate/Assert/Description) so the
// matrix cannot silently drop a rule, and every C.11 / C.13 / MONTH / DISABLED /
// CONTROL family is covered at the functional layer.
func (s *Scenario120Suite) TestCatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"C.11": true, "C.13": true, "MONTH": true,
		"DISABLED": true, "CONTROL": true, "PERSIST": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario120Cases() {
		if c.Layer != cases.Scenario120LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Channel, "%s must carry a Channel", c.ID)
		assert.NotEmptyf(s.T(), c.Assert, "%s must carry an Assert token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req], "functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario120 %s (%s, gate=%s, ch=%s): %s", c.ID, c.Req, c.Gate, c.Channel, c.Assert)
	}
	for _, req := range []string{"C.11", "C.13", "MONTH", "DISABLED", "CONTROL"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover family %s", req)
	}
}

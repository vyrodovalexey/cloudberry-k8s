//go:build functional

package functional

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 121: All Storage CLI Commands (L.1–L.6) — functional
// ============================================================================
//
// This suite proves the cloudberry-ctl `storage` CLI surface drives the
// operator storage REST API (the Scenario 119/120 P.1–P.6 endpoints) with the
// documented EFFECT, WITHOUT a live cluster. The CLI verbs themselves are
// exercised in-process (cobra-exec) in
// cmd/cloudberry-ctl/scenario121_storage_cli_test.go against a recording
// stand-in; THIS suite closes the loop the other end: it drives the SAME client
// the CLI uses (internal/ctl.OperatorClient + ctl.ClusterSubresourcePath path
// helpers) over a REAL api.Server router (fake controller-runtime client + fake
// db.Client + auth/RBAC middleware) wrapped in an httptest server, and asserts
// the REAL operator response for each L.x:
//
//   L.1 storage disk-usage              → GET  storage/disk-usage      (diskUsagePercent)
//   L.2 storage tables list             → GET  storage/tables          (total == len)
//   L.3 storage tables detail --flags   → GET  storage/tables/{s}/{t}  (schema/table echoed)
//   L.4 storage recommendations list    → GET  storage/recommendations (recommendationCount)
//   L.5 storage recommendations scan    → POST storage/recommendations/scan (202, Operator)
//   L.6 storage usage-report --month    → GET  storage/usage-report    (month label round-trips)
//
// Each verb's request is built EXACTLY as the production CLI builds it (same ctl
// path helper + method), so the CLI→operator contract is exercised end-to-end
// through the whole router (mux → auth → withPermission RBAC → handler → fake
// db.Client). The CLI's own required-flag guards (which fail BEFORE any API
// call) are covered by the cobra-exec suite; here we assert the operator EFFECT
// those well-formed requests produce.
//
// This storage-recommendations CLI family (L.1–L.6) is DISTINCT from the
// data-loading L.1–L.16 CLI family of Scenario 108.
// ============================================================================

const (
	scenario121Namespace = "cloudberry-test"
	scenario121Cluster   = "s121-storage"

	scenario121BasicUser = "s121basic"
	scenario121BasicPass = "s121basicpass"
	scenario121OperUser  = "s121oper"
	scenario121OperPass  = "s121operpass"
)

// Scenario121Suite drives the storage CLI verbs through the real ctl
// OperatorClient against a real api.Server over a fake client + fake db.
type Scenario121Suite struct {
	suite.Suite
	server   *api.Server
	httpSrv  *httptest.Server
	client   client.Client
	dbClient *testutil.MockDBClient
	ctx      context.Context
}

func TestFunctional_Scenario121(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario121Suite))
}

func (s *Scenario121Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario121Suite) TearDownTest() {
	if s.httpSrv != nil {
		s.httpSrv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario121StorageCluster builds a running cluster with storage management
// enabled (disk monitoring + recommendation scan + usage report) so the gated
// endpoints take their enabled branches.
func scenario121StorageCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario121Cluster, scenario121Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Status.DiskUsagePercent = 73
	cluster.Status.RecommendationCount = 5
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

// boot builds the API server (real router + auth/RBAC + a MockDBClient factory)
// over a fake client seeded with the cluster, wraps it in an httptest server and
// stores the handles on the suite.
func (s *Scenario121Suite) boot(cluster *cbv1alpha1.CloudberryCluster) {
	env := testutil.NewTestK8sEnv(cluster)
	s.client = env.Client
	s.dbClient = &testutil.MockDBClient{}
	factory := &testutil.MockDBClientFactory{Client: s.dbClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario121BasicUser, scenario121BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario121OperUser, scenario121OperPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, env.Logger, 0)
	s.httpSrv = httptest.NewServer(s.server.Handler())
}

// ctlClient returns a ctl.OperatorClient (the SAME client the CLI uses) pointed
// at the in-process api.Server with the given basic-auth identity.
func (s *Scenario121Suite) ctlClient(user, pass string) *ctl.OperatorClient {
	return ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.httpSrv.URL,
		Username:   user,
		Password:   pass,
		AuthMethod: "basic",
		Timeout:    10 * time.Second,
	})
}

// scenario121StoragePath builds the namespaced storage subresource path the CLI
// verbs build via ctl.ClusterSubresourcePath.
func scenario121StoragePath(subresource string) string {
	return ctl.ClusterSubresourcePath(scenario121Cluster,
		"storage/"+subresource, scenario121Namespace)
}

// ----------------------------------------------------------------------------
// L.1 — storage disk-usage (read)
// ----------------------------------------------------------------------------

// TestDiskUsage covers 121a-L1-F: the disk-usage read reaches the operator and
// returns 200 with the status-sourced diskUsagePercent.
func (s *Scenario121Suite) TestDiskUsage() {
	s.boot(scenario121StorageCluster())
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("disk-usage"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), float64(73), resp.Body["diskUsagePercent"],
		"diskUsagePercent must be sourced from the cluster status")
}

// ----------------------------------------------------------------------------
// L.2 — storage tables list (read)
// ----------------------------------------------------------------------------

// TestTablesList covers 121b-L2-F: the tables list read reaches the operator and
// returns 200 with the per-table storage rows (total == len of the mock rows).
func (s *Scenario121Suite) TestTablesList() {
	s.boot(scenario121StorageCluster())
	s.dbClient.GetTablesFunc = func(_ context.Context) ([]db.TableStorageInfo, error) {
		return []db.TableStorageInfo{
			{Schema: "public", Table: "events", SizeBytes: 2 << 30, SizeHuman: "2 GB"},
			{Schema: "public", Table: "orders", SizeBytes: 1 << 30, SizeHuman: "1 GB"},
		}, nil
	}
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("tables"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), float64(2), resp.Body["total"],
		"total must equal the number of table rows")
}

// ----------------------------------------------------------------------------
// L.3 — storage tables detail --schema --table (flag-driven path segments)
// ----------------------------------------------------------------------------

// TestTablesDetailFlags covers 121c-L3-F: the flag-driven detail read builds the
// .../storage/tables/public/orders path and returns 200 with that table's detail
// (schema/table echoed). The ctl path helper builds EXACTLY the path the CLI's
// --schema/--table flags produce.
func (s *Scenario121Suite) TestTablesDetailFlags() {
	s.boot(scenario121StorageCluster())
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("tables/public/orders"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), "public", resp.Body["schema"])
	assert.Equal(s.T(), "orders", resp.Body["table"])
}

// ----------------------------------------------------------------------------
// L.4 — storage recommendations list (read)
// ----------------------------------------------------------------------------

// TestRecommendationsList covers 121d-L4-F: the recommendations list read reaches
// the operator and returns 200 with the recommendationCount shape.
func (s *Scenario121Suite) TestRecommendationsList() {
	s.boot(scenario121StorageCluster())
	s.dbClient.GetBloatRecommendationsFunc = func(
		_ context.Context, _ db.RecommendationThresholds,
	) ([]db.Recommendation, error) {
		return []db.Recommendation{
			{Type: "bloat", Schema: "public", Table: "events", Severity: "critical"},
		}, nil
	}
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("recommendations"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), float64(1), resp.Body["recommendationCount"],
		"recommendationCount must reflect the live recommendation total")
}

// ----------------------------------------------------------------------------
// L.5 — storage recommendations scan (Operator-tier POST; 202)
// ----------------------------------------------------------------------------

// TestRecommendationsScan covers 121e-L5-F: the scan POST reaches the operator
// and is accepted (202) for an Operator-tier caller (scan enabled).
func (s *Scenario121Suite) TestRecommendationsScan() {
	s.boot(scenario121StorageCluster())
	oper := s.ctlClient(scenario121OperUser, scenario121OperPass)

	resp, err := oper.Post(s.ctx, scenario121StoragePath("recommendations/scan"), nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.StatusAccepted, resp.StatusCode,
		"recommendations scan must be accepted (202) when enabled")
}

// TestRecommendationsScanForbiddenBelowTier covers 121-PERSIST (functional
// slice): the scan is Operator-tier (P.5); a Basic identity is rejected with 403
// through the real withPermission gate, while the read verbs are Basic-tier.
func (s *Scenario121Suite) TestRecommendationsScanForbiddenBelowTier() {
	s.boot(scenario121StorageCluster())

	_, err := s.ctlClient(scenario121BasicUser, scenario121BasicPass).
		Post(s.ctx, scenario121StoragePath("recommendations/scan"), nil)
	require.Error(s.T(), err)
	apiErr, ok := err.(*ctl.APIError)
	require.Truef(s.T(), ok, "expected a ctl.APIError, got %T", err)
	assert.Equal(s.T(), http.StatusForbidden, apiErr.StatusCode,
		"a Basic identity must be forbidden from the Operator-tier scan")
}

// ----------------------------------------------------------------------------
// L.6 — storage usage-report --month (the month label round-trips)
// ----------------------------------------------------------------------------

// TestUsageReportMonth covers 121f-L6-F + 121-MONTH-period: the usage-report read
// threads month=2026-05 to the operator and the report echoes that month LABEL
// (the reporting period round-trips). HONEST: an on-demand report labeled by the
// month, NOT a persisted historical snapshot.
func (s *Scenario121Suite) TestUsageReportMonth() {
	s.boot(scenario121StorageCluster())
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("usage-report")+"&month=2026-05")
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), "2026-05", resp.Body["month"],
		"the --month reporting period must round-trip into the report label")
	assert.Equal(s.T(), true, resp.Body["usageReportEnabled"])
}

// TestUsageReportNoMonth covers 121f-L6-usage-nomonth (functional slice): without
// --month the report echoes an empty month label (no fabricated period).
func (s *Scenario121Suite) TestUsageReportNoMonth() {
	s.boot(scenario121StorageCluster())
	c := s.ctlClient(scenario121BasicUser, scenario121BasicPass)

	resp, err := c.Get(s.ctx, scenario121StoragePath("usage-report"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), "", resp.Body["month"],
		"without --month the report must echo an empty month label")
}

// ----------------------------------------------------------------------------
// Catalog-honest cross-check
// ----------------------------------------------------------------------------

// TestCatalogHonest iterates cases.Scenario121Cases() and asserts the catalog is
// well-formed (unique IDs, every L.1–L.6 + CONTROL + PERSIST family present, the
// four DETAIL-* rows + MONTH-period present, every row carries a Layer +
// Method/Path-or-negative + Assert + Description with a known Layer token).
func (s *Scenario121Suite) TestCatalogHonest() {
	catalog := cases.Scenario121Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario121LayerUnit,
		cases.Scenario121LayerFunctional,
		cases.Scenario121LayerLive,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
		})
	}
	for i := 1; i <= 6; i++ {
		req := fmt.Sprintf("L.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover CLI command family %s", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL negative row")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST parity row")

	// The named edge rows must all be present.
	for _, id := range []string{
		"121-DETAIL-flags", "121-DETAIL-positional", "121-DETAIL-precedence",
		"121-DETAIL-missing", "121-MONTH-period",
	} {
		assert.Truef(s.T(), seen[id], "catalog must carry the named edge row %s", id)
	}
}

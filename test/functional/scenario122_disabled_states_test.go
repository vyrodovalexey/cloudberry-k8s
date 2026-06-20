//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 122: Disabled States (C.2 / C.4 / C.12) + Re-enablement — functional
// ============================================================================
//
// This functional layer drives the PUBLIC AdminReconciler.Reconcile entrypoint
// (C.2 diskMonitoring + C.4 recommendationScan) over a fake-client TestK8sEnv
// with an injected dbFactory and a real PrometheusRecorder, and the C.12
// usage-report soft-gate through the REAL api.Server HTTP router + auth/RBAC
// middleware — infra-free, no live cluster.
//
//   - 122a C.2: diskMonitoring:false → no measurement (DB factory never called),
//     status.diskUsagePercent==0 + cloudberry_disk_usage_percent==0 (reset-on-
//     disable). Re-enable → status + gauge repopulate from the measured value.
//   - 122b C.4: recommendationScan disabled → no scan, status.recommendationCount
//     ==0 + recommendations_total{type}==0 for all four types + CronJob GC'd;
//     POST scan → 400 RECOMMENDATION_SCAN_NOT_ENABLED. Re-enable → CronJob +
//     count + per-type gauges repopulate.
//   - 122c C.12: usageReport disabled → 200 {usageReportEnabled:false, empty}
//     (soft-gate, not 400). Re-enable → 200 {usageReportEnabled:true, entries[]}.
//   - 122-CONTROL: the disabled→enabled→disabled round-trip returns no error.
//
// It is catalog-honest via a coverage test that keeps the -F matrix from
// silently dropping a rule. The live proof is the KUBECONFIG/SCENARIO122_LIVE-
// gated Scenario 122 integration/e2e Part B.
// ============================================================================

const (
	scenario122Namespace = "cloudberry-test"
	scenario122Cluster   = "s122-usage"
	scenario122Prefix    = "/api/v1alpha1"

	scenario122BasicUser = "s122basic"
	scenario122BasicPass = "s122basicpass"
	scenario122OperUser  = "s122oper"
	scenario122OperPass  = "s122operpass"
)

// recommendationTypes122 mirrors the controller's canonical per-type set.
var recommendationTypes122 = []string{
	cases.Scenario122TypeBloat,
	cases.Scenario122TypeSkew,
	cases.Scenario122TypeAge,
	cases.Scenario122TypeIndexBloat,
}

// Scenario122Suite drives the disabled/re-enable behaviors over the reconciler
// (C.2/C.4) and the api.Server router (C.12).
type Scenario122Suite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context

	server  *api.Server
	handler http.Handler
}

func TestFunctional_Scenario122(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario122Suite))
}

func (s *Scenario122Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario122Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
		s.server = nil
	}
}

func (s *Scenario122Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario122Reconciler builds an AdminReconciler over a fake-client TestK8sEnv
// seeded with the supplied cluster, wired with the supplied dbFactory and a real
// PrometheusRecorder over reg so the gauges can be inspected.
func (s *Scenario122Suite) scenario122Reconciler(
	cluster *cbv1alpha1.CloudberryCluster,
	dbFactory db.DBClientFactory,
	reg *prometheus.Registry,
) *controller.AdminReconciler {
	s.env = testutil.NewTestK8sEnv(cluster)
	return controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), dbFactory, metrics.NewPrometheusRecorder(reg), s.env.Logger,
	)
}

// scenario122Rec builds a recommendation row of a given type/schema/table/ratio.
func scenario122Rec(recType, schema, table string, ratio float64) db.Recommendation {
	return db.Recommendation{Type: recType, Schema: schema, Table: table, Ratio: ratio}
}

// scenario122GaugeValue gathers the gauge series whose labels match all of the
// provided key/value pairs (found=false when no matching series is present).
func scenario122GaugeValue(
	t require.TestingT, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			matched := true
			for k, v := range want {
				if labels[k] != v {
					matched = false
					break
				}
			}
			if matched {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// scenario122DiskGauge gathers cloudberry_disk_usage_percent for a
// {cluster,namespace} series.
func scenario122DiskGauge(
	t require.TestingT, reg *prometheus.Registry, cluster, namespace string,
) (float64, bool) {
	return scenario122GaugeValue(t, reg, cases.Scenario122DiskMetricName, map[string]string{
		"cluster": cluster, "namespace": namespace,
	})
}

// scenario122RecGauge gathers cloudberry_recommendations_total for a
// {cluster,namespace,type} series.
func scenario122RecGauge(
	t require.TestingT, reg *prometheus.Registry, cluster, namespace, recType string,
) (float64, bool) {
	return scenario122GaugeValue(t, reg, cases.Scenario122RecsMetricName, map[string]string{
		"cluster": cluster, "namespace": namespace, "type": recType,
	})
}

// scenario122CronJobExists reports whether the recommendation-scan CronJob exists
// for the cluster in the fake client.
func (s *Scenario122Suite) scenario122CronJobExists(cluster *cbv1alpha1.CloudberryCluster) bool {
	cj := &batchv1.CronJob{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj)
	if err == nil {
		return true
	}
	require.True(s.T(), apierrors.IsNotFound(err), "unexpected error getting CronJob: %v", err)
	return false
}

// scenario122ScanCluster builds a running cluster with diskMonitoring on and the
// recommendation scan enabled (the four thresholds + a schedule so the CronJob is
// created).
func scenario122ScanCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:        true,
			Schedule:       "0 3 * * 0",
			BloatThreshold: 20,
			ScanDuration:   "2h",
		},
	}
	return cluster
}

// ----------------------------------------------------------------------------
// 122a C.2 — diskMonitoring disabled (reset) + re-enable (repopulate)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario122a_C2_DisabledResets covers 122a-C2-disabled-F:
// diskMonitoring:false → reconcileStorage does NOT measure (the DB factory is
// never called), status.diskUsagePercent is reset to 0, and the
// cloudberry_disk_usage_percent gauge is published 0.
func (s *Scenario122Suite) TestFunctional_Scenario122a_C2_DisabledResets() {
	cluster := testutil.NewClusterBuilder("s122-c2-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 42 // stale prior value.

	calls := 0
	dbFactory := &countingDBFactory122{calls: &calls}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario122Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Zero(s.T(), calls, "122a-C2-disabled-F: DB factory must not be called when monitoring is off")
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(0), updated.Status.DiskUsagePercent,
		"122a-C2-disabled-F: status.diskUsagePercent must be reset to 0 on disable")
	gauge, found := scenario122DiskGauge(s.T(), reg, cluster.Name, cluster.Namespace)
	require.True(s.T(), found, "122a-C2-disabled-F: the disabled reset must publish the gauge as 0")
	assert.InDelta(s.T(), 0.0, gauge, 0.001)
}

// TestFunctional_Scenario122a_C2_Reenable covers 122a-C2-reenable-F: flip
// diskMonitoring back on → recordDiskUsage resumes; status + gauge repopulate
// from the measured value (M.1==S.1).
func (s *Scenario122Suite) TestFunctional_Scenario122a_C2_Reenable() {
	cluster := testutil.NewClusterBuilder("s122-c2-reenable", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	cluster.Status.DiskUsagePercent = 0 // started from the disabled-reset value.

	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 63, nil },
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario122Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(63), updated.Status.DiskUsagePercent,
		"122a-C2-reenable-F: status must repopulate from the measured value")
	gauge, found := scenario122DiskGauge(s.T(), reg, cluster.Name, cluster.Namespace)
	require.True(s.T(), found, "122a-C2-reenable-F: the re-enabled path must publish the measured gauge")
	assert.InDelta(s.T(), 63.0, gauge, 0.001)
	assert.InDelta(s.T(), float64(updated.Status.DiskUsagePercent), gauge, 0.001,
		"122a-C2-reenable-F: gauge must match status (M.1==S.1)")
}

// ----------------------------------------------------------------------------
// 122b C.4 — recommendationScan disabled (clear) + re-enable (repopulate)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario122b_C4_DisabledClears covers 122b-C4-disabled-F:
// recommendationScan disabled → no scan runs, status.recommendationCount is
// cleared to 0, all four recommendations_total{type} gauges read 0, and the
// recommendation-scan CronJob is GC'd.
func (s *Scenario122Suite) TestFunctional_Scenario122b_C4_DisabledClears() {
	tests := []struct {
		name string
		scan *cbv1alpha1.RecommendationScanSpec
	}{
		{name: "scan nil", scan: nil},
		{name: "scan disabled", scan: &cbv1alpha1.RecommendationScanSpec{Enabled: false}},
	}
	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := testutil.NewClusterBuilder("s122-c4-disabled", "default").
				WithFinalizer().
				WithPhase(cbv1alpha1.ClusterPhaseRunning).
				Build()
			cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring:     true,
				RecommendationScan: tt.scan,
			}
			cluster.Status.RecommendationCount = 7 // stale prior value.

			// A scan-capable mock that, if consulted, would produce a non-zero
			// count — so the disabled path cannot accidentally count.
			dbFactory := &testutil.MockDBClientFactory{
				Client: &testutil.MockDBClient{
					GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 10, nil },
					GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
						return []db.Recommendation{scenario122Rec(cases.Scenario122TypeBloat, "public", "x", 99)}, nil
					},
				},
			}
			reg := prometheus.NewRegistry()
			reconciler := s.scenario122Reconciler(cluster, dbFactory, reg)

			_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err)

			updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			assert.Equal(s.T(), int32(0), updated.Status.RecommendationCount,
				"122b-C4-disabled-F: a stale count must be cleared to 0 when the scan is disabled")
			for _, recType := range recommendationTypes122 {
				v, found := scenario122RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, recType)
				require.Truef(s.T(), found,
					"122b-C4-disabled-F: recommendations_total{type=%s} must be published as 0", recType)
				assert.InDelta(s.T(), 0.0, v, 0.001)
			}
			assert.False(s.T(), s.scenario122CronJobExists(updated),
				"122b-C4-disabled-F: the recommendation-scan CronJob must be GC'd when disabled")
		})
	}
}

// TestFunctional_Scenario122b_C4_Reenable covers 122b-C4-reenable-F: flip the
// scan back on → recordRecommendations resumes (count = sum of per-type),
// per-type gauges repopulate, and the CronJob is (re-)created.
func (s *Scenario122Suite) TestFunctional_Scenario122b_C4_Reenable() {
	cluster := scenario122ScanCluster("s122-c4-reenable")
	cluster.Status.RecommendationCount = 0 // started from the disabled-clear value.

	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 10, nil },
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{
					scenario122Rec(cases.Scenario122TypeBloat, "public", "t1", 25),
					scenario122Rec(cases.Scenario122TypeBloat, "public", "t2", 55),
				}, nil
			},
			GetSkewRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario122Rec(cases.Scenario122TypeSkew, "public", "s1", 40)}, nil
			},
			GetIndexBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario122Rec(cases.Scenario122TypeIndexBloat, "public", "i1", 70)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario122Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	// 2 bloat + 1 skew + 0 age + 1 index = 4.
	assert.Equal(s.T(), int32(4), updated.Status.RecommendationCount,
		"122b-C4-reenable-F: count must repopulate as the scan total")
	want := map[string]float64{
		cases.Scenario122TypeBloat: 2, cases.Scenario122TypeSkew: 1,
		cases.Scenario122TypeAge: 0, cases.Scenario122TypeIndexBloat: 1,
	}
	for recType, exp := range want {
		v, found := scenario122RecGauge(s.T(), reg, cluster.Name, cluster.Namespace, recType)
		require.Truef(s.T(), found,
			"122b-C4-reenable-F: recommendations_total{type=%s} must be published on re-enable", recType)
		assert.InDeltaf(s.T(), exp, v, 0.001, "gauge for %s", recType)
	}
	assert.True(s.T(), s.scenario122CronJobExists(updated),
		"122b-C4-reenable-F: the recommendation-scan CronJob must be re-created on re-enable")
}

// countingDBFactory122 returns a mock that records whether it was reached so the
// disabled no-op (C.2) case can assert the DB layer is never used. NewClient
// errors are impossible here; the measured value would be non-zero if reached.
type countingDBFactory122 struct {
	calls *int
}

func (f *countingDBFactory122) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	*f.calls++
	return &testutil.MockDBClient{
		GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 99, nil },
	}, nil
}

// ----------------------------------------------------------------------------
// 122-CONTROL — disabled→enabled→disabled round-trip returns no error
// ----------------------------------------------------------------------------

// TestFunctional_Scenario122_ControlRoundTrip covers 122-CONTROL: the
// disabled→enabled→disabled reconcile round-trip returns NO error at each step
// with a healthy DB stub (no-false-positive control).
func (s *Scenario122Suite) TestFunctional_Scenario122_ControlRoundTrip() {
	cluster := testutil.NewClusterBuilder("s122-control", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}

	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 25, nil },
			GetBloatRecommendationsFunc: func(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
				return []db.Recommendation{scenario122Rec(cases.Scenario122TypeBloat, "public", "t1", 25)}, nil
			},
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario122Reconciler(cluster, dbFactory, reg)

	// disabled.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "122-CONTROL: the disabled reconcile must not error")

	// enabled (persist the flip first, as the API server would).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * *", BloatThreshold: 10,
		},
	}
	require.NoError(s.T(), s.env.Client.Update(s.ctx, updated))
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "122-CONTROL: the enabled reconcile must not error")

	// re-disabled.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	require.NoError(s.T(), s.env.Client.Update(s.ctx, updated))
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "122-CONTROL: the re-disabled reconcile must not error")
}

// ----------------------------------------------------------------------------
// 122c C.12 — usageReport soft-gate disabled + re-enable (api.Server router)
// ----------------------------------------------------------------------------

// scenario122MockDBClient extends testutil.MockDBClient with a configurable
// GetUsageReport so the C.12 re-enable path can surface real entries through the
// full router.
type scenario122MockDBClient struct {
	testutil.MockDBClient

	usageReport []db.UsageReportEntry
}

func (m *scenario122MockDBClient) GetUsageReport(
	_ context.Context, _ string,
) ([]db.UsageReportEntry, error) {
	return m.usageReport, nil
}

// scenario122Factory is a db.DBClientFactory returning the supplied client.
type scenario122Factory struct {
	client db.Client
}

func (f *scenario122Factory) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	return f.client, nil
}

// scenario122UsageCluster builds a running cluster with usageReport set per the
// supplied enabled flag (nil = the disabled gate when enabled is false).
func scenario122UsageCluster(usageEnabled bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario122Cluster, scenario122Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	storage := &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	if usageEnabled {
		storage.UsageReport = &cbv1alpha1.UsageReportSpec{Enabled: true}
	}
	cluster.Spec.Storage = storage
	return cluster
}

// scenario122BootAPI builds the API server (real router + auth/RBAC + the
// injected dbFactory) over a fake client seeded with the cluster.
func (s *Scenario122Suite) scenario122BootAPI(
	cluster *cbv1alpha1.CloudberryCluster, factory db.DBClientFactory,
) {
	env := testutil.NewTestK8sEnv(cluster)
	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario122BasicUser, scenario122BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario122OperUser, scenario122OperPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, env.Logger, 0)
	s.handler = s.server.Handler()
}

// scenario122URL builds a full cluster-scoped URL for the given suffix path.
func scenario122URL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario122Prefix + "/clusters/" + scenario122Cluster +
		suffix + sep + "namespace=" + scenario122Namespace
}

// scenario122Do issues a request through the FULL handler with the given
// basic-auth identity.
func (s *Scenario122Suite) scenario122Do(user, pass, method, url string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, url, strings.NewReader(""))
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// scenario122Decode JSON-decodes a recorder body into a generic map.
func scenario122Decode(t require.TestingT, rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	return resp
}

// TestFunctional_Scenario122c_C12_DisabledSoftGate covers 122c-C12-disabled-F:
// usageReport disabled/nil/storage-nil → 200 {usageReportEnabled:false, empty}
// (the SOFT gate, NOT a 400) through the REAL api.Server router.
func (s *Scenario122Suite) TestFunctional_Scenario122c_C12_DisabledSoftGate() {
	// Even with usable usage data, the disabled gate must short-circuit the query.
	dbClient := &scenario122MockDBClient{usageReport: scenario122UsageEntries()}
	s.scenario122BootAPI(scenario122UsageCluster(false), &scenario122Factory{client: dbClient})

	rec := s.scenario122Do(scenario122BasicUser, scenario122BasicPass, http.MethodGet,
		scenario122URL("/storage/usage-report?month=2026-06"))
	require.Equal(s.T(), http.StatusOK, rec.Code,
		"122c-C12-disabled-F: a disabled READ is a SOFT 200 gate, NOT a 400")
	resp := scenario122Decode(s.T(), rec)
	assert.Equal(s.T(), false, resp["usageReportEnabled"],
		"122c-C12-disabled-F: disabled usage report must report usageReportEnabled:false")
	assert.Equal(s.T(), float64(0), resp["total"])
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	assert.Empty(s.T(), entries, "122c-C12-disabled-F: disabled usage report must be empty")
}

// TestFunctional_Scenario122c_C12_Reenable covers 122c-C12-reenable-F: flip
// usageReport.enabled:true → 200 {usageReportEnabled:true, entries[…]}.
func (s *Scenario122Suite) TestFunctional_Scenario122c_C12_Reenable() {
	dbClient := &scenario122MockDBClient{usageReport: scenario122UsageEntries()}
	s.scenario122BootAPI(scenario122UsageCluster(true), &scenario122Factory{client: dbClient})

	rec := s.scenario122Do(scenario122BasicUser, scenario122BasicPass, http.MethodGet,
		scenario122URL("/storage/usage-report?month=2026-06"))
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario122Decode(s.T(), rec)
	assert.Equal(s.T(), true, resp["usageReportEnabled"],
		"122c-C12-reenable-F: re-enabled usage report must report usageReportEnabled:true")
	assert.Equal(s.T(), float64(1), resp["total"], "re-enabled report carries the collected entries")
	entries, ok := resp["entries"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), entries, 1)
	first, ok := entries[0].(map[string]interface{})
	require.True(s.T(), ok)
	assert.Equal(s.T(), "testdb", first["database"])
}

// TestFunctional_Scenario122b_C4_PostScanDisabled covers 122b-C4-disabled-api-F:
// POST …/storage/recommendations/scan with the scan disabled → 400
// RECOMMENDATION_SCAN_NOT_ENABLED through the REAL api.Server router.
func (s *Scenario122Suite) TestFunctional_Scenario122b_C4_PostScanDisabled() {
	cluster := scenario122UsageCluster(false) // scan absent → disabled.
	s.scenario122BootAPI(cluster, &scenario122Factory{client: &scenario122MockDBClient{}})

	// The POST scan mutating endpoint requires PermissionOperator.
	rec := s.scenario122Do(scenario122OperUser, scenario122OperPass, http.MethodPost,
		scenario122URL("/storage/recommendations/scan"))
	require.Equal(s.T(), http.StatusBadRequest, rec.Code,
		"122b-C4-disabled-api-F: a disabled scan POST must be 400")
	assert.Contains(s.T(), rec.Body.String(), cases.Scenario122ScanNotEnabledCode)
}

// scenario122UsageEntries returns a small canned usage report for the re-enable
// assertion.
func scenario122UsageEntries() []db.UsageReportEntry {
	return []db.UsageReportEntry{
		{Month: "2026-06", Database: "testdb", SizeBytes: 1073741824, SizeHuman: "1 GB", Connections: 4},
	}
}

// ----------------------------------------------------------------------------
// Catalog-honest cross-check
// ----------------------------------------------------------------------------

// TestFunctional_Scenario122_CatalogCoversFunctionalRows asserts every functional
// (-F) catalog row is honest: a known Req family and a non-empty
// Gate/Assert/Description — so the matrix cannot silently drop a rule.
func (s *Scenario122Suite) TestFunctional_Scenario122_CatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"C.2": true, "C.4": true, "C.12": true, "CONTROL": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario122Cases() {
		if c.Layer != cases.Scenario122LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Assert, "%s must carry an Assert token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req],
			"functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario122 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Assert)
	}
	for _, req := range []string{"C.2", "C.4", "C.12", "CONTROL"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover family %s", req)
	}
}

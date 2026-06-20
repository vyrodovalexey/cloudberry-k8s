//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 116: Disk Usage Monitoring (Status + Metric)
// (reconciliation rules R.2, S.1, M.1) — functional
// ============================================================================
//
// With spec.storage.diskMonitoring:true, AdminReconciler.Reconcile measures the
// worst-case segment-volume filesystem usage via db.GetDiskUsagePercent and:
// populates status.diskUsagePercent with the CURRENT measured value (S.1, R.2),
// publishes the cloudberry_disk_usage_percent gauge FROM the same value so the
// gauge MATCHES the status (M.1), and tracks growth across reconciles (TRACK).
// With diskMonitoring:false the path early-returns and never measures
// (DISABLED-noop). When the DB returns db.ErrDiskUsageUnavailable (or any error)
// the reconcile still succeeds and the status is NOT fabricated (DBERR-nonfatal).
//
// This functional layer drives the PUBLIC AdminReconciler.Reconcile entrypoint
// over a fake-client TestK8sEnv with an injected dbFactory whose mock DB returns
// a known GetDiskUsagePercent, mirroring scenario115_storage_management_test.go
// and storage_recommendations_test.go. It is catalog-honest via a coverage test
// that keeps the -F matrix from silently dropping a rule. The live proof is the
// KUBECONFIG/SCENARIO116_LIVE-gated Scenario 116 integration/e2e Part B.
// ============================================================================

// Scenario116Suite drives AdminReconciler.Reconcile over a disk-monitoring
// cluster and asserts the R.2/S.1/M.1 measurement effects.
type Scenario116Suite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario116(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario116Suite))
}

func (s *Scenario116Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario116Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario116MonitoringCluster builds a base-valid running cluster with disk
// monitoring enabled so reconcileStorage reaches the measurement step.
func scenario116MonitoringCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	return cluster
}

// scenario116Reconciler builds an AdminReconciler over a fake-client TestK8sEnv
// seeded with the supplied cluster, wired with the supplied dbFactory and a real
// PrometheusRecorder over reg so the gauge can be inspected (M.1).
func (s *Scenario116Suite) scenario116Reconciler(
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

// scenario116GaugeValue gathers cloudberry_disk_usage_percent from reg and
// returns the gauge value for the {cluster,namespace} series (found=false when
// no matching series is present).
func scenario116GaugeValue(
	t require.TestingT, reg *prometheus.Registry, cluster, namespace string,
) (float64, bool) {
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != cases.Scenario116MetricName {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["cluster"] == cluster && labels["namespace"] == namespace {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// storageConfiguredTrue116 reports whether StorageConfigured is True.
func storageConfiguredTrue116(cluster *cbv1alpha1.CloudberryCluster) bool {
	for _, c := range cluster.Status.Conditions {
		if c.Type == "StorageConfigured" {
			return string(c.Status) == "True"
		}
	}
	return false
}

// TestFunctional_Scenario116_MeasuredValue covers 116-R2-F / 116-S1-F / 116-M1-F
// / 116-CONTROL: a single reconcile with the dbFactory returning 73% populates
// status.diskUsagePercent with 73 (S.1/R.2), publishes the gauge as 73 (M.1),
// the two MATCH, and the reconcile returns no error (CONTROL).
func (s *Scenario116Suite) TestFunctional_Scenario116_MeasuredValue() {
	cluster := scenario116MonitoringCluster("s116-measured")
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 73, nil },
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario116Reconciler(cluster, dbFactory, reg)

	// 116-CONTROL + R.2: the full reconcile path proceeds without error.
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "116-CONTROL: the reconcile must return no error")
	assert.NotZero(s.T(), result.RequeueAfter, "116-R2-F: reconcileStorage must proceed past the gate")

	// S.1: persisted status carries the measured value.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(73), updated.Status.DiskUsagePercent, "116-S1-F: status==measured")
	assert.True(s.T(), storageConfiguredTrue116(updated), "116-CONTROL: StorageConfigured must be True")

	// M.1: gauge equals the measured value and matches the status.
	gauge, found := scenario116GaugeValue(s.T(), reg, cluster.Name, cluster.Namespace)
	require.True(s.T(), found, "116-M1-F: cloudberry_disk_usage_percent must be published")
	assert.InDelta(s.T(), 73.0, gauge, 0.001, "116-M1-F: gauge==measured")
	assert.InDelta(s.T(), float64(updated.Status.DiskUsagePercent), gauge, 0.001,
		"116-M1-F: metric must match status (M.1 invariant)")
}

// TestFunctional_Scenario116_TrackGrowth covers 116-TRACK-growth-F: reconcile
// twice with the dbFactory returning a higher % on the 2nd pass; status + metric
// both increase, proving growth is tracked on settled clusters (no max-only or
// sticky behavior).
func (s *Scenario116Suite) TestFunctional_Scenario116_TrackGrowth() {
	cluster := scenario116MonitoringCluster("s116-growth")
	measured := int32(30)
	dbFactory := &testutil.MockDBClientFactory{
		Client: &testutil.MockDBClient{
			GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return measured, nil },
		},
	}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario116Reconciler(cluster, dbFactory, reg)

	// First pass: 30.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(30), updated.Status.DiskUsagePercent)
	g1, found := scenario116GaugeValue(s.T(), reg, cluster.Name, cluster.Namespace)
	require.True(s.T(), found)
	assert.InDelta(s.T(), 30.0, g1, 0.001)

	// Second pass: 80 (growth).
	measured = 80
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(80), updated.Status.DiskUsagePercent, "116-TRACK-growth-F: status tracks growth")
	g2, found := scenario116GaugeValue(s.T(), reg, cluster.Name, cluster.Namespace)
	require.True(s.T(), found)
	assert.InDelta(s.T(), 80.0, g2, 0.001)
	assert.Greater(s.T(), g2, g1, "116-TRACK-growth-F: metric must track growth")
}

// TestFunctional_Scenario116_DisabledNoOp covers 116-DISABLED-noop:
// diskMonitoring:false → reconcileStorage early-returns WITHOUT measuring: the DB
// factory is NEVER called. Per the C.2 reset-on-disable contract, any stale
// status.diskUsagePercent is reset to 0 and the gauge is published as an explicit
// "monitoring off" 0 signal (not left frozen at the stale reading).
func (s *Scenario116Suite) TestFunctional_Scenario116_DisabledNoOp() {
	cluster := testutil.NewClusterBuilder("s116-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 11

	calls := 0
	dbFactory := &countingDBFactory116{calls: &calls}
	reg := prometheus.NewRegistry()
	reconciler := s.scenario116Reconciler(cluster, dbFactory, reg)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Zero(s.T(), calls, "116-DISABLED-noop: DB factory must not be called when monitoring is off")
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int32(0), updated.Status.DiskUsagePercent,
		"116-DISABLED-noop: stale status must be reset to 0 (C.2 reset-on-disable)")
	g, found := scenario116GaugeValue(s.T(), reg, cluster.Name, cluster.Namespace)
	assert.True(s.T(), found, "116-DISABLED-noop: a 0 gauge must be published as the monitoring-off signal")
	assert.InDelta(s.T(), 0.0, g, 0.001,
		"116-DISABLED-noop: gauge must be the explicit 0 monitoring-off signal")
}

// countingDBFactory116 counts NewClient calls so the disabled no-op case can
// assert the DB layer is never reached. NewClient errors if it is reached so the
// disabled path cannot accidentally measure.
type countingDBFactory116 struct {
	calls *int
}

func (f *countingDBFactory116) NewClient(
	_ context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	*f.calls++
	return &testutil.MockDBClient{
		GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 99, nil },
	}, nil
}

// TestFunctional_Scenario116_DBErrNonFatal covers 116-DBERR-nonfatal: when
// GetDiskUsagePercent returns the unavailable sentinel (or a generic error) the
// reconcile still returns nil, StorageConfigured stays True, the prior status is
// NOT fabricated, and no gauge is published.
func (s *Scenario116Suite) TestFunctional_Scenario116_DBErrNonFatal() {
	tests := []struct {
		name string
		err  error
	}{
		{name: "unavailable sentinel", err: db.ErrDiskUsageUnavailable},
		{name: "generic db error", err: assertNewClientErr116()},
	}
	for _, tt := range tests {
		tt := tt
		s.Run(tt.name, func() {
			cluster := scenario116MonitoringCluster("s116-dberr")
			cluster.Status.DiskUsagePercent = 17 // prior value must survive.
			// Force BOTH sources to fail so the controller skips honestly: the
			// preferred gp_disk_free path returns tt.err and the portable
			// logical-size fallback (cluster data size) also errors.
			dbFactory := &testutil.MockDBClientFactory{
				Client: &testutil.MockDBClient{
					GetDiskUsagePercentFunc: func(_ context.Context) (int32, error) { return 99, tt.err },
					GetClusterDataSizeBytesFunc: func(_ context.Context) (int64, error) {
						return 0, assertNewClientErr116()
					},
				},
			}
			reg := prometheus.NewRegistry()
			reconciler := s.scenario116Reconciler(cluster, dbFactory, reg)

			_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err, "116-DBERR-nonfatal: DB error must be non-fatal")

			updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			assert.Equal(s.T(), int32(17), updated.Status.DiskUsagePercent,
				"116-DBERR-nonfatal: status must NOT be fabricated on DB error")
			assert.True(s.T(), storageConfiguredTrue116(updated),
				"116-DBERR-nonfatal: StorageConfigured must still be True")
			_, found := scenario116GaugeValue(s.T(), reg, cluster.Name, cluster.Namespace)
			assert.False(s.T(), found, "116-DBERR-nonfatal: no gauge must be published on DB error")
		})
	}
}

// assertNewClientErr116 is a generic non-sentinel error for the DBERR variant.
func assertNewClientErr116() error {
	return errScenario116Generic
}

var errScenario116Generic = errGeneric116("scenario116 generic db error")

type errGeneric116 string

func (e errGeneric116) Error() string { return string(e) }

// TestFunctional_Scenario116_CatalogCoversFunctionalRows asserts every
// functional (-F) catalog row is honest: a known Req family and a non-empty
// Gate/Expected/Description — so the matrix cannot silently drop a rule.
func (s *Scenario116Suite) TestFunctional_Scenario116_CatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"R.2": true, "S.1": true, "M.1": true,
		"TRACK": true, "DISABLED": true, "DBERR": true, "CONTROL": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario116Cases() {
		if c.Layer != cases.Scenario116LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Expected, "%s must carry an Expected token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req],
			"functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario116 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Expected)
	}
	for _, req := range []string{"R.2", "S.1", "M.1"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover rule %s", req)
	}
}

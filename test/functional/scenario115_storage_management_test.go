//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 115: Enable Storage Management with Full Configuration
// (reconciliation rules R.1, C.1, C.3, C.5, R.5) — functional
// ============================================================================
//
// Applying a FULL spec.storage block (diskMonitoring:true; recommendationScan
// enabled with schedule "0 3 * * 0" and all five thresholds; usageReport
// enabled+monthly) drives AdminReconciler.reconcileStorage() to: proceed past
// the diskMonitoring gate (R.1); accept the scan config (C.1) + thresholds
// (C.3); create a CronJob "<cluster>-recommendation-scan" for the schedule
// (C.5); and set the StorageConfigured condition True (R.5). The full path
// returns no error (CONTROL), and usageReport is accepted without a CronJob
// side-effect (USAGE-accept). The disabled gate (diskMonitoring:false) is the
// early-return no-op (DISABLED-noop): no CronJob, no condition.
//
// This functional layer drives the PUBLIC AdminReconciler.Reconcile entrypoint
// (the same path the controller-runtime manager uses) over a fake-client
// TestK8sEnv, mirroring storage_recommendations_test.go. It is catalog-driven by
// cases.Scenario115Cases() (the -F rows), with a catalog-coverage honesty test
// that keeps the -F matrix from silently dropping a rule. The live persisted
// proof is the KUBECONFIG/SCENARIO115_LIVE-gated Scenario 115 integration/e2e
// Part B.
// ============================================================================

// Scenario115Suite drives AdminReconciler.Reconcile over a full-storage cluster
// and asserts the R.1/C.1/C.3/C.5/R.5 reconciliation effects.
type Scenario115Suite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestFunctional_Scenario115(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario115Suite))
}

func (s *Scenario115Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario115Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario115FullStorage returns the FULL Scenario 115 storage block:
// diskMonitoring on; recommendationScan enabled with schedule "0 3 * * 0" and
// all five thresholds; usageReport enabled + monthly.
func scenario115FullStorage() *cbv1alpha1.StorageManagementSpec {
	return &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       50,
			AgeThreshold:        500000000,
			IndexBloatThreshold: 30,
			ScanDuration:        "2h",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true, Monthly: true},
	}
}

// scenario115Reconciler builds an AdminReconciler over a fake-client TestK8sEnv
// seeded with the supplied cluster (mirrors storage_recommendations_test.go).
func (s *Scenario115Suite) scenario115Reconciler(cluster *cbv1alpha1.CloudberryCluster) *controller.AdminReconciler {
	s.env = testutil.NewTestK8sEnv(cluster)
	return controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, s.env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, s.env.Logger,
	)
}

// scenario115FullCluster builds a base-valid, running cluster carrying the full
// storage block.
func scenario115FullCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Storage = scenario115FullStorage()
	return cluster
}

// storageConfiguredTrue reports whether the StorageConfigured condition is set
// True on the cluster status.
func storageConfiguredTrue(cluster *cbv1alpha1.CloudberryCluster) bool {
	for _, c := range cluster.Status.Conditions {
		if c.Type == "StorageConfigured" {
			return string(c.Status) == "True"
		}
	}
	return false
}

// TestFunctional_Scenario115_FullBlock_Reconciles is the core full-block proof
// covering 115-R1-F (gate proceeds), 115-C1-F/C3-F (scan + thresholds accepted),
// 115-C5-F (CronJob created), 115-R5-F (StorageConfigured=True), USAGE-accept,
// and CONTROL-noerror — a single reconcile pass over the full storage block.
func (s *Scenario115Suite) TestFunctional_Scenario115_FullBlock_Reconciles() {
	cluster := scenario115FullCluster("s115-full")
	reconciler := s.scenario115Reconciler(cluster)

	// CONTROL-noerror + R.1: the full reconcile path proceeds without error.
	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "115-CONTROL-noerror: the full reconcile must return no error")
	assert.NotZero(s.T(), result.RequeueAfter, "115-R1-F: reconcileStorage must proceed past the gate")

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// C.1: scan config accepted/persisted (not rewritten).
	require.NotNil(s.T(), updated.Spec.Storage)
	scan := updated.Spec.Storage.RecommendationScan
	require.NotNil(s.T(), scan, "115-C1-F: recommendationScan must be accepted")
	assert.True(s.T(), scan.Enabled, "115-C1-F: scan stays enabled")
	assert.Equal(s.T(), "0 3 * * 0", scan.Schedule, "115-C1-F: schedule accepted")

	// C.3: the five thresholds survive a reconcile pass unmutated.
	assert.Equal(s.T(), int32(20), scan.BloatThreshold, "115-C3-F: bloatThreshold")
	assert.Equal(s.T(), int32(50), scan.SkewThreshold, "115-C3-F: skewThreshold")
	assert.Equal(s.T(), int64(500000000), scan.AgeThreshold, "115-C3-F: ageThreshold")
	assert.Equal(s.T(), int32(30), scan.IndexBloatThreshold, "115-C3-F: indexBloatThreshold")
	assert.Equal(s.T(), "2h", scan.ScanDuration, "115-C3-F: scanDuration")

	// USAGE-accept: usageReport accepted + persisted.
	require.NotNil(s.T(), updated.Spec.Storage.UsageReport, "115-USAGE-accept: usageReport accepted")
	assert.True(s.T(), updated.Spec.Storage.UsageReport.Enabled, "115-USAGE-accept: enabled")
	assert.True(s.T(), updated.Spec.Storage.UsageReport.Monthly, "115-USAGE-accept: monthly")

	// R.5: StorageConfigured condition True.
	assert.True(s.T(), storageConfiguredTrue(updated), "115-R5-F: StorageConfigured must be True")

	// C.5: exactly one recommendation-scan CronJob exists with the schedule and
	// the cluster owner reference.
	s.assertScanCronJob(cluster)
}

// assertScanCronJob lists CronJobs in the cluster namespace and asserts exactly
// one named util.RecommendationScanCronJobName(cluster) exists with the
// "0 3 * * 0" schedule and the cluster owner reference (115-C5-F).
func (s *Scenario115Suite) assertScanCronJob(cluster *cbv1alpha1.CloudberryCluster) {
	list := &batchv1.CronJobList{}
	require.NoError(s.T(), s.env.Client.List(s.ctx, list, client.InNamespace(cluster.Namespace)))

	wantName := util.RecommendationScanCronJobName(cluster.Name)
	matches := 0
	for i := range list.Items {
		cj := &list.Items[i]
		if cj.Name != wantName {
			continue
		}
		matches++
		assert.Equal(s.T(), "0 3 * * 0", cj.Spec.Schedule, "115-C5-F: CronJob schedule")
		require.Len(s.T(), cj.OwnerReferences, 1, "115-C5-F: CronJob must be owner-referenced")
		assert.Equal(s.T(), cluster.Name, cj.OwnerReferences[0].Name, "115-C5-F: owner is the cluster")
	}
	assert.Equal(s.T(), 1, matches,
		"115-C5-F: exactly one recommendation-scan CronJob must exist")
}

// TestFunctional_Scenario115_DisabledNoOp is 115-DISABLED-noop: diskMonitoring
// false → reconcileStorage returns early: NO recommendation-scan CronJob and NO
// StorageConfigured condition.
func (s *Scenario115Suite) TestFunctional_Scenario115_DisabledNoOp() {
	cluster := testutil.NewClusterBuilder("s115-disabled", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	reconciler := s.scenario115Reconciler(cluster)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	assert.NotZero(s.T(), result.RequeueAfter)

	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// No StorageConfigured condition.
	for _, c := range updated.Status.Conditions {
		assert.NotEqual(s.T(), "StorageConfigured", c.Type,
			"115-DISABLED-noop: StorageConfigured must not be set when disabled")
	}

	// No recommendation-scan CronJob.
	cj := &batchv1.CronJob{}
	getErr := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj)
	assert.Error(s.T(), getErr,
		"115-DISABLED-noop: no recommendation-scan CronJob must be created when disabled")
}

// TestFunctional_Scenario115_CatalogCoversFunctionalRows asserts every
// functional (-F) catalog row is honest: a known Req family and a non-empty
// Gate/Expected/Description — so the matrix cannot silently drop a rule.
func (s *Scenario115Suite) TestFunctional_Scenario115_CatalogCoversFunctionalRows() {
	knownReqs := map[string]bool{
		"R.1": true, "C.1": true, "C.3": true, "C.5": true, "R.5": true,
		"CONTROL": true, "DISABLED": true, "USAGE": true,
	}
	seen := map[string]bool{}
	for _, c := range cases.Scenario115Cases() {
		if c.Layer != cases.Scenario115LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		assert.NotEmptyf(s.T(), c.Expected, "%s must carry an Expected token", c.ID)
		assert.NotEmptyf(s.T(), c.Description, "%s must carry a Description", c.ID)
		assert.Truef(s.T(), knownReqs[c.Req],
			"functional row %s must be a known family; got %q", c.ID, c.Req)
		seen[c.Req] = true
		s.T().Logf("scenario115 %s (%s, gate=%s): %s", c.ID, c.Req, c.Gate, c.Expected)
	}
	for _, req := range []string{"R.1", "C.1", "C.3", "C.5", "R.5"} {
		assert.Truef(s.T(), seen[req], "functional catalog must cover rule %s", req)
	}
}

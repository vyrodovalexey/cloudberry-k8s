package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// scenario115Cluster returns a cluster carrying the FULL Scenario 115 storage
// block: diskMonitoring on; recommendationScan enabled with schedule "0 3 * * 0"
// and all five thresholds; usageReport enabled + monthly.
func scenario115Cluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
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
	return c
}

// storageConfiguredCondition returns the StorageConfigured condition (or nil).
func storageConfiguredCondition(c *cbv1alpha1.CloudberryCluster) *struct {
	Status string
	Reason string
} {
	for _, cond := range c.Status.Conditions {
		if cond.Type == "StorageConfigured" {
			return &struct {
				Status string
				Reason string
			}{Status: string(cond.Status), Reason: cond.Reason}
		}
	}
	return nil
}

// TestScenario115_R1_DiskMonitoringProceeds (115-R1) verifies that with
// diskMonitoring:true reconcileStorage proceeds past the gate and returns no
// error (downstream effects observed: the StorageConfigured condition is set).
func TestScenario115_R1_DiskMonitoringProceeds(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)
	// Proceeded past the gate => condition set.
	require.NotNil(t, storageConfiguredCondition(cluster))
}

// TestScenario115_Disabled_Noop (115-DISABLED-noop) verifies that with
// diskMonitoring:false reconcileStorage returns early: NO CronJob is created and
// NO StorageConfigured condition is added.
func TestScenario115_Disabled_Noop(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	// No StorageConfigured condition (early return before the condition set).
	assert.Nil(t, storageConfiguredCondition(cluster),
		"disabled storage must not set the StorageConfigured condition")

	// No CronJob created.
	cj := &batchv1.CronJob{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj)
	assert.True(t, apierrors.IsNotFound(getErr),
		"disabled storage must not create the recommendation-scan CronJob")
}

// TestScenario115_C1C3_FullScanAccepted (115-C1/C3) verifies the full
// recommendationScan config (schedule + five thresholds) is parsed/accepted by
// reconcileStorage without error and without controller-side mutation.
func TestScenario115_C1C3_FullScanAccepted(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	// C.1/C.3: config not mutated by the controller.
	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(t, scan)
	assert.True(t, scan.Enabled)
	assert.Equal(t, "0 3 * * 0", scan.Schedule)
	assert.Equal(t, int32(20), scan.BloatThreshold)
	assert.Equal(t, int32(50), scan.SkewThreshold)
	assert.Equal(t, int64(500000000), scan.AgeThreshold)
	assert.Equal(t, int32(30), scan.IndexBloatThreshold)
	assert.Equal(t, "2h", scan.ScanDuration)
}

// TestScenario115_C5_CronJobCreatedThenGCd (115-C5) verifies the CronJob is
// CREATED after reconcileStorage with the full scan, and REMOVED (GC) on a
// subsequent reconcile once the scan is disabled.
func TestScenario115_C5_CronJobCreatedThenGCd(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// First reconcile: CronJob is created.
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	cronName := types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}
	cj := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(), cronName, cj),
		"recommendation-scan CronJob must exist after the full reconcile")
	assert.Equal(t, "test-cluster-recommendation-scan", cj.Name)
	assert.Equal(t, "0 3 * * 0", cj.Spec.Schedule)

	// Disable the scan and reconcile again: the CronJob is GC'd (disk
	// monitoring stays on so reconcileStorage still runs the ensure/GC path).
	cluster.Spec.Storage.RecommendationScan.Enabled = false
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	getErr := k8sClient.Get(context.Background(), cronName, &batchv1.CronJob{})
	assert.True(t, apierrors.IsNotFound(getErr),
		"disabling the scan must GC the recommendation-scan CronJob")
}

// TestScenario115_C5_GCWhenStorageDisabled covers the GC path driven via the
// remove helper directly when storage management is turned off entirely.
func TestScenario115_C5_GCWhenStorageDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	// Seed a pre-existing CronJob so the remove path has something to delete.
	existing := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, existing)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Storage disabled: ensure drives the GC (builder returns nil => delete).
	cluster.Spec.Storage = nil
	require.NoError(t, r.ensureRecommendationScanCronJob(context.Background(), cluster))

	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, &batchv1.CronJob{})
	assert.True(t, apierrors.IsNotFound(getErr),
		"disabling storage must GC the recommendation-scan CronJob")
}

// TestScenario115_C5_RemoveTolerates absent CronJob is a no-op (the GC path
// tolerates NotFound for clusters that never had a scan).
func TestScenario115_C5_RemoveToleratesAbsent(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.removeRecommendationScanCronJob(context.Background(), cluster))
}

// TestScenario115_C5_EnsureUpdatesOnDrift verifies the ensure helper updates an
// existing CronJob in place when its spec drifts from the desired shape.
func TestScenario115_C5_EnsureUpdatesOnDrift(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	// Seed a CronJob with a stale schedule so ensure must update it.
	stale := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, stale)
	stale.Spec.Schedule = "0 0 * * *"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, stale).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensureRecommendationScanCronJob(context.Background(), cluster))

	cj := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj))
	assert.Equal(t, "0 3 * * 0", cj.Spec.Schedule, "ensure must reconcile the drifted schedule")
}

// TestScenario115_R5_StorageConfiguredTrue (115-R5) verifies the
// StorageConfigured condition is True with reason StorageReconciled.
func TestScenario115_R5_StorageConfiguredTrue(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	cond := storageConfiguredCondition(cluster)
	require.NotNil(t, cond, "StorageConfigured condition must be set")
	assert.Equal(t, "True", cond.Status)
	assert.Equal(t, "StorageReconciled", cond.Reason)
}

// TestScenario115_Usage_Accept (115-USAGE-accept) verifies usageReport
// enabled+monthly is parsed without error and not mutated.
func TestScenario115_Usage_Accept(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		UsageReport:    &cbv1alpha1.UsageReportSpec{Enabled: true, Monthly: true},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	require.NotNil(t, cluster.Spec.Storage.UsageReport)
	assert.True(t, cluster.Spec.Storage.UsageReport.Enabled)
	assert.True(t, cluster.Spec.Storage.UsageReport.Monthly)
}

// TestScenario115_Control_NoError (115-CONTROL-noerror) verifies the FULL
// reconcile path (R.1->C.1->C.3->C.5->R.5) returns no error.
func TestScenario115_Control_NoError(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	assert.NoError(t, r.reconcileStorage(context.Background(), cluster))
}

// TestScenario115_C5_EnsureGetError verifies a non-NotFound Get failure
// surfaces as a reconcile error (the no-false-positive control).
func TestScenario115_C5_EnsureGetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				if key.Name == util.RecommendationScanCronJobName(cluster.Name) {
					return errors.New("simulated get failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureRecommendationScanCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting recommendation-scan cronjob")
}

// TestScenario115_C5_EnsureCreateError verifies a Create failure on the
// NotFound path surfaces as a reconcile error.
func TestScenario115_C5_EnsureCreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.CreateOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return errors.New("simulated create failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureRecommendationScanCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating recommendation-scan cronjob")
}

// TestScenario115_C5_EnsureUpdateError verifies an Update failure on the
// spec-drift path surfaces as a reconcile error.
func TestScenario115_C5_EnsureUpdateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	stale := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, stale)
	stale.Spec.Schedule = "0 0 * * *"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, stale).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.UpdateOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return errors.New("simulated update failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensureRecommendationScanCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "updating recommendation-scan cronjob")
}

// TestScenario115_C5_RemoveGetError verifies a non-NotFound Get failure on the
// GC path surfaces as an error.
func TestScenario115_C5_RemoveGetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				if key.Name == util.RecommendationScanCronJobName(cluster.Name) {
					return errors.New("simulated get failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.removeRecommendationScanCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting recommendation-scan cronjob")
}

// TestScenario115_C5_RemoveDeleteError verifies a Delete failure on the GC path
// surfaces as an error.
func TestScenario115_C5_RemoveDeleteError(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115Cluster()

	existing := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, existing)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return errors.New("simulated delete failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.removeRecommendationScanCronJob(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleting recommendation-scan cronjob")
}

// scanCronJobGauge gathers cloudberry_recommendation_scan_cronjob from the
// registry and returns the gauge value for {cluster, namespace} (0 when absent).
func scanCronJobGauge(t *testing.T, reg *prometheus.Registry, cluster, namespace string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "cloudberry_recommendation_scan_cronjob" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["cluster"] == cluster && labels["namespace"] == namespace {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// TestScenario115_C5_MetricSetWhenEnsured (115 metric) drives reconcileStorage
// against the REAL PrometheusRecorder and asserts
// cloudberry_recommendation_scan_cronjob is 1 once the CronJob is ensured, then
// 0 after the scan is disabled and the CronJob is GC'd.
func TestScenario115_C5_MetricSetWhenEnsured(t *testing.T) {
	scheme := newTestScheme()
	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	cluster := scenario115Cluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, rec, nil)

	// Ensured => gauge 1.
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, 1.0, scanCronJobGauge(t, reg, "test-cluster", "default"),
		"recommendation_scan_cronjob must be 1 when ensured")

	// Disabled => GC => gauge 0.
	cluster.Spec.Storage.RecommendationScan.Enabled = false
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, 0.0, scanCronJobGauge(t, reg, "test-cluster", "default"),
		"recommendation_scan_cronjob must be 0 when removed")
}

// scenario115SteadyStateCluster returns a Scenario 115 cluster that is already
// at steady state: Running phase, ObservedGeneration == Generation, and the
// exporter role ready (QueryMonitoring unset => isExporterRoleReady is true).
// This is the precondition under which handleAdminEarlyReturns short-circuits
// the spec-driven reconcileStorage and instead drives refreshStorageOnSteadyState.
func scenario115SteadyStateCluster() *cbv1alpha1.CloudberryCluster {
	c := scenario115Cluster()
	c.Generation = 5
	c.Status.ObservedGeneration = 5
	return c
}

// TestScenario115_SteadyState_C5R5_Converges (115-STEADY-C5R5) verifies that on
// the steady-state path (generation gate engaged) refreshStorageOnSteadyState
// CONVERGES both C.5 (the recommendation-scan CronJob is created) and R.5 (the
// StorageConfigured condition is True with reason StorageReconciled), even though
// the spec-driven reconcileStorage is short-circuited.
func TestScenario115_SteadyState_C5R5_Converges(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Drive the steady-state (generation-gated) path directly.
	r.refreshStorageOnSteadyState(context.Background(), cluster)

	// C.5: the CronJob must exist in the fake client.
	cj := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj), "recommendation-scan CronJob must be created on the steady-state path")
	assert.Equal(t, "test-cluster-recommendation-scan", cj.Name)
	assert.Equal(t, "0 3 * * 0", cj.Spec.Schedule)

	// R.5: the StorageConfigured condition must be True (reason StorageReconciled).
	cond := storageConfiguredCondition(cluster)
	require.NotNil(t, cond, "StorageConfigured condition must be set on the steady-state path")
	assert.Equal(t, "True", cond.Status)
	assert.Equal(t, "StorageReconciled", cond.Reason)
}

// TestScenario115_SteadyState_DisabledGC (115-STEADY-disabled-GC) verifies that
// when storage management is disabled (DiskMonitoring false) on the steady-state
// path, a pre-existing recommendation-scan CronJob is GC'd by
// refreshStorageOnSteadyState (the enabled->disabled convergence the spec-driven
// reconcile can no longer perform once the generation has settled).
func TestScenario115_SteadyState_DisabledGC(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	// Seed a pre-existing CronJob so the GC path has something to delete.
	existing := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, existing)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Disk monitoring off => steady-state GC path.
	cluster.Spec.Storage.DiskMonitoring = false
	r.refreshStorageOnSteadyState(context.Background(), cluster)

	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, &batchv1.CronJob{})
	assert.True(t, apierrors.IsNotFound(getErr),
		"disabled disk monitoring must GC the recommendation-scan CronJob on the steady-state path")

	// The GC branch returns before setting the condition.
	assert.Nil(t, storageConfiguredCondition(cluster),
		"GC branch must not set the StorageConfigured condition")
}

// TestScenario115_SteadyState_StorageNilNoop (115-STEADY-noop) verifies that with
// Spec.Storage nil refreshStorageOnSteadyState is a safe no-op: no panic, no
// CronJob created, and no StorageConfigured condition set.
func TestScenario115_SteadyState_StorageNilNoop(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()
	cluster.Spec.Storage = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		r.refreshStorageOnSteadyState(context.Background(), cluster)
	})

	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, &batchv1.CronJob{})
	assert.True(t, apierrors.IsNotFound(getErr),
		"nil storage must not create the recommendation-scan CronJob")
	assert.Nil(t, storageConfiguredCondition(cluster),
		"nil storage must not set the StorageConfigured condition")
}

// TestScenario115_SteadyState_WiringViaReconcile (115-STEADY-wiring) proves the
// WIRING: a cluster at steady state (Running, ObservedGeneration == Generation,
// exporter role ready) reconciled through the PUBLIC Reconcile entry point takes
// the generation-gated early-return path, which calls refreshStorageOnSteadyState
// and converges the recommendation-scan CronJob into existence.
func TestScenario115_SteadyState_WiringViaReconcile(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	require.NoError(t, err)
	// The generation gate requeues on the default interval (steady-state path).
	assert.Positive(t, res.RequeueAfter, "steady-state path must requeue")

	// The wiring created the CronJob via refreshStorageOnSteadyState.
	cj := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj), "reconcile at steady state must converge the recommendation-scan CronJob")
	assert.Equal(t, "0 3 * * 0", cj.Spec.Schedule)

	// R.5: the persisted condition must be True (reason StorageReconciled).
	persisted := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, persisted))
	cond := storageConfiguredCondition(persisted)
	require.NotNil(t, cond, "StorageConfigured condition must be persisted via the wiring")
	assert.Equal(t, "True", cond.Status)
	assert.Equal(t, "StorageReconciled", cond.Reason)
}

// TestScenario115_SteadyState_MetricSet (115-STEADY-metric) drives
// refreshStorageOnSteadyState against the REAL PrometheusRecorder and asserts the
// cloudberry_recommendation_scan_cronjob gauge is 1 after the enabled
// steady-state path ensures the CronJob.
func TestScenario115_SteadyState_MetricSet(t *testing.T) {
	scheme := newTestScheme()
	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	cluster := scenario115SteadyStateCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, rec, nil)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.Equal(t, 1.0, scanCronJobGauge(t, reg, "test-cluster", "default"),
		"recommendation_scan_cronjob must be 1 after the enabled steady-state path")
}

// TestScenario115_SteadyState_EnsureErrorNonFatal verifies the steady-state path
// is non-fatal: an ensure failure is swallowed (logged), so no panic occurs and
// the StorageConfigured condition is NOT set (the function returns early after
// the ensure error, before the condition/persist).
func TestScenario115_SteadyState_EnsureErrorNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, key client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				if key.Name == util.RecommendationScanCronJobName(cluster.Name) {
					return errors.New("simulated get failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		r.refreshStorageOnSteadyState(context.Background(), cluster)
	})
	assert.Nil(t, storageConfiguredCondition(cluster),
		"an ensure failure must short-circuit before the StorageConfigured condition is set")
}

// TestScenario115_SteadyState_GCErrorNonFatal verifies the disabled/GC branch is
// non-fatal: a delete failure during GC is swallowed (logged) without panic.
func TestScenario115_SteadyState_GCErrorNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	existing := builder.NewBuilder().BuildRecommendationScanCronJob(cluster)
	require.NotNil(t, existing)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existing).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, obj client.Object,
				_ ...client.DeleteOption) error {
				if _, ok := obj.(*batchv1.CronJob); ok {
					return errors.New("simulated delete failure")
				}
				return nil
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	cluster.Spec.Storage.DiskMonitoring = false
	require.NotPanics(t, func() {
		r.refreshStorageOnSteadyState(context.Background(), cluster)
	})
}

// TestScenario115_SteadyState_PatchErrorNonFatal verifies the final persist is
// non-fatal: a status patch failure is swallowed (logged) without panic, while
// the in-memory condition is still set and the CronJob is still ensured.
func TestScenario115_SteadyState_PatchErrorNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := scenario115SteadyStateCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
				_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
				return errors.New("simulated status patch failure")
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(10), builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		r.refreshStorageOnSteadyState(context.Background(), cluster)
	})

	// The CronJob is still ensured and the in-memory condition still set even
	// though the persist failed (non-fatal).
	cj := &batchv1.CronJob{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.RecommendationScanCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cj))
	cond := storageConfiguredCondition(cluster)
	require.NotNil(t, cond)
	assert.Equal(t, "True", cond.Status)
}

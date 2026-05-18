package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func TestNewAdminReconciler(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)
	require.NotNil(t, r)
}

func TestAdminReconciler_Reconcile_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestAdminReconciler_Reconcile_NotRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_Running_NoConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_WithConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{
			"max_connections": "200",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_MaintenanceAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify annotation was removed
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	_, exists := updated.Annotations[util.AnnotationMaintenance]
	assert.False(t, exists)
}

func TestAdminReconciler_ReconcileConfig_NoChange(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// First reconcile sets the hash
	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)

	// Second reconcile with same config should be a no-op
	err = r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileConfig_NilConfig(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Config = nil

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileBackup_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileBackup(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileBackup_Enabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Destination: cbv1alpha1.BackupDestination{
			Type:   "s3",
			Bucket: "my-bucket",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileBackup(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "BackupConfigured" {
			found = true
			assert.Equal(t, "True", string(c.Status))
			break
		}
	}
	assert.True(t, found, "BackupConfigured condition should be set")
}

func TestAdminReconciler_ReconcileDataLoading_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileDataLoading_Enabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "job1", Type: "s3", Enabled: true, TargetTable: "public.data"},
			{Name: "job2", Type: "kafka", Enabled: false, TargetTable: "public.stream"},
			{Name: "job3", Type: "rabbitmq", Enabled: true, TargetTable: "public.queue"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.NoError(t, err)

	assert.Equal(t, int32(2), cluster.Status.DataLoadingJobs)
}

func TestAdminReconciler_ReconcileWorkload_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileWorkload_Enabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "etl", Concurrency: 5, CPUMaxPercent: 30},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{Name: "cancel-long", Action: "cancel", ThresholdType: "running_time", Threshold: "3600"},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "idle-analytics", ResourceGroup: "analytics", IdleTimeout: "30m"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "True", string(c.Status))
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileQueryMonitoring_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileQueryMonitoring(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileQueryMonitoring_Enabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "30d",
		SamplingInterval:   5,
		SlowQueryThreshold: "1000ms",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileQueryMonitoring(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_Reconcile_WithAllFeatures(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "default", Concurrency: 20},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:          true,
		HistoryRetention: "30d",
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type:   "s3",
			Bucket: "backups",
		},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader", Type: "s3", Enabled: true, TargetTable: "public.data"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ReconcileStorage_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileStorage_DiskMonitoringEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "StorageConfigured" {
			found = true
			assert.Equal(t, "True", string(c.Status))
			break
		}
	}
	assert.True(t, found, "StorageConfigured condition should be set")
}

func TestAdminReconciler_ReconcileStorage_WithRecommendationScan(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.RecommendationCount = 5
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:        true,
			Schedule:       "0 3 * * 0",
			BloatThreshold: 20,
			SkewThreshold:  50,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	assert.Equal(t, int32(5), cluster.Status.RecommendationCount)
}

func TestAdminReconciler_Reconcile_WithAllFeaturesAndStorage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "default", Concurrency: 20},
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{Name: "cancel-long", Action: "cancel", ThresholdType: "running_time", Threshold: "3600"},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: "idle-30m", ResourceGroup: "analytics", IdleTimeout: "30m"},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "90d",
		SamplingInterval:   5,
		PlanCollection:     true,
		SlowQueryThreshold: "500ms",
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3", Bucket: "backups",
		},
		Incremental: true,
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader1", Type: "s3", Enabled: true, TargetTable: "public.data"},
			{Name: "loader2", Type: "kafka", Enabled: true, TargetTable: "public.stream"},
			{Name: "loader3", Type: "rabbitmq", Enabled: false, TargetTable: "public.queue"},
		},
	}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:        true,
			Schedule:       "0 3 * * 0",
			BloatThreshold: 20,
			SkewThreshold:  50,
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true, Monthly: true},
	}
	cluster.Status.ActiveQueries = 10
	cluster.Status.QueuedQueries = 3
	cluster.Status.BlockedQueries = 1
	cluster.Status.DiskUsagePercent = 55
	cluster.Status.RecommendationCount = 7

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(30)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify data loading jobs count.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.Equal(t, int32(2), updated.Status.DataLoadingJobs)
}

func TestAdminReconciler_ReconcileStorage_WithUsageReport(t *testing.T) {
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
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_Reconcile_WithAllFeaturesEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "200"},
	}
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled:        true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{{Name: "default", Concurrency: 20}},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled: true, HistoryRetention: "30d",
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", Bucket: "b"},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs:    []cbv1alpha1.DataLoadingJob{{Name: "j", Type: "s3", Enabled: true, TargetTable: "t"}},
	}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true, Schedule: "0 3 * * 0",
		},
		UsageReport: &cbv1alpha1.UsageReportSpec{Enabled: true},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(30)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_WithConfigAndExistingConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	b := builder.NewBuilder()
	// Pre-create the postgresql.conf configmap.
	existingCM := b.BuildPostgresqlConfConfigMap(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ReconcileConfig_ConfigChange(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// First reconcile sets the hash.
	err := r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)

	// Change config.
	cluster.Spec.Config.Parameters["max_connections"] = "200"
	err = r.reconcileConfig(context.Background(), cluster)
	require.NoError(t, err)

	assert.NotNil(t, cluster.Status.LastConfigChangeTime)
}

func TestAdminReconciler_HandleMaintenance(t *testing.T) {
	tests := []struct {
		name        string
		maintenance string
	}{
		{"vacuum", util.MaintenanceVacuum},
		{"vacuum-analyze", util.MaintenanceVacuumAnalyze},
		{"vacuum-full", util.MaintenanceVacuumFull},
		{"analyze", util.MaintenanceAnalyze},
		{"reindex", util.MaintenanceReindex},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			cluster.Annotations = map[string]string{
				util.AnnotationMaintenance: tt.maintenance,
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()
			recorder := record.NewFakeRecorder(10)
			b := builder.NewBuilder()
			m := &metrics.NoopRecorder{}

			r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

			result, err := r.handleMaintenance(context.Background(), cluster, tt.maintenance)
			require.NoError(t, err)
			assert.NotZero(t, result.RequeueAfter)
		})
	}
}

func TestAdminReconciler_Reconcile_ObservedGenerationSkip(t *testing.T) {
	// When ObservedGeneration matches and no maintenance annotation, should skip.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestAdminReconciler_Reconcile_ObservedGenerationNotSkippedWithMaintenanceAnnotation(t *testing.T) {
	// When ObservedGeneration matches but maintenance annotation is present, should NOT skip.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Generation = 2
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = 2
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})

	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)

	// Verify annotation was removed.
	updated := &cbv1alpha1.CloudberryCluster{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: "test-cluster", Namespace: "default"}, updated)
	require.NoError(t, err)
	_, exists := updated.Annotations[util.AnnotationMaintenance]
	assert.False(t, exists)
}

func TestAdminReconciler_ConfigHashes_SyncMap(t *testing.T) {
	// Test that configHashes sync.Map works correctly for multiple clusters.
	scheme := newTestScheme()
	cluster1 := newTestCluster()
	cluster1.Name = "cluster-1"
	cluster1.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster1.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	cluster2 := newTestCluster()
	cluster2.Name = "cluster-2"
	cluster2.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster2.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "200"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster1, cluster2).
		WithStatusSubresource(cluster1, cluster2).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Reconcile config for cluster1.
	err := r.reconcileConfig(context.Background(), cluster1)
	require.NoError(t, err)

	// Reconcile config for cluster2.
	err = r.reconcileConfig(context.Background(), cluster2)
	require.NoError(t, err)

	// Verify both hashes are stored.
	_, ok1 := r.configHashes.Load("default/cluster-1")
	assert.True(t, ok1)
	_, ok2 := r.configHashes.Load("default/cluster-2")
	assert.True(t, ok2)
}

func TestAdminReconciler_ReconcileStorage_RecommendationScanDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.RecommendationCount = 10
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	// When recommendation scan is disabled, count should be reset to 0.
	assert.Equal(t, int32(0), cluster.Status.RecommendationCount)
}

func TestAdminReconciler_ReconcileWorkload_NotEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: false,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileQueryMonitoring_NotEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled: false,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileQueryMonitoring(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileBackup_NotEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: false,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileBackup(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileDataLoading_NotEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: false,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ReconcileStorage_NotDiskMonitoring(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: false,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_Reconcile_ConfigReconcileError(t *testing.T) {
	// Test that config reconcile error is properly handled.
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("configmap create failed")
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Equal(t, requeueAfterError, result.RequeueAfter)
}

func TestAdminReconciler_HandleMaintenance_PatchError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: util.MaintenanceVacuum,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return fmt.Errorf("patch failed")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	_, err := r.handleMaintenance(context.Background(), cluster, util.MaintenanceVacuum)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "removing maintenance annotation")
}

func TestAdminReconciler_ReconcileConfig_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok {
					return fmt.Errorf("get failed")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileConfig(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting postgresql.conf configmap")
}

// ============================================================================
// Rolling Restart Tests
// ============================================================================

func TestAdminReconciler_ContinueRollingRestart_InvalidAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: "invalid-json",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ContinueRollingRestart_PhaseComplete(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	// Create the primary STS with all replicas rolled (ready + updated + revisions match).
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4
	primarySts.Status.UpdatedReplicas = 4
	primarySts.Status.CurrentRevision = "rev-2"
	primarySts.Status.UpdateRevision = "rev-2"

	// Create coordinator STS with all replicas rolled.
	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordReplicas := int32(1)
	coordSts.Spec.Replicas = &coordReplicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1
	coordSts.Status.CurrentRevision = "rev-2"
	coordSts.Status.UpdateRevision = "rev-2"

	state := `{"phase":"primaries","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, coordSts).
		WithStatusSubresource(cluster, primarySts, coordSts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ContinueRollingRestart_WaitingForRolled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4
	primarySts.Status.UpdatedReplicas = 2 // Not all updated yet.
	primarySts.Status.CurrentRevision = "rev-1"
	primarySts.Status.UpdateRevision = "rev-2"

	state := `{"phase":"primaries","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster, primarySts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.continueRollingRestart(context.Background(), cluster)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterShort, result.RequeueAfter)
}

func TestAdminReconciler_CompleteRollingRestart(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: `{"phase":"completed"}`,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	state := rollingRestartState{
		Phase:         "completed",
		RestartParams: []string{"max_connections"},
	}
	result, err := r.completeRollingRestart(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterDefault, result.RequeueAfter)
}

func TestAdminReconciler_UpdateRestartAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	state := rollingRestartState{
		Phase:         "coordinator",
		RestartParams: []string{"max_connections"},
	}
	result, err := r.updateRestartAnnotation(context.Background(), cluster, state)
	require.NoError(t, err)
	assert.Equal(t, requeueAfterShort, result.RequeueAfter)
}

func TestAdminReconciler_RestartStatefulSet_Success(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.restartStatefulSet(context.Background(), "default", sts.Name)
	require.NoError(t, err)

	// Verify the restart trigger annotation was set.
	updated := &appsv1.StatefulSet{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{Name: sts.Name, Namespace: "default"}, updated)
	require.NoError(t, err)
	assert.NotEmpty(t, updated.Spec.Template.Annotations[util.AnnotationRestartTrigger])
}

func TestAdminReconciler_RestartStatefulSet_NotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.restartStatefulSet(context.Background(), "default", "nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting statefulset")
}

func TestAdminReconciler_IsStatefulSetRolled_True(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 1
	sts.Status.UpdatedReplicas = 1
	sts.Status.CurrentRevision = "rev-2"
	sts.Status.UpdateRevision = "rev-2"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	rolled, err := r.isStatefulSetRolled(context.Background(), "default", sts.Name)
	require.NoError(t, err)
	assert.True(t, rolled)
}

func TestAdminReconciler_IsStatefulSetRolled_NotReady(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 0
	sts.Status.UpdatedReplicas = 0
	sts.Status.CurrentRevision = "rev-1"
	sts.Status.UpdateRevision = "rev-2"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	rolled, err := r.isStatefulSetRolled(context.Background(), "default", sts.Name)
	require.NoError(t, err)
	assert.False(t, rolled)
}

func TestAdminReconciler_IsStatefulSetRolled_RevisionMismatch(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(1)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 1
	sts.Status.UpdatedReplicas = 1
	// Revisions don't match — rolling update not yet complete.
	sts.Status.CurrentRevision = "rev-1"
	sts.Status.UpdateRevision = "rev-2"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	rolled, err := r.isStatefulSetRolled(context.Background(), "default", sts.Name)
	require.NoError(t, err)
	assert.False(t, rolled)
}

func TestAdminReconciler_IsStatefulSetRolled_NilReplicas(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	sts.Spec.Replicas = nil
	sts.Status.ReadyReplicas = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	rolled, err := r.isStatefulSetRolled(context.Background(), "default", sts.Name)
	require.NoError(t, err)
	assert.False(t, rolled)
}

func TestAdminReconciler_IsStatefulSetRolled_UpdatedReplicasMismatch(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	b := builder.NewBuilder()
	sts, _ := b.BuildCoordinatorStatefulSet(cluster)
	replicas := int32(3)
	sts.Spec.Replicas = &replicas
	sts.Status.ReadyReplicas = 3
	// Only 2 of 3 replicas updated so far.
	sts.Status.UpdatedReplicas = 2
	sts.Status.CurrentRevision = "rev-1"
	sts.Status.UpdateRevision = "rev-2"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sts).
		WithStatusSubresource(sts).
		Build()
	recorder := record.NewFakeRecorder(10)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	rolled, err := r.isStatefulSetRolled(context.Background(), "default", sts.Name)
	require.NoError(t, err)
	assert.False(t, rolled)
}

func TestAdminReconciler_StatefulSetNameForPhase(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	tests := []struct {
		phase    string
		expected string
	}{
		{"mirrors", util.SegmentMirrorName(cluster.Name)},
		{"primaries", util.SegmentPrimaryName(cluster.Name)},
		{"standby", util.StandbyName(cluster.Name)},
		{"coordinator", util.CoordinatorName(cluster.Name)},
		{"unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			name := r.statefulSetNameForPhase(cluster, tt.phase)
			assert.Equal(t, tt.expected, name)
		})
	}
}

func TestAdminReconciler_StatefulSetNameForPhase_MirroringDisabled(t *testing.T) {
	cluster := newTestCluster()
	// No mirroring.

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	name := r.statefulSetNameForPhase(cluster, "mirrors")
	assert.Empty(t, name)
}

func TestAdminReconciler_NextRestartPhase(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	assert.Equal(t, "primaries", r.nextRestartPhase(cluster, "mirrors"))
	assert.Equal(t, "standby", r.nextRestartPhase(cluster, "primaries"))
	assert.Equal(t, "coordinator", r.nextRestartPhase(cluster, "standby"))
	assert.Equal(t, "completed", r.nextRestartPhase(cluster, "coordinator"))
}

func TestAdminReconciler_NextRestartPhase_SkipDisabled(t *testing.T) {
	cluster := newTestCluster()
	// No mirroring, no standby.

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// From mirrors (disabled), should skip to primaries.
	assert.Equal(t, "primaries", r.nextRestartPhase(cluster, "mirrors"))
	// From primaries, should skip standby (disabled) to coordinator.
	assert.Equal(t, "coordinator", r.nextRestartPhase(cluster, "primaries"))
}

func TestAdminReconciler_ApplyReloadSafe(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyReloadSafe(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyReloadSafe_WithDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	// Use a mock DB factory that returns a mock client.
	dbFactory := &mockDBClientFactory{
		client: &mockDBClient{},
	}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.applyReloadSafe(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyReloadSafe_DBFactoryError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	// Use a mock DB factory that returns an error.
	dbFactory := &mockDBClientFactory{
		err: fmt.Errorf("connection refused"),
	}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Should not return an error — DB reload failure is non-fatal.
	err := r.applyReloadSafe(context.Background(), cluster)
	require.NoError(t, err)
}

func TestAdminReconciler_ApplyRestartRequired(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.applyRestartRequired(context.Background(), cluster, []string{"max_connections"})
	require.NoError(t, err)
}

// ============================================================================
// Config Classification Tests
// ============================================================================

func TestClassifyConfigChanges_RestartRequired(t *testing.T) {
	current := map[string]string{"max_connections": "200", "work_mem": "64MB"}
	previous := map[string]string{"max_connections": "100", "work_mem": "64MB"}

	changes := classifyConfigChanges(current, previous)
	assert.Contains(t, changes.restartNeeded, "max_connections")
	assert.Empty(t, changes.reloadSafe)
}

func TestClassifyConfigChanges_ReloadSafe(t *testing.T) {
	current := map[string]string{"work_mem": "128MB"}
	previous := map[string]string{"work_mem": "64MB"}

	changes := classifyConfigChanges(current, previous)
	assert.Empty(t, changes.restartNeeded)
	assert.Contains(t, changes.reloadSafe, "work_mem")
}

func TestClassifyConfigChanges_Mixed(t *testing.T) {
	current := map[string]string{"max_connections": "200", "work_mem": "128MB"}
	previous := map[string]string{"max_connections": "100", "work_mem": "64MB"}

	changes := classifyConfigChanges(current, previous)
	assert.Contains(t, changes.restartNeeded, "max_connections")
	assert.Contains(t, changes.reloadSafe, "work_mem")
}

func TestClassifyConfigChanges_RemovedParam(t *testing.T) {
	current := map[string]string{}
	previous := map[string]string{"max_connections": "100", "work_mem": "64MB"}

	changes := classifyConfigChanges(current, previous)
	assert.Contains(t, changes.restartNeeded, "max_connections")
	assert.Contains(t, changes.reloadSafe, "work_mem")
}

func TestAdminReconciler_HandleMaintenance_UnknownType(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Annotations = map[string]string{
		util.AnnotationMaintenance: "unknown-type",
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.handleMaintenance(context.Background(), cluster, "unknown-type")
	require.NoError(t, err)
	assert.False(t, result.Requeue)
}

func TestAdminReconciler_Reconcile_WithRollingRestartAnnotation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	b := builder.NewBuilder()
	primarySts, _ := b.BuildSegmentPrimaryStatefulSet(cluster)
	replicas := int32(4)
	primarySts.Spec.Replicas = &replicas
	primarySts.Status.ReadyReplicas = 4
	primarySts.Status.UpdatedReplicas = 4
	primarySts.Status.CurrentRevision = "rev-2"
	primarySts.Status.UpdateRevision = "rev-2"

	coordSts, _ := b.BuildCoordinatorStatefulSet(cluster)
	coordReplicas := int32(1)
	coordSts.Spec.Replicas = &coordReplicas
	coordSts.Status.ReadyReplicas = 1
	coordSts.Status.UpdatedReplicas = 1
	coordSts.Status.CurrentRevision = "rev-2"
	coordSts.Status.UpdateRevision = "rev-2"

	state := `{"phase":"primaries","startedAt":"2026-01-01T00:00:00Z","restartParams":["max_connections"]}`
	cluster.Annotations = map[string]string{
		util.AnnotationRollingRestart: state,
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, primarySts, coordSts).
		WithStatusSubresource(cluster, primarySts, coordSts).
		Build()
	recorder := record.NewFakeRecorder(20)
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.NoError(t, err)
	assert.NotZero(t, result.RequeueAfter)
}

func TestAdminReconciler_ReconcileSubComponents_AllEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:     true,
		Destination: cbv1alpha1.BackupDestination{Type: "s3", Bucket: "b"},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(30)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	// Should not panic.
	r.reconcileSubComponents(context.Background(), r.logger, cluster)
}

func TestAdminReconciler_Reconcile_GetError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("connection refused")
			},
		}).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fetching cluster")
}

// ============================================================================
// Workload Reconciliation with DB Tests (Scenario 25)
// ============================================================================

// mockDBClientWithWorkload extends mockDBClient with configurable workload methods.
type mockDBClientWithWorkload struct {
	*mockDBClient
	resourceGroups     []db.ResourceGroupInfo
	listRGErr          error
	createRGErr        error
	alterRGErr         error
	dropRGErr          error
	createdGroups      []db.ResourceGroupOptions
	alteredGroups      []db.ResourceGroupOptions
	droppedGroups      []string
	resourceGroupUsage map[string][2]float64 // name -> [cpu, mem]
}

func (m *mockDBClientWithWorkload) ListResourceGroups(
	_ context.Context,
) ([]db.ResourceGroupInfo, error) {
	return m.resourceGroups, m.listRGErr
}

func (m *mockDBClientWithWorkload) CreateResourceGroup(
	_ context.Context,
	opts db.ResourceGroupOptions,
) error {
	m.createdGroups = append(m.createdGroups, opts)
	return m.createRGErr
}

func (m *mockDBClientWithWorkload) AlterResourceGroup(
	_ context.Context,
	opts db.ResourceGroupOptions,
) error {
	m.alteredGroups = append(m.alteredGroups, opts)
	return m.alterRGErr
}

func (m *mockDBClientWithWorkload) DropResourceGroup(
	_ context.Context,
	name string,
) error {
	m.droppedGroups = append(m.droppedGroups, name)
	return m.dropRGErr
}

func (m *mockDBClientWithWorkload) GetResourceGroupUsage(
	_ context.Context,
	group string,
) (float64, float64, error) {
	if m.resourceGroupUsage != nil {
		if usage, ok := m.resourceGroupUsage[group]; ok {
			return usage[0], usage[1], nil
		}
	}
	return 0, 0, nil
}

func TestAdminReconciler_ReconcileWorkload_WithDBFactory_CreatesNewGroups(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "etl", Concurrency: 5, CPUMaxPercent: 30},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient:   &mockDBClient{},
		resourceGroups: nil, // No existing groups in DB.
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify both groups were created.
	assert.Len(t, dbClient.createdGroups, 2)
	assert.Equal(t, "analytics", dbClient.createdGroups[0].Name)
	assert.Equal(t, "etl", dbClient.createdGroups[1].Name)
	assert.Empty(t, dbClient.alteredGroups)
	assert.Empty(t, dbClient.droppedGroups)

	// Verify condition was set to True.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "True", string(c.Status))
			assert.Equal(t, "WorkloadReconciled", c.Reason)
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileWorkload_WithDBFactory_AltersChangedGroups(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 20, CPUMaxPercent: 60},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify the group was altered (concurrency and CPU changed).
	assert.Empty(t, dbClient.createdGroups)
	assert.Len(t, dbClient.alteredGroups, 1)
	assert.Equal(t, "analytics", dbClient.alteredGroups[0].Name)
	assert.Equal(t, int32(20), dbClient.alteredGroups[0].Concurrency)
	assert.Empty(t, dbClient.droppedGroups)
}

func TestAdminReconciler_ReconcileWorkload_WithDBFactory_DropsOrphanedGroups(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "old_group", Concurrency: 5, CPUMaxPercent: 20},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify the orphaned group was dropped.
	assert.Empty(t, dbClient.createdGroups)
	assert.Empty(t, dbClient.alteredGroups) // analytics matches, no alter needed.
	assert.Len(t, dbClient.droppedGroups, 1)
	assert.Equal(t, "old_group", dbClient.droppedGroups[0])
}

func TestAdminReconciler_ReconcileWorkload_WithDBFactory_NoChanges(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// No changes needed — everything matches.
	assert.Empty(t, dbClient.createdGroups)
	assert.Empty(t, dbClient.alteredGroups)
	assert.Empty(t, dbClient.droppedGroups)
}

func TestAdminReconciler_ReconcileWorkload_DBFactoryError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbFactory := &mockDBClientFactory{err: fmt.Errorf("connection refused")}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Should not return an error — DB unavailability is non-fatal.
	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set to False with DBUnavailable reason.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "False", string(c.Status))
			assert.Equal(t, "DBUnavailable", c.Reason)
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileWorkload_ListResourceGroupsError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		listRGErr:    fmt.Errorf("query failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	// Should not return an error — resource group failure is non-fatal.
	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set to False.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "False", string(c.Status))
			assert.Equal(t, "ResourceGroupReconcileFailed", c.Reason)
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileWorkload_CreateResourceGroupError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient:   &mockDBClient{},
		resourceGroups: nil,
		createRGErr:    fmt.Errorf("permission denied"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set to False.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "False", string(c.Status))
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileWorkload_WithRulesCreatesConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{
				Name:          "cancel-long",
				Action:        "cancel",
				ThresholdType: "running_time",
				Threshold:     "3600",
			},
		},
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{
				Name:          "idle-analytics",
				ResourceGroup: "analytics",
				IdleTimeout:   "30m",
			},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient:   &mockDBClient{},
		resourceGroups: nil,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify ConfigMap was created with rules.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{
		Name:      "test-cluster-workload-rules",
		Namespace: "default",
	}
	err = k8sClient.Get(context.Background(), cmKey, cm)
	require.NoError(t, err)

	assert.Contains(t, cm.Data, "rules.json")
	assert.Contains(t, cm.Data["rules.json"], "cancel-long")
	assert.Contains(t, cm.Data, "idle-rules.json")
	assert.Contains(t, cm.Data["idle-rules.json"], "idle-analytics")

	// Verify labels.
	assert.Equal(t, "cloudberry-operator", cm.Labels["app.kubernetes.io/managed-by"])
	assert.Equal(t, "workload-rules", cm.Labels["app.kubernetes.io/component"])
	assert.Equal(t, "test-cluster", cm.Labels["app.kubernetes.io/instance"])

	// Verify owner reference.
	assert.Len(t, cm.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", cm.OwnerReferences[0].Name)
}

func TestAdminReconciler_ReconcileWorkload_UpdatesExistingConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		Rules: []cbv1alpha1.WorkloadRule{
			{Name: "new-rule", Action: "log", Threshold: "100"},
		},
	}

	// Pre-create the ConfigMap.
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-workload-rules",
			Namespace: "default",
		},
		Data: map[string]string{
			"rules.json": `[{"name":"old-rule"}]`,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient:   &mockDBClient{},
		resourceGroups: nil,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify ConfigMap was updated.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{
		Name:      "test-cluster-workload-rules",
		Namespace: "default",
	}
	err = k8sClient.Get(context.Background(), cmKey, cm)
	require.NoError(t, err)

	assert.Contains(t, cm.Data["rules.json"], "new-rule")
	assert.NotContains(t, cm.Data["rules.json"], "old-rule")
}

func TestNeedsAlter(t *testing.T) {
	tests := []struct {
		name     string
		desired  cbv1alpha1.ResourceGroupSpec
		actual   db.ResourceGroupInfo
		expected bool
	}{
		{
			name:     "no changes",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", Concurrency: 10, CPUMaxPercent: 50},
			actual:   db.ResourceGroupInfo{Name: "rg", Concurrency: 10, CPUMaxPercent: 50},
			expected: false,
		},
		{
			name:     "concurrency changed",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", Concurrency: 20, CPUMaxPercent: 50},
			actual:   db.ResourceGroupInfo{Name: "rg", Concurrency: 10, CPUMaxPercent: 50},
			expected: true,
		},
		{
			name:     "cpu max percent changed",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", Concurrency: 10, CPUMaxPercent: 60},
			actual:   db.ResourceGroupInfo{Name: "rg", Concurrency: 10, CPUMaxPercent: 50},
			expected: true,
		},
		{
			name:     "cpu weight changed",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", CPUWeight: 100},
			actual:   db.ResourceGroupInfo{Name: "rg", CPUWeight: 50},
			expected: true,
		},
		{
			name:     "memory limit changed",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", MemoryLimit: 40},
			actual:   db.ResourceGroupInfo{Name: "rg", MemoryLimit: 20},
			expected: true,
		},
		{
			name:     "zero desired concurrency skipped",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", Concurrency: 0, CPUMaxPercent: 50},
			actual:   db.ResourceGroupInfo{Name: "rg", Concurrency: 10, CPUMaxPercent: 50},
			expected: false,
		},
		{
			name:     "zero desired cpu max percent skipped",
			desired:  cbv1alpha1.ResourceGroupSpec{Name: "rg", CPUMaxPercent: 0},
			actual:   db.ResourceGroupInfo{Name: "rg", CPUMaxPercent: 50},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := needsAlter(tt.desired, tt.actual)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAdminReconciler_ReconcileWorkload_NoRulesSkipsConfigMap(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		// No rules or idle rules.
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient:   &mockDBClient{},
		resourceGroups: nil,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify no ConfigMap was created.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{
		Name:      "test-cluster-workload-rules",
		Namespace: "default",
	}
	err = k8sClient.Get(context.Background(), cmKey, cm)
	assert.True(t, apierrors.IsNotFound(err), "ConfigMap should not exist")
}

func TestAdminReconciler_ReconcileWorkload_MixedOperations(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 20, CPUMaxPercent: 60}, // alter
			{Name: "new_group", Concurrency: 5, CPUMaxPercent: 20},  // create
			// "old_group" is not in desired — should be dropped
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "old_group", Concurrency: 5, CPUMaxPercent: 20},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify mixed operations.
	assert.Len(t, dbClient.createdGroups, 1)
	assert.Equal(t, "new_group", dbClient.createdGroups[0].Name)

	assert.Len(t, dbClient.alteredGroups, 1)
	assert.Equal(t, "analytics", dbClient.alteredGroups[0].Name)

	assert.Len(t, dbClient.droppedGroups, 1)
	assert.Equal(t, "old_group", dbClient.droppedGroups[0])
}

func TestAdminReconciler_ReconcileWorkload_DropResourceGroupError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "orphan", Concurrency: 5, CPUMaxPercent: 20},
		},
		dropRGErr: fmt.Errorf("resource group in use"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set to False.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "False", string(c.Status))
			assert.Equal(t, "ResourceGroupReconcileFailed", c.Reason)
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

func TestAdminReconciler_ReconcileWorkload_AlterResourceGroupError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 20, CPUMaxPercent: 60},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(20)
	b := builder.NewBuilder()
	m := &metrics.NoopRecorder{}

	dbClient := &mockDBClientWithWorkload{
		mockDBClient: &mockDBClient{},
		resourceGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
		},
		alterRGErr: fmt.Errorf("alter failed"),
	}
	dbFactory := &mockDBClientFactory{client: dbClient}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, dbFactory, m, nil)

	err := r.reconcileWorkload(context.Background(), cluster)
	require.NoError(t, err)

	// Verify condition was set to False.
	found := false
	for _, c := range cluster.Status.Conditions {
		if c.Type == "WorkloadConfigured" {
			found = true
			assert.Equal(t, "False", string(c.Status))
			break
		}
	}
	assert.True(t, found, "WorkloadConfigured condition should be set")
}

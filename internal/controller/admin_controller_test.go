package controller

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
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
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
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

func TestAdminReconciler_EnsurePostRestoreValidation(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
		},
	}

	restoreJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RestoreJobName(cluster.Name, "20260519020000"),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, restoreJob).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// First pass creates the validation Job.
	require.NoError(t, r.ensurePostRestoreValidation(context.Background(), cluster))

	validationName := util.PostRestoreValidationJobName(cluster.Name, "20260519020000")
	validationJob := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: validationName, Namespace: cluster.Namespace}, validationJob))
	assert.Equal(t, util.BackupOperationValidate,
		validationJob.Labels[util.LabelBackupOperation])

	// Second pass is idempotent: it must not error or duplicate.
	require.NoError(t, r.ensurePostRestoreValidation(context.Background(), cluster))

	jobs := &batchv1.JobList{}
	require.NoError(t, k8sClient.List(context.Background(), jobs))
	validateCount := 0
	for i := range jobs.Items {
		if jobs.Items[i].Labels[util.LabelBackupOperation] == util.BackupOperationValidate {
			validateCount++
		}
	}
	assert.Equal(t, 1, validateCount)
}

func TestAdminReconciler_EnsurePostRestoreValidation_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true, Image: "img"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, _ client.ObjectList, _ ...client.ListOption) error {
				return fmt.Errorf("list boom")
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensurePostRestoreValidation(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listing restore jobs")
}

func TestAdminReconciler_EnsurePostRestoreValidation_CreateError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
		},
	}
	restoreJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RestoreJobName(cluster.Name, "20260519020000"),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, restoreJob).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
				return fmt.Errorf("create boom")
			},
		}).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.ensurePostRestoreValidation(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "creating validation job")
}

func TestAdminReconciler_EnsurePostRestoreValidation_SkipsRunning(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:           "my-bucket",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{Name: "creds"},
			},
		},
	}
	runningRestore := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RestoreJobName(cluster.Name, "20260519030000"),
			Namespace: cluster.Namespace,
			Labels: map[string]string{
				util.LabelCluster:         cluster.Name,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, runningRestore).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePostRestoreValidation(context.Background(), cluster))

	validationJob := &batchv1.Job{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PostRestoreValidationJobName(cluster.Name, "20260519030000"),
		Namespace: cluster.Namespace,
	}, validationJob)
	assert.Error(t, err, "no validation job should be created for a running restore")
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

	// Disabled/absent spec is a no-op: the lightweight status stays unset.
	assert.Nil(t, cluster.Status.DataLoading)
	assert.Equal(t, int32(0), cluster.Status.DataLoadingJobs)
}

func TestAdminReconciler_ReconcileDataLoading_Enabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "job1", Type: "pxf", Enabled: true},
			{Name: "job2", Type: "pxf", Enabled: false},
			{Name: "job3", Type: "gpload", Enabled: true},
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

	// Backcompat mirror: DataLoadingJobs counts only enabled jobs.
	assert.Equal(t, int32(2), cluster.Status.DataLoadingJobs)

	// Lightweight DataLoading status.
	require.NotNil(t, cluster.Status.DataLoading)
	assert.Equal(t, "Configured", cluster.Status.DataLoading.Phase)
	assert.Equal(t, int32(3), cluster.Status.DataLoading.ConfiguredJobs)
	assert.Equal(t, int32(2), cluster.Status.DataLoading.ActiveJobs)
	require.Len(t, cluster.Status.DataLoading.Jobs, 3)
	assert.Equal(t, cbv1alpha1.DataLoadingJobStatus{Name: "job1", Enabled: true},
		cluster.Status.DataLoading.Jobs[0])
	assert.Equal(t, cbv1alpha1.DataLoadingJobStatus{Name: "job2", Enabled: false},
		cluster.Status.DataLoading.Jobs[1])
	assert.Equal(t, cbv1alpha1.DataLoadingJobStatus{Name: "job3", Enabled: true},
		cluster.Status.DataLoading.Jobs[2])

	// The status patch persists the new fields (verify via re-Get).
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	require.NotNil(t, updated.Status.DataLoading)
	assert.Equal(t, "Configured", updated.Status.DataLoading.Phase)
	assert.Equal(t, int32(3), updated.Status.DataLoading.ConfiguredJobs)
	assert.Equal(t, int32(2), updated.Status.DataLoading.ActiveJobs)
	require.Len(t, updated.Status.DataLoading.Jobs, 3)
	assert.Equal(t, int32(2), updated.Status.DataLoadingJobs)
}

// pxfCapturingRecorder records the last SetPXFServersConfigured call so PXF
// reconcile tests can assert the gauge was set with the expected count. It
// embeds NoopRecorder so all other Recorder methods are no-ops.
type pxfCapturingRecorder struct {
	metrics.NoopRecorder
	calls      int
	lastCount  float64
	lastClust  string
	lastNSpace string
}

func (m *pxfCapturingRecorder) SetPXFServersConfigured(cluster, namespace string, count float64) {
	m.calls++
	m.lastCount = count
	m.lastClust = cluster
	m.lastNSpace = namespace
}

func newPXFDataLoadingCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry/pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3-a", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "https://m"}},
				{Name: "s3-b", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "https://n"}},
				{Name: "hdfs", Type: "hdfs", Config: map[string]string{"fs.defaultFS": "hdfs://nn:8020"},
					Hive: map[string]string{"hive.metastore.uris": "thrift://h:9083"}},
				{Name: "mysql", Type: "jdbc", Config: map[string]string{"jdbc.driver": "com.mysql.cj.jdbc.Driver"}},
				{Name: "pg", Type: "jdbc", Config: map[string]string{"jdbc.driver": "org.postgresql.Driver"}},
			},
		},
	}
	return cluster
}

func TestAdminReconciler_ReconcileDataLoading_PXFEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &pxfCapturingRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.NoError(t, err)

	// Status.DataLoading.Pxf populated from spec.
	require.NotNil(t, cluster.Status.DataLoading)
	require.NotNil(t, cluster.Status.DataLoading.Pxf)
	assert.True(t, cluster.Status.DataLoading.Pxf.Configured)
	assert.Equal(t, int32(5), cluster.Status.DataLoading.Pxf.Servers)

	// Gauge recorded with the configured server count.
	assert.Equal(t, 1, m.calls)
	assert.Equal(t, 5.0, m.lastCount)
	assert.Equal(t, cluster.Name, m.lastClust)
	assert.Equal(t, cluster.Namespace, m.lastNSpace)

	// The admin reconcile NO LONGER creates the "<cluster>-pxf-servers" ConfigMap
	// (that moved to the CLUSTER controller's reconcileSegments so it exists
	// before segment pods start). The admin path is status/metric/condition only.
	cm := &corev1.ConfigMap{}
	getErr := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm)
	assert.True(t, apierrors.IsNotFound(getErr),
		"admin reconcile must NOT create the PXF servers ConfigMap (cluster controller owns it)")

	// Persisted status includes pxf sub-object.
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	require.NotNil(t, updated.Status.DataLoading.Pxf)
	assert.True(t, updated.Status.DataLoading.Pxf.Configured)
	assert.Equal(t, int32(5), updated.Status.DataLoading.Pxf.Servers)

	// Idempotent re-reconcile: no error, ConfigMap update path exercised.
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
}

func TestAdminReconciler_ReconcileDataLoading_PXFDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: false, Image: "cloudberry/pxf:2.1.0"},
		Jobs:    []cbv1alpha1.DataLoadingJob{{Name: "j1", Type: "gpload", Enabled: true}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	recorder := record.NewFakeRecorder(10)
	b := builder.NewBuilder()
	m := &pxfCapturingRecorder{}

	r := NewAdminReconciler(k8sClient, scheme, recorder, b, nil, m, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.NoError(t, err)

	// PXF status stays nil; gauge never set.
	require.NotNil(t, cluster.Status.DataLoading)
	assert.Nil(t, cluster.Status.DataLoading.Pxf)
	assert.Equal(t, 0, m.calls)

	// No ConfigMap created.
	cm := &corev1.ConfigMap{}
	err = k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm)
	assert.True(t, apierrors.IsNotFound(err))
}

// TestAdminReconciler_ReconcilePxf_ConditionMessage validates the
// DataLoadingConfigured condition is set True with the enriched PXF-server
// count message. It asserts on the in-memory condition BEFORE the
// status-subresource patch (which intentionally only carries dataLoading and
// therefore refreshes the object without the in-memory conditions). This is
// verified by driving reconcilePxf directly and replicating the exact condition
// path reconcileDataLoading uses.
func TestAdminReconciler_ReconcilePxf_ConditionMessage(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	b := builder.NewBuilder()
	m := &pxfCapturingRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10), b, nil, m, nil)

	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}
	count := r.reconcilePxf(context.Background(), cluster)
	assert.Equal(t, 5, count)

	conditionMsg := "Data loading configuration is applied"
	if cluster.Status.DataLoading.Pxf != nil {
		conditionMsg = conditionMsg + "; PXF configured: 5 servers"
	}
	cluster.Status.Conditions = util.SetCondition(
		cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataLoadingConfigured),
		metav1.ConditionTrue,
		"DataLoadingReconciled",
		conditionMsg,
	)

	cond := util.FindCondition(cluster.Status.Conditions,
		string(cbv1alpha1.ConditionDataLoadingConfigured))
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, "PXF configured: 5 servers")
}

func TestClampInt32(t *testing.T) {
	assert.Equal(t, int32(0), clampInt32(0))
	assert.Equal(t, int32(5), clampInt32(5))
	assert.Equal(t, int32(0), clampInt32(-1))
	assert.Equal(t, int32(math.MaxInt32), clampInt32(math.MaxInt32+1))
}

func TestAdminReconciler_ReconcileDataLoading_NoJobs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
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

	require.NotNil(t, cluster.Status.DataLoading)
	assert.Equal(t, "Configured", cluster.Status.DataLoading.Phase)
	assert.Equal(t, int32(0), cluster.Status.DataLoading.ConfiguredJobs)
	assert.Equal(t, int32(0), cluster.Status.DataLoading.ActiveJobs)
	assert.Empty(t, cluster.Status.DataLoading.Jobs)
	assert.Equal(t, int32(0), cluster.Status.DataLoadingJobs)

	// The empty jobs array must persist (MergePatch empty-array path).
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	require.NotNil(t, updated.Status.DataLoading)
	assert.Empty(t, updated.Status.DataLoading.Jobs)
}

func TestAdminReconciler_ReconcileDataLoading_MultipleAllEnabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "a", Type: "pxf", Enabled: true},
			{Name: "b", Type: "gpload", Enabled: true},
			{Name: "c", Type: "pxf", Enabled: true},
			{Name: "d", Type: "gpload", Enabled: true},
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

	require.NotNil(t, cluster.Status.DataLoading)
	assert.Equal(t, int32(4), cluster.Status.DataLoading.ConfiguredJobs)
	assert.Equal(t, int32(4), cluster.Status.DataLoading.ActiveJobs)
	assert.Equal(t, int32(4), cluster.Status.DataLoadingJobs)
	require.Len(t, cluster.Status.DataLoading.Jobs, 4)
}

func TestAdminReconciler_PatchDataLoadingStatus_EmptyJobs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.DataLoadingJobs = 0
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase:          "Configured",
		ConfiguredJobs: 0,
		ActiveJobs:     0,
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

	require.NoError(t, r.patchDataLoadingStatus(context.Background(), cluster))

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	require.NotNil(t, updated.Status.DataLoading)
	assert.Equal(t, "Configured", updated.Status.DataLoading.Phase)
	assert.Empty(t, updated.Status.DataLoading.Jobs)
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
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "backups"},
		},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader", Type: "pxf", Enabled: true},
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
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "backups"},
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{Incremental: true},
	}
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader1", Type: "pxf", Enabled: true},
			{Name: "loader2", Type: "pxf", Enabled: true},
			{Name: "loader3", Type: "gpload", Enabled: false},
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
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "b"},
		},
	}
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "b"},
		},
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
	assert.Equal(t, util.LabelManagedByValue, cm.Labels[util.LabelManagedBy])
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

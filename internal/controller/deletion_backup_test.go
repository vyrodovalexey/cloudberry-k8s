package controller

// Tests for A-5 (H-5): the backupOnDelete deletion state machine. With
// spec.backupOnDelete=true and deletionPolicy=Delete, PVC deletion and
// finalizer removal must wait for the deletion-backup Job to reach a terminal
// state (with a timeout safety net) so the backup runs against intact volumes.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// backupCall captures a RecordBackup invocation.
type backupCall struct {
	cluster    string
	namespace  string
	backupType string
	status     string
}

// backupMetricsRecorder wraps NoopRecorder and tracks backup metric calls.
type backupMetricsRecorder struct {
	metrics.NoopRecorder
	mu    sync.Mutex
	calls []backupCall
}

func (r *backupMetricsRecorder) RecordBackup(cluster, namespace, backupType, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, backupCall{cluster, namespace, backupType, status})
}

func (r *backupMetricsRecorder) onDeleteResults() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, c := range r.calls {
		if c.backupType == backupOnDeleteMetricType {
			out = append(out, c.status)
		}
	}
	return out
}

// deletionTestEnv bundles the fake-client world for deletion tests.
type deletionTestEnv struct {
	r       *ClusterReconciler
	client  client.Client
	events  *record.FakeRecorder
	metrics *backupMetricsRecorder
	key     types.NamespacedName
}

func newDeletionTestEnv(t *testing.T, mutate func(*cbv1alpha1.CloudberryCluster)) *deletionTestEnv {
	t.Helper()
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.BackupOnDelete = true
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete
	if mutate != nil {
		mutate(cluster)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-data",
			Namespace: cluster.Namespace,
			Labels:    map[string]string{util.LabelCluster: cluster.Name},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pvc).
		WithStatusSubresource(cluster).
		Build()
	events := record.NewFakeRecorder(50)
	metricsRec := &backupMetricsRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, events, builder.NewBuilder(), metricsRec, nil)

	return &deletionTestEnv{
		r:       r,
		client:  k8sClient,
		events:  events,
		metrics: metricsRec,
		key:     types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	}
}

// getCluster re-fetches the cluster (fails the test when it is gone).
func (e *deletionTestEnv) getCluster(t *testing.T) *cbv1alpha1.CloudberryCluster {
	t.Helper()
	cluster := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, e.client.Get(context.Background(), e.key, cluster))
	return cluster
}

// clusterGone reports whether the cluster object was fully deleted.
func (e *deletionTestEnv) clusterGone() bool {
	cluster := &cbv1alpha1.CloudberryCluster{}
	err := e.client.Get(context.Background(), e.key, cluster)
	return apierrors.IsNotFound(err)
}

// pvcCount returns the number of cluster-labeled PVCs still present.
func (e *deletionTestEnv) pvcCount(t *testing.T) int {
	t.Helper()
	pvcList := &corev1.PersistentVolumeClaimList{}
	require.NoError(t, e.client.List(context.Background(), pvcList,
		client.InNamespace(e.key.Namespace)))
	return len(pvcList.Items)
}

// trackedJob returns the Job referenced by the deletion-backup annotation.
func (e *deletionTestEnv) trackedJob(t *testing.T) *batchv1.Job {
	t.Helper()
	cluster := e.getCluster(t)
	jobName := cluster.Annotations[util.AnnotationDeletionBackupJob]
	require.NotEmpty(t, jobName, "deletion-backup job annotation must be stamped")
	job := &batchv1.Job{}
	require.NoError(t, e.client.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: e.key.Namespace}, job))
	return job
}

func TestDeletionBackup_HappyPath_PVCsSurviveUntilJobSucceeds(t *testing.T) {
	env := newDeletionTestEnv(t, nil)

	// Pass 1: creates the backup Job, stamps annotations, requeues. PVCs and
	// finalizer must be untouched.
	result, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterDeletionBackup, result.RequeueAfter)
	assert.Equal(t, 1, env.pvcCount(t), "PVCs must survive while the backup Job is active")

	cluster := env.getCluster(t)
	assert.Contains(t, cluster.Finalizers, util.FinalizerName,
		"finalizer must remain while the backup Job is active")
	job := env.trackedJob(t)
	assert.NotEmpty(t, cluster.Annotations[util.AnnotationDeletionBackupDeadline])

	// Pass 2: Job still running → requeue again, nothing deleted.
	result, err = env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.Equal(t, requeueAfterDeletionBackup, result.RequeueAfter)
	assert.Equal(t, 1, env.pvcCount(t))

	// The Job succeeds.
	job.Status.Succeeded = 1
	require.NoError(t, env.client.Status().Update(context.Background(), job))

	// Pass 3: PVCs deleted, finalizer removed LAST → object fully gone.
	result, err = env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)
	assert.Equal(t, 0, env.pvcCount(t), "PVCs must be deleted after the Job succeeded")
	assert.True(t, env.clusterGone(), "no orphaned finalizer: object must be fully deleted")
	assert.Equal(t, []string{"completed"}, env.metrics.onDeleteResults())
}

func TestDeletionBackup_JobFailed_WarningAndProceed(t *testing.T) {
	env := newDeletionTestEnv(t, nil)

	// Pass 1 creates the Job.
	_, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)

	// The Job fails terminally.
	job := env.trackedJob(t)
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
	}
	require.NoError(t, env.client.Status().Update(context.Background(), job))

	// Pass 2: warning event + failed metric, deletion proceeds anyway.
	_, err = env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.True(t, env.clusterGone(), "failed backup must not wedge deletion")
	assert.Equal(t, 0, env.pvcCount(t))
	assert.Equal(t, []string{"failed"}, env.metrics.onDeleteResults())
	assert.True(t, drainEventsContains(env.events, "Warning", "BackupOnDeleteFailed"),
		"a Warning BackupOnDeleteFailed event must be emitted")
}

func TestDeletionBackup_Timeout_ProceedsWithExplanation(t *testing.T) {
	env := newDeletionTestEnv(t, nil)

	// Pass 1 creates the Job and stamps the deadline.
	_, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)

	// Force the deadline into the past (Job stays non-terminal).
	cluster := env.getCluster(t)
	cluster.Annotations[util.AnnotationDeletionBackupDeadline] =
		time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	require.NoError(t, env.client.Update(context.Background(), cluster))

	// Pass 2: timeout path → deletion proceeds with a Warning explaining why.
	_, err = env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.True(t, env.clusterGone(), "timeout must not wedge deletion")
	assert.Equal(t, []string{"failed"}, env.metrics.onDeleteResults())
	assert.True(t, drainEventsContains(env.events, "Warning", "did not finish within"),
		"the timeout event must explain why deletion proceeded")
}

func TestDeletionBackup_JobDisappeared_Proceeds(t *testing.T) {
	env := newDeletionTestEnv(t, nil)

	_, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)

	// Delete the tracked Job out from under the state machine.
	job := env.trackedJob(t)
	require.NoError(t, env.client.Delete(context.Background(), job))

	_, err = env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.True(t, env.clusterGone())
	assert.Equal(t, []string{"failed"}, env.metrics.onDeleteResults())
}

func TestDeletionBackup_Regression_NoBackupOnDelete_SinglePass(t *testing.T) {
	env := newDeletionTestEnv(t, func(c *cbv1alpha1.CloudberryCluster) {
		c.Spec.BackupOnDelete = false
	})

	result, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "no backup gate: deletion completes in one pass")
	assert.True(t, env.clusterGone())
	assert.Equal(t, 0, env.pvcCount(t))
	assert.Empty(t, env.metrics.onDeleteResults(), "no backup-on-delete metric without a backup")
}

func TestDeletionBackup_Regression_RetainPolicy_SinglePassBestEffort(t *testing.T) {
	env := newDeletionTestEnv(t, func(c *cbv1alpha1.CloudberryCluster) {
		c.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyRetain
	})

	result, err := env.r.handleDeletion(context.Background(), env.getCluster(t))
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter, "Retain policy keeps the one-pass fire-and-forget behavior")
	assert.True(t, env.clusterGone())
	assert.Equal(t, 1, env.pvcCount(t), "PVCs must be retained (deletionPolicy: Retain)")

	// The best-effort backup Job was still created.
	jobList := &batchv1.JobList{}
	require.NoError(t, env.client.List(context.Background(), jobList,
		client.InNamespace(env.key.Namespace)))
	assert.Len(t, jobList.Items, 1)
}

func TestDeletionBackup_JobCreateFails_ProceedsWithFailureReported(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Spec.BackupOnDelete = true
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptorFailJobCreate()).
		Build()
	events := record.NewFakeRecorder(50)
	metricsRec := &backupMetricsRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, events, builder.NewBuilder(), metricsRec, nil)

	_, err := r.handleDeletion(context.Background(), cluster)
	require.NoError(t, err)

	// Deletion proceeded despite the un-creatable backup Job.
	got := &cbv1alpha1.CloudberryCluster{}
	getErr := k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, got)
	assert.True(t, apierrors.IsNotFound(getErr), "deletion must proceed when the Job cannot be created")
	assert.Equal(t, []string{"failed"}, metricsRec.onDeleteResults())
	assert.True(t, drainEventsContains(events, "Warning", "Failed to create backup job"))
}

func TestDeletionBackup_AnnotationPatchFails_Requeues(t *testing.T) {
	scheme := newTestScheme()
	now := metav1.Now()
	cluster := newTestCluster()
	cluster.Finalizers = []string{util.FinalizerName}
	cluster.DeletionTimestamp = &now
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseDeleting
	cluster.Spec.BackupOnDelete = true
	cluster.Spec.DeletionPolicy = cbv1alpha1.DeletionPolicyDelete

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptorFailClusterPatch()).
		Build()
	events := record.NewFakeRecorder(50)
	metricsRec := &backupMetricsRecorder{}
	r := NewClusterReconciler(k8sClient, scheme, events, builder.NewBuilder(), metricsRec, nil)

	_, err := r.handleDeletion(context.Background(), cluster)
	require.Error(t, err, "an unpersisted tracking annotation must surface for retry")
	assert.Contains(t, err.Error(), "recording deletion-backup annotations")

	// Finalizer intact: deletion did NOT proceed without the tracking state.
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, got))
	assert.Contains(t, got.Finalizers, util.FinalizerName)
}

func TestDeletionBackupDeadlineExceeded_Parsing(t *testing.T) {
	env := newDeletionTestEnv(t, nil)
	logger := env.r.logger

	cluster := env.getCluster(t)

	// No annotation → not exceeded.
	assert.False(t, env.r.deletionBackupDeadlineExceeded(cluster, logger))

	// Unparseable annotation → treated as exceeded (fail-open by design).
	cluster.Annotations = map[string]string{
		util.AnnotationDeletionBackupDeadline: "not-a-timestamp",
	}
	assert.True(t, env.r.deletionBackupDeadlineExceeded(cluster, logger))

	// Future deadline → not exceeded.
	cluster.Annotations[util.AnnotationDeletionBackupDeadline] =
		time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	assert.False(t, env.r.deletionBackupDeadlineExceeded(cluster, logger))
}

func TestRecordBackupOnDelete_NilMetricsSafe(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(5),
		builder.NewBuilder(), nil, nil)

	assert.NotPanics(t, func() {
		r.recordBackupOnDelete(newTestCluster(), "completed")
	})
}

// interceptorFailJobCreate fails Create calls for batch Jobs only.
func interceptorFailJobCreate() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
			opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return apierrors.NewInternalError(assert.AnError)
			}
			return c.Create(ctx, obj, opts...)
		},
	}
}

// interceptorFailClusterPatch fails Patch calls on CloudberryCluster objects.
func interceptorFailClusterPatch() interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object,
			patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
				return apierrors.NewInternalError(assert.AnError)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

// drainEventsContains drains the fake recorder looking for an event matching
// both substrings (type and message fragment).
func drainEventsContains(rec *record.FakeRecorder, kind, substr string) bool {
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, kind) && strings.Contains(ev, substr) {
				return true
			}
		default:
			return false
		}
	}
}

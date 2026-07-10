package controller

// Tests for the steady-state refresh error counter (task T12/E1:
// cloudberry_steady_state_refresh_errors_total{cluster,namespace,component}),
// the refetch-error logging before status patch (task T14/B5) and the
// workload-rules ConfigMap name helper (task T17/D1).

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// steadyStateErrCall captures one RecordSteadyStateRefreshError invocation.
type steadyStateErrCall struct {
	cluster   string
	namespace string
	component string
}

// steadyStateErrRecorder wraps NoopRecorder and tracks steady-state refresh
// error metric calls.
type steadyStateErrRecorder struct {
	metrics.NoopRecorder
	mu    sync.Mutex
	calls []steadyStateErrCall
}

func (r *steadyStateErrRecorder) RecordSteadyStateRefreshError(cluster, namespace, component string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, steadyStateErrCall{cluster, namespace, component})
}

func (r *steadyStateErrRecorder) recorded() []steadyStateErrCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]steadyStateErrCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// newSteadyStateEnv builds an AdminReconciler over a fake client with the
// given interceptors and a tracking metrics recorder.
func newSteadyStateEnv(
	cluster *cbv1alpha1.CloudberryCluster,
	funcs interceptor.Funcs,
) (*AdminReconciler, *steadyStateErrRecorder) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(funcs).
		Build()
	rec := &steadyStateErrRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)
	return r, rec
}

// failJobList returns interceptors that fail every List of batch Jobs while
// passing through all other reads/writes.
func failJobList() interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch,
			list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*batchv1.JobList); ok {
				return fmt.Errorf("job list boom")
			}
			return c.List(ctx, list, opts...)
		},
	}
}

func TestRefreshBackupStatusOnSteadyState_ErrorIncrementsCounter(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}

	r, rec := newSteadyStateEnv(cluster, failJobList())
	r.refreshBackupStatusOnSteadyState(context.Background(), cluster)

	calls := rec.recorded()
	require.NotEmpty(t, calls, "a failed backup refresh must increment the counter")
	for _, c := range calls {
		assert.Equal(t, steadyStateComponentBackup, c.component)
		assert.Equal(t, cluster.Name, c.cluster)
		assert.Equal(t, cluster.Namespace, c.namespace)
	}
}

func TestRefreshBackupStatusOnSteadyState_SuccessNoIncrement(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}

	r, rec := newSteadyStateEnv(cluster, interceptor.Funcs{})
	r.refreshBackupStatusOnSteadyState(context.Background(), cluster)

	assert.Empty(t, rec.recorded(),
		"a successful refresh must NOT increment the counter (honest metrics)")
}

func TestRefreshDataLoadingStatusOnSteadyState_ErrorIncrementsCounter(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}

	// Fail the data-loading status patch: SubResourcePatch covers the status
	// subresource writes issued by patchDataLoadingStatus.
	r, rec := newSteadyStateEnv(cluster, interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
			_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			return fmt.Errorf("status patch boom")
		},
	})
	r.refreshDataLoadingStatusOnSteadyState(context.Background(), cluster)

	calls := rec.recorded()
	require.Len(t, calls, 1)
	assert.Equal(t, steadyStateComponentDataLoading, calls[0].component)
}

func TestRefreshDataLoadingStatusOnSteadyState_DisabledNoIncrement(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	r, rec := newSteadyStateEnv(cluster, failJobList())
	r.refreshDataLoadingStatusOnSteadyState(context.Background(), cluster)

	assert.Empty(t, rec.recorded(),
		"disabled data loading is a no-op and must not increment the counter")
}

func TestRefreshStorageOnSteadyState_ErrorIncrementsCounter(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:  true,
			Schedule: "0 3 * * 0",
		},
	}

	// Fail CronJob reads so ensureRecommendationScanCronJob errors.
	r, rec := newSteadyStateEnv(cluster, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch,
			key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.CronJob); ok {
				return fmt.Errorf("cronjob get boom")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	r.refreshStorageOnSteadyState(context.Background(), cluster)

	calls := rec.recorded()
	require.Len(t, calls, 1)
	assert.Equal(t, steadyStateComponentStorage, calls[0].component)
}

func TestRefreshStorageOnSteadyState_SuccessNoIncrement(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}

	r, rec := newSteadyStateEnv(cluster, interceptor.Funcs{})
	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.Empty(t, rec.recorded(),
		"a successful storage refresh must NOT increment the counter")
}

// TestRefreshPhaseFromServer_RefetchErrorLoggedAndPhaseKept covers T14/B5: a
// failing refetch before the status patch keeps the in-memory phase
// (behavior unchanged) and emits a warning log instead of silently dropping
// the error.
func TestRefreshPhaseFromServer_RefetchErrorLoggedAndPhaseKept(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	r, _ := newSteadyStateEnv(cluster, interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch,
			_ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return fmt.Errorf("refetch boom")
		},
	})

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	r.refreshPhaseFromServer(context.Background(), logger, cluster)

	assert.Equal(t, cbv1alpha1.ClusterPhaseRunning, cluster.Status.Phase,
		"in-memory phase must be kept on refetch failure")
	logged := buf.String()
	assert.Contains(t, logged, "failed to re-read cluster before status patch")
	assert.Contains(t, logged, "refetch boom")
	assert.Contains(t, logged, cluster.Name)
}

// TestRefreshPhaseFromServer_AdoptsServerPhase pins the happy path: the
// latest phase from the API server is adopted.
func TestRefreshPhaseFromServer_AdoptsServerPhase(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseScaling

	r, _ := newSteadyStateEnv(cluster, interceptor.Funcs{})
	require.NoError(t, r.client.Status().Update(context.Background(), cluster))

	inMemory := cluster.DeepCopy()
	inMemory.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	r.refreshPhaseFromServer(context.Background(), logger, inMemory)

	assert.Equal(t, cbv1alpha1.ClusterPhaseScaling, inMemory.Status.Phase,
		"server phase must be adopted so concurrent phase changes are not clobbered")
	assert.Empty(t, buf.String(), "no warning on a successful refetch")
}

// TestWorkloadRulesConfigMapKey pins the rendered ConfigMap name (T17/D1):
// the helper must produce the same "{cluster}-workload-rules" name the
// literal concatenations produced before the extraction.
func TestWorkloadRulesConfigMapKey(t *testing.T) {
	cluster := newTestCluster()
	key := workloadRulesConfigMapKey(cluster)
	assert.Equal(t, "test-cluster-workload-rules", key.Name)
	assert.Equal(t, cluster.Namespace, key.Namespace)
}

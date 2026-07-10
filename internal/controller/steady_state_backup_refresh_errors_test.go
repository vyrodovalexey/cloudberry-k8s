package controller

// Per-branch tests for the steady-state refresh error surface introduced by
// fix E1 (handoff tasks T-1/T-2):
//   - each of the 6 previously-untested warnSteadyStateRefreshError call
//     sites in refreshBackupStatusOnSteadyState increments
//     cloudberry_steady_state_refresh_errors_total EXACTLY once with
//     component="backup",
//   - non-fatal branches keep executing the later steps (status patch still
//     issued), fatal branches early-return (no Job list / no status patch),
//   - refreshDataLoadingStatusOnSteadyState increments exactly once with
//     component="dataloading" on a Job LIST failure and issues NO status
//     patch,
//   - the reconcile outcome stays SUCCESS despite a steady-state refresh
//     error (best-effort semantics preserved).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// steadyCallTracker counts the Job LISTs and cluster status subresource
// patches observed by the interceptors, so tests can pin early-return vs.
// continue-non-fatally behavior.
type steadyCallTracker struct {
	jobLists      atomic.Int32
	statusPatches atomic.Int32
}

// trackingFuncs wires the tracker into interceptor funcs, chaining the
// optional per-row fault injection.
func trackingFuncs(tr *steadyCallTracker, inject interceptor.Funcs) interceptor.Funcs {
	out := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch,
			list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*batchv1.JobList); ok {
				tr.jobLists.Add(1)
			}
			if inject.List != nil {
				return inject.List(ctx, c, list, opts...)
			}
			return c.List(ctx, list, opts...)
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string,
			obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
				tr.statusPatches.Add(1)
			}
			if inject.SubResourcePatch != nil {
				return inject.SubResourcePatch(ctx, c, subResourceName, obj, patch, opts...)
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
		Get:    inject.Get,
		Create: inject.Create,
		Patch:  inject.Patch,
	}
	return out
}

// failBackupCronJobGet fails Get calls for the BACKUP schedule CronJob only,
// keeping the storage recommendation-scan CronJob (and everything else)
// functional so components stay isolated.
func failBackupCronJobGet(cluster string) interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch,
			key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.CronJob); ok && key.Name == util.BackupCronJobName(cluster) {
				return fmt.Errorf("backup cronjob get boom")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
}

// failAllJobLists fails every List of batch Jobs.
func failAllJobLists() interceptor.Funcs {
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

// failJobCreates fails every Create of a batch Job.
func failJobCreates() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch,
			obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return fmt.Errorf("job create boom")
			}
			return c.Create(ctx, obj, opts...)
		},
	}
}

// failJobPatches fails every Patch of a batch Job.
func failJobPatches() interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch,
			obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return fmt.Errorf("job patch boom")
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
}

// failClusterStatusPatch fails the cluster status subresource patch.
func failClusterStatusPatch() interceptor.Funcs {
	return interceptor.Funcs{
		SubResourcePatch: func(_ context.Context, _ client.Client, _ string,
			obj client.Object, _ client.Patch, _ ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
				return fmt.Errorf("status patch boom")
			}
			return fmt.Errorf("unexpected subresource patch")
		},
	}
}

// steadyOpJob builds a terminal (Succeeded) backup-operation Job fixture.
func steadyOpJob(c *cbv1alpha1.CloudberryCluster, name, operation string, annotations map[string]string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   c.Namespace,
			Labels:      map[string]string{util.LabelCluster: c.Name, util.LabelBackupOperation: operation},
			Annotations: annotations,
		},
		Status: batchv1.JobStatus{Succeeded: 1},
	}
}

// newBackupSteadyEnv builds an AdminReconciler over a fake client seeded with
// the cluster + extra objects, tracked interceptors and a recording metrics
// recorder.
func newBackupSteadyEnv(
	cluster *cbv1alpha1.CloudberryCluster,
	extra []client.Object,
	tr *steadyCallTracker,
	inject interceptor.Funcs,
) (*AdminReconciler, *steadyStateErrRecorder) {
	scheme := newTestScheme()
	objects := append([]client.Object{cluster}, extra...)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(trackingFuncs(tr, inject)).
		Build()
	rec := &steadyStateErrRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)
	return r, rec
}

const steadyTestTimestamp = "20260101020000"

func TestRefreshBackupStatusOnSteadyState_ErrorBranches(t *testing.T) {
	tests := []struct {
		name string
		// mutate configures the backup spec shape of the branch.
		mutate func(c *cbv1alpha1.CloudberryCluster)
		// extra seeds additional objects (terminal Jobs).
		extra func(c *cbv1alpha1.CloudberryCluster) []client.Object
		// inject is the per-branch fault.
		inject func(c *cbv1alpha1.CloudberryCluster) interceptor.Funcs
		// wantIncrements is the EXACT number of counter increments.
		wantIncrements int
		// wantStatusPatch pins whether the final status patch is still
		// ATTEMPTED (proof the later steps ran after a non-fatal error).
		wantStatusPatch bool
		// wantJobLists pins the exact number of Job LIST calls (early-return
		// proof for the fatal branches). Negative means "do not pin".
		wantJobLists int32
	}{
		{
			name: "disabled backup: removeBackupCronJob failure increments once and returns",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: false}
			},
			inject:          func(c *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failBackupCronJobGet(c.Name) },
			wantIncrements:  1,
			wantStatusPatch: false,
			wantJobLists:    0, // early return: refreshBackupStatus never runs
		},
		{
			name: "ensureBackupCronJob failure is non-fatal: later steps still run",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true, Schedule: "0 2 * * *"}
			},
			inject:          func(c *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failBackupCronJobGet(c.Name) },
			wantIncrements:  1,
			wantStatusPatch: true, // refreshBackupStatus + patchStatus still ran
			wantJobLists:    -1,
		},
		{
			name: "refreshBackupStatus list failure aborts before any status patch",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}
			},
			inject:          func(_ *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failAllJobLists() },
			wantIncrements:  1,
			wantStatusPatch: false,
			wantJobLists:    1, // exactly the aborted refreshBackupStatus list
		},
		{
			name: "ensureRetentionCleanup failure is non-fatal: later steps still run",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{
					Enabled:   true,
					Retention: cbv1alpha1.BackupRetention{FullCount: 1},
				}
			},
			extra: func(c *cbv1alpha1.CloudberryCluster) []client.Object {
				return []client.Object{steadyOpJob(c,
					util.BackupJobName(c.Name, steadyTestTimestamp),
					util.BackupOperationBackup,
					map[string]string{util.AnnotationBackupTimestamp: steadyTestTimestamp})}
			},
			// The only Job CREATE in this flow is the retention cleanup Job.
			inject:          func(_ *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failJobCreates() },
			wantIncrements:  1,
			wantStatusPatch: true,
			wantJobLists:    -1,
		},
		{
			name: "ensurePostRestoreValidation failure is non-fatal: later steps still run",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}
			},
			extra: func(c *cbv1alpha1.CloudberryCluster) []client.Object {
				return []client.Object{steadyOpJob(c,
					util.RestoreJobName(c.Name, steadyTestTimestamp),
					util.BackupOperationRestore, nil)}
			},
			// The only Job CREATE in this flow is the validation Job.
			inject:          func(_ *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failJobCreates() },
			wantIncrements:  1,
			wantStatusPatch: true,
			wantJobLists:    -1,
		},
		{
			name: "observeValidationJobs failure is non-fatal: later steps still run",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}
			},
			extra: func(c *cbv1alpha1.CloudberryCluster) []client.Object {
				return []client.Object{steadyOpJob(c,
					util.PostRestoreValidationJobName(c.Name, steadyTestTimestamp),
					util.BackupOperationValidate, nil)}
			},
			// The only Job PATCH in this flow is the validation-recorded
			// de-dup annotation.
			inject:          func(_ *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failJobPatches() },
			wantIncrements:  1,
			wantStatusPatch: true,
			wantJobLists:    -1,
		},
		{
			name: "patchStatus failure increments once",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: true}
			},
			inject:          func(_ *cbv1alpha1.CloudberryCluster) interceptor.Funcs { return failClusterStatusPatch() },
			wantIncrements:  1,
			wantStatusPatch: true, // attempted (and failed)
			wantJobLists:    -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange.
			cluster := newTestCluster()
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			tt.mutate(cluster)
			var extra []client.Object
			if tt.extra != nil {
				extra = tt.extra(cluster)
			}
			tr := &steadyCallTracker{}
			r, rec := newBackupSteadyEnv(cluster, extra, tr, tt.inject(cluster))

			// Act.
			r.refreshBackupStatusOnSteadyState(context.Background(), cluster)

			// Assert: EXACT increment count, component/cluster labels pinned.
			calls := rec.recorded()
			require.Len(t, calls, tt.wantIncrements,
				"the branch must increment the counter exactly %d time(s): %v",
				tt.wantIncrements, calls)
			for _, c := range calls {
				assert.Equal(t, steadyStateComponentBackup, c.component)
				assert.Equal(t, cluster.Name, c.cluster)
				assert.Equal(t, cluster.Namespace, c.namespace)
			}

			// Assert: continue-vs-abort behavior.
			if tt.wantStatusPatch {
				assert.Positive(t, tr.statusPatches.Load(),
					"later steps must still run: the status patch must be attempted")
			} else {
				assert.Zero(t, tr.statusPatches.Load(),
					"the aborting branch must not attempt a status patch")
			}
			if tt.wantJobLists >= 0 {
				assert.Equal(t, tt.wantJobLists, tr.jobLists.Load(),
					"unexpected number of Job LIST calls")
			}
		})
	}
}

// TestRefreshDataLoadingStatusOnSteadyState_JobListFailure covers T-2: a
// failing Job LIST inside reconcileDataLoadingJobs increments the counter
// exactly once with component="dataloading", returns early and issues NO
// data-loading status patch.
func TestRefreshDataLoadingStatusOnSteadyState_JobListFailure(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		// A configured-but-disabled job keeps the jobs slice non-empty (so
		// reconcileDataLoadingJobs proceeds to the owned-Jobs LIST) without
		// creating any workload first.
		Jobs: []cbv1alpha1.DataLoadingJob{{Name: "j1", Type: "pxf", Enabled: false}},
	}

	tr := &steadyCallTracker{}
	r, rec := newBackupSteadyEnv(cluster, nil, tr, failAllJobLists())

	r.refreshDataLoadingStatusOnSteadyState(context.Background(), cluster)

	calls := rec.recorded()
	require.Len(t, calls, 1, "exactly one increment for the failed Job LIST")
	assert.Equal(t, steadyStateComponentDataLoading, calls[0].component)
	assert.Equal(t, cluster.Name, calls[0].cluster)
	assert.Equal(t, cluster.Namespace, calls[0].namespace)
	assert.Zero(t, tr.statusPatches.Load(),
		"the early return must not issue a data-loading status patch")
}

// steadyOutcomeRecorder additionally captures RecordReconcile outcomes so the
// reconcile-result label can be pinned alongside the refresh-error counter.
type steadyOutcomeRecorder struct {
	metrics.NoopRecorder
	mu               sync.Mutex
	refreshCalls     []steadyStateErrCall
	reconcileResults []string
}

func (r *steadyOutcomeRecorder) RecordSteadyStateRefreshError(cluster, namespace, component string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refreshCalls = append(r.refreshCalls, steadyStateErrCall{cluster, namespace, component})
}

func (r *steadyOutcomeRecorder) RecordReconcile(_, _, result string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reconcileResults = append(r.reconcileResults, result)
}

func (r *steadyOutcomeRecorder) snapshot() ([]steadyStateErrCall, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	refresh := make([]steadyStateErrCall, len(r.refreshCalls))
	copy(refresh, r.refreshCalls)
	results := make([]string, len(r.reconcileResults))
	copy(results, r.reconcileResults)
	return refresh, results
}

// TestSteadyStateRefreshError_ReconcileOutcomeStaysSuccess drives the FULL
// admin Reconcile through the steady-state generation gate with a failing
// backup refresh and pins that the refresh error is counted while the
// reconcile outcome metric stays "success" and no error is returned
// (best-effort semantics of fix E1 preserved).
func TestSteadyStateRefreshError_ReconcileOutcomeStaysSuccess(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Status.ObservedGeneration = cluster.Generation
	// Disabled backup with a failing CronJob GET: the disabled-cleanup branch
	// increments the counter, everything else on the steady-state path is
	// healthy.
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{Enabled: false}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(failBackupCronJobGet(cluster.Name)).
		Build()
	rec := &steadyOutcomeRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)

	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})

	require.NoError(t, err, "a steady-state refresh error must never fail the reconcile")
	assert.Equal(t, r.requeueDefault(), result.RequeueAfter,
		"the periodic requeue must be preserved")

	refresh, results := rec.snapshot()
	require.Len(t, refresh, 1, "exactly one backup refresh error must be counted")
	assert.Equal(t, steadyStateComponentBackup, refresh[0].component)
	assert.Equal(t, []string{reconcileResultSuccess}, results,
		"the reconcile outcome metric must record success exactly once")
}

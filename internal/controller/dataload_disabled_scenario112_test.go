package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
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

// Scenario 112 — Disabled States (DIS.1) controller teardown. These unit tests
// drive cleanupDataLoading / reconcileDataLoading (the disabled dispatch) and
// deleteDataLoadingWorkloads / deleteGploadControlFileConfigMaps over a fake
// client + spy metrics + a record.FakeRecorder. They prove the HONEST teardown
// invariants from the catalog (112-DIS1-*):
//
//   - teardown deletes ALL stale dataload objects ONLY when disabled,
//   - the DataLoadingDisabled event fires ONCE on the transition (de-dup),
//   - status is cleared, the jobs-active gauge zeroed, the condition is False,
//   - label scoping never touches foreign clusters/components,
//   - every delete is best-effort / NotFound-tolerant (an absent object never
//     fails the reconcile).

// assertErrList / assertErrDelete are injected (via interceptors) to drive the
// best-effort, non-fatal log branches of the teardown helpers — proving a List
// or Delete failure never aborts the reconcile.
var (
	assertErrList   = errors.New("injected list error")
	assertErrDelete = errors.New("injected delete error")
	assertErrPatch  = errors.New("injected patch error")
)

// dataLoadingGaugeSpy is a metrics.Recorder (via embedded NoopRecorder) that
// records the SetDataLoadingJobsActive + SetPXFServersConfigured calls so the
// teardown tests can assert the gauges were zeroed exactly as expected.
type dataLoadingGaugeSpy struct {
	metrics.NoopRecorder
	jobsActive []float64
	pxfServers []float64
}

func (m *dataLoadingGaugeSpy) SetDataLoadingJobsActive(_, _ string, count float64) {
	m.jobsActive = append(m.jobsActive, count)
}

func (m *dataLoadingGaugeSpy) SetPXFServersConfigured(_, _ string, count float64) {
	m.pxfServers = append(m.pxfServers, count)
}

// dataLoadLabelSet is the shared {LabelCluster, LabelComponent=dataload} label
// set every dataload workload / control-file ConfigMap carries — the same
// selector the teardown lists by.
func dataLoadLabelSet(cluster string) map[string]string {
	return map[string]string{
		util.LabelCluster:   cluster,
		util.LabelComponent: util.ComponentDataLoad,
	}
}

// seedStaleDataLoadingObjects creates the full set of stale data-loading objects
// a disabled-transition must reclaim: a gpfdist Deployment/Service/PVC, a
// dataload Job + CronJob (with the dataload labels), a gpload control-file
// ConfigMap (same labels) and the PXF NetworkPolicy. It returns the objects so
// the test can pass them to the fake client builder.
func seedStaleDataLoadingObjects(clusterName, namespace string) []client.Object {
	labels := dataLoadLabelSet(clusterName)
	return []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: util.SanitizeK8sName(clusterName + "-gpfdist"), Namespace: namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name: util.GpfdistServiceName2(clusterName), Namespace: namespace}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Name: util.GpfdistDataPVCName(clusterName), Namespace: namespace}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.DataLoadJobName(clusterName, "loader"), Namespace: namespace, Labels: labels}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
			Name: util.DataLoadJobName(clusterName, "nightly"), Namespace: namespace, Labels: labels}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name:      util.GploadControlFileConfigMapName(clusterName, "loader"),
			Namespace: namespace, Labels: labels}},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{
			Name: util.PxfNetworkPolicyName(clusterName), Namespace: namespace}},
	}
}

// assertNotFound asserts the given object is gone (Get returns NotFound).
func assertNotFound(t *testing.T, c client.Client, obj client.Object, key types.NamespacedName) {
	t.Helper()
	err := c.Get(context.Background(), key, obj)
	assert.True(t, apierrors.IsNotFound(err), "expected %T %s to be deleted, got err=%v", obj, key.Name, err)
}

func countDataLoadingDisabledEvents(events []string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, corev1.EventTypeNormal) &&
			strings.Contains(e, cbv1alpha1.EventReasonDataLoadingDisabled) {
			n++
		}
	}
	return n
}

// 112-DIS1-TEARDOWN (U): a disabled cluster with all the stale data-loading
// objects pre-seeded — reconcileDataLoading dispatches cleanupDataLoading, which
// deletes EVERY stale object, clears status, zeroes the jobs-active gauge, sets
// the condition False with reason DataLoadingDisabled, and emits exactly ONE
// DataLoadingDisabled event.
func TestCleanupDataLoading_TeardownDeletesAllStaleObjects(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	// Disabled spec.
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	// Status still present => this is the disabled TRANSITION (event fires once).
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured", ActiveJobs: 2}
	cluster.Status.DataLoadingJobs = 2

	objs := append([]client.Object{cluster}, seedStaleDataLoadingObjects(cluster.Name, cluster.Namespace)...)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(cluster).
		Build()

	recorder := record.NewFakeRecorder(10)
	spy := &dataLoadingGaugeSpy{}
	r := NewAdminReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), nil, spy, nil)

	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))

	ns := cluster.Namespace
	assertNotFound(t, k8sClient, &appsv1.Deployment{},
		types.NamespacedName{Name: util.SanitizeK8sName(cluster.Name + "-gpfdist"), Namespace: ns})
	assertNotFound(t, k8sClient, &corev1.Service{},
		types.NamespacedName{Name: util.GpfdistServiceName2(cluster.Name), Namespace: ns})
	assertNotFound(t, k8sClient, &corev1.PersistentVolumeClaim{},
		types.NamespacedName{Name: util.GpfdistDataPVCName(cluster.Name), Namespace: ns})
	assertNotFound(t, k8sClient, &batchv1.Job{},
		types.NamespacedName{Name: util.DataLoadJobName(cluster.Name, "loader"), Namespace: ns})
	assertNotFound(t, k8sClient, &batchv1.CronJob{},
		types.NamespacedName{Name: util.DataLoadJobName(cluster.Name, "nightly"), Namespace: ns})
	assertNotFound(t, k8sClient, &corev1.ConfigMap{},
		types.NamespacedName{Name: util.GploadControlFileConfigMapName(cluster.Name, "loader"), Namespace: ns})
	assertNotFound(t, k8sClient, &networkingv1.NetworkPolicy{},
		types.NamespacedName{Name: util.PxfNetworkPolicyName(cluster.Name), Namespace: ns})

	// In-memory status cleared back to nil; mirror count zeroed.
	assert.Nil(t, cluster.Status.DataLoading)
	assert.Equal(t, int32(0), cluster.Status.DataLoadingJobs)

	// Gauges zeroed (jobs-active=0 AND pxf-servers=0).
	require.NotEmpty(t, spy.jobsActive)
	assert.Equal(t, 0.0, spy.jobsActive[len(spy.jobsActive)-1])
	require.NotEmpty(t, spy.pxfServers)
	assert.Equal(t, 0.0, spy.pxfServers[len(spy.pxfServers)-1])

	// Exactly one DataLoadingDisabled event on the transition.
	assert.Equal(t, 1, countDataLoadingDisabledEvents(drainEvents(recorder)))
}

// 112-DIS1 condition: cleanupDataLoading sets the DataLoadingConfigured
// condition to False with reason DataLoadingDisabled in memory before it
// persists the status. Because patchDataLoadingStatus only carries the
// dataLoading sub-object (not conditions) and the fake client round-trips the
// server object back into `cluster` on Status().Patch (clearing the in-memory
// conditions), the condition is captured via a SubResourcePatch interceptor at
// patch time — the moment it is authoritative.
func TestCleanupDataLoading_SetsConditionFalseDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-cond"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured", ActiveJobs: 1}
	cluster.Status.DataLoadingJobs = 1

	var capturedStatus metav1.ConditionStatus
	var capturedReason string
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context, c client.Client, subResourceName string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
			) error {
				if cc, ok := obj.(*cbv1alpha1.CloudberryCluster); ok {
					if cond := util.FindCondition(cc.Status.Conditions,
						string(cbv1alpha1.ConditionDataLoadingConfigured)); cond != nil {
						capturedStatus = cond.Status
						capturedReason = cond.Reason
					}
				}
				return c.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))

	assert.Equal(t, metav1.ConditionFalse, capturedStatus)
	assert.Equal(t, "DataLoadingDisabled", capturedReason)
}

// 112-DIS1-TEARDOWN (U) de-dup: a SECOND disabled reconcile (status already
// cleared) must NOT re-emit the DataLoadingDisabled event — the event is a
// one-shot transition signal.
func TestCleanupDataLoading_EventNotReEmittedOnSecondReconcile(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-dedup"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured", ActiveJobs: 1}
	cluster.Status.DataLoadingJobs = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// First reconcile: transition => one event.
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	assert.Equal(t, 1, countDataLoadingDisabledEvents(drainEvents(recorder)))

	// Second reconcile: status already cleared (transitioning=false) => no event.
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	assert.Equal(t, 0, countDataLoadingDisabledEvents(drainEvents(recorder)),
		"DataLoadingDisabled event must NOT be re-emitted on a steady disabled reconcile")
}

// 112-DIS1-U-LABELSCOPE: only THIS cluster's ComponentDataLoad objects are
// deleted; a foreign cluster's dataload objects AND a same-cluster non-dataload
// ConfigMap are left untouched (label + cluster scoping).
func TestDeleteDataLoadingWorkloads_LabelScopedOnly(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-scope"
	ns := cluster.Namespace

	mineJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: util.DataLoadJobName(cluster.Name, "mine"), Namespace: ns,
		Labels: dataLoadLabelSet(cluster.Name)}}
	mineCron := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
		Name: util.DataLoadJobName(cluster.Name, "mine-cron"), Namespace: ns,
		Labels: dataLoadLabelSet(cluster.Name)}}
	// Foreign cluster's dataload Job (same component, different cluster).
	foreignJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "other-dataload-x", Namespace: ns, Labels: dataLoadLabelSet("other-cluster")}}
	// Same cluster, different component.
	otherComponentJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "s112-scope-backup", Namespace: ns, Labels: map[string]string{
			util.LabelCluster: cluster.Name, util.LabelComponent: "backup"}}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mineJob, mineCron, foreignJob, otherComponentJob).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	r.deleteDataLoadingWorkloads(context.Background(), cluster)

	// Mine: deleted.
	assertNotFound(t, k8sClient, &batchv1.Job{},
		types.NamespacedName{Name: mineJob.Name, Namespace: ns})
	assertNotFound(t, k8sClient, &batchv1.CronJob{},
		types.NamespacedName{Name: mineCron.Name, Namespace: ns})

	// Foreign cluster + other-component: untouched.
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: foreignJob.Name, Namespace: ns}, &batchv1.Job{}))
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: otherComponentJob.Name, Namespace: ns}, &batchv1.Job{}))
}

// 112-DIS1 label-scope (control-file ConfigMaps): the gpload control-file
// ConfigMaps for this cluster are deleted; a foreign cluster's control-file CM
// is untouched.
func TestDeleteGploadControlFileConfigMaps_LabelScopedOnly(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-cm"
	ns := cluster.Namespace

	mineCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: util.GploadControlFileConfigMapName(cluster.Name, "j1"), Namespace: ns,
		Labels: dataLoadLabelSet(cluster.Name)}}
	foreignCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "other-gpload-j1", Namespace: ns, Labels: dataLoadLabelSet("other-cluster")}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, mineCM, foreignCM).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	r.deleteGploadControlFileConfigMaps(context.Background(), cluster)

	assertNotFound(t, k8sClient, &corev1.ConfigMap{},
		types.NamespacedName{Name: mineCM.Name, Namespace: ns})
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: foreignCM.Name, Namespace: ns}, &corev1.ConfigMap{}))
}

// 112-DIS1 NotFound-tolerance: cleanupDataLoading on a disabled cluster with NO
// stale objects present must NOT error (every delete is best-effort) and still
// drives the status/condition path.
func TestCleanupDataLoading_NoStaleObjects_NoError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-empty"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// Absent objects => no error, no panic.
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	assert.Nil(t, cluster.Status.DataLoading)
	assert.Equal(t, int32(0), cluster.Status.DataLoadingJobs)
}

// 112-DIS1-NoTransitionNoEvent: when the cluster has been disabled all along
// (status absent => transitioning=false), NO DataLoadingDisabled event fires on
// the very first reconcile — the event only marks the enabled→disabled
// transition, not a never-enabled cluster.
func TestCleanupDataLoading_NeverEnabled_NoEvent(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-never"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = nil // never enabled
	// No Status.DataLoading and DataLoadingJobs==0 => transitioning=false.

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := NewAdminReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	assert.Equal(t, 0, countDataLoadingDisabledEvents(drainEvents(recorder)),
		"a never-enabled cluster must not emit a DataLoadingDisabled event")
}

// 112-DIS1-ENABLED-UNCHANGED: with dataLoading.enabled=true, the normal
// reconcile body runs (cleanupDataLoading is NOT invoked): pre-seeded stale
// objects are NOT spuriously deleted and the enabled jobs are built. This proves
// the teardown runs ONLY on the disabled path.
func TestReconcileDataLoading_EnabledPathDoesNotTeardown(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-enabled"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs:    []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)},
	}

	// Pre-seed a foreign-but-dataload-labeled CM that must NOT be deleted on the
	// enabled path (it is only deleted by the disabled teardown).
	staleCM := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "preexisting-dataload-cm", Namespace: cluster.Namespace,
		Labels: dataLoadLabelSet(cluster.Name)}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, staleCM).
		WithStatusSubresource(cluster).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))

	// Enabled path: status configured, the enabled job was built.
	require.NotNil(t, cluster.Status.DataLoading)
	assert.Equal(t, "Configured", cluster.Status.DataLoading.Phase)
	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "loader"), Namespace: cluster.Namespace}, job))

	// The pre-seeded dataload-labeled CM is NOT deleted on the enabled path.
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: staleCM.Name, Namespace: cluster.Namespace}, &corev1.ConfigMap{}))
}

// 112-DIS1 status-persist failure: when the final patchDataLoadingStatus fails,
// cleanupDataLoading wraps and returns the error (the teardown deletes succeed
// best-effort, but a failed status persist is reported so the reconcile retries).
func TestCleanupDataLoading_PatchStatusErrorPropagates(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-patcherr"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured", ActiveJobs: 1}
	cluster.Status.DataLoadingJobs = 1

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				_ context.Context, _ client.Client, _ string,
				_ client.Object, _ client.Patch, _ ...client.SubResourcePatchOption,
			) error {
				return assertErrPatch
			},
		}).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	err := r.reconcileDataLoading(context.Background(), cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "patching cleared data loading status")
}

// 112-DIS1 best-effort (List failure): when the JobList/CronJob/ConfigMap LIST
// itself fails, deleteDataLoadingWorkloads / deleteGploadControlFileConfigMaps
// log a warning and return WITHOUT failing the caller — the cleanup is
// best-effort and a list error must never abort the reconcile.
func TestDeleteDataLoadingWorkloads_ListErrorIsNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-listerr"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				_ context.Context, _ client.WithWatch, list client.ObjectList, _ ...client.ListOption,
			) error {
				return assertErrList
			},
		}).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		r.deleteDataLoadingWorkloads(context.Background(), cluster)
		r.deleteGploadControlFileConfigMaps(context.Background(), cluster)
	})
}

// 112-DIS1 best-effort (Delete failure): when the individual Job/CronJob/CM/
// gpfdist Delete returns a NON-NotFound error, the helpers log a warning and
// keep going (best-effort) — they never panic or propagate the error.
func TestDeleteHelpers_DeleteErrorIsNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-delerr"
	ns := cluster.Namespace
	labels := dataLoadLabelSet(cluster.Name)

	objs := []client.Object{
		cluster,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Name: util.SanitizeK8sName(cluster.Name + "-gpfdist"), Namespace: ns}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{
			Name: util.DataLoadJobName(cluster.Name, "j"), Namespace: ns, Labels: labels}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{
			Name: util.DataLoadJobName(cluster.Name, "c"), Namespace: ns, Labels: labels}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: util.GploadControlFileConfigMapName(cluster.Name, "j"), Namespace: ns, Labels: labels}},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(
				_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption,
			) error {
				return assertErrDelete
			},
		}).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NotPanics(t, func() {
		r.deleteGpfdistResources(context.Background(), cluster)
		r.deleteDataLoadingWorkloads(context.Background(), cluster)
		r.deleteGploadControlFileConfigMaps(context.Background(), cluster)
	})
}

// 112-DIS1/DIS2 best-effort (CM delete failure): deletePxfServersConfigMap logs
// a warning and returns nil on a non-NotFound delete error (never fails the
// cluster reconcile).
func TestDeletePxfServersConfigMap_DeleteErrorIsNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-cmdelerr"
	stale := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: builder.PxfServersConfigMapName(cluster.Name), Namespace: cluster.Namespace}}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, stale).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(
				_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption,
			) error {
				return assertErrDelete
			},
		}).
		Build()

	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.deletePxfServersConfigMap(context.Background(), cluster))
}

// 112-DIS1-REENABLE (idempotency): after a disabled teardown, flipping the spec
// back to enabled and reconciling rebuilds the enabled jobs (no error, the
// get-or-create path redeploys).
func TestReconcileDataLoading_ReEnableRebuildsJobs(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Name = "s112-reenable"
	cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	// 1. Disabled teardown.
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured", ActiveJobs: 1}
	cluster.Status.DataLoadingJobs = 1
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))
	assert.Nil(t, cluster.Status.DataLoading)

	// 2. Re-enable: the normal reconcile rebuilds.
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs:    []cbv1alpha1.DataLoadingJob{dlPxfJob("loader", true)},
	}
	require.NoError(t, r.reconcileDataLoading(context.Background(), cluster))

	require.NotNil(t, cluster.Status.DataLoading)
	assert.Equal(t, "Configured", cluster.Status.DataLoading.Phase)
	job := &batchv1.Job{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name: util.DataLoadJobName(cluster.Name, "loader"), Namespace: cluster.Namespace}, job))
}

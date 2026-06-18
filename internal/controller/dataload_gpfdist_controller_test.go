package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// gpfdistReconcileCall captures a single RecordGpfdistReconcile invocation so a
// controller-level test can assert the {operation,result} label pair recorded at
// the real call site (ensureGpfdistPVC/Deployment/Service + deleteGpfdistResources).
type gpfdistReconcileCall struct {
	cluster   string
	namespace string
	operation string
	result    string
}

// pxfSetupCall captures a single RecordPXFExtensionSetup invocation so a
// controller-level test can assert the result label recorded at setupPXFExtensions.
type pxfSetupCall struct {
	cluster   string
	namespace string
	result    string
}

// dataLoadCapturingRecorder is the shared capturing metrics recorder for the
// gpfdist + PXF families (S-1). It overrides ONLY RecordGpfdistReconcile and
// RecordPXFExtensionSetup over an embedded NoopRecorder, so the controller tests
// (T2–T7) can assert the new B-1/B-2 counters at their real call sites instead of
// the recorder unit level. It mirrors the existing dataLoadMetricsRecorder /
// wave345Recorder capturing pattern.
type dataLoadCapturingRecorder struct {
	metrics.NoopRecorder
	gpfdistCalls []gpfdistReconcileCall
	pxfCalls     []pxfSetupCall
}

func (r *dataLoadCapturingRecorder) RecordGpfdistReconcile(cluster, namespace, operation, result string) {
	r.gpfdistCalls = append(r.gpfdistCalls, gpfdistReconcileCall{
		cluster: cluster, namespace: namespace, operation: operation, result: result,
	})
}

func (r *dataLoadCapturingRecorder) RecordPXFExtensionSetup(cluster, namespace, result string) {
	r.pxfCalls = append(r.pxfCalls, pxfSetupCall{
		cluster: cluster, namespace: namespace, result: result,
	})
}

// gpfdistResults returns the captured {operation->result} pairs for assertions.
func (r *dataLoadCapturingRecorder) gpfdistResult(operation string) (string, bool) {
	for _, c := range r.gpfdistCalls {
		if c.operation == operation {
			return c.result, true
		}
	}
	return "", false
}

// assertGpfdistBoom is the sentinel injected into a gpfdist object write so the
// controller-level error-path tests can prove the real cause propagates.
var assertGpfdistBoom = errors.New("gpfdist write boom")

// gpfdistCluster builds a cluster with a data-loading spec whose gpfdist
// sub-spec enabled flag is controlled by the caller. It mirrors dlCluster but is
// focused on the gpfdist file-server reconcile (PVC + Deployment + Service).
func gpfdistCluster(name string, gpfdistEnabled bool) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Name = name
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: gpfdistEnabled},
	}
	return c
}

// gpfdistReconciler builds an AdminReconciler over a fake client seeded with the
// cluster, mirroring dlReconciler's fake-client + scheme + builder construction.
func gpfdistReconciler(cluster *cbv1alpha1.CloudberryCluster) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	return NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
}

// gpfdistKeys returns the namespaced names of the three gpfdist objects for a
// cluster (PVC, Deployment, Service).
func gpfdistKeys(cluster *cbv1alpha1.CloudberryCluster) (pvc, dep, svc types.NamespacedName) {
	ns := cluster.Namespace
	pvc = types.NamespacedName{Name: util.GpfdistDataPVCName(cluster.Name), Namespace: ns}
	dep = types.NamespacedName{Name: util.SanitizeK8sName(cluster.Name + "-gpfdist"), Namespace: ns}
	svc = types.NamespacedName{Name: util.GpfdistServiceName2(cluster.Name), Namespace: ns}
	return pvc, dep, svc
}

// TestReconcileGpfdist_EnableCreatesResources proves the ENABLE path ensures the
// PVC, Deployment and Service (with the expected names + cluster ownerRef),
// covering ensureGpfdistPVC/Deployment/Service create branches.
func TestReconcileGpfdist_EnableCreatesResources(t *testing.T) {
	cluster := gpfdistCluster("gpf-enable", true)
	r := gpfdistReconciler(cluster)
	ctx := context.Background()

	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	pvcKey, depKey, svcKey := gpfdistKeys(cluster)

	pvc := &corev1.PersistentVolumeClaim{}
	require.NoError(t, r.client.Get(ctx, pvcKey, pvc))
	assert.Equal(t, util.GpfdistDataPVCName(cluster.Name), pvc.Name)
	require.Len(t, pvc.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, pvc.OwnerReferences[0].Name)

	dep := &appsv1.Deployment{}
	require.NoError(t, r.client.Get(ctx, depKey, dep))
	assert.Equal(t, util.SanitizeK8sName(cluster.Name+"-gpfdist"), dep.Name)
	require.Len(t, dep.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, dep.OwnerReferences[0].Name)

	svc := &corev1.Service{}
	require.NoError(t, r.client.Get(ctx, svcKey, svc))
	assert.Equal(t, util.GpfdistServiceName2(cluster.Name), svc.Name)
	require.Len(t, svc.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, svc.OwnerReferences[0].Name)
}

// TestReconcileGpfdist_Idempotent proves a second reconcile is a no-op error-wise
// and the objects remain present, exercising the CreateOrUpdate update branch of
// ensureGpfdistDeployment/Service (PVC is left untouched when present).
func TestReconcileGpfdist_Idempotent(t *testing.T) {
	cluster := gpfdistCluster("gpf-idem", true)
	r := gpfdistReconciler(cluster)
	ctx := context.Background()

	require.NoError(t, r.reconcileGpfdist(ctx, cluster))
	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	pvcKey, depKey, svcKey := gpfdistKeys(cluster)
	require.NoError(t, r.client.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
	require.NoError(t, r.client.Get(ctx, depKey, &appsv1.Deployment{}))
	require.NoError(t, r.client.Get(ctx, svcKey, &corev1.Service{}))

	// Still exactly one of each (no duplicates).
	deps := &appsv1.DeploymentList{}
	require.NoError(t, r.client.List(ctx, deps))
	assert.Len(t, deps.Items, 1)
	svcs := &corev1.ServiceList{}
	require.NoError(t, r.client.List(ctx, svcs))
	assert.Len(t, svcs.Items, 1)
}

// TestReconcileGpfdist_DisableGCsResources proves the DISABLE path runs
// deleteGpfdistResources: after enabling then flipping gpfdist.enabled:false, a
// reconcile removes the PVC/Deployment/Service (Get returns NotFound).
func TestReconcileGpfdist_DisableGCsResources(t *testing.T) {
	cluster := gpfdistCluster("gpf-disable", true)
	r := gpfdistReconciler(cluster)
	ctx := context.Background()

	// Enable first so the three objects exist.
	require.NoError(t, r.reconcileGpfdist(ctx, cluster))
	pvcKey, depKey, svcKey := gpfdistKeys(cluster)
	require.NoError(t, r.client.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{}))
	require.NoError(t, r.client.Get(ctx, depKey, &appsv1.Deployment{}))
	require.NoError(t, r.client.Get(ctx, svcKey, &corev1.Service{}))

	// Flip gpfdist off and reconcile: the GC path must delete all three.
	cluster.Spec.DataLoading.Gpfdist.Enabled = false
	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{})))
	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, depKey, &appsv1.Deployment{})))
	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, svcKey, &corev1.Service{})))
}

// TestReconcileGpfdist_DisableNoExistingResources proves the GC path is a clean
// best-effort no-op when the gpfdist objects do not exist (NotFound ignored).
func TestReconcileGpfdist_DisableNoExistingResources(t *testing.T) {
	cluster := gpfdistCluster("gpf-disable-empty", false)
	r := gpfdistReconciler(cluster)
	ctx := context.Background()

	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	pvcKey, depKey, svcKey := gpfdistKeys(cluster)
	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{})))
	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, depKey, &appsv1.Deployment{})))
	assert.True(t, apierrors.IsNotFound(
		r.client.Get(ctx, svcKey, &corev1.Service{})))
}

// TestReconcileGpfdist_NoOpWhenDataLoadingOrGpfdistNil proves the disabled branch
// creates no objects when DataLoading is nil or the Gpfdist sub-spec is nil.
func TestReconcileGpfdist_NoOpWhenDataLoadingOrGpfdistNil(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *cbv1alpha1.CloudberryCluster)
	}{
		{
			name: "DataLoading nil",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.DataLoading = nil
			},
		},
		{
			name: "Gpfdist nil",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true, Gpfdist: nil}
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Name = "gpf-noop"
			cluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning
			tc.mutate(cluster)
			r := gpfdistReconciler(cluster)
			ctx := context.Background()

			require.NoError(t, r.reconcileGpfdist(ctx, cluster))

			pvcKey, depKey, svcKey := gpfdistKeys(cluster)
			assert.True(t, apierrors.IsNotFound(
				r.client.Get(ctx, pvcKey, &corev1.PersistentVolumeClaim{})))
			assert.True(t, apierrors.IsNotFound(
				r.client.Get(ctx, depKey, &appsv1.Deployment{})))
			assert.True(t, apierrors.IsNotFound(
				r.client.Get(ctx, svcKey, &corev1.Service{})))
		})
	}
}

// TestEnsureGploadControlFileConfigMap_CreateAndUpdate proves a gpload job's
// control-file ConfigMap is created with the "<job>.yml" data key, and that a
// second call exercises the update path (idempotent, single ConfigMap).
func TestEnsureGploadControlFileConfigMap_CreateAndUpdate(t *testing.T) {
	job := dlGploadJob("bulk", "", true)
	cluster := dlCluster("gpload-cm", []cbv1alpha1.DataLoadingJob{job})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})
	ctx := context.Background()

	// First call creates the ConfigMap.
	require.NoError(t, r.ensureGploadControlFileConfigMap(ctx, cluster, job))

	cmKey := types.NamespacedName{
		Name:      util.GploadControlFileConfigMapName(cluster.Name, job.Name),
		Namespace: cluster.Namespace,
	}
	cm := &corev1.ConfigMap{}
	require.NoError(t, r.client.Get(ctx, cmKey, cm))
	dataKey := util.SanitizeK8sName(job.Name) + ".yml"
	require.Contains(t, cm.Data, dataKey)
	assert.NotEmpty(t, cm.Data[dataKey], "rendered control file must be non-empty")
	require.Len(t, cm.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, cm.OwnerReferences[0].Name)

	// Second call exercises the update branch and stays idempotent.
	require.NoError(t, r.ensureGploadControlFileConfigMap(ctx, cluster, job))
	cms := &corev1.ConfigMapList{}
	require.NoError(t, r.client.List(ctx, cms))
	count := 0
	for i := range cms.Items {
		if cms.Items[i].Name == cmKey.Name {
			count++
		}
	}
	assert.Equal(t, 1, count, "ensure must not duplicate the gpload control-file ConfigMap")
}

// TestEnsureGploadControlFileConfigMap_ViaEnsureDataLoadingWorkloads drives the
// ConfigMap creation through the higher-level ensureDataLoadingWorkloads path
// (the production caller) for an enabled gpload job.
func TestEnsureGploadControlFileConfigMap_ViaEnsureDataLoadingWorkloads(t *testing.T) {
	job := dlGploadJob("nightly", "0 3 * * *", true)
	cluster := dlCluster("gpload-wl", []cbv1alpha1.DataLoadingJob{job})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})
	ctx := context.Background()

	require.NoError(t, r.ensureDataLoadingWorkloads(ctx, cluster))

	cm := &corev1.ConfigMap{}
	require.NoError(t, r.client.Get(ctx, types.NamespacedName{
		Name:      util.GploadControlFileConfigMapName(cluster.Name, job.Name),
		Namespace: cluster.Namespace,
	}, cm))
	dataKey := util.SanitizeK8sName(job.Name) + ".yml"
	require.Contains(t, cm.Data, dataKey)
	assert.NotEmpty(t, cm.Data[dataKey])
}

// TestEnsureGploadControlFileConfigMap_MisconfiguredSkipped proves a gpload job
// whose control file cannot be rendered (nil GploadJob) is skipped (logged), not
// errored, and creates no ConfigMap.
func TestEnsureGploadControlFileConfigMap_MisconfiguredSkipped(t *testing.T) {
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "gpload", Enabled: true} // nil GploadJob
	cluster := dlCluster("gpload-bad", []cbv1alpha1.DataLoadingJob{job})
	r := dlReconciler(cluster, &metrics.NoopRecorder{})
	ctx := context.Background()

	require.NoError(t, r.ensureGploadControlFileConfigMap(ctx, cluster, job))

	cms := &corev1.ConfigMapList{}
	require.NoError(t, r.client.List(ctx, cms))
	for i := range cms.Items {
		assert.NotEqual(t, util.GploadControlFileConfigMapName(cluster.Name, job.Name),
			cms.Items[i].Name, "mis-configured gpload job must create no ConfigMap")
	}
}

// gpfdistReconcilerWith builds an AdminReconciler over a fake client seeded with
// the cluster, using the supplied capturing recorder and optional interceptor
// funcs so the controller-level metric / error-path tests (T2–T4) can both force
// K8s write errors AND assert the new gpfdist counter at its real call site.
func gpfdistReconcilerWith(
	cluster *cbv1alpha1.CloudberryCluster,
	rec metrics.Recorder,
	funcs interceptor.Funcs,
) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).
		WithInterceptorFuncs(funcs).Build()
	return NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, rec, nil)
}

// TestReconcileGpfdist_CreateError_RecordsErrorMetric drives the CREATE-error
// path of ensureGpfdistPVC/Deployment/Service (T2): an interceptor returns boom
// for the target object type, gpfdist is ENABLED, and a capturing recorder
// asserts RecordGpfdistReconcile(operation, result="error") is recorded at the
// real call site while reconcileGpfdist propagates the wrapped error. Covers the
// create-error returns AND reconcileGpfdist's propagation lines (B-1 controller
// verification + the previously-uncovered ensureGpfdist* create-error branches).
func TestReconcileGpfdist_CreateError_RecordsErrorMetric(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		failsOn   func(obj client.Object) bool
	}{
		{
			name:      "pvc create error",
			operation: gpfdistOpPVC,
			failsOn: func(obj client.Object) bool {
				_, ok := obj.(*corev1.PersistentVolumeClaim)
				return ok
			},
		},
		{
			name:      "deployment create error",
			operation: gpfdistOpDeployment,
			failsOn: func(obj client.Object) bool {
				_, ok := obj.(*appsv1.Deployment)
				return ok
			},
		},
		{
			name:      "service create error",
			operation: gpfdistOpService,
			failsOn: func(obj client.Object) bool {
				_, ok := obj.(*corev1.Service)
				return ok
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := gpfdistCluster("gpf-createerr", true)
			rec := &dataLoadCapturingRecorder{}
			r := gpfdistReconcilerWith(cluster, rec, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object,
					opts ...client.CreateOption) error {
					if tc.failsOn(obj) {
						return assertGpfdistBoom
					}
					return c.Create(ctx, obj, opts...)
				},
			})

			err := r.reconcileGpfdist(context.Background(), cluster)
			require.Error(t, err, "the create error must propagate out of reconcileGpfdist")
			assert.ErrorIs(t, err, assertGpfdistBoom)

			result, found := rec.gpfdistResult(tc.operation)
			require.True(t, found,
				"RecordGpfdistReconcile must be called for operation %q", tc.operation)
			assert.Equal(t, metricResultError, result,
				"the failed %q write must record result=error", tc.operation)
			// Labels are honest: cluster + namespace match the reconciled object.
			require.NotEmpty(t, rec.gpfdistCalls)
			last := rec.gpfdistCalls[len(rec.gpfdistCalls)-1]
			assert.Equal(t, cluster.Name, last.cluster)
			assert.Equal(t, cluster.Namespace, last.namespace)
		})
	}
}

// TestReconcileGpfdist_UpdateError_RecordsErrorMetric drives the UPDATE-error
// path of ensureGpfdistService / ensureGpfdistDeployment (T3): the gpfdist
// objects are pre-created by a first clean reconcile, then an interceptor returns
// boom on Update for the target object type and a second reconcile runs. Asserts
// RecordGpfdistReconcile(operation, result="error") and the propagated error
// (covers ensureGpfdistService update-error L882-884, previously uncovered).
func TestReconcileGpfdist_UpdateError_RecordsErrorMetric(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		failsOn   func(obj client.Object) bool
	}{
		{
			name:      "service update error",
			operation: gpfdistOpService,
			failsOn: func(obj client.Object) bool {
				_, ok := obj.(*corev1.Service)
				return ok
			},
		},
		{
			name:      "deployment update error",
			operation: gpfdistOpDeployment,
			failsOn: func(obj client.Object) bool {
				_, ok := obj.(*appsv1.Deployment)
				return ok
			},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := gpfdistCluster("gpf-updateerr", true)
			rec := &dataLoadCapturingRecorder{}
			var failUpdate bool
			r := gpfdistReconcilerWith(cluster, rec, interceptor.Funcs{
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object,
					opts ...client.UpdateOption) error {
					if failUpdate && tc.failsOn(obj) {
						return assertGpfdistBoom
					}
					return c.Update(ctx, obj, opts...)
				},
			})

			// First reconcile: clean create of all three objects (no failure yet).
			require.NoError(t, r.reconcileGpfdist(context.Background(), cluster))

			// Second reconcile: the update of the target object now fails.
			failUpdate = true
			rec.gpfdistCalls = nil // isolate the update pass.
			err := r.reconcileGpfdist(context.Background(), cluster)
			require.Error(t, err, "the update error must propagate out of reconcileGpfdist")
			assert.ErrorIs(t, err, assertGpfdistBoom)

			result, found := rec.gpfdistResult(tc.operation)
			require.True(t, found,
				"RecordGpfdistReconcile must be called for operation %q on update", tc.operation)
			assert.Equal(t, metricResultError, result,
				"the failed %q update must record result=error", tc.operation)
		})
	}
}

// TestReconcileGpfdist_Success_RecordsSuccessMetrics asserts the B-1 counter at
// its real call sites on the SUCCESS paths (T4): (a) an enabled clean reconcile
// records result="success" for operations pvc, deployment and service; (b)
// flipping gpfdist.enabled:false records result="success" for the delete op. It
// also checks the cluster+namespace labels are honest.
func TestReconcileGpfdist_Success_RecordsSuccessMetrics(t *testing.T) {
	cluster := gpfdistCluster("gpf-success", true)
	rec := &dataLoadCapturingRecorder{}
	r := gpfdistReconcilerWith(cluster, rec, interceptor.Funcs{})
	ctx := context.Background()

	// (a) Enable: PVC + Deployment + Service created → three success records.
	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	for _, op := range []string{gpfdistOpPVC, gpfdistOpDeployment, gpfdistOpService} {
		result, found := rec.gpfdistResult(op)
		require.True(t, found, "RecordGpfdistReconcile must fire for operation %q", op)
		assert.Equal(t, metricResultSuccess, result,
			"a clean %q create must record result=success", op)
	}
	// Labels are honest across the captured calls.
	for _, c := range rec.gpfdistCalls {
		assert.Equal(t, cluster.Name, c.cluster)
		assert.Equal(t, cluster.Namespace, c.namespace)
	}

	// (b) Disable: deleteGpfdistResources removes the three objects → a delete
	// success record (a real delete write, not a NotFound no-op).
	rec.gpfdistCalls = nil
	cluster.Spec.DataLoading.Gpfdist.Enabled = false
	require.NoError(t, r.reconcileGpfdist(ctx, cluster))

	result, found := rec.gpfdistResult(gpfdistOpDelete)
	require.True(t, found, "RecordGpfdistReconcile must fire for the delete operation")
	assert.Equal(t, metricResultSuccess, result,
		"a successful delete write must record result=success")
}

// TestReconcileGpfdist_CreateRace_RecordsSuccess covers the IsAlreadyExists
// create-RACE success branch of ensureGpfdistPVC/Deployment/Service (S-2): an
// interceptor returns AlreadyExists on Create so the no-op-win path runs. The
// reconcile must succeed and the metric must record result="success" (a race is
// a benign win, not an error).
func TestReconcileGpfdist_CreateRace_RecordsSuccess(t *testing.T) {
	cluster := gpfdistCluster("gpf-race", true)
	rec := &dataLoadCapturingRecorder{}
	r := gpfdistReconcilerWith(cluster, rec, interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object,
			_ ...client.CreateOption) error {
			// Simulate a concurrent create having already won the race.
			return apierrors.NewAlreadyExists(
				appsv1.Resource(obj.GetObjectKind().GroupVersionKind().Kind), obj.GetName())
		},
	})

	// All three ensure* see AlreadyExists on Create → no-op success, no error.
	require.NoError(t, r.reconcileGpfdist(context.Background(), cluster))

	for _, op := range []string{gpfdistOpPVC, gpfdistOpDeployment, gpfdistOpService} {
		result, found := rec.gpfdistResult(op)
		require.True(t, found, "RecordGpfdistReconcile must fire for operation %q", op)
		assert.Equal(t, metricResultSuccess, result,
			"an AlreadyExists create-race for %q must record result=success", op)
	}
}

// TestReconcileGpfdist_GetError_Propagates covers the non-NotFound Get error
// return of ensureGpfdistPVC (L805-806): an interceptor returns boom on Get for
// the PVC so the get-error branch runs and reconcileGpfdist propagates it. No
// metric is recorded on a Get failure (the write never happened).
func TestReconcileGpfdist_GetError_Propagates(t *testing.T) {
	cluster := gpfdistCluster("gpf-geterr", true)
	rec := &dataLoadCapturingRecorder{}
	r := gpfdistReconcilerWith(cluster, rec, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return assertGpfdistBoom
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})

	err := r.reconcileGpfdist(context.Background(), cluster)
	require.Error(t, err)
	assert.ErrorIs(t, err, assertGpfdistBoom)
	// A Get failure is not a write outcome, so no PVC metric is recorded.
	_, found := rec.gpfdistResult(gpfdistOpPVC)
	assert.False(t, found, "a Get error must not record a write-outcome metric")
}

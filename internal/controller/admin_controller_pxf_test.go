package controller

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// pxfObservabilityRecorder captures the HONEST observability gauge calls
// (SetPXFStatus / SetPXFExtensionsInstalled) so the PXF reconcile tests can
// assert they are emitted ONLY when the underlying signal is observable. It
// embeds NoopRecorder so all other Recorder methods are no-ops.
type pxfObservabilityRecorder struct {
	metrics.NoopRecorder
	statusCalls     int
	lastStatusValue float64
	extCalls        int
	lastExtCount    float64
}

func (m *pxfObservabilityRecorder) SetPXFStatus(_, _ string, value float64) {
	m.statusCalls++
	m.lastStatusValue = value
}

func (m *pxfObservabilityRecorder) SetPXFExtensionsInstalled(_, _ string, count float64) {
	m.extCalls++
	m.lastExtCount = count
}

// pxfSegmentPrimaryPod builds a segment-primary pod (with the SHARED selector
// labels) whose "pxf" container has the given readiness. When hasPXF is false
// the pod carries no "pxf" container status (counts toward total, never ready).
func pxfSegmentPrimaryPod(name, cluster, namespace string, ready, hasPXF bool) *corev1.Pod {
	statuses := []corev1.ContainerStatus{{Name: "segment", Ready: true}}
	if hasPXF {
		statuses = append(statuses, corev1.ContainerStatus{Name: util.PXFContainerName, Ready: ready})
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelCluster:   cluster,
				util.LabelComponent: util.ComponentSegmentPrimary,
			},
		},
		Status: corev1.PodStatus{ContainerStatuses: statuses},
	}
}

// ---------------------------------------------------------------------------
// S.1 — pxf.status from real segment-primary "pxf" container readiness
// ---------------------------------------------------------------------------

// TestReconcilePxf_StatusFromReadiness drives reconcilePxf with a fake client
// returning segment-primary pods of varying "pxf" readiness and asserts the
// HONEST status mapping is stamped onto Status.DataLoading.Pxf.Status.
//
// Covers 105-S1-B1 (all ready → "Running"), 105-S1-B2 (partial → "Error"),
// 105-S1-B3 (none ready → "Stopped") and 105-S1-B4 (no pods → ABSENT "").
func TestReconcilePxf_StatusFromReadiness(t *testing.T) {
	tests := []struct {
		name        string
		pods        []*corev1.Pod
		wantStatus  string
		wantMetric  bool
		metricValue float64
	}{
		{
			name: "all pxf ready → Running (105-S1-B1)",
			pods: []*corev1.Pod{
				pxfSegmentPrimaryPod("seg-0", "test-cluster", "default", true, true),
				pxfSegmentPrimaryPod("seg-1", "test-cluster", "default", true, true),
			},
			wantStatus:  util.PXFStatusRunning,
			wantMetric:  true,
			metricValue: 1,
		},
		{
			name: "partial readiness → Error (105-S1-B2)",
			pods: []*corev1.Pod{
				pxfSegmentPrimaryPod("seg-0", "test-cluster", "default", true, true),
				pxfSegmentPrimaryPod("seg-1", "test-cluster", "default", false, true),
			},
			wantStatus:  util.PXFStatusError,
			wantMetric:  true,
			metricValue: 2,
		},
		{
			name: "none ready → Stopped (105-S1-B3)",
			pods: []*corev1.Pod{
				pxfSegmentPrimaryPod("seg-0", "test-cluster", "default", false, true),
				pxfSegmentPrimaryPod("seg-1", "test-cluster", "default", false, true),
			},
			wantStatus:  util.PXFStatusStopped,
			wantMetric:  true,
			metricValue: 0,
		},
		{
			name:       "no segment-primary pods → Status ABSENT (105-S1-B4)",
			pods:       nil,
			wantStatus: "",
			wantMetric: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newPXFDataLoadingCluster()
			cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

			objs := []client.Object{cluster}
			for _, p := range tt.pods {
				objs = append(objs, p)
			}
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(cluster).
				Build()
			b := builder.NewBuilder()
			m := &pxfObservabilityRecorder{}
			// No dbFactory → extensions probe stays absent; this isolates status.
			r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10), b, nil, m, nil)

			r.reconcilePxf(context.Background(), cluster)

			require.NotNil(t, cluster.Status.DataLoading.Pxf)
			assert.Equal(t, tt.wantStatus, cluster.Status.DataLoading.Pxf.Status)

			// 105-MX-B1: the status gauge is emitted ONLY when observable.
			if tt.wantMetric {
				assert.Equal(t, 1, m.statusCalls, "status gauge must be emitted when observable")
				assert.Equal(t, tt.metricValue, m.lastStatusValue)
			} else {
				assert.Zero(t, m.statusCalls,
					"status gauge must NOT be emitted when status is absent/unobservable")
			}
		})
	}
}

// TestReconcilePxf_StatusAbsent_NoPxfContainer covers 105-S1-B4: segment-primary
// pods that exist but carry NO "pxf" container status are observed (total>0,
// ready==0) → the honest mapping is "Stopped", never a synthesized "Running".
func TestReconcilePxf_StatusAbsent_NoPxfContainer(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	pod := pxfSegmentPrimaryPod("seg-0", "test-cluster", "default", false, false)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, pod).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	r.reconcilePxf(context.Background(), cluster)

	require.NotNil(t, cluster.Status.DataLoading.Pxf)
	// total==1, ready==0 → Stopped (honest; the pxf agent is not observably up).
	assert.Equal(t, util.PXFStatusStopped, cluster.Status.DataLoading.Pxf.Status)
}

// TestReconcilePxf_StatusAbsent_ListError covers 105-S1-B4 non-fatal contract:
// when listing the segment-primary pods FAILS, observePxfStatus leaves the
// Status ABSENT ("") and reconcile still succeeds — a list error never blocks
// reconcile and never fabricates a status.
func TestReconcilePxf_StatusAbsent_ListError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(
				ctx context.Context, c client.WithWatch, list client.ObjectList,
				opts ...client.ListOption,
			) error {
				if _, ok := list.(*corev1.PodList); ok {
					return errors.New("connection refused listing pods")
				}
				return c.List(ctx, list, opts...)
			},
		}).
		Build()
	m := &pxfObservabilityRecorder{}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, m, nil)

	count := r.reconcilePxf(context.Background(), cluster)
	assert.Equal(t, 5, count, "list error must not block reconcile")
	require.NotNil(t, cluster.Status.DataLoading.Pxf)
	assert.Empty(t, cluster.Status.DataLoading.Pxf.Status,
		"a pod-list error must leave Status ABSENT, never fabricated")
	assert.Zero(t, m.statusCalls,
		"no status gauge must be emitted when the status is unobservable")
}

// ---------------------------------------------------------------------------
// S.3 — pxf.extensionsInstalled from a live pg_extension probe
// ---------------------------------------------------------------------------

// TestReconcilePxf_ExtensionsInstalled covers the extensions enrichment via the
// fake db.Client's ListPXFExtensionsFunc, including the HONEST absent cases.
//
// Covers 105-S3-B1 (both → set), 105-S3-B3 (none → absent),
// 105-S3-B4 (DB error → absent + non-fatal) and the nil-dbFactory absent case.
func TestReconcilePxf_ExtensionsInstalled(t *testing.T) {
	tests := []struct {
		name        string
		dbFactory   bool
		listFunc    func(ctx context.Context) ([]string, error)
		wantExts    []string
		wantExtCall bool
		wantExtVal  float64
	}{
		{
			name:        "both extensions observed → set (105-S3-B1)",
			dbFactory:   true,
			listFunc:    func(_ context.Context) ([]string, error) { return []string{"pxf", "pxf_fdw"}, nil },
			wantExts:    []string{"pxf", "pxf_fdw"},
			wantExtCall: true,
			wantExtVal:  2,
		},
		{
			name:        "only pxf observed → honest subset (105-S3-B2)",
			dbFactory:   true,
			listFunc:    func(_ context.Context) ([]string, error) { return []string{"pxf"}, nil },
			wantExts:    []string{"pxf"},
			wantExtCall: true,
			wantExtVal:  1,
		},
		{
			name:        "reachable DB, none installed → ABSENT (105-S3-B3)",
			dbFactory:   true,
			listFunc:    func(_ context.Context) ([]string, error) { return nil, nil },
			wantExts:    nil,
			wantExtCall: false,
		},
		{
			name:        "DB unreachable / query error → ABSENT, non-fatal (105-S3-B4)",
			dbFactory:   true,
			listFunc:    func(_ context.Context) ([]string, error) { return nil, errors.New("connection refused") },
			wantExts:    nil,
			wantExtCall: false,
		},
		{
			name:        "nil dbFactory → ABSENT (no probe)",
			dbFactory:   false,
			wantExts:    nil,
			wantExtCall: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := newPXFDataLoadingCluster()
			cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()

			m := &pxfObservabilityRecorder{}
			var factory *testutil.MockDBClientFactory
			if tt.dbFactory {
				factory = &testutil.MockDBClientFactory{
					Client: &testutil.MockDBClient{ListPXFExtensionsFunc: tt.listFunc},
				}
			}

			var r *AdminReconciler
			if factory != nil {
				r = NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
					builder.NewBuilder(), factory, m, nil)
			} else {
				r = NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
					builder.NewBuilder(), nil, m, nil)
			}

			// Must never fail reconcile regardless of the probe outcome.
			count := r.reconcilePxf(context.Background(), cluster)
			assert.Equal(t, 5, count)

			require.NotNil(t, cluster.Status.DataLoading.Pxf)
			assert.Equal(t, tt.wantExts, cluster.Status.DataLoading.Pxf.ExtensionsInstalled)

			// 105-MX-B2: the extensions gauge is emitted ONLY when observed.
			if tt.wantExtCall {
				assert.Equal(t, 1, m.extCalls)
				assert.Equal(t, tt.wantExtVal, m.lastExtCount)
			} else {
				assert.Zero(t, m.extCalls,
					"extensions gauge must NOT be emitted when extensions are absent")
			}
		})
	}
}

// TestReconcilePxf_ExtensionsAbsent_DBFactoryError covers 105-S3-B4 at the
// factory boundary: NewClient itself failing (DB not available) leaves the
// extensions ABSENT and reconcile still succeeds (non-fatal).
func TestReconcilePxf_ExtensionsAbsent_DBFactoryError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	factory := &testutil.MockDBClientFactory{Err: errors.New("dial tcp: connection refused")}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	count := r.reconcilePxf(context.Background(), cluster)
	assert.Equal(t, 5, count)
	require.NotNil(t, cluster.Status.DataLoading.Pxf)
	assert.Nil(t, cluster.Status.DataLoading.Pxf.ExtensionsInstalled,
		"DB factory error must leave extensionsInstalled NIL (absent), not synthesized")
}

// ---------------------------------------------------------------------------
// S.1/S.3 — patchDataLoadingStatus MergePatch leak guard (105-S1-B5/105-S3-B5)
// ---------------------------------------------------------------------------

// pxfPatchMap drives patchDataLoadingStatus against a patch-capturing fake
// client and returns the status.dataLoading.pxf sub-map actually emitted in the
// MergePatch body (nil if no pxf object was emitted). This exposes EXACTLY which
// keys leave the controller, which is the highest-risk leak point.
func pxfPatchMap(t *testing.T, cluster *cbv1alpha1.CloudberryCluster) (map[string]interface{}, bool) {
	t.Helper()
	scheme := newTestScheme()
	var lastPatch []byte
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(
				ctx context.Context, c client.Client, subResourceName string,
				obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption,
			) error {
				data, err := patch.Data(obj)
				if err != nil {
					return err
				}
				lastPatch = data
				return c.Status().Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)
	require.NoError(t, r.patchDataLoadingStatus(context.Background(), cluster))

	require.NotNil(t, lastPatch, "a status patch must have been issued")

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(lastPatch, &body))
	status, ok := body["status"].(map[string]interface{})
	require.True(t, ok)
	dataLoading, ok := status["dataLoading"].(map[string]interface{})
	require.True(t, ok)
	pxf, ok := dataLoading["pxf"].(map[string]interface{})
	return pxf, ok
}

// TestPatchDataLoadingStatus_PxfLeakGuard_FieldsSet covers 105-S1-B5 / 105-S3-B5
// (SET side): when Status and ExtensionsInstalled are populated in memory, the
// MergePatch body MUST carry status.dataLoading.pxf.{status,extensionsInstalled}
// (otherwise the explicit-map patch silently drops them).
func TestPatchDataLoadingStatus_PxfLeakGuard_FieldsSet(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase: "Configured",
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{
			Configured:          true,
			Servers:             3,
			Status:              util.PXFStatusRunning,
			ExtensionsInstalled: []string{"pxf", "pxf_fdw"},
		},
	}

	pxf, ok := pxfPatchMap(t, cluster)
	require.True(t, ok, "pxf sub-object must be present when PXF is enabled")

	assert.Equal(t, true, pxf["configured"])
	assert.Equal(t, float64(3), pxf["servers"])
	assert.Equal(t, "Running", pxf["status"], "set status MUST appear in the patch")
	exts, ok := pxf["extensionsInstalled"].([]interface{})
	require.True(t, ok, "set extensionsInstalled MUST appear in the patch")
	assert.Equal(t, []interface{}{"pxf", "pxf_fdw"}, exts)
}

// TestPatchDataLoadingStatus_PxfLeakGuard_FieldsAbsent covers 105-S1-B5 /
// 105-S3-B5 (ABSENT side): when Status is "" and ExtensionsInstalled is nil
// (PXF enabled but unobservable), the patch MUST still carry the config-derived
// configured/servers BUT must OMIT status and extensionsInstalled entirely — no
// empty string / empty array may be synthesized.
func TestPatchDataLoadingStatus_PxfLeakGuard_FieldsAbsent(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase: "Configured",
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{
			Configured:          true,
			Servers:             3,
			Status:              "",  // unobservable
			ExtensionsInstalled: nil, // unobservable
		},
	}

	pxf, ok := pxfPatchMap(t, cluster)
	require.True(t, ok, "pxf sub-object must still be present (configured/servers)")

	assert.Equal(t, true, pxf["configured"])
	assert.Equal(t, float64(3), pxf["servers"])

	_, hasStatus := pxf["status"]
	assert.False(t, hasStatus,
		"absent status MUST be omitted from the patch (no empty string synthesized)")
	_, hasExts := pxf["extensionsInstalled"]
	assert.False(t, hasExts,
		"absent extensionsInstalled MUST be omitted from the patch (no empty array synthesized)")
}

// TestPatchDataLoadingStatus_NonPxfCluster covers 105-S1-B6: a non-PXF cluster
// (no Status.DataLoading.Pxf) emits NO pxf sub-object at all — status is never
// forced for clusters that did not configure PXF.
func TestPatchDataLoadingStatus_NonPxfCluster(t *testing.T) {
	cluster := newTestCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{Phase: "Configured"}

	_, ok := pxfPatchMap(t, cluster)
	assert.False(t, ok, "non-PXF cluster must not emit a status.dataLoading.pxf object")
}

// TestPatchDataLoadingStatus_PxfRoundTrip covers 105-S1-B5 / 105-S3-B5
// round-trip: applying the patch to a fake status subresource client reads the
// observed fields back (the SET fields survive the MergePatch).
func TestPatchDataLoadingStatus_PxfRoundTrip(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Phase: "Configured",
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{
			Configured:          true,
			Servers:             2,
			Status:              util.PXFStatusError,
			ExtensionsInstalled: []string{"pxf"},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.patchDataLoadingStatus(context.Background(), cluster))

	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	require.NotNil(t, updated.Status.DataLoading.Pxf)
	assert.Equal(t, util.PXFStatusError, updated.Status.DataLoading.Pxf.Status)
	assert.Equal(t, []string{"pxf"}, updated.Status.DataLoading.Pxf.ExtensionsInstalled)
}

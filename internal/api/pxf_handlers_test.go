package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// pxfRestartCall captures a single RecordPXFRestart invocation.
type pxfRestartCall struct {
	cluster   string
	namespace string
	result    string
}

// pxfSpyRecorder wraps NoopRecorder and records RecordPXFRestart calls so the
// handler tests can assert the metric is emitted with the right labels.
type pxfSpyRecorder struct {
	metrics.NoopRecorder
	restarts       []pxfRestartCall
	serversChanged []pxfServersChangedCall
	syncs          []pxfSyncCall
}

func (r *pxfSpyRecorder) RecordPXFRestart(cluster, namespace, result string) {
	r.restarts = append(r.restarts, pxfRestartCall{cluster: cluster, namespace: namespace, result: result})
}

// pxfServersChangedCall captures a single IncPXFServersChanged invocation so the
// sync tests can assert the HONEST servers-changed counter fired with the right
// labels — and, by counting, that it fired EXACTLY on a real Data diff.
type pxfServersChangedCall struct {
	cluster   string
	namespace string
}

func (r *pxfSpyRecorder) IncPXFServersChanged(cluster, namespace string) {
	r.serversChanged = append(r.serversChanged, pxfServersChangedCall{cluster: cluster, namespace: namespace})
}

// pxfSyncCall captures a single RecordPXFSync invocation so the W2-B6 request
// counter can be asserted as ORTHOGONAL to the honest servers-changed counter.
type pxfSyncCall struct {
	cluster   string
	namespace string
	result    string
}

func (r *pxfSpyRecorder) RecordPXFSync(cluster, namespace, result string) {
	r.syncs = append(r.syncs, pxfSyncCall{cluster: cluster, namespace: namespace, result: result})
}

// newPXFCluster builds a cluster with PXF data loading enabled.
func newPXFCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster(name, namespace)
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "s3.local"}},
			},
		},
	}
	c.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{Configured: true, Servers: 1},
	}
	return c
}

// newSegmentPrimarySTS builds the segment-primary StatefulSet object.
func newSegmentPrimarySTS(cluster, namespace string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(cluster),
			Namespace: namespace,
		},
	}
}

// newPXFPod builds a segment-primary pod whose "pxf" container has the given
// readiness, with the segment-primary component labels.
func newPXFPod(name, cluster, namespace string, ready bool) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelCluster:   cluster,
				util.LabelComponent: util.ComponentSegmentPrimary,
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "segment", Ready: true},
				{Name: util.PXFContainerName, Ready: ready},
			},
		},
	}
}

func pxfRequest(method, name, namespace, action string) *http.Request {
	req := httptest.NewRequest(method,
		apiPrefix+"/clusters/"+name+"/data-loading/pxf/"+action+"?namespace="+namespace, nil)
	req.SetPathValue("name", name)
	return req
}

// --- handlePXFStatus -------------------------------------------------------

func TestHandlePXFStatus_NotFound(t *testing.T) {
	s := newTestServer()
	req := pxfRequest(http.MethodGet, "missing", "default", "status")
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

func TestHandlePXFStatus_NotEnabled(t *testing.T) {
	// DL enabled but PXF absent: the PXF-specific gate fires (PXF_NOT_ENABLED).
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	s := newTestServer(cluster)
	req := pxfRequest(http.MethodGet, "test-cluster", "default", "status")
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotEnabled)
}

func TestHandlePXFStatus_DataLoadingDisabled(t *testing.T) {
	// DL disabled => the broader subsystem gate takes precedence over PXF.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: false}
	s := newTestServer(cluster)
	req := pxfRequest(http.MethodGet, "test-cluster", "default", "status")
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
}

func TestHandlePXFStatus_AggregatesReadiness(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	readyPod := newPXFPod("seg-0", "test-cluster", "default", true)
	notReadyPod := newPXFPod("seg-1", "test-cluster", "default", false)
	s := newTestServerWithObjects(cluster, readyPod, notReadyPod)

	req := pxfRequest(http.MethodGet, "test-cluster", "default", "status")
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["readySidecars"])
	assert.Equal(t, float64(2), resp["totalSidecars"])
	assert.Equal(t, float64(1), resp["servers"])
	assert.Equal(t, true, resp["configured"])
	sidecars, ok := resp["sidecars"].([]interface{})
	require.True(t, ok)
	assert.Len(t, sidecars, 2)
}

func TestHandlePXFStatus_NoPods(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := pxfRequest(http.MethodGet, "test-cluster", "default", "status")
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(0), resp["readySidecars"])
	assert.Equal(t, float64(0), resp["totalSidecars"])
}

// --- handlePXFRestart ------------------------------------------------------

func TestHandlePXFRestart_NotEnabled(t *testing.T) {
	// DL enabled but PXF absent: the PXF-specific gate fires (PXF_NOT_ENABLED).
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	s := newTestServer(cluster)
	req := pxfRequest(http.MethodPost, "test-cluster", "default", "restart")
	rec := httptest.NewRecorder()
	s.handlePXFRestart(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotEnabled)
}

func TestHandlePXFRestart_PatchesSTSAndRecordsMetric(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster, sts)

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "restart")
	rec := httptest.NewRecorder()
	s.handlePXFRestart(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["restarted"])
	assert.Equal(t, util.SegmentPrimaryName("test-cluster"), resp["statefulSet"])

	// The STS now carries the restart-trigger annotation.
	got := &appsv1.StatefulSet{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: util.SegmentPrimaryName("test-cluster"), Namespace: "default"}, got))
	assert.NotEmpty(t, got.Spec.Template.Annotations[util.AnnotationRestartTrigger])

	require.Len(t, spy.restarts, 1)
	assert.Equal(t, pxfRestartCall{cluster: "test-cluster", namespace: "default", result: pxfRestartResultStarted},
		spy.restarts[0])
}

func TestHandlePXFRestart_MissingSTS(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster) // no STS seeded

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "restart")
	rec := httptest.NewRecorder()
	s.handlePXFRestart(rec, req)

	// A missing segment-primary StatefulSet is a precondition failure (409
	// PXF_NOT_READY), not a 404/INTERNAL_ERROR mismatch.
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotReady)
	require.Len(t, spy.restarts, 1)
	assert.Equal(t, pxfRestartResultFailed, spy.restarts[0].result)
}

// --- handlePXFSync ---------------------------------------------------------

func TestHandlePXFSync_NotEnabled(t *testing.T) {
	// DL enabled but PXF absent: the PXF-specific gate fires (PXF_NOT_ENABLED).
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	s := newTestServer(cluster)
	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotEnabled)
}

func TestHandlePXFSync_CreatesConfigMapAndBumpsSTS(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	s := newTestServer(cluster)
	require.NoError(t, s.k8sClient.Create(t.Context(), sts))

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["synced"])
	cmName := builder.PxfServersConfigMapName("test-cluster")
	assert.Equal(t, cmName, resp["configMap"])

	// ConfigMap was created.
	cm := &corev1.ConfigMap{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: cmName, Namespace: "default"}, cm))
	assert.NotEmpty(t, cm.Data)

	// STS annotation bumped.
	got := &appsv1.StatefulSet{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: util.SegmentPrimaryName("test-cluster"), Namespace: "default"}, got))
	assert.NotEmpty(t, got.Spec.Template.Annotations[util.AnnotationRestartTrigger])
}

func TestHandlePXFSync_UpdatesStaleConfigMap(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	staleCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      builder.PxfServersConfigMapName("test-cluster"),
			Namespace: "default",
		},
		Data: map[string]string{"stale": "value"},
	}
	s := newTestServerWithObjects(cluster, sts, staleCM)

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	cm := &corev1.ConfigMap{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: builder.PxfServersConfigMapName("test-cluster"), Namespace: "default"}, cm))
	// Stale key replaced by the rendered server config.
	_, hasStale := cm.Data["stale"]
	assert.False(t, hasStale)
	assert.NotEmpty(t, cm.Data)
}

func TestHandlePXFSync_MissingSTS(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	s := newTestServer(cluster) // no STS seeded
	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)
	// A missing segment-primary StatefulSet is a precondition failure (409
	// PXF_NOT_READY), not a 404/INTERNAL_ERROR mismatch.
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotReady)
}

// 106-MX-B1 (api real-diff): a pxf sync that REALLY changes the persisted
// ConfigMap Data (a stale CM replaced by the rendered server config) increments
// cloudberry_pxf_servers_changed_total EXACTLY once with the cluster labels.
func TestHandlePXFSync_RealDiff_IncrementsServersChanged(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	staleCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      builder.PxfServersConfigMapName("test-cluster"),
			Namespace: "default",
		},
		// A different server name so the rendered Data is a REAL diff (the desired
		// CM renders "s3srv__*"; this seeds "old-server__*").
		Data: map[string]string{"old-server__s3-site.xml": "<old/>"},
	}
	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster, sts, staleCM)

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)

	// The stale key is gone, the rendered server config is present.
	cm := &corev1.ConfigMap{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: builder.PxfServersConfigMapName("test-cluster"), Namespace: "default"}, cm))
	_, hasStale := cm.Data["old-server__s3-site.xml"]
	assert.False(t, hasStale)
	assert.Contains(t, cm.Data, "s3srv__s3-site.xml")

	// HONEST counter fired exactly once with the right labels.
	require.Len(t, spy.serversChanged, 1)
	assert.Equal(t,
		pxfServersChangedCall{cluster: "test-cluster", namespace: "default"},
		spy.serversChanged[0])
}

// 106-MX-B2 (api no-op honesty): a pxf sync whose desired Data already equals the
// persisted Data does NOT increment the servers-changed counter.
func TestHandlePXFSync_NoOp_DoesNotIncrementServersChanged(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	// Seed the EXACT rendered CM so the sync sees byte-identical Data.
	rendered := builder.NewBuilder().BuildPXFServersConfigMap(cluster)
	require.NotNil(t, rendered)

	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster, sts, rendered)

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	// Honesty: no Data diff → counter must NOT fire.
	assert.Empty(t, spy.serversChanged,
		"no-op sync (identical Data) must NOT increment the servers-changed counter")
}

// 106-MX (api create honesty): a pxf sync that CREATES the ConfigMap (none
// existed) does NOT increment the servers-changed counter — a create is not a
// change.
func TestHandlePXFSync_Create_DoesNotIncrementServersChanged(t *testing.T) {
	cluster := newPXFCluster("test-cluster", "default")
	sts := newSegmentPrimarySTS("test-cluster", "default")
	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster, sts) // no ConfigMap seeded

	req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
	rec := httptest.NewRecorder()
	s.handlePXFSync(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	// ConfigMap created...
	cm := &corev1.ConfigMap{}
	require.NoError(t, s.k8sClient.Get(req.Context(),
		types.NamespacedName{Name: builder.PxfServersConfigMapName("test-cluster"), Namespace: "default"}, cm))
	assert.NotEmpty(t, cm.Data)
	// ...but the servers-changed signal did NOT fire.
	assert.Empty(t, spy.serversChanged, "create must NOT increment the servers-changed counter")
}

// TestPXFSyncCounter_SuccessAndError verifies the W2-B6 request counter
// (cloudberry_pxf_sync_total) fires once per request with a bounded
// {cluster,namespace,result} tuple, ORTHOGONAL to the honest servers-changed
// counter (TASK 8, C-FORCE-PAIR).
func TestPXFSyncCounter_SuccessAndError(t *testing.T) {
	// Success on a NO-OP sync: result=success, but the honest servers-changed
	// counter must NOT fire (a no-op is not a change).
	t.Run("success_noop_does_not_touch_servers_changed", func(t *testing.T) {
		cluster := newPXFCluster("test-cluster", "default")
		sts := newSegmentPrimarySTS("test-cluster", "default")
		rendered := builder.NewBuilder().BuildPXFServersConfigMap(cluster)
		require.NotNil(t, rendered)
		spy := &pxfSpyRecorder{}
		s := newTestServerWithRecorder(spy, cluster, sts, rendered)

		req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
		rec := httptest.NewRecorder()
		s.handlePXFSync(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		// W2-B6: exactly one sync request counted as success.
		require.Len(t, spy.syncs, 1)
		assert.Equal(t,
			pxfSyncCall{cluster: "test-cluster", namespace: "default", result: "success"},
			spy.syncs[0])
		// C-FORCE-PAIR: the honest servers-changed counter stays flat on a no-op.
		assert.Empty(t, spy.serversChanged,
			"a no-op sync must NOT increment the servers-changed counter")
	})

	// Success on a REAL diff: result=success AND servers-changed fires once.
	// These are two independent counters.
	t.Run("success_realdiff_both_counters_independent", func(t *testing.T) {
		cluster := newPXFCluster("test-cluster", "default")
		sts := newSegmentPrimarySTS("test-cluster", "default")
		staleCM := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      builder.PxfServersConfigMapName("test-cluster"),
				Namespace: "default",
			},
			Data: map[string]string{"old-server__s3-site.xml": "<old/>"},
		}
		spy := &pxfSpyRecorder{}
		s := newTestServerWithRecorder(spy, cluster, sts, staleCM)

		req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
		rec := httptest.NewRecorder()
		s.handlePXFSync(rec, req)

		require.Equal(t, http.StatusAccepted, rec.Code)
		require.Len(t, spy.syncs, 1)
		assert.Equal(t, "success", spy.syncs[0].result)
		assert.Len(t, spy.serversChanged, 1,
			"a real diff must increment the servers-changed counter exactly once")
	})

	// Error path: a sync that fails to roll the segment-primary StatefulSet
	// (missing STS → 409) records result=error and NEVER fires servers-changed.
	t.Run("error_missing_sts", func(t *testing.T) {
		cluster := newPXFCluster("test-cluster", "default")
		spy := &pxfSpyRecorder{}
		s := newTestServerWithRecorder(spy, cluster) // no STS seeded

		req := pxfRequest(http.MethodPost, "test-cluster", "default", "sync")
		rec := httptest.NewRecorder()
		s.handlePXFSync(rec, req)

		require.Equal(t, http.StatusConflict, rec.Code)
		require.Len(t, spy.syncs, 1)
		assert.Equal(t,
			pxfSyncCall{cluster: "test-cluster", namespace: "default", result: "error"},
			spy.syncs[0])
		assert.Empty(t, spy.serversChanged)
	})
}

// --- helpers ---------------------------------------------------------------

// newTestServerWithRecorder builds a server seeded with the given objects and a
// custom metrics recorder so the restart-metric calls can be asserted.
func newTestServerWithRecorder(recorder metrics.Recorder, objs ...runtime.Object) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, nil, recorder, nil, 0))
}

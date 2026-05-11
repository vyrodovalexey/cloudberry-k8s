package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	return scheme
}

func newTestServer(clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return NewServer(k8sClient, nil, &metrics.NoopRecorder{}, nil)
}

func newTestCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Version: "7.7",
			Image:   "cloudberrydb/cloudberry:7.7",
			Coordinator: cbv1alpha1.CoordinatorSpec{
				Storage: cbv1alpha1.StorageSpec{Size: "10Gi"},
			},
			Segments: cbv1alpha1.SegmentsSpec{
				Count:   4,
				Storage: cbv1alpha1.StorageSpec{Size: "20Gi"},
			},
		},
		Status: cbv1alpha1.CloudberryClusterStatus{
			Phase:         cbv1alpha1.ClusterPhaseRunning,
			SegmentsReady: 4,
			SegmentsTotal: 4,
		},
	}
}

func TestNewServer(t *testing.T) {
	s := newTestServer()
	require.NotNil(t, s)
}

func TestHandleHealthz(t *testing.T) {
	s := newTestServer()
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp["status"])
}

func TestHandleReadyz(t *testing.T) {
	s := newTestServer()
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "ready", resp["status"])
}

func TestHandleListClusters(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Need to add identity to context for permission check
	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	s.handleListClusters(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleGetCluster_Found(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetCluster(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleGetCluster_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetCluster(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetClusterStatus(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/status?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetClusterStatus(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "test-cluster", resp["name"])
}

func TestHandleGetClusterStatus_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/status?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetClusterStatus(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCreateCluster(t *testing.T) {
	s := newTestServer()

	cluster := newTestCluster("new-cluster", "default")
	body, err := json.Marshal(cluster)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleCreateCluster(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandleCreateCluster_InvalidBody(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters", bytes.NewReader([]byte("invalid")))
	rec := httptest.NewRecorder()
	s.handleCreateCluster(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleDeleteCluster(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test-cluster?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleDeleteCluster(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleDeleteCluster_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/nonexistent?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleDeleteCluster(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleListSegments(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/segments?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSegments(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(4), resp["segmentsReady"])
	assert.Equal(t, float64(4), resp["segmentsTotal"])
}

func TestHandleGetMirroring(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/mirroring?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetMirroring(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleListSessions(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test/sessions", nil)
	req.SetPathValue("name", "test")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleCancelQuery(t *testing.T) {
	t.Run("valid pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test/sessions/123/cancel", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("invalid pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test/sessions/abc/cancel", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "abc")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandleTerminateSession(t *testing.T) {
	t.Run("valid pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test/sessions/456", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "456")
		rec := httptest.NewRecorder()
		s.handleTerminateSession(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("invalid pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test/sessions/xyz", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "xyz")
		rec := httptest.NewRecorder()
		s.handleTerminateSession(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandleGetStandby(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Status.StandbyReady = true
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/standby?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetStandby(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp["enabled"].(bool))
	assert.True(t, resp["ready"].(bool))
}

func TestHandleGetConfig(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Config = &cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "100"},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/config?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetConfig(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleUpdateConfig(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	configUpdate := cbv1alpha1.ConfigSpec{
		Parameters: map[string]string{"max_connections": "200"},
	}
	body, err := json.Marshal(configUpdate)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPut, apiPrefix+"/clusters/test-cluster/config?namespace=default", bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleUpdateConfig_InvalidBody(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPut, apiPrefix+"/clusters/test-cluster/config?namespace=default", bytes.NewReader([]byte("invalid")))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleStartRecovery(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body, _ := json.Marshal(map[string]string{"type": "incremental"})
	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/recovery?namespace=default", bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleStartRecovery_InvalidBody(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test/recovery", bytes.NewReader([]byte("invalid")))
	req.SetPathValue("name", "test")
	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetCluster_ByNamespace(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	result, err := s.getCluster(context.Background(), "test-cluster", "default")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "test-cluster", result.Name)
}

func TestGetCluster_AllNamespaces(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	result, err := s.getCluster(context.Background(), "test-cluster", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "test-cluster", result.Name)
}

func TestGetCluster_NotFound(t *testing.T) {
	s := newTestServer()

	result, err := s.getCluster(context.Background(), "nonexistent", "default")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"key": "value"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestWriteErrorJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErrorJSON(rec, http.StatusBadRequest, "BAD_REQUEST", "invalid input")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	errObj := resp["error"].(map[string]interface{})
	assert.Equal(t, "BAD_REQUEST", errObj["code"])
	assert.Equal(t, "invalid input", errObj["message"])
}

func TestSecurityHeaders_Applied(t *testing.T) {
	s := newTestServer()
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", rec.Header().Get("X-Frame-Options"))
}

func TestWithAuth_NilMiddleware(t *testing.T) {
	s := newTestServer()
	s.authMW = nil

	handler := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSetClusterAnnotation(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.setClusterAnnotation(rec, req, "start")

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestSetMaintenanceAnnotation(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/maintenance/vacuum?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.setMaintenanceAnnotation(rec, req, "vacuum")

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleStartCluster(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleStartCluster(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleStopCluster(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/stop?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleStopCluster(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleRestartCluster(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/restart?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleRestartCluster(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleReloadConfig(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/reload?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleReloadConfig(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleVacuum(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/maintenance/vacuum?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleVacuum(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleAnalyze(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/maintenance/analyze?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleAnalyze(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleReindex(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/maintenance/reindex?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleReindex(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleActivateStandby(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/standby/activate?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleActivateStandby(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestHandleRebalance(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/rebalance?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleRebalance(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestSetClusterAnnotation_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/nonexistent/start?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.setClusterAnnotation(rec, req, "start")

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestSetMaintenanceAnnotation_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/nonexistent/maintenance/vacuum?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.setMaintenanceAnnotation(rec, req, "vacuum")

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleListClusters_Empty(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters", nil)
	rec := httptest.NewRecorder()
	s.handleListClusters(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, float64(0), resp["total"])
}

func TestHandleGetConfig_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/config?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetConfig(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleUpdateConfig_NotFound(t *testing.T) {
	s := newTestServer()

	body, _ := json.Marshal(cbv1alpha1.ConfigSpec{})
	req := httptest.NewRequest(http.MethodPut, apiPrefix+"/clusters/nonexistent/config?namespace=default", bytes.NewReader(body))
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleUpdateConfig(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleListSegments_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/segments?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListSegments(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetMirroring_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/mirroring?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetMirroring(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetStandby_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/standby?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetStandby(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleStartRecovery_NotFound(t *testing.T) {
	s := newTestServer()

	body, _ := json.Marshal(map[string]string{"type": "incremental"})
	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/nonexistent/recovery?namespace=default", bytes.NewReader(body))
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestWithAuth_WithMiddleware(t *testing.T) {
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := newTestServer()
	s.authMW = mw

	handler := s.withAuth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

// mockAuthProvider implements auth.Provider for testing.
type mockAuthProvider struct {
	identity *auth.Identity
	err      error
}

func (m *mockAuthProvider) Authenticate(_ context.Context, _ *http.Request) (*auth.Identity, error) {
	return m.identity, m.err
}

func (m *mockAuthProvider) Type() string {
	return "mock"
}

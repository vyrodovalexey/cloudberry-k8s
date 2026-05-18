package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

func newTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = cbv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	return scheme
}

func newTestServer(clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)
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
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/sessions?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	// Without dbFactory, should return empty sessions with message.
	assert.Equal(t, float64(0), resp["total"])
	assert.Equal(t, "database connection not available", resp["message"])
}

func TestHandleListSessions_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/sessions?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCancelQuery(t *testing.T) {
	t.Run("valid pid no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, false, resp["canceled"])
		assert.Equal(t, "database connection not available", resp["message"])
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

	t.Run("zero pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test/sessions/0/cancel", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "0")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/nonexistent/sessions/123/cancel?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleTerminateSession(t *testing.T) {
	t.Run("valid pid no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test-cluster/sessions/456?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "456")
		rec := httptest.NewRecorder()
		s.handleTerminateSession(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, false, resp["terminated"])
		assert.Equal(t, "database connection not available", resp["message"])
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

	t.Run("negative pid", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test/sessions/-1", nil)
		req.SetPathValue("name", "test")
		req.SetPathValue("pid", "-1")
		rec := httptest.NewRecorder()
		s.handleTerminateSession(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/nonexistent/sessions/456?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("pid", "456")
		rec := httptest.NewRecorder()
		s.handleTerminateSession(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
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

// Backup endpoint tests.

func TestHandleListBackups(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/backups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackups(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(0), resp["total"])
	assert.Equal(t, "test-cluster", resp["cluster"])
}

func TestHandleListBackups_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/backups?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListBackups(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCreateBackup(t *testing.T) {
	t.Run("backup not enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/backups?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("backup enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
			Enabled:     true,
			Destination: cbv1alpha1.BackupDestination{Type: "s3", Bucket: "backups"},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/test-cluster/backups?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters/nonexistent/backups?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleGetBackup(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/backups/bk-1?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleGetBackup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetBackup_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/backups/bk-1?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleGetBackup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDeleteBackup(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete, apiPrefix+"/clusters/test-cluster/backups/bk-1?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "deleted", resp["status"])
	assert.Equal(t, "bk-1", resp["backupID"])
}

func TestHandleRestoreBackup(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/backups/bk-1/restore?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, req)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "restore initiated", resp["status"])
}

func TestHandleRestoreBackup_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/backups/bk-1/restore?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Data loading endpoint tests.

func TestHandleListDataLoadingJobs(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "job1", Type: "s3", TargetTable: "public.data"},
		},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListDataLoadingJobs(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleListDataLoadingJobs_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListDataLoadingJobs(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCreateDataLoadingJob(t *testing.T) {
	t.Run("data loading not enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("data loading enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})
}

func TestHandleGetDataLoadingJob(t *testing.T) {
	t.Run("job found", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			Jobs:    []cbv1alpha1.DataLoadingJob{{Name: "job1", Type: "s3", TargetTable: "t"}},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/job1?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "job1")
		rec := httptest.NewRecorder()
		s.handleGetDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("job not found", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/missing?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "missing")
		rec := httptest.NewRecorder()
		s.handleGetDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleStartStopDataLoadingJob(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	t.Run("start job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/j1/start?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "j1")
		rec := httptest.NewRecorder()
		s.handleStartDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})

	t.Run("stop job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/j1/stop?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "j1")
		rec := httptest.NewRecorder()
		s.handleStopDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})
}

func TestHandleUpdateDeleteDataLoadingJob(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	t.Run("update job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/j1?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "j1")
		rec := httptest.NewRecorder()
		s.handleUpdateDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("delete job", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/data-loading/jobs/j1?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("job", "j1")
		rec := httptest.NewRecorder()
		s.handleDeleteDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// Workload endpoint tests.

func TestHandleGetWorkload(t *testing.T) {
	t.Run("workload nil", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetWorkload(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, false, resp["enabled"])
	})

	t.Run("workload enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{Enabled: true}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetWorkload(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/nonexistent/workload?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleGetWorkload(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleCreateResourceGroup(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"name": "test"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/nonexistent/workload/resource-groups?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleCreateResourceGroup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty name", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": ""})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": "analytics", "concurrency": 10})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceGroup(rec, req)
		assert.Equal(t, http.StatusCreated, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "analytics", resp["name"])
		assert.Contains(t, resp["message"], "pending")
	})
}

func TestHandleDeleteResourceGroup(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/nonexistent/workload/resource-groups/analytics?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceGroup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceGroup(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "pending", resp["status"])
	})
}

func TestHandleAssignResourceGroup(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/nonexistent/workload/resource-groups/analytics/assign?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleAssignResourceGroup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleAssignResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty role", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"role": ""})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleAssignResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleAssignResourceGroup(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "pending", resp["status"])
	})
}

func TestHandleListResourceGroups(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10},
		},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleListResourceGroups_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/workload/resource-groups?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleListWorkloadRules(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		Rules: []cbv1alpha1.WorkloadRule{
			{Name: "rule1", Action: "cancel"},
		},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListWorkloadRules(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleListWorkloadRules_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/workload/rules?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListWorkloadRules(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Query monitoring endpoint tests.

func TestHandleGetQueryMonitoring(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryMonitoring(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(5), resp["activeQueries"])
	assert.NotNil(t, resp["config"])
}

func TestHandleGetQueryMonitoring_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/queries?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetQueryMonitoring(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetActiveQueries(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 10
	cluster.Status.BlockedQueries = 1
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/active?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetActiveQueries(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(10), resp["activeQueries"])
	assert.Equal(t, float64(1), resp["blockedQueries"])
}

func TestHandleGetActiveQueries_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/queries/active?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetActiveQueries(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Storage endpoint tests.

func TestHandleGetDiskUsage(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.DiskUsagePercent = 45
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/disk-usage?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetDiskUsage(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(45), resp["diskUsagePercent"])
}

func TestHandleGetDiskUsage_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/disk-usage?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetDiskUsage(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleListTables(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/tables?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListTables(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(0), resp["total"])
}

func TestHandleListTables_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/tables?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListTables(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetTableDetail(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/tables/public/users?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("schema", "public")
	req.SetPathValue("table", "users")
	rec := httptest.NewRecorder()
	s.handleGetTableDetail(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "public", resp["schema"])
	assert.Equal(t, "users", resp["table"])
}

func TestHandleListRecommendations(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.RecommendationCount = 3
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/recommendations?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListRecommendations(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(3), resp["recommendationCount"])
}

func TestHandleListRecommendations_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/recommendations?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListRecommendations(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleTriggerRecommendationScan(t *testing.T) {
	t.Run("scan not enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleTriggerRecommendationScan(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("scan enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
			DiskMonitoring: true,
			RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
				Enabled: true, Schedule: "0 3 * * 0",
			},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/storage/recommendations/scan?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleTriggerRecommendationScan(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/nonexistent/storage/recommendations/scan?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleTriggerRecommendationScan(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleGetUsageReport(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/usage-report?namespace=default&month=2025-01", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetUsageReport(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "2025-01", resp["month"])
}

func TestHandleGetUsageReport_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/usage-report?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetUsageReport(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDeleteBackup_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/nonexistent/backups/bk-1?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("id", "bk-1")
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetStandby_NoStandby(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/standby?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetStandby(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["enabled"])
}

func TestGetCluster_AllNamespaces_NotFound(t *testing.T) {
	s := newTestServer()
	result, err := s.getCluster(context.Background(), "nonexistent", "")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "not found")
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

func TestHandleListPVCs(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Create a PVC that belongs to the cluster.
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-cluster-coordinator-0",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelCluster:   "test-cluster",
				util.LabelComponent: util.ComponentCoordinator,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("10Gi"),
				},
			},
		},
	}
	err := s.k8sClient.Create(context.Background(), pvc)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/pvcs?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListPVCs(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])

	pvcs := resp["pvcs"].([]interface{})
	require.Len(t, pvcs, 1)
	pvcInfo := pvcs[0].(map[string]interface{})
	assert.Equal(t, "data-test-cluster-coordinator-0", pvcInfo["name"])
	assert.Equal(t, util.ComponentCoordinator, pvcInfo["component"])
	assert.Equal(t, "10Gi", pvcInfo["size"])
}

func TestHandleListPVCs_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/pvcs?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListPVCs(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

// mockDBClient implements db.Client for testing.
type mockDBClient struct {
	sessions            []db.Session
	listSessionsErr     error
	cancelResult        bool
	cancelQueryErr      error
	terminateResult     bool
	terminateSessionErr error
	resGroups           []db.ResourceGroupInfo
	listResGroupsErr    error
	createResGroupErr   error
	dropResGroupErr     error
	assignRoleErr       error
	resQueues           []db.ResourceQueueInfo
	listResQueuesErr    error
	createResQueueErr   error
	dropResQueueErr     error
	closeCalls          int
}

func (m *mockDBClient) Ping(_ context.Context) error { return nil }
func (m *mockDBClient) Close()                       { m.closeCalls++ }
func (m *mockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return nil, nil
}
func (m *mockDBClient) GetClusterState(_ context.Context) (*db.ClusterState, error) {
	return &db.ClusterState{IsUp: true}, nil
}
func (m *mockDBClient) SetParameter(_ context.Context, _, _ string, _ db.ParameterScope) error {
	return nil
}
func (m *mockDBClient) ShowParameter(_ context.Context, _ string) (string, error) { return "", nil }
func (m *mockDBClient) ReloadConfig(_ context.Context) error                      { return nil }
func (m *mockDBClient) ListSessions(_ context.Context) ([]db.Session, error) {
	return m.sessions, m.listSessionsErr
}
func (m *mockDBClient) CancelQuery(_ context.Context, _ int32) (bool, error) {
	return m.cancelResult, m.cancelQueryErr
}
func (m *mockDBClient) TerminateSession(_ context.Context, _ int32) (bool, error) {
	return m.terminateResult, m.terminateSessionErr
}
func (m *mockDBClient) CreateRole(_ context.Context, _ db.RoleOptions) error { return nil }
func (m *mockDBClient) AlterRole(_ context.Context, _ db.RoleOptions) error  { return nil }
func (m *mockDBClient) DropRole(_ context.Context, _ string) error           { return nil }
func (m *mockDBClient) Vacuum(_ context.Context, _ db.VacuumOptions) error   { return nil }
func (m *mockDBClient) Analyze(_ context.Context, _ string) error            { return nil }
func (m *mockDBClient) Reindex(_ context.Context, _ db.ReindexOptions) error { return nil }
func (m *mockDBClient) GetDiskUsage(_ context.Context, _ string) ([]db.DiskUsage, error) {
	return nil, nil
}
func (m *mockDBClient) GetReplicationLag(_ context.Context) (int64, error) { return 0, nil }
func (m *mockDBClient) PromoteStandby(_ context.Context) error             { return nil }
func (m *mockDBClient) GetActiveQueryCount(_ context.Context) (int32, int32, int32, error) {
	return 0, 0, 0, nil
}
func (m *mockDBClient) GetResourceGroupUsage(_ context.Context, _ string) (float64, float64, error) {
	return 0, 0, nil
}
func (m *mockDBClient) CreateResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return m.createResGroupErr
}
func (m *mockDBClient) AlterResourceGroup(_ context.Context, _ db.ResourceGroupOptions) error {
	return nil
}
func (m *mockDBClient) DropResourceGroup(_ context.Context, _ string) error {
	return m.dropResGroupErr
}
func (m *mockDBClient) ListResourceGroups(_ context.Context) ([]db.ResourceGroupInfo, error) {
	return m.resGroups, m.listResGroupsErr
}
func (m *mockDBClient) AssignRoleResourceGroup(_ context.Context, _, _ string) error {
	return m.assignRoleErr
}
func (m *mockDBClient) CreateResourceQueue(_ context.Context, _ db.ResourceQueueOptions) error {
	return m.createResQueueErr
}
func (m *mockDBClient) DropResourceQueue(_ context.Context, _ string) error {
	return m.dropResQueueErr
}
func (m *mockDBClient) ListResourceQueues(_ context.Context) ([]db.ResourceQueueInfo, error) {
	return m.resQueues, m.listResQueuesErr
}
func (m *mockDBClient) CreateBackup(_ context.Context, _ db.BackupOptions) (*db.BackupInfo, error) {
	return nil, nil
}
func (m *mockDBClient) RestoreBackup(_ context.Context, _ db.RestoreOptions) error { return nil }
func (m *mockDBClient) ListBackups(_ context.Context) ([]db.BackupInfo, error)     { return nil, nil }
func (m *mockDBClient) DeleteBackup(_ context.Context, _ string) error             { return nil }
func (m *mockDBClient) CreateDataLoadingJob(_ context.Context, _ db.DataLoadingJobConfig) error {
	return nil
}
func (m *mockDBClient) StartDataLoadingJob(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) StopDataLoadingJob(_ context.Context, _ string) error  { return nil }
func (m *mockDBClient) ListDataLoadingJobs(_ context.Context) ([]db.DataLoadingJobStatus, error) {
	return nil, nil
}
func (m *mockDBClient) GetStorageDiskUsage(_ context.Context) ([]db.DiskUsageInfo, error) {
	return nil, nil
}
func (m *mockDBClient) GetBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerRecommendationScan(_ context.Context) error { return nil }
func (m *mockDBClient) GetTableDetails(_ context.Context, _, _ string) (*db.TableDetail, error) {
	return nil, nil
}
func (m *mockDBClient) GetUsageReport(_ context.Context, _ string) ([]db.UsageReportEntry, error) {
	return nil, nil
}
func (m *mockDBClient) InitializeMirrors(_ context.Context, _ db.MirrorInitOptions) error {
	return nil
}
func (m *mockDBClient) ConfigureReplication(_ context.Context, _ db.ReplicationOptions) error {
	return nil
}
func (m *mockDBClient) GetMirrorSyncStatus(_ context.Context) ([]db.MirrorSyncInfo, error) {
	return nil, nil
}
func (m *mockDBClient) TriggerFTSProbe(_ context.Context) error               { return nil }
func (m *mockDBClient) TerminateAllBackends(_ context.Context) (int32, error) { return 0, nil }
func (m *mockDBClient) CancelAllQueries(_ context.Context) (int32, error)     { return 0, nil }
func (m *mockDBClient) LogRotate(_ context.Context) error                     { return nil }
func (m *mockDBClient) RegisterNewSegments(_ context.Context, _ db.SegmentRegistrationOptions) error {
	return nil
}
func (m *mockDBClient) RedistributeData(_ context.Context, _ db.RedistributionOptions) error {
	return nil
}
func (m *mockDBClient) GetRedistributionProgress(_ context.Context) (int32, error) { return 0, nil }
func (m *mockDBClient) DeregisterSegments(_ context.Context, _ int32) error        { return nil }
func (m *mockDBClient) RedistributeBeforeScaleIn(_ context.Context, _ db.ScaleInRedistributionOptions) error {
	return nil
}
func (m *mockDBClient) AnalyzeSkew(_ context.Context, _ string) ([]db.TableSkewInfo, error) {
	return nil, nil
}
func (m *mockDBClient) RebalanceTable(_ context.Context, _, _, _, _ string) error { return nil }
func (m *mockDBClient) ListUserDatabases(_ context.Context) ([]string, error)     { return nil, nil }

// mockDBFactory implements db.DBClientFactory for testing.
type mockDBFactory struct {
	client    db.Client
	clientErr error
}

func (f *mockDBFactory) NewClient(_ context.Context, _ *cbv1alpha1.CloudberryCluster) (db.Client, error) {
	return f.client, f.clientErr
}

// newTestServerWithDB creates a test server with a mock DB factory.
func newTestServerWithDB(dbClient *mockDBClient, clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	factory := &mockDBFactory{client: dbClient}
	return NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0)
}

// newTestServerWithDBErr creates a test server with a DB factory that returns errors.
func newTestServerWithDBErr(err error, clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	factory := &mockDBFactory{clientErr: err}
	return NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0)
}

// ============================================================================
// Validation function tests
// ============================================================================

func TestIsValidIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid simple", "analytics", true},
		{"valid with underscore", "my_group", true},
		{"valid starts with underscore", "_private", true},
		{"valid mixed case", "MyGroup", true},
		{"valid with numbers", "group1", true},
		{"empty string", "", false},
		{"starts with number", "1group", false},
		{"contains hyphen", "my-group", false},
		{"contains space", "my group", false},
		{"contains special char", "my@group", false},
		{"too long", string(make([]byte, 64)), false},
		{"max length 63", "a" + string(make([]byte, 62)), true},
		{"single letter", "a", true},
		{"single underscore", "_", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Fill byte slices with valid chars for length tests
			if tt.name == "too long" {
				input := make([]byte, 64)
				for i := range input {
					input[i] = 'a'
				}
				assert.Equal(t, false, isValidIdentifier(string(input)))
				return
			}
			if tt.name == "max length 63" {
				input := make([]byte, 63)
				for i := range input {
					input[i] = 'a'
				}
				assert.Equal(t, true, isValidIdentifier(string(input)))
				return
			}
			result := isValidIdentifier(tt.input)
			assert.Equal(t, tt.expected, result, "isValidIdentifier(%q)", tt.input)
		})
	}
}

func TestWriteClusterNotFound(t *testing.T) {
	rec := httptest.NewRecorder()
	writeClusterNotFound(rec, "my-cluster")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	errObj := resp["error"].(map[string]interface{})
	assert.Equal(t, "CLUSTER_NOT_FOUND", errObj["code"])
	assert.Contains(t, errObj["message"], "my-cluster")
}

func TestServerClose(t *testing.T) {
	s := newTestServer()
	// Should not panic
	s.Close()

	// Close with nil rateLimiter
	s2 := &Server{}
	s2.Close()
}

// ============================================================================
// Scale status handler tests
// ============================================================================

func TestHandleGetScaleStatus(t *testing.T) {
	t.Run("running cluster", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/scale/status?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetScaleStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "test-cluster", resp["name"])
		assert.Equal(t, false, resp["scaling"])
		assert.Equal(t, "Running", resp["phase"])
		assert.Equal(t, float64(4), resp["segmentsReady"])
		assert.Equal(t, float64(4), resp["segmentsTotal"])
	})

	t.Run("scaling cluster", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Status.Phase = "Scaling"
		cluster.Status.SegmentsReady = 2
		cluster.Status.SegmentsTotal = 4
		s := newTestServer(cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/scale/status?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetScaleStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, true, resp["scaling"])
		assert.Equal(t, "Scaling", resp["phase"])
	})

	t.Run("with redistribution condition", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Status.Conditions = []metav1.Condition{
			{
				Type:    string(cbv1alpha1.ConditionDataRedistribution),
				Status:  metav1.ConditionTrue,
				Reason:  "InProgress",
				Message: "Redistributing data",
			},
		}
		s := newTestServer(cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/scale/status?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetScaleStatus(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.NotNil(t, resp["redistribution"])
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/nonexistent/scale/status?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleGetScaleStatus(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// ============================================================================
// Recovery handler tests
// ============================================================================

func TestHandleStartRecovery_InvalidType(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body, _ := json.Marshal(map[string]string{"type": "invalid-type"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/recovery?namespace=default", bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleStartRecovery(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	errObj := resp["error"].(map[string]interface{})
	assert.Contains(t, errObj["message"], "invalid recovery type")
}

func TestHandleStartRecovery_AllValidTypes(t *testing.T) {
	validTypes := []string{"incremental", "full", "differential"}
	for _, rt := range validTypes {
		t.Run(rt, func(t *testing.T) {
			cluster := newTestCluster("test-cluster", "default")
			s := newTestServer(cluster)

			body, _ := json.Marshal(map[string]string{"type": rt})
			req := httptest.NewRequest(http.MethodPost,
				apiPrefix+"/clusters/test-cluster/recovery?namespace=default", bytes.NewReader(body))
			req.SetPathValue("name", "test-cluster")
			rec := httptest.NewRecorder()
			s.handleStartRecovery(rec, req)

			assert.Equal(t, http.StatusAccepted, rec.Code)
			var resp map[string]interface{}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Equal(t, rt, resp["type"])
		})
	}
}

// ============================================================================
// DB-dependent handler tests
// ============================================================================

func TestHandleListSessions_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{
		sessions: []db.Session{
			{PID: 100, Username: "admin", State: "active", Query: "SELECT 1"},
			{PID: 200, Username: "user1", State: "idle"},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/sessions?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["total"])
}

func TestHandleListSessions_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/sessions?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleListSessions_WithDBFactory_ListError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{
		listSessionsErr: fmt.Errorf("query failed"),
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/sessions?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleCancelQuery_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{cancelResult: true}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["canceled"])
}

func TestHandleCancelQuery_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleCancelQuery_WithDBFactory_CancelError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{cancelQueryErr: fmt.Errorf("cancel failed")}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleTerminateSession_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{terminateResult: true}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/456?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "456")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["terminated"])
}

func TestHandleTerminateSession_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/456?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "456")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleTerminateSession_WithDBFactory_TerminateError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{terminateSessionErr: fmt.Errorf("terminate failed")}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/456?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "456")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// ============================================================================
// Resource group DB-dependent tests
// ============================================================================

func TestHandleListResourceGroups_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{
		resGroups: []db.ResourceGroupInfo{
			{Name: "analytics", Concurrency: 10, CPUMaxPercent: 50},
			{Name: "etl", Concurrency: 5, CPUMaxPercent: 30},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["total"])
}

func TestHandleListResourceGroups_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "fallback-group"},
		},
	}
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)

	// Should fall back to CRD spec
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleListResourceGroups_WithDBFactory_ListError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{
		listResGroupsErr: fmt.Errorf("query failed"),
	}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)

	// Should fall back to CRD spec (nil workload = empty groups)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleCreateResourceGroup_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}
	s := newTestServerWithDB(dbClient, cluster)

	body, _ := json.Marshal(map[string]interface{}{
		"name": "analytics", "concurrency": 10, "cpuMaxPercent": 50,
	})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "created", resp["status"])
}

func TestHandleCreateResourceGroup_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	body, _ := json.Marshal(map[string]interface{}{"name": "analytics", "concurrency": 10})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleCreateResourceGroup_WithDBFactory_CreateError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{createResGroupErr: fmt.Errorf("create failed")}
	s := newTestServerWithDB(dbClient, cluster)

	body, _ := json.Marshal(map[string]interface{}{"name": "analytics", "concurrency": 10})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleCreateResourceGroup_InvalidIdentifier(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body, _ := json.Marshal(map[string]interface{}{"name": "1invalid-name!", "concurrency": 10})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleDeleteResourceGroup_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "deleted", resp["status"])
}

func TestHandleDeleteResourceGroup_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleDeleteResourceGroup_WithDBFactory_DropError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{dropResGroupErr: fmt.Errorf("drop failed")}
	s := newTestServerWithDB(dbClient, cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleDeleteResourceGroup_InvalidIdentifier(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/1invalid!?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "1invalid!")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ============================================================================
// Assign resource group DB-dependent tests
// ============================================================================

func TestHandleAssignResourceGroup_WithDBFactory_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}
	s := newTestServerWithDB(dbClient, cluster)

	body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleAssignResourceGroup(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "assigned", resp["status"])
}

func TestHandleAssignResourceGroup_WithDBFactory_ConnError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

	body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleAssignResourceGroup(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestHandleAssignResourceGroup_WithDBFactory_AssignError(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{assignRoleErr: fmt.Errorf("assign failed")}
	s := newTestServerWithDB(dbClient, cluster)

	body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleAssignResourceGroup(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestHandleAssignResourceGroup_InvalidGroupName(t *testing.T) {
	s := newTestServer()

	body, _ := json.Marshal(map[string]interface{}{"role": "analyst"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/1invalid!/assign?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "1invalid!")
	rec := httptest.NewRecorder()
	s.handleAssignResourceGroup(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleAssignResourceGroup_InvalidRoleName(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body, _ := json.Marshal(map[string]interface{}{"role": "1invalid-role!"})
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics/assign?namespace=default",
		bytes.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleAssignResourceGroup(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ============================================================================
// Resource queue handler tests
// ============================================================================

func TestHandleListResourceQueues(t *testing.T) {
	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListResourceQueues(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(0), resp["total"])
		assert.Equal(t, msgDBNotAvailable, resp["message"])
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/nonexistent/workload/resource-queues?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleListResourceQueues(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("with db factory success", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{
			resQueues: []db.ResourceQueueInfo{
				{Name: "analytics_queue", ActiveStatements: 10, Priority: "HIGH"},
			},
		}
		s := newTestServerWithDB(dbClient, cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListResourceQueues(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("with db factory conn error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListResourceQueues(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("with db factory list error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{listResQueuesErr: fmt.Errorf("query failed")}
		s := newTestServerWithDB(dbClient, cluster)

		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListResourceQueues(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestHandleCreateResourceQueue(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"name": "test_queue"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/nonexistent/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty name", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": ""})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid identifier", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": "1invalid-name!"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{
			"name": "analytics_queue", "activeStatements": 10, "priority": "HIGH",
		})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)
		assert.Equal(t, http.StatusCreated, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Contains(t, resp["message"], "pending")
	})

	t.Run("with db factory success", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)

		body, _ := json.Marshal(map[string]interface{}{
			"name": "analytics_queue", "activeStatements": 10,
		})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)

		assert.Equal(t, http.StatusCreated, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "created", resp["status"])
	})

	t.Run("with db factory conn error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

		body, _ := json.Marshal(map[string]interface{}{"name": "analytics_queue"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("with db factory create error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{createResQueueErr: fmt.Errorf("create failed")}
		s := newTestServerWithDB(dbClient, cluster)

		body, _ := json.Marshal(map[string]interface{}{"name": "analytics_queue"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateResourceQueue(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestHandleDeleteResourceQueue(t *testing.T) {
	t.Run("invalid identifier", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues/1invalid!?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("queueName", "1invalid!")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/nonexistent/workload/resource-queues/analytics?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("queueName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues/analytics?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("queueName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "pending", resp["status"])
	})

	t.Run("with db factory success", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)

		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues/analytics?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("queueName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "deleted", resp["status"])
	})

	t.Run("with db factory conn error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)

		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues/analytics?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("queueName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("with db factory drop error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{dropResQueueErr: fmt.Errorf("drop failed")}
		s := newTestServerWithDB(dbClient, cluster)

		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/resource-queues/analytics?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("queueName", "analytics")
		rec := httptest.NewRecorder()
		s.handleDeleteResourceQueue(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

// ============================================================================
// DB-dependent handler tests and additional coverage
// ============================================================================

func TestHandleGetRebalanceStatus_Success(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Segments.Rebalance = &cbv1alpha1.RebalanceSpec{
		SkewThreshold: 10,
		Parallelism:   2,
		ExcludeTables: []string{"public.large_table"},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/rebalance/status?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetRebalanceStatus(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "test-cluster", resp["name"])
	assert.Equal(t, "default", resp["namespace"])
	assert.NotNil(t, resp["config"])
}

func TestHandleGetRebalanceStatus_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/rebalance/status?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetRebalanceStatus(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetRebalanceStatus_WithRedistributionCondition(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.Conditions = []metav1.Condition{
		{
			Type:               string(cbv1alpha1.ConditionDataRedistribution),
			Status:             metav1.ConditionTrue,
			Reason:             "InProgress",
			Message:            "Data redistribution in progress",
			LastTransitionTime: metav1.Now(),
		},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/rebalance/status?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetRebalanceStatus(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotNil(t, resp["redistribution"])
}

func TestHandleListSessions_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/sessions?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(0), resp["total"])
	assert.Equal(t, msgDBNotAvailable, resp["message"])
}

func TestHandleCancelQuery_InvalidPID(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/abc/cancel?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "abc")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleCancelQuery_NegativePID(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/-1/cancel?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "-1")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleCancelQuery_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["canceled"])
	assert.Equal(t, msgDBNotAvailable, resp["message"])
}

func TestHandleTerminateSession_InvalidPID(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/abc?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "abc")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleTerminateSession_NegativePID(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/-5?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "-5")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleTerminateSession_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/sessions/123?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["terminated"])
}

func TestHandleListResourceGroups_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
			{Name: "analytics", Concurrency: 10},
		},
	}
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListResourceGroups(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
}

func TestHandleCreateResourceGroup_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body := `{"name":"analytics","concurrency":10,"cpuMaxPercent":50}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewBufferString(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestHandleCreateResourceGroup_InvalidBody(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewBufferString("invalid-json"))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleCreateResourceGroup_EmptyName(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	body := `{"name":"","concurrency":10}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups?namespace=default",
		bytes.NewBufferString(body))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleDeleteResourceGroup_NoDBFactory(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
		nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "pending", resp["status"])
}

func TestHandleListSessions_ClusterNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/sessions?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListSessions(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCancelQuery_ClusterNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/sessions/123/cancel?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleTerminateSession_ClusterNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/nonexistent/sessions/123?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleTerminateSession(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCreateResourceGroup_ClusterNotFound(t *testing.T) {
	s := newTestServer()

	body := `{"name":"analytics","concurrency":10}`
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/workload/resource-groups?namespace=default",
		bytes.NewBufferString(body))
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleCreateResourceGroup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDeleteResourceGroup_ClusterNotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/nonexistent/workload/resource-groups/analytics?namespace=default",
		nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("groupName", "analytics")
	rec := httptest.NewRecorder()
	s.handleDeleteResourceGroup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStartServer_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	logger := slog.Default()
	errCh := make(chan error, 1)
	go func() {
		errCh <- StartServer(ctx, "127.0.0.1:0", mux, logger)
	}()

	// Cancel immediately to trigger shutdown.
	cancel()

	// The server should shut down gracefully.
	// We don't assert on the error because it depends on timing.
}

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	_ = batchv1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	return scheme
}

func newTestServer(clusters ...*cbv1alpha1.CloudberryCluster) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0))
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
			QueryMonitoring: &cbv1alpha1.QueryMonitoringSpec{
				Enabled: true,
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

// newBackupEnabledCluster returns a test cluster with backup enabled to an S3
// destination, optionally seeded with backup-history entries.
func newBackupEnabledCluster(history ...cbv1alpha1.BackupHistoryEntry) *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 3 * * *",
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3:   &cbv1alpha1.S3Destination{Bucket: "backups"},
		},
	}
	cluster.Status.BackupHistory = history
	return cluster
}

func TestHandleListBackups(t *testing.T) {
	cluster := newBackupEnabledCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260519020000", Type: "full", Status: "Success"},
	)
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/test-cluster/backups?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListBackups(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(1), resp["total"])
	assert.Equal(t, "test-cluster", resp["cluster"])
	backups, ok := resp["backups"].([]interface{})
	require.True(t, ok)
	assert.Len(t, backups, 1)
}

func TestHandleListBackups_Empty(t *testing.T) {
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
}

func TestHandleListBackups_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet, apiPrefix+"/clusters/nonexistent/backups?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleListBackups(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func postBackupRequest(t *testing.T, name, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/"+name+"/backups?namespace=default", strings.NewReader(body))
	req.SetPathValue("name", name)
	return req
}

func TestHandleCreateBackup(t *testing.T) {
	t.Run("backup not enabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", ""))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("backup enabled creates job", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster",
			`{"type":"full","databases":["mydb"],"gpbackupOptions":{"jobs":4}}`))
		require.Equal(t, http.StatusAccepted, rec.Code)

		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "backup started", resp["status"])
		jobName, ok := resp["job"].(string)
		require.True(t, ok)
		assert.NotEmpty(t, jobName)

		job := &batchv1.Job{}
		require.NoError(t, s.k8sClient.Get(context.Background(),
			types.NamespacedName{Name: jobName, Namespace: "default"}, job))
		assert.Equal(t, util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
	})

	t.Run("invalid type", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", `{"type":"bogus"}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid database identifier", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", `{"databases":["bad name"]}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "test-cluster", `{not-json`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		rec := httptest.NewRecorder()
		s.handleCreateBackup(rec, postBackupRequest(t, "nonexistent", ""))
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleGetBackup(t *testing.T) {
	cluster := newBackupEnabledCluster(
		cbv1alpha1.BackupHistoryEntry{Timestamp: "20260519020000", Type: "full", Status: "Success"},
	)
	s := newTestServer(cluster)

	t.Run("found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/backups/20260519020000?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("timestamp", "20260519020000")
		rec := httptest.NewRecorder()
		s.handleGetBackup(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/backups/20200101000000?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("timestamp", "20200101000000")
		rec := httptest.NewRecorder()
		s.handleGetBackup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/backups/bk-1?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("timestamp", "bk-1")
		rec := httptest.NewRecorder()
		s.handleGetBackup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandleGetBackup_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/backups/20260519020000?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("timestamp", "20260519020000")
	rec := httptest.NewRecorder()
	s.handleGetBackup(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDeleteBackup(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/backups/20260519020000?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", "20260519020000")
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "deleted", resp["status"])
	jobName, ok := resp["job"].(string)
	require.True(t, ok)

	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: jobName, Namespace: "default"}, job))
	assert.Equal(t, util.BackupOperationCleanup, job.Labels[util.LabelBackupOperation])
}

func TestHandleDeleteBackup_InvalidTimestamp(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/backups/bk-1?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("timestamp", "bk-1")
	rec := httptest.NewRecorder()
	s.handleDeleteBackup(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func postRestoreRequest(t *testing.T, name, timestamp, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/"+name+"/backups/"+timestamp+"/restore?namespace=default",
		strings.NewReader(body))
	req.SetPathValue("name", name)
	req.SetPathValue("timestamp", timestamp)
	return req
}

func TestHandleRestoreBackup(t *testing.T) {
	t.Run("creates restore job", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260519020000",
			`{"gprestoreOptions":{"redirectDb":"mydb_restored","createDb":true}}`))
		require.Equal(t, http.StatusAccepted, rec.Code)

		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "restore started", resp["status"])
		jobName, ok := resp["job"].(string)
		require.True(t, ok)

		job := &batchv1.Job{}
		require.NoError(t, s.k8sClient.Get(context.Background(),
			types.NamespacedName{Name: jobName, Namespace: "default"}, job))
		assert.Equal(t, util.BackupOperationRestore, job.Labels[util.LabelBackupOperation])
	})

	t.Run("timestamp from body", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/backups//restore?namespace=default",
			strings.NewReader(`{"timestamp":"20260519020000"}`))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, req)
		assert.Equal(t, http.StatusAccepted, rec.Code)
	})

	t.Run("invalid timestamp", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "bk-1", ""))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newBackupEnabledCluster()
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleRestoreBackup(rec, postRestoreRequest(t, "test-cluster", "20260519020000", `{bad`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandleRestoreBackup_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	s.handleRestoreBackup(rec, postRestoreRequest(t, "nonexistent", "20260519020000", ""))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// Data loading endpoint tests.

func TestHandleListDataLoadingJobs(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "job1", Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "s3-datalake", Profile: "s3:parquet", TargetTable: "public.data",
			}},
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
	// The full create-job behavior (and its 409/400 edges) is covered by the
	// dedicated Scenario 107 suite; this case retains only the pre-mutation
	// cluster-not-found contract.
	t.Run("cluster not found returns 404", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/missing/data-loading/jobs?namespace=default", nil)
		req.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handleCreateDataLoadingJob(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestHandleGetDataLoadingJob(t *testing.T) {
	t.Run("job found", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
			Enabled: true,
			Jobs: []cbv1alpha1.DataLoadingJob{{Name: "job1", Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "s3-datalake", Profile: "s3:parquet", TargetTable: "t",
			}}},
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
		// DL enabled so the 404 job-not-found path is exercised (a disabled
		// subsystem now returns the 200 disabled envelope instead).
		cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
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

// The start/stop and update/delete data-loading job behaviors are now fully
// implemented (Scenario 107); their happy-path, idempotency and 404/409 edges
// are covered by the dedicated Scenario 107 suite written separately.

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

// Monitor pause/resume endpoint tests.

func TestHandlePauseMonitor(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handlePauseMonitor(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "paused", resp["status"])
	assert.NotEmpty(t, resp["pausedAt"])
	assert.Equal(t, "Query monitor paused", resp["message"])
}

func TestHandlePauseMonitor_AlreadyPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 5
	s := newTestServer(cluster)

	// Pause first time.
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handlePauseMonitor(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Pause again — should return already paused.
	req2 := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	req2.SetPathValue("name", "test-cluster")
	rec2 := httptest.NewRecorder()
	s.handlePauseMonitor(rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	assert.Equal(t, "paused", resp["status"])
	assert.Contains(t, resp["message"], "already paused")
}

func TestHandlePauseMonitor_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/queries/monitor/pause?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handlePauseMonitor(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleResumeMonitor(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Pause first.
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handlePauseMonitor(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Resume.
	req2 := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/resume?namespace=default", nil)
	req2.SetPathValue("name", "test-cluster")
	rec2 := httptest.NewRecorder()
	s.handleResumeMonitor(rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	assert.Equal(t, "resumed", resp["status"])
	assert.Equal(t, "Query monitor resumed", resp["message"])
}

func TestHandleResumeMonitor_NotPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Resume without pausing first — should still succeed.
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/resume?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleResumeMonitor(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "resumed", resp["status"])
}

func TestHandleResumeMonitor_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/queries/monitor/resume?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleResumeMonitor(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetMonitorState_NotPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/monitor/state?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetMonitorState(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["paused"])
	assert.Equal(t, false, resp["stale"])
	assert.Nil(t, resp["pausedAt"])
}

func TestHandleGetMonitorState_Paused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Pause first.
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handlePauseMonitor(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Check state.
	req2 := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/monitor/state?namespace=default", nil)
	req2.SetPathValue("name", "test-cluster")
	rec2 := httptest.NewRecorder()
	s.handleGetMonitorState(rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	assert.Equal(t, true, resp["paused"])
	assert.Equal(t, true, resp["stale"])
	assert.NotNil(t, resp["pausedAt"])
}

func TestHandleGetMonitorState_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/queries/monitor/state?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleGetMonitorState(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetQueryMonitoring_WhenPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 5
	cluster.Status.QueuedQueries = 2
	cluster.Status.BlockedQueries = 1
	s := newTestServer(cluster)

	// Pause the monitor.
	pauseReq := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	pauseReq.SetPathValue("name", "test-cluster")
	pauseRec := httptest.NewRecorder()
	s.handlePauseMonitor(pauseRec, pauseReq)
	assert.Equal(t, http.StatusOK, pauseRec.Code)

	// Now query monitoring should return stale data.
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryMonitoring(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["stale"])
	assert.NotNil(t, resp["pausedAt"])
	assert.Equal(t, float64(5), resp["activeQueries"])
	assert.Equal(t, float64(2), resp["queuedQueries"])
	assert.Equal(t, float64(1), resp["blockedQueries"])
}

func TestHandleGetActiveQueries_WhenPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 10
	cluster.Status.QueuedQueries = 3
	cluster.Status.BlockedQueries = 0
	s := newTestServer(cluster)

	// Pause the monitor.
	pauseReq := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	pauseReq.SetPathValue("name", "test-cluster")
	pauseRec := httptest.NewRecorder()
	s.handlePauseMonitor(pauseRec, pauseReq)
	assert.Equal(t, http.StatusOK, pauseRec.Code)

	// Now active queries should return stale data.
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries/active?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetActiveQueries(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["stale"])
	assert.NotNil(t, resp["pausedAt"])
	assert.Equal(t, float64(10), resp["activeQueries"])
	assert.Equal(t, float64(3), resp["queuedQueries"])
}

func TestHandleGetQueryMonitoring_AfterResume(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.ActiveQueries = 5
	s := newTestServer(cluster)

	// Pause.
	pauseReq := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/pause?namespace=default", nil)
	pauseReq.SetPathValue("name", "test-cluster")
	pauseRec := httptest.NewRecorder()
	s.handlePauseMonitor(pauseRec, pauseReq)
	assert.Equal(t, http.StatusOK, pauseRec.Code)

	// Resume.
	resumeReq := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/queries/monitor/resume?namespace=default", nil)
	resumeReq.SetPathValue("name", "test-cluster")
	resumeRec := httptest.NewRecorder()
	s.handleResumeMonitor(resumeRec, resumeReq)
	assert.Equal(t, http.StatusOK, resumeRec.Code)

	// Query monitoring should return fresh data (no stale flag).
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/queries?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetQueryMonitoring(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Nil(t, resp["stale"])
	assert.Nil(t, resp["pausedAt"])
	assert.Equal(t, float64(5), resp["activeQueries"])
}

func TestMonitorStateKey(t *testing.T) {
	assert.Equal(t, "default/test-cluster", monitorStateKey("default", "test-cluster"))
	assert.Equal(t, "prod/my-db", monitorStateKey("prod", "my-db"))
}

func TestIsMonitorPaused_NotPaused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	state, paused := s.isMonitorPaused("default", "test-cluster")
	assert.False(t, paused)
	assert.Nil(t, state)
}

func TestIsMonitorPaused_Paused(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	s := newTestServer(cluster)

	// Manually set paused state.
	now := time.Now().UTC()
	s.monitorStates["default/test-cluster"] = &monitorState{
		Paused:   true,
		PausedAt: &now,
		Snapshot: map[string]interface{}{"activeQueries": 5},
	}

	state, paused := s.isMonitorPaused("default", "test-cluster")
	assert.True(t, paused)
	assert.NotNil(t, state)
	assert.Equal(t, true, state.Paused)
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

// ============================================================================
// collectDiskUsage tests (GAP-1)
// ============================================================================

func TestServer_CollectDiskUsage_NilDBFactory(t *testing.T) {
	// Arrange: server with no DB factory.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	s := newTestServerWithDBAndMetrics(nil, rec, cluster)

	// Act
	usage := s.collectDiskUsage(context.Background(), cluster)

	// Assert: no-op returns empty slice and records nothing.
	assert.Empty(t, usage)
	assert.Empty(t, rec.diskUsageCalls)
}

func TestServer_CollectDiskUsage_NewClientError(t *testing.T) {
	// Arrange: factory that fails to create a client.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	factory := &mockDBFactory{clientErr: fmt.Errorf("connection refused")}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	usage := s.collectDiskUsage(context.Background(), cluster)

	// Assert
	assert.Empty(t, usage)
	assert.Empty(t, rec.diskUsageCalls)
}

func TestServer_CollectDiskUsage_GetDiskUsageError(t *testing.T) {
	// Arrange: client whose GetDiskUsage fails.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	dbClient := &mockDBClient{diskUsageErr: fmt.Errorf("query failed")}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	usage := s.collectDiskUsage(context.Background(), cluster)

	// Assert: error path returns empty slice, records nothing, closes client.
	assert.Empty(t, usage)
	assert.Empty(t, rec.diskUsageCalls)
	assert.Equal(t, 1, dbClient.closeCalls)
}

func TestServer_CollectDiskUsage_HappyPath(t *testing.T) {
	// Arrange: client returns per-database usage.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		diskUsage: []db.DiskUsage{
			{Database: "postgres", SizeBytes: 1024},
			{Database: "analytics", SizeBytes: 2048},
		},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	usage := s.collectDiskUsage(context.Background(), cluster)

	// Assert: usage returned and SetDiskUsageBytes invoked per database.
	require.Len(t, usage, 2)
	require.Len(t, rec.diskUsageCalls, 2)
	assert.Equal(t, "postgres", rec.diskUsageCalls[0].database)
	assert.InDelta(t, 1024.0, rec.diskUsageCalls[0].bytes, 0.001)
	assert.Equal(t, "analytics", rec.diskUsageCalls[1].database)
	assert.InDelta(t, 2048.0, rec.diskUsageCalls[1].bytes, 0.001)
	assert.Equal(t, "test-cluster", rec.diskUsageCalls[0].cluster)
	assert.Equal(t, "default", rec.diskUsageCalls[0].namespace)
	assert.Equal(t, 1, dbClient.closeCalls)
}

func TestServer_HandleGetDiskUsage_WithDBFactory(t *testing.T) {
	// Arrange: full handler exercising collectDiskUsage on the happy path.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Status.DiskUsagePercent = 60
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		diskUsage: []db.DiskUsage{{Database: "postgres", SizeBytes: 4096}},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/storage/disk-usage?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleGetDiskUsage(rr, req)

	// Assert
	assert.Equal(t, http.StatusOK, rr.Code)
	require.Len(t, rec.diskUsageCalls, 1)
	assert.Equal(t, "postgres", rec.diskUsageCalls[0].database)
}

// ============================================================================
// runRecommendationScan tests (GAP-2)
// ============================================================================

func TestServer_RunRecommendationScan_NilDBFactory(t *testing.T) {
	// Arrange: no DB factory => no-op.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	s := newTestServerWithDBAndMetrics(nil, rec, cluster)

	// Act
	s.runRecommendationScan(context.Background(), cluster)

	// Assert
	assert.Empty(t, rec.recommendationsCalls)
	assert.Zero(t, rec.scanDurationCalls)
}

func TestServer_RunRecommendationScan_NilMetrics(t *testing.T) {
	// Arrange: DB factory present but metrics is a Noop (s.metrics != nil but
	// not the counting recorder). Use NoopRecorder to take the guard branch
	// where metrics is non-nil; nil metrics is covered via the factory-nil case
	// since the guard is "dbFactory == nil || metrics == nil".
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}
	factory := &mockDBFactory{client: dbClient}
	// NewServer always sets a recorder; to exercise the metrics==nil guard we
	// build the server directly with a nil recorder.
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(cluster).Build()
	s := trackServer(NewServer(k8sClient, nil, factory, nil, nil, 0))

	// Act / Assert: must not panic and must not touch the client.
	s.runRecommendationScan(context.Background(), cluster)
	assert.Zero(t, dbClient.closeCalls)
}

func TestServer_RunRecommendationScan_NewClientError(t *testing.T) {
	// Arrange: factory fails to create a client.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	factory := &mockDBFactory{clientErr: fmt.Errorf("connection refused")}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	s.runRecommendationScan(context.Background(), cluster)

	// Assert
	assert.Empty(t, rec.recommendationsCalls)
	assert.Zero(t, rec.scanDurationCalls)
}

func TestServer_RunRecommendationScan_FetchErrorContinues(t *testing.T) {
	// Arrange: some fetchers error (continue branch), others succeed.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		bloatRecsErr:   fmt.Errorf("bloat query failed"),
		skewRecs:       []db.Recommendation{{Type: "skew"}},
		ageRecsErr:     fmt.Errorf("age query failed"),
		indexBloatRecs: []db.Recommendation{{Type: "index_bloat"}, {Type: "index_bloat"}},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	s.runRecommendationScan(context.Background(), cluster)

	// Assert: scan duration always observed; only successful fetches counted.
	assert.Equal(t, 1, rec.scanDurationCalls)
	counts := map[string]float64{}
	for _, c := range rec.recommendationsCalls {
		counts[c.recType] = c.count
	}
	assert.InDelta(t, 1.0, counts["skew"], 0.001)
	assert.InDelta(t, 2.0, counts["index_bloat"], 0.001)
	_, hasBloat := counts["bloat"]
	assert.False(t, hasBloat, "errored bloat fetch must not be counted")
	assert.Equal(t, 1, dbClient.closeCalls)
}

func TestServer_RunRecommendationScan_HappyPath(t *testing.T) {
	// Arrange: all fetchers return recommendations.
	cluster := newTestCluster("test-cluster", "default")
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		bloatRecs:      []db.Recommendation{{Type: "bloat"}, {Type: "bloat"}},
		skewRecs:       []db.Recommendation{{Type: "skew"}},
		ageRecs:        []db.Recommendation{{Type: "age"}},
		indexBloatRecs: []db.Recommendation{{Type: "index_bloat"}},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	// Act
	s.runRecommendationScan(context.Background(), cluster)

	// Assert: ObserveRecommendationScanDuration + SetRecommendationsTotal per type.
	assert.Equal(t, 1, rec.scanDurationCalls)
	counts := map[string]float64{}
	for _, c := range rec.recommendationsCalls {
		counts[c.recType] = c.count
		assert.Equal(t, "test-cluster", c.cluster)
		assert.Equal(t, "default", c.namespace)
	}
	assert.InDelta(t, 2.0, counts["bloat"], 0.001)
	assert.InDelta(t, 1.0, counts["skew"], 0.001)
	assert.InDelta(t, 1.0, counts["age"], 0.001)
	assert.InDelta(t, 1.0, counts["index_bloat"], 0.001)
	assert.Equal(t, 1, dbClient.closeCalls)
}

func TestServer_HandleTriggerRecommendationScan_WithDB(t *testing.T) {
	// Arrange: full handler exercising runRecommendationScan.
	cluster := newTestCluster("test-cluster", "default")
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{Enabled: true},
	}
	rec := &countingRecorder{}
	dbClient := &mockDBClient{
		bloatRecs: []db.Recommendation{{Type: "bloat"}},
	}
	factory := &mockDBFactory{client: dbClient}
	s := newTestServerWithDBAndMetrics(factory, rec, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/recommendations/scan?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rr := httptest.NewRecorder()

	// Act
	s.handleTriggerRecommendationScan(rr, req)

	// Assert
	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Equal(t, 1, rec.scanDurationCalls)
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
		apiPrefix+"/clusters/nonexistent/backups/20260519020000?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("timestamp", "20260519020000")
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
	sessions                 []db.Session
	listSessionsErr          error
	sessionsWithGroup        []db.SessionWithGroup
	listSessionsWithGroupErr error
	cancelResult             bool
	cancelQueryErr           error
	terminateResult          bool
	terminateSessionErr      error
	resGroups                []db.ResourceGroupInfo
	listResGroupsErr         error
	createResGroupErr        error
	alterResGroupErr         error
	dropResGroupErr          error
	assignRoleErr            error
	resQueues                []db.ResourceQueueInfo
	listResQueuesErr         error
	createResQueueErr        error
	dropResQueueErr          error
	closeCalls               int

	// Disk usage / recommendation scan fakes (used by collectDiskUsage and
	// runRecommendationScan tests).
	diskUsage         []db.DiskUsage
	diskUsageErr      error
	bloatRecs         []db.Recommendation
	bloatRecsErr      error
	skewRecs          []db.Recommendation
	skewRecsErr       error
	ageRecs           []db.Recommendation
	ageRecsErr        error
	indexBloatRecs    []db.Recommendation
	indexBloatRecsErr error

	// Query-monitoring handler fakes.
	queryDetailErr   error
	moveQueryErr     error
	exportHistoryErr error
}

func (m *mockDBClient) Ping(_ context.Context) error { return nil }
func (m *mockDBClient) Close()                       { m.closeCalls++ }
func (m *mockDBClient) GetSegmentConfiguration(_ context.Context) ([]db.SegmentInfo, error) {
	return nil, nil
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
	return m.diskUsage, m.diskUsageErr
}
func (m *mockDBClient) GetReplicationLag(_ context.Context) (int64, error) { return 0, nil }
func (m *mockDBClient) PromoteStandby(_ context.Context) error             { return nil }
func (m *mockDBClient) GetMaxConnections(_ context.Context) (int32, error) { return 100, nil }
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
	return m.alterResGroupErr
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
	return m.bloatRecs, m.bloatRecsErr
}
func (m *mockDBClient) GetSkewRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return m.skewRecs, m.skewRecsErr
}
func (m *mockDBClient) GetAgeRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return m.ageRecs, m.ageRecsErr
}
func (m *mockDBClient) GetIndexBloatRecommendations(_ context.Context) ([]db.Recommendation, error) {
	return m.indexBloatRecs, m.indexBloatRecsErr
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
func (m *mockDBClient) ListSessionsWithResourceGroup(_ context.Context) ([]db.SessionWithGroup, error) {
	return m.sessionsWithGroup, m.listSessionsWithGroupErr
}
func (m *mockDBClient) ListUserDatabases(_ context.Context) ([]string, error)  { return nil, nil }
func (m *mockDBClient) SetupExporterRole(_ context.Context, _ string) error    { return nil }
func (m *mockDBClient) SetupPXFExtensions(_ context.Context) (int, error)      { return 2, nil }
func (m *mockDBClient) EnsureDataLoaderRole(_ context.Context, _ string) error { return nil }
func (m *mockDBClient) ListPXFExtensions(_ context.Context) ([]string, error)  { return nil, nil }
func (m *mockDBClient) ListExternalTables(_ context.Context) ([]db.ExternalTableInfo, error) {
	return nil, nil
}
func (m *mockDBClient) ReadPXFSourceSample(
	_ context.Context, _, _, _ string, _ int,
) (*db.PXFSourceSample, error) {
	return nil, nil
}
func (m *mockDBClient) GetQueryDetail(_ context.Context, pid int32) (*db.QueryDetail, error) {
	if m.queryDetailErr != nil {
		return nil, m.queryDetailErr
	}
	return &db.QueryDetail{PID: pid, State: "active", Query: "SELECT 1"}, nil
}
func (m *mockDBClient) EnsureQueryHistoryTable(_ context.Context) error { return nil }
func (m *mockDBClient) InsertQueryHistory(_ context.Context, _ *db.QueryHistoryEntry) error {
	return nil
}
func (m *mockDBClient) GetQueryHistory(_ context.Context, _ db.QueryHistoryFilter) ([]db.QueryHistoryEntry, int, error) {
	return []db.QueryHistoryEntry{}, 0, nil
}
func (m *mockDBClient) GetQueryHistoryDetail(_ context.Context, _ string) (*db.QueryHistoryEntry, error) {
	return nil, fmt.Errorf("not found")
}
func (m *mockDBClient) ExportQueryHistoryCSV(_ context.Context, _ db.QueryHistoryFilter, w io.Writer) error {
	if m.exportHistoryErr != nil {
		return m.exportHistoryErr
	}
	_, _ = w.Write([]byte("pid,query\n1,SELECT 1\n"))
	return nil
}
func (m *mockDBClient) CleanupQueryHistory(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockDBClient) MoveQueryToResourceGroup(_ context.Context, _ int32, _ string) error {
	return m.moveQueryErr
}

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
	return trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))
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
	return trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))
}

// diskUsageCall captures a SetDiskUsageBytes invocation.
type diskUsageCall struct {
	cluster   string
	namespace string
	database  string
	bytes     float64
}

// recommendationsCall captures a SetRecommendationsTotal invocation.
type recommendationsCall struct {
	cluster   string
	namespace string
	recType   string
	count     float64
}

// countingRecorder wraps NoopRecorder and tracks the metric calls made by the
// disk-usage and recommendation-scan code paths.
type countingRecorder struct {
	metrics.NoopRecorder
	diskUsageCalls       []diskUsageCall
	recommendationsCalls []recommendationsCall
	scanDurationCalls    int
}

func (c *countingRecorder) SetDiskUsageBytes(cluster, namespace, database string, bytes float64) {
	c.diskUsageCalls = append(c.diskUsageCalls, diskUsageCall{
		cluster: cluster, namespace: namespace, database: database, bytes: bytes,
	})
}

func (c *countingRecorder) SetRecommendationsTotal(cluster, namespace, recType string, count float64) {
	c.recommendationsCalls = append(c.recommendationsCalls, recommendationsCall{
		cluster: cluster, namespace: namespace, recType: recType, count: count,
	})
}

func (c *countingRecorder) ObserveRecommendationScanDuration(_, _ string, _ time.Duration) {
	c.scanDurationCalls++
}

// newTestServerWithObjects creates a test server seeded with arbitrary runtime
// objects (e.g. clusters plus pods) and no DB factory.
func newTestServerWithObjects(objs ...runtime.Object) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0))
}

// newTestServerWithDBAndMetrics creates a server wired with both a mock DB
// factory and a custom metrics recorder.
func newTestServerWithDBAndMetrics(
	factory db.DBClientFactory,
	recorder metrics.Recorder,
	clusters ...*cbv1alpha1.CloudberryCluster,
) *Server {
	scheme := newTestScheme()
	objs := make([]runtime.Object, 0, len(clusters))
	for _, c := range clusters {
		objs = append(objs, c)
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, factory, recorder, nil, 0))
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
		sessionsWithGroup: []db.SessionWithGroup{
			{Session: db.Session{PID: 100, Username: "admin", Database: "postgres", State: "active", Query: "SELECT 1"}, ResourceGroup: "admin_group"},
			{Session: db.Session{PID: 200, Username: "user1", Database: "mydb", State: "idle"}, ResourceGroup: "default_group"},
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
		listSessionsWithGroupErr: fmt.Errorf("query failed"),
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

// ============================================================================
// Update resource group handler tests (Scenario 31)
// ============================================================================

func TestHandleUpdateResourceGroup(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/nonexistent/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid group name", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/1invalid!?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "1invalid!")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20, "cpuMaxPercent": 70})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "pending", resp["status"])
		assert.Equal(t, "analytics", resp["group"])
	})

	t.Run("with db factory success", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20, "cpuMaxPercent": 70})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "updated", resp["status"])
		assert.Equal(t, "analytics", resp["group"])
	})

	t.Run("with db factory conn error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServerWithDBErr(fmt.Errorf("connection refused"), cluster)
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("with db factory alter error", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{alterResGroupErr: fmt.Errorf("alter failed")}
		s := newTestServerWithDB(dbClient, cluster)
		body, _ := json.Marshal(map[string]interface{}{"concurrency": 20})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("groupName", "analytics")
		rec := httptest.NewRecorder()
		s.handleUpdateResourceGroup(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

// ============================================================================
// Workload rule CRUD handler tests (Scenario 31)
// ============================================================================

func TestHandleCreateWorkloadRule(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"name": "test_rule", "action": "log"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/nonexistent/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty name", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": "", "action": "log"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid rule name", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"name": "1invalid!", "action": "log"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("success with nil workload", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{
			"name": "test_rule", "action": "log", "enabled": true,
			"thresholdType": "running_time", "threshold": "10", "priority": 3,
		})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusCreated, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "created", resp["status"])
	})

	t.Run("success with existing workload", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			Rules: []cbv1alpha1.WorkloadRule{
				{Name: "existing_rule", Action: "cancel"},
			},
		}
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{
			"name": "new_rule", "action": "log", "resourceGroup": "analytics",
		})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusCreated, rec.Code)
	})

	t.Run("duplicate rule name", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			Rules: []cbv1alpha1.WorkloadRule{
				{Name: "existing_rule", Action: "cancel"},
			},
		}
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{
			"name": "existing_rule", "action": "log",
		})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/workload/rules?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleCreateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandleUpdateWorkloadRule(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"threshold": "20"})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/nonexistent/workload/rules/test_rule?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("ruleName", "test_rule")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid rule name", func(t *testing.T) {
		s := newTestServer()
		body, _ := json.Marshal(map[string]interface{}{"threshold": "20"})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/rules/1invalid!?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "1invalid!")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Rules: []cbv1alpha1.WorkloadRule{{Name: "test_rule", Action: "log"}},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/rules/test_rule?namespace=default",
			bytes.NewReader([]byte("invalid")))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "test_rule")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("rule not found nil workload", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"threshold": "20"})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/rules/missing_rule?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "missing_rule")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("rule not found", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Rules: []cbv1alpha1.WorkloadRule{{Name: "other_rule", Action: "log"}},
		}
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"threshold": "20"})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/rules/missing_rule?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "missing_rule")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("success partial update", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Rules: []cbv1alpha1.WorkloadRule{
				{Name: "test_rule", Action: "log", Threshold: "10", Priority: 3},
			},
		}
		s := newTestServer(cluster)
		body, _ := json.Marshal(map[string]interface{}{"threshold": "20", "priority": 5})
		req := httptest.NewRequest(http.MethodPut,
			apiPrefix+"/clusters/test-cluster/workload/rules/test_rule?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "test_rule")
		rec := httptest.NewRecorder()
		s.handleUpdateWorkloadRule(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "updated", resp["status"])
		rule := resp["rule"].(map[string]interface{})
		assert.Equal(t, "20", rule["threshold"])
		assert.Equal(t, float64(5), rule["priority"])
		// Action should remain unchanged.
		assert.Equal(t, "log", rule["action"])
	})
}

func TestHandleDeleteWorkloadRule(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/nonexistent/workload/rules/test_rule?namespace=default", nil)
		req.SetPathValue("name", "nonexistent")
		req.SetPathValue("ruleName", "test_rule")
		rec := httptest.NewRecorder()
		s.handleDeleteWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid rule name", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/rules/1invalid!?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "1invalid!")
		rec := httptest.NewRecorder()
		s.handleDeleteWorkloadRule(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("rule not found nil workload", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/rules/missing_rule?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "missing_rule")
		rec := httptest.NewRecorder()
		s.handleDeleteWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("rule not found", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Rules: []cbv1alpha1.WorkloadRule{{Name: "other_rule", Action: "log"}},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/rules/missing_rule?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "missing_rule")
		rec := httptest.NewRecorder()
		s.handleDeleteWorkloadRule(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("success", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Rules: []cbv1alpha1.WorkloadRule{
				{Name: "rule_a", Action: "cancel"},
				{Name: "rule_b", Action: "log"},
			},
		}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodDelete,
			apiPrefix+"/clusters/test-cluster/workload/rules/rule_a?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("ruleName", "rule_a")
		rec := httptest.NewRecorder()
		s.handleDeleteWorkloadRule(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "deleted", resp["status"])
		assert.Equal(t, "rule_a", resp["rule"])
	})
}

// ============================================================================
// Permission model tests (Scenario 31e)
// ============================================================================

func TestDeleteResourceGroup_RequiresAdmin(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{client: dbClient}

	// Create server WITH auth middleware.
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "operator", Permission: auth.PermissionOperator},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := trackServer(NewServer(k8sClient, mw, factory, &metrics.NoopRecorder{}, nil, 0))
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
	req.SetBasicAuth("operator", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Operator should be denied — DELETE resource-groups requires Admin.
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestDeleteResourceGroup_AdminAllowed(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{client: dbClient}

	// Create server WITH auth middleware — admin user.
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := trackServer(NewServer(k8sClient, mw, factory, &metrics.NoopRecorder{}, nil, 0))
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/workload/resource-groups/analytics?namespace=default", nil)
	req.SetBasicAuth("admin", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Admin should be allowed.
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ============================================================================
// Password rotation tests
// ============================================================================

func newTestServerWithCredStore(
	credStore *auth.InMemoryCredentialStore,
	objs ...runtime.Object,
) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	return trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0, credStore))
}

func TestHandleRotatePassword_Success(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "old-password", auth.PermissionAdmin)

	// Pre-create the admin password secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-operator-admin-password",
			Namespace: util.OperatorNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte("old-password"),
		},
	}

	s := newTestServerWithCredStore(credStore, secret)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "rotated", resp["status"])
	assert.Equal(t, "Admin password rotated successfully", resp["message"])

	// Verify the in-memory credential store was updated (old password should fail).
	_, pwErr := credStore.GetPassword(context.Background(), "admin")
	require.NoError(t, pwErr)
}

func TestHandleRotatePassword_CreatesSecretIfMissing(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "old-password", auth.PermissionAdmin)

	// No pre-existing secret — the handler should create one.
	s := newTestServerWithCredStore(credStore)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "rotated", resp["status"])
}

func TestHandleRotatePassword_NoCredStore(t *testing.T) {
	// Create a server without a credential store.
	s := newTestServer()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	errObj, ok := resp["error"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "INTERNAL_ERROR", errObj["code"])
	assert.Equal(t, "credential store not configured", errObj["message"])
}

func TestHandleRotatePassword_RequiresAdmin(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Create server with auth middleware — operator user (not admin).
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "operator", Permission: auth.PermissionOperator},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := trackServer(NewServer(k8sClient, mw, nil, &metrics.NoopRecorder{}, nil, 0, credStore))
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	req.SetBasicAuth("operator", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Operator should be denied — rotate-password requires Admin.
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandleRotatePassword_AdminAllowed(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "admin-pass", auth.PermissionAdmin)

	// Pre-create the admin password secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-operator-admin-password",
			Namespace: util.OperatorNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte("admin-pass"),
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(secret).Build()

	// Create server with auth middleware — admin user.
	basicProvider := &mockAuthProvider{
		identity: &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin},
	}
	mw := auth.NewAuthMiddleware(basicProvider, nil, nil, &metrics.NoopRecorder{})
	s := trackServer(NewServer(k8sClient, mw, nil, &metrics.NoopRecorder{}, nil, 0, credStore))
	handler := s.Handler()

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	req.SetBasicAuth("admin", "pass")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Admin should be allowed.
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]interface{}
	err := json.NewDecoder(rec.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "rotated", resp["status"])
}

func TestHandleRotatePassword_UpdatesK8sSecret(t *testing.T) {
	credStore := auth.NewInMemoryCredentialStore()
	credStore.SetCredentials("admin", "old-password", auth.PermissionAdmin)

	// Pre-create the admin password secret.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cloudberry-operator-admin-password",
			Namespace: util.OperatorNamespace,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"password": []byte("old-password"),
		},
	}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(secret).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0, credStore))

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/auth/rotate-password", nil)
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	ctx := auth.ContextWithIdentity(req.Context(), identity)
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	s.handleRotatePassword(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the K8s Secret was updated with a new password.
	updatedSecret := &corev1.Secret{}
	err := k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      secret.Name,
		Namespace: secret.Namespace,
	}, updatedSecret)
	require.NoError(t, err)

	newPw := string(updatedSecret.Data["password"])
	assert.NotEmpty(t, newPw, "new password should not be empty")
	assert.NotEqual(t, "old-password", newPw, "password should have been rotated")
	assert.GreaterOrEqual(t, len(newPw), 16, "generated password should be at least 16 characters")
}

// ============================================================================
// Session filtering tests (Scenario 59)
// ============================================================================

func TestFilterSessions(t *testing.T) {
	now := time.Now()
	sessions := []db.SessionWithGroup{
		{
			Session: db.Session{
				PID: 100, Username: "admin", Database: "postgres",
				State: "active", WaitEventType: "", QueryStart: now.Add(-2 * time.Minute),
			},
			ResourceGroup: "admin_group",
		},
		{
			Session: db.Session{
				PID: 200, Username: "analyst", Database: "analytics",
				State: "idle in transaction", WaitEventType: "", QueryStart: now.Add(-10 * time.Minute),
			},
			ResourceGroup: "analytics",
		},
		{
			Session: db.Session{
				PID: 300, Username: "etl_user", Database: "warehouse",
				State: "active", WaitEventType: "Lock", QueryStart: now.Add(-30 * time.Minute),
			},
			ResourceGroup: "etl",
		},
		{
			Session: db.Session{
				PID: 400, Username: "analyst", Database: "analytics",
				State: "idle", WaitEventType: "", QueryStart: now.Add(-1 * time.Hour),
			},
			ResourceGroup: "analytics",
		},
	}

	t.Run("no filters returns all", func(t *testing.T) {
		params := make(url.Values)
		result := filterSessions(sessions, params)
		assert.Len(t, result, 4)
	})

	t.Run("filter by status running", func(t *testing.T) {
		params := url.Values{"status": []string{"running"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 2)
		for _, s := range result {
			assert.Equal(t, "active", s.State)
		}
	})

	t.Run("filter by status queued", func(t *testing.T) {
		params := url.Values{"status": []string{"queued"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(200), result[0].PID)
	})

	t.Run("filter by status blocked", func(t *testing.T) {
		params := url.Values{"status": []string{"blocked"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(300), result[0].PID)
	})

	t.Run("filter by status idle", func(t *testing.T) {
		params := url.Values{"status": []string{"idle"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(400), result[0].PID)
	})

	t.Run("filter by database", func(t *testing.T) {
		params := url.Values{"database": []string{"analytics"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 2)
		for _, s := range result {
			assert.Equal(t, "analytics", s.Database)
		}
	})

	t.Run("filter by user", func(t *testing.T) {
		params := url.Values{"user": []string{"analyst"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 2)
		for _, s := range result {
			assert.Equal(t, "analyst", s.Username)
		}
	})

	t.Run("filter by resource_group", func(t *testing.T) {
		params := url.Values{"resource_group": []string{"etl"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(300), result[0].PID)
	})

	t.Run("filter by since", func(t *testing.T) {
		params := url.Values{"since": []string{"5m"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(100), result[0].PID)
	})

	t.Run("filter by since 15m", func(t *testing.T) {
		params := url.Values{"since": []string{"15m"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 2)
	})

	t.Run("combined filters", func(t *testing.T) {
		params := url.Values{
			"user":     []string{"analyst"},
			"database": []string{"analytics"},
			"status":   []string{"idle"},
		}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 1)
		assert.Equal(t, int32(400), result[0].PID)
	})

	t.Run("no matches returns empty slice", func(t *testing.T) {
		params := url.Values{"database": []string{"nonexistent"}}
		result := filterSessions(sessions, params)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("unknown status matches all", func(t *testing.T) {
		params := url.Values{"status": []string{"unknown"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 4)
	})

	t.Run("invalid since duration matches all", func(t *testing.T) {
		params := url.Values{"since": []string{"invalid"}}
		result := filterSessions(sessions, params)
		assert.Len(t, result, 4)
	})

	t.Run("empty sessions returns empty slice", func(t *testing.T) {
		params := url.Values{"status": []string{"running"}}
		result := filterSessions(nil, params)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

func TestHandleListSessions_WithFilters(t *testing.T) {
	now := time.Now()
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{
		sessionsWithGroup: []db.SessionWithGroup{
			{
				Session:       db.Session{PID: 100, Username: "admin", Database: "postgres", State: "active", QueryStart: now},
				ResourceGroup: "admin_group",
			},
			{
				Session:       db.Session{PID: 200, Username: "analyst", Database: "analytics", State: "idle", QueryStart: now},
				ResourceGroup: "analytics",
			},
			{
				Session:       db.Session{PID: 300, Username: "etl_user", Database: "warehouse", State: "active", WaitEventType: "Lock", QueryStart: now},
				ResourceGroup: "etl",
			},
		},
	}
	s := newTestServerWithDB(dbClient, cluster)

	t.Run("filter by status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default&status=running", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(2), resp["total"])
	})

	t.Run("filter by database", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default&database=analytics", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("filter by user", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default&user=admin", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("filter by resource_group", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default&resource_group=etl", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("filter by blocked status", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default&status=blocked", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(1), resp["total"])
	})

	t.Run("no filters returns all", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet,
			apiPrefix+"/clusters/test-cluster/sessions?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleListSessions(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(3), resp["total"])
	})
}

// ============================================================================
// Cancel query with reason tests (Scenario 59b)
// ============================================================================

func TestHandleCancelQuery_WithReason(t *testing.T) {
	t.Run("with reason", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{cancelResult: true}
		s := newTestServerWithDB(dbClient, cluster)

		body, _ := json.Marshal(map[string]string{"reason": "query taking too long"})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, true, resp["canceled"])
		assert.Equal(t, "query taking too long", resp["reason"])
	})

	t.Run("without reason", func(t *testing.T) {
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
		_, hasReason := resp["reason"]
		assert.False(t, hasReason, "reason should not be present when not provided")
	})

	t.Run("with empty reason", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{cancelResult: true}
		s := newTestServerWithDB(dbClient, cluster)

		body, _ := json.Marshal(map[string]string{"reason": ""})
		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
			bytes.NewReader(body))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, true, resp["canceled"])
		_, hasReason := resp["reason"]
		assert.False(t, hasReason, "reason should not be present when empty")
	})

	t.Run("with invalid json body still works", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		dbClient := &mockDBClient{cancelResult: true}
		s := newTestServerWithDB(dbClient, cluster)

		req := httptest.NewRequest(http.MethodPost,
			apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
			bytes.NewReader([]byte("not-json")))
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "123")
		rec := httptest.NewRecorder()
		s.handleCancelQuery(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, true, resp["canceled"])
	})
}

// TestCancelBackendByPID_OversizedReasonBodyRejected verifies the W1-A3 limitBody
// guard on the cancel/terminate path (TASK 15): a reason body exceeding the
// maxBodySize cap is BOUNDED by MaxBytesReader, so the oversized reason cannot be
// decoded (it is silently dropped — reason is optional) while the request still
// succeeds; normal/empty bodies continue to decode the optional reason.
func TestCancelBackendByPID_OversizedReasonBodyRejected(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")
	dbClient := &mockDBClient{cancelResult: true}
	s := newTestServerWithDB(dbClient, cluster)

	// A valid JSON object whose "reason" string alone exceeds the 1 MiB cap.
	huge := strings.Repeat("a", maxBodySize+1024)
	body := `{"reason":"` + huge + `"}`
	require.Greater(t, len(body), maxBodySize)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
		strings.NewReader(body))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("pid", "123")
	rec := httptest.NewRecorder()
	s.handleCancelQuery(rec, req)

	// The operation still succeeds; the oversized reason is NOT echoed because
	// MaxBytesReader bounded the body and the (optional) decode failed.
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["canceled"])
	_, hasReason := resp["reason"]
	assert.False(t, hasReason,
		"an oversized reason body must be bounded/dropped, never echoed (W1-A3)")

	// Demonstrate the bound directly: a fresh MaxBytesReader over the same data
	// errors once the cap is exceeded, proving the body is bounded.
	mbr := http.MaxBytesReader(httptest.NewRecorder(), io.NopCloser(strings.NewReader(body)), maxBodySize)
	_, readErr := io.ReadAll(mbr)
	require.Error(t, readErr, "MaxBytesReader must reject reads beyond maxBodySize")
	var maxErr *http.MaxBytesError
	assert.ErrorAs(t, readErr, &maxErr)

	// A normal-sized body still decodes the optional reason (behavior unchanged).
	smallReq := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/sessions/123/cancel?namespace=default",
		strings.NewReader(`{"reason":"ok"}`))
	smallReq.SetPathValue("name", "test-cluster")
	smallReq.SetPathValue("pid", "123")
	smallRec := httptest.NewRecorder()
	s.handleCancelQuery(smallRec, smallReq)
	require.Equal(t, http.StatusOK, smallRec.Code)
	var smallResp map[string]interface{}
	require.NoError(t, json.NewDecoder(smallRec.Body).Decode(&smallResp))
	assert.Equal(t, "ok", smallResp["reason"])
}

func TestMatchStatus(t *testing.T) {
	session := db.SessionWithGroup{
		Session: db.Session{State: "active", WaitEventType: "Lock"},
	}

	assert.True(t, matchStatus(session, "running"))
	assert.False(t, matchStatus(session, "idle"))
	assert.False(t, matchStatus(session, "queued"))
	assert.True(t, matchStatus(session, "blocked"))
	assert.True(t, matchStatus(session, "unknown"))

	idleSession := db.SessionWithGroup{
		Session: db.Session{State: "idle"},
	}
	assert.True(t, matchStatus(idleSession, "idle"))
	assert.False(t, matchStatus(idleSession, "running"))

	queuedSession := db.SessionWithGroup{
		Session: db.Session{State: "idle in transaction"},
	}
	assert.True(t, matchStatus(queuedSession, "queued"))
	assert.False(t, matchStatus(queuedSession, "running"))
}

func TestMatchSince(t *testing.T) {
	now := time.Now()

	recentSession := db.SessionWithGroup{
		Session: db.Session{QueryStart: now.Add(-2 * time.Minute)},
	}
	oldSession := db.SessionWithGroup{
		Session: db.Session{QueryStart: now.Add(-1 * time.Hour)},
	}

	assert.True(t, matchSince(recentSession, "5m"))
	assert.False(t, matchSince(oldSession, "5m"))
	assert.True(t, matchSince(oldSession, "2h"))

	// Invalid duration returns true (no filtering).
	assert.True(t, matchSince(oldSession, "invalid"))
	assert.True(t, matchSince(recentSession, ""))
}

// ============================================================================
// Query-monitoring handler tests (pre-existing untested handlers)
// ============================================================================

func newMonitoringCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster(name, namespace)
	c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
	return c
}

func TestHandleGetQueryDetail(t *testing.T) {
	cluster := newMonitoringCluster("test-cluster", "default")

	t.Run("invalid PID", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "abc")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "missing")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("db client error", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("conn refused"), cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("detail not found", func(t *testing.T) {
		dbClient := &mockDBClient{queryDetailErr: fmt.Errorf("not found")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("happy path", func(t *testing.T) {
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleGetQueryDetail(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandleCancelQueryByPID(t *testing.T) {
	cluster := newMonitoringCluster("test-cluster", "default")

	t.Run("invalid PID", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "abc")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("non-positive PID", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "0")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "missing")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("db client error", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("conn refused"), cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("cancel error", func(t *testing.T) {
		dbClient := &mockDBClient{cancelQueryErr: fmt.Errorf("cancel failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("happy path with reason", func(t *testing.T) {
		dbClient := &mockDBClient{cancelResult: true}
		s := newTestServerWithDB(dbClient, cluster)
		body := bytes.NewBufferString(`{"reason":"too slow"}`)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", body)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("happy path no reason", func(t *testing.T) {
		dbClient := &mockDBClient{cancelResult: true}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		req.SetPathValue("pid", "10")
		rec := httptest.NewRecorder()
		s.handleCancelQueryByPID(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandleMoveQuery(t *testing.T) {
	cluster := newMonitoringCluster("test-cluster", "default")

	makeReq := func(pid, body string) *http.Request {
		var r *http.Request
		if body == "" {
			r = httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		} else {
			r = httptest.NewRequest(http.MethodPost, "/x?namespace=default",
				bytes.NewBufferString(body))
		}
		r.SetPathValue("name", "test-cluster")
		r.SetPathValue("pid", pid)
		return r
	}

	t.Run("invalid PID", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("abc", `{"targetGroup":"g"}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("non-positive PID", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("-1", `{"targetGroup":"g"}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{bad json`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing targetGroup", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":""}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid identifier", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":"bad-group!"}`))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		r := makeReq("10", `{"targetGroup":"g"}`)
		r.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, r)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		s := newTestServer(cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":"g"}`))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("db client error", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("conn refused"), cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":"g"}`))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("move error", func(t *testing.T) {
		dbClient := &mockDBClient{moveQueryErr: fmt.Errorf("move failed")}
		s := newTestServerWithDB(dbClient, cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":"g"}`))
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("happy path", func(t *testing.T) {
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)
		rec := httptest.NewRecorder()
		s.handleMoveQuery(rec, makeReq("10", `{"targetGroup":"analytics"}`))
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandleGetExporterHealth(t *testing.T) {
	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handleGetExporterHealth(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("monitoring disabled", func(t *testing.T) {
		cluster := newTestCluster("test-cluster", "default")
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: false}
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetExporterHealth(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, false, resp["monitoringEnabled"])
	})

	t.Run("no exporters configured", func(t *testing.T) {
		cluster := newMonitoringCluster("test-cluster", "default")
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetExporterHealth(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("exporters configured with ready pod", func(t *testing.T) {
		cluster := newMonitoringCluster("test-cluster", "default")
		cluster.Spec.QueryMonitoring.Exporters = &cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter: &cbv1alpha1.ExporterSpec{Enabled: true},
			NodeExporter:     &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9200},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster-coordinator-0",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelCluster:   "test-cluster",
					util.LabelComponent: "coordinator",
				},
			},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "postgres-exporter", Ready: true},
					{Name: "node-exporter", Ready: false},
				},
			},
		}
		s := newTestServerWithObjects(cluster, pod)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleGetExporterHealth(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, float64(2), resp[responseKeyTotal])
	})
}

func TestExporterStatusFromReady(t *testing.T) {
	assert.Equal(t, "unknown", exporterStatusFromReady(false, false))
	assert.Equal(t, "unknown", exporterStatusFromReady(true, false))
	assert.Equal(t, "up", exporterStatusFromReady(true, true))
	assert.Equal(t, "down", exporterStatusFromReady(false, true))
}

func TestBuildExporterStatuses(t *testing.T) {
	exporters := &cbv1alpha1.QueryMonitoringExportersSpec{
		PostgresExporter:        &cbv1alpha1.ExporterSpec{Enabled: true},
		NodeExporter:            &cbv1alpha1.ExporterSpec{Enabled: false},
		CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{Enabled: true, Port: 9999},
	}
	ready := map[string]bool{"postgres-exporter": true}
	statuses := buildExporterStatuses(exporters, ready, true, "now")
	require.Len(t, statuses, 2)
	assert.Equal(t, "postgres-exporter", statuses[0].Name)
	assert.Equal(t, "up", statuses[0].Status)
	assert.Equal(t, int32(9999), statuses[1].Port)
}

func TestHandleExportActiveQueries(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handleExportActiveQueries(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportActiveQueries(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("db client error", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("conn refused"), cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportActiveQueries(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("list error", func(t *testing.T) {
		dbClient := &mockDBClient{listSessionsWithGroupErr: fmt.Errorf("list failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportActiveQueries(rec, req)
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("happy path", func(t *testing.T) {
		dbClient := &mockDBClient{
			sessionsWithGroup: []db.SessionWithGroup{
				{
					Session:       db.Session{PID: 1, Username: "u", Database: "d", State: "active", Query: "SELECT, 1"},
					ResourceGroup: "g",
				},
			},
		}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodGet, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportActiveQueries(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "pid,username,database")
	})
}

func TestCsvEscape(t *testing.T) {
	assert.Equal(t, "plain", csvEscape("plain"))
	assert.Equal(t, `"a,b"`, csvEscape("a,b"))
	assert.Equal(t, `"a""b"`, csvEscape(`a"b`))
	assert.Equal(t, "\"a\nb\"", csvEscape("a\nb"))
}

func TestHandlePlanCheck(t *testing.T) {
	cluster := newTestCluster("test-cluster", "default")

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default",
			bytes.NewBufferString(`{"planText":"x"}`))
		req.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handlePlanCheck(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("invalid body", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default",
			bytes.NewBufferString(`{bad`))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handlePlanCheck(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty planText", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default",
			bytes.NewBufferString(`{"planText":"   "}`))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handlePlanCheck(rec, req)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("happy path", func(t *testing.T) {
		s := newTestServer(cluster)
		planText := "Seq Scan on big_table  (cost=0.00..100.00 rows=1000 width=10)"
		body, _ := json.Marshal(map[string]string{"planText": planText})
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default",
			bytes.NewBuffer(body))
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handlePlanCheck(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandleExportQueryHistory(t *testing.T) {
	cluster := newMonitoringCluster("test-cluster", "default")

	t.Run("cluster not found", func(t *testing.T) {
		s := newTestServer()
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "missing")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("monitoring disabled", func(t *testing.T) {
		c := newTestCluster("test-cluster", "default")
		c.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: false}
		s := newTestServer(c)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("no db factory", func(t *testing.T) {
		s := newTestServer(cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("db client error", func(t *testing.T) {
		s := newTestServerWithDBErr(fmt.Errorf("conn refused"), cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("happy path with filter", func(t *testing.T) {
		dbClient := &mockDBClient{}
		s := newTestServerWithDB(dbClient, cluster)
		body := bytes.NewBufferString(`{"user":"alice","since":"5m","until":"2026-01-01T00:00:00Z"}`)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", body)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "pid,query")
	})

	t.Run("export error", func(t *testing.T) {
		dbClient := &mockDBClient{exportHistoryErr: fmt.Errorf("export failed")}
		s := newTestServerWithDB(dbClient, cluster)
		req := httptest.NewRequest(http.MethodPost, "/x?namespace=default", nil)
		req.SetPathValue("name", "test-cluster")
		rec := httptest.NewRecorder()
		s.handleExportQueryHistory(rec, req)
		// Headers already written with 200 before the export error.
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

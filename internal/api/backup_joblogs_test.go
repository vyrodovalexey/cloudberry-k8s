package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// newJobLogsTestServer builds a server with both a fake controller-runtime
// client (seeded with the given objects) and a fake typed clientset (which
// returns "fake logs" for pod log streams).
func newJobLogsTestServer(objs ...runtime.Object) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objs...).Build()
	s := NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)
	return s.WithClientset(k8sfake.NewSimpleClientset())
}

func jobPod(name, job string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{labelJobNameBatch: job},
		},
	}
}

func newJobLogsRequest(cluster, job string) *http.Request {
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/"+cluster+"/backups/jobs/"+job+"/logs?namespace=default", nil)
	req.SetPathValue("name", cluster)
	req.SetPathValue("job", job)
	return req
}

func TestHandleBackupJobLogs_Stream(t *testing.T) {
	cluster := newBackupEnabledCluster()
	pod := jobPod("test-cluster-backup-1-abcde", "test-cluster-backup-1")
	s := newJobLogsTestServer(cluster, pod)

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "test-cluster-backup-1"))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	// The fake clientset returns "fake logs" for any pod log stream.
	assert.Equal(t, "fake logs", rec.Body.String())
}

func TestHandleBackupJobLogs_LegacyLabel(t *testing.T) {
	cluster := newBackupEnabledCluster()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-backup-1-legacy",
			Namespace: "default",
			Labels:    map[string]string{labelJobName: "test-cluster-backup-1"},
		},
	}
	s := newJobLogsTestServer(cluster, pod)

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "test-cluster-backup-1"))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "fake logs", rec.Body.String())
}

func TestHandleBackupJobLogs_JobNotFound(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newJobLogsTestServer(cluster)

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "missing-job"))

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "JOB_NOT_FOUND")
}

func TestHandleBackupJobLogs_ClusterNotFound(t *testing.T) {
	s := newJobLogsTestServer()

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("nonexistent", "some-job"))

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

func TestHandleBackupJobLogs_InvalidJobName(t *testing.T) {
	cluster := newBackupEnabledCluster()
	s := newJobLogsTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/backups/jobs/Invalid_Job/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "Invalid_Job")
	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleBackupJobLogs_NilClientset(t *testing.T) {
	cluster := newBackupEnabledCluster()
	pod := jobPod("test-cluster-backup-1-abcde", "test-cluster-backup-1")
	// Server without a clientset (e.g. non-live setup) must not panic.
	s := newTestServerWithBatch(cluster, pod)

	rec := httptest.NewRecorder()
	s.handleBackupJobLogs(rec, newJobLogsRequest("test-cluster", "test-cluster-backup-1"))

	assert.Equal(t, http.StatusNotImplemented, rec.Code)
	assert.Contains(t, rec.Body.String(), "LOGS_NOT_AVAILABLE")
}

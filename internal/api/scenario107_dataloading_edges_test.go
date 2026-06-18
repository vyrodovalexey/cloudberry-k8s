package api

// Scenario 107 — additional edge/error-path coverage for the data-loading
// handlers: the conflict-retry 500 mappings (handlePXFServerMutationError /
// handleJobMutationError default branch), the start-Job pre-check 500, and the
// stop path that suspends a scheduled CronJob.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// failUpdate is an interceptor that fails every Update with a non-conflict error
// so the conflict-retry helper surfaces it as a 500 (default mutation branch).
var failUpdate = interceptor.Funcs{
	Update: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.UpdateOption) error {
		return errAlways
	},
}

// webhookDeniedUpdate is an interceptor whose Update is rejected by a (simulated)
// validating admission webhook. The apiserver surfaces such a rejection through
// apimachinery as an Invalid status error whose message carries the webhook's
// "denied the request" reason — exactly what the production path classifies as a
// 400 VALIDATION_FAILED rather than a 500.
var webhookDeniedUpdate = interceptor.Funcs{
	Update: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.UpdateOption) error {
		return apierrors.NewInvalid(
			obj.GetObjectKind().GroupVersionKind().GroupKind(),
			obj.GetName(),
			field.ErrorList{field.Required(
				field.NewPath("spec", "dataLoading", "pxf", "servers"),
				"admission webhook \"vpxfserver.kb.io\" denied the request: "+
					"credentialSecrets is required for s3 servers")},
		)
	},
}

func TestScenario107_CreatePXFServer_UpdateError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(failUpdate, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"minio2","type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

func TestScenario107_UpdatePXFServer_UpdateError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(failUpdate, cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

func TestScenario107_UpdateDataLoadingJob_UpdateError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(failUpdate, cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw?namespace=default",
		strings.NewReader(`{"type":"pxf"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

// A non-NotFound Get error while checking the existing one-off Job is mapped to
// a 500 (the !apierrors.IsNotFound branch of handleStartDataLoadingJob).
func TestScenario107_StartDataLoadingJob_GetError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey,
			obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return errAlways
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInternal)
}

// A generic (non-AlreadyExists) Create failure while starting the one-off Job is
// mapped to a 500 (the create-error branch of handleStartDataLoadingJob).
func TestScenario107_StartDataLoadingJob_CreateError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errAlways
		},
	}, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// An AlreadyExists Create failure (a concurrent create won the race) is mapped to
// 409 JOB_ALREADY_RUNNING rather than 500.
func TestScenario107_StartDataLoadingJob_CreateAlreadyExists409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return apierrors.NewAlreadyExists(
				schema.GroupResource{Group: "batch", Resource: "jobs"},
				util.DataLoadJobName("test-cluster", "loadfdw"))
		},
	}, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobAlreadyRunning)
}

// 107-P13: stop suspends a scheduled CronJob (no one-off Job present). The
// response reports suspended=true and the CronJob is flipped to Suspend in the
// object store.
func TestScenario107_StopDataLoadingJob_SuspendsCronJob(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: "default"},
		Spec:       batchv1.CronJobSpec{Schedule: "@daily"},
	}
	s := newTestServerWithObjects(cluster, cronJob)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/stop?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)

	got := &batchv1.CronJob{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: k8sName, Namespace: "default"}, got))
	require.NotNil(t, got.Spec.Suspend)
	assert.True(t, *got.Spec.Suspend)
}

// 107-P13: stop surfaces a hard Delete error (non-NotFound) as a 500.
func TestScenario107_StopDataLoadingJob_DeleteError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	running := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: "default"}}
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
			return errAlways
		},
	}, cluster, running)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/stop?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// 107-P14: a pod-List error (with a clientset present) is mapped to a 500.
func TestScenario107_DataLoadingJobLogs_ListError500(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(_ context.Context, _ client.WithWatch, list client.ObjectList, _ ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					return errAlways
				}
				return nil
			},
		}).
		Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- Bug 1: webhook-validation rejection on CR mutation → 400 VALIDATION_FAILED.

// Creating a PXF server that the validating admission webhook rejects (e.g. an s3
// server missing credentialSecrets) must surface as 400 VALIDATION_FAILED with
// the webhook's reason, NOT a misleading 500 INTERNAL_ERROR.
func TestScenario107_CreatePXFServer_WebhookDenied400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(webhookDeniedUpdate, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"minio2","type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeValidationFailed)
	assert.Contains(t, rec.Body.String(), "denied the request")
}

// Updating a PXF server rejected by the webhook → 400 VALIDATION_FAILED.
func TestScenario107_UpdatePXFServer_WebhookDenied400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(webhookDeniedUpdate, cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeValidationFailed)
}

// Updating a data-loading job rejected by the webhook → 400 VALIDATION_FAILED.
func TestScenario107_UpdateDataLoadingJob_WebhookDenied400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(webhookDeniedUpdate, cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw?namespace=default",
		strings.NewReader(`{"type":"pxf"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeValidationFailed)
}

// A conflict exhaustion (retry budget spent) surfaces as 409 CONFLICT, not 500.
func TestScenario107_CreatePXFServer_ConflictExhausted409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServerWithInterceptor(interceptor.Funcs{
		Update: func(_ context.Context, _ client.WithWatch, obj client.Object,
			_ ...client.UpdateOption) error {
			return apierrors.NewConflict(
				schema.GroupResource{Group: "cloudberry.cloudberrydb.org", Resource: "cloudberryclusters"},
				obj.GetName(), errAlways)
		},
	}, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"minio2","type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeConflict)
}

// --- Bug 2: logs from a not-yet-started pod → 409 LOGS_NOT_READY -----------

// dataLoadingLogsServer wires a server whose fake client holds the cluster plus
// the given pod, with a fake clientset so streaming is reachable.
func dataLoadingLogsServer(pod *corev1.Pod) *Server {
	cluster := newDataLoadingCluster("test-cluster", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(cluster, pod).
		Build()
	return trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())
}

// A pod in Pending phase (container not started) → 409 LOGS_NOT_READY.
func TestScenario107_DataLoadingJobLogs_PendingPod409(t *testing.T) {
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sName + "-abcde",
			Namespace: "default",
			Labels:    map[string]string{labelJobNameBatch: k8sName},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	s := dataLoadingLogsServer(pod)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeLogsNotReady)
}

// A pod whose init container is still running (main container not started) →
// 409 LOGS_NOT_READY.
func TestScenario107_DataLoadingJobLogs_InitRunning409(t *testing.T) {
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sName + "-abcde",
			Namespace: "default",
			Labels:    map[string]string{labelJobNameBatch: k8sName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:  "health-check",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "loader",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "PodInitializing",
				}},
			}},
		},
	}
	s := dataLoadingLogsServer(pod)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeLogsNotReady)
}

// A pod whose main container is running streams logs (200) — the readiness
// pre-check must NOT block a started container.
func TestScenario107_DataLoadingJobLogs_RunningContainerStreams(t *testing.T) {
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sName + "-abcde",
			Namespace: "default",
			Labels:    map[string]string{labelJobNameBatch: k8sName},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "loader",
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
	s := dataLoadingLogsServer(pod)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

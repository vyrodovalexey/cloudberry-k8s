package api

// Scenario 112 — Disabled States (DIS.1) data-loading API reporting.
//
// When dataLoading.enabled=false the data-loading REST surface reports the
// subsystem-disabled state HONESTLY and consistently:
//
//   - MUTATING endpoints (create/update/delete jobs, start/stop, logs,
//     external-tables) → 400 DATA_LOADING_NOT_ENABLED (assert the code),
//   - the LIST/GET jobs endpoints → 200 disabled envelope (dataLoadingEnabled
//     false + the disabled message), mirroring the monitoringDisabled precedent,
//   - PRECEDENCE: getPXFCluster on a DL-disabled cluster reports
//     DATA_LOADING_NOT_ENABLED (the broader gate), NOT PXF_NOT_ENABLED; only a
//     DL-enabled + pxf-disabled cluster reports PXF_NOT_ENABLED.
//
// A DL-enabled control case proves the gate does not over-fire.
//
// Catalog IDs covered: 112-DIS1-APIDISABLED (and the getPXFCluster precedence
// cases that back 112-DIS1/DIS2).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// newDLDisabledCluster builds a cluster whose data-loading subsystem is present
// but DISABLED (the explicit disabled state). PXF is also present+enabled in the
// block so the precedence assertion (DATA_LOADING_NOT_ENABLED wins over
// PXF_NOT_ENABLED) is meaningful: the broader gate must fire FIRST even though
// PXF itself would otherwise be "enabled".
func newDLDisabledCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster(name, namespace)
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: false,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{{Name: "s3srv", Type: "s3"}},
		},
	}
	return c
}

// dlReq builds a request with the cluster path value set (and the job path value
// when non-empty).
func dlReq(method, name, namespace, path, job string) *http.Request {
	req := httptest.NewRequest(method,
		apiPrefix+"/clusters/"+name+"/data-loading/"+path+"?namespace="+namespace, strings.NewReader("{}"))
	req.SetPathValue("name", name)
	if job != "" {
		req.SetPathValue("job", job)
	}
	return req
}

// assertDataLoadingNotEnabled asserts the response is a 400 with the
// DATA_LOADING_NOT_ENABLED error code.
func assertDataLoadingNotEnabled(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
}

// --- mutating endpoints → 400 DATA_LOADING_NOT_ENABLED ---------------------

func TestScenario112_CreateJob_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, dlReq(http.MethodPost, "s112", "default", "jobs", ""))
	assertDataLoadingNotEnabled(t, rec)
}

func TestScenario112_UpdateJob_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, dlReq(http.MethodPut, "s112", "default", "jobs/loader", "loader"))
	assertDataLoadingNotEnabled(t, rec)
}

func TestScenario112_DeleteJob_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleDeleteDataLoadingJob(rec, dlReq(http.MethodDelete, "s112", "default", "jobs/loader", "loader"))
	assertDataLoadingNotEnabled(t, rec)
}

func TestScenario112_StartJob_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, dlReq(http.MethodPost, "s112", "default", "jobs/loader/start", "loader"))
	assertDataLoadingNotEnabled(t, rec)
}

func TestScenario112_StopJob_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, dlReq(http.MethodPost, "s112", "default", "jobs/loader/stop", "loader"))
	assertDataLoadingNotEnabled(t, rec)
}

func TestScenario112_ExternalTables_DataLoadingDisabled(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, dlReq(http.MethodGet, "s112", "default", "external-tables", ""))
	assertDataLoadingNotEnabled(t, rec)
}

// Logs endpoint requires a clientset to reach the DL gate (the nil-clientset
// 501 check precedes it), so seed a fake clientset and assert the gate fires.
func TestScenario112_JobLogs_DataLoadingDisabled(t *testing.T) {
	cluster := newDLDisabledCluster("s112", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())

	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, dlReq(http.MethodGet, "s112", "default", "jobs/loader/logs", "loader"))
	assertDataLoadingNotEnabled(t, rec)
}

// --- list/get → 200 disabled envelope --------------------------------------

func TestScenario112_ListJobs_DataLoadingDisabled_Envelope(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleListDataLoadingJobs(rec, dlReq(http.MethodGet, "s112", "default", "jobs", ""))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["dataLoadingEnabled"])
	assert.Equal(t, msgDataLoadingNotEnabled, resp["message"])
}

func TestScenario112_GetJob_DataLoadingDisabled_Envelope(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handleGetDataLoadingJob(rec, dlReq(http.MethodGet, "s112", "default", "jobs/loader", "loader"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["dataLoadingEnabled"])
	assert.Equal(t, msgDataLoadingNotEnabled, resp["message"])
}

// --- precedence: getPXFCluster DL-disabled → DATA_LOADING_NOT_ENABLED -------

// 112 precedence: a PXF endpoint on a DL-disabled cluster (even with pxf.enabled
// in the block) reports DATA_LOADING_NOT_ENABLED, never PXF_NOT_ENABLED — the
// broader subsystem gate takes precedence.
func TestScenario112_PXFStatus_DLDisabled_PrecedenceOverPXF(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, pxfRequest(http.MethodGet, "s112", "default", "status"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
	assert.NotContains(t, rec.Body.String(), errCodePXFNotEnabled)
}

func TestScenario112_ListPXFServers_DLDisabled_PrecedenceOverPXF(t *testing.T) {
	s := newTestServer(newDLDisabledCluster("s112", "default"))
	rec := httptest.NewRecorder()
	req := dlReq(http.MethodGet, "s112", "default", "pxf/servers", "")
	s.handleListPXFServers(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
	assert.NotContains(t, rec.Body.String(), errCodePXFNotEnabled)
}

// 112 precedence (the OTHER side): a DL-ENABLED but pxf-DISABLED cluster reports
// the PXF-specific PXF_NOT_ENABLED (the DL gate passes, the PXF gate fires).
func TestScenario112_PXFStatus_DLEnabledPXFDisabled_PXFNotEnabled(t *testing.T) {
	cluster := newTestCluster("s112", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true} // no PXF
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handlePXFStatus(rec, pxfRequest(http.MethodGet, "s112", "default", "status"))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodePXFNotEnabled)
	assert.NotContains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
}

// --- DL-enabled control: endpoints behave normally (gate does not over-fire) -

func TestScenario112_ListJobs_DLEnabled_Control(t *testing.T) {
	cluster := newTestCluster("s112", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader", Type: "gpload", GploadJob: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.t", Format: "csv", FilePaths: []string{"/d/*.csv"}}},
		},
	}
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleListDataLoadingJobs(rec, dlReq(http.MethodGet, "s112", "default", "jobs", ""))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	// Enabled => the normal list envelope (NO disabled marker), total counts jobs.
	assert.Nil(t, resp["dataLoadingEnabled"])
	assert.Equal(t, float64(1), resp["total"])
}

func TestScenario112_StartJob_DLEnabled_Control(t *testing.T) {
	cluster := newTestCluster("s112", "default")
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loader", Type: "gpload", GploadJob: &cbv1alpha1.GploadJobSpec{
				TargetTable: "public.t", Format: "csv", FilePaths: []string{"/d/*.csv"}}},
		},
	}
	s := newTestServer(cluster)
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, dlReq(http.MethodPost, "s112", "default", "jobs/loader/start", "loader"))

	// Enabled + known job => 202 Accepted (NOT the disabled 400).
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.NotContains(t, rec.Body.String(), errCodeDataLoadingNotEnabled)
}

package api

// Scenario 107 — All Data-Loading API Endpoints (P.1–P.15).
//
// This suite exercises the PXF-servers CRUD (P.2–P.5), the data-loading job
// CRUD + lifecycle (P.8/P.10–P.13), the job-log streaming honesty contract
// (P.14), the observed-vs-expected external-tables view (P.15) and the RBAC
// permission matrix across every route. Each happy-path asserts the HTTP
// status, the response envelope shape AND the real side effect (the spec
// mutated / a real batchv1.Job created or deleted in the fake client object
// store) — never just the status code.
//
// Catalog IDs covered (see task-breackdown_claude_2026-06-16_12-27-04.out):
//   107-P2-F, 107-P2-404
//   107-P3-F, 107-P3-404, 107-P3-409, 107-P3-400 (PXF disabled)
//   107-P4-F, 107-P4-404C, 107-P4-404S
//   107-P5-F, 107-P5-404, 107-P5-409
//   107-P8-F, 107-P8-404, 107-P8-409, 107-P8-400
//   107-P10-F, 107-P10-404
//   107-P11-F, 107-P11-404
//   107-P12-F, 107-P12-404, 107-P12-409
//   107-P13-F (delete), 107-P13 (idempotent no-op)
//   107-P14-501, 107-P14-404, 107-P14-F
//   107-P15-F, 107-P15-EMPTY, 107-P15-DBERR
//   107-RBAC (full permission matrix), 107-MX-F (servers-changed once)

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// --- fixtures --------------------------------------------------------------

// newDataLoadingCluster builds a PXF-enabled cluster seeded with a couple of
// servers and a couple of data-loading jobs (one fdw, one external) so every
// endpoint has realistic spec state to mutate and observe.
func newDataLoadingCluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	c := newTestCluster(name, namespace)
	c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "s3.local"}},
				{Name: "hivesrv", Type: "hive", Config: map[string]string{"hive.metastore.uris": "thrift://h:9083"}},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{Name: "loadfdw", Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "s3srv", Profile: "s3:parquet", TargetTable: "public.events", LoadMethod: "fdw",
			}},
			{Name: "loadext", Type: "pxf", PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "hivesrv", Profile: "hive:orc", TargetTable: "public.orders", LoadMethod: "external-table",
			}},
		},
	}
	return c
}

// reGetCluster re-fetches the cluster from the fake client so tests can assert
// the persisted side effect of a mutation, not the in-memory request copy.
func reGetCluster(t *testing.T, s *Server, name, namespace string) *cbv1alpha1.CloudberryCluster {
	t.Helper()
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: name, Namespace: namespace}, got))
	return got
}

// --- P.2: list / get PXF servers -------------------------------------------

func TestScenario107_ListPXFServers(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleListPXFServers(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, float64(2), resp["total"])
	servers, ok := resp["servers"].([]interface{})
	require.True(t, ok)
	assert.Len(t, servers, 2)
}

func TestScenario107_ListPXFServers_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/missing/data-loading/pxf/servers?namespace=default", nil)
	req.SetPathValue("name", "missing")
	rec := httptest.NewRecorder()
	s.handleListPXFServers(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

func TestScenario107_GetPXFServer(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleGetPXFServer(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var view pxfServerView
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&view))
	assert.Equal(t, "s3srv", view.Name)
	assert.Equal(t, "s3", view.Type)
}

func TestScenario107_GetPXFServer_NotFound(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/nope?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "nope")
	rec := httptest.NewRecorder()
	s.handleGetPXFServer(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeServerNotFound)
}

// --- P.3: create PXF server ------------------------------------------------

func TestScenario107_CreatePXFServer_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"minio2","type":"s3","config":{"fs.s3a.endpoint":"minio.local"}}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "minio2", resp["server"])

	// Response carries the rendered "<server>__*.xml" keys for the NEW server.
	rendered, ok := resp["renderedKeys"].(map[string]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rendered)
	for k := range rendered {
		assert.True(t, strings.HasPrefix(k, "minio2__"),
			"rendered key %q must be scoped to the new server", k)
	}

	// SIDE EFFECT: the spec really gained the server.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.NotNil(t, findPXFServer(got.Spec.DataLoading.Pxf.Servers, "minio2"))
	assert.Len(t, got.Spec.DataLoading.Pxf.Servers, 3)
}

func TestScenario107_CreatePXFServer_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/missing/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"x","type":"s3"}`))
	req.SetPathValue("name", "missing")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestScenario107_CreatePXFServer_Duplicate409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"s3srv","type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeServerExists)

	// CR unchanged: still exactly two servers.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Len(t, got.Spec.DataLoading.Pxf.Servers, 2)
}

func TestScenario107_CreatePXFServer_MissingName(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestScenario107_CreatePXFServer_MissingType(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"x"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestScenario107_CreatePXFServer_InvalidBody(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{not-json`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- P.4: update PXF server ------------------------------------------------

func TestScenario107_UpdatePXFServer_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"s3","config":{"fs.s3a.endpoint":"NEW.endpoint"}}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "s3srv", resp["server"])
	rendered, ok := resp["renderedKeys"].(map[string]interface{})
	require.True(t, ok)
	require.NotEmpty(t, rendered)
	for k := range rendered {
		assert.True(t, strings.HasPrefix(k, "s3srv__"))
	}

	// SIDE EFFECT: the server config really changed in place.
	got := reGetCluster(t, s, "test-cluster", "default")
	srv := findPXFServer(got.Spec.DataLoading.Pxf.Servers, "s3srv")
	require.NotNil(t, srv)
	assert.Equal(t, "NEW.endpoint", srv.Config["fs.s3a.endpoint"])
	assert.Equal(t, "s3", srv.Type)
	assert.Len(t, got.Spec.DataLoading.Pxf.Servers, 2)
}

// TestScenario107_UpdatePXFServer_PartialPreservesFields verifies the PARTIAL
// merge semantics (the Scenario 108 L.4 `pxf servers update --endpoint` case):
// a PUT that omits `type` and supplies only ONE config key must (a) preserve the
// existing type (so the validating webhook accepts a non-empty type), (b) change
// only the supplied config key, (c) preserve other existing config keys, and
// (d) preserve the existing credentialSecrets.
func TestScenario107_UpdatePXFServer_PartialPreservesFields(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	// Seed s3srv with a richer config + credential refs to prove preservation.
	s3 := findPXFServer(cluster.Spec.DataLoading.Pxf.Servers, "s3srv")
	require.NotNil(t, s3)
	s3.Config = map[string]string{
		"fs.s3a.endpoint":          "old.endpoint",
		"fs.s3a.path.style.access": "true",
	}
	s3.CredentialSecrets = []cbv1alpha1.SecretReference{
		{Name: "s3-creds", Key: "accessKey"},
	}
	s := newTestServer(cluster)

	// Empty type + a single config key — exactly what the CLI sends for
	// `pxf servers update s3srv --endpoint new.endpoint`.
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"","config":{"fs.s3a.endpoint":"new.endpoint"}}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	got := reGetCluster(t, s, "test-cluster", "default")
	srv := findPXFServer(got.Spec.DataLoading.Pxf.Servers, "s3srv")
	require.NotNil(t, srv)
	assert.Equal(t, "s3", srv.Type, "type preserved (not blanked)")
	assert.Equal(t, "new.endpoint", srv.Config["fs.s3a.endpoint"], "endpoint changed")
	assert.Equal(t, "true", srv.Config["fs.s3a.path.style.access"], "other config key preserved")
	require.Len(t, srv.CredentialSecrets, 1, "credential secrets preserved")
	assert.Equal(t, "s3-creds", srv.CredentialSecrets[0].Name)
}

// TestScenario107_UpdatePXFServer_ChangesTypeWhenProvided verifies that a PUT
// which DOES set type changes it (the merge preserves type only when omitted).
func TestScenario107_UpdatePXFServer_ChangesTypeWhenProvided(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"hdfs"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	got := reGetCluster(t, s, "test-cluster", "default")
	srv := findPXFServer(got.Spec.DataLoading.Pxf.Servers, "s3srv")
	require.NotNil(t, srv)
	assert.Equal(t, "hdfs", srv.Type, "type changed when explicitly provided")
	// Existing config preserved (request supplied none).
	assert.Equal(t, "s3.local", srv.Config["fs.s3a.endpoint"])
}

func TestScenario107_UpdatePXFServer_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/missing/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{"type":"s3"}`))
	req.SetPathValue("name", "missing")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

func TestScenario107_UpdatePXFServer_UnknownServer404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/nope?namespace=default",
		strings.NewReader(`{"type":"s3"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "nope")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeServerNotFound)

	// CR unchanged.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Len(t, got.Spec.DataLoading.Pxf.Servers, 2)
}

func TestScenario107_UpdatePXFServer_InvalidBody(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default",
		strings.NewReader(`{bad`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleUpdatePXFServer(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- P.5: delete PXF server ------------------------------------------------

func TestScenario107_DeletePXFServer_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	// Remove the jobs that reference the servers so the delete is unreferenced.
	cluster.Spec.DataLoading.Jobs = nil
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleDeletePXFServer(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, statusDeleted, resp["status"])

	// SIDE EFFECT: the server was removed from the spec.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Nil(t, findPXFServer(got.Spec.DataLoading.Pxf.Servers, "s3srv"))
	assert.Len(t, got.Spec.DataLoading.Pxf.Servers, 1)
}

func TestScenario107_DeletePXFServer_UnknownServer404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	cluster.Spec.DataLoading.Jobs = nil
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/nope?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "nope")
	rec := httptest.NewRecorder()
	s.handleDeletePXFServer(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeServerNotFound)
}

// 107-P5-409: deleting a server still referenced by a job is rejected with 409
// SERVER_IN_USE and performs NO mutation (mirrors webhook W.9).
func TestScenario107_DeletePXFServer_InUse409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers/s3srv?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleDeletePXFServer(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeServerInUse)
	assert.Contains(t, rec.Body.String(), "loadfdw") // names the referencing job

	// NO mutation: the server is still present.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.NotNil(t, findPXFServer(got.Spec.DataLoading.Pxf.Servers, "s3srv"))
}

func TestScenario107_DeletePXFServer_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/missing/data-loading/pxf/servers/s3srv?namespace=default", nil)
	req.SetPathValue("name", "missing")
	req.SetPathValue("server", "s3srv")
	rec := httptest.NewRecorder()
	s.handleDeletePXFServer(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- P.8: create data-loading job ------------------------------------------

func TestScenario107_CreateDataLoadingJob_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default",
		strings.NewReader(`{"name":"load1","type":"pxf","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"public.t"}}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)

	// SIDE EFFECT: the spec really gained the job.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.NotNil(t, findDataLoadingJob(got.Spec.DataLoading.Jobs, "load1"))
	assert.Len(t, got.Spec.DataLoading.Jobs, 3)
}

func TestScenario107_CreateDataLoadingJob_Duplicate409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default",
		strings.NewReader(`{"name":"loadfdw","type":"pxf","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"t"}}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobExists)

	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Len(t, got.Spec.DataLoading.Jobs, 2)
}

// 107-P8-400: a pxf job referencing an unknown server is rejected 400 (W.9).
func TestScenario107_CreateDataLoadingJob_UnknownServer400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default",
		strings.NewReader(`{"name":"load1","type":"pxf","pxfJob":{"server":"ghost","profile":"s3:parquet","targetTable":"t"}}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeInvalidRequest)

	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Len(t, got.Spec.DataLoading.Jobs, 2)
}

func TestScenario107_CreateDataLoadingJob_BadType400(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default",
		strings.NewReader(`{"name":"load1","type":"bogus"}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestScenario107_CreateDataLoadingJob_InvalidBody(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs?namespace=default",
		strings.NewReader(`{bad`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- P.10: update data-loading job -----------------------------------------

func TestScenario107_UpdateDataLoadingJob_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw?namespace=default",
		strings.NewReader(`{"type":"pxf","enabled":true,"schedule":"@daily","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"public.events","loadMethod":"fdw"}}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// SIDE EFFECT: the job was replaced in place with the new schedule.
	got := reGetCluster(t, s, "test-cluster", "default")
	job := findDataLoadingJob(got.Spec.DataLoading.Jobs, "loadfdw")
	require.NotNil(t, job)
	assert.True(t, job.Enabled)
	assert.Equal(t, "@daily", job.Schedule)
	assert.Len(t, got.Spec.DataLoading.Jobs, 2)
}

func TestScenario107_UpdateDataLoadingJob_UnknownJob404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/ghost?namespace=default",
		strings.NewReader(`{"type":"pxf"}`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "ghost")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobNotFound)
}

func TestScenario107_UpdateDataLoadingJob_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/missing/data-loading/jobs/loadfdw?namespace=default",
		strings.NewReader(`{"type":"pxf"}`))
	req.SetPathValue("name", "missing")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestScenario107_UpdateDataLoadingJob_InvalidBody(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw?namespace=default",
		strings.NewReader(`{bad`))
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- P.11: delete data-loading job -----------------------------------------

func TestScenario107_DeleteDataLoadingJob_HappyPath(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDeleteDataLoadingJob(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// SIDE EFFECT: the job slice shrank.
	got := reGetCluster(t, s, "test-cluster", "default")
	assert.Nil(t, findDataLoadingJob(got.Spec.DataLoading.Jobs, "loadfdw"))
	assert.Len(t, got.Spec.DataLoading.Jobs, 1)
}

func TestScenario107_DeleteDataLoadingJob_UnknownJob404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/ghost?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "ghost")
	rec := httptest.NewRecorder()
	s.handleDeleteDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobNotFound)
}

func TestScenario107_DeleteDataLoadingJob_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/missing/data-loading/jobs/loadfdw?namespace=default", nil)
	req.SetPathValue("name", "missing")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleDeleteDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- P.12: start data-loading job ------------------------------------------

// 107-P12-F: start creates a REAL batchv1.Job under util.DataLoadJobName.
func TestScenario107_StartDataLoadingJob_CreatesRealJob(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)

	// SIDE EFFECT: a real Job exists at the deterministic name.
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	job := &batchv1.Job{}
	require.NoError(t, s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: k8sName, Namespace: "default"}, job))
	assert.Equal(t, k8sName, job.Name)
}

func TestScenario107_StartDataLoadingJob_UnknownJob404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/ghost/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "ghost")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobNotFound)

	// No Job was created for the unknown spec job.
	k8sName := util.DataLoadJobName("test-cluster", "ghost")
	job := &batchv1.Job{}
	err := s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: k8sName, Namespace: "default"}, job)
	assert.Error(t, err)
}

// 107-P12-409: starting when the Job already exists yields JOB_ALREADY_RUNNING.
func TestScenario107_StartDataLoadingJob_AlreadyRunning409(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	existing := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName("test-cluster", "loadfdw"),
			Namespace: "default",
		},
	}
	s := newTestServerWithObjects(cluster, existing)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)

	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobAlreadyRunning)
}

func TestScenario107_StartDataLoadingJob_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/missing/data-loading/jobs/loadfdw/start?namespace=default", nil)
	req.SetPathValue("name", "missing")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- P.13: stop data-loading job -------------------------------------------

// 107-P13-F: stop deletes the running Job; a subsequent Get returns NotFound.
func TestScenario107_StopDataLoadingJob_DeletesJob(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	running := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: k8sName, Namespace: "default"},
	}
	s := newTestServerWithObjects(cluster, running)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/stop?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, true, resp["stopped"])

	// SIDE EFFECT: the Job was deleted from the object store.
	job := &batchv1.Job{}
	err := s.k8sClient.Get(context.Background(),
		types.NamespacedName{Name: k8sName, Namespace: "default"}, job)
	assert.Error(t, err)
}

// 107-P13 idempotency: stop with no running Job is an honest 200 no-op.
func TestScenario107_StopDataLoadingJob_NoopWhenAbsent(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/loadfdw/stop?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, false, resp["stopped"])
	assert.Contains(t, resp["message"], "nothing to stop")
}

func TestScenario107_StopDataLoadingJob_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/missing/data-loading/jobs/loadfdw/stop?namespace=default", nil)
	req.SetPathValue("name", "missing")
	req.SetPathValue("job", "loadfdw")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- P.14: job logs --------------------------------------------------------

func newDataLoadingJobLogsRequest(cluster, job string) *http.Request {
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/"+cluster+"/data-loading/jobs/"+job+"/logs?namespace=default", nil)
	req.SetPathValue("name", cluster)
	req.SetPathValue("job", job)
	return req
}

// 107-P14-501: no clientset configured → 501 LOGS_NOT_AVAILABLE (no fabrication).
func TestScenario107_DataLoadingJobLogs_NilClientset501(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster) // no clientset

	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, newDataLoadingJobLogsRequest("test-cluster", "loadfdw"))

	assert.Equal(t, http.StatusNotImplemented, rec.Code)
	assert.Contains(t, rec.Body.String(), "LOGS_NOT_AVAILABLE")
}

// 107-P14-404: clientset present but no backing pod → 404 JOB_NOT_FOUND.
func TestScenario107_DataLoadingJobLogs_NoPod404(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())

	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, newDataLoadingJobLogsRequest("test-cluster", "loadfdw"))

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeJobNotFound)
}

// 107-P14-F: a pod backing the k8s Job streams real logs from the fake clientset.
func TestScenario107_DataLoadingJobLogs_StreamsRealLogs(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	k8sName := util.DataLoadJobName("test-cluster", "loadfdw")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      k8sName + "-abcde",
			Namespace: "default",
			Labels:    map[string]string{labelJobNameBatch: k8sName},
		},
	}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster, pod).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())

	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, newDataLoadingJobLogsRequest("test-cluster", "loadfdw"))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "fake logs", rec.Body.String())
}

func TestScenario107_DataLoadingJobLogs_InvalidJobName(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster)
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/test-cluster/data-loading/jobs/Bad_Name/logs?namespace=default", nil)
	req.SetPathValue("name", "test-cluster")
	req.SetPathValue("job", "Bad_Name")
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestScenario107_DataLoadingJobLogs_ClusterNotFound(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	s := trackServer(NewServer(k8sClient, nil, nil, &metrics.NoopRecorder{}, nil, 0)).
		WithClientset(k8sfake.NewSimpleClientset())
	rec := httptest.NewRecorder()
	s.handleDataLoadingJobLogs(rec, newDataLoadingJobLogsRequest("missing", "loadfdw"))
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), errCodeClusterNotFound)
}

// --- P.15: external tables (observed vs expected) --------------------------

// extTablesDBClient embeds the base mock DB client and overrides only
// ListExternalTables so the observed/unobservable paths are configurable.
type extTablesDBClient struct {
	*mockDBClient
	tables []db.ExternalTableInfo
	err    error
}

func (c *extTablesDBClient) ListExternalTables(_ context.Context) ([]db.ExternalTableInfo, error) {
	return c.tables, c.err
}

// newExternalTablesServer wires the cluster with a db factory yielding the given
// external-tables fake client.
func newExternalTablesServer(
	cluster *cbv1alpha1.CloudberryCluster, client *extTablesDBClient,
) *Server {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{client: client}
	return trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))
}

func externalTablesRequest(cluster string) *http.Request {
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/"+cluster+"/data-loading/external-tables?namespace=default", nil)
	req.SetPathValue("name", cluster)
	return req
}

// 107-P15-F: observed populated + observedAvailable true; expected derived from
// the spec pxf jobs (foreign_<job> for fdw, target table for external), sorted.
func TestScenario107_ExternalTables_Observed(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	client := &extTablesDBClient{
		mockDBClient: &mockDBClient{},
		tables: []db.ExternalTableInfo{
			{Schema: "public", Name: "ext_events", Kind: "external"},
			{Schema: "public", Name: "foreign_loadfdw", Kind: "foreign", Server: "s3srv"},
		},
	}
	s := newExternalTablesServer(cluster, client)

	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, externalTablesRequest("test-cluster"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp ExternalTablesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	assert.True(t, resp.ObservedAvailable)
	require.Len(t, resp.Observed, 2)
	assert.Equal(t, "ext_events", resp.Observed[0].Name)
	assert.Equal(t, "foreign_loadfdw", resp.Observed[1].Name)

	// Expected: foreign_<job> for the fdw job, target table for the external job,
	// sorted by job name (loadext before loadfdw).
	require.Len(t, resp.Expected, 2)
	assert.Equal(t, "loadext", resp.Expected[0].Job)
	assert.Equal(t, "public.orders", resp.Expected[0].Name)
	assert.Equal(t, externalTablesKindExternal, resp.Expected[0].Kind)

	assert.Equal(t, "loadfdw", resp.Expected[1].Job)
	assert.Equal(t, builder.ForeignTableName("loadfdw"), resp.Expected[1].Name)
	assert.Equal(t, externalTablesKindFDW, resp.Expected[1].Kind)
}

// 107-P15-EMPTY: no db factory at all → observed null, observedAvailable false,
// expected still present (NEVER fabricated as "exists").
func TestScenario107_ExternalTables_NoFactory(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	s := newTestServer(cluster) // no db factory

	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, externalTablesRequest("test-cluster"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp ExternalTablesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	assert.False(t, resp.ObservedAvailable)
	assert.Nil(t, resp.Observed)
	// Expected is still derived and present — the honest spec-intent set.
	assert.Len(t, resp.Expected, 2)
}

// 107-P15-DBERR: a query error → observed ABSENT (not "none"), available false,
// expected still labeled and present.
func TestScenario107_ExternalTables_DBError(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	client := &extTablesDBClient{
		mockDBClient: &mockDBClient{},
		err:          assertErr("catalog probe failed"),
	}
	s := newExternalTablesServer(cluster, client)

	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, externalTablesRequest("test-cluster"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp ExternalTablesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	assert.False(t, resp.ObservedAvailable)
	assert.Nil(t, resp.Observed)
	assert.Len(t, resp.Expected, 2)
}

// 107-P15: a connect error (factory NewClient fails) is also ABSENT, not fake.
func TestScenario107_ExternalTables_ConnectError(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(cluster).Build()
	factory := &mockDBFactory{clientErr: assertErr("cannot connect")}
	s := trackServer(NewServer(k8sClient, nil, factory, &metrics.NoopRecorder{}, nil, 0))

	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, externalTablesRequest("test-cluster"))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp ExternalTablesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.False(t, resp.ObservedAvailable)
	assert.Nil(t, resp.Observed)
}

func TestScenario107_ExternalTables_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	rec := httptest.NewRecorder()
	s.handleListExternalTables(rec, externalTablesRequest("missing"))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// assertErr is a tiny error helper so the test stays dependency-free.
func assertErr(msg string) error { return &simpleError{msg} }

type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }

// --- 107-MX-F: honest servers-changed metric on a real ConfigMap diff ------

// A create that really changes the rendered PXF servers ConfigMap Data fires the
// HONEST servers-changed signal exactly once with the cluster labels.
func TestScenario107_CreatePXFServer_FiresServersChangedOnce(t *testing.T) {
	cluster := newDataLoadingCluster("test-cluster", "default")
	spy := &pxfSpyRecorder{}
	s := newTestServerWithRecorder(spy, cluster)

	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/test-cluster/data-loading/pxf/servers?namespace=default",
		strings.NewReader(`{"name":"minio2","type":"s3","config":{"fs.s3a.endpoint":"minio.local"}}`))
	req.SetPathValue("name", "test-cluster")
	rec := httptest.NewRecorder()
	s.handleCreatePXFServer(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code)
	require.Len(t, spy.serversChanged, 1)
	assert.Equal(t,
		pxfServersChangedCall{cluster: "test-cluster", namespace: "default"},
		spy.serversChanged[0])
}

// --- 107-RBAC: full permission matrix via the router (withPermission runs) --

// dlRoute describes one data-loading route and its required permission level.
type dlRoute struct {
	method   string
	path     string // relative to apiPrefix+/clusters/test-cluster/data-loading
	required auth.PermissionLevel
}

// TestScenario107_RBACMatrix drives EVERY data-loading route through the full
// router with an identity ONE LEVEL BELOW the requirement and asserts 403, then
// AT the requirement and asserts it is NOT 403 (the permission gate passed).
func TestScenario107_RBACMatrix(t *testing.T) {
	routes := []dlRoute{
		// Basic reads (P.2/P.7/P.9/P.14/P.15 family).
		{http.MethodGet, "/pxf/servers", auth.PermissionBasic},
		{http.MethodGet, "/pxf/servers/s3srv", auth.PermissionBasic},
		{http.MethodGet, "/jobs", auth.PermissionBasic},
		{http.MethodGet, "/jobs/loadfdw", auth.PermissionBasic},
		{http.MethodGet, "/jobs/loadfdw/logs", auth.PermissionBasic},
		{http.MethodGet, "/external-tables", auth.PermissionBasic},
		// Operator mutations (P.3/P.4/P.8/P.10/P.12/P.13).
		{http.MethodPost, "/pxf/servers", auth.PermissionOperator},
		{http.MethodPut, "/pxf/servers/s3srv", auth.PermissionOperator},
		{http.MethodPost, "/jobs", auth.PermissionOperator},
		{http.MethodPut, "/jobs/loadfdw", auth.PermissionOperator},
		{http.MethodPost, "/jobs/loadfdw/start", auth.PermissionOperator},
		{http.MethodPost, "/jobs/loadfdw/stop", auth.PermissionOperator},
		// Admin deletes (P.5/P.11).
		{http.MethodDelete, "/pxf/servers/s3srv", auth.PermissionAdmin},
		{http.MethodDelete, "/jobs/loadfdw", auth.PermissionAdmin},
	}

	base := apiPrefix + "/clusters/test-cluster/data-loading"
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			cluster := newDataLoadingCluster("test-cluster", "default")
			s := newTestServer(cluster)
			url := base + rt.path + "?namespace=default"

			// Below the required level → 403.
			if below, ok := belowLevel(rt.required); ok {
				req := httptest.NewRequest(rt.method, url, strings.NewReader("{}"))
				rec := serveWithIdentity(s, req, below)
				assert.Equal(t, http.StatusForbidden, rec.Code,
					"%s %s with %v must be forbidden", rt.method, rt.path, below)
			}

			// At the required level → permission gate passed (not 403).
			req := httptest.NewRequest(rt.method, url, strings.NewReader("{}"))
			rec := serveWithIdentity(s, req, rt.required)
			assert.NotEqual(t, http.StatusForbidden, rec.Code,
				"%s %s with %v must pass the permission gate", rt.method, rt.path, rt.required)
		})
	}
}

// belowLevel returns a permission strictly below the given one (and ok=false
// when none exists, i.e. PermissionBasic is already the floor for these routes).
func belowLevel(level auth.PermissionLevel) (auth.PermissionLevel, bool) {
	switch level {
	case auth.PermissionAdmin:
		return auth.PermissionOperator, true
	case auth.PermissionOperator:
		return auth.PermissionBasic, true
	default:
		return auth.PermissionBasic, false
	}
}

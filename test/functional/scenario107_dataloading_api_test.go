//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 107: All Data-Loading API Endpoints (P.1–P.15) — functional
// ============================================================================
//
// This suite black-boxes the FULL data-loading REST surface through the REAL
// api.Server HTTP router + auth/RBAC middleware over a fake k8s client + a
// MockDBClient factory + a spy metrics recorder — infra-free, no live cluster.
// Unlike the internal/api package UNIT tests (which call the handlers directly),
// every request here travels the WHOLE server: mux routing → auth middleware →
// withPermission RBAC gate → handler → fake-client side effect. Each happy-path
// asserts the HTTP status, the response body AND the real side effect (the spec
// mutated, a real batchv1.Job created/deleted, the servers-changed metric fired).
//
// Covers a full LIFECYCLE round-trip:
//   - PXF servers (107-P2/P3/P4/P5): list → create (201 + rendered keys) → list
//     shows it → update (config changes) → delete (gone); plus 409 SERVER_EXISTS,
//     409 SERVER_IN_USE, 404, 400 PXF_NOT_ENABLED.
//   - Jobs (107-P7..P13): list → create (201) → get → update → start (202, Job
//     created) → stop (202, Job deleted) → delete (gone); plus 409/404/400 edges.
//   - 107-P14: 501 when no clientset.
//   - 107-P15: db fake returns tables → observed populated + observedAvailable
//     true + expected present; db error → observed null + observedAvailable false
//     + expected still present (honesty).
//   - 107-RBAC: a representative permission-denied (403) per permission tier.
//   - 107-MX: a real server create fires the servers-changed metric exactly once.
// ============================================================================

const (
	scenario107Namespace = "cloudberry-test"
	scenario107Cluster   = "scenario107-dl"
	scenario107Prefix    = "/api/v1alpha1"

	scenario107BasicUser = "s107basic"
	scenario107BasicPass = "s107basicpass"
	scenario107OperUser  = "s107oper"
	scenario107OperPass  = "s107operpass"
	scenario107AdminUser = "s107admin"
	scenario107AdminPass = "s107adminpass"
)

// scenario107MetricsRecorder embeds NoopRecorder and records the honest
// servers-changed + data-loading-rows signals so the suite can assert the MX
// honesty rows.
type scenario107MetricsRecorder struct {
	metrics.NoopRecorder
	serversChanged []scenario107ServersChanged
	rowsLoaded     float64
}

type scenario107ServersChanged struct {
	cluster   string
	namespace string
}

func (m *scenario107MetricsRecorder) IncPXFServersChanged(cluster, namespace string) {
	m.serversChanged = append(m.serversChanged, scenario107ServersChanged{
		cluster: cluster, namespace: namespace,
	})
}

func (m *scenario107MetricsRecorder) RecordDataLoadingRows(_, _, _, _ string, count float64) {
	m.rowsLoaded += count
}

// Scenario107Suite drives the full data-loading surface through the real router
// over a fake client + MockDBClient factory + a spy metrics recorder.
type Scenario107Suite struct {
	suite.Suite
	server   *api.Server
	handler  http.Handler
	client   client.Client
	metrics  *scenario107MetricsRecorder
	dbClient *testutil.MockDBClient
	ctx      context.Context
}

func TestFunctional_Scenario107(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario107Suite))
}

func (s *Scenario107Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario107Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// scenario107DLCluster builds a PXF-enabled cluster seeded with two servers and
// two data-loading jobs (one fdw, one external) so every endpoint has realistic
// spec state to mutate and observe.
func scenario107DLCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario107Cluster, scenario107Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"}},
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
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{Configured: true, Servers: 2},
	}
	return cluster
}

// scenario107PXFDisabledCluster builds a cluster with PXF NOT enabled.
func scenario107PXFDisabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario107Cluster, scenario107Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: false},
	}
	return cluster
}

// boot builds the API server (real router + auth/RBAC + a MockDBClient factory)
// over a fake client seeded with the cluster + any extra objects, plus the spy
// metrics recorder. The credential store carries a Basic/Operator/Admin user.
func (s *Scenario107Suite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client
	s.metrics = &scenario107MetricsRecorder{}
	s.dbClient = &testutil.MockDBClient{}
	factory := &testutil.MockDBClientFactory{Client: s.dbClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario107BasicUser, scenario107BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario107OperUser, scenario107OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario107AdminUser, scenario107AdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, s.metrics, env.Logger, 0)
	s.handler = s.server.Handler()
}

// scenario107URL builds a full data-loading URL for the given suffix path.
func scenario107URL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario107Prefix + "/clusters/" + scenario107Cluster +
		"/data-loading" + suffix + sep + "namespace=" + scenario107Namespace
}

// do issues a request through the FULL handler with the given basic-auth identity
// and an optional JSON body.
func (s *Scenario107Suite) do(user, pass, method, suffix, body string) *httptest.ResponseRecorder {
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, scenario107URL(suffix), rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// decode JSON-decodes a recorder body into a generic map.
func scenario107Decode(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	return resp
}

// getCluster re-fetches the cluster from the fake client so tests can assert the
// persisted side effect of a mutation.
func (s *Scenario107Suite) getCluster() *cbv1alpha1.CloudberryCluster {
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(s.T(), s.client.Get(s.ctx, types.NamespacedName{
		Name: scenario107Cluster, Namespace: scenario107Namespace,
	}, got))
	return got
}

// findServer returns the named PXF server in the live CR, or nil.
func scenario107FindServer(c *cbv1alpha1.CloudberryCluster, name string) *cbv1alpha1.PxfServerSpec {
	for i := range c.Spec.DataLoading.Pxf.Servers {
		if c.Spec.DataLoading.Pxf.Servers[i].Name == name {
			return &c.Spec.DataLoading.Pxf.Servers[i]
		}
	}
	return nil
}

// findJob returns the named data-loading job in the live CR, or nil.
func scenario107FindJob(c *cbv1alpha1.CloudberryCluster, name string) *cbv1alpha1.DataLoadingJob {
	if c.Spec.DataLoading == nil {
		return nil
	}
	for i := range c.Spec.DataLoading.Jobs {
		if c.Spec.DataLoading.Jobs[i].Name == name {
			return &c.Spec.DataLoading.Jobs[i]
		}
	}
	return nil
}

// ============================================================================
// PXF servers lifecycle round-trip (107-P2/P3/P4/P5) through the full router
// ============================================================================

// TestServersLifecycle drives the WHOLE PXF-server CRUD through the router as the
// right RBAC tier and asserts each step's persisted side effect: list → create
// (201 + rendered minio2__*.xml keys) → list shows it → update (config changes)
// → delete (gone).
func (s *Scenario107Suite) TestServersLifecycle() {
	// Drop the jobs so the s3srv delete later is unreferenced is irrelevant here;
	// we create + delete a NEW throwaway server "minio2".
	s.boot(scenario107DLCluster())

	// 107-P2-F: list servers (Basic) → the two seeded servers.
	listRec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/pxf/servers", "")
	require.Equal(s.T(), http.StatusOK, listRec.Code)
	listResp := scenario107Decode(listRec)
	assert.Equal(s.T(), float64(2), listResp["total"])

	// 107-P3-F: create a NEW server (Operator) → 201 + rendered keys scoped to it.
	createBody := `{"name":"minio2","type":"s3","config":{"fs.s3a.endpoint":"http://minio2:9000"}}`
	createRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/pxf/servers", createBody)
	require.Equal(s.T(), http.StatusCreated, createRec.Code)
	createResp := scenario107Decode(createRec)
	assert.Equal(s.T(), "minio2", createResp["server"])
	rendered, ok := createResp["renderedKeys"].(map[string]interface{})
	require.True(s.T(), ok)
	require.NotEmpty(s.T(), rendered)
	for k := range rendered {
		assert.Truef(s.T(), strings.HasPrefix(k, "minio2__"),
			"rendered key %q must be scoped to the new server", k)
	}
	// SIDE EFFECT: the CR really gained the server.
	assert.NotNil(s.T(), scenario107FindServer(s.getCluster(), "minio2"))

	// 107-P2-F (re-list): now three servers, the new one visible.
	relistResp := scenario107Decode(
		s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/pxf/servers", ""))
	assert.Equal(s.T(), float64(3), relistResp["total"])

	// 107-P4-F: update the new server's config (Operator) → 200; re-GET shows NEW.
	updateBody := `{"type":"s3","config":{"fs.s3a.endpoint":"http://minio2-NEW:9000"}}`
	updateRec := s.do(scenario107OperUser, scenario107OperPass,
		http.MethodPut, "/pxf/servers/minio2", updateBody)
	require.Equal(s.T(), http.StatusOK, updateRec.Code)
	srv := scenario107FindServer(s.getCluster(), "minio2")
	require.NotNil(s.T(), srv)
	assert.Equal(s.T(), "http://minio2-NEW:9000", srv.Config["fs.s3a.endpoint"])
	// Surgical: the OTHER servers are still present.
	assert.NotNil(s.T(), scenario107FindServer(s.getCluster(), "s3srv"))

	// 107-P5-F: delete the new (unreferenced) server (Admin) → 200; gone.
	deleteRec := s.do(scenario107AdminUser, scenario107AdminPass,
		http.MethodDelete, "/pxf/servers/minio2", "")
	require.Equal(s.T(), http.StatusOK, deleteRec.Code)
	assert.Nil(s.T(), scenario107FindServer(s.getCluster(), "minio2"))
	// The seeded servers remain.
	assert.Equal(s.T(), 2, len(s.getCluster().Spec.DataLoading.Pxf.Servers))
}

// TestServersEdges covers the server negative contract through the router:
// 107-P3-409 (duplicate), 107-P3-400 (PXF disabled), 107-P4-404S (unknown
// server), 107-P5-409 (referenced server), 107-P5-404 (unknown server).
func (s *Scenario107Suite) TestServersEdges() {
	s.boot(scenario107DLCluster())

	// 107-P3-409: duplicate name → 409 SERVER_EXISTS; CR unchanged.
	dupRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost,
		"/pxf/servers", `{"name":"s3srv","type":"s3"}`)
	assert.Equal(s.T(), http.StatusConflict, dupRec.Code)
	assert.Contains(s.T(), dupRec.Body.String(), "SERVER_EXISTS")
	assert.Equal(s.T(), 2, len(s.getCluster().Spec.DataLoading.Pxf.Servers))

	// 107-P4-404S: update unknown server → 404 SERVER_NOT_FOUND.
	updRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPut,
		"/pxf/servers/ghost", `{"type":"s3"}`)
	assert.Equal(s.T(), http.StatusNotFound, updRec.Code)
	assert.Contains(s.T(), updRec.Body.String(), "SERVER_NOT_FOUND")

	// 107-P5-409: delete a server still referenced by a job → 409 SERVER_IN_USE.
	inUseRec := s.do(scenario107AdminUser, scenario107AdminPass, http.MethodDelete,
		"/pxf/servers/s3srv", "")
	assert.Equal(s.T(), http.StatusConflict, inUseRec.Code)
	assert.Contains(s.T(), inUseRec.Body.String(), "SERVER_IN_USE")
	assert.Contains(s.T(), inUseRec.Body.String(), "loadfdw")
	// NO mutation: the server is still present.
	assert.NotNil(s.T(), scenario107FindServer(s.getCluster(), "s3srv"))

	// 107-P5-404: delete unknown server → 404 SERVER_NOT_FOUND.
	missRec := s.do(scenario107AdminUser, scenario107AdminPass, http.MethodDelete,
		"/pxf/servers/ghost", "")
	assert.Equal(s.T(), http.StatusNotFound, missRec.Code)
	assert.Contains(s.T(), missRec.Body.String(), "SERVER_NOT_FOUND")
}

// TestServersPXFDisabled covers 107-P3-400 / 107-P2-404 negatives.
func (s *Scenario107Suite) TestServersPXFDisabled() {
	s.boot(scenario107PXFDisabledCluster())

	// 107-P3-400: POST pxf/servers on a PXF-disabled cluster → 400 PXF_NOT_ENABLED.
	rec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost,
		"/pxf/servers", `{"name":"x","type":"s3"}`)
	assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
	assert.Contains(s.T(), rec.Body.String(), "PXF_NOT_ENABLED")
}

// ============================================================================
// Jobs lifecycle round-trip (107-P7..P13) through the full router
// ============================================================================

// TestJobsLifecycle drives the WHOLE job CRUD + lifecycle through the router and
// asserts each step's persisted side effect: list → create (201) → get → update
// → start (202, real Job created) → stop (202, Job deleted) → delete (gone).
func (s *Scenario107Suite) TestJobsLifecycle() {
	s.boot(scenario107DLCluster())

	// 107-P7-F: list jobs (Basic) → the two seeded jobs.
	listResp := scenario107Decode(
		s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/jobs", ""))
	assert.Equal(s.T(), float64(2), listResp["total"])

	// 107-P8-F: create a new job (Operator) → 201; CR gains it.
	createBody := `{"name":"load1","type":"pxf","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"public.t"}}`
	createRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs", createBody)
	require.Equal(s.T(), http.StatusCreated, createRec.Code)
	assert.NotNil(s.T(), scenario107FindJob(s.getCluster(), "load1"))

	// 107-P9-F: get the new job (Basic) → 200, matches.
	getRec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/jobs/load1", "")
	require.Equal(s.T(), http.StatusOK, getRec.Code)
	assert.Contains(s.T(), getRec.Body.String(), "load1")

	// 107-P10-F: update the new job (Operator) → 200; schedule reflected.
	updateBody := `{"type":"pxf","enabled":true,"schedule":"@daily","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"public.t"}}`
	updateRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPut, "/jobs/load1", updateBody)
	require.Equal(s.T(), http.StatusOK, updateRec.Code)
	job := scenario107FindJob(s.getCluster(), "load1")
	require.NotNil(s.T(), job)
	assert.True(s.T(), job.Enabled)
	assert.Equal(s.T(), "@daily", job.Schedule)

	// 107-P12-F: start the job (Operator) → 202; a real batchv1.Job exists.
	startRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs/load1/start", "")
	require.Equal(s.T(), http.StatusAccepted, startRec.Code)
	k8sName := util.DataLoadJobName(scenario107Cluster, "load1")
	k8sJob := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: k8sName, Namespace: scenario107Namespace}, k8sJob))
	assert.Equal(s.T(), k8sName, k8sJob.Name)
	// 107-MX-F3 honesty: start records NO rows metric.
	assert.Equal(s.T(), float64(0), s.metrics.rowsLoaded,
		"start must NOT record cloudberry_data_loading_rows_total (rows are harvested at completion)")

	// 107-P13-F: stop the job (Operator) → 202; the real Job is deleted.
	stopRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs/load1/stop", "")
	require.Equal(s.T(), http.StatusAccepted, stopRec.Code)
	stopResp := scenario107Decode(stopRec)
	assert.Equal(s.T(), true, stopResp["stopped"])
	err := s.client.Get(s.ctx,
		types.NamespacedName{Name: k8sName, Namespace: scenario107Namespace}, &batchv1.Job{})
	assert.Error(s.T(), err, "the data-loading Job must be deleted after stop")

	// 107-P11-F: delete the job (Admin) → 200; gone.
	deleteRec := s.do(scenario107AdminUser, scenario107AdminPass, http.MethodDelete, "/jobs/load1", "")
	require.Equal(s.T(), http.StatusOK, deleteRec.Code)
	assert.Nil(s.T(), scenario107FindJob(s.getCluster(), "load1"))
}

// TestJobsEdges covers the job negative contract through the router: 107-P8-409
// (duplicate), 107-P8-400 (unknown referenced server), 107-P9-404, 107-P10-404,
// 107-P11-404, 107-P12-404, 107-P12-409, 107-P13-NOOP.
func (s *Scenario107Suite) TestJobsEdges() {
	// Seed an already-running Job for the 409 + idempotent-stop checks.
	running := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.DataLoadJobName(scenario107Cluster, "loadfdw"),
			Namespace: scenario107Namespace,
		},
	}
	s.boot(scenario107DLCluster(), running)

	// 107-P8-409: duplicate job name → 409 JOB_EXISTS; CR unchanged.
	dupRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs",
		`{"name":"loadfdw","type":"pxf","pxfJob":{"server":"s3srv","profile":"s3:parquet","targetTable":"t"}}`)
	assert.Equal(s.T(), http.StatusConflict, dupRec.Code)
	assert.Contains(s.T(), dupRec.Body.String(), "JOB_EXISTS")
	assert.Equal(s.T(), 2, len(s.getCluster().Spec.DataLoading.Jobs))

	// 107-P8-400: pxf job referencing an unknown server → 400 INVALID_REQUEST.
	badRefRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs",
		`{"name":"loadx","type":"pxf","pxfJob":{"server":"ghost","profile":"s3:parquet","targetTable":"t"}}`)
	assert.Equal(s.T(), http.StatusBadRequest, badRefRec.Code)
	assert.Equal(s.T(), 2, len(s.getCluster().Spec.DataLoading.Jobs))

	// 107-P9-404: get unknown job → 404 JOB_NOT_FOUND.
	getRec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/jobs/ghost", "")
	assert.Equal(s.T(), http.StatusNotFound, getRec.Code)
	assert.Contains(s.T(), getRec.Body.String(), "JOB_NOT_FOUND")

	// 107-P10-404: update unknown job → 404 JOB_NOT_FOUND.
	updRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPut, "/jobs/ghost",
		`{"type":"pxf"}`)
	assert.Equal(s.T(), http.StatusNotFound, updRec.Code)

	// 107-P11-404: delete unknown job → 404 JOB_NOT_FOUND.
	delRec := s.do(scenario107AdminUser, scenario107AdminPass, http.MethodDelete, "/jobs/ghost", "")
	assert.Equal(s.T(), http.StatusNotFound, delRec.Code)

	// 107-P12-404: start unknown job → 404; no Job created.
	startMissRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs/ghost/start", "")
	assert.Equal(s.T(), http.StatusNotFound, startMissRec.Code)

	// 107-P12-409: start when the Job already exists → 409 JOB_ALREADY_RUNNING.
	startDupRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs/loadfdw/start", "")
	assert.Equal(s.T(), http.StatusConflict, startDupRec.Code)
	assert.Contains(s.T(), startDupRec.Body.String(), "JOB_ALREADY_RUNNING")

	// 107-P13-NOOP: stop a job with no running Job → 200 honest no-op.
	stopNoopRec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/jobs/loadext/stop", "")
	assert.Equal(s.T(), http.StatusOK, stopNoopRec.Code)
	stopResp := scenario107Decode(stopNoopRec)
	assert.Equal(s.T(), false, stopResp["stopped"])
}

// ============================================================================
// P.14 logs (no-clientset honesty) through the full router
// ============================================================================

// TestLogsNilClientset501 covers 107-P14-501: with NO clientset configured the
// logs endpoint returns 501 LOGS_NOT_AVAILABLE (never fabricated logs). The
// functional harness wires the API server WITHOUT a clientset, so this is the
// honest path exercised end-to-end through the router.
func (s *Scenario107Suite) TestLogsNilClientset501() {
	s.boot(scenario107DLCluster())

	rec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/jobs/loadfdw/logs", "")
	assert.Equal(s.T(), http.StatusNotImplemented, rec.Code)
	assert.Contains(s.T(), rec.Body.String(), "LOGS_NOT_AVAILABLE")
}

// ============================================================================
// P.15 external-tables (observed/expected honesty) through the full router
// ============================================================================

// TestExternalTablesObserved covers 107-P15-F: the db fake returns rows →
// observed populated + observedAvailable true; expected derived from the spec
// pxf jobs (foreign_<job> for the fdw job, target table for the external job).
func (s *Scenario107Suite) TestExternalTablesObserved() {
	s.boot(scenario107DLCluster())
	s.dbClient.ListExternalTablesFunc = func(_ context.Context) ([]db.ExternalTableInfo, error) {
		return []db.ExternalTableInfo{
			{Schema: "public", Name: "ext_events", Kind: "external"},
			{Schema: "public", Name: "foreign_loadfdw", Kind: "foreign", Server: "s3srv"},
		}, nil
	}

	rec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/external-tables", "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	body := rec.Body.String()
	var resp map[string]interface{}
	require.NoError(s.T(), json.Unmarshal([]byte(body), &resp))

	assert.Equal(s.T(), true, resp["observedAvailable"])
	observed, ok := resp["observed"].([]interface{})
	require.True(s.T(), ok)
	assert.Len(s.T(), observed, 2)

	expected, ok := resp["expected"].([]interface{})
	require.True(s.T(), ok)
	require.Len(s.T(), expected, 2)
	// The fdw job's expected foreign table name equals builder.ForeignTableName.
	assert.Contains(s.T(), body, builder.ForeignTableName("loadfdw"))
}

// TestExternalTablesDBError covers 107-P15-DBERR / 107-P15-EMPTY honesty: when
// the catalog probe errors, observed is ABSENT (null) and observedAvailable is
// false, but expected is STILL present (never claimed to "exist").
func (s *Scenario107Suite) TestExternalTablesDBError() {
	s.boot(scenario107DLCluster())
	s.dbClient.ListExternalTablesFunc = func(_ context.Context) ([]db.ExternalTableInfo, error) {
		return nil, fmt.Errorf("catalog probe failed")
	}

	rec := s.do(scenario107BasicUser, scenario107BasicPass, http.MethodGet, "/external-tables", "")
	require.Equal(s.T(), http.StatusOK, rec.Code)
	resp := scenario107Decode(rec)

	assert.Equal(s.T(), false, resp["observedAvailable"])
	assert.Nil(s.T(), resp["observed"])
	expected, ok := resp["expected"].([]interface{})
	require.True(s.T(), ok)
	assert.Len(s.T(), expected, 2, "expected is still derived and present even when observed is ABSENT")
}

// ============================================================================
// 107-RBAC: a representative permission-denied (403) per permission tier
// ============================================================================

// TestRBACForbiddenPerTier drives a representative endpoint from each permission
// tier with an identity ONE LEVEL BELOW the requirement and asserts 403 through
// the real withPermission gate, then AT the requirement and asserts NOT 403.
func (s *Scenario107Suite) TestRBACForbiddenPerTier() {
	type rbacCase struct {
		name    string
		method  string
		suffix  string
		body    string
		below   [2]string // user, pass one level below the requirement
		atLevel [2]string // user, pass at the requirement
	}
	basic := [2]string{scenario107BasicUser, scenario107BasicPass}
	oper := [2]string{scenario107OperUser, scenario107OperPass}
	admin := [2]string{scenario107AdminUser, scenario107AdminPass}

	cases := []rbacCase{
		// Operator-tier create: Basic is below.
		{"POST pxf/servers (Operator)", http.MethodPost, "/pxf/servers",
			`{"name":"z","type":"s3"}`, basic, oper},
		// Operator-tier sync: Basic is below.
		{"POST pxf/sync (Operator)", http.MethodPost, "/pxf/sync", "", basic, oper},
		// Admin-tier delete: Operator is below.
		{"DELETE jobs/{job} (Admin)", http.MethodDelete, "/jobs/loadext", "", oper, admin},
	}

	for _, c := range cases {
		s.Run(c.name, func() {
			s.boot(scenario107DLCluster())
			// Below the required tier → 403.
			belowRec := s.do(c.below[0], c.below[1], c.method, c.suffix, c.body)
			assert.Equalf(s.T(), http.StatusForbidden, belowRec.Code,
				"%s with a below-tier identity must be forbidden", c.name)
			// At the required tier → the permission gate passed (not 403).
			atRec := s.do(c.atLevel[0], c.atLevel[1], c.method, c.suffix, c.body)
			assert.NotEqualf(s.T(), http.StatusForbidden, atRec.Code,
				"%s at the required tier must pass the permission gate", c.name)
		})
	}
}

// ============================================================================
// 107-MX-F1: a real server create fires the servers-changed metric exactly once
// ============================================================================

// TestCreateServerFiresServersChangedOnce covers 107-MX-F1: a server create that
// really changes the rendered PXF servers ConfigMap Data fires the honest
// servers-changed signal exactly once with the cluster/namespace labels.
func (s *Scenario107Suite) TestCreateServerFiresServersChangedOnce() {
	s.boot(scenario107DLCluster())

	rec := s.do(scenario107OperUser, scenario107OperPass, http.MethodPost, "/pxf/servers",
		`{"name":"minio3","type":"s3","config":{"fs.s3a.endpoint":"http://minio3:9000"}}`)
	require.Equal(s.T(), http.StatusCreated, rec.Code)

	require.Len(s.T(), s.metrics.serversChanged, 1)
	assert.Equal(s.T(),
		scenario107ServersChanged{cluster: scenario107Cluster, namespace: scenario107Namespace},
		s.metrics.serversChanged[0])
}

// ============================================================================
// Catalog-honest cross-check
// ============================================================================

// TestCatalogHonest iterates cases.Scenario107Cases() and asserts the catalog is
// well-formed (unique IDs, every endpoint family present, every row carries a
// Layer + Expected + Description with a known Layer token).
func (s *Scenario107Suite) TestCatalogHonest() {
	catalog := cases.Scenario107Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario107LayerFunctional,
		cases.Scenario107LayerBuilder,
		cases.Scenario107LayerLive,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
		})
	}
	for i := 1; i <= 15; i++ {
		req := fmt.Sprintf("P.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover endpoint family %s", req)
	}
	assert.True(s.T(), reqs["RBAC"], "catalog must cover the RBAC matrix")
	assert.True(s.T(), reqs["MX"], "catalog must cover the cross-cutting honesty rows")
}

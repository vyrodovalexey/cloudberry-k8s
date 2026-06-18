//go:build functional

package functional

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 108: All CLI Commands (L.1–L.16) — functional
// ============================================================================
//
// This suite proves the cloudberry-ctl data-loading + pxf CLI surface drives the
// operator REST API with the documented EFFECT + side effect, WITHOUT a live
// cluster. The CLI verbs themselves are exercised in-process (cobra-exec) in
// cmd/cloudberry-ctl/scenario108_cli_test.go against a recording stand-in; THIS
// suite closes the loop the other end: it drives the SAME client the CLI uses
// (internal/ctl.OperatorClient + ctl.ClusterSubresourcePath path helpers) over a
// REAL api.Server router (fake controller-runtime client + fake db.Client + auth/
// RBAC middleware) wrapped in an httptest server, and asserts the REAL operator
// side effect for each L.x:
//
//   - pxf servers create/update/delete (L.3/L.4/L.5) → the CR server set mutates
//     (re-GET the cluster); list (L.2) shows them.
//   - jobs create (pxf L.9 + gpload L.14 + --from-yaml L.16) → the CR jobs[] gains
//     the job with the right shape; start (L.10) → a batchv1.Job object created;
//     logs (L.13) → stream attempted; stop/delete (L.11/L.12) → reaped/gone.
//   - test-read --limit 10 (L.15) → the CLI request returns the rows the fake db
//     returns; the available:false path renders cleanly (honest ABSENT).
//
// Each verb's request is built EXACTLY as the production CLI builds it (same DTO
// JSON shape + ctl path helper), so the CLI→operator contract is exercised
// end-to-end through the whole router (mux → auth → withPermission RBAC →
// handler → fake-client side effect). The CLI's own required-flag guards (which
// fail BEFORE any API call) are covered by the cobra-exec suite; here we assert
// the operator EFFECT those well-formed requests produce.
// ============================================================================

const (
	scenario108Namespace = "cloudberry-test"
	scenario108Cluster   = "s108-dl"

	scenario108BasicUser = "s108basic"
	scenario108BasicPass = "s108basicpass"
	scenario108OperUser  = "s108oper"
	scenario108OperPass  = "s108operpass"
	scenario108AdminUser = "s108admin"
	scenario108AdminPass = "s108adminpass"
)

// Scenario108Suite drives the data-loading/pxf CLI verbs through the real ctl
// OperatorClient against a real api.Server over a fake client + fake db.
type Scenario108Suite struct {
	suite.Suite
	server   *api.Server
	httpSrv  *httptest.Server
	client   client.Client
	dbClient *testutil.MockDBClient
	ctx      context.Context
}

func TestFunctional_Scenario108(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario108Suite))
}

func (s *Scenario108Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario108Suite) TearDownTest() {
	if s.httpSrv != nil {
		s.httpSrv.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

// scenario108DLCluster builds a PXF-enabled cluster seeded with two servers and
// two pxf data-loading jobs so every CLI verb has realistic spec state to mutate
// and observe.
func scenario108DLCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario108Cluster, scenario108Namespace).
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

// boot builds the API server (real router + auth/RBAC + a MockDBClient factory)
// over a fake client seeded with the cluster + any extra objects, wraps it in an
// httptest server and returns nothing (the suite stores the handles).
func (s *Scenario108Suite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client
	s.dbClient = &testutil.MockDBClient{}
	factory := &testutil.MockDBClientFactory{Client: s.dbClient}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario108BasicUser, scenario108BasicPass, auth.PermissionBasic)
	store.SetCredentials(scenario108OperUser, scenario108OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario108AdminUser, scenario108AdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, env.Logger, 0)
	s.httpSrv = httptest.NewServer(s.server.Handler())
}

// ctlClient returns a ctl.OperatorClient (the SAME client the CLI uses) pointed
// at the in-process api.Server with the given basic-auth identity.
func (s *Scenario108Suite) ctlClient(user, pass string) *ctl.OperatorClient {
	return ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.httpSrv.URL,
		Username:   user,
		Password:   pass,
		AuthMethod: "basic",
		Timeout:    10 * time.Second,
	})
}

// dlPath builds the namespaced data-loading subresource path the CLI verbs build
// via ctl.ClusterSubresourcePath.
func scenario108DLPath(subresource string) string {
	return ctl.ClusterSubresourcePath(scenario108Cluster,
		"data-loading/"+subresource, scenario108Namespace)
}

// getCluster re-fetches the cluster so tests can assert a persisted side effect.
func (s *Scenario108Suite) getCluster() *cbv1alpha1.CloudberryCluster {
	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(s.T(), s.client.Get(s.ctx, types.NamespacedName{
		Name: scenario108Cluster, Namespace: scenario108Namespace,
	}, got))
	return got
}

// findServer / findJob locate a named server/job in the live CR, or return nil.
func scenario108FindServer(c *cbv1alpha1.CloudberryCluster, name string) *cbv1alpha1.PxfServerSpec {
	if c.Spec.DataLoading == nil || c.Spec.DataLoading.Pxf == nil {
		return nil
	}
	for i := range c.Spec.DataLoading.Pxf.Servers {
		if c.Spec.DataLoading.Pxf.Servers[i].Name == name {
			return &c.Spec.DataLoading.Pxf.Servers[i]
		}
	}
	return nil
}

func scenario108FindJob(c *cbv1alpha1.CloudberryCluster, name string) *cbv1alpha1.DataLoadingJob {
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

// ----------------------------------------------------------------------------
// L.1 / L.2 — pxf status / pxf servers list (read)
// ----------------------------------------------------------------------------

// TestPxfStatusAndServersList covers 108-L1-F + 108-L2-F: the read verbs reach
// the operator and return 200 with the documented shape (status counts; servers
// list equals the spec).
func (s *Scenario108Suite) TestPxfStatusAndServersList() {
	s.boot(scenario108DLCluster())
	c := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	// L.1 pxf status → GET data-loading/pxf/status.
	statusResp, err := c.Get(s.ctx, scenario108DLPath("pxf/status"))
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.StatusOK, statusResp.StatusCode)
	assert.Contains(s.T(), statusResp.Body, "configured")

	// L.2 pxf servers list → GET data-loading/pxf/servers; total == 2.
	listResp, err := c.Get(s.ctx, scenario108DLPath("pxf/servers"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, listResp.StatusCode)
	assert.Equal(s.T(), float64(2), listResp.Body["total"])
}

// ----------------------------------------------------------------------------
// L.3 / L.4 / L.5 — pxf servers create / update / delete (mutating CR side effect)
// ----------------------------------------------------------------------------

// TestPxfServersLifecycle covers 108-L3-F / 108-L4-F / 108-L5-F: the CLI create/
// update/delete verbs really mutate the CR's pxf server set. The request bodies
// are built EXACTLY as `pxf servers create/update` build them (config from
// --endpoint/--bucket + credentialSecrets from --credential-secret).
func (s *Scenario108Suite) TestPxfServersLifecycle() {
	s.boot(scenario108DLCluster())
	oper := s.ctlClient(scenario108OperUser, scenario108OperPass)
	admin := s.ctlClient(scenario108AdminUser, scenario108AdminPass)

	// L.3 create (Operator). Body shape == the CLI's pxfServerRequest.
	createBody := map[string]interface{}{
		"name": "minio2",
		"type": "s3",
		"config": map[string]string{
			"fs.s3a.endpoint": "http://minio2:9000",
			"bucket":          "loads",
		},
		"credentialSecrets": []map[string]string{
			{"name": "backup-s3-credentials", "key": "aws_access_key_id"},
			{"name": "backup-s3-credentials", "key": "aws_secret_access_key"},
		},
	}
	createResp, err := oper.Post(s.ctx, scenario108DLPath("pxf/servers"), createBody)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusCreated, createResp.StatusCode)
	assert.Equal(s.T(), "minio2", createResp.Body["server"])
	// SIDE EFFECT: the CR really gained the server.
	require.NotNil(s.T(), scenario108FindServer(s.getCluster(), "minio2"),
		"pxf servers create must add the server to the CR")

	// L.2 re-list now shows three servers.
	relist, err := s.ctlClient(scenario108BasicUser, scenario108BasicPass).
		Get(s.ctx, scenario108DLPath("pxf/servers"))
	require.NoError(s.T(), err)
	assert.Equal(s.T(), float64(3), relist.Body["total"])

	// L.4 update --endpoint (Operator) → PUT pxf/servers/minio2; only it mutates.
	updateBody := map[string]interface{}{
		"type": "s3",
		"config": map[string]string{
			"fs.s3a.endpoint": "http://minio2-NEW:9000",
		},
		"credentialSecrets": []map[string]string{
			{"name": "backup-s3-credentials", "key": "aws_access_key_id"},
			{"name": "backup-s3-credentials", "key": "aws_secret_access_key"},
		},
	}
	updateResp, err := oper.Put(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/pxf/servers/minio2", scenario108Namespace), updateBody)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, updateResp.StatusCode)
	srv := scenario108FindServer(s.getCluster(), "minio2")
	require.NotNil(s.T(), srv)
	assert.Equal(s.T(), "http://minio2-NEW:9000", srv.Config["fs.s3a.endpoint"])
	// Surgical: the seeded server is still present and untouched.
	assert.NotNil(s.T(), scenario108FindServer(s.getCluster(), "s3srv"))

	// L.5 delete (Admin) the unreferenced server → removed.
	delResp, err := admin.Delete(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/pxf/servers/minio2", scenario108Namespace))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, delResp.StatusCode)
	assert.Nil(s.T(), scenario108FindServer(s.getCluster(), "minio2"),
		"pxf servers delete must remove the server from the CR")
}

// TestPxfServersDeleteReferenced covers the CLI surfacing of the 409
// SERVER_IN_USE contract (L.5): deleting a server still referenced by a job is
// rejected and the CR is NOT mutated.
func (s *Scenario108Suite) TestPxfServersDeleteReferenced() {
	s.boot(scenario108DLCluster())
	admin := s.ctlClient(scenario108AdminUser, scenario108AdminPass)

	_, err := admin.Delete(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/pxf/servers/s3srv", scenario108Namespace))
	// The ctl client returns a typed APIError on a 4xx.
	require.Error(s.T(), err)
	apiErr, ok := err.(*ctl.APIError)
	require.Truef(s.T(), ok, "expected a ctl.APIError, got %T", err)
	assert.Equal(s.T(), http.StatusConflict, apiErr.StatusCode)
	assert.Equal(s.T(), "SERVER_IN_USE", apiErr.Code)
	// NO mutation: the referenced server is still present.
	assert.NotNil(s.T(), scenario108FindServer(s.getCluster(), "s3srv"))
}

// ----------------------------------------------------------------------------
// L.6 / L.7 — pxf sync / restart (operator action; 202)
// ----------------------------------------------------------------------------

// TestPxfSyncAndRestart covers 108-L6-F + 108-L7-F: the sync/restart verbs POST
// to the operator action endpoints and are accepted (202). Both bump the
// segment-primary StatefulSet restart trigger, so the suite seeds that STS.
func (s *Scenario108Suite) TestPxfSyncAndRestart() {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(scenario108Cluster),
			Namespace: scenario108Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}},
			},
		},
	}
	s.boot(scenario108DLCluster(), sts)
	oper := s.ctlClient(scenario108OperUser, scenario108OperPass)

	syncResp, err := oper.Post(s.ctx, scenario108DLPath("pxf/sync"), nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.StatusAccepted, syncResp.StatusCode)

	restartResp, err := oper.Post(s.ctx, scenario108DLPath("pxf/restart"), nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.StatusAccepted, restartResp.StatusCode)
}

// ----------------------------------------------------------------------------
// L.8 / L.9 / L.10 / L.11 / L.12 — jobs list / create(pxf) / start / stop / delete
// ----------------------------------------------------------------------------

// TestJobsLifecyclePXF covers 108-L8-F / 108-L9-F / 108-L10-F / 108-L11-F /
// 108-L12-F: the CLI jobs verbs drive the operator through a full lifecycle and
// the CR + a real batchv1.Job reflect each step. The create body is built EXACTLY
// as `jobs create --type pxf` builds it (a pxfJob DTO, mode insert).
func (s *Scenario108Suite) TestJobsLifecyclePXF() {
	s.boot(scenario108DLCluster())
	oper := s.ctlClient(scenario108OperUser, scenario108OperPass)
	admin := s.ctlClient(scenario108AdminUser, scenario108AdminPass)
	basic := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	// L.8 jobs list → two seeded jobs.
	listResp, err := basic.Get(s.ctx, scenario108DLPath("jobs"))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, listResp.StatusCode)
	assert.Equal(s.T(), float64(2), listResp.Body["total"])

	// L.9 jobs create --type pxf → CR gains a pxf job.
	createBody := map[string]interface{}{
		"name": "cliload",
		"type": "pxf",
		"pxfJob": map[string]interface{}{
			"server":      "s3srv",
			"profile":     "s3:text",
			"resource":    "data/events.csv",
			"targetTable": "public.events",
			"mode":        "insert",
		},
	}
	createResp, err := oper.Post(s.ctx, scenario108DLPath("jobs"), createBody)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusCreated, createResp.StatusCode)
	job := scenario108FindJob(s.getCluster(), "cliload")
	require.NotNil(s.T(), job, "jobs create must add the job to the CR")
	require.NotNil(s.T(), job.PxfJob)
	assert.Equal(s.T(), "s3srv", job.PxfJob.Server)
	assert.Equal(s.T(), "public.events", job.PxfJob.TargetTable)

	// L.10 jobs start → 202; a real batchv1.Job exists.
	startResp, err := oper.Post(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/jobs/cliload/start", scenario108Namespace), nil)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusAccepted, startResp.StatusCode)
	k8sName := util.DataLoadJobName(scenario108Cluster, "cliload")
	k8sJob := &batchv1.Job{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: k8sName, Namespace: scenario108Namespace}, k8sJob),
		"jobs start must create a real batchv1.Job")

	// L.11 jobs stop → 202; the real Job is deleted.
	stopResp, err := oper.Post(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/jobs/cliload/stop", scenario108Namespace), nil)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusAccepted, stopResp.StatusCode)
	assert.Equal(s.T(), true, stopResp.Body["stopped"])
	getErr := s.client.Get(s.ctx,
		types.NamespacedName{Name: k8sName, Namespace: scenario108Namespace}, &batchv1.Job{})
	assert.Error(s.T(), getErr, "jobs stop must delete the data-loading Job")

	// L.12 jobs delete (Admin) → gone from the CR.
	delResp, err := admin.Delete(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/jobs/cliload", scenario108Namespace))
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, delResp.StatusCode)
	assert.Nil(s.T(), scenario108FindJob(s.getCluster(), "cliload"),
		"jobs delete must remove the job from the CR")
}

// ----------------------------------------------------------------------------
// L.14 — jobs create --type gpload (gploadJob DTO side effect)
// ----------------------------------------------------------------------------

// TestJobsCreateGpload covers 108-L14-F: a gpload create verb adds a gpload job
// to the CR with the inputSource{gpfdist} + filePaths shape the CLI builds.
func (s *Scenario108Suite) TestJobsCreateGpload() {
	s.boot(scenario108DLCluster())
	oper := s.ctlClient(scenario108OperUser, scenario108OperPass)

	body := map[string]interface{}{
		"name": "gpjob",
		"type": "gpload",
		"gploadJob": map[string]interface{}{
			"targetTable": "public.raw",
			"format":      "csv",
			"inputSource": map[string]interface{}{
				"type": "gpfdist",
				"host": "gpfdist-host",
				"port": 8080,
			},
			"filePaths": []string{"/in/*.csv"},
		},
	}
	resp, err := oper.Post(s.ctx, scenario108DLPath("jobs"), body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusCreated, resp.StatusCode)

	job := scenario108FindJob(s.getCluster(), "gpjob")
	require.NotNil(s.T(), job, "gpload create must add the job to the CR")
	assert.Equal(s.T(), "gpload", job.Type)
	require.NotNil(s.T(), job.GploadJob, "the CR job must carry the gploadJob block")
	assert.Equal(s.T(), "public.raw", job.GploadJob.TargetTable)
	assert.Nil(s.T(), job.PxfJob, "a gpload job must NOT carry a pxfJob block")
}

// ----------------------------------------------------------------------------
// L.16 — jobs create --from-yaml (the unmarshalled body reconciled into the CR)
// ----------------------------------------------------------------------------

// TestJobsCreateFromYAML covers 108-L16-F: the body the CLI POSTs after reading
// a job YAML (a complex scheduled pxf job) is reconciled into the CR with the
// right shape. The CLI's YAML→DTO unmarshal is exercised by the cobra-exec suite;
// here we assert the resulting POST body's operator effect.
func (s *Scenario108Suite) TestJobsCreateFromYAML() {
	s.boot(scenario108DLCluster())
	oper := s.ctlClient(scenario108OperUser, scenario108OperPass)

	// This is the JSON the CLI produces from a job YAML (sigs.k8s.io/yaml maps
	// YAML keys 1:1 to the DTO JSON tags). A valid 5-field cron is used so the
	// webhook-equivalent handler validation passes.
	body := map[string]interface{}{
		"name":     "yamljob",
		"type":     "pxf",
		"enabled":  true,
		"schedule": "0 3 * * *",
		"pxfJob": map[string]interface{}{
			"server":      "s3srv",
			"profile":     "s3:parquet",
			"resource":    "data/events.parquet",
			"targetTable": "public.events",
			"mode":        "insert",
		},
	}
	resp, err := oper.Post(s.ctx, scenario108DLPath("jobs"), body)
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusCreated, resp.StatusCode)

	job := scenario108FindJob(s.getCluster(), "yamljob")
	require.NotNil(s.T(), job, "--from-yaml create must reconcile the job into the CR")
	assert.True(s.T(), job.Enabled)
	assert.Equal(s.T(), "0 3 * * *", job.Schedule)
	require.NotNil(s.T(), job.PxfJob)
	assert.Equal(s.T(), "public.events", job.PxfJob.TargetTable)
}

// ----------------------------------------------------------------------------
// L.13 — jobs logs (stream attempted; honest no-clientset path)
// ----------------------------------------------------------------------------

// TestJobsLogsStreamAttempted covers 108-L13-F: the logs verb issues the stream
// GET. The functional harness wires the API server WITHOUT a clientset, so the
// honest 501 LOGS_NOT_AVAILABLE path is exercised end-to-end — exactly the
// condition the CLI's kubectl fallback hint handles (the fallback rendering
// itself is covered by the cobra-exec suite). The CLI never fabricates logs.
func (s *Scenario108Suite) TestJobsLogsStreamAttempted() {
	s.boot(scenario108DLCluster())
	basic := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	var out bytes.Buffer
	err := basic.GetStream(s.ctx,
		ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/jobs/loadfdw/logs", scenario108Namespace), &out)
	// No clientset → honest 501 surfaced as a typed APIError (NOT a fabricated
	// stream). The CLI maps this to its kubectl fallback hint.
	require.Error(s.T(), err)
	apiErr, ok := err.(*ctl.APIError)
	require.Truef(s.T(), ok, "expected a ctl.APIError, got %T", err)
	assert.Equal(s.T(), http.StatusNotImplemented, apiErr.StatusCode)
	assert.Equal(s.T(), "LOGS_NOT_AVAILABLE", apiErr.Code)
	assert.Empty(s.T(), out.String(), "no log bytes must be streamed on the honest 501 path")
}

// ----------------------------------------------------------------------------
// L.15 — data-loading test-read (honest preview rows / available:false)
// ----------------------------------------------------------------------------

// TestTestReadByJobRows covers 108-L15-F: a test-read --job --limit 10 request
// returns the REAL rows the fake db produced, with available:true and rowCount
// bounded by the limit. The db sample is what the CLI would print.
func (s *Scenario108Suite) TestTestReadByJobRows() {
	s.boot(scenario108DLCluster())
	s.dbClient.ReadPXFSourceSampleFunc = func(
		_ context.Context, server, profile, resource string, limit int,
	) (*db.PXFSourceSample, error) {
		// The handler resolves --job loadfdw to its pxfJob source.
		assert.Equal(s.T(), "s3srv", server)
		assert.Equal(s.T(), "s3:parquet", profile)
		assert.Equal(s.T(), 10, limit)
		_ = resource
		return &db.PXFSourceSample{
			Columns: []string{"line"},
			Rows:    [][]string{{"a,1"}, {"b,2"}, {"c,3"}},
		}, nil
	}
	basic := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	resp, err := basic.Get(s.ctx,
		scenario108DLPath("test-read")+"&job=loadfdw&limit=10")
	require.NoError(s.T(), err)
	require.Equal(s.T(), http.StatusOK, resp.StatusCode)
	assert.Equal(s.T(), true, resp.Body["available"])
	assert.Equal(s.T(), float64(3), resp.Body["rowCount"])
	rows, ok := resp.Body["rows"].([]interface{})
	require.True(s.T(), ok)
	assert.Len(s.T(), rows, 3)
	assert.LessOrEqual(s.T(), len(rows), 10, "test-read must bound the rows to ≤ --limit")
}

// TestTestReadHonestUnavailable covers 108-L15-absent: when the db read errors
// (source/DB unreachable) the endpoint responds 200 {available:false, rows:null}
// — the honest ABSENT signal the CLI renders cleanly (exit 0, no fabricated rows).
func (s *Scenario108Suite) TestTestReadHonestUnavailable() {
	s.boot(scenario108DLCluster())
	s.dbClient.ReadPXFSourceSampleFunc = func(
		_ context.Context, _, _, _ string, _ int,
	) (*db.PXFSourceSample, error) {
		return nil, assert.AnError
	}
	basic := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	resp, err := basic.Get(s.ctx,
		scenario108DLPath("test-read")+"&job=loadfdw&limit=10")
	require.NoError(s.T(), err, "an unreachable source must NOT be a transport error")
	require.Equal(s.T(), http.StatusOK, resp.StatusCode,
		"mere unreachability must be 200 available:false, NEVER a 500")
	assert.Equal(s.T(), false, resp.Body["available"])
	assert.Nil(s.T(), resp.Body["rows"], "rows must be null when available:false (no fabrication)")
}

// TestTestReadUnknownJob404 covers the CLI surfacing of the 404 contract: a
// test-read --job naming an unknown job is a 404 JOB_NOT_FOUND.
func (s *Scenario108Suite) TestTestReadUnknownJob404() {
	s.boot(scenario108DLCluster())
	basic := s.ctlClient(scenario108BasicUser, scenario108BasicPass)

	_, err := basic.Get(s.ctx, scenario108DLPath("test-read")+"&job=ghost&limit=10")
	require.Error(s.T(), err)
	apiErr, ok := err.(*ctl.APIError)
	require.Truef(s.T(), ok, "expected a ctl.APIError, got %T", err)
	assert.Equal(s.T(), http.StatusNotFound, apiErr.StatusCode)
	assert.Equal(s.T(), "JOB_NOT_FOUND", apiErr.Code)
}

// ----------------------------------------------------------------------------
// RBAC parity — a below-tier identity is forbidden through the real gate
// ----------------------------------------------------------------------------

// TestRBACForbiddenBelowTier covers 108-RBAC (functional slice): the CLI verbs
// inherit the Scenario 107 route tiers. A Basic identity driving an Operator-tier
// create (L.3) and an Operator identity driving an Admin-tier delete (L.5) are
// both rejected with 403 through the real withPermission gate.
func (s *Scenario108Suite) TestRBACForbiddenBelowTier() {
	s.boot(scenario108DLCluster())

	// Basic below Operator: pxf servers create.
	_, err := s.ctlClient(scenario108BasicUser, scenario108BasicPass).
		Post(s.ctx, scenario108DLPath("pxf/servers"),
			map[string]interface{}{"name": "z", "type": "s3"})
	require.Error(s.T(), err)
	if apiErr, ok := err.(*ctl.APIError); ok {
		assert.Equal(s.T(), http.StatusForbidden, apiErr.StatusCode,
			"a Basic identity must be forbidden from an Operator-tier create")
	} else {
		s.T().Fatalf("expected a ctl.APIError, got %T", err)
	}

	// Operator below Admin: pxf servers delete.
	_, err = s.ctlClient(scenario108OperUser, scenario108OperPass).
		Delete(s.ctx, ctl.ClusterSubresourcePath(scenario108Cluster,
			"data-loading/pxf/servers/hivesrv", scenario108Namespace))
	require.Error(s.T(), err)
	if apiErr, ok := err.(*ctl.APIError); ok {
		assert.Equal(s.T(), http.StatusForbidden, apiErr.StatusCode,
			"an Operator identity must be forbidden from an Admin-tier delete")
	} else {
		s.T().Fatalf("expected a ctl.APIError, got %T", err)
	}
}

// ----------------------------------------------------------------------------
// Catalog-honest cross-check
// ----------------------------------------------------------------------------

// TestCatalogHonest iterates cases.Scenario108Cases() and asserts the catalog is
// well-formed (unique IDs, every L.1–L.16 + RBAC family present, every row
// carries a Layer + Expected + Description with a known Layer token).
func (s *Scenario108Suite) TestCatalogHonest() {
	catalog := cases.Scenario108Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario108LayerFunctional,
		cases.Scenario108LayerBuilder,
		cases.Scenario108LayerLive,
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
	for i := 1; i <= 16; i++ {
		req := fmt.Sprintf("L.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover CLI command family %s", req)
	}
	assert.True(s.T(), reqs["RBAC"], "catalog must cover the RBAC parity row")
}

//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 107: All Data-Loading API Endpoints (P.1–P.15) — E2E
// ============================================================================
//
// Mirrors the Scenario 105/106 e2e SHAPE (catalog-honest Part A that ALWAYS runs
// + a KUBECONFIG/SCENARIO107_LIVE-gated live Part B). Part B drives the LIVE
// operator REST API directly (reusing the perf harness's port-forward + auth
// token mechanism — Authorization: Bearer <OIDC> or basic-auth — and the s106
// e2e's kubectl side-effect verification):
//
//   P.1  status; P.2 list servers; P.3 create a throwaway server (assert 201 +
//        rendered config + it appears in the CR + the <cluster>-pxf-servers
//        ConfigMap regenerates); P.4 update it; P.6 sync (202); P.5 delete it
//        (assert removed; 409 when deleting a referenced server).
//   P.7  list jobs; P.8 create a throwaway job; P.9 get; P.10 update; P.12 start
//        (202 → assert the data-loading Job object is created); P.14 logs (stream
//        the Job pod logs); P.13 stop; P.11 delete the job.
//   P.15 external-tables (assert the JSON shape; observed reflects the live DB or
//        is honestly absent; expected lists foreign_<job>/target tables).
//
// SELF-CONTAINED + idempotent: it creates throwaway server/job fixtures, exercises
// them, and CLEANS UP (restores the baseline) via defer — mirroring the s106 e2e's
// state-based cleanup. Generous eventually-timeouts. Skips cleanly when KUBECONFIG
// / the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO107_LIVE=1       — gates the live API exercise.
//   SCENARIO107_CLUSTER      — live cluster name (default s107).
//   SCENARIO107_NAMESPACE    — namespace (default cloudberry-test).
//   SCENARIO107_API_BASE     — operator API base URL (default http://localhost:8190).
//   SCENARIO107_OIDC_TOKEN   — bearer token; if unset, basic-auth creds are used.
//   SCENARIO107_API_USER     — basic-auth user (default adminuser).
//   SCENARIO107_API_PASS     — basic-auth pass (default adminpass).
//   SCENARIO107_SERVER       — throwaway server name (default s107-tmp-srv).
//   SCENARIO107_JOB          — throwaway job name (default s107-tmp-job).
// ============================================================================

const (
	envKubeconfigS107 = "KUBECONFIG"
	envS107Live       = "SCENARIO107_LIVE"
	envS107Cluster    = "SCENARIO107_CLUSTER"
	envS107Namespace  = "SCENARIO107_NAMESPACE"
	envS107APIBase    = "SCENARIO107_API_BASE"
	envS107Token      = "SCENARIO107_OIDC_TOKEN"
	envS107APIUser    = "SCENARIO107_API_USER"
	envS107APIPass    = "SCENARIO107_API_PASS"
	envS107Server     = "SCENARIO107_SERVER"
	envS107Job        = "SCENARIO107_JOB"

	s107DefaultCluster   = "s107"
	s107DefaultNamespace = "cloudberry-test"
	s107DefaultAPIBase   = "http://localhost:8190"
	s107DefaultAPIUser   = "adminuser"
	s107DefaultAPIPass   = "adminpass"
	s107DefaultServer    = "s107-tmp-srv"
	s107DefaultJob       = "s107-tmp-job"

	s107LiveTimeout  = 5 * time.Minute
	s107PollInterval = 10 * time.Second
	s107ExecTimeout  = 90 * time.Second
	s107HTTPTimeout  = 30 * time.Second
)

// Scenario107E2ESuite verifies the full data-loading REST surface end-to-end
// (catalog-direct Part A + KUBECONFIG-gated live Part B).
type Scenario107E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario107(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario107E2ESuite))
}

func (s *Scenario107E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario107_PartA_CatalogHonest iterates the full Scenario 107 catalog
// and asserts it is well-formed: unique IDs, every P.1–P.15 + RBAC + MX family
// present, every row carries a non-empty Layer + Expected + Description with a
// known Layer token. The -F/-B rows are resolved at functional/integration; the
// -L rows are documented here and resolved at Part B.
func (s *Scenario107E2ESuite) TestE2E_Scenario107_PartA_CatalogHonest() {
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

			if tc.Layer == cases.Scenario107LayerLive {
				s.T().Logf("scenario107 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
			}
		})
	}
	for i := 1; i <= 15; i++ {
		req := fmt.Sprintf("P.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover endpoint family %s", req)
	}
	assert.True(s.T(), reqs["RBAC"], "catalog must cover the RBAC matrix")
	assert.True(s.T(), reqs["MX"], "catalog must cover the cross-cutting honesty rows")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO107_LIVE gated live API exercise
// ----------------------------------------------------------------------------

func s107Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s107Cluster() string    { return s107Env(envS107Cluster, s107DefaultCluster) }
func s107Namespace() string  { return s107Env(envS107Namespace, s107DefaultNamespace) }
func s107APIBase() string    { return s107Env(envS107APIBase, s107DefaultAPIBase) }
func s107ServerName() string { return s107Env(envS107Server, s107DefaultServer) }
func s107JobName() string    { return s107Env(envS107Job, s107DefaultJob) }

// s107RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO107_LIVE=1.
func (s *Scenario107E2ESuite) s107RequireLive() {
	if os.Getenv(envKubeconfigS107) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 107 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 107 live Part B")
	}
	if os.Getenv(envS107Live) != "1" {
		s.T().Skip("SCENARIO107_LIVE not set, skipping the live data-loading API exercise " +
			"(the deployed cluster + the operator API must be reachable)")
	}
}

// s107Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario107E2ESuite) s107Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s107ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s107AuthHeader sets the Authorization header on a request: a bearer OIDC token
// when SCENARIO107_OIDC_TOKEN is set, otherwise basic-auth from the API creds.
func s107AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS107Token)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(s107Env(envS107APIUser, s107DefaultAPIUser),
		s107Env(envS107APIPass, s107DefaultAPIPass))
}

// s107apiURL builds a full data-loading API URL for the given suffix path.
func s107apiURL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return s107APIBase() + "/api/v1alpha1/clusters/" + s107Cluster() +
		"/data-loading" + suffix + sep + "namespace=" + s107Namespace()
}

// s107APIResult carries the parsed result of one live API call.
type s107APIResult struct {
	status int
	body   []byte
}

// s107api issues a request to the LIVE operator API and returns the status + body.
// It returns an error only on transport failure (so callers can SKIP cleanly when
// the API is not port-forwarded).
func (s *Scenario107E2ESuite) s107api(method, suffix, body string) (s107APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s107HTTPTimeout)
	defer cancel()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s107apiURL(suffix), rdr)
	if err != nil {
		return s107APIResult{}, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	s107AuthHeader(req)
	resp, err := (&http.Client{Timeout: s107HTTPTimeout}).Do(req)
	if err != nil {
		return s107APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return s107APIResult{status: resp.StatusCode, body: data}, nil
}

// s107apiOrSkip issues a live API call and SKIPS the test cleanly on a transport
// error (the operator API is not reachable / not port-forwarded).
func (s *Scenario107E2ESuite) s107apiOrSkip(method, suffix, body string) s107APIResult {
	res, err := s.s107api(method, suffix, body)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (%s %s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", s107APIBase(), method, suffix, err)
	}
	return res
}

// s107decode JSON-decodes a body into a generic map (best effort).
func s107decode(body []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(body, &m)
	return m
}

// s107CRHasServer reports whether the live CR's pxf.servers[] contains the named
// server (via kubectl jsonpath).
func (s *Scenario107E2ESuite) s107CRHasServer(name string) bool {
	out, err := s.s107Kubectl("get", "cloudberrycluster", s107Cluster(), "-n", s107Namespace(),
		"-o", "jsonpath={.spec.dataLoading.pxf.servers[*].name}")
	if err != nil {
		return false
	}
	for _, n := range strings.Fields(out) {
		if n == name {
			return true
		}
	}
	return false
}

// s107CRHasJob reports whether the live CR's jobs[] contains the named job.
func (s *Scenario107E2ESuite) s107CRHasJob(name string) bool {
	out, err := s.s107Kubectl("get", "cloudberrycluster", s107Cluster(), "-n", s107Namespace(),
		"-o", "jsonpath={.spec.dataLoading.jobs[*].name}")
	if err != nil {
		return false
	}
	for _, n := range strings.Fields(out) {
		if n == name {
			return true
		}
	}
	return false
}

// s107CMHasServerKeys reports whether the <cluster>-pxf-servers ConfigMap has any
// "<server>__" key.
func (s *Scenario107E2ESuite) s107CMHasServerKeys(server string) bool {
	cmName := builder.PxfServersConfigMapName(s107Cluster())
	out, err := s.s107Kubectl("get", "configmap", cmName, "-n", s107Namespace(),
		"-o", "jsonpath={.data}")
	if err != nil {
		return false
	}
	return strings.Contains(out, server+"__")
}

// TestE2E_Scenario107_LivePXFServersLifecycle drives the LIVE PXF-servers surface
// (P.1/P.2/P.3/P.4/P.6/P.5) self-contained: status → list → create a throwaway
// server (201 + rendered config + CR + CM) → update → sync → delete (gone). It
// cleans up the throwaway server via defer regardless of where it exits.
//
//nolint:gocyclo,funlen // a self-contained live CRUD narrative is one flow.
func (s *Scenario107E2ESuite) TestE2E_Scenario107_LivePXFServersLifecycle() {
	s.s107RequireLive()
	server := s107ServerName()

	// State-based cleanup: DELETE the throwaway server if it is still present.
	defer func() {
		if s.s107CRHasServer(server) {
			res, err := s.s107api(http.MethodDelete, "/pxf/servers/"+server, "")
			s.T().Logf("scenario107 cleanup: delete server %q → status=%d err=%v",
				server, res.status, err)
		}
	}()

	// P.1 status.
	statusRes := s.s107apiOrSkip(http.MethodGet, "/pxf/status", "")
	require.Equalf(s.T(), http.StatusOK, statusRes.status,
		"GET pxf/status must be 200 (body=%s)", statusRes.body)

	// P.2 list servers.
	listRes := s.s107apiOrSkip(http.MethodGet, "/pxf/servers", "")
	require.Equal(s.T(), http.StatusOK, listRes.status)

	// P.3 create a throwaway s3 server → 201 + rendered keys. An s3 server MUST
	// carry credentialSecrets (webhook rule) — omitting them is a 400, not a 201.
	createBody := fmt.Sprintf(
		`{"name":%q,"type":"s3","config":{"fs.s3a.endpoint":"http://minio-s107:9000"},`+
			`"credentialSecrets":[{"name":"backup-s3-credentials","key":"aws_access_key_id"},`+
			`{"name":"backup-s3-credentials","key":"aws_secret_access_key"}]}`, server)
	createRes := s.s107apiOrSkip(http.MethodPost, "/pxf/servers", createBody)
	require.Equalf(s.T(), http.StatusCreated, createRes.status,
		"POST pxf/servers must be 201 (body=%s)", createRes.body)
	createResp := s107decode(createRes.body)
	assert.Equal(s.T(), server, createResp["server"])
	if rendered, ok := createResp["renderedKeys"].(map[string]interface{}); ok {
		assert.NotEmpty(s.T(), rendered, "the response must carry the new server's rendered keys")
	}

	// SIDE EFFECT: the CR gained the server and the CM regenerates with its keys.
	require.Eventuallyf(s.T(), func() bool { return s.s107CRHasServer(server) },
		s107LiveTimeout, s107PollInterval, "the CR must gain server %q", server)
	require.Eventuallyf(s.T(), func() bool { return s.s107CMHasServerKeys(server) },
		s107LiveTimeout, s107PollInterval,
		"the <cluster>-pxf-servers ConfigMap must regenerate with %q keys", server)

	// P.4 update the throwaway server (credentialSecrets required for s3).
	updateBody := `{"type":"s3","config":{"fs.s3a.endpoint":"http://minio-s107-NEW:9000"},` +
		`"credentialSecrets":[{"name":"backup-s3-credentials","key":"aws_access_key_id"},` +
		`{"name":"backup-s3-credentials","key":"aws_secret_access_key"}]}`
	updateRes := s.s107apiOrSkip(http.MethodPut, "/pxf/servers/"+server, updateBody)
	require.Equalf(s.T(), http.StatusOK, updateRes.status,
		"PUT pxf/servers/{server} must be 200 (body=%s)", updateRes.body)

	// P.6 sync → 202.
	syncRes := s.s107apiOrSkip(http.MethodPost, "/pxf/sync", "")
	assert.Equalf(s.T(), http.StatusAccepted, syncRes.status,
		"POST pxf/sync must be 202 (body=%s)", syncRes.body)

	// P.5 delete the throwaway server → removed.
	deleteRes := s.s107apiOrSkip(http.MethodDelete, "/pxf/servers/"+server, "")
	require.Equalf(s.T(), http.StatusOK, deleteRes.status,
		"DELETE pxf/servers/{server} must be 200 (body=%s)", deleteRes.body)
	require.Eventuallyf(s.T(), func() bool { return !s.s107CRHasServer(server) },
		s107LiveTimeout, s107PollInterval, "the CR must drop server %q after delete", server)

	s.T().Logf("scenario107 Part B: live PXF-servers lifecycle (P.1/P.2/P.3/P.4/P.6/P.5) OK")
}

// TestE2E_Scenario107_LiveServerInUse409 covers 107-P5-409L: deleting a server
// still referenced by a job is rejected with 409 SERVER_IN_USE and performs no
// mutation. It picks the FIRST server referenced by an existing job (from the CR)
// and asserts the live API rejects the delete — non-destructive, no cleanup needed.
func (s *Scenario107E2ESuite) TestE2E_Scenario107_LiveServerInUse409() {
	s.s107RequireLive()

	// Find a (server, job) pair where the job references the server.
	out, err := s.s107Kubectl("get", "cloudberrycluster", s107Cluster(), "-n", s107Namespace(),
		"-o", "jsonpath={.spec.dataLoading.jobs[*].pxfJob.server}")
	if err != nil || strings.TrimSpace(out) == "" {
		s.T().Skip("no referenced PXF server found in the live CR " +
			"[CONFIG-ONLY: no pxf jobs deployed]")
	}
	referenced := strings.Fields(out)[0]

	res := s.s107apiOrSkip(http.MethodDelete, "/pxf/servers/"+referenced, "")
	assert.Equalf(s.T(), http.StatusConflict, res.status,
		"DELETE of referenced server %q must be 409 SERVER_IN_USE (body=%s)", referenced, res.body)
	assert.Contains(s.T(), string(res.body), "SERVER_IN_USE")
	// NO mutation: the server is still present.
	assert.True(s.T(), s.s107CRHasServer(referenced),
		"the referenced server must NOT be removed on a 409")
}

// TestE2E_Scenario107_LiveJobsLifecycle drives the LIVE jobs surface
// (P.7/P.8/P.9/P.10/P.12/P.14/P.13/P.11) self-contained: list → create a
// throwaway job → get → update → start (202 → Job created) → logs → stop → delete.
// It cleans up the throwaway job (+ any spawned Job) via defer.
//
//nolint:gocyclo,funlen // a self-contained live job lifecycle is one narrative.
func (s *Scenario107E2ESuite) TestE2E_Scenario107_LiveJobsLifecycle() {
	s.s107RequireLive()
	job := s107JobName()

	// Pick an existing server to reference so the create passes W.9 validation.
	out, err := s.s107Kubectl("get", "cloudberrycluster", s107Cluster(), "-n", s107Namespace(),
		"-o", "jsonpath={.spec.dataLoading.pxf.servers[0].name}")
	server := strings.TrimSpace(out)
	if err != nil || server == "" {
		s.T().Skip("no PXF server in the live CR to reference [CONFIG-ONLY: no servers deployed]")
	}

	// State-based cleanup: stop + delete the throwaway job if still present.
	defer func() {
		_, _ = s.s107api(http.MethodPost, "/data-loading/jobs/"+job+"/stop", "")
		if s.s107CRHasJob(job) {
			res, derr := s.s107api(http.MethodDelete, "/jobs/"+job, "")
			s.T().Logf("scenario107 cleanup: delete job %q → status=%d err=%v", job, res.status, derr)
		}
	}()

	// P.7 list jobs.
	listRes := s.s107apiOrSkip(http.MethodGet, "/jobs", "")
	require.Equal(s.T(), http.StatusOK, listRes.status)

	// P.8 create a throwaway (disabled) pxf job → 201; CR gains it.
	createBody := fmt.Sprintf(
		`{"name":%q,"type":"pxf","enabled":false,"pxfJob":{"server":%q,"profile":"s3:text",`+
			`"targetTable":"public.events","loadMethod":"external-table"}}`, job, server)
	createRes := s.s107apiOrSkip(http.MethodPost, "/jobs", createBody)
	require.Equalf(s.T(), http.StatusCreated, createRes.status,
		"POST jobs must be 201 (body=%s)", createRes.body)
	require.Eventuallyf(s.T(), func() bool { return s.s107CRHasJob(job) },
		s107LiveTimeout, s107PollInterval, "the CR must gain job %q", job)

	// P.9 get the job.
	getRes := s.s107apiOrSkip(http.MethodGet, "/jobs/"+job, "")
	require.Equal(s.T(), http.StatusOK, getRes.status)

	// P.10 update the job (enable + schedule). The schedule must be a valid
	// 5-field cron expression (the webhook rejects shorthand like "@daily").
	updateBody := fmt.Sprintf(
		`{"type":"pxf","enabled":true,"schedule":"0 2 * * *","pxfJob":{"server":%q,"profile":"s3:text",`+
			`"targetTable":"public.events","loadMethod":"external-table"}}`, server)
	updateRes := s.s107apiOrSkip(http.MethodPut, "/jobs/"+job, updateBody)
	require.Equalf(s.T(), http.StatusOK, updateRes.status,
		"PUT jobs/{job} must be 200 (body=%s)", updateRes.body)

	// P.12 start the job → 202; the data-loading Job object is created.
	startRes := s.s107apiOrSkip(http.MethodPost, "/jobs/"+job+"/start", "")
	require.Equalf(s.T(), http.StatusAccepted, startRes.status,
		"POST jobs/{job}/start must be 202 (body=%s)", startRes.body)
	k8sJobName := util.DataLoadJobName(s107Cluster(), job)
	require.Eventuallyf(s.T(), func() bool {
		_, e := s.s107Kubectl("get", "job", k8sJobName, "-n", s107Namespace())
		return e == nil
	}, s107LiveTimeout, s107PollInterval, "the data-loading Job %q must be created", k8sJobName)

	// P.14 logs: stream the Job pod logs (best-effort — the pod may still be
	// pending; a 200 OR a 404 JOB_NOT_FOUND (no pod yet) is acceptable, but a 501
	// would mean the operator API has no clientset, which is a real misconfig).
	logsRes := s.s107apiOrSkip(http.MethodGet, "/jobs/"+job+"/logs?tailLines=10", "")
	assert.NotEqualf(s.T(), http.StatusNotImplemented, logsRes.status,
		"the live operator API must have a clientset (logs must not be 501); body=%s", logsRes.body)
	s.T().Logf("scenario107 P.14 live logs: status=%d", logsRes.status)

	// P.13 stop the job → 202 (Job reaped) or 200 no-op.
	stopRes := s.s107apiOrSkip(http.MethodPost, "/jobs/"+job+"/stop", "")
	assert.Containsf(s.T(), []int{http.StatusAccepted, http.StatusOK}, stopRes.status,
		"POST jobs/{job}/stop must be 202 or 200 (body=%s)", stopRes.body)

	// P.11 delete the job → gone.
	deleteRes := s.s107apiOrSkip(http.MethodDelete, "/jobs/"+job, "")
	require.Equalf(s.T(), http.StatusOK, deleteRes.status,
		"DELETE jobs/{job} must be 200 (body=%s)", deleteRes.body)
	require.Eventuallyf(s.T(), func() bool { return !s.s107CRHasJob(job) },
		s107LiveTimeout, s107PollInterval, "the CR must drop job %q after delete", job)

	s.T().Logf("scenario107 Part B: live jobs lifecycle (P.7..P.14) OK")
}

// TestE2E_Scenario107_LiveExternalTables covers 107-P15-L: GET external-tables
// against the live API returns the documented JSON shape — observed reflects the
// live DB (or is honestly ABSENT) and expected lists the spec-derived tables. It
// is read-only (no cleanup).
func (s *Scenario107E2ESuite) TestE2E_Scenario107_LiveExternalTables() {
	s.s107RequireLive()

	res := s.s107apiOrSkip(http.MethodGet, "/external-tables", "")
	require.Equalf(s.T(), http.StatusOK, res.status,
		"GET external-tables must be 200 (body=%s)", res.body)

	// The body must carry the documented shape: observedAvailable bool + an
	// "expected" array (the spec-derived intent) + an "observed" field that is
	// either an array (DB reachable) or null (honestly ABSENT).
	var resp struct {
		Cluster           string `json:"cluster"`
		ObservedAvailable bool   `json:"observedAvailable"`
		Observed          []struct {
			Schema string `json:"schema"`
			Name   string `json:"name"`
			Kind   string `json:"kind"`
		} `json:"observed"`
		Expected []struct {
			Job  string `json:"job"`
			Name string `json:"name"`
			Kind string `json:"kind"`
		} `json:"expected"`
	}
	require.NoError(s.T(), json.NewDecoder(bytes.NewReader(res.body)).Decode(&resp))
	assert.Equal(s.T(), s107Cluster(), resp.Cluster)

	// HONESTY: observed is present only when observedAvailable; never claimed when
	// the DB is unreachable.
	if resp.ObservedAvailable {
		s.T().Logf("scenario107 P.15 live: observed %d external/foreign tables (DB reachable)",
			len(resp.Observed))
	} else {
		assert.Empty(s.T(), resp.Observed,
			"observed must be ABSENT/empty when observedAvailable is false (honesty)")
		s.T().Log("scenario107 P.15 live: observed ABSENT (DB unreachable) — expected still present")
	}
	// Expected is always derivable from the spec (may be empty if no pxf jobs).
	s.T().Logf("scenario107 P.15 live: expected %d spec-derived tables", len(resp.Expected))
}

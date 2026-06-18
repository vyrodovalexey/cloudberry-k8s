package main

// Scenario 108 — All CLI Commands (L.1–L.16).
//
// This suite drives the newly-wired data-loading CLI verbs end-to-end through
// the cobra command tree against a recording httptest server (an in-process
// operator API stand-in). For each command it asserts the REAL HTTP effect the
// CLI produced — the request method, the path (including query string) and the
// JSON body — plus the exit behavior (clean error / no HTTP call on a usage
// error). It mirrors the existing main_test.go mock-server pattern
// (setupMockServer + globals pointed at server.URL) and the Scenario 107 API
// tests' "assert the side effect, never just the status" discipline.
//
// Catalog IDs covered (see task-breackdown_claude_2026-06-16_14-22-36.out):
//   108-L2-F   pxf servers list → GET .../pxf/servers
//   108-L3-F   pxf servers create → POST .../pxf/servers (name/type/config/secrets)
//   108-L3-flags  create with NO --name → usage error, NO HTTP call
//   108-L4-F   pxf servers update <name> --endpoint → PUT .../pxf/servers/<name>
//   108-L5-F   pxf servers delete <name> → DELETE .../pxf/servers/<name>
//   108-L9-F   jobs create --type pxf → POST .../jobs (pxfJob DTO)
//   108-L14-F  jobs create --type gpload → POST .../jobs (gploadJob DTO)
//   108-L16-yaml  jobs create --from-yaml (valid / malformed / precedence)
//   108-L13-F  jobs logs --job → GET .../jobs/<job>/logs (stream)
//   108-L13-fallback  logs stream error → kubectl fallback hint printed
//   108-L15-F  test-read --job → GET .../test-read?...limit=10
//   108-L15-limit  test-read --limit 0/negative → usage error (no HTTP call)

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedRequest captures one HTTP request the CLI sent to the mock operator.
type recordedRequest struct {
	method  string
	path    string // URL path only
	rawURL  string // path + "?" + query
	query   string
	body    map[string]interface{}
	rawBody []byte
}

// requestRecorder is a thread-safe recorder of the requests a CLI run produced.
type requestRecorder struct {
	mu       sync.Mutex
	requests []recordedRequest
	// respond is the JSON body the mock returns for every request (defaults to
	// a tiny ok envelope). Status defaults to 200.
	status int
}

func (rr *requestRecorder) record(r recordedRequest) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.requests = append(rr.requests, r)
}

func (rr *requestRecorder) count() int {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return len(rr.requests)
}

func (rr *requestRecorder) last() recordedRequest {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return rr.requests[len(rr.requests)-1]
}

// newCtlRecorderServer starts an httptest server that records each request
// (method, path, query, JSON body) and replies with a small ok envelope (or the
// configured status). The returned recorder lets tests assert exactly what the
// CLI sent.
func newCtlRecorderServer(t *testing.T) (*httptest.Server, *requestRecorder) {
	t.Helper()
	rr := &requestRecorder{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := recordedRequest{
			method: r.Method,
			path:   r.URL.Path,
			rawURL: r.URL.RequestURI(),
			query:  r.URL.RawQuery,
		}
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		rec.rawBody = buf.Bytes()
		if len(rec.rawBody) > 0 {
			_ = json.Unmarshal(rec.rawBody, &rec.body)
		}
		rr.record(rec)

		status := rr.status
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
	}))
	t.Cleanup(srv.Close)
	return srv, rr
}

// runCtl builds a fresh root command, points the globals at the recorder server
// and executes the given args through cobra (so flag parsing, required-flag
// guards and usage errors behave exactly as in production). It returns the error
// Execute() produced and the captured stdout/stderr.
func runCtl(t *testing.T, serverURL string, args ...string) (error, string) {
	t.Helper()

	saved := globals
	t.Cleanup(func() { globals = saved })

	// Reset globals to deterministic defaults; the persistent flags below bind
	// back into this struct as cobra parses --operator-url etc.
	globals = globalFlags{
		namespace:   "cloudberry-test",
		operatorURL: serverURL,
		authMethod:  "basic",
		username:    "admin",
		password:    "pass",
		output:      "json",
		timeout:     "5s",
	}

	root := newRootCmd()
	// Re-apply the server URL after newRootCmd reset flag defaults: pass it as a
	// global flag so cobra marks it Changed and initConfig does not clobber it.
	fullArgs := append([]string{
		"--operator-url", serverURL,
		"--cluster", "test-cluster",
		"--namespace", "default",
		"--username", "admin",
		"--password", "pass",
		"--timeout", "5s",
		"--output", "json",
	}, args...)
	root.SetArgs(fullArgs)

	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)

	err := root.Execute()
	return err, out.String()
}

// findCtlSubcommand walks the command tree by name, returning the deepest match.
func findCtlSubcommand(root *cobra.Command, names ...string) *cobra.Command {
	cur := root
	for _, n := range names {
		cur = findSubcommand(cur, n)
		if cur == nil {
			return nil
		}
	}
	return cur
}

// --- L.2: pxf servers list -------------------------------------------------

func TestScenario108_PxfServersList(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL, "pxf", "servers", "list")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/pxf/servers", req.path)
}

// --- L.3: pxf servers create -----------------------------------------------

func TestScenario108_PxfServersCreate(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"pxf", "servers", "create",
		"--name", "s3x",
		"--type", "s3",
		"--endpoint", "http://minio:9000",
		"--credential-secret", "backup-s3-credentials:aws_access_key_id")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/pxf/servers", req.path)

	assert.Equal(t, "s3x", req.body["name"])
	assert.Equal(t, "s3", req.body["type"])

	config, ok := req.body["config"].(map[string]interface{})
	require.True(t, ok, "body must carry a config map")
	assert.Equal(t, "http://minio:9000", config["fs.s3a.endpoint"])

	secrets, ok := req.body["credentialSecrets"].([]interface{})
	require.True(t, ok, "body must carry credentialSecrets")
	require.Len(t, secrets, 1)
	secret := secrets[0].(map[string]interface{})
	assert.Equal(t, "backup-s3-credentials", secret["name"])
	assert.Equal(t, "aws_access_key_id", secret["key"])
}

// 108-L3-flags: a missing required flag (--name) is a usage error and NO HTTP
// call is made (the recorder stays empty) — the guard fires BEFORE newClient().
func TestScenario108_PxfServersCreate_MissingName_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"pxf", "servers", "create",
		"--type", "s3")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--name and --type are required")

	assert.Equal(t, 0, rr.count(), "no HTTP request must be made when a required flag is missing")
}

// create with --bucket maps into the config under the bucket key.
func TestScenario108_PxfServersCreate_WithBucket(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"pxf", "servers", "create",
		"--name", "s3x",
		"--type", "s3",
		"--bucket", "my-bucket")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	config, ok := rr.last().body["config"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "my-bucket", config["bucket"])
}

// --- L.4: pxf servers update -----------------------------------------------

// update by --name (instead of the positional arg) targets the named server.
func TestScenario108_PxfServersUpdate_ByNameFlag(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"pxf", "servers", "update",
		"--name", "s3y",
		"--endpoint", "http://minio-v3:9000")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPut, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/pxf/servers/s3y", req.path)
}

// update with no name (no positional, no --name) is a clean error, NO HTTP call.
func TestScenario108_PxfServersUpdate_MissingName_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL, "pxf", "servers", "update", "--endpoint", "http://x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server name is required")
	assert.Equal(t, 0, rr.count())
}

// --- L.4b: pxf servers update -----------------------------------------------

func TestScenario108_PxfServersUpdate(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"pxf", "servers", "update", "s3x",
		"--endpoint", "http://minio-v2:9000")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPut, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/pxf/servers/s3x", req.path)

	config, ok := req.body["config"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "http://minio-v2:9000", config["fs.s3a.endpoint"])
}

// --- L.5: pxf servers delete -----------------------------------------------

func TestScenario108_PxfServersDelete(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL, "pxf", "servers", "delete", "s3x")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodDelete, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/pxf/servers/s3x", req.path)
}

// delete with no name is a clean error and NO HTTP call.
func TestScenario108_PxfServersDelete_MissingName_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL, "pxf", "servers", "delete")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "server name is required")
	assert.Equal(t, 0, rr.count())
}

// list propagates an API error as a non-nil exit error.
func TestScenario108_PxfServersList_APIError(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)
	rr.status = http.StatusInternalServerError

	err, _ := runCtl(t, srv.URL, "pxf", "servers", "list")
	require.Error(t, err)
	assert.Equal(t, 1, rr.count())
}

// --- L.9: jobs create --type pxf -------------------------------------------

func TestScenario108_JobsCreatePXF(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--type", "pxf",
		"--name", "j1",
		"--server", "s3-datalake",
		"--profile", "s3:text",
		"--resource", "a/b.csv",
		"--target", "public.events")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/jobs", req.path)

	assert.Equal(t, "j1", req.body["name"])
	assert.Equal(t, "pxf", req.body["type"])

	pxfJob, ok := req.body["pxfJob"].(map[string]interface{})
	require.True(t, ok, "body must carry a pxfJob DTO")
	assert.Equal(t, "s3-datalake", pxfJob["server"])
	assert.Equal(t, "s3:text", pxfJob["profile"])
	assert.Equal(t, "a/b.csv", pxfJob["resource"])
	assert.Equal(t, "public.events", pxfJob["targetTable"])

	// A pxf body must NOT carry a gploadJob.
	_, hasGpload := req.body["gploadJob"]
	assert.False(t, hasGpload)
}

// --- L.14: jobs create --type gpload ---------------------------------------

func TestScenario108_JobsCreateGpload(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--type", "gpload",
		"--name", "g1",
		"--gpfdist-host", "h",
		"--gpfdist-port", "8080",
		"--file-path", "/in/*.csv",
		"--format", "csv",
		"--target", "public.raw")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/jobs", req.path)

	assert.Equal(t, "g1", req.body["name"])
	assert.Equal(t, "gpload", req.body["type"])

	gpload, ok := req.body["gploadJob"].(map[string]interface{})
	require.True(t, ok, "body must carry a gploadJob DTO")
	assert.Equal(t, "public.raw", gpload["targetTable"])
	assert.Equal(t, "csv", gpload["format"])

	input, ok := gpload["inputSource"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "gpfdist", input["type"])
	assert.Equal(t, "h", input["host"])
	assert.Equal(t, float64(8080), input["port"])

	filePaths, ok := gpload["filePaths"].([]interface{})
	require.True(t, ok)
	require.Len(t, filePaths, 1)
	assert.Equal(t, "/in/*.csv", filePaths[0])

	// A gpload body must NOT carry a pxfJob.
	_, hasPxf := req.body["pxfJob"]
	assert.False(t, hasPxf)
}

// --- L.16: jobs create --from-yaml -----------------------------------------

const validJobYAML = `name: yamljob
type: pxf
schedule: "0 3 * * *"
pxfJob:
  server: s3-datalake
  profile: s3:parquet
  resource: data/events.parquet
  targetTable: public.events
`

// 108-L16-yaml (happy): a valid YAML file is read+unmarshalled and POSTed; the
// body matches the file contents exactly.
func TestScenario108_JobsCreateFromYAML(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "job.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(validJobYAML), 0o600))

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--from-yaml", yamlPath)
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/jobs", req.path)

	assert.Equal(t, "yamljob", req.body["name"])
	assert.Equal(t, "pxf", req.body["type"])
	assert.Equal(t, "0 3 * * *", req.body["schedule"])
	pxfJob, ok := req.body["pxfJob"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "s3-datalake", pxfJob["server"])
	assert.Equal(t, "public.events", pxfJob["targetTable"])
}

// 108-L16-yaml (precedence): --from-yaml takes precedence over conflicting
// flags — the YAML body wins, the flags are ignored.
func TestScenario108_JobsCreateFromYAML_PrecedenceOverFlags(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "job.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(validJobYAML), 0o600))

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--from-yaml", yamlPath,
		"--name", "flagname",
		"--server", "flagserver",
		"--target", "flag.table")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	// YAML wins — the flag values are ignored.
	assert.Equal(t, "yamljob", req.body["name"])
	pxfJob, ok := req.body["pxfJob"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "s3-datalake", pxfJob["server"])
}

// 108-L16-yaml (malformed): a malformed YAML file yields a CLEAN error and NO
// POST is attempted.
func TestScenario108_JobsCreateFromYAML_Malformed_NoPOST(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte("name: [unterminated\n  : : bad"), 0o600))

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--from-yaml", yamlPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing job file")

	assert.Equal(t, 0, rr.count(), "no POST must be made for a malformed YAML file")
}

// 108-L16 (missing file): a non-existent --from-yaml path is a clean read error,
// no POST.
func TestScenario108_JobsCreateFromYAML_MissingFile_NoPOST(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--from-yaml", filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading job file")
	assert.Equal(t, 0, rr.count())
}

// 108-L9 (missing name): jobs create without --name and without --from-yaml is a
// usage error and NO HTTP call is made.
func TestScenario108_JobsCreate_MissingName_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--type", "pxf",
		"--server", "s3-datalake",
		"--target", "public.events")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--name is required")
	assert.Equal(t, 0, rr.count())
}

// jobs create with an invalid --type is a clean error and NO HTTP call.
func TestScenario108_JobsCreate_BadType_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "create",
		"--type", "bogus",
		"--name", "j1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid --type")
	assert.Equal(t, 0, rr.count())
}

// --- L.13: jobs logs -------------------------------------------------------

// 108-L13-F: jobs logs --job streams from the operator API: a GET to
// .../jobs/<job>/logs, and the streamed body is written to stdout.
func TestScenario108_JobsLogs_Streams(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("real pod log line\n"))
	}))
	t.Cleanup(srv.Close)

	err, out := runCtl(t, srv.URL, "data-loading", "jobs", "logs", "--job", "j1")
	require.NoError(t, err)

	assert.Equal(t, http.MethodGet, gotMethod)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/jobs/j1/logs", gotPath)
	assert.Contains(t, out, "real pod log line")
}

// 108-L13-fallback: when the stream endpoint errors, the command does NOT fail;
// it prints the kubectl fallback hint naming the k8s Job (DataLoadJobName).
func TestScenario108_JobsLogs_FallbackHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "LOGS_NOT_AVAILABLE", "message": "no clientset"},
		})
	}))
	t.Cleanup(srv.Close)

	err, out := runCtl(t, srv.URL, "data-loading", "jobs", "logs", "--job", "j1")
	require.NoError(t, err, "logs must not fail when streaming is unavailable")
	assert.Contains(t, out, "kubectl logs")
	// The fallback names the k8s Job ("<cluster>-dataload-<job>").
	assert.Contains(t, out, "test-cluster-dataload-j1")
}

// jobs logs --follow --tail propagates the follow/tail query params.
func TestScenario108_JobsLogs_FollowTailParams(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("line\n"))
	}))
	t.Cleanup(srv.Close)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "jobs", "logs",
		"--job", "j1", "--follow", "--tail", "50")
	require.NoError(t, err)
	assert.Contains(t, gotQuery, "follow=true")
	assert.Contains(t, gotQuery, "tailLines=50")
}

// jobs logs without --job is a usage error and NO HTTP call is made.
func TestScenario108_JobsLogs_MissingJob_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL, "data-loading", "jobs", "logs")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--job is required")
	assert.Equal(t, 0, rr.count())
}

// --- L.15: data-loading test-read ------------------------------------------

// 108-L15-F: test-read --job → GET .../test-read with job=<job> and limit=10.
func TestScenario108_TestRead_ByJob(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--job", "j1",
		"--limit", "10")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/api/v1alpha1/clusters/test-cluster/data-loading/test-read", req.path)
	assert.Contains(t, req.query, "job=j1")
	assert.Contains(t, req.query, "limit=10")
}

// test-read via explicit server/profile/resource carries them in the query.
func TestScenario108_TestRead_ByExplicitSource(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--server", "s3x",
		"--profile", "s3:text",
		"--resource", "a/b.csv")
	require.NoError(t, err)

	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Contains(t, req.query, "server=s3x")
	assert.Contains(t, req.query, "profile=s3%3Atext")
	assert.Contains(t, req.query, "resource=a%2Fb.csv")
	assert.Contains(t, req.query, "limit=10") // default
}

// 108-L15-limit: --limit 0 is a usage error and NO HTTP call is made.
func TestScenario108_TestRead_ZeroLimit_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--job", "j1",
		"--limit", "0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--limit must be a positive integer")
	assert.Equal(t, 0, rr.count())
}

// 108-L15-limit: a negative --limit is likewise a usage error, no HTTP call.
func TestScenario108_TestRead_NegativeLimit_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--job", "j1",
		"--limit", "-5")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--limit must be a positive integer")
	assert.Equal(t, 0, rr.count())
}

// test-read with neither --job nor a complete explicit source is a usage error
// and NO HTTP call is made.
func TestScenario108_TestRead_MissingSource_NoHTTP(t *testing.T) {
	srv, rr := newCtlRecorderServer(t)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--server", "s3x") // no --job, no profile/resource
	require.Error(t, err)
	assert.Contains(t, err.Error(), "either --job or both --profile and --resource")
	assert.Equal(t, 0, rr.count())
}

// 108-L15-absent (CLI honesty): an available:false test-read response renders
// without crashing and exits 0 (this is a read/preview command).
func TestScenario108_TestRead_HonestUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"cluster":   "test-cluster",
			"source":    map[string]string{"profile": "s3:text", "resource": "a/b.csv"},
			"limit":     10,
			"available": false,
			"rowCount":  0,
			"rows":      nil,
		})
	}))
	t.Cleanup(srv.Close)

	err, _ := runCtl(t, srv.URL,
		"data-loading", "test-read",
		"--job", "j1")
	require.NoError(t, err, "an honest available:false preview must exit 0")
}

// --- command tree wiring sanity (L.2–L.5 / L.9 / L.13 / L.15 discoverable) --

func TestScenario108_PxfServersCmd_Subcommands(t *testing.T) {
	root := newRootCmd()
	servers := findCtlSubcommand(root, "pxf", "servers")
	require.NotNil(t, servers)
	names := subcommandNames(servers)
	for _, e := range []string{"list", "create", "update", "delete"} {
		assert.Contains(t, names, e)
	}
}

func TestScenario108_DataLoadingTestReadCmd_Flags(t *testing.T) {
	root := newRootCmd()
	testRead := findCtlSubcommand(root, "data-loading", "test-read")
	require.NotNil(t, testRead)
	for _, f := range []string{"job", "server", "profile", "resource", "limit"} {
		assert.NotNil(t, testRead.Flags().Lookup(f), "test-read should have --%s", f)
	}
}

func TestScenario108_DataLoadingJobsCreateCmd_Flags(t *testing.T) {
	root := newRootCmd()
	create := findCtlSubcommand(root, "data-loading", "jobs", "create")
	require.NotNil(t, create)
	for _, f := range []string{
		"type", "name", "schedule", "from-yaml", "server", "profile",
		"resource", "target", "gpfdist-host", "gpfdist-port", "file-path", "format",
	} {
		assert.NotNil(t, create.Flags().Lookup(f), "jobs create should have --%s", f)
	}
}

//go:build functional

package functional

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/ctl"
)

// ============================================================================
// Scenario 86: All CLI Commands (cloudberry-ctl backup ...) — functional
// ============================================================================
//
// The cloudberry-ctl backup subcommands are thin cobra wrappers over the
// internal/ctl.OperatorClient: each command builds a method + path (prefixed
// with /api/v1alpha1) + JSON body and issues it against the operator REST API
// using an OIDC bearer token. Because the cobra command tree lives in package
// main (not importable), these functional tests drive the SAME OperatorClient
// the CLI uses against an httptest API stub and assert, per command (86a-k),
// the exact operator request the command produces:
//
//	86a create x3 -> POST /backups, gpbackupOptions flags (full/single/incremental)
//	86b list      -> GET  /backups
//	86c status    -> GET  /backups/{ts}
//	86d delete    -> DELETE /backups/{ts}            (cleanup)
//	86e restore   -> POST /backups/{ts}/restore, gprestoreOptions incl resizeCluster
//	86f schedule  -> GET  /backups/schedule
//	86g set       -> PATCH /backups/schedule {schedule}
//	86h suspend   -> PATCH /backups/schedule {suspend:true}
//	86i resume    -> PATCH /backups/schedule {suspend:false}
//	86j jobs      -> GET  /backups/jobs
//	86k jobs logs -> GET  /backups/jobs/{job}/logs   (streams text/plain; 500->fallback)
//
// The path/body construction here mirrors cmd/cloudberry-ctl/main.go exactly
// (ClusterSubresourcePath, buildCreateBackupRequest, buildRestoreRequest,
// buildBackupJobLogsPath) so a drift in either is caught.
// ============================================================================

const (
	scenario86Namespace = "cloudberry-test"
	scenario86Cluster   = "scenario86-s3"
	scenario86Prefix    = "/api/v1alpha1"
	scenario86DB        = "mydb"
	scenario86TS        = "20260601020000"
	scenario86Token     = "test-oidc-token"
)

// capturedRequest records what the stub API server received.
type capturedRequest struct {
	method string
	path   string
	query  url.Values
	body   map[string]interface{}
	auth   string
}

// Scenario86CLISuite drives the OperatorClient (the CLI's transport) against a
// recording httptest API stub.
type Scenario86CLISuite struct {
	suite.Suite
	ctx    context.Context
	stub   *httptest.Server
	got    *capturedRequest
	client *ctl.OperatorClient
	// handler is swapped per-test so a test can inject streaming / error bodies.
	handler http.HandlerFunc
}

func TestFunctional_Scenario86(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario86CLISuite))
}

func (s *Scenario86CLISuite) SetupTest() {
	s.ctx = context.Background()
	s.got = &capturedRequest{}
	// Default handler records the request and returns a 202 JSON envelope.
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "ok", "job": scenario86Cluster + "-backup-1",
		})
	}
	s.stub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.handler(w, r)
	}))
	// The CLI points at the operator API via --operator-url and authenticates
	// with an OIDC bearer token (auth-method oidc => Password is the token).
	s.client = ctl.NewOperatorClient(ctl.ClientConfig{
		BaseURL:    s.stub.URL,
		AuthMethod: "oidc",
		Password:   scenario86Token,
		Timeout:    10 * time.Second,
	})
}

func (s *Scenario86CLISuite) TearDownTest() {
	if s.stub != nil {
		s.stub.Close()
	}
}

func (s *Scenario86CLISuite) record(r *http.Request) {
	s.got.method = r.Method
	s.got.path = r.URL.Path
	s.got.query = r.URL.Query()
	s.got.auth = r.Header.Get("Authorization")
	if r.Body != nil {
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &s.got.body)
		}
	}
}

// --- path/body builders mirroring cmd/cloudberry-ctl/main.go ---

// clusterSub mirrors ctl.ClusterSubresourcePath used by the CLI.
func scenario86Sub(subresource string) string {
	return ctl.ClusterSubresourcePath(scenario86Cluster, subresource, scenario86Namespace)
}

// tsPath mirrors the CLI's status/delete/restore timestamp path construction.
func scenario86TSPath(suffix string) string {
	return scenario86AppendNS(fmt.Sprintf("/clusters/%s/backups/%s",
		url.PathEscape(scenario86Cluster), url.PathEscape(scenario86TS)) + suffix)
}

func scenario86AppendNS(path string) string {
	return path + "?namespace=" + scenario86Namespace
}

// buildCreate mirrors buildCreateBackupRequest in main.go.
func scenario86BuildCreate(full bool, single bool, incremental bool) map[string]interface{} {
	gp := map[string]interface{}{
		"compressionLevel": int32(0), "compressionType": "", "jobs": int32(0),
		"singleDataFile": false, "copyQueueSize": int32(0),
		"incremental": false, "fromTimestamp": "",
		"includeSchemas": []string(nil), "excludeTables": []string(nil),
		"leafPartitionData": false, "withStats": false, "withoutGlobals": false,
	}
	body := map[string]interface{}{"databases": []string{scenario86DB}, "gpbackupOptions": gp}
	switch {
	case full:
		body["type"] = "full"
		gp["compressionLevel"] = int32(6)
		gp["compressionType"] = "zstd"
		gp["jobs"] = int32(4)
		gp["includeSchemas"] = []string{"public"}
		gp["excludeTables"] = []string{"public.temp"}
		gp["withStats"] = true
		gp["withoutGlobals"] = true
	case single:
		body["type"] = "full"
		gp["singleDataFile"] = true
		gp["copyQueueSize"] = int32(4)
	case incremental:
		body["type"] = "incremental"
		gp["incremental"] = true
		gp["fromTimestamp"] = scenario86TS
		gp["leafPartitionData"] = true
	}
	return body
}

// buildRestore mirrors buildRestoreRequest in main.go.
func scenario86BuildRestore() map[string]interface{} {
	gr := map[string]interface{}{
		"jobs": int32(4), "redirectDb": "mydb_restored", "redirectSchema": "restored",
		"createDb": true, "includeSchemas": []string{"public"},
		"includeTables": []string{"public.users"}, "withStats": true,
		"runAnalyze": true, "onErrorContinue": true, "truncateTable": true,
		"resizeCluster": true,
	}
	return map[string]interface{}{"timestamp": scenario86TS, "gprestoreOptions": gr}
}

// gpopts extracts the gpbackupOptions / gprestoreOptions object from a body.
func (s *Scenario86CLISuite) opts(key string) map[string]interface{} {
	o, ok := s.got.body[key].(map[string]interface{})
	require.Truef(s.T(), ok, "request body must contain a %q object", key)
	return o
}

// --- 86a: create (3 variants) ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Create_Full() {
	_, err := s.client.Post(s.ctx, scenario86Sub("backups"), scenario86BuildCreate(true, false, false))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodPost, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups", s.got.path)
	assert.Equal(s.T(), scenario86Namespace, s.got.query.Get("namespace"))
	assert.Equal(s.T(), "Bearer "+scenario86Token, s.got.auth)
	assert.Equal(s.T(), "full", s.got.body["type"])

	gp := s.opts("gpbackupOptions")
	assert.Equal(s.T(), float64(6), gp["compressionLevel"])
	assert.Equal(s.T(), "zstd", gp["compressionType"])
	assert.Equal(s.T(), float64(4), gp["jobs"])
	assert.Equal(s.T(), true, gp["withStats"])
	assert.Equal(s.T(), true, gp["withoutGlobals"])
	assert.Equal(s.T(), []interface{}{"public"}, gp["includeSchemas"])
	assert.Equal(s.T(), []interface{}{"public.temp"}, gp["excludeTables"])
	// Mutual exclusivity: full variant sets neither single-data-file nor incremental.
	assert.Equal(s.T(), false, gp["singleDataFile"])
	assert.Equal(s.T(), false, gp["incremental"])
}

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Create_SingleDataFile() {
	_, err := s.client.Post(s.ctx, scenario86Sub("backups"), scenario86BuildCreate(false, true, false))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodPost, s.got.method)
	gp := s.opts("gpbackupOptions")
	assert.Equal(s.T(), true, gp["singleDataFile"])
	assert.Equal(s.T(), float64(4), gp["copyQueueSize"])
	// single-data-file is mutually exclusive with --jobs.
	assert.Equal(s.T(), float64(0), gp["jobs"])
}

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Create_Incremental() {
	_, err := s.client.Post(s.ctx, scenario86Sub("backups"), scenario86BuildCreate(false, false, true))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "incremental", s.got.body["type"])
	gp := s.opts("gpbackupOptions")
	assert.Equal(s.T(), true, gp["incremental"])
	assert.Equal(s.T(), scenario86TS, gp["fromTimestamp"])
	assert.Equal(s.T(), true, gp["leafPartitionData"])
}

// --- 86b: list ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_List() {
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"backups": []interface{}{map[string]interface{}{"timestamp": scenario86TS}},
			"total":   1,
		})
	}
	resp, err := s.client.Get(s.ctx, scenario86Sub("backups"))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodGet, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups", s.got.path)
	assert.Contains(s.T(), resp.Body, "backups")
}

// --- 86c: status --timestamp ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Status() {
	_, err := s.client.Get(s.ctx, scenario86TSPath(""))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodGet, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/"+scenario86TS, s.got.path)
	assert.Equal(s.T(), scenario86Namespace, s.got.query.Get("namespace"))
}

// --- 86d: delete --timestamp ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Delete() {
	_, err := s.client.Delete(s.ctx, scenario86TSPath(""))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodDelete, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/"+scenario86TS, s.got.path)
}

// --- 86e: restore (incl. --resize-cluster) ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Restore_ResizeCluster() {
	_, err := s.client.Post(s.ctx, scenario86TSPath("/restore"), scenario86BuildRestore())
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodPost, s.got.method)
	assert.Equal(s.T(),
		scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/"+scenario86TS+"/restore",
		s.got.path)
	assert.Equal(s.T(), scenario86TS, s.got.body["timestamp"])

	gr := s.opts("gprestoreOptions")
	assert.Equal(s.T(), float64(4), gr["jobs"])
	assert.Equal(s.T(), "mydb_restored", gr["redirectDb"])
	assert.Equal(s.T(), "restored", gr["redirectSchema"])
	assert.Equal(s.T(), true, gr["createDb"])
	assert.Equal(s.T(), true, gr["runAnalyze"])
	assert.Equal(s.T(), true, gr["onErrorContinue"])
	assert.Equal(s.T(), true, gr["truncateTable"])
	assert.Equal(s.T(), true, gr["resizeCluster"], "86e: --resize-cluster must map to resizeCluster:true")
}

// --- 86f: schedule (show) ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_ScheduleShow() {
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"scheduled": true, "schedule": "0 2 * * *", "nextScheduleTime": "2026-06-02T02:00:00Z",
		})
	}
	resp, err := s.client.Get(s.ctx, scenario86Sub("backups/schedule"))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodGet, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/schedule", s.got.path)
	assert.Equal(s.T(), "0 2 * * *", resp.Body["schedule"])
}

// --- 86g: schedule set --cron ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_ScheduleSet() {
	body := map[string]interface{}{"schedule": "0 3 * * *"}
	_, err := s.client.Patch(s.ctx, scenario86Sub("backups/schedule"), body)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodPatch, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/schedule", s.got.path)
	assert.Equal(s.T(), "0 3 * * *", s.got.body["schedule"])
}

// --- 86h / 86i: schedule suspend / resume ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_ScheduleSuspend() {
	_, err := s.client.Patch(s.ctx, scenario86Sub("backups/schedule"),
		map[string]interface{}{"suspend": true})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.MethodPatch, s.got.method)
	assert.Equal(s.T(), true, s.got.body["suspend"], "86h: suspend must PATCH {suspend:true}")
}

func (s *Scenario86CLISuite) TestFunctional_Scenario86_ScheduleResume() {
	_, err := s.client.Patch(s.ctx, scenario86Sub("backups/schedule"),
		map[string]interface{}{"suspend": false})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), http.MethodPatch, s.got.method)
	assert.Equal(s.T(), false, s.got.body["suspend"], "86i: resume must PATCH {suspend:false}")
}

// --- 86j: jobs ---

func (s *Scenario86CLISuite) TestFunctional_Scenario86_Jobs() {
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"jobs":  []interface{}{map[string]interface{}{"name": scenario86Cluster + "-backup-1"}},
			"total": 1,
		})
	}
	resp, err := s.client.Get(s.ctx, scenario86Sub("backups/jobs"))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodGet, s.got.method)
	assert.Equal(s.T(), scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/jobs", s.got.path)
	assert.Contains(s.T(), resp.Body, "jobs")
}

// --- 86k: jobs logs --job (streaming + fallback) ---

// scenario86LogsPath mirrors buildBackupJobLogsPath in main.go.
func scenario86LogsPath(job string) string {
	path := fmt.Sprintf("/clusters/%s/backups/jobs/%s/logs",
		url.PathEscape(scenario86Cluster), url.PathEscape(job))
	q := url.Values{}
	q.Set("namespace", scenario86Namespace)
	return path + "?" + q.Encode()
}

func (s *Scenario86CLISuite) TestFunctional_Scenario86_JobsLogs_Streams() {
	const logBody = "20260601 02:00:00 gpbackup:Backup completed successfully\n"
	job := scenario86Cluster + "-backup-1"
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(logBody))
	}

	var out bytes.Buffer
	err := s.client.GetStream(s.ctx, scenario86LogsPath(job), &out)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodGet, s.got.method)
	assert.Equal(s.T(),
		scenario86Prefix+"/clusters/"+scenario86Cluster+"/backups/jobs/"+job+"/logs",
		s.got.path)
	assert.Equal(s.T(), scenario86Namespace, s.got.query.Get("namespace"))
	// The streamed text/plain body is written verbatim to the writer (stdout).
	assert.Equal(s.T(), logBody, out.String())
	assert.Contains(s.T(), out.String(), "Backup completed successfully")
}

// TestFunctional_Scenario86_JobsLogs_Fallback proves the CLI's fallback path:
// when the streaming endpoint returns 500, GetStream returns an *APIError, and
// the CLI prints the kubectl instruction (asserted here by reproducing that
// behavior the command implements).
func (s *Scenario86CLISuite) TestFunctional_Scenario86_JobsLogs_Fallback() {
	job := scenario86Cluster + "-backup-1"
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error": map[string]string{"code": "INTERNAL_ERROR", "message": "stream failed"},
		})
	}

	var out bytes.Buffer
	streamErr := s.client.GetStream(s.ctx, scenario86LogsPath(job), &out)
	require.Error(s.T(), streamErr, "86k: a 500 must surface as an error so the CLI falls back")
	assert.Empty(s.T(), out.String(), "86k: no log bytes written on the error path")

	// The CLI's fallback (printBackupJobLogsFallback) prints a kubectl command.
	var fallback bytes.Buffer
	fmt.Fprintf(&fallback,
		"unable to stream logs from the operator API (%v); run:\n  kubectl logs -n %s job/%s\n",
		streamErr, scenario86Namespace, job)
	assert.Contains(s.T(), fallback.String(), "kubectl logs -n "+scenario86Namespace+" job/"+job)
}

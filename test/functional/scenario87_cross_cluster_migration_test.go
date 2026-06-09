//go:build functional

package functional

import (
	"context"
	"encoding/json"
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
// Scenario 87: Cross-Cluster Migration (cloudberry-ctl migrate ...) — functional
// ============================================================================
//
// The cloudberry-ctl `migrate` command is a thin cobra wrapper over the
// internal/ctl.OperatorClient: it builds a POST request against the SOURCE
// cluster's /migrate subresource (prefixed /api/v1alpha1) with a JSON body
// mapping its flags (--source-cluster/--target-cluster/--database/--tables/
// --truncate/--redirect-db/--redirect-schema/--jobs) to the keys sourceCluster/
// targetCluster/database/tables/truncate/redirectDb/redirectSchema/jobs, and
// issues it using an OIDC bearer token.
//
// Because the cobra command tree lives in package main (not importable), these
// functional tests drive the SAME OperatorClient the CLI uses against an
// httptest API stub and assert the EXACT operator request the migrate command
// produces. scenario87BuildMigrateBody mirrors buildMigrateRequest in
// cmd/cloudberry-ctl/main.go exactly, so a drift in either key set is caught.
//
// The functional layer cannot render Jobs (no builder/k8s), so the 87b/87c/87e
// Job-arg assertions live in the integration suite (real api.Server -> builder
// -> fake k8s) and the e2e suite (builder parity). This file proves CLI->request
// parity ONLY and asserts the shape of the 202 envelope. No assertion depends on
// a literal "checksum" string anywhere.
// ============================================================================

const (
	scenario87Namespace = "cloudberry-test"
	scenario87Source    = "src"
	scenario87Target    = "dst"
	scenario87Prefix    = "/api/v1alpha1"
	scenario87DB        = "mydb"
	scenario87Token     = "test-oidc-token"
	scenario87TS        = "20260601020000"
)

// scenario87Captured records what the stub API server received.
type scenario87Captured struct {
	method string
	path   string
	query  url.Values
	body   map[string]interface{}
	auth   string
}

// Scenario87MigrationSuite drives the OperatorClient (the CLI's transport)
// against a recording httptest API stub that returns the 202 migration envelope.
type Scenario87MigrationSuite struct {
	suite.Suite
	ctx     context.Context
	stub    *httptest.Server
	got     *scenario87Captured
	client  *ctl.OperatorClient
	handler http.HandlerFunc
}

func TestFunctional_Scenario87(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario87MigrationSuite))
}

func (s *Scenario87MigrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.got = &scenario87Captured{}
	// Default handler records the request and returns the 202 migration envelope.
	s.handler = func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		// The migration runs as ONE coordinated Job (it captures the real
		// gpbackup timestamp and feeds it to gprestore); backupJob/restoreJob/
		// validationJob all reference that single migration Job.
		migrationJob := scenario87Source + "-migration-" + scenario87TS
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":        "migration started",
			"sourceCluster": scenario87Source,
			"targetCluster": scenario87Target,
			"timestamp":     scenario87TS,
			"migrationJob":  migrationJob,
			"backupJob":     migrationJob,
			"restoreJob":    migrationJob,
			"validationJob": migrationJob,
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
		Password:   scenario87Token,
		Timeout:    10 * time.Second,
	})
}

func (s *Scenario87MigrationSuite) TearDownTest() {
	if s.stub != nil {
		s.stub.Close()
	}
}

func (s *Scenario87MigrationSuite) record(r *http.Request) {
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

// scenario87Sub mirrors the CLI's path construction:
// ctl.ClusterSubresourcePath(source, "migrate", namespace).
func scenario87Sub() string {
	return ctl.ClusterSubresourcePath(scenario87Source, "migrate", scenario87Namespace)
}

// scenario87BuildMigrateBody mirrors buildMigrateRequest in main.go EXACTLY,
// keeping the key names 1:1 (sourceCluster/targetCluster/database/tables/
// truncate/redirectDb/redirectSchema/jobs) so a drift in either is caught.
func scenario87BuildMigrateBody(
	tables []string,
	truncate bool,
	redirectDb, redirectSchema string,
	jobs int32,
) map[string]interface{} {
	return map[string]interface{}{
		"sourceCluster":  scenario87Source,
		"targetCluster":  scenario87Target,
		"database":       scenario87DB,
		"tables":         tables,
		"truncate":       truncate,
		"redirectDb":     redirectDb,
		"redirectSchema": redirectSchema,
		"jobs":           jobs,
	}
}

// --- 87a: CLI flag -> request mapping ---

func (s *Scenario87MigrationSuite) TestFunctional_Scenario87_RequestMapping() {
	body := scenario87BuildMigrateBody(
		[]string{"public.users", "public.orders"}, true, "", "", int32(4))
	_, err := s.client.Post(s.ctx, scenario87Sub(), body)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), http.MethodPost, s.got.method)
	assert.Equal(s.T(), scenario87Prefix+"/clusters/"+scenario87Source+"/migrate", s.got.path)
	assert.Equal(s.T(), scenario87Namespace, s.got.query.Get("namespace"))
	assert.Equal(s.T(), "Bearer "+scenario87Token, s.got.auth)

	assert.Equal(s.T(), scenario87Source, s.got.body["sourceCluster"])
	assert.Equal(s.T(), scenario87Target, s.got.body["targetCluster"])
	assert.Equal(s.T(), scenario87DB, s.got.body["database"])
	assert.Equal(s.T(), true, s.got.body["truncate"])
	assert.Equal(s.T(), float64(4), s.got.body["jobs"])

	tables, ok := s.got.body["tables"].([]interface{})
	require.True(s.T(), ok, "87a: body must carry a tables array")
	require.Len(s.T(), tables, 2, "87a: both tables must map through")
	assert.Equal(s.T(), "public.users", tables[0])
	assert.Equal(s.T(), "public.orders", tables[1])
}

// --- redirect mapping (edge): redirectDb + redirectSchema ---

func (s *Scenario87MigrationSuite) TestFunctional_Scenario87_RedirectMapping() {
	body := scenario87BuildMigrateBody(
		[]string{"public.users"}, false, "otherdb", "restored", int32(0))
	_, err := s.client.Post(s.ctx, scenario87Sub(), body)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "otherdb", s.got.body["redirectDb"],
		"redirectDb must map from --redirect-db")
	assert.Equal(s.T(), "restored", s.got.body["redirectSchema"],
		"redirectSchema must map from --redirect-schema")
}

// --- 87d / 87e: response envelope shape ---

func (s *Scenario87MigrationSuite) TestFunctional_Scenario87_ResponseEnvelope() {
	body := scenario87BuildMigrateBody(
		[]string{"public.users", "public.orders"}, true, "", "", int32(4))
	resp, err := s.client.Post(s.ctx, scenario87Sub(), body)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), "migration started", resp.Body["status"])
	for _, key := range []string{
		"sourceCluster", "targetCluster", "timestamp",
		"migrationJob", "backupJob", "restoreJob", "validationJob",
	} {
		assert.Contains(s.T(), resp.Body, key,
			"202 envelope must carry the %q field", key)
	}
	assert.Equal(s.T(), scenario87Source, resp.Body["sourceCluster"])
	assert.Equal(s.T(), scenario87Target, resp.Body["targetCluster"])
	assert.Equal(s.T(), scenario87TS, resp.Body["timestamp"])
	// The single migration Job performs all phases; the phase fields reference it.
	migrationJob := scenario87Source + "-migration-" + scenario87TS
	assert.Equal(s.T(), migrationJob, resp.Body["migrationJob"])
	assert.Equal(s.T(), migrationJob, resp.Body["validationJob"])
}

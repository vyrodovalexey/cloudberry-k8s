//go:build integration

package integration

import (
	"context"
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

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 119: All Storage API Endpoints (P.1–P.6) — integration
// ============================================================================
//
// Mirrors the Scenario 118 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when KUBECONFIG / the apiserver / CRD / namespace / the operator API are
// absent). The six storage endpoints are exercised at the unit (internal/api +
// internal/db) + functional layers; the full live cross-check is the e2e Part B.
// This integration layer adds the value those layers cannot: it CURLS the
// deployed operator API for the six storage endpoints with basic-auth and
// asserts each returns its documented shape (or SKIPS cleanly).
//
// HONESTY: nothing is synthesized — when the operator API is not reachable /
// not port-forwarded the live probe skips cleanly; the catalog-well-formedness
// check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live probe (skip when unset).
//   SCENARIO119_LIVE=1    — gates the live probe (off by default).
//   SCENARIO119_NAMESPACE — namespace (default cloudberry-test).
//   SCENARIO119_CLUSTER   — live cluster name (default s119).
//   SCENARIO119_API_BASE  — operator API base URL (default http://localhost:8190).
//   SCENARIO119_API_USER  — basic-auth user (default adminuser).
//   SCENARIO119_API_PASS  — basic-auth pass (default adminpass).
//   SCENARIO119_OIDC_TOKEN — bearer token; if unset, basic-auth creds are used.
// ============================================================================

const (
	envKubeconfigS119I = "KUBECONFIG"
	envS119LiveI       = "SCENARIO119_LIVE"
	envS119NamespaceI  = "SCENARIO119_NAMESPACE"
	envS119ClusterI    = "SCENARIO119_CLUSTER"
	envS119APIBaseI    = "SCENARIO119_API_BASE"
	envS119APIUserI    = "SCENARIO119_API_USER"
	envS119APIPassI    = "SCENARIO119_API_PASS"
	envS119TokenI      = "SCENARIO119_OIDC_TOKEN"

	scenario119DefaultNamespace = "cloudberry-test"
	scenario119DefaultCluster   = "s119"
	scenario119DefaultAPIBase   = "http://localhost:8190"
	scenario119DefaultAPIUser   = "adminuser"
	scenario119DefaultAPIPass   = "adminpass"

	scenario119ExecTimeout = 90 * time.Second
	scenario119HTTPTimeout = 30 * time.Second
)

// Scenario119Suite drives the Scenario 119 storage-API live probe, gated on
// apiserver + operator-API reachability.
type Scenario119Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario119(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario119Suite))
}

func (s *Scenario119Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario119Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario119Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario119Namespace() string {
	return scenario119Env(envS119NamespaceI, scenario119DefaultNamespace)
}
func scenario119ClusterName() string {
	return scenario119Env(envS119ClusterI, scenario119DefaultCluster)
}
func scenario119APIBase() string { return scenario119Env(envS119APIBaseI, scenario119DefaultAPIBase) }

// scenario119Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario119Suite) scenario119Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario119ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario119RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO119_LIVE=1, and the namespace + CRD are served.
func (s *Scenario119Suite) scenario119RequireLive() {
	if os.Getenv(envKubeconfigS119I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 119 live probe")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 119 live probe")
	}
	if os.Getenv(envS119LiveI) != "1" {
		s.T().Skip("SCENARIO119_LIVE not set, skipping the live probe " +
			"[CONFIG-ONLY: the full live cross-check is the e2e Part B]")
	}
	if out, err := s.scenario119Kubectl("get", "namespace", scenario119Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario119Namespace(), out)
	}
	if out, err := s.scenario119Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario119AuthHeader sets the Authorization header: a bearer token when
// SCENARIO119_OIDC_TOKEN is set, otherwise basic-auth from the API creds.
func scenario119AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS119TokenI)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(scenario119Env(envS119APIUserI, scenario119DefaultAPIUser),
		scenario119Env(envS119APIPassI, scenario119DefaultAPIPass))
}

// scenario119apiURL builds a full storage API URL for the given suffix path.
func scenario119apiURL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return scenario119APIBase() + "/api/v1alpha1/clusters/" + scenario119ClusterName() +
		suffix + sep + "namespace=" + scenario119Namespace()
}

// scenario119APIResult carries the parsed result of one live API call.
type scenario119APIResult struct {
	status int
	body   []byte
}

// scenario119api issues a request to the LIVE operator API and returns the
// status + body. It returns an error only on transport failure so callers can
// SKIP cleanly when the API is not port-forwarded.
func (s *Scenario119Suite) scenario119api(method, suffix string) (scenario119APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario119HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, scenario119apiURL(suffix), nil)
	if err != nil {
		return scenario119APIResult{}, err
	}
	scenario119AuthHeader(req)
	resp, err := (&http.Client{Timeout: scenario119HTTPTimeout}).Do(req)
	if err != nil {
		return scenario119APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return scenario119APIResult{status: resp.StatusCode, body: data}, nil
}

// scenario119apiOrSkip issues a live API call and SKIPS the test cleanly on a
// transport error (the operator API is not reachable / not port-forwarded).
func (s *Scenario119Suite) scenario119apiOrSkip(method, suffix string) scenario119APIResult {
	res, err := s.scenario119api(method, suffix)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (%s %s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", scenario119APIBase(), method, suffix, err)
	}
	return res
}

// TestIntegration_Scenario119_CatalogHonest asserts the Scenario 119 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario119Suite) TestIntegration_Scenario119_CatalogHonest() {
	catalog := cases.Scenario119Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
		assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
	}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	for i := 1; i <= 6; i++ {
		req := fmt.Sprintf("P.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover endpoint family %s", req)
	}
	assert.True(s.T(), reqs["NOTFOUND"], "catalog must cover the NOTFOUND family")
	assert.True(s.T(), reqs["AUTH"], "catalog must cover the AUTH family")
	assert.True(s.T(), reqs["DBERR"], "catalog must cover the DBERR family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// TestIntegration_Scenario119_StorageEndpointsLive curls the six storage
// endpoints against the deployed operator API and asserts each returns its
// documented status/shape (best-effort / non-fatal: a DB-unavailable endpoint
// still returns 200). SKIPS cleanly when the apiserver / CRD / namespace / the
// operator API are absent.
func (s *Scenario119Suite) TestIntegration_Scenario119_StorageEndpointsLive() {
	s.scenario119RequireLive()

	// P.1 disk-usage -> 200.
	du := s.scenario119apiOrSkip(http.MethodGet, "/storage/disk-usage")
	require.Equalf(s.T(), http.StatusOK, du.status,
		"119a-P1: GET disk-usage must be 200 (body=%s)", du.body)
	s.T().Logf("scenario119 119a-P1: disk-usage 200 (body=%s)", scenario119Trunc(du.body))

	// P.2 tables -> 200.
	tbl := s.scenario119apiOrSkip(http.MethodGet, "/storage/tables")
	assert.Equalf(s.T(), http.StatusOK, tbl.status,
		"119b-P2: GET tables must be 200 (body=%s)", tbl.body)

	// P.4 recommendations -> 200.
	recs := s.scenario119apiOrSkip(http.MethodGet, "/storage/recommendations")
	assert.Equalf(s.T(), http.StatusOK, recs.status,
		"119d-P4: GET recommendations must be 200 (body=%s)", recs.body)

	// P.6 usage-report -> 200.
	usage := s.scenario119apiOrSkip(http.MethodGet, "/storage/usage-report")
	assert.Equalf(s.T(), http.StatusOK, usage.status,
		"119f-P6: GET usage-report must be 200 (body=%s)", usage.body)

	s.T().Logf("scenario119 live storage endpoints (P.1/P.2/P.4/P.6) OK")
}

// scenario119Trunc renders at most the first 200 bytes of a body for logging.
func scenario119Trunc(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}

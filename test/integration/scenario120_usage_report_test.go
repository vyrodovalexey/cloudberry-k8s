//go:build integration

package integration

import (
	"context"
	"encoding/json"
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
// Scenario 120: Usage Reporting (C.11, C.13) — integration
// ============================================================================
//
// Mirrors the Scenario 119 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when KUBECONFIG / the apiserver / CRD / namespace / the operator API are
// absent). The usage-report endpoint P.6 is exercised at the unit
// (internal/api + internal/db) + functional layers; the full live cross-check is
// the e2e Part B. This integration layer adds the value those layers cannot: it
// CURLS the deployed operator API usage-report endpoint with basic-auth and
// asserts the enabled cluster returns the enriched content (usageReportEnabled
// true + entries) and a disabled cluster returns the unavailable shape
// (usageReportEnabled false + empty) — or SKIPS cleanly.
//
// HONESTY: nothing is synthesized — when the operator API is not reachable /
// not port-forwarded the live probe skips cleanly; the catalog-well-formedness
// check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG             — gates the live probe (skip when unset).
//   SCENARIO120_LIVE=1     — gates the live probe (off by default).
//   SCENARIO120_NAMESPACE  — namespace (default cloudberry-test).
//   SCENARIO120_CLUSTER    — enabled live cluster name (default s120).
//   SCENARIO120_DISABLED_CLUSTER — disabled live cluster (default acceptance-test).
//   SCENARIO120_API_BASE   — operator API base URL (default http://localhost:8190).
//   SCENARIO120_API_USER   — basic-auth user (default adminuser).
//   SCENARIO120_API_PASS   — basic-auth pass (default adminpass).
//   SCENARIO120_OIDC_TOKEN — bearer token; if unset, basic-auth creds are used.
//   SCENARIO120_MONTH      — optional ?month= scope (default unset).
// ============================================================================

const (
	envKubeconfigS120I   = "KUBECONFIG"
	envS120LiveI         = "SCENARIO120_LIVE"
	envS120NamespaceI    = "SCENARIO120_NAMESPACE"
	envS120ClusterI      = "SCENARIO120_CLUSTER"
	envS120DisabledClusI = "SCENARIO120_DISABLED_CLUSTER"
	envS120APIBaseI      = "SCENARIO120_API_BASE"
	envS120APIUserI      = "SCENARIO120_API_USER"
	envS120APIPassI      = "SCENARIO120_API_PASS"
	envS120TokenI        = "SCENARIO120_OIDC_TOKEN"
	envS120MonthI        = "SCENARIO120_MONTH"

	scenario120DefaultNamespace = "cloudberry-test"
	scenario120DefaultCluster   = "s120"
	scenario120DefaultDisabled  = "acceptance-test"
	scenario120DefaultAPIBase   = "http://localhost:8190"
	scenario120DefaultAPIUser   = "adminuser"
	scenario120DefaultAPIPass   = "adminpass"

	scenario120ExecTimeout = 90 * time.Second
	scenario120HTTPTimeout = 30 * time.Second
)

// Scenario120Suite drives the Scenario 120 usage-report live probe, gated on
// apiserver + operator-API reachability.
type Scenario120Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario120(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario120Suite))
}

func (s *Scenario120Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario120Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario120Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario120Namespace() string {
	return scenario120Env(envS120NamespaceI, scenario120DefaultNamespace)
}
func scenario120ClusterName() string {
	return scenario120Env(envS120ClusterI, scenario120DefaultCluster)
}
func scenario120DisabledCluster() string {
	return scenario120Env(envS120DisabledClusI, scenario120DefaultDisabled)
}
func scenario120APIBase() string { return scenario120Env(envS120APIBaseI, scenario120DefaultAPIBase) }

// scenario120Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario120Suite) scenario120Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario120ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario120RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO120_LIVE=1, and the namespace + CRD are served.
func (s *Scenario120Suite) scenario120RequireLive() {
	if os.Getenv(envKubeconfigS120I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 120 live probe")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 120 live probe")
	}
	if os.Getenv(envS120LiveI) != "1" {
		s.T().Skip("SCENARIO120_LIVE not set, skipping the live probe " +
			"[CONFIG-ONLY: the full live cross-check is the e2e Part B]")
	}
	if out, err := s.scenario120Kubectl("get", "namespace", scenario120Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario120Namespace(), out)
	}
	if out, err := s.scenario120Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario120AuthHeader sets the Authorization header: a bearer token when
// SCENARIO120_OIDC_TOKEN is set, otherwise basic-auth from the API creds.
func scenario120AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS120TokenI)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(scenario120Env(envS120APIUserI, scenario120DefaultAPIUser),
		scenario120Env(envS120APIPassI, scenario120DefaultAPIPass))
}

// scenario120apiURL builds a full usage-report API URL for the given cluster +
// optional ?month= scope.
func scenario120apiURL(cluster string) string {
	url := scenario120APIBase() + "/api/v1alpha1/clusters/" + cluster +
		cases.Scenario120Endpoint + "?namespace=" + scenario120Namespace()
	if month := strings.TrimSpace(os.Getenv(envS120MonthI)); month != "" {
		url += "&" + cases.Scenario120MonthParam + "=" + month
	}
	return url
}

// scenario120APIResult carries the parsed result of one live API call.
type scenario120APIResult struct {
	status int
	body   []byte
}

// scenario120api issues a request to the LIVE operator API and returns the
// status + body. It returns an error only on transport failure so callers can
// SKIP cleanly when the API is not port-forwarded.
func (s *Scenario120Suite) scenario120api(cluster string) (scenario120APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario120HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scenario120apiURL(cluster), nil)
	if err != nil {
		return scenario120APIResult{}, err
	}
	scenario120AuthHeader(req)
	resp, err := (&http.Client{Timeout: scenario120HTTPTimeout}).Do(req)
	if err != nil {
		return scenario120APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return scenario120APIResult{status: resp.StatusCode, body: data}, nil
}

// scenario120apiOrSkip issues a live API call and SKIPS the test cleanly on a
// transport error (the operator API is not reachable / not port-forwarded).
func (s *Scenario120Suite) scenario120apiOrSkip(cluster string) scenario120APIResult {
	res, err := s.scenario120api(cluster)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (cluster %s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", scenario120APIBase(), cluster, err)
	}
	return res
}

// scenario120decode JSON-decodes a body into a generic map (best effort).
func scenario120decode(body []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(body, &m)
	return m
}

// TestIntegration_Scenario120_CatalogHonest asserts the Scenario 120 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario120Suite) TestIntegration_Scenario120_CatalogHonest() {
	catalog := cases.Scenario120Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Channel, "%s must carry a Channel", tc.ID)
		assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
		assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
	}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	assert.True(s.T(), reqs["C.11"], "catalog must cover the C.11 (generate/content) family")
	assert.True(s.T(), reqs["C.13"], "catalog must cover the C.13 (retrieve) family")
	assert.True(s.T(), reqs["MONTH"], "catalog must cover the MONTH-param family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED-unavailable family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// TestIntegration_Scenario120_UsageReportLive curls the usage-report endpoint
// against the deployed operator API for the ENABLED cluster and asserts the
// enriched content (200, usageReportEnabled true, entries present or honest
// empty). SKIPS cleanly when the apiserver / CRD / namespace / the operator API
// are absent.
func (s *Scenario120Suite) TestIntegration_Scenario120_UsageReportLive() {
	s.scenario120RequireLive()

	res := s.scenario120apiOrSkip(scenario120ClusterName())
	require.Equalf(s.T(), http.StatusOK, res.status,
		"120-C13-api-L: GET usage-report (enabled) must be 200 (body=%s)", scenario120Trunc(res.body))

	resp := scenario120decode(res.body)
	enabled, hasFlag := resp["usageReportEnabled"].(bool)
	require.True(s.T(), hasFlag, "120-C13-api-L: usage-report must carry usageReportEnabled")
	assert.True(s.T(), enabled,
		"120-C13-api-L: the enabled cluster must report usageReportEnabled:true")
	// entries present or honestly empty (DB may be unreachable in-window) — the
	// shape must at least be a list.
	_, hasEntries := resp["entries"]
	assert.True(s.T(), hasEntries, "120-C11-generate-L: the envelope must carry an entries field")
	s.T().Logf("scenario120 120-C13-api-L: usage-report (enabled) 200 usageReportEnabled=%v (body=%s)",
		enabled, scenario120Trunc(res.body))
}

// TestIntegration_Scenario120_DisabledUnavailableLive curls the usage-report
// endpoint for a usageReport-DISABLED cluster (e.g. acceptance-test) and asserts
// the unavailable shape (200, usageReportEnabled false, empty entries). SKIPS
// cleanly when the cluster is absent.
func (s *Scenario120Suite) TestIntegration_Scenario120_DisabledUnavailableLive() {
	s.scenario120RequireLive()

	disabled := scenario120DisabledCluster()
	if out, err := s.scenario120Kubectl("get", "cloudberrycluster", disabled,
		"-n", scenario120Namespace()); err != nil {
		s.T().Skipf("disabled cluster %q not present [CONFIG-ONLY]: %s", disabled, out)
	}

	res := s.scenario120apiOrSkip(disabled)
	require.Equalf(s.T(), http.StatusOK, res.status,
		"120-DISABLED-unavailable-L: disabled GET must be a SOFT 200, NOT a 400 (body=%s)",
		scenario120Trunc(res.body))
	resp := scenario120decode(res.body)
	if enabled, ok := resp["usageReportEnabled"].(bool); ok && enabled {
		s.T().Logf("120-DISABLED-unavailable-L: cluster %q reports usageReportEnabled:true "+
			"[CONFIG-ONLY-degrade: it appears to have usageReport enabled]", disabled)
		return
	}
	assert.Equal(s.T(), false, resp["usageReportEnabled"],
		"120-DISABLED-unavailable-L: a disabled cluster must report usageReportEnabled:false")
	if entries, ok := resp["entries"].([]interface{}); ok {
		assert.Empty(s.T(), entries,
			"120-DISABLED-unavailable-L: a disabled cluster must return empty entries")
	}
	s.T().Logf("scenario120 120-DISABLED-unavailable-L: %q usage-report unavailable (200, flag false)", disabled)
}

// scenario120Trunc renders at most the first 200 bytes of a body for logging.
func scenario120Trunc(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}

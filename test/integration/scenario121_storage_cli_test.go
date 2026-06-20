//go:build integration

package integration

import (
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

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 121: All Storage CLI Commands (L.1–L.6) — integration
// ============================================================================
//
// Mirrors the Scenario 120 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when KUBECONFIG / the apiserver / CRD / namespace / the operator API are
// absent). The six storage CLI commands are thin clients over the Scenario
// 119/120 P.1–P.6 endpoints, exercised at the unit (cmd/cloudberry-ctl) +
// functional layers; the full live ctl-binary cross-check is the e2e Part B.
// This integration layer adds the value those layers cannot: it PROBES the
// deployed operator API endpoints the six commands target (with basic-auth /
// bearer) and asserts each returns the documented shape — or SKIPS cleanly.
//
//   L.1 storage disk-usage           → GET  /storage/disk-usage           (diskUsagePercent)
//   L.2 storage tables list          → GET  /storage/tables                (tables/total)
//   L.3 storage tables detail        → GET  /storage/tables/{schema}/{table} (schema/table)
//   L.4 storage recommendations list → GET  /storage/recommendations       (recommendationCount)
//   L.5 storage recommendations scan → POST /storage/recommendations/scan   (202 / honest 400)
//   L.6 storage usage-report --month → GET  /storage/usage-report?month=    (month label)
//
// HONESTY: nothing is synthesized — when the operator API is not reachable /
// not port-forwarded the live probe skips cleanly; the catalog-well-formedness
// check always runs (no infra). The scan POST tolerates a 400/409 when the
// recommendationScan feature is not enabled (an honest degrade, not a failure).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG             — gates the live probe (skip when unset).
//   SCENARIO121_LIVE=1     — gates the live probe (off by default).
//   SCENARIO121_NAMESPACE  — namespace (default cloudberry-test).
//   SCENARIO121_CLUSTER    — storage-enabled live cluster name (default s121).
//   SCENARIO121_API_BASE   — operator API base URL (default http://localhost:8190).
//   SCENARIO121_API_USER   — basic-auth user (default adminuser).
//   SCENARIO121_API_PASS   — basic-auth pass (default adminpass).
//   SCENARIO121_OIDC_TOKEN — bearer token; if unset, basic-auth creds are used.
//   SCENARIO121_SCHEMA     — L.3 detail schema (default public).
//   SCENARIO121_TABLE      — L.3 detail table (default pg_class).
//   SCENARIO121_MONTH      — L.6 ?month= reporting period (default 2026-05).
// ============================================================================

const (
	envKubeconfigS121I = "KUBECONFIG"
	envS121LiveI       = "SCENARIO121_LIVE"
	envS121NamespaceI  = "SCENARIO121_NAMESPACE"
	envS121ClusterI    = "SCENARIO121_CLUSTER"
	envS121APIBaseI    = "SCENARIO121_API_BASE"
	envS121APIUserI    = "SCENARIO121_API_USER"
	envS121APIPassI    = "SCENARIO121_API_PASS"
	envS121TokenI      = "SCENARIO121_OIDC_TOKEN"
	envS121SchemaI     = "SCENARIO121_SCHEMA"
	envS121TableI      = "SCENARIO121_TABLE"
	envS121MonthI      = "SCENARIO121_MONTH"

	scenario121DefaultNamespace = "cloudberry-test"
	scenario121DefaultCluster   = "s121"
	scenario121DefaultAPIBase   = "http://localhost:8190"
	scenario121DefaultAPIUser   = "adminuser"
	scenario121DefaultAPIPass   = "adminpass"
	scenario121DefaultSchema    = "public"
	scenario121DefaultTable     = "pg_class"
	scenario121DefaultMonth     = "2026-05"

	scenario121ExecTimeout = 90 * time.Second
	scenario121HTTPTimeout = 30 * time.Second
)

// Scenario121Suite drives the Scenario 121 storage-CLI live probe, gated on
// apiserver + operator-API reachability.
type Scenario121Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario121(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario121Suite))
}

func (s *Scenario121Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario121Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario121Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario121Namespace() string {
	return scenario121Env(envS121NamespaceI, scenario121DefaultNamespace)
}
func scenario121ClusterName() string {
	return scenario121Env(envS121ClusterI, scenario121DefaultCluster)
}
func scenario121APIBase() string { return scenario121Env(envS121APIBaseI, scenario121DefaultAPIBase) }
func scenario121Schema() string  { return scenario121Env(envS121SchemaI, scenario121DefaultSchema) }
func scenario121Table() string   { return scenario121Env(envS121TableI, scenario121DefaultTable) }
func scenario121Month() string   { return scenario121Env(envS121MonthI, scenario121DefaultMonth) }

// scenario121Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario121Suite) scenario121Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario121ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario121RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO121_LIVE=1, and the namespace + CRD are served.
func (s *Scenario121Suite) scenario121RequireLive() {
	if os.Getenv(envKubeconfigS121I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 121 live probe")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 121 live probe")
	}
	if os.Getenv(envS121LiveI) != "1" {
		s.T().Skip("SCENARIO121_LIVE not set, skipping the live probe " +
			"[CONFIG-ONLY: the full live ctl-binary cross-check is the e2e Part B]")
	}
	if out, err := s.scenario121Kubectl("get", "namespace", scenario121Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario121Namespace(), out)
	}
	if out, err := s.scenario121Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario121AuthHeader sets the Authorization header: a bearer token when
// SCENARIO121_OIDC_TOKEN is set, otherwise basic-auth from the API creds.
func scenario121AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS121TokenI)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(scenario121Env(envS121APIUserI, scenario121DefaultAPIUser),
		scenario121Env(envS121APIPassI, scenario121DefaultAPIPass))
}

// scenario121apiURL builds a full storage subresource API URL for the configured
// cluster + the given subresource path + optional extra query (e.g. month=).
func scenario121apiURL(subresource, extraQuery string) string {
	url := scenario121APIBase() + "/api/v1alpha1/clusters/" + scenario121ClusterName() +
		"/storage/" + subresource + "?namespace=" + scenario121Namespace()
	if extraQuery != "" {
		url += "&" + extraQuery
	}
	return url
}

// scenario121APIResult carries the parsed result of one live API call.
type scenario121APIResult struct {
	status int
	body   []byte
}

// scenario121api issues a request to the LIVE operator API and returns the
// status + body. It returns an error only on transport failure so callers can
// SKIP cleanly when the API is not port-forwarded.
func (s *Scenario121Suite) scenario121api(method, subresource, extraQuery string) (scenario121APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario121HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, scenario121apiURL(subresource, extraQuery), nil)
	if err != nil {
		return scenario121APIResult{}, err
	}
	scenario121AuthHeader(req)
	resp, err := (&http.Client{Timeout: scenario121HTTPTimeout}).Do(req)
	if err != nil {
		return scenario121APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return scenario121APIResult{status: resp.StatusCode, body: data}, nil
}

// scenario121apiOrSkip issues a live API call and SKIPS the test cleanly on a
// transport error (the operator API is not reachable / not port-forwarded).
func (s *Scenario121Suite) scenario121apiOrSkip(method, subresource, extraQuery string) scenario121APIResult {
	res, err := s.scenario121api(method, subresource, extraQuery)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (%s /storage/%s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", scenario121APIBase(), method, subresource, err)
	}
	return res
}

// scenario121decode JSON-decodes a body into a generic map (best effort).
func scenario121decode(body []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(body, &m)
	return m
}

// scenario121Trunc renders at most the first 200 bytes of a body for logging.
func scenario121Trunc(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}

// TestIntegration_Scenario121_CatalogHonest asserts the Scenario 121 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario121Suite) TestIntegration_Scenario121_CatalogHonest() {
	catalog := cases.Scenario121Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		reqs[tc.Req] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
		assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
	}
	for i := 1; i <= 6; i++ {
		req := fmt.Sprintf("L.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover CLI command family %s", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// TestIntegration_Scenario121_StorageReadsLive probes the FIVE read endpoints the
// read CLI commands target (L.1/L.2/L.3/L.4/L.6) against the deployed operator
// API and asserts each returns the documented 200 shape. SKIPS cleanly when the
// apiserver / CRD / namespace / the operator API are absent.
//
//nolint:funlen // a five-endpoint read probe is one narrative.
func (s *Scenario121Suite) TestIntegration_Scenario121_StorageReadsLive() {
	s.scenario121RequireLive()

	// L.1 disk-usage → diskUsagePercent present.
	diskRes := s.scenario121apiOrSkip(http.MethodGet, "disk-usage", "")
	require.Equalf(s.T(), http.StatusOK, diskRes.status,
		"121a-L1-L: GET disk-usage must be 200 (body=%s)", scenario121Trunc(diskRes.body))
	diskResp := scenario121decode(diskRes.body)
	_, hasPct := diskResp["diskUsagePercent"]
	assert.True(s.T(), hasPct, "121a-L1-L: disk-usage must carry diskUsagePercent")

	// L.2 tables list → tables/total present.
	tablesRes := s.scenario121apiOrSkip(http.MethodGet, "tables", "")
	require.Equalf(s.T(), http.StatusOK, tablesRes.status,
		"121b-L2-L: GET tables must be 200 (body=%s)", scenario121Trunc(tablesRes.body))
	tablesResp := scenario121decode(tablesRes.body)
	_, hasTotal := tablesResp["total"]
	assert.True(s.T(), hasTotal, "121b-L2-L: tables list must carry a total field")

	// L.3 tables detail --schema/--table → schema/table echoed (degrade if absent).
	detailSub := "tables/" + scenario121Schema() + "/" + scenario121Table()
	detailRes := s.scenario121apiOrSkip(http.MethodGet, detailSub, "")
	if detailRes.status == http.StatusNotFound {
		s.T().Logf("121c-L3-L: table %s.%s not present [CONFIG-ONLY: no such table]",
			scenario121Schema(), scenario121Table())
	} else {
		require.Equalf(s.T(), http.StatusOK, detailRes.status,
			"121c-L3-L: GET tables detail must be 200 (body=%s)", scenario121Trunc(detailRes.body))
		detailResp := scenario121decode(detailRes.body)
		assert.Equal(s.T(), scenario121Schema(), detailResp["schema"],
			"121c-L3-L: detail must echo the requested schema")
		assert.Equal(s.T(), scenario121Table(), detailResp["table"],
			"121c-L3-L: detail must echo the requested table")
	}

	// L.4 recommendations list → recommendationCount present.
	recsRes := s.scenario121apiOrSkip(http.MethodGet, "recommendations", "")
	require.Equalf(s.T(), http.StatusOK, recsRes.status,
		"121d-L4-L: GET recommendations must be 200 (body=%s)", scenario121Trunc(recsRes.body))
	recsResp := scenario121decode(recsRes.body)
	_, hasCount := recsResp["recommendationCount"]
	assert.True(s.T(), hasCount, "121d-L4-L: recommendations must carry recommendationCount")

	// L.6 usage-report --month → the month label round-trips.
	reportRes := s.scenario121apiOrSkip(http.MethodGet, "usage-report", "month="+scenario121Month())
	require.Equalf(s.T(), http.StatusOK, reportRes.status,
		"121f-L6-L: GET usage-report must be 200 (body=%s)", scenario121Trunc(reportRes.body))
	reportResp := scenario121decode(reportRes.body)
	assert.Equal(s.T(), scenario121Month(), reportResp["month"],
		"121f-L6-L: the --month reporting period must round-trip into the report label")

	s.T().Logf("scenario121 storage reads (L.1/L.2/L.3/L.4/L.6) probed OK against %s", scenario121APIBase())
}

// TestIntegration_Scenario121_RecommendationsScanLive probes the L.5 scan POST
// endpoint against the deployed operator API. A scan-enabled cluster returns 202
// (an honest "scan initiated"); a cluster without the recommendationScan feature
// returns a clean 400/409/501 (degrade, NOT a transport failure). SKIPS cleanly
// when the API is absent.
func (s *Scenario121Suite) TestIntegration_Scenario121_RecommendationsScanLive() {
	s.scenario121RequireLive()

	res := s.scenario121apiOrSkip(http.MethodPost, "recommendations/scan", "")
	switch res.status {
	case http.StatusAccepted, http.StatusOK:
		s.T().Logf("121e-L5-L: recommendations scan initiated (%d) (body=%s)",
			res.status, scenario121Trunc(res.body))
	case http.StatusBadRequest, http.StatusConflict, http.StatusNotImplemented:
		s.T().Logf("121e-L5-L: recommendations scan not enabled (%d) "+
			"[CONFIG-ONLY-degrade: recommendationScan disabled] (body=%s)",
			res.status, scenario121Trunc(res.body))
	default:
		assert.Failf(s.T(), "unexpected scan status",
			"121e-L5-L: POST recommendations/scan returned %d (expected 202/200 or a clean "+
				"400/409/501 degrade) (body=%s)", res.status, scenario121Trunc(res.body))
	}
}

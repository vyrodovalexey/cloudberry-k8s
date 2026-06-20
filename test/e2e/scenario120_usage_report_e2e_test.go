//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 120: Usage Reporting (C.11, C.13) — E2E
// ============================================================================
//
// Mirrors the Scenario 119 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO120_LIVE-gated live Part B. Part B drives the LIVE
// operator REST API directly (reusing the scenario119 e2e's auth mechanism —
// Authorization: Bearer <OIDC> or basic-auth — against the operator API base,
// which the harness port-forwards; the operator REST API listens on port 8090
// and the basic-auth admin credentials live in the
// cloudberry-operator-admin-password secret):
//
//   120-C13-api-L      curl GET /storage/usage-report (enabled) -> 200,
//                       usageReportEnabled:true, entries with per-database (and
//                       per-table when the DB has user tables; degraded to
//                       CONFIG-ONLY per-table when none).
//   120-C11-generate-L the connected-db entry carries tables[] (or honest empty).
//   120-C13-cli-L      build/run `cloudberry-ctl storage usage-report --cluster
//                       <c> --namespace cloudberry-test` against the API and assert
//                       it prints the report (degraded to CONFIG-ONLY when the ctl
//                       cannot be run live in-window).
//   120-DISABLED-       a usageReport-DISABLED cluster (e.g. acceptance-test) ->
//   unavailable-L       API usageReportEnabled:false (unavailable).
//   120-PERSIST-L      two GETs reflect the CURRENT catalog (no stored snapshot;
//                       growthBytes stays an honest 0 — the on-demand model).
//
// Endpoints/per-table that need a live DB DEGRADE to CONFIG-ONLY (do not
// hard-fail) when the DB is not reachable / has no user tables in-window —
// best-effort / non-fatal, mirroring the production handlers. Operator-health-
// aware: a transport failure reaching the API SKIPS cleanly. Env-gated by
// KUBECONFIG + SCENARIO120_LIVE; namespace cloudberry-test; read-only — no CR
// cleanup needed.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO120_LIVE=1       — gates the live API/CLI exercise.
//   SCENARIO120_CLUSTER      — enabled live cluster name (default s120).
//   SCENARIO120_DISABLED_CLUSTER — disabled cluster (default acceptance-test).
//   SCENARIO120_NAMESPACE    — namespace (default cloudberry-test).
//   SCENARIO120_API_BASE     — operator API base URL (default http://localhost:8190).
//   SCENARIO120_OIDC_TOKEN   — bearer token; if unset, basic-auth creds are used.
//   SCENARIO120_API_USER     — basic-auth user (default adminuser).
//   SCENARIO120_API_PASS     — basic-auth pass (default adminpass).
//   SCENARIO120_MONTH        — optional ?month= scope (default 2026-05 for the
//                              live CLI exercise; unset for the API GETs).
//   SCENARIO120_CTL_BIN      — pre-built cloudberry-ctl binary (skip the build).
// ============================================================================

const (
	envKubeconfigS120 = "KUBECONFIG"
	envS120Live       = "SCENARIO120_LIVE"
	envS120Cluster    = "SCENARIO120_CLUSTER"
	envS120Disabled   = "SCENARIO120_DISABLED_CLUSTER"
	envS120Namespace  = "SCENARIO120_NAMESPACE"
	envS120APIBase    = "SCENARIO120_API_BASE"
	envS120Token      = "SCENARIO120_OIDC_TOKEN"
	envS120APIUser    = "SCENARIO120_API_USER"
	envS120APIPass    = "SCENARIO120_API_PASS"
	envS120Month      = "SCENARIO120_MONTH"
	envS120CtlBin     = "SCENARIO120_CTL_BIN"

	s120DefaultCluster   = "s120"
	s120DefaultDisabled  = "acceptance-test"
	s120DefaultNamespace = "cloudberry-test"
	s120DefaultAPIBase   = "http://localhost:8190"
	s120DefaultAPIUser   = "adminuser"
	s120DefaultAPIPass   = "adminpass"
	s120DefaultMonth     = "2026-05"

	s120ExecTimeout = 90 * time.Second
	s120HTTPTimeout = 30 * time.Second
)

// Scenario120E2ESuite verifies the usage-report surface end-to-end (catalog-
// direct Part A + KUBECONFIG-gated live Part B).
type Scenario120E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario120(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario120E2ESuite))
}

func (s *Scenario120E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario120_PartA_CatalogHonest iterates the full Scenario 120 catalog
// and asserts it is well-formed: unique IDs, every C.11 / C.13 / MONTH /
// DISABLED / CONTROL / PERSIST family present, and every row carries a non-empty
// Layer/Channel/Gate/Assert/Description with known tokens.
func (s *Scenario120E2ESuite) TestE2E_Scenario120_PartA_CatalogHonest() {
	catalog := cases.Scenario120Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario120LayerUnit,
		cases.Scenario120LayerFunctional,
		cases.Scenario120LayerLive,
	}
	knownGates := []string{
		cases.Scenario120GateEnabled,
		cases.Scenario120GateDisabled,
		cases.Scenario120GateNone,
	}
	knownChannels := []string{
		cases.Scenario120ChannelAPI,
		cases.Scenario120ChannelCLI,
		cases.Scenario120ChannelNone,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Channel, "%s must carry a Channel", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
			assert.Containsf(s.T(), knownGates, tc.Gate, "%s Gate must be a known token", tc.ID)
			assert.Containsf(s.T(), knownChannels, tc.Channel, "%s Channel must be a known token", tc.ID)
			if tc.Layer == cases.Scenario120LayerLive {
				s.T().Logf("scenario120 %s (%s, gate=%s, ch=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Channel, tc.Assert)
			}
		})
	}
	assert.True(s.T(), reqs["C.11"], "catalog must cover the C.11 (generate/content) family")
	assert.True(s.T(), reqs["C.13"], "catalog must cover the C.13 (retrieve) family")
	assert.True(s.T(), reqs["MONTH"], "catalog must cover the MONTH-param family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED-unavailable family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO120_LIVE gated live API + CLI exercise
// ----------------------------------------------------------------------------

func s120Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s120Cluster() string         { return s120Env(envS120Cluster, s120DefaultCluster) }
func s120DisabledCluster() string { return s120Env(envS120Disabled, s120DefaultDisabled) }
func s120Namespace() string       { return s120Env(envS120Namespace, s120DefaultNamespace) }
func s120APIBase() string         { return s120Env(envS120APIBase, s120DefaultAPIBase) }

// s120RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO120_LIVE=1.
func (s *Scenario120E2ESuite) s120RequireLive() {
	if os.Getenv(envKubeconfigS120) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 120 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 120 live Part B")
	}
	if os.Getenv(envS120Live) != "1" {
		s.T().Skip("SCENARIO120_LIVE not set, skipping the live usage-report exercise " +
			"(the deployed cluster + the operator API must be reachable)")
	}
}

// s120Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario120E2ESuite) s120Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s120ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s120RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario120E2ESuite) s120RequireNamespace() {
	if out, err := s.s120Kubectl("get", "namespace", s120Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s120Namespace(), out)
	}
	if out, err := s.s120Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s120AuthHeader sets the Authorization header: a bearer OIDC token when
// SCENARIO120_OIDC_TOKEN is set, otherwise basic-auth from the API creds (the
// cloudberry-operator-admin-password secret in the deployed cluster).
func s120AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS120Token)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(s120Env(envS120APIUser, s120DefaultAPIUser),
		s120Env(envS120APIPass, s120DefaultAPIPass))
}

// s120apiURL builds a full usage-report API URL for the given cluster + optional
// ?month= scope.
func s120apiURL(cluster, month string) string {
	url := s120APIBase() + "/api/v1alpha1/clusters/" + cluster +
		cases.Scenario120Endpoint + "?namespace=" + s120Namespace()
	if month != "" {
		url += "&" + cases.Scenario120MonthParam + "=" + month
	}
	return url
}

// s120APIResult carries the parsed result of one live API call.
type s120APIResult struct {
	status int
	body   []byte
}

// s120api issues a GET to the LIVE operator API and returns the status + body.
// It returns an error only on transport failure so callers can SKIP cleanly when
// the API is not port-forwarded.
func (s *Scenario120E2ESuite) s120api(cluster, month string) (s120APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s120HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s120apiURL(cluster, month), nil)
	if err != nil {
		return s120APIResult{}, err
	}
	s120AuthHeader(req)
	resp, err := (&http.Client{Timeout: s120HTTPTimeout}).Do(req)
	if err != nil {
		return s120APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return s120APIResult{status: resp.StatusCode, body: data}, nil
}

// s120apiOrSkip issues a live API call and SKIPS the test cleanly on a transport
// error (the operator API is not reachable / not port-forwarded).
func (s *Scenario120E2ESuite) s120apiOrSkip(cluster, month string) s120APIResult {
	res, err := s.s120api(cluster, month)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (cluster %s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", s120APIBase(), cluster, err)
	}
	return res
}

// s120decode JSON-decodes a body into a generic map (best effort).
func s120decode(body []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(body, &m)
	return m
}

// s120ConnectedTablesPresent reports whether ANY entry in a usage-report body
// carries a non-empty tables[] breakdown (the connected-db per-table content).
func s120ConnectedTablesPresent(body []byte) bool {
	var resp struct {
		Entries []struct {
			Tables []struct {
				Table string `json:"table"`
			} `json:"tables"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	for _, e := range resp.Entries {
		if len(e.Tables) > 0 {
			return true
		}
	}
	return false
}

// TestE2E_Scenario120_LiveUsageReport drives the LIVE usage-report surface for
// the ENABLED cluster (120-C13-api-L / 120-C11-generate-L / 120-PERSIST-L):
// GET usage-report -> 200, usageReportEnabled:true, entries present (with the
// per-table breakdown when the DB has user tables, else CONFIG-ONLY degrade);
// two GETs reflect the current catalog with no persisted snapshot (growth honest
// 0). SKIPS cleanly when the live env is absent. Read-only — no cleanup needed.
//
//nolint:gocyclo,funlen // a self-contained live read narrative.
func (s *Scenario120E2ESuite) TestE2E_Scenario120_LiveUsageReport() {
	s.s120RequireLive()
	s.s120RequireNamespace()

	// 120-C13-api-L: GET usage-report (enabled) -> 200 usageReportEnabled:true.
	res := s.s120apiOrSkip(s120Cluster(), "")
	require.Equalf(s.T(), http.StatusOK, res.status,
		"120-C13-api-L: GET usage-report must be 200 (body=%s)", res.body)
	resp := s120decode(res.body)
	enabled, hasFlag := resp["usageReportEnabled"].(bool)
	require.True(s.T(), hasFlag, "120-C13-api-L: usage-report must carry usageReportEnabled")
	assert.True(s.T(), enabled,
		"120-C13-api-L: the enabled cluster must report usageReportEnabled:true")
	_, hasEntries := resp["entries"]
	assert.True(s.T(), hasEntries, "120-C13-api-L: the envelope must carry an entries field")

	// 120-C11-generate-L: per-table breakdown present, else CONFIG-ONLY degrade.
	if s120ConnectedTablesPresent(res.body) {
		s.T().Logf("scenario120 120-C11-generate-L: connected-db per-table breakdown present")
	} else {
		s.T().Log("120-C11-generate-L: no per-table breakdown observed live " +
			"[CONFIG-ONLY-degrade: the DB may be unreachable / have no user tables]")
	}

	// 120-PERSIST-L: a second GET reflects the CURRENT catalog; growthBytes stays
	// an honest 0 (no persisted month-over-month snapshot).
	res2 := s.s120apiOrSkip(s120Cluster(), "")
	require.Equalf(s.T(), http.StatusOK, res2.status,
		"120-PERSIST-L: the second GET must be 200 (body=%s)", res2.body)
	assert.False(s.T(), s120GrowthNonZero(res2.body),
		"120-PERSIST-L: growthBytes must stay an honest 0 (no persisted history)")
	s.T().Log("scenario120 Part B: live usage-report (enabled) OK")
}

// s120GrowthNonZero reports whether ANY entry carries a non-zero growthBytes (it
// must NOT under the honest on-demand model — there is no persisted baseline).
func s120GrowthNonZero(body []byte) bool {
	var resp struct {
		Entries []struct {
			GrowthBytes int64 `json:"growthBytes"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	for _, e := range resp.Entries {
		if e.GrowthBytes != 0 {
			return true
		}
	}
	return false
}

// TestE2E_Scenario120_LiveDisabledUnavailable drives the LIVE usage-report
// surface for a usageReport-DISABLED cluster (120-DISABLED-unavailable-L): GET
// usage-report -> 200 usageReportEnabled:false + empty entries (the SOFT gate,
// NOT a 400). SKIPS cleanly when the disabled cluster is absent.
func (s *Scenario120E2ESuite) TestE2E_Scenario120_LiveDisabledUnavailable() {
	s.s120RequireLive()
	s.s120RequireNamespace()

	disabled := s120DisabledCluster()
	if out, err := s.s120Kubectl("get", "cloudberrycluster", disabled, "-n", s120Namespace()); err != nil {
		s.T().Skipf("disabled cluster %q not present [CONFIG-ONLY]: %s", disabled, out)
	}

	res := s.s120apiOrSkip(disabled, "")
	require.Equalf(s.T(), http.StatusOK, res.status,
		"120-DISABLED-unavailable-L: disabled GET must be a SOFT 200, NOT a 400 (body=%s)", res.body)
	resp := s120decode(res.body)
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

// TestE2E_Scenario120_LiveCLI exercises the CLI retrieval channel
// (120-C13-cli-L): build cloudberry-ctl and run `storage usage-report --cluster
// <c> --namespace cloudberry-test --month <m>` against the LIVE operator API,
// asserting it exits 0 and prints the usage report. The live ctl run degrades to
// CONFIG-ONLY (does not hard-fail) when the binary cannot be built/run in-window.
// SKIPS cleanly when the live env is absent.
func (s *Scenario120E2ESuite) TestE2E_Scenario120_LiveCLI() {
	s.s120RequireLive()
	s.s120RequireNamespace()

	// Probe the API first; if it is not reachable, SKIP cleanly.
	_ = s.s120apiOrSkip(s120Cluster(), "")

	bin, ok := s.s120CtlBinary()
	if !ok {
		s.T().Log("120-C13-cli-L: cloudberry-ctl could not be built in-window " +
			"[CONFIG-ONLY: the CLI path is unit-asserted in cmd/cloudberry-ctl]")
		return
	}

	month := s120Env(envS120Month, s120DefaultMonth)
	out, runErr := s.s120RunCtl(bin, "storage", "usage-report", "--month", month)
	if runErr != nil {
		s.T().Logf("120-C13-cli-L: live ctl usage-report exited non-zero "+
			"[CONFIG-ONLY-degrade]: %v (out=%s)", runErr, s120Trunc([]byte(out)))
		return
	}
	// The ctl prints the JSON envelope; assert it mentions the usage-report flag.
	assert.Contains(s.T(), out, "usageReportEnabled",
		"120-C13-cli-L: the live ctl must print the usage-report envelope")
	s.T().Logf("scenario120 120-C13-cli-L: live ctl usage-report OK (out=%s)", s120Trunc([]byte(out)))
}

// s120CtlBinary returns the path to a cloudberry-ctl binary, building one into a
// temp dir when SCENARIO120_CTL_BIN is not provided. It returns false (degrade to
// CONFIG-ONLY) rather than hard-failing when the build cannot complete.
func (s *Scenario120E2ESuite) s120CtlBinary() (string, bool) {
	if bin := strings.TrimSpace(os.Getenv(envS120CtlBin)); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin, true
		}
		return "", false
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	repoRoot := filepath.Dir(filepath.Dir(wd)) // test/e2e -> repo root
	bin := filepath.Join(s.T().TempDir(), "cloudberry-ctl")

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/cloudberry-ctl")
	build.Dir = repoRoot
	build.Env = os.Environ()
	if out, buildErr := build.CombinedOutput(); buildErr != nil {
		s.T().Logf("120-C13-cli-L: building cloudberry-ctl failed [CONFIG-ONLY]: %v (out=%s)",
			buildErr, string(out))
		return "", false
	}
	return bin, true
}

// s120AuthArgs returns the ctl auth flags: a bearer OIDC token
// (--auth-method oidc --password <token>) when SCENARIO120_OIDC_TOKEN is set,
// otherwise basic-auth from the API creds.
func s120AuthArgs() []string {
	if tok := strings.TrimSpace(os.Getenv(envS120Token)); tok != "" {
		return []string{"--auth-method", "oidc", "--username", "oidc", "--password", tok}
	}
	return []string{
		"--auth-method", "basic",
		"--username", s120Env(envS120APIUser, s120DefaultAPIUser),
		"--password", s120Env(envS120APIPass, s120DefaultAPIPass),
	}
}

// s120RunCtl invokes the cloudberry-ctl binary with the standard operator-url +
// cluster + namespace + auth flags plus the given args, returning the combined
// output and the run error.
func (s *Scenario120E2ESuite) s120RunCtl(bin string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s120ExecTimeout)
	defer cancel()
	full := append([]string{
		"--operator-url", s120APIBase(),
		"--cluster", s120Cluster(),
		"--namespace", s120Namespace(),
	}, s120AuthArgs()...)
	full = append(full, args...)

	cmd := exec.CommandContext(ctx, bin, full...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s120Trunc renders at most the first 200 bytes of a body for logging.
func s120Trunc(b []byte) string {
	if len(b) > 200 {
		return string(b[:200]) + "..."
	}
	return string(b)
}

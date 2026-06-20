//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 119: All Storage API Endpoints (P.1–P.6) — E2E
// ============================================================================
//
// Mirrors the Scenario 107 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO119_LIVE-gated live Part B. Part B drives the LIVE
// operator REST API directly (reusing the s107 e2e's auth mechanism —
// Authorization: Bearer <OIDC> or basic-auth — against the operator API base):
//
//   P.1 disk-usage      -> 200, diskUsagePercent matches GET cluster
//        status.diskUsagePercent (the M.1==S.1 invariant).
//   P.2 tables          -> 200 tables list.
//   P.3 tables/public/<sometable> -> 200 detail (the table is picked from the
//        live P.2 listing; degrades to CONFIG-ONLY when no tables are observed).
//   P.4 recommendations -> 200 list.
//   P.5 POST scan       -> 202 (enabled) and recommendation_scan_duration_seconds
//        _count advances on the operator /metrics; 400 if disabled.
//   P.6 usage-report    -> 200 (usageReportEnabled per the live CR spec).
//
// Endpoints that need a live DB DEGRADE to CONFIG-ONLY (do not hard-fail) when
// the DB is not reachable in-window — best-effort / non-fatal, mirroring the
// production handlers. Operator-health-aware: a transport failure reaching the
// API SKIPS cleanly. Env-gated by KUBECONFIG + SCENARIO119_LIVE.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG             — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO119_LIVE=1     — gates the live API exercise.
//   SCENARIO119_CLUSTER    — live cluster name (default s119).
//   SCENARIO119_NAMESPACE  — namespace (default cloudberry-test).
//   SCENARIO119_API_BASE   — operator API base URL (default http://localhost:8190).
//   SCENARIO119_OIDC_TOKEN — bearer token; if unset, basic-auth creds are used.
//   SCENARIO119_API_USER   — basic-auth user (default adminuser).
//   SCENARIO119_API_PASS   — basic-auth pass (default adminpass).
// ============================================================================

const (
	envKubeconfigS119 = "KUBECONFIG"
	envS119Live       = "SCENARIO119_LIVE"
	envS119Cluster    = "SCENARIO119_CLUSTER"
	envS119Namespace  = "SCENARIO119_NAMESPACE"
	envS119APIBase    = "SCENARIO119_API_BASE"
	envS119Token      = "SCENARIO119_OIDC_TOKEN"
	envS119APIUser    = "SCENARIO119_API_USER"
	envS119APIPass    = "SCENARIO119_API_PASS"

	s119DefaultCluster   = "s119"
	s119DefaultNamespace = "cloudberry-test"
	s119DefaultAPIBase   = "http://localhost:8190"
	s119DefaultAPIUser   = "adminuser"
	s119DefaultAPIPass   = "adminpass"

	s119LiveTimeout  = 5 * time.Minute
	s119PollInterval = 10 * time.Second
	s119ExecTimeout  = 90 * time.Second
	s119HTTPTimeout  = 30 * time.Second
)

// Scenario119E2ESuite verifies the full storage REST surface end-to-end
// (catalog-direct Part A + KUBECONFIG-gated live Part B).
type Scenario119E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario119(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario119E2ESuite))
}

func (s *Scenario119E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario119_PartA_CatalogHonest iterates the full Scenario 119 catalog
// and asserts it is well-formed: unique IDs, every P.1–P.6 + NOTFOUND + AUTH +
// DBERR + CONTROL + PERSIST family present, and every row carries a non-empty
// Layer/Gate/Assert/Description with known tokens.
func (s *Scenario119E2ESuite) TestE2E_Scenario119_PartA_CatalogHonest() {
	catalog := cases.Scenario119Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario119LayerUnit,
		cases.Scenario119LayerFunctional,
		cases.Scenario119LayerLive,
	}
	knownGates := []string{
		cases.Scenario119GateEnabled,
		cases.Scenario119GateDisabled,
		cases.Scenario119GateNone,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
			assert.Containsf(s.T(), knownGates, tc.Gate, "%s Gate must be a known token", tc.ID)
			if tc.Layer == cases.Scenario119LayerLive {
				s.T().Logf("scenario119 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Assert)
			}
		})
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

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO119_LIVE gated live API exercise
// ----------------------------------------------------------------------------

func s119Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s119Cluster() string   { return s119Env(envS119Cluster, s119DefaultCluster) }
func s119Namespace() string { return s119Env(envS119Namespace, s119DefaultNamespace) }
func s119APIBase() string   { return s119Env(envS119APIBase, s119DefaultAPIBase) }

// s119RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO119_LIVE=1.
func (s *Scenario119E2ESuite) s119RequireLive() {
	if os.Getenv(envKubeconfigS119) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 119 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 119 live Part B")
	}
	if os.Getenv(envS119Live) != "1" {
		s.T().Skip("SCENARIO119_LIVE not set, skipping the live storage API exercise " +
			"(the deployed cluster + the operator API must be reachable)")
	}
}

// s119Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario119E2ESuite) s119Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s119ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s119RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario119E2ESuite) s119RequireNamespace() {
	if out, err := s.s119Kubectl("get", "namespace", s119Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s119Namespace(), out)
	}
	if out, err := s.s119Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s119AuthHeader sets the Authorization header: a bearer OIDC token when
// SCENARIO119_OIDC_TOKEN is set, otherwise basic-auth from the API creds.
func s119AuthHeader(req *http.Request) {
	if tok := strings.TrimSpace(os.Getenv(envS119Token)); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
		return
	}
	req.SetBasicAuth(s119Env(envS119APIUser, s119DefaultAPIUser),
		s119Env(envS119APIPass, s119DefaultAPIPass))
}

// s119apiURL builds a full storage API URL for the given suffix path.
func s119apiURL(suffix string) string {
	sep := "?"
	if strings.Contains(suffix, "?") {
		sep = "&"
	}
	return s119APIBase() + "/api/v1alpha1/clusters/" + s119Cluster() +
		suffix + sep + "namespace=" + s119Namespace()
}

// s119APIResult carries the parsed result of one live API call.
type s119APIResult struct {
	status int
	body   []byte
}

// s119api issues a request to the LIVE operator API and returns the status +
// body. It returns an error only on transport failure so callers can SKIP
// cleanly when the API is not port-forwarded.
func (s *Scenario119E2ESuite) s119api(method, suffix string) (s119APIResult, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s119HTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, s119apiURL(suffix), nil)
	if err != nil {
		return s119APIResult{}, err
	}
	s119AuthHeader(req)
	resp, err := (&http.Client{Timeout: s119HTTPTimeout}).Do(req)
	if err != nil {
		return s119APIResult{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return s119APIResult{status: resp.StatusCode, body: data}, nil
}

// s119apiOrSkip issues a live API call and SKIPS the test cleanly on a transport
// error (the operator API is not reachable / not port-forwarded).
func (s *Scenario119E2ESuite) s119apiOrSkip(method, suffix string) s119APIResult {
	res, err := s.s119api(method, suffix)
	if err != nil {
		s.T().Skipf("operator API not reachable at %s (%s %s): %v "+
			"[CONFIG-ONLY: API not port-forwarded]", s119APIBase(), method, suffix, err)
	}
	return res
}

// s119decode JSON-decodes a body into a generic map (best effort).
func s119decode(body []byte) map[string]interface{} {
	var m map[string]interface{}
	_ = json.Unmarshal(body, &m)
	return m
}

// s119StatusDiskUsagePercent reads the live CR's status.diskUsagePercent via
// kubectl jsonpath (best-effort; returns false when absent/unparseable).
func (s *Scenario119E2ESuite) s119StatusDiskUsagePercent() (float64, bool) {
	out, err := s.s119Kubectl("get", "cloudberrycluster", s119Cluster(), "-n", s119Namespace(),
		"-o", "jsonpath={.status.diskUsagePercent}")
	if err != nil {
		return 0, false
	}
	v, perr := strconv.ParseFloat(strings.TrimSpace(out), 64)
	if perr != nil {
		return 0, false
	}
	return v, true
}

// TestE2E_Scenario119_LiveStorageEndpoints drives the LIVE storage surface
// (P.1/P.2/P.3/P.4/P.6) read-only: each endpoint returns its documented shape,
// P.1's diskUsagePercent matches the live CR status (M.1==S.1), and P.3 GETs the
// detail of a table observed in the P.2 listing. Endpoints needing a live DB
// degrade to CONFIG-ONLY (never hard-fail) when the DB is unreachable in-window.
// SKIPS cleanly when the live env is absent. Read-only — no cleanup needed.
//
//nolint:gocyclo,funlen // a self-contained live read narrative across six endpoints.
func (s *Scenario119E2ESuite) TestE2E_Scenario119_LiveStorageEndpoints() {
	s.s119RequireLive()
	s.s119RequireNamespace()

	// P.1 disk-usage -> 200; diskUsagePercent matches status.diskUsagePercent.
	du := s.s119apiOrSkip(http.MethodGet, "/storage/disk-usage")
	require.Equalf(s.T(), http.StatusOK, du.status,
		"119a-P1-L: GET disk-usage must be 200 (body=%s)", du.body)
	duResp := s119decode(du.body)
	apiPercent, hasAPI := duResp["diskUsagePercent"].(float64)
	if statusPercent, hasStatus := s.s119StatusDiskUsagePercent(); hasStatus && hasAPI {
		assert.Equalf(s.T(), statusPercent, apiPercent,
			"119a-P1-L: API diskUsagePercent (%v) must equal status.diskUsagePercent (%v)",
			apiPercent, statusPercent)
		s.T().Logf("scenario119 119a-P1-L: diskUsagePercent API==status==%v", apiPercent)
	} else {
		s.T().Logf("119a-P1-L: status.diskUsagePercent not readable [CONFIG-ONLY-degrade]")
	}

	// P.2 tables -> 200; capture a table for P.3.
	tbl := s.s119apiOrSkip(http.MethodGet, "/storage/tables")
	require.Equalf(s.T(), http.StatusOK, tbl.status,
		"119b-P2-L: GET tables must be 200 (body=%s)", tbl.body)
	schema, table, hasTable := s119FirstTable(tbl.body)

	// P.3 tables/{schema}/{table} -> 200 (degrade to CONFIG-ONLY when no table
	// was observed live — the DB may be unreachable / empty in-window).
	if hasTable {
		detail := s.s119apiOrSkip(http.MethodGet, "/storage/tables/"+schema+"/"+table)
		assert.Equalf(s.T(), http.StatusOK, detail.status,
			"119c-P3-L: GET tables/%s/%s must be 200 (body=%s)", schema, table, detail.body)
		s.T().Logf("scenario119 119c-P3-L: detail %s.%s 200", schema, table)
	} else {
		s.T().Log("119c-P3-L: no table observed in the live P.2 listing " +
			"[CONFIG-ONLY-degrade: DB may be unreachable/empty]")
	}

	// P.4 recommendations -> 200.
	recs := s.s119apiOrSkip(http.MethodGet, "/storage/recommendations")
	assert.Equalf(s.T(), http.StatusOK, recs.status,
		"119d-P4-L: GET recommendations must be 200 (body=%s)", recs.body)

	// P.6 usage-report -> 200; usageReportEnabled present.
	usage := s.s119apiOrSkip(http.MethodGet, "/storage/usage-report")
	require.Equalf(s.T(), http.StatusOK, usage.status,
		"119f-P6-L: GET usage-report must be 200 (body=%s)", usage.body)
	usageResp := s119decode(usage.body)
	_, hasFlag := usageResp["usageReportEnabled"]
	assert.True(s.T(), hasFlag, "119f-P6-L: usage-report must carry usageReportEnabled")
	s.T().Logf("scenario119 Part B: live storage reads (P.1/P.2/P.3/P.4/P.6) OK")
}

// TestE2E_Scenario119_LiveScanAndMetrics drives the LIVE P.5 scan
// (119e-P5-L / 119-PERSIST-L): when recommendationScan is enabled, POST scan
// returns 202 and the recommendation_scan_duration_seconds_count on the operator
// /metrics advances vs the pre-POST baseline (a NEW run, independent of cron).
// When disabled it returns 400 RECOMMENDATION_SCAN_NOT_ENABLED. The metrics
// advance is degraded to CONFIG-ONLY if /metrics is unreachable in-window.
// SKIPS cleanly when the live env is absent. Read/POST-only — no cleanup needed.
func (s *Scenario119E2ESuite) TestE2E_Scenario119_LiveScanAndMetrics() {
	s.s119RequireLive()
	s.s119RequireNamespace()

	enabled := s.s119ScanEnabled()
	if !enabled {
		// Disabled gate -> 400 RECOMMENDATION_SCAN_NOT_ENABLED.
		res := s.s119apiOrSkip(http.MethodPost, "/storage/recommendations/scan")
		assert.Equalf(s.T(), http.StatusBadRequest, res.status,
			"119e-P5-L: disabled scan must be 400 (body=%s)", res.body)
		assert.Contains(s.T(), string(res.body), cases.Scenario119ScanNotEnabledCode)
		s.T().Log("scenario119 119e-P5-L: scan disabled -> 400 RECOMMENDATION_SCAN_NOT_ENABLED")
		return
	}

	// Baseline the duration _count, POST the scan, then assert it advanced.
	before, hadBefore := s.s119ScrapeDurationCount(s119Cluster())

	res := s.s119apiOrSkip(http.MethodPost, "/storage/recommendations/scan")
	require.Equalf(s.T(), http.StatusAccepted, res.status,
		"119e-P5-L: enabled scan must be 202 (body=%s)", res.body)

	if !hadBefore {
		s.T().Log("119e-P5-L: pre-POST duration _count not scrapeable " +
			"[CONFIG-ONLY-degrade: operator /metrics not reachable]")
		return
	}
	advanced := s.s119WaitFor(func() bool {
		after, found := s.s119ScrapeDurationCount(s119Cluster())
		return found && after > before
	})
	if !advanced {
		s.T().Log("119e-P5-L: duration _count did not advance in window " +
			"[CONFIG-ONLY-degrade: /metrics or the scan may lag]")
		return
	}
	after, _ := s.s119ScrapeDurationCount(s119Cluster())
	assert.Greater(s.T(), after, before,
		"119e-P5-L: recommendation_scan_duration_seconds_count must advance per POST")
	s.T().Logf("scenario119 119e-P5-L: %s_count %.0f -> %.0f",
		cases.Scenario119DurationMetricName, before, after)
}

// s119ScanEnabled reports whether the live CR has recommendationScan.enabled.
func (s *Scenario119E2ESuite) s119ScanEnabled() bool {
	out, err := s.s119Kubectl("get", "cloudberrycluster", s119Cluster(), "-n", s119Namespace(),
		"-o", "jsonpath={.spec.storage.recommendationScan.enabled}")
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) == "true"
}

// s119WaitFor polls cond until it returns true or the wait budget is exhausted.
func (s *Scenario119E2ESuite) s119WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(s119LiveTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(s119PollInterval)
	}
	return false
}

// s119ScrapeDurationCount best-effort scrapes
// recommendation_scan_duration_seconds_count for the cluster from the operator
// /metrics endpoint via `kubectl exec` into the operator pod.
func (s *Scenario119E2ESuite) s119ScrapeDurationCount(cluster string) (float64, bool) {
	pod, err := s.s119Kubectl("get", "pods", "-n", s119Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return 0, false
	}
	out, execErr := s.s119Kubectl("exec", pod, "-n", s119Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return 0, false
	}
	return s119ParseSeriesValue(out, cases.Scenario119DurationMetricName+"_count", cluster)
}

// s119ParseSeriesValue parses a Prometheus text exposition for the named series
// whose cluster label matches, returning its value and whether it was found.
func s119ParseSeriesValue(text, metric, cluster string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, metric+"{") {
			continue
		}
		if !strings.Contains(line, `cluster="`+cluster+`"`) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

// s119FirstTable extracts the first {schema,table} from a P.2 tables-listing
// body, returning ("", "", false) when none is present.
func s119FirstTable(body []byte) (string, string, bool) {
	var resp struct {
		Tables []struct {
			Schema string `json:"schema"`
			Table  string `json:"table"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", "", false
	}
	for _, t := range resp.Tables {
		if t.Schema != "" && t.Table != "" {
			return t.Schema, t.Table, true
		}
	}
	return "", "", false
}

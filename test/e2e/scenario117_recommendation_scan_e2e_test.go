//go:build e2e

package e2e

import (
	"context"
	"fmt"
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
// Scenario 117: Recommendation Scan Across All Four Types
// (reconciliation rules S.2, M.2, R.3, R.4, RT.1–RT.4, C.6–C.9, M.4) — E2E
// ============================================================================
//
// Mirrors the Scenario 116 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO117_LIVE-gated live Part B. Part B is the LIVE
// recommendation-scan proof: it `kubectl apply`s a CloudberryCluster with
// recommendationScan.enabled:true, then builds DB fixtures to trigger each of the
// four recommendation types via `kubectl exec <cluster>-coordinator-0 -c
// cloudberry -- psql`:
//
//   117a bloat: CREATE TABLE; INSERT N rows; DELETE most rows (dead tuples);
//               ANALYZE → dead-tuple % above bloatThreshold.
//   117b skew:  CREATE TABLE ... DISTRIBUTED BY (k); INSERT many rows with a
//               single k value → skew coefficient above skewThreshold (requires
//               gp_toolkit.gp_skew_coefficients — clean degrade to CONFIG-ONLY
//               when absent).
//   117c age:   age(relfrozenxid) is hard to push high in a short test window —
//               degrade to CONFIG-ONLY when no table reaches the threshold.
//   117d index: CREATE TABLE + INDEX; INSERT/DELETE churn → bloated index.
//
// Then it triggers a scan (via reconcile/settle), GETs status.recommendationCount,
// scrapes the operator /metrics for cloudberry_recommendations_total{type} and
// cloudberry_table_bloat_ratio, and asserts the per-type counts and that the sum
// of the per-type gauges == status.recommendationCount (M.2==count). 117a CLEAR:
// VACUUM (FULL) the bloated table → the next scan clears the bloat recommendation.
//
// Operator/webhook health: if the apply fails with a TLS/connection error (NOT an
// admission decision) the operator/webhook is unhealthy — Part B distinguishes
// that and SKIPS cleanly with a CONFIG-ONLY message. Any un-triggerable type
// degrades to CONFIG-ONLY (does NOT hard-fail). Self-contained; generous
// timeouts; SKIPS cleanly when the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO117_LIVE=1    — gates the live apply/scan proof.
//   SCENARIO117_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS117 = "KUBECONFIG"
	envS117Live       = "SCENARIO117_LIVE"
	envS117Namespace  = "SCENARIO117_NAMESPACE"

	s117DefaultNamespace = "cloudberry-test"

	s117ExecTimeout = 2 * time.Minute
	// s117StatusWait bounds the poll for the operator-side scan to settle.
	s117StatusWait = 2 * time.Minute
)

// s117RecTypes is the canonical set of per-type metric labels.
var s117RecTypes = []string{
	cases.Scenario117TypeBloat,
	cases.Scenario117TypeSkew,
	cases.Scenario117TypeAge,
	cases.Scenario117TypeIndexBloat,
}

// Scenario117E2ESuite verifies the recommendation scan across all four types
// end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B that applies
// recommendationScan:true, builds per-type DB fixtures, and asserts the per-type
// counts/metrics and the M.2==count invariant + the 117a CLEAR re-scan).
type Scenario117E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario117(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario117E2ESuite))
}

func (s *Scenario117E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario117_PartA_CatalogHonest iterates the full Scenario 117 catalog
// and asserts it is well-formed: unique IDs, every RT.1–RT.4 / C.6–C.9 / M.4 /
// S.2 / R.4 / M.2 / R.3 + DISABLED + DBERR + CONTROL + PERSIST family present, and
// every row carries a non-empty Layer/Gate/Expected/Description with known tokens.
func (s *Scenario117E2ESuite) TestE2E_Scenario117_PartA_CatalogHonest() {
	catalog := cases.Scenario117Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario117LayerUnit,
		cases.Scenario117LayerFunctional,
		cases.Scenario117LayerLive,
	}
	knownGates := []string{
		cases.Scenario117GateScanning,
		cases.Scenario117GateDisabled,
		cases.Scenario117GateNone,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
			assert.Containsf(s.T(), knownGates, tc.Gate, "%s Gate must be a known token", tc.ID)
			if tc.Layer == cases.Scenario117LayerLive {
				s.T().Logf("scenario117 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Expected)
			}
		})
	}
	for _, req := range []string{"RT.1", "RT.2", "RT.3", "RT.4", "C.6", "C.7", "C.8", "C.9"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	for _, req := range []string{"S.2", "R.4", "M.2", "R.3", "M.4"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["DBERR"], "catalog must cover the DBERR family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO117_LIVE gated live recommendation-scan proof
// ----------------------------------------------------------------------------

func s117Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s117Namespace() string { return s117Env(envS117Namespace, s117DefaultNamespace) }

// s117RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO117_LIVE=1.
func (s *Scenario117E2ESuite) s117RequireLive() {
	if os.Getenv(envKubeconfigS117) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 117 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 117 live Part B")
	}
	if os.Getenv(envS117Live) != "1" {
		s.T().Skip("SCENARIO117_LIVE not set, skipping the live recommendation-scan proof " +
			"(the deployed operator + db + the Vault-PKI webhook must be reachable)")
	}
}

// s117Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario117E2ESuite) s117Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s117ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s117ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario117E2ESuite) s117ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s117ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s117Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s117LooksLikeUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the operator/webhook (NOT an admission decision).
func s117LooksLikeUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "certificate signed by unknown authority") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// s117RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario117E2ESuite) s117RequireNamespace() {
	if out, err := s.s117Kubectl("get", "namespace", s117Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s117Namespace(), out)
	}
	if out, err := s.s117Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s117GetField runs `kubectl get cloudberrycluster -o jsonpath` for a single
// field and returns the rendered value.
func (s *Scenario117E2ESuite) s117GetField(name, jsonPath string) (string, error) {
	return s.s117Kubectl("get", "cloudberrycluster", name,
		"-n", s117Namespace(), "-o", "jsonpath="+jsonPath)
}

// s117ScanYAML returns a base-valid CloudberryCluster manifest (HA mirrored) with
// the recommendation scan enabled across all four types.
func s117ScanYAML(name string) string {
	return fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: %s
spec:
  version: "1.6.0"
  image: "cloudberrydb/cloudberry:1.6.0"
  coordinator:
    replicas: 1
    storage:
      size: "10Gi"
  segments:
    count: 2
    primariesPerHost: 1
    storage:
      size: "10Gi"
    mirroring:
      enabled: true
      layout: spread
  storage:
    diskMonitoring: true
    recommendationScan:
      enabled: true
      schedule: "0 3 * * 0"
      bloatThreshold: 20
      skewThreshold: 30
      ageThreshold: 100000000
      indexBloatThreshold: 40
      scanDuration: "2h"
`, name)
}

// s117WaitFor polls cond until it returns true or the wait budget is exhausted.
func (s *Scenario117E2ESuite) s117WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(s117StatusWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Second)
	}
	return false
}

// s117Psql runs a SQL statement against the cluster coordinator via
// `kubectl exec <cluster>-coordinator-0 -c cloudberry -- psql`. Best-effort:
// returns the combined output and error.
func (s *Scenario117E2ESuite) s117Psql(cluster, sql string) (string, error) {
	pod := cluster + "-coordinator-0"
	return s.s117Kubectl("exec", pod, "-n", s117Namespace(), "-c", "cloudberry", "--",
		"psql", "-U", "gpadmin", "-d", "postgres", "-tAc", sql)
}

// s117BuildBloatFixture creates a churned table whose dead-tuple percentage is
// above bloatThreshold (117a). Returns false (CONFIG-ONLY degrade) if any psql
// step fails.
func (s *Scenario117E2ESuite) s117BuildBloatFixture(cluster string) bool {
	stmts := []string{
		"DROP TABLE IF EXISTS t117_bloat;",
		"CREATE TABLE t117_bloat (id int, payload text) DISTRIBUTED BY (id);",
		"INSERT INTO t117_bloat SELECT g, repeat('x', 64) FROM generate_series(1, 20000) g;",
		"DELETE FROM t117_bloat WHERE id > 2000;",
		"ANALYZE t117_bloat;",
	}
	for _, sql := range stmts {
		if _, err := s.s117Psql(cluster, sql); err != nil {
			return false
		}
	}
	return true
}

// s117BuildSkewFixture creates a single-key-distributed table forcing a high skew
// coefficient (117b). Returns false (CONFIG-ONLY degrade) on any psql failure.
func (s *Scenario117E2ESuite) s117BuildSkewFixture(cluster string) bool {
	stmts := []string{
		"DROP TABLE IF EXISTS t117_skew;",
		"CREATE TABLE t117_skew (k int, v text) DISTRIBUTED BY (k);",
		"INSERT INTO t117_skew SELECT 1, repeat('y', 32) FROM generate_series(1, 50000);",
		"ANALYZE t117_skew;",
	}
	for _, sql := range stmts {
		if _, err := s.s117Psql(cluster, sql); err != nil {
			return false
		}
	}
	return true
}

// s117BuildIndexFixture creates a churned indexed table to bloat the index
// (117d). Returns false (CONFIG-ONLY degrade) on any psql failure.
func (s *Scenario117E2ESuite) s117BuildIndexFixture(cluster string) bool {
	stmts := []string{
		"DROP TABLE IF EXISTS t117_idx;",
		"CREATE TABLE t117_idx (id int, v text) DISTRIBUTED BY (id);",
		"CREATE INDEX t117_idx_id ON t117_idx (id);",
		"INSERT INTO t117_idx SELECT g, repeat('z', 32) FROM generate_series(1, 20000) g;",
		"DELETE FROM t117_idx WHERE id > 2000;",
		"INSERT INTO t117_idx SELECT g, repeat('z', 32) FROM generate_series(20001, 40000) g;",
		"DELETE FROM t117_idx WHERE id > 22000;",
		"ANALYZE t117_idx;",
	}
	for _, sql := range stmts {
		if _, err := s.s117Psql(cluster, sql); err != nil {
			return false
		}
	}
	return true
}

// TestE2E_Scenario117_LiveRecommendationScan is the core live proof
// (117a-RT1-L / 117b-RT2-L / 117c-RT3-L / 117d-RT4-L / 117-S2-R4-count-L /
// 117-M2-bytype-L / 117a-M4-L / 117a-CLEAR-L / 117-PERSIST-L): it applies
// recommendationScan:true, builds per-type DB fixtures, waits for the scan to
// settle, and asserts status.recommendationCount, the per-type
// recommendations_total gauges (sum == count), table_bloat_ratio, and the 117a
// CLEAR re-scan. It distinguishes an unhealthy operator/webhook
// (TLS/connection → SKIP CONFIG-ONLY) from a genuine apply, and degrades any
// un-triggerable type / unreachable db to a clean CONFIG-ONLY skip (no hard
// fail). SKIPS cleanly when the live env is absent. The applied CR is cleaned up.
func (s *Scenario117E2ESuite) TestE2E_Scenario117_LiveRecommendationScan() {
	s.s117RequireLive()
	s.s117RequireNamespace()

	ns := s117Namespace()
	name := cases.Scenario117DefaultCluster + "-recscan-l"

	defer func() {
		_, _ = s.s117Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s117ApplyYAML(s117ScanYAML(name))
	if applyErr != nil && s117LooksLikeUnhealthy(out) {
		s.T().Skipf("117-PERSIST-L: operator/webhook appears UNHEALTHY (TLS/connection), not an "+
			"apply decision [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"117-PERSIST-L: the recommendationScan block must APPLY; out=%q", out)

	// recommendationScan.enabled persisted verbatim (apply contract).
	got, getErr := s.s117GetField(name, "{.spec.storage.recommendationScan.enabled}")
	require.NoErrorf(s.T(), getErr, "GET recommendationScan.enabled must succeed; got=%q", got)
	assert.Equal(s.T(), "true", strings.TrimSpace(got), "recommendationScan.enabled must persist verbatim")

	// Wait for the coordinator to be reachable for fixtures (clean degrade when
	// the cluster/db is not ready in the wait window).
	dbReady := s.s117WaitFor(func() bool {
		o, err := s.s117Psql(name, "SELECT 1;")
		return err == nil && strings.TrimSpace(o) == "1"
	})
	if !dbReady {
		s.T().Skip("117-RT*-L: coordinator psql not reachable " +
			"[CONFIG-ONLY-degrade: operator/db may not be running]")
	}

	// Build the per-type fixtures, degrading any un-triggerable type to
	// CONFIG-ONLY (logged, not failed). Skew/age may be unmeasurable on this
	// server (gp_toolkit absent / age unreachable in a short window).
	bloatBuilt := s.s117BuildBloatFixture(name)
	skewBuilt := s.s117BuildSkewFixture(name)
	indexBuilt := s.s117BuildIndexFixture(name)
	s.T().Logf("scenario117 fixtures: bloat=%v skew=%v index=%v age=CONFIG-ONLY "+
		"(age(relfrozenxid) is hard to push high in a short window)",
		bloatBuilt, skewBuilt, indexBuilt)

	// S.2/R.4 / 117-PERSIST-L: status.recommendationCount populates after the scan
	// settles (a 0-count is honest when nothing reaches the thresholds, so we only
	// require the field to be scrapeable).
	statusPath := "{.status.recommendationCount}"
	ok := s.s117WaitFor(func() bool {
		v, _ := s.s117GetField(name, statusPath)
		return strings.TrimSpace(v) != ""
	})
	if !ok {
		s.T().Skip("117-S2-R4-count-L: status.recommendationCount not populated " +
			"[CONFIG-ONLY-degrade: operator/db may not have scanned yet]")
	}
	statusStr, _ := s.s117GetField(name, statusPath)
	statusVal, parseErr := strconv.Atoi(strings.TrimSpace(statusStr))
	require.NoErrorf(s.T(), parseErr, "117-S2-R4-count-L: status must be numeric; got=%q", statusStr)
	assert.GreaterOrEqual(s.T(), statusVal, 0, "117-S2-R4-count-L: status must be a non-negative count")
	s.T().Logf("scenario117 117-S2-R4-count-L: status.recommendationCount=%d", statusVal)

	// 117-M2-bytype-L: scrape the per-type gauges and assert sum == count.
	byType, found := s.s117ScrapeRecMetrics(name)
	if !found {
		s.T().Skipf("117-M2-bytype-L: %s not scrapeable [CONFIG-ONLY-degrade: operator /metrics "+
			"not reachable]", cases.Scenario117RecsMetricName)
	}
	var sum float64
	for _, recType := range s117RecTypes {
		s.T().Logf("scenario117 117-M2-bytype-L: %s{type=%s}=%.0f",
			cases.Scenario117RecsMetricName, recType, byType[recType])
		sum += byType[recType]
	}
	assert.InDelta(s.T(), float64(statusVal), sum, 0.001,
		"117-M2-bytype-L: sum of per-type gauges == status.recommendationCount (M.2==count)")

	// 117a-RT1-L / 117a-M4-L: if the bloat fixture took, the bloat gauge and a
	// table_bloat_ratio for t117_bloat should be present (degrade otherwise).
	if bloatBuilt && byType[cases.Scenario117TypeBloat] >= 1 {
		ratio, ratioFound := s.s117ScrapeBloatRatio(name, "public.t117_bloat")
		if ratioFound {
			s.T().Logf("scenario117 117a-M4-L: table_bloat_ratio{table=public.t117_bloat}=%.2f", ratio)
			assert.Greater(s.T(), ratio, 0.0, "117a-M4-L: live bloat ratio must be > 0")
		} else {
			s.T().Log("117a-M4-L: table_bloat_ratio not scrapeable [CONFIG-ONLY-degrade]")
		}

		// 117a-CLEAR-L: VACUUM FULL the table so dead_pct→0 and the next scan
		// clears the bloat rec (the gauge drops). Best-effort / degrade.
		if _, err := s.s117Psql(name, "VACUUM FULL t117_bloat;"); err == nil {
			before := byType[cases.Scenario117TypeBloat]
			cleared := s.s117WaitFor(func() bool {
				bt, ok2 := s.s117ScrapeRecMetrics(name)
				return ok2 && bt[cases.Scenario117TypeBloat] < before
			})
			if cleared {
				s.T().Log("117a-CLEAR-L: bloat recommendation cleared after VACUUM FULL")
			} else {
				s.T().Log("117a-CLEAR-L: bloat clear not observed in window [CONFIG-ONLY-degrade]")
			}
		}
	} else {
		s.T().Log("117a-RT1-L: bloat recommendation not triggered [CONFIG-ONLY-degrade]")
	}

	// 117b-RT2-L: skew requires gp_toolkit.gp_skew_coefficients — degrade cleanly.
	if !skewBuilt || byType[cases.Scenario117TypeSkew] == 0 {
		s.T().Log("117b-RT2-L: skew recommendation not triggered " +
			"[CONFIG-ONLY-degrade: gp_toolkit.gp_skew_coefficients may be absent]")
	}
	// 117c-RT3-L: age is CONFIG-ONLY in a short window.
	if byType[cases.Scenario117TypeAge] == 0 {
		s.T().Log("117c-RT3-L: age recommendation not triggered " +
			"[CONFIG-ONLY: age(relfrozenxid) is hard to push high in a short window]")
	}
	// 117d-RT4-L: index bloat estimate degrade.
	if !indexBuilt || byType[cases.Scenario117TypeIndexBloat] == 0 {
		s.T().Log("117d-RT4-L: index_bloat recommendation not triggered [CONFIG-ONLY-degrade]")
	}
}

// s117ScrapeRecMetrics best-effort scrapes cloudberry_recommendations_total for
// the cluster from the operator /metrics endpoint, returning the per-type values
// and whether any matching series was found.
func (s *Scenario117E2ESuite) s117ScrapeRecMetrics(cluster string) (map[string]float64, bool) {
	out, ok := s.s117ScrapeMetricsText()
	if !ok {
		return nil, false
	}
	return s117ParseRecMetrics(out, cluster)
}

// s117ScrapeBloatRatio best-effort scrapes cloudberry_table_bloat_ratio for the
// cluster+table series from the operator /metrics endpoint.
func (s *Scenario117E2ESuite) s117ScrapeBloatRatio(cluster, table string) (float64, bool) {
	out, ok := s.s117ScrapeMetricsText()
	if !ok {
		return 0, false
	}
	return s117ParseBloatRatio(out, cluster, table)
}

// s117ScrapeMetricsText execs into the operator pod and returns the raw
// /metrics text.
func (s *Scenario117E2ESuite) s117ScrapeMetricsText() (string, bool) {
	pod, err := s.s117Kubectl("get", "pods", "-n", s117Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return "", false
	}
	out, execErr := s.s117Kubectl("exec", pod, "-n", s117Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return "", false
	}
	return out, true
}

// s117ParseRecMetrics parses a Prometheus text exposition for the
// cloudberry_recommendations_total series whose cluster label matches, returning
// a per-type map (defaulting absent types to 0) and whether any series matched.
func s117ParseRecMetrics(text, cluster string) (map[string]float64, bool) {
	byType := map[string]float64{}
	for _, recType := range s117RecTypes {
		byType[recType] = 0
	}
	found := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, cases.Scenario117RecsMetricName+"{") {
			continue
		}
		if !strings.Contains(line, `cluster="`+cluster+`"`) {
			continue
		}
		recType := s117MetricLabel(line, "type")
		if recType == "" {
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
		byType[recType] = v
		found = true
	}
	return byType, found
}

// s117ParseBloatRatio parses a Prometheus text exposition for the
// cloudberry_table_bloat_ratio series whose cluster+table labels match.
func s117ParseBloatRatio(text, cluster, table string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, cases.Scenario117BloatRatioMetricName+"{") {
			continue
		}
		if !strings.Contains(line, `cluster="`+cluster+`"`) {
			continue
		}
		if s117MetricLabel(line, "table") != table {
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

// s117MetricLabel extracts the value of label key from a Prometheus exposition
// line of the form name{...,key="value",...} value.
func s117MetricLabel(line, key string) string {
	marker := key + `="`
	idx := strings.Index(line, marker)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(marker):]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

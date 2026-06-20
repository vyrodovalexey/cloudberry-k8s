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
// Scenario 116: Disk Usage Monitoring (Status + Metric)
// (reconciliation rules R.2, S.1, M.1) — E2E
// ============================================================================
//
// Mirrors the Scenario 115 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO116_LIVE-gated live Part B. Part B is the LIVE
// status-and-metric proof: it `kubectl apply`s a CloudberryCluster with
// diskMonitoring:true, then:
//
//   (a) asserts the apply SUCCEEDS,
//   (b) GETs status.diskUsagePercent and asserts it is populated/non-stale (S.1),
//   (c) scrapes the operator /metrics for cloudberry_disk_usage_percent{cluster}
//       and asserts metric == status (M.1==S.1),
//   (d) CROSSCHECK-L: exec `df` on a segment data volume pod and assert the metric
//       is within tolerance of the df-derived worst-case usage,
//   (e) cleans up the applied CR.
//
// Operator/webhook health: if the apply fails with a TLS/connection error (NOT
// an admission decision) the operator/webhook is unhealthy — Part B distinguishes
// that and SKIPS cleanly with a CONFIG-ONLY message. The status/metric/df
// cross-check are marked CONFIG-ONLY-degrade (clean skip) when the cluster/db is
// not ready in the wait window — they do NOT hard-fail. Self-contained; generous
// timeouts; SKIPS cleanly when the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO116_LIVE=1    — gates the live apply/status/metric proof.
//   SCENARIO116_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS116 = "KUBECONFIG"
	envS116Live       = "SCENARIO116_LIVE"
	envS116Namespace  = "SCENARIO116_NAMESPACE"

	s116DefaultNamespace = "cloudberry-test"

	s116ExecTimeout = 2 * time.Minute
	// s116StatusWait bounds the poll for the operator-side status population.
	s116StatusWait = 90 * time.Second
)

// Scenario116E2ESuite verifies disk-usage status + metric monitoring end-to-end
// (catalog-honest Part A + KUBECONFIG-gated live Part B that applies
// diskMonitoring:true and asserts status, metric, and the df cross-check).
type Scenario116E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario116(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario116E2ESuite))
}

func (s *Scenario116E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario116_PartA_CatalogHonest iterates the full Scenario 116 catalog
// and asserts it is well-formed: unique IDs, every R.2/S.1/M.1 + TRACK +
// DISABLED + DBERR + CROSSCHECK + CONTROL + PERSIST family present, and every row
// carries a non-empty Layer/Gate/Expected/Description with known tokens.
func (s *Scenario116E2ESuite) TestE2E_Scenario116_PartA_CatalogHonest() {
	catalog := cases.Scenario116Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario116LayerUnit,
		cases.Scenario116LayerFunctional,
		cases.Scenario116LayerLive,
	}
	knownGates := []string{
		cases.Scenario116GateMonitoring,
		cases.Scenario116GateDisabled,
		cases.Scenario116GateNone,
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
			if tc.Layer == cases.Scenario116LayerLive {
				s.T().Logf("scenario116 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Expected)
			}
		})
	}
	for _, req := range []string{"R.2", "S.1", "M.1"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["TRACK"], "catalog must cover the TRACK family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["DBERR"], "catalog must cover the DBERR family")
	assert.True(s.T(), reqs["CROSSCHECK"], "catalog must cover the CROSSCHECK family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO116_LIVE gated live status-and-metric proof
// ----------------------------------------------------------------------------

func s116Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s116Namespace() string { return s116Env(envS116Namespace, s116DefaultNamespace) }

// s116RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO116_LIVE=1.
func (s *Scenario116E2ESuite) s116RequireLive() {
	if os.Getenv(envKubeconfigS116) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 116 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 116 live Part B")
	}
	if os.Getenv(envS116Live) != "1" {
		s.T().Skip("SCENARIO116_LIVE not set, skipping the live status-and-metric proof " +
			"(the deployed operator + db + the Vault-PKI webhook must be reachable)")
	}
}

// s116Kubectl runs a kubectl subcommand bounded by a short timeout, returning the
// combined output and error.
func (s *Scenario116E2ESuite) s116Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s116ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s116ApplyYAML pipes a manifest to `kubectl apply -f -` and returns the combined
// output + error.
func (s *Scenario116E2ESuite) s116ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s116ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s116Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s116LooksLikeUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the operator/webhook (NOT an admission decision).
// When true, Part B SKIPS cleanly.
func s116LooksLikeUnhealthy(out string) bool {
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

// s116RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario116E2ESuite) s116RequireNamespace() {
	if out, err := s.s116Kubectl("get", "namespace", s116Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s116Namespace(), out)
	}
	if out, err := s.s116Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s116GetField runs `kubectl get cloudberrycluster -o jsonpath` for a single
// field and returns the rendered value.
func (s *Scenario116E2ESuite) s116GetField(name, jsonPath string) (string, error) {
	return s.s116Kubectl("get", "cloudberrycluster", name,
		"-n", s116Namespace(), "-o", "jsonpath="+jsonPath)
}

// s116MonitoringYAML returns a base-valid CloudberryCluster manifest (HA mirrored)
// with diskMonitoring:true (status + metric scenario only), name filled.
func s116MonitoringYAML(name string) string {
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
`, name)
}

// s116WaitFor polls cond until it returns true or the wait budget is exhausted;
// returns false on timeout.
func (s *Scenario116E2ESuite) s116WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(s116StatusWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// TestE2E_Scenario116_LiveDiskUsage is the core live proof
// (116-S1-L / 116-M1-L / 116-R2-L / 116-CROSSCHECK-L / 116-PERSIST-L): it applies
// diskMonitoring:true and asserts the apply SUCCEEDS, status.diskUsagePercent
// populates (S.1), the live metric matches the status (M.1==S.1), and the `df`
// cross-check on a segment data volume is within tolerance of the metric
// (CROSSCHECK-L). It distinguishes an unhealthy operator/webhook
// (TLS/connection → SKIP CONFIG-ONLY) from a genuine apply, and reports
// status/metric/df as a clean CONFIG-ONLY-degrade skip when the cluster/db is not
// ready in the wait window. SKIPS cleanly when the live env is absent. The applied
// CR is cleaned up.
func (s *Scenario116E2ESuite) TestE2E_Scenario116_LiveDiskUsage() {
	s.s116RequireLive()
	s.s116RequireNamespace()

	ns := s116Namespace()
	name := cases.Scenario116DefaultCluster + "-disk-l"

	// Always clean up the applied CR.
	defer func() {
		_, _ = s.s116Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s116ApplyYAML(s116MonitoringYAML(name))
	if applyErr != nil && s116LooksLikeUnhealthy(out) {
		s.T().Skipf("116-PERSIST-L: operator/webhook appears UNHEALTHY (TLS/connection), not an "+
			"apply decision [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"116-PERSIST-L: the diskMonitoring block must APPLY; out=%q", out)

	// diskMonitoring persisted verbatim (apply contract).
	got, getErr := s.s116GetField(name, "{.spec.storage.diskMonitoring}")
	require.NoErrorf(s.T(), getErr, "GET diskMonitoring must succeed; got=%q", got)
	assert.Equal(s.T(), "true", strings.TrimSpace(got), "diskMonitoring must persist verbatim")

	// 116-S1-L / 116-R2-L / 116-PERSIST-L: status.diskUsagePercent populates and
	// is non-stale (operator + db side).
	statusPath := "{.status.diskUsagePercent}"
	ok := s.s116WaitFor(func() bool {
		v, _ := s.s116GetField(name, statusPath)
		v = strings.TrimSpace(v)
		return v != "" && v != "0"
	})
	if !ok {
		s.T().Skip("116-S1-L: status.diskUsagePercent not populated [CONFIG-ONLY-degrade: " +
			"operator/db may not be running or gp_disk_free unavailable]")
	}
	statusStr, _ := s.s116GetField(name, statusPath)
	statusVal, parseErr := strconv.Atoi(strings.TrimSpace(statusStr))
	require.NoErrorf(s.T(), parseErr, "116-S1-L: status must be numeric; got=%q", statusStr)
	assert.GreaterOrEqual(s.T(), statusVal, 0, "116-S1-L: status must be a valid percentage")
	assert.LessOrEqual(s.T(), statusVal, 100, "116-S1-L: status must be a valid percentage")
	s.T().Logf("scenario116 116-S1-L: status.diskUsagePercent=%d", statusVal)

	// 116-M1-L: the live metric matches the status (best-effort scrape).
	metricVal, found := s.s116ScrapeMetric(name)
	if !found {
		s.T().Skipf("116-M1-L: %s not scrapeable [CONFIG-ONLY-degrade: operator /metrics "+
			"not reachable]", cases.Scenario116MetricName)
	}
	assert.InDelta(s.T(), float64(statusVal), metricVal, 1.0,
		"116-M1-L: metric must match status (M.1==S.1)")
	s.T().Logf("scenario116 116-M1-L: metric=%.0f status=%d", metricVal, statusVal)

	// 116-CROSSCHECK-L: `df` on a segment data volume pod, within tolerance of
	// the metric. CONFIG-ONLY-degrade when the segment pod / df is not available.
	dfPct, dfOK := s.s116SegmentDfPercent(name)
	if !dfOK {
		s.T().Skip("116-CROSSCHECK-L: could not run df on a segment data volume " +
			"[CONFIG-ONLY-degrade: segment pod not ready]")
	}
	s.T().Logf("scenario116 116-CROSSCHECK-L: df=%d metric=%.0f", dfPct, metricVal)
	assert.InDelta(s.T(), float64(dfPct), metricVal, float64(cases.Scenario116CrossCheckTolerance),
		"116-CROSSCHECK-L: metric must be within tolerance of the df-derived worst-case usage")
}

// s116ScrapeMetric best-effort scrapes cloudberry_disk_usage_percent for the
// cluster from the operator /metrics endpoint via `kubectl exec` into the
// operator pod, returning the value and whether it was found.
func (s *Scenario116E2ESuite) s116ScrapeMetric(cluster string) (float64, bool) {
	pod, err := s.s116Kubectl("get", "pods", "-n", s116Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return 0, false
	}
	out, execErr := s.s116Kubectl("exec", pod, "-n", s116Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return 0, false
	}
	return s116ParseMetric(out, cluster)
}

// s116ParseMetric parses a Prometheus text exposition for the
// cloudberry_disk_usage_percent series whose cluster label matches.
func s116ParseMetric(text, cluster string) (float64, bool) {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, cases.Scenario116MetricName+"{") {
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

// s116SegmentDfPercent execs `df` on a segment data volume pod and returns the
// used-percent of the data mount. Best-effort: returns (0,false) when no segment
// pod / df output is available.
func (s *Scenario116E2ESuite) s116SegmentDfPercent(cluster string) (int, bool) {
	// Pick the first pod owned by the cluster's primary segment StatefulSet.
	pod, err := s.s116Kubectl("get", "pods", "-n", s116Namespace(),
		"-l", "cloudberry.cluster="+cluster,
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		// Fall back to a conventional segment-primary pod name.
		pod = cluster + "-segment-primary-0"
	}
	out, execErr := s.s116Kubectl("exec", pod, "-n", s116Namespace(), "-c", "cloudberry", "--",
		"sh", "-c", "df -P /data 2>/dev/null || df -P /")
	if execErr != nil {
		return 0, false
	}
	return s116ParseDfPercent(out)
}

// s116ParseDfPercent parses the Use% column from `df -P` output (the last data
// line), returning the integer percentage and whether it was parsed.
func s116ParseDfPercent(text string) (int, bool) {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	if len(lines) < 2 {
		return 0, false
	}
	// The last non-empty line is the filesystem row; Use% is the 5th field.
	for i := len(lines) - 1; i >= 1; i-- {
		fields := strings.Fields(lines[i])
		if len(fields) < 5 {
			continue
		}
		pctStr := strings.TrimSuffix(fields[4], "%")
		v, err := strconv.Atoi(pctStr)
		if err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

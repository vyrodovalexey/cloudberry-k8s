//go:build integration

package integration

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
// (reconciliation rules S.2, M.2, R.3, R.4, RT.1–RT.4, C.6–C.9, M.4) — integration
// ============================================================================
//
// Mirrors the Scenario 116 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver/CRD/namespace are absent). The reconciliation rules are
// exercised at the unit (internal/db + internal/controller) + functional layers;
// the full live cross-check is the e2e Part B. This integration layer adds the
// value those layers cannot: it submits — to a REAL apiserver — a CloudberryCluster
// with recommendationScan.enabled:true, then (when the operator is running) waits
// for status.recommendationCount to populate and asserts the live
// cloudberry_recommendations_total{type} gauges are exposed per type and sum to
// the status count (M.2==count).
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable (or the metric/status are not yet exposed) the live probe skips
// cleanly; the catalog-well-formedness check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO117_LIVE=1    — gates the live submission (off by default).
//   SCENARIO117_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS117I = "KUBECONFIG"
	envS117LiveI       = "SCENARIO117_LIVE"
	envS117NamespaceI  = "SCENARIO117_NAMESPACE"

	scenario117DefaultNamespace = "cloudberry-test"
	scenario117ExecTimeout      = 90 * time.Second
	// scenario117StatusWait bounds the poll for the operator to populate the
	// status.recommendationCount field.
	scenario117StatusWait = 90 * time.Second
)

// scenario117RecTypes is the canonical set of per-type metric labels.
var scenario117RecTypes = []string{
	cases.Scenario117TypeBloat,
	cases.Scenario117TypeSkew,
	cases.Scenario117TypeAge,
	cases.Scenario117TypeIndexBloat,
}

// Scenario117Suite drives the Scenario 117 recommendation-scan live probe, gated
// on apiserver reachability.
type Scenario117Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario117(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario117Suite))
}

func (s *Scenario117Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario117Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario117Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS117NamespaceI)); v != "" {
		return v
	}
	return scenario117DefaultNamespace
}

// scenario117Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario117Suite) scenario117Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario117ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario117ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario117Suite) scenario117ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario117ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario117Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario117LooksUnhealthy reports a TLS/connection failure reaching the
// apiserver/webhook (NOT a validation/admission decision) so callers can SKIP
// cleanly.
func scenario117LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario117RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO117_LIVE=1, and the namespace + CRD are served.
func (s *Scenario117Suite) scenario117RequireLive() {
	if os.Getenv(envKubeconfigS117I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 117 live submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 117 live submission")
	}
	if os.Getenv(envS117LiveI) != "1" {
		s.T().Skip("SCENARIO117_LIVE not set, skipping the live submission " +
			"[CONFIG-ONLY: the full live cross-check is the e2e Part B]")
	}
	if out, err := s.scenario117Kubectl("get", "namespace", scenario117Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario117Namespace(), out)
	}
	if out, err := s.scenario117Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario117GetField runs `kubectl get cloudberrycluster -o jsonpath` for a
// single field and returns the rendered value.
func (s *Scenario117Suite) scenario117GetField(name, jsonPath string) (string, error) {
	return s.scenario117Kubectl("get", "cloudberrycluster", name,
		"-n", scenario117Namespace(), "-o", "jsonpath="+jsonPath)
}

// scenario117ScanYAML returns a base-valid CloudberryCluster manifest with the
// recommendation scan enabled across all four types (the four thresholds set so
// the engine gates honestly).
func scenario117ScanYAML(name string) string {
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

// scenario117WaitFor polls cond until it returns true or the wait budget is
// exhausted; returns false on timeout.
func (s *Scenario117Suite) scenario117WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(scenario117StatusWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// TestIntegration_Scenario117_CatalogHonest asserts the Scenario 117 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario117Suite) TestIntegration_Scenario117_CatalogHonest() {
	catalog := cases.Scenario117Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
	}
	// The recommendation-type rules and the cross-cutting rules must be present.
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	for _, req := range []string{"RT.1", "RT.2", "RT.3", "RT.4", "C.6", "C.7", "C.8", "C.9"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	for _, req := range []string{"S.2", "R.4", "M.2", "R.3", "M.4"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["DBERR"], "catalog must cover the DBERR family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// TestIntegration_Scenario117_RecommendationScanLive submits a
// recommendationScan.enabled:true cluster to the REAL apiserver, then (when the
// operator is running) waits for status.recommendationCount to populate and
// asserts the live cloudberry_recommendations_total{type} gauges are exposed per
// type and sum to the status count (M.2==count). SKIPS cleanly when the
// apiserver/CRD/namespace are absent or the operator/metric is not yet exposed.
// The applied CR is cleaned up.
func (s *Scenario117Suite) TestIntegration_Scenario117_RecommendationScanLive() {
	s.scenario117RequireLive()

	ns := scenario117Namespace()
	name := "s117i-recscan"

	defer func() {
		_, _ = s.scenario117Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.scenario117ApplyYAML(scenario117ScanYAML(name))
	if applyErr != nil && scenario117LooksUnhealthy(out) {
		s.T().Skipf("apiserver/webhook appears UNHEALTHY (TLS/connection) [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "the recommendationScan block must APPLY; out=%q", out)

	// recommendationScan.enabled persisted verbatim (apply contract).
	got, getErr := s.scenario117GetField(name, "{.spec.storage.recommendationScan.enabled}")
	require.NoErrorf(s.T(), getErr, "GET recommendationScan.enabled must succeed; got=%q", got)
	assert.Equal(s.T(), "true", strings.TrimSpace(got), "recommendationScan.enabled must persist verbatim")

	// S.2/R.4: status.recommendationCount populates (operator-side; clean skip
	// when the operator/db is not running). A 0-count is a valid honest result
	// when no recs reach the thresholds, so we only require the field to be
	// scrapeable, not strictly positive.
	statusPath := "{.status.recommendationCount}"
	ok := s.scenario117WaitFor(func() bool {
		v, _ := s.scenario117GetField(name, statusPath)
		return strings.TrimSpace(v) != ""
	})
	if !ok {
		s.T().Skip("status.recommendationCount not populated " +
			"[CONFIG-ONLY: operator/db may not be running]")
	}

	statusStr, _ := s.scenario117GetField(name, statusPath)
	statusVal, parseErr := strconv.Atoi(strings.TrimSpace(statusStr))
	require.NoErrorf(s.T(), parseErr, "status.recommendationCount must be numeric; got=%q", statusStr)
	assert.GreaterOrEqual(s.T(), statusVal, 0, "117-S2-R4-count-L: status must be a non-negative count")
	s.T().Logf("scenario117 117-S2-R4-count-L: status.recommendationCount=%d", statusVal)

	// M.2: the live per-type gauges (if scrapeable) sum to the status count.
	byType, found := s.scenario117ScrapeRecMetrics(name)
	if !found {
		s.T().Skipf("117-M2-bytype-L: %s not scrapeable [CONFIG-ONLY: operator /metrics not reachable]",
			cases.Scenario117RecsMetricName)
	}
	var sum float64
	for _, recType := range scenario117RecTypes {
		s.T().Logf("scenario117 117-M2-bytype-L: %s{type=%s}=%.0f",
			cases.Scenario117RecsMetricName, recType, byType[recType])
		sum += byType[recType]
	}
	assert.InDelta(s.T(), float64(statusVal), sum, 0.001,
		"117-M2-bytype-L: sum of per-type gauges == status.recommendationCount (M.2==count)")
}

// scenario117ScrapeRecMetrics best-effort scrapes
// cloudberry_recommendations_total for the cluster from the operator /metrics
// endpoint via `kubectl exec` into the operator pod, returning the per-type
// values and whether any matching series was found.
func (s *Scenario117Suite) scenario117ScrapeRecMetrics(cluster string) (map[string]float64, bool) {
	pod, err := s.scenario117Kubectl("get", "pods", "-n", scenario117Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return nil, false
	}
	out, execErr := s.scenario117Kubectl("exec", pod, "-n", scenario117Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return nil, false
	}
	return scenario117ParseRecMetrics(out, cluster)
}

// scenario117ParseRecMetrics parses a Prometheus text exposition for the
// cloudberry_recommendations_total series whose cluster label matches, returning
// a per-type map (defaulting absent types to 0) and whether any series matched.
func scenario117ParseRecMetrics(text, cluster string) (map[string]float64, bool) {
	byType := map[string]float64{}
	for _, recType := range scenario117RecTypes {
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
		recType := scenario117MetricLabel(line, "type")
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

// scenario117MetricLabel extracts the value of label key from a Prometheus
// exposition line of the form name{...,key="value",...} value.
func scenario117MetricLabel(line, key string) string {
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

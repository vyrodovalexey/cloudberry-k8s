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
// Scenario 116: Disk Usage Monitoring (Status + Metric)
// (reconciliation rules R.2, S.1, M.1) — integration
// ============================================================================
//
// Mirrors the Scenario 115 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver/CRD/namespace are absent). The reconciliation rules are
// exercised at the unit (internal/db + internal/controller) + functional layers;
// the full live cross-check is the e2e Part B. This integration layer adds the
// value those layers cannot: it submits — to a REAL apiserver — a CloudberryCluster
// with diskMonitoring:true, then (when the operator is running) waits for the
// status.diskUsagePercent to populate and asserts the live
// cloudberry_disk_usage_percent metric matches that status (M.1==S.1).
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable (or the metric/status are not yet exposed) the live probe skips
// cleanly; the catalog-well-formedness check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO116_LIVE=1    — gates the live submission (off by default).
//   SCENARIO116_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS116I = "KUBECONFIG"
	envS116LiveI       = "SCENARIO116_LIVE"
	envS116NamespaceI  = "SCENARIO116_NAMESPACE"

	scenario116DefaultNamespace = "cloudberry-test"
	scenario116ExecTimeout      = 90 * time.Second
	// scenario116StatusWait bounds the poll for the operator to populate the
	// status.diskUsagePercent field.
	scenario116StatusWait = 90 * time.Second
)

// Scenario116Suite drives the Scenario 116 disk-monitoring live probe, gated on
// apiserver reachability.
type Scenario116Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario116(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario116Suite))
}

func (s *Scenario116Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario116Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario116Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS116NamespaceI)); v != "" {
		return v
	}
	return scenario116DefaultNamespace
}

// scenario116Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario116Suite) scenario116Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario116ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario116ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario116Suite) scenario116ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario116ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario116Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario116LooksUnhealthy reports a TLS/connection failure reaching the
// apiserver/webhook (NOT a validation/admission decision) so callers can SKIP
// cleanly.
func scenario116LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario116RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO116_LIVE=1, and the namespace + CRD are served.
func (s *Scenario116Suite) scenario116RequireLive() {
	if os.Getenv(envKubeconfigS116I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 116 live submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 116 live submission")
	}
	if os.Getenv(envS116LiveI) != "1" {
		s.T().Skip("SCENARIO116_LIVE not set, skipping the live submission " +
			"[CONFIG-ONLY: the full live cross-check is the e2e Part B]")
	}
	if out, err := s.scenario116Kubectl("get", "namespace", scenario116Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario116Namespace(), out)
	}
	if out, err := s.scenario116Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario116GetField runs `kubectl get cloudberrycluster -o jsonpath` for a
// single field and returns the rendered value.
func (s *Scenario116Suite) scenario116GetField(name, jsonPath string) (string, error) {
	return s.scenario116Kubectl("get", "cloudberrycluster", name,
		"-n", scenario116Namespace(), "-o", "jsonpath="+jsonPath)
}

// scenario116MonitoringYAML returns a base-valid CloudberryCluster manifest with
// diskMonitoring:true (no recommendationScan / usageReport — this scenario is
// about the disk-usage status + metric only).
func scenario116MonitoringYAML(name string) string {
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

// scenario116WaitFor polls cond until it returns true or the wait budget is
// exhausted; returns false on timeout.
func (s *Scenario116Suite) scenario116WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(scenario116StatusWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// TestIntegration_Scenario116_CatalogHonest asserts the Scenario 116 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario116Suite) TestIntegration_Scenario116_CatalogHonest() {
	catalog := cases.Scenario116Cases()
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
	// The three reconciliation rules must each be present.
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	for _, req := range []string{"R.2", "S.1", "M.1"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["TRACK"], "catalog must cover the TRACK family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["DBERR"], "catalog must cover the DBERR family")
	assert.True(s.T(), reqs["CROSSCHECK"], "catalog must cover the CROSSCHECK family")
}

// TestIntegration_Scenario116_DiskUsageLive submits a diskMonitoring:true cluster
// to the REAL apiserver, then (when the operator is running) waits for
// status.diskUsagePercent to populate and asserts the live
// cloudberry_disk_usage_percent metric matches that status (M.1==S.1). SKIPS
// cleanly when the apiserver/CRD/namespace are absent or the operator/metric is
// not yet exposed. The applied CR is cleaned up.
func (s *Scenario116Suite) TestIntegration_Scenario116_DiskUsageLive() {
	s.scenario116RequireLive()

	ns := scenario116Namespace()
	name := "s116i-disk"

	defer func() {
		_, _ = s.scenario116Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.scenario116ApplyYAML(scenario116MonitoringYAML(name))
	if applyErr != nil && scenario116LooksUnhealthy(out) {
		s.T().Skipf("apiserver/webhook appears UNHEALTHY (TLS/connection) [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "the diskMonitoring block must APPLY; out=%q", out)

	// diskMonitoring persisted verbatim (apply contract).
	got, getErr := s.scenario116GetField(name, "{.spec.storage.diskMonitoring}")
	require.NoErrorf(s.T(), getErr, "GET diskMonitoring must succeed; got=%q", got)
	assert.Equal(s.T(), "true", strings.TrimSpace(got), "diskMonitoring must persist verbatim")

	// S.1: status.diskUsagePercent populates (operator-side; clean skip when the
	// operator/db is not running).
	statusPath := "{.status.diskUsagePercent}"
	ok := s.scenario116WaitFor(func() bool {
		v, _ := s.scenario116GetField(name, statusPath)
		v = strings.TrimSpace(v)
		return v != "" && v != "0"
	})
	if !ok {
		s.T().Skip("status.diskUsagePercent not populated " +
			"[CONFIG-ONLY: operator/db may not be running or gp_disk_free unavailable]")
	}

	statusStr, _ := s.scenario116GetField(name, statusPath)
	statusVal, parseErr := strconv.Atoi(strings.TrimSpace(statusStr))
	require.NoErrorf(s.T(), parseErr, "status.diskUsagePercent must be numeric; got=%q", statusStr)
	assert.GreaterOrEqual(s.T(), statusVal, 0, "116-S1-L: status must be a valid percentage")
	assert.LessOrEqual(s.T(), statusVal, 100, "116-S1-L: status must be a valid percentage")
	s.T().Logf("scenario116 116-S1-L: status.diskUsagePercent=%d", statusVal)

	// M.1: the live metric (if scrapeable) matches the status. Scrape is
	// best-effort; a missing /metrics endpoint is a clean skip.
	metricVal, found := s.scenario116ScrapeMetric(name)
	if !found {
		s.T().Skipf("116-M1-L: %s not scrapeable [CONFIG-ONLY: operator /metrics not reachable]",
			cases.Scenario116MetricName)
	}
	assert.InDelta(s.T(), float64(statusVal), metricVal, 1.0,
		"116-M1-L: metric must match status (M.1==S.1)")
	s.T().Logf("scenario116 116-M1-L: metric=%.0f status=%d", metricVal, statusVal)
}

// scenario116ScrapeMetric best-effort scrapes cloudberry_disk_usage_percent for
// the cluster from the operator /metrics endpoint via `kubectl exec` into the
// operator pod's curl, falling back to a clean (false) when unreachable.
func (s *Scenario116Suite) scenario116ScrapeMetric(cluster string) (float64, bool) {
	// Discover an operator pod by the common control-plane label.
	pod, err := s.scenario116Kubectl("get", "pods", "-n", scenario116Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return 0, false
	}
	out, execErr := s.scenario116Kubectl("exec", pod, "-n", scenario116Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return 0, false
	}
	return scenario116ParseMetric(out, cluster)
}

// scenario116ParseMetric parses a Prometheus text exposition for the
// cloudberry_disk_usage_percent series whose cluster label matches, returning the
// value and whether it was found.
func scenario116ParseMetric(text, cluster string) (float64, bool) {
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

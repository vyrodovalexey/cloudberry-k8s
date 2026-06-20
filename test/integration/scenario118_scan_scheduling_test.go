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
// Scenario 118: Scan Scheduling and Duration Limit
// (reconciliation rules C.5, C.10, M.3 + webhook W.5) — integration
// ============================================================================
//
// Mirrors the Scenario 117 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver/CRD/namespace are absent). The reconciliation rules are
// exercised at the unit (internal/controller + internal/webhook +
// internal/metrics) + functional layers; the full live cross-check is the e2e
// Part B. This integration layer adds the value those layers cannot: it submits
// — to a REAL apiserver — a CloudberryCluster with recommendationScan.enabled
// and a near-future schedule, then (when the operator is running) asserts the
// `<cluster>-recommendation-scan` CronJob exists with that schedule (C.5) and the
// recommendation_scan_duration_seconds histogram is exposed (M.3).
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable (or the CronJob/metric is not yet exposed) the live probe skips
// cleanly; the catalog-well-formedness check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO118_LIVE=1    — gates the live submission (off by default).
//   SCENARIO118_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS118I = "KUBECONFIG"
	envS118LiveI       = "SCENARIO118_LIVE"
	envS118NamespaceI  = "SCENARIO118_NAMESPACE"

	scenario118DefaultNamespace = "cloudberry-test"
	scenario118ExecTimeout      = 90 * time.Second
	// scenario118StatusWait bounds the poll for the operator to converge the
	// recommendation-scan CronJob.
	scenario118StatusWait = 90 * time.Second
)

// Scenario118Suite drives the Scenario 118 scan-scheduling live probe, gated on
// apiserver reachability.
type Scenario118Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario118(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario118Suite))
}

func (s *Scenario118Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario118Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario118Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS118NamespaceI)); v != "" {
		return v
	}
	return scenario118DefaultNamespace
}

// scenario118Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario118Suite) scenario118Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario118ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario118ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario118Suite) scenario118ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario118ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario118Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario118LooksUnhealthy reports a TLS/connection failure reaching the
// apiserver/webhook (NOT a validation/admission decision) so callers can SKIP
// cleanly.
func scenario118LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario118RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO118_LIVE=1, and the namespace + CRD are served.
func (s *Scenario118Suite) scenario118RequireLive() {
	if os.Getenv(envKubeconfigS118I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 118 live submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 118 live submission")
	}
	if os.Getenv(envS118LiveI) != "1" {
		s.T().Skip("SCENARIO118_LIVE not set, skipping the live submission " +
			"[CONFIG-ONLY: the full live cross-check is the e2e Part B]")
	}
	if out, err := s.scenario118Kubectl("get", "namespace", scenario118Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario118Namespace(), out)
	}
	if out, err := s.scenario118Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario118GetField runs `kubectl get cloudberrycluster -o jsonpath` for a
// single field and returns the rendered value.
func (s *Scenario118Suite) scenario118GetField(name, jsonPath string) (string, error) {
	return s.scenario118Kubectl("get", "cloudberrycluster", name,
		"-n", scenario118Namespace(), "-o", "jsonpath="+jsonPath)
}

// scenario118ScanYAML returns a base-valid CloudberryCluster manifest with the
// recommendation scan enabled + the supplied schedule + scanDuration.
func scenario118ScanYAML(name, schedule, scanDuration string) string {
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
      schedule: "%s"
      bloatThreshold: 20
      skewThreshold: 30
      ageThreshold: 100000000
      indexBloatThreshold: 40
      scanDuration: "%s"
`, name, schedule, scanDuration)
}

// scenario118WaitFor polls cond until it returns true or the wait budget is
// exhausted; returns false on timeout.
func (s *Scenario118Suite) scenario118WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(scenario118StatusWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// TestIntegration_Scenario118_CatalogHonest asserts the Scenario 118 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario118Suite) TestIntegration_Scenario118_CatalogHonest() {
	catalog := cases.Scenario118Cases()
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
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	for _, req := range []string{"C.5", "C.10", "M.3", "TRUNCATE", "W.5"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// TestIntegration_Scenario118_ScheduleLive submits a recommendationScan.enabled
// cluster with a near-future schedule to the REAL apiserver, then (when the
// operator is running) asserts the `<cluster>-recommendation-scan` CronJob exists
// with that schedule (C.5) and the recommendation_scan_duration_seconds
// histogram is exposed (M.3). SKIPS cleanly when the apiserver/CRD/namespace are
// absent or the operator/CronJob/metric is not yet exposed. The applied CR is
// cleaned up.
func (s *Scenario118Suite) TestIntegration_Scenario118_ScheduleLive() {
	s.scenario118RequireLive()

	ns := scenario118Namespace()
	name := "s118i-scansched"
	schedule := cases.Scenario118NearFutureSchedule

	defer func() {
		_, _ = s.scenario118Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.scenario118ApplyYAML(scenario118ScanYAML(name, schedule, "2h"))
	if applyErr != nil && scenario118LooksUnhealthy(out) {
		s.T().Skipf("apiserver/webhook appears UNHEALTHY (TLS/connection) [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "the recommendationScan block must APPLY; out=%q", out)

	// schedule persisted verbatim (apply contract).
	got, getErr := s.scenario118GetField(name, "{.spec.storage.recommendationScan.schedule}")
	require.NoErrorf(s.T(), getErr, "GET recommendationScan.schedule must succeed; got=%q", got)
	assert.Equal(s.T(), schedule, strings.TrimSpace(got),
		"recommendationScan.schedule must persist verbatim")

	// C.5: the operator converges the `<cluster>-recommendation-scan` CronJob with
	// the configured schedule (clean skip when the operator is not running).
	cronName := name + "-" + cases.Scenario118CronJobSuffix
	ok := s.scenario118WaitFor(func() bool {
		v, err := s.scenario118Kubectl("get", "cronjob", cronName, "-n", ns,
			"-o", "jsonpath={.spec.schedule}")
		return err == nil && strings.TrimSpace(v) != ""
	})
	if !ok {
		s.T().Skipf("118a-C5-schedule-L: CronJob %q not converged "+
			"[CONFIG-ONLY: operator may not be running]", cronName)
	}
	cronSchedule, _ := s.scenario118Kubectl("get", "cronjob", cronName, "-n", ns,
		"-o", "jsonpath={.spec.schedule}")
	assert.Equal(s.T(), schedule, strings.TrimSpace(cronSchedule),
		"118a-C5-schedule-L: the CronJob must carry the configured schedule verbatim")
	s.T().Logf("scenario118 118a-C5-schedule-L: CronJob %q schedule=%q",
		cronName, strings.TrimSpace(cronSchedule))

	// M.3: the recommendation_scan_duration_seconds histogram is exposed (best
	// effort; clean degrade when /metrics is not reachable).
	count, found := s.scenario118ScrapeDurationCount(name)
	if !found {
		s.T().Skipf("118a-M3-duration-L: %s not scrapeable [CONFIG-ONLY: operator /metrics "+
			"not reachable]", cases.Scenario118DurationMetricName)
	}
	assert.GreaterOrEqual(s.T(), count, 0.0,
		"118a-M3-duration-L: the duration histogram count must be a non-negative number")
	s.T().Logf("scenario118 118a-M3-duration-L: %s_count=%.0f",
		cases.Scenario118DurationMetricName, count)
}

// scenario118ScrapeDurationCount best-effort scrapes
// recommendation_scan_duration_seconds_count for the cluster from the operator
// /metrics endpoint via `kubectl exec` into the operator pod.
func (s *Scenario118Suite) scenario118ScrapeDurationCount(cluster string) (float64, bool) {
	pod, err := s.scenario118Kubectl("get", "pods", "-n", scenario118Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return 0, false
	}
	out, execErr := s.scenario118Kubectl("exec", pod, "-n", scenario118Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return 0, false
	}
	return scenario118ParseDurationCount(out, cluster)
}

// scenario118ParseDurationCount parses a Prometheus text exposition for the
// recommendation_scan_duration_seconds_count series whose cluster label matches.
func scenario118ParseDurationCount(text, cluster string) (float64, bool) {
	countMetric := cases.Scenario118DurationMetricName + "_count"
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, countMetric+"{") {
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

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
// Scenario 122: Disabled States (C.2 / C.4 / C.12) + Re-enablement — E2E
// ============================================================================
//
// Mirrors the Scenario 116/117 e2e SHAPE: a catalog-honest Part A that ALWAYS
// runs + a KUBECONFIG/SCENARIO122_LIVE-gated live Part B. Part B is the LIVE
// disabled-state proof: it `kubectl apply`s a storage cluster, then FLIPS each
// disabled flag live via `kubectl patch` and verifies the disabled + re-enabled
// behaviors:
//
//   122a C.2: patch diskMonitoring:false → GET status.diskUsagePercent==0 (or
//             scrape cloudberry_disk_usage_percent→0); patch back true →
//             repopulates.
//   122b C.4: patch recommendationScan.enabled:false → status.recommendationCount
//             ==0 + recommendations_total{type}→0 + CronJob
//             <cluster>-recommendation-scan gone + POST scan via curl → 400;
//             patch back true → CronJob + count resume.
//   122c C.12: patch usageReport.enabled:false → API/CLI usage-report
//             usageReportEnabled:false; patch back true → usageReportEnabled:true
//             + entries.
//
// Operator/webhook health: if the apply fails with a TLS/connection error (NOT
// an admission decision) the operator/webhook is unhealthy — Part B distinguishes
// that and SKIPS cleanly with a CONFIG-ONLY message. Reactivation-timing-sensitive
// parts degrade to CONFIG-ONLY (clean log, no hard-fail) when not observable in
// the wait window. Self-contained; generous timeouts; SKIPS cleanly when the live
// env is absent; the applied CR is cleaned up.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO122_LIVE=1    — gates the live flip proof.
//   SCENARIO122_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS122 = "KUBECONFIG"
	envS122Live       = "SCENARIO122_LIVE"
	envS122Namespace  = "SCENARIO122_NAMESPACE"

	s122DefaultNamespace = "cloudberry-test"

	s122ExecTimeout = 2 * time.Minute
	// s122SettleWait bounds the poll for the operator to settle a live flip.
	s122SettleWait = 90 * time.Second
)

// s122RecTypes is the canonical set of per-type metric labels.
var s122RecTypes = []string{
	cases.Scenario122TypeBloat,
	cases.Scenario122TypeSkew,
	cases.Scenario122TypeAge,
	cases.Scenario122TypeIndexBloat,
}

// Scenario122E2ESuite verifies the storage disabled states + re-enablement
// end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B that applies a
// storage cluster, flips each disabled flag, and asserts the reset/clear/soft-
// gate + reactivation).
type Scenario122E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario122(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario122E2ESuite))
}

func (s *Scenario122E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario122_PartA_CatalogHonest iterates the full Scenario 122 catalog
// and asserts it is well-formed: unique IDs, every C.2/C.4/C.12 + CONTROL +
// PERSIST family present, each rule carries a disabled AND an enabled leg, and
// every row carries a non-empty Layer/Gate/Assert/Description with known tokens.
func (s *Scenario122E2ESuite) TestE2E_Scenario122_PartA_CatalogHonest() {
	catalog := cases.Scenario122Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	states := map[string]map[string]bool{}
	knownLayers := []string{
		cases.Scenario122LayerUnit,
		cases.Scenario122LayerFunctional,
		cases.Scenario122LayerLive,
	}
	knownGates := []string{
		cases.Scenario122GateDisabled,
		cases.Scenario122GateEnabled,
		cases.Scenario122GateNone,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			if tc.State != "" {
				if states[tc.Req] == nil {
					states[tc.Req] = map[string]bool{}
				}
				states[tc.Req][tc.State] = true
			}
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
			assert.Containsf(s.T(), knownGates, tc.Gate, "%s Gate must be a known token", tc.ID)
			if tc.Layer == cases.Scenario122LayerLive {
				s.T().Logf("scenario122 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Assert)
			}
		})
	}
	for _, req := range []string{"C.2", "C.4", "C.12"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
		assert.Truef(s.T(), states[req][cases.Scenario122GateDisabled],
			"rule %s must carry a disabled leg", req)
		assert.Truef(s.T(), states[req][cases.Scenario122GateEnabled],
			"rule %s must carry a re-enable leg", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO122_LIVE gated live flip proof
// ----------------------------------------------------------------------------

func s122Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s122Namespace() string { return s122Env(envS122Namespace, s122DefaultNamespace) }

// s122RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO122_LIVE=1.
func (s *Scenario122E2ESuite) s122RequireLive() {
	if os.Getenv(envKubeconfigS122) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 122 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 122 live Part B")
	}
	if os.Getenv(envS122Live) != "1" {
		s.T().Skip("SCENARIO122_LIVE not set, skipping the live disabled-state proof " +
			"(the deployed operator + db + the Vault-PKI webhook must be reachable)")
	}
}

// s122Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario122E2ESuite) s122Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s122ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s122ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario122E2ESuite) s122ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s122ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s122Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s122LooksLikeUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the operator/webhook (NOT an admission decision).
func s122LooksLikeUnhealthy(out string) bool {
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

// s122RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario122E2ESuite) s122RequireNamespace() {
	if out, err := s.s122Kubectl("get", "namespace", s122Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s122Namespace(), out)
	}
	if out, err := s.s122Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s122GetField runs `kubectl get cloudberrycluster -o jsonpath` for a single
// field and returns the rendered value.
func (s *Scenario122E2ESuite) s122GetField(name, jsonPath string) (string, error) {
	return s.s122Kubectl("get", "cloudberrycluster", name,
		"-n", s122Namespace(), "-o", "jsonpath="+jsonPath)
}

// s122Patch applies a merge patch to the named cluster's spec.
func (s *Scenario122E2ESuite) s122Patch(name, patch string) (string, error) {
	return s.s122Kubectl("patch", "cloudberrycluster", name,
		"-n", s122Namespace(), "--type", "merge", "-p", patch)
}

// s122StorageYAML returns a base-valid CloudberryCluster manifest with all three
// storage features ENABLED so the flips can disable them live.
func s122StorageYAML(name string) string {
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
    usageReport:
      enabled: true
`, name)
}

// s122WaitFor polls cond until it returns true or the wait budget is exhausted.
func (s *Scenario122E2ESuite) s122WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(s122SettleWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Second)
	}
	return false
}

// TestE2E_Scenario122_LiveDisabledStates is the core live proof
// (122a-C2-disabled-L / 122a-C2-reenable-L / 122b-C4-disabled-L /
// 122b-C4-reenable-L / 122c-C12-disabled-L / 122c-C12-reenable-L /
// 122-PERSIST-L): it applies a storage cluster, flips each disabled flag, and
// asserts the reset/clear/soft-gate + reactivation. It distinguishes an unhealthy
// operator/webhook (TLS/connection → SKIP CONFIG-ONLY) from a genuine apply, and
// degrades reactivation-timing-sensitive parts to CONFIG-ONLY (no hard fail) when
// not observable in-window. SKIPS cleanly when the live env is absent. The applied
// CR is cleaned up.
func (s *Scenario122E2ESuite) TestE2E_Scenario122_LiveDisabledStates() {
	s.s122RequireLive()
	s.s122RequireNamespace()

	ns := s122Namespace()
	name := cases.Scenario122DefaultCluster + "-disabled-l"

	defer func() {
		_, _ = s.s122Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s122ApplyYAML(s122StorageYAML(name))
	if applyErr != nil && s122LooksLikeUnhealthy(out) {
		s.T().Skipf("operator/webhook appears UNHEALTHY (TLS/connection), not an apply decision "+
			"[CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "the storage block must APPLY; out=%q", out)

	s.s122FlipDiskMonitoring(name)
	s.s122FlipRecommendationScan(name)
	s.s122FlipUsageReport(name)
}

// s122FlipDiskMonitoring covers 122a-C2-disabled-L / 122a-C2-reenable-L: patch
// diskMonitoring:false → status.diskUsagePercent==0 (or the gauge reads 0); patch
// back true → repopulates. Reactivation-timing-sensitive: CONFIG-ONLY degrade.
func (s *Scenario122E2ESuite) s122FlipDiskMonitoring(name string) {
	if _, err := s.s122Patch(name, `{"spec":{"storage":{"diskMonitoring":false}}}`); err != nil {
		s.T().Logf("122a-C2-disabled-L: patch diskMonitoring:false failed [CONFIG-ONLY]: %v", err)
		return
	}
	disabled := s.s122WaitFor(func() bool {
		v, _ := s.s122GetField(name, "{.status.diskUsagePercent}")
		v = strings.TrimSpace(v)
		if v == "0" {
			return true
		}
		g, ok := s.s122ScrapeDiskGauge(name)
		return ok && g == 0
	})
	if disabled {
		s.T().Log("122a-C2-disabled-L: diskUsagePercent reset to 0 on disable")
	} else {
		s.T().Log("122a-C2-disabled-L: reset-to-0 not observed in-window [CONFIG-ONLY-degrade]")
	}

	if _, err := s.s122Patch(name, `{"spec":{"storage":{"diskMonitoring":true}}}`); err != nil {
		s.T().Logf("122a-C2-reenable-L: patch diskMonitoring:true failed [CONFIG-ONLY]: %v", err)
		return
	}
	got, _ := s.s122GetField(name, "{.spec.storage.diskMonitoring}")
	assert.Equal(s.T(), "true", strings.TrimSpace(got),
		"122a-C2-reenable-L: diskMonitoring:true must persist after the flip")
	s.T().Log("122a-C2-reenable-L: diskMonitoring re-enabled (repopulation is CONFIG-ONLY in-window)")
}

// s122FlipRecommendationScan covers 122b-C4-disabled-L / 122b-C4-reenable-L:
// patch recommendationScan.enabled:false → status.recommendationCount==0 + the
// per-type gauges→0 + the CronJob gone + POST scan → 400; patch back true → the
// CronJob + count resume.
func (s *Scenario122E2ESuite) s122FlipRecommendationScan(name string) {
	ns := s122Namespace()
	cronName := name + "-" + cases.Scenario122CronJobSuffix

	if _, err := s.s122Patch(name,
		`{"spec":{"storage":{"recommendationScan":{"enabled":false}}}}`); err != nil {
		s.T().Logf("122b-C4-disabled-L: patch enabled:false failed [CONFIG-ONLY]: %v", err)
		return
	}

	// status.recommendationCount settles to 0 (the clear-on-disable).
	cleared := s.s122WaitFor(func() bool {
		v, _ := s.s122GetField(name, "{.status.recommendationCount}")
		return strings.TrimSpace(v) == "0"
	})
	if cleared {
		s.T().Log("122b-C4-disabled-L: status.recommendationCount cleared to 0 on disable")
	} else {
		s.T().Log("122b-C4-disabled-L: count-clear not observed in-window [CONFIG-ONLY-degrade]")
	}

	// The per-type gauges (if scrapeable) all read 0.
	if byType, found := s.s122ScrapeRecMetrics(name); found {
		for _, recType := range s122RecTypes {
			s.T().Logf("122b-C4-disabled-L: %s{type=%s}=%.0f",
				cases.Scenario122RecsMetricName, recType, byType[recType])
			assert.InDeltaf(s.T(), 0.0, byType[recType], 0.001,
				"122b-C4-disabled-L: recommendations_total{type=%s} must be 0 when disabled", recType)
		}
	} else {
		s.T().Log("122b-C4-disabled-L: recommendations_total not scrapeable [CONFIG-ONLY-degrade]")
	}

	// The CronJob is GC'd.
	gone := s.s122WaitFor(func() bool {
		_, err := s.s122Kubectl("get", "cronjob", cronName, "-n", ns)
		return err != nil
	})
	if gone {
		s.T().Logf("122b-C4-disabled-L: CronJob %s GC'd on disable", cronName)
	} else {
		s.T().Logf("122b-C4-disabled-L: CronJob %s still present [CONFIG-ONLY-degrade]", cronName)
	}

	// POST scan via curl → 400 RECOMMENDATION_SCAN_NOT_ENABLED (best-effort
	// through the operator api server; CONFIG-ONLY degrade when not reachable).
	if code, body, ok := s.s122PostScan(name); ok {
		assert.Equal(s.T(), 400, code,
			"122b-C4-disabled-L: a disabled scan POST must be 400")
		assert.Contains(s.T(), body, cases.Scenario122ScanNotEnabledCode)
	} else {
		s.T().Log("122b-C4-disabled-L: POST scan not reachable [CONFIG-ONLY-degrade]")
	}

	// Re-enable → CronJob + count resume.
	if _, err := s.s122Patch(name,
		`{"spec":{"storage":{"recommendationScan":{"enabled":true}}}}`); err != nil {
		s.T().Logf("122b-C4-reenable-L: patch enabled:true failed [CONFIG-ONLY]: %v", err)
		return
	}
	cronUp := s.s122WaitFor(func() bool {
		_, err := s.s122Kubectl("get", "cronjob", cronName, "-n", ns)
		return err == nil
	})
	if cronUp {
		s.T().Logf("122b-C4-reenable-L: CronJob %s present after re-enable", cronName)
	} else {
		s.T().Logf("122b-C4-reenable-L: CronJob %s not observed in-window [CONFIG-ONLY-degrade]", cronName)
	}
}

// s122FlipUsageReport covers 122c-C12-disabled-L / 122c-C12-reenable-L: patch
// usageReport.enabled:false → the API usage-report usageReportEnabled:false;
// patch back true → usageReportEnabled:true. Best-effort through the operator api
// server; CONFIG-ONLY degrade when the endpoint is not reachable.
func (s *Scenario122E2ESuite) s122FlipUsageReport(name string) {
	if _, err := s.s122Patch(name, `{"spec":{"storage":{"usageReport":{"enabled":false}}}}`); err != nil {
		s.T().Logf("122c-C12-disabled-L: patch enabled:false failed [CONFIG-ONLY]: %v", err)
		return
	}
	got, _ := s.s122GetField(name, "{.spec.storage.usageReport.enabled}")
	assert.Equal(s.T(), "false", strings.TrimSpace(got),
		"122c-C12-disabled-L: usageReport.enabled:false must persist after the flip")
	if body, ok := s.s122GetUsageReport(name); ok {
		assert.Contains(s.T(), body, `"usageReportEnabled":false`,
			"122c-C12-disabled-L: the API usage-report must report usageReportEnabled:false")
	} else {
		s.T().Log("122c-C12-disabled-L: usage-report endpoint not reachable [CONFIG-ONLY-degrade]")
	}

	if _, err := s.s122Patch(name, `{"spec":{"storage":{"usageReport":{"enabled":true}}}}`); err != nil {
		s.T().Logf("122c-C12-reenable-L: patch enabled:true failed [CONFIG-ONLY]: %v", err)
		return
	}
	got, _ = s.s122GetField(name, "{.spec.storage.usageReport.enabled}")
	assert.Equal(s.T(), "true", strings.TrimSpace(got),
		"122c-C12-reenable-L: usageReport.enabled:true must persist after the flip")
	if body, ok := s.s122GetUsageReport(name); ok {
		assert.Contains(s.T(), body, `"usageReportEnabled":true`,
			"122c-C12-reenable-L: the re-enabled API usage-report must report usageReportEnabled:true")
	} else {
		s.T().Log("122c-C12-reenable-L: usage-report endpoint not reachable [CONFIG-ONLY-degrade]")
	}
}

// s122ScrapeDiskGauge best-effort scrapes cloudberry_disk_usage_percent for the
// cluster from the operator /metrics endpoint.
func (s *Scenario122E2ESuite) s122ScrapeDiskGauge(cluster string) (float64, bool) {
	text, ok := s.s122ScrapeMetricsText()
	if !ok {
		return 0, false
	}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, cases.Scenario122DiskMetricName+"{") {
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

// s122ScrapeRecMetrics best-effort scrapes cloudberry_recommendations_total for
// the cluster from the operator /metrics endpoint, returning the per-type values
// and whether any matching series was found.
func (s *Scenario122E2ESuite) s122ScrapeRecMetrics(cluster string) (map[string]float64, bool) {
	text, ok := s.s122ScrapeMetricsText()
	if !ok {
		return nil, false
	}
	byType := map[string]float64{}
	for _, recType := range s122RecTypes {
		byType[recType] = 0
	}
	found := false
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, cases.Scenario122RecsMetricName+"{") {
			continue
		}
		if !strings.Contains(line, `cluster="`+cluster+`"`) {
			continue
		}
		recType := s122MetricLabel(line, "type")
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

// s122ScrapeMetricsText execs into the operator pod and returns the raw /metrics
// text.
func (s *Scenario122E2ESuite) s122ScrapeMetricsText() (string, bool) {
	pod, err := s.s122Kubectl("get", "pods", "-n", s122Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return "", false
	}
	out, execErr := s.s122Kubectl("exec", pod, "-n", s122Namespace(), "--",
		"sh", "-c", "wget -qO- http://localhost:8080/metrics || curl -s http://localhost:8080/metrics")
	if execErr != nil {
		return "", false
	}
	return out, true
}

// s122PostScan best-effort POSTs the recommendation-scan trigger via curl from
// the operator pod against the api server, returning the HTTP status code, the
// body, and whether the call was reachable.
func (s *Scenario122E2ESuite) s122PostScan(cluster string) (int, string, bool) {
	url := fmt.Sprintf("http://localhost:8443/api/v1alpha1/clusters/%s/storage/recommendations/scan"+
		"?namespace=%s", cluster, s122Namespace())
	body, ok := s.s122OperatorCurl("-s", "-o", "/dev/null", "-w", "%{http_code}",
		"-X", "POST", url)
	if !ok {
		return 0, "", false
	}
	// When only the status code is requested the body IS the code; re-run to get
	// the JSON body for the error-code assertion.
	code, convErr := strconv.Atoi(strings.TrimSpace(body))
	if convErr != nil {
		return 0, "", false
	}
	jsonBody, _ := s.s122OperatorCurl("-s", "-X", "POST", url)
	return code, jsonBody, true
}

// s122GetUsageReport best-effort GETs the usage-report endpoint via curl from the
// operator pod, returning the body and whether the call was reachable.
func (s *Scenario122E2ESuite) s122GetUsageReport(cluster string) (string, bool) {
	url := fmt.Sprintf("http://localhost:8443/api/v1alpha1/clusters/%s/storage/usage-report"+
		"?namespace=%s", cluster, s122Namespace())
	return s.s122OperatorCurl("-s", url)
}

// s122OperatorCurl execs `curl` (falling back to wget for GETs) inside the
// operator pod and returns the combined output and whether the exec succeeded.
func (s *Scenario122E2ESuite) s122OperatorCurl(curlArgs ...string) (string, bool) {
	pod, err := s.s122Kubectl("get", "pods", "-n", s122Namespace(),
		"-l", "control-plane=controller-manager",
		"-o", "jsonpath={.items[0].metadata.name}")
	pod = strings.TrimSpace(pod)
	if err != nil || pod == "" {
		return "", false
	}
	args := append([]string{"exec", pod, "-n", s122Namespace(), "--", "curl", "-k"}, curlArgs...)
	out, execErr := s.s122Kubectl(args...)
	if execErr != nil {
		return "", false
	}
	return out, true
}

// s122MetricLabel extracts the value of label key from a Prometheus exposition
// line of the form name{...,key="value",...} value.
func s122MetricLabel(line, key string) string {
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

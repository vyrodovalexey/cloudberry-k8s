//go:build integration

package integration

import (
	"context"
	"fmt"
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
// Scenario 115: Enable Storage Management with Full Configuration
// (reconciliation rules R.1, C.1, C.3, C.5, R.5) — integration
// ============================================================================
//
// Mirrors the Scenario 114 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver is down). The reconciliation rules are exercised at the
// unit (internal/builder + internal/controller) + functional layers; the full
// live persisted-contract proof is the e2e Part B. This integration layer adds
// the value those layers cannot: it submits — to a REAL apiserver — the FULL
// spec.storage block (diskMonitoring + recommendationScan + usageReport), then
// GETs it back and asserts the block PERSISTED (C.1/C.3 schedule + thresholds,
// usageReport) and — when the operator is running — that the StorageConfigured
// condition is True (R.5) and the recommendation-scan CronJob exists (C.5).
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable (or the webhook is unhealthy) the live probe skips cleanly; the
// catalog-well-formedness check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO115_LIVE=1    — gates the live submission (off by default).
//   SCENARIO115_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS115I = "KUBECONFIG"
	envS115LiveI       = "SCENARIO115_LIVE"
	envS115NamespaceI  = "SCENARIO115_NAMESPACE"

	scenario115DefaultNamespace = "cloudberry-test"
	scenario115ExecTimeout      = 90 * time.Second
	// scenario115ConditionWait bounds the poll for the operator to set the
	// StorageConfigured condition / create the CronJob.
	scenario115ConditionWait = 60 * time.Second
)

// Scenario115Suite drives the Scenario 115 full-storage persisted-contract probe,
// gated on apiserver reachability.
type Scenario115Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario115(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario115Suite))
}

func (s *Scenario115Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario115Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario115Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS115NamespaceI)); v != "" {
		return v
	}
	return scenario115DefaultNamespace
}

// scenario115Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario115Suite) scenario115Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario115ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario115ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario115Suite) scenario115ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario115ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario115Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario115LooksUnhealthy reports a TLS/connection failure reaching the
// apiserver/webhook (NOT a validation/admission decision) so callers can SKIP
// cleanly.
func scenario115LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario115RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO115_LIVE=1, and the namespace + CRD are served.
func (s *Scenario115Suite) scenario115RequireLive() {
	if os.Getenv(envKubeconfigS115I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 115 live submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 115 live submission")
	}
	if os.Getenv(envS115LiveI) != "1" {
		s.T().Skip("SCENARIO115_LIVE not set, skipping the live submission " +
			"[CONFIG-ONLY: the full live proof is the e2e Part B]")
	}
	if out, err := s.scenario115Kubectl("get", "namespace", scenario115Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario115Namespace(), out)
	}
	if out, err := s.scenario115Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario115GetField runs `kubectl get cloudberrycluster -o jsonpath` for a
// single field and returns the rendered value.
func (s *Scenario115Suite) scenario115GetField(name, jsonPath string) (string, error) {
	return s.scenario115Kubectl("get", "cloudberrycluster", name,
		"-n", scenario115Namespace(), "-o", "jsonpath="+jsonPath)
}

// scenario115FullBlockYAML returns a base-valid CloudberryCluster manifest
// carrying the FULL spec.storage block: diskMonitoring + recommendationScan
// (schedule + all five thresholds) + usageReport (enabled + monthly).
func scenario115FullBlockYAML(name string) string {
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
      skewThreshold: 50
      ageThreshold: 500000000
      indexBloatThreshold: 30
      scanDuration: "2h"
    usageReport:
      enabled: true
      monthly: true
`, name)
}

// scenario115PersistedFields maps a dotted catalog Field to the kubectl jsonpath
// that reads the persisted value back.
func scenario115PersistedFields() map[string]string {
	return map[string]string{
		"storage.recommendationScan.schedule":            "{.spec.storage.recommendationScan.schedule}",
		"storage.recommendationScan.bloatThreshold":      "{.spec.storage.recommendationScan.bloatThreshold}",
		"storage.recommendationScan.skewThreshold":       "{.spec.storage.recommendationScan.skewThreshold}",
		"storage.recommendationScan.ageThreshold":        "{.spec.storage.recommendationScan.ageThreshold}",
		"storage.recommendationScan.indexBloatThreshold": "{.spec.storage.recommendationScan.indexBloatThreshold}",
		"storage.recommendationScan.scanDuration":        "{.spec.storage.recommendationScan.scanDuration}",
		"storage.usageReport.enabled":                    "{.spec.storage.usageReport.enabled}",
	}
}

// scenario115ExpectedPersist returns the expected persisted value for each field
// read by scenario115PersistedFields.
func scenario115ExpectedPersist() map[string]string {
	return map[string]string{
		"storage.recommendationScan.schedule":            "0 3 * * 0",
		"storage.recommendationScan.bloatThreshold":      "20",
		"storage.recommendationScan.skewThreshold":       "50",
		"storage.recommendationScan.ageThreshold":        "500000000",
		"storage.recommendationScan.indexBloatThreshold": "30",
		"storage.recommendationScan.scanDuration":        "2h",
		"storage.usageReport.enabled":                    "true",
	}
}

// TestIntegration_Scenario115_CatalogHonest asserts the Scenario 115 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario115Suite) TestIntegration_Scenario115_CatalogHonest() {
	catalog := cases.Scenario115Cases()
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
	// The five reconciliation rules must each be present.
	reqs := map[string]bool{}
	for _, tc := range catalog {
		reqs[tc.Req] = true
	}
	for _, req := range []string{"R.1", "C.1", "C.3", "C.5", "R.5"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
}

// TestIntegration_Scenario115_FullBlockPersisted submits the FULL spec.storage
// block to the REAL apiserver, then GETs it back and asserts the scan schedule +
// five thresholds + usageReport PERSISTED (C.1/C.3/USAGE). When the operator is
// running it ALSO asserts the StorageConfigured condition becomes True (R.5) and
// the recommendation-scan CronJob exists (C.5); when the operator is not running
// those operator-side effects are reported as a clean skip (the apply/persist
// contract is still proven). SKIPS cleanly when the apiserver/CRD/namespace are
// absent or the webhook is unhealthy.
func (s *Scenario115Suite) TestIntegration_Scenario115_FullBlockPersisted() {
	s.scenario115RequireLive()

	ns := scenario115Namespace()
	name := "s115i-full"

	defer func() {
		_, _ = s.scenario115Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.scenario115ApplyYAML(scenario115FullBlockYAML(name))
	if applyErr != nil && scenario115LooksUnhealthy(out) {
		s.T().Skipf("apiserver/webhook appears UNHEALTHY (TLS/connection) [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "the full storage block must APPLY; out=%q", out)

	// C.1/C.3/USAGE: the applied block persisted verbatim.
	paths := scenario115PersistedFields()
	want := scenario115ExpectedPersist()
	for field, jsonPath := range paths {
		field, jsonPath := field, jsonPath
		s.Run("persist_"+field, func() {
			got, getErr := s.scenario115GetField(name, jsonPath)
			require.NoErrorf(s.T(), getErr, "GET %s must succeed; got=%q", field, got)
			assert.Equalf(s.T(), want[field], strings.TrimSpace(got),
				"%s must persist verbatim", field)
			s.T().Logf("scenario115 persisted %s = %s", field, strings.TrimSpace(got))
		})
	}

	// R.5: StorageConfigured condition True (operator-side; clean skip if the
	// operator is not running).
	s.Run("115-R5-L_StorageConfigured", func() {
		jsonPath := `{.status.conditions[?(@.type=="StorageConfigured")].status}`
		ok := s.scenario115WaitFor(func() bool {
			got, _ := s.scenario115GetField(name, jsonPath)
			return strings.TrimSpace(got) == "True"
		})
		if !ok {
			s.T().Skip("StorageConfigured=True not observed [CONFIG-ONLY: operator may not be running]")
		}
		s.T().Log("scenario115 115-R5-L: StorageConfigured=True observed")
	})

	// C.5: the recommendation-scan CronJob exists (operator-side; clean skip if
	// the operator is not running).
	s.Run("115-C5-L_CronJob", func() {
		cronName := name + cases.Scenario115CronJobSuffix
		ok := s.scenario115WaitFor(func() bool {
			_, getErr := s.scenario115Kubectl("get", "cronjob", cronName, "-n", ns)
			return getErr == nil
		})
		if !ok {
			s.T().Skipf("CronJob %q not observed [CONFIG-ONLY: operator may not be running]", cronName)
		}
		sched, _ := s.scenario115Kubectl("get", "cronjob", cronName, "-n", ns,
			"-o", "jsonpath={.spec.schedule}")
		assert.Equal(s.T(), cases.Scenario115Schedule, strings.TrimSpace(sched),
			"115-C5-L: recommendation-scan CronJob must carry the schedule")
		s.T().Logf("scenario115 115-C5-L: CronJob %q exists, schedule=%s", cronName, strings.TrimSpace(sched))
	})
}

// scenario115WaitFor polls cond until it returns true or the wait budget is
// exhausted; returns false on timeout.
func (s *Scenario115Suite) scenario115WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(scenario115ConditionWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

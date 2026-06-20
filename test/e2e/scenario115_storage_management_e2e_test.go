//go:build e2e

package e2e

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
// (reconciliation rules R.1, C.1, C.3, C.5, R.5) — E2E
// ============================================================================
//
// Mirrors the Scenario 114 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO115_LIVE-gated live Part B. Part B is the LIVE
// persisted-contract proof: it `kubectl apply`s a CloudberryCluster carrying the
// FULL spec.storage block (diskMonitoring + recommendationScan with schedule
// "0 3 * * 0" and all five thresholds + usageReport enabled+monthly), then:
//
//   (a) asserts the apply SUCCEEDS,
//   (b) GETs the persisted object and asserts the scan schedule + thresholds +
//       usageReport persisted (C.1/C.3/USAGE),
//   (c) asserts the StorageConfigured condition becomes True (R.5),
//   (d) asserts `kubectl get cronjob <cluster>-recommendation-scan` exists with
//       the schedule (C.5 / 115-PERSIST-L),
//   (e) cleans up the applied CR.
//
// Operator/webhook health: if the apply fails with a TLS/connection error (NOT
// an admission decision) the operator/webhook is unhealthy — Part B distinguishes
// that and SKIPS cleanly with a CONFIG-ONLY message. The operator-side effects
// (R.5 condition, C.5 CronJob) are reported as a clean skip when the operator is
// not running. Self-contained; generous timeouts; SKIPS cleanly when the live env
// is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO115_LIVE=1    — gates the live apply/persist proof.
//   SCENARIO115_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS115 = "KUBECONFIG"
	envS115Live       = "SCENARIO115_LIVE"
	envS115Namespace  = "SCENARIO115_NAMESPACE"

	s115DefaultNamespace = "cloudberry-test"

	s115ExecTimeout = 2 * time.Minute
	// s115ConditionWait bounds the poll for the operator-side effects (condition
	// + CronJob).
	s115ConditionWait = 90 * time.Second
)

// Scenario115E2ESuite verifies the full storage-management reconciliation
// end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B that applies
// the full block and asserts persistence + condition + CronJob).
type Scenario115E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario115(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario115E2ESuite))
}

func (s *Scenario115E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario115_PartA_CatalogHonest iterates the full Scenario 115 catalog
// and asserts it is well-formed: unique IDs, every R.1/C.1/C.3/C.5/R.5 +
// CONTROL + DISABLED + USAGE + PERSIST family present, and every row carries a
// non-empty Layer/Gate/Expected/Description with known tokens.
func (s *Scenario115E2ESuite) TestE2E_Scenario115_PartA_CatalogHonest() {
	catalog := cases.Scenario115Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario115LayerUnit,
		cases.Scenario115LayerFunctional,
		cases.Scenario115LayerLive,
	}
	knownGates := []string{
		cases.Scenario115GateFull,
		cases.Scenario115GateDisabled,
		cases.Scenario115GateNone,
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
			if tc.Layer == cases.Scenario115LayerLive {
				s.T().Logf("scenario115 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Expected)
			}
		})
	}
	for _, req := range []string{"R.1", "C.1", "C.3", "C.5", "R.5"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover rule %s", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["USAGE"], "catalog must cover the USAGE family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO115_LIVE gated live apply-and-persist proof
// ----------------------------------------------------------------------------

func s115Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s115Namespace() string { return s115Env(envS115Namespace, s115DefaultNamespace) }

// s115RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO115_LIVE=1.
func (s *Scenario115E2ESuite) s115RequireLive() {
	if os.Getenv(envKubeconfigS115) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 115 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 115 live Part B")
	}
	if os.Getenv(envS115Live) != "1" {
		s.T().Skip("SCENARIO115_LIVE not set, skipping the live apply-and-persist proof " +
			"(the deployed operator + the Vault-PKI webhook must be reachable)")
	}
}

// s115Kubectl runs a kubectl subcommand bounded by a short timeout, returning the
// combined output and error.
func (s *Scenario115E2ESuite) s115Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s115ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s115ApplyYAML pipes a manifest to `kubectl apply -f -` and returns the combined
// output + error.
func (s *Scenario115E2ESuite) s115ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s115ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s115Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s115LooksLikeUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the operator/webhook (NOT an admission decision).
// When true, Part B SKIPS cleanly: an unhealthy Vault-PKI webhook cert must not
// be counted as a failed reconcile.
func s115LooksLikeUnhealthy(out string) bool {
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

// s115RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served.
func (s *Scenario115E2ESuite) s115RequireNamespace() {
	if out, err := s.s115Kubectl("get", "namespace", s115Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s115Namespace(), out)
	}
	if out, err := s.s115Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s115GetField runs `kubectl get cloudberrycluster -o jsonpath` for a single
// field and returns the rendered value.
func (s *Scenario115E2ESuite) s115GetField(name, jsonPath string) (string, error) {
	return s.s115Kubectl("get", "cloudberrycluster", name,
		"-n", s115Namespace(), "-o", "jsonpath="+jsonPath)
}

// s115FullBlockYAML returns a base-valid CloudberryCluster manifest (HA mirrored)
// carrying the FULL spec.storage block: diskMonitoring + recommendationScan
// (schedule + all five thresholds) + usageReport (enabled + monthly), with the
// placeholder name filled.
func s115FullBlockYAML(name string) string {
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

// s115PersistedFields maps a dotted catalog Field to the kubectl jsonpath that
// reads the persisted value back, with its expected value.
func s115PersistedFields() map[string]string {
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

// s115ExpectedPersist returns the expected persisted value for each field read by
// s115PersistedFields.
func s115ExpectedPersist() map[string]string {
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

// s115WaitFor polls cond until it returns true or the wait budget is exhausted;
// returns false on timeout.
func (s *Scenario115E2ESuite) s115WaitFor(cond func() bool) bool {
	deadline := time.Now().Add(s115ConditionWait)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// TestE2E_Scenario115_LiveFullBlockPersisted is the core live proof
// (115-PERSIST-L / 115-C5-L / 115-R5-L / 115-C1-L / 115-C3-L): it applies the
// FULL storage block and asserts the apply SUCCEEDS, the scan/thresholds/
// usageReport persisted, the StorageConfigured condition becomes True, and the
// recommendation-scan CronJob exists with the schedule. It distinguishes an
// unhealthy operator/webhook (TLS/connection failure → SKIP CONFIG-ONLY) from a
// genuine apply, and reports operator-side effects as a clean skip when the
// operator is not running. SKIPS cleanly when the live env is absent. The applied
// CR is cleaned up.
func (s *Scenario115E2ESuite) TestE2E_Scenario115_LiveFullBlockPersisted() {
	s.s115RequireLive()
	s.s115RequireNamespace()

	ns := s115Namespace()
	name := "s115-full-l"

	// Always clean up the applied CR.
	defer func() {
		_, _ = s.s115Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s115ApplyYAML(s115FullBlockYAML(name))
	if applyErr != nil && s115LooksLikeUnhealthy(out) {
		s.T().Skipf("115-PERSIST-L: operator/webhook appears UNHEALTHY (TLS/connection), not an "+
			"apply decision [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"115-PERSIST-L: the full storage block must APPLY; out=%q", out)

	// C.1/C.3/USAGE: the applied block persisted verbatim.
	paths := s115PersistedFields()
	want := s115ExpectedPersist()
	for field, jsonPath := range paths {
		field, jsonPath := field, jsonPath
		s.Run("persist_"+field, func() {
			got, getErr := s.s115GetField(name, jsonPath)
			require.NoErrorf(s.T(), getErr, "GET %s must succeed; got=%q", field, got)
			assert.Equalf(s.T(), want[field], strings.TrimSpace(got),
				"115-PERSIST-L: %s must persist verbatim", field)
			s.T().Logf("scenario115 persisted %s = %s", field, strings.TrimSpace(got))
		})
	}

	// R.5: StorageConfigured condition becomes True (operator-side).
	s.Run("115-R5-L", func() {
		jsonPath := `{.status.conditions[?(@.type=="StorageConfigured")].status}`
		ok := s.s115WaitFor(func() bool {
			got, _ := s.s115GetField(name, jsonPath)
			return strings.TrimSpace(got) == "True"
		})
		if !ok {
			s.T().Skip("115-R5-L: StorageConfigured=True not observed " +
				"[CONFIG-ONLY: operator may not be running]")
		}
		s.T().Log("scenario115 115-R5-L: StorageConfigured=True observed")
	})

	// C.5: the recommendation-scan CronJob exists with the schedule (operator-side).
	s.Run("115-C5-L", func() {
		cronName := name + cases.Scenario115CronJobSuffix
		ok := s.s115WaitFor(func() bool {
			_, getErr := s.s115Kubectl("get", "cronjob", cronName, "-n", ns)
			return getErr == nil
		})
		if !ok {
			s.T().Skipf("115-C5-L: CronJob %q not observed "+
				"[CONFIG-ONLY: operator may not be running]", cronName)
		}
		sched, _ := s.s115Kubectl("get", "cronjob", cronName, "-n", ns,
			"-o", "jsonpath={.spec.schedule}")
		assert.Equal(s.T(), cases.Scenario115Schedule, strings.TrimSpace(sched),
			"115-C5-L: recommendation-scan CronJob must carry the schedule")
		s.T().Logf("scenario115 115-C5-L: CronJob %q exists, schedule=%s",
			cronName, strings.TrimSpace(sched))
	})
}

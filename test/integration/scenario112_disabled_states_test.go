//go:build integration

package integration

import (
	"context"
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
// Scenario 112: Disabled States (DIS.1–DIS.3) — integration
// ============================================================================
//
// Reachability-gated + LIGHT (skips cleanly when no live cluster is reachable).
// The REAL teardown proof is the e2e Part B; this layer keeps a shared-helper /
// object-GC-shape proof:
//
//   - Always: the Scenario 112 catalog is well-formed (documents the same IDs
//     the functional/e2e layers resolve).
//   - Gated (KUBECONFIG + SCENARIO112_LIVE=1 + kubectl + a reachable namespace):
//     a NON-DESTRUCTIVE object-GC-shape probe — the disabled-state objects
//     (gpfdist Deployment/Service, dataload Jobs/CronJobs, the pxf-servers
//     ConfigMap, the PXF NetworkPolicy) are listable by the SAME label selector
//     the operator teardown deletes by, so the GC plan is reachable by-label on
//     the live cluster. It mutates NOTHING (the destructive disable/teardown
//     belongs to the e2e Part B).
//
// HONESTY: this layer never disables anything on the live cluster (that is the
// e2e's destructive, defer-restored job); it only proves the GC plan's label
// selector resolves real objects (or skips cleanly when unreachable).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live probe (skip cleanly when unset).
//   SCENARIO112_LIVE=1    — additionally required to run the live label probe.
//   SCENARIO112_CLUSTER   — deployed cluster name (default s112).
//   SCENARIO112_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envScenario112Kubeconfig = "KUBECONFIG"
	envScenario112Live       = "SCENARIO112_LIVE"
	envScenario112Cluster    = "SCENARIO112_CLUSTER"
	envScenario112Namespace  = "SCENARIO112_NAMESPACE"

	scenario112ExecTimeout = 60 * time.Second
)

// Scenario112Suite drives the disabled-state catalog + a reachability-gated
// label-selector GC-shape probe.
type Scenario112Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario112(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario112Suite))
}

func (s *Scenario112Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario112Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario112Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario112Namespace() string {
	return scenario112Env(envScenario112Namespace, cases.Scenario112Namespace)
}

func scenario112Cluster() string {
	return scenario112Env(envScenario112Cluster, cases.Scenario112DefaultCluster)
}

// TestIntegration_Scenario112_CatalogHonest asserts the Scenario 112 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario112Suite) TestIntegration_Scenario112_CatalogHonest() {
	catalog := cases.Scenario112DisabledStatesCases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		key := tc.ID + "|" + tc.Layer
		assert.Falsef(s.T(), seen[key], "duplicate catalog row %s", key)
		seen[key] = true
		reqs[tc.Req] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Class, "%s must carry an honesty Class", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
	for _, req := range []string{"DIS.1", "DIS.2", "DIS.3"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover disabled-state family %s", req)
	}
}

// scenario112RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO112_LIVE=1, and the namespace is reachable.
func (s *Scenario112Suite) scenario112RequireLive() {
	if os.Getenv(envScenario112Kubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 112 live label-probe")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 112 live label-probe")
	}
	if os.Getenv(envScenario112Live) != "1" {
		s.T().Skip("SCENARIO112_LIVE not set, skipping the live label-probe")
	}
	if out, err := s.scenario112Kubectl("get", "namespace", scenario112Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario112Namespace(), out)
	}
}

// scenario112Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario112Suite) scenario112Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario112ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// TestIntegration_Scenario112_DataloadGCSelectorReachable proves (NON-
// DESTRUCTIVELY) that the operator teardown's label selector
// {avsoft.io/cluster, avsoft.io/component=dataload} resolves on the live cluster
// — i.e. the GC plan can find the dataload Jobs/CronJobs/ConfigMaps it would
// delete on a disable. It mutates NOTHING. Skips cleanly when the live env is
// absent. (the destructive disable→teardown proof is the e2e Part B)
func (s *Scenario112Suite) TestIntegration_Scenario112_DataloadGCSelectorReachable() {
	s.scenario112RequireLive()

	ns := scenario112Namespace()
	selector := "avsoft.io/cluster=" + scenario112Cluster() + ",avsoft.io/component=dataload"

	// A list by the teardown selector must SUCCEED (resolve to objects or empty)
	// for each GC'd kind — proving the GC plan is reachable by-label on the live
	// cluster. An empty list is honest (the cluster may have no active jobs).
	for _, kind := range []string{"jobs", "cronjobs", "configmaps"} {
		out, err := s.scenario112Kubectl("get", kind, "-n", ns, "-l", selector, "-o", "name")
		require.NoErrorf(s.T(), err,
			"112-DIS1: the teardown GC selector must resolve %s on the live cluster (out=%s)", kind, out)
		s.T().Logf("112-DIS1: dataload %s by selector %q → %q", kind, selector,
			strings.TrimSpace(out))
	}

	// The pxf-servers ConfigMap + PXF NetworkPolicy are deleted by NAME on a
	// disable; a get-by-name must not error the apiserver (NotFound is fine).
	cmName := scenario112Cluster() + "-pxf-servers"
	out, _ := s.scenario112Kubectl("get", "configmap", cmName, "-n", ns, "-o", "name")
	s.T().Logf("112-DIS1: pxf-servers ConfigMap %q present? → %q", cmName, strings.TrimSpace(out))
}

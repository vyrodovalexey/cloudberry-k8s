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
// Scenario 114: Mutating Webhook Defaults — storage-recommendation scan
// (D.1–D.6) — integration
// ============================================================================
//
// Mirrors the Scenario 113 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver is down). The defaulter rules are exercised at the unit
// (internal/webhook) + functional layers; the full live persisted-defaults proof
// is the e2e Part B. This integration layer adds the value those layers cannot:
// it submits — to a REAL apiserver — a MINIMAL enabled+omitted recommendation
// scan, then GETs it back and asserts the six webhook-injected defaults D.1–D.6
// are PERSISTED on the stored object (proving the server-side mutating webhook
// ran). The object is NOT pre-defaulted, so the persisted values can only come
// from the webhook.
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable (or the webhook is unhealthy) the live probe skips cleanly; the
// catalog-well-formedness check always runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO114_LIVE=1    — gates the live submission (off by default).
//   SCENARIO114_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS114I = "KUBECONFIG"
	envS114LiveI       = "SCENARIO114_LIVE"
	envS114NamespaceI  = "SCENARIO114_NAMESPACE"

	scenario114DefaultNamespace = "cloudberry-test"
	scenario114ExecTimeout      = 90 * time.Second
)

// Scenario114Suite drives the Scenario 114 webhook persisted-defaults probe,
// gated on apiserver reachability.
type Scenario114Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario114(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario114Suite))
}

func (s *Scenario114Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario114Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario114Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS114NamespaceI)); v != "" {
		return v
	}
	return scenario114DefaultNamespace
}

// scenario114Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario114Suite) scenario114Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario114ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario114ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario114Suite) scenario114ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario114ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario114Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario114LooksUnhealthy reports a TLS/connection failure reaching the webhook
// (NOT a validation/admission decision) so callers can SKIP cleanly.
func scenario114LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario114RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO114_LIVE=1, and the namespace + CRD are served.
func (s *Scenario114Suite) scenario114RequireLive() {
	if os.Getenv(envKubeconfigS114I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 114 live webhook submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 114 live webhook submission")
	}
	if os.Getenv(envS114LiveI) != "1" {
		s.T().Skip("SCENARIO114_LIVE not set, skipping the live webhook submission " +
			"[CONFIG-ONLY: the full live proof is the e2e Part B]")
	}
	if out, err := s.scenario114Kubectl("get", "namespace", scenario114Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario114Namespace(), out)
	}
	if out, err := s.scenario114Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario114GetField runs `kubectl get -o jsonpath` for a single
// recommendation-scan field and returns the rendered value.
func (s *Scenario114Suite) scenario114GetField(name, jsonPath string) (string, error) {
	return s.scenario114Kubectl("get", "cloudberrycluster", name,
		"-n", scenario114Namespace(), "-o", "jsonpath="+jsonPath)
}

// scenario114MinimalEnabledYAML returns a MINIMAL but valid CloudberryCluster
// manifest whose recommendation scan is ENABLED with ALL six defaulted fields
// OMITTED, so the server-side mutating webhook must inject D.1–D.6. The object is
// intentionally NOT pre-defaulted.
func scenario114MinimalEnabledYAML(name string) string {
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
`, name)
}

// scenario114LiveFieldPaths maps each per-D catalog Field to the kubectl
// jsonpath that reads it back from the persisted object.
func scenario114LiveFieldPaths() map[string]string {
	return map[string]string{
		"storage.recommendationScan.schedule":            "{.spec.storage.recommendationScan.schedule}",
		"storage.recommendationScan.bloatThreshold":      "{.spec.storage.recommendationScan.bloatThreshold}",
		"storage.recommendationScan.skewThreshold":       "{.spec.storage.recommendationScan.skewThreshold}",
		"storage.recommendationScan.ageThreshold":        "{.spec.storage.recommendationScan.ageThreshold}",
		"storage.recommendationScan.indexBloatThreshold": "{.spec.storage.recommendationScan.indexBloatThreshold}",
		"storage.recommendationScan.scanDuration":        "{.spec.storage.recommendationScan.scanDuration}",
	}
}

// TestIntegration_Scenario114_CatalogHonest asserts the Scenario 114 catalog is
// well-formed (always runs; no infra) so the integration layer documents the same
// IDs the functional/e2e layers resolve.
func (s *Scenario114Suite) TestIntegration_Scenario114_CatalogHonest() {
	catalog := cases.Scenario114Cases()
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
}

// TestIntegration_Scenario114_WebhookDefaultsPersisted submits a MINIMAL enabled+
// omitted recommendation scan to the REAL apiserver, then GETs each live (-L)
// per-D field back and asserts the webhook-injected default D.1–D.6 PERSISTED.
// Since the object is not pre-defaulted, a persisted default can only come from
// the deployed mutating webhook — the unique value this layer adds over the
// defaulter-direct unit/functional tests. SKIPS cleanly when the apiserver/CRD/
// namespace are absent or the webhook is unhealthy.
func (s *Scenario114Suite) TestIntegration_Scenario114_WebhookDefaultsPersisted() {
	s.scenario114RequireLive()

	ns := scenario114Namespace()
	name := "s114i-defaults"

	// Index the live per-D rows by Field for expected-value lookup.
	rows := map[string]cases.Scenario114Case{}
	for _, c := range cases.Scenario114Cases() {
		if c.Layer == cases.Scenario114LayerLive && c.Field != "" {
			rows[c.Field] = c
		}
	}
	require.NotEmpty(s.T(), rows, "catalog must enumerate live per-D rows")

	defer func() {
		_, _ = s.scenario114Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.scenario114ApplyYAML(scenario114MinimalEnabledYAML(name))
	if applyErr != nil && scenario114LooksUnhealthy(out) {
		s.T().Skipf("webhook/apiserver appears UNHEALTHY (TLS/connection) [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr, "minimal enabled scan must APPLY; out=%q", out)

	paths := scenario114LiveFieldPaths()
	for field, row := range rows {
		field, row := field, row
		s.Run(row.ID, func() {
			jsonPath, ok := paths[field]
			require.Truef(s.T(), ok, "no jsonpath wired for %s", field)

			got, getErr := s.scenario114GetField(name, jsonPath)
			require.NoErrorf(s.T(), getErr, "%s: GET %s must succeed; got=%q", row.ID, field, got)
			assert.Equalf(s.T(), row.ExpectedValue, strings.TrimSpace(got),
				"%s (%s): webhook must persist the default value", row.ID, field)
			s.T().Logf("scenario114 %s (gate=%s): persisted %s = %s", row.ID, row.Gate, field, got)
		})
	}
}

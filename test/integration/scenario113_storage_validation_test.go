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
// Scenario 113: Validation Rules (Negative Tests) — storage-recommendation
// thresholds (W.1–W.4) — integration
// ============================================================================
//
// Mirrors the Scenario 110 integration SHAPE (reachability-gated; SKIPS CLEANLY
// when the apiserver is down). The validator-direct rules are exercised at the
// unit (internal/webhook) + functional layers; the full live reject matrix +
// no-persist proof is the e2e Part B. This integration layer adds the value those
// layers cannot: it submits — to a REAL apiserver — each WEBHOOK-sourced reject
// case and asserts (a) the apply is DENIED by OUR descriptive webhook message and
// (b) the CR does NOT persist (a follow-up GET is NotFound).
//
// Since ALL four Scenario 113 rules are webhook-authoritative (no CRD enum/Min/
// Max markers on the threshold fields), the rejection always comes from the
// deployed Vault-PKI webhook. This layer distinguishes an unhealthy webhook
// (TLS / connection failure) from a genuine validation denial and SKIPS cleanly
// when the apiserver/CRD/namespace are absent.
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable the suite skips cleanly; the catalog-well-formedness check always
// runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO113_LIVE=1    — gates the live submission (off by default).
//   SCENARIO113_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS113I = "KUBECONFIG"
	envS113LiveI       = "SCENARIO113_LIVE"
	envS113NamespaceI  = "SCENARIO113_NAMESPACE"

	scenario113DefaultNamespace = "cloudberry-test"
	scenario113ExecTimeout      = 90 * time.Second
)

// Scenario113Suite drives the Scenario 113 webhook rejection probe, gated on
// apiserver reachability.
type Scenario113Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario113(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario113Suite))
}

func (s *Scenario113Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario113Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario113Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS113NamespaceI)); v != "" {
		return v
	}
	return scenario113DefaultNamespace
}

// scenario113Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario113Suite) scenario113Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario113ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario113ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario113Suite) scenario113ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario113ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario113Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario113LooksUnhealthy reports a TLS/connection failure reaching the webhook
// (NOT a validation denial) so callers can SKIP cleanly.
func scenario113LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario113RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO113_LIVE=1, and the namespace + CRD are served.
func (s *Scenario113Suite) scenario113RequireLive() {
	if os.Getenv(envKubeconfigS113I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 113 live webhook submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 113 live webhook submission")
	}
	if os.Getenv(envS113LiveI) != "1" {
		s.T().Skip("SCENARIO113_LIVE not set, skipping the live webhook submission " +
			"[CONFIG-ONLY: the full live matrix is the e2e Part B]")
	}
	if out, err := s.scenario113Kubectl("get", "namespace", scenario113Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario113Namespace(), out)
	}
	if out, err := s.scenario113Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario113CRExists reports whether a CloudberryCluster with the given name
// exists in the deploy namespace (used for the no-persist GET).
func (s *Scenario113Suite) scenario113CRExists(name string) bool {
	_, err := s.scenario113Kubectl("get", "cloudberrycluster", name,
		"-n", scenario113Namespace())
	return err == nil
}

// scenario113ValidBaseYAML returns a base-valid CloudberryCluster manifest with
// the recommendation scan ENABLED and all four thresholds in range. Each reject
// case injects EXACTLY ONE out-of-range threshold via a targeted string swap.
func scenario113ValidBaseYAML(name string) string {
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
      indexBloatThreshold: 30
      ageThreshold: 500000000
`, name)
}

// scenario113NegativeManifests returns, for each Scenario 113 live (-L) reject
// rule ID, a base-valid manifest mutated to carry EXACTLY ONE out-of-range
// threshold. The mutation is a targeted string swap on the base YAML; the
// resulting manifest matches the catalog's OffendingField.
func (s *Scenario113Suite) scenario113NegativeManifests(name string) map[string]string {
	base := scenario113ValidBaseYAML(name)
	return map[string]string{
		// W.1 — bloatThreshold above the upper bound (WEBHOOK).
		"113-W1-150-L": strings.Replace(base, "bloatThreshold: 20", "bloatThreshold: 150", 1),
		// W.1 — bloatThreshold below the lower bound (WEBHOOK).
		"113-W1-neg1-L": strings.Replace(base, "bloatThreshold: 20", "bloatThreshold: -1", 1),
		// W.2 — skewThreshold above the upper bound (WEBHOOK).
		"113-W2-L": strings.Replace(base, "skewThreshold: 50", "skewThreshold: 101", 1),
		// W.3 — indexBloatThreshold above the upper bound (WEBHOOK).
		"113-W3-L": strings.Replace(base, "indexBloatThreshold: 30", "indexBloatThreshold: 200", 1),
		// W.4 — ageThreshold negative (WEBHOOK).
		"113-W4-L": strings.Replace(base, "ageThreshold: 500000000", "ageThreshold: -5", 1),
	}
}

// TestIntegration_Scenario113_CatalogHonest asserts the Scenario 113 catalog is
// well-formed (always runs; no infra) so the integration layer documents the same
// IDs the functional/e2e layers resolve.
func (s *Scenario113Suite) TestIntegration_Scenario113_CatalogHonest() {
	catalog := cases.Scenario113Cases()
	require.NotEmpty(s.T(), catalog)
	seen := map[string]bool{}
	for _, tc := range catalog {
		assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
		seen[tc.ID] = true
		assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
		assert.NotEmptyf(s.T(), tc.Source, "%s must carry a Source", tc.ID)
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
	}
}

// TestIntegration_Scenario113_WebhookRejection submits the WEBHOOK-sourced reject
// cases (W.1–W.4) to the REAL apiserver and asserts each apply is DENIED with OUR
// descriptive webhook message AND the CR does NOT persist (a follow-up GET is
// NotFound). Since these rules are webhook-authoritative, this is the unique value
// this layer adds over the validator-direct unit/functional tests. SKIPS cleanly
// when the apiserver/CRD/namespace are absent or the webhook is unhealthy.
func (s *Scenario113Suite) TestIntegration_Scenario113_WebhookRejection() {
	s.scenario113RequireLive()

	ns := scenario113Namespace()

	// Index the live reject rows by ID for substring lookup.
	rows := map[string]cases.Scenario113Case{}
	for _, c := range cases.Scenario113Cases() {
		if c.Layer != cases.Scenario113LayerLive {
			continue
		}
		if c.Req == "CONTROL" || c.Req == "NOPERSIST" {
			continue
		}
		rows[c.ID] = c
	}
	require.NotEmpty(s.T(), rows, "catalog must enumerate live reject rows")

	for id, row := range rows {
		id, row := id, row
		s.Run(id, func() {
			name := "s113i-" + strings.ToLower(strings.TrimPrefix(id, "113-"))
			name = strings.ReplaceAll(name, ".", "-")

			manifests := s.scenario113NegativeManifests(name)
			manifest, ok := manifests[id]
			require.Truef(s.T(), ok, "no manifest wired for %s", id)

			defer func() {
				_, _ = s.scenario113Kubectl("delete", "cloudberrycluster", name, "-n", ns,
					"--ignore-not-found", "--wait=false")
			}()

			out, applyErr := s.scenario113ApplyYAML(manifest)
			if applyErr != nil && scenario113LooksUnhealthy(out) {
				s.T().Skipf("%s: webhook/apiserver appears UNHEALTHY (TLS/connection) "+
					"[CONFIG-ONLY]: %s", id, out)
			}

			require.Errorf(s.T(), applyErr, "%s: apply must be DENIED by the webhook; out=%q",
				id, out)
			for _, substr := range row.ErrorSubstrings {
				assert.Containsf(s.T(), out, substr,
					"%s (source=%s): webhook rejection must contain %q; got %q",
					id, row.Source, substr, out)
			}
			assert.Falsef(s.T(), s.scenario113CRExists(name),
				"%s: webhook-rejected CR %q must NOT persist", id, name)
			s.T().Logf("scenario113 %s (source=%s): webhook reject + no-persist OK",
				id, row.Source)
		})
	}
}

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
// Scenario 113: Validation Rules (Negative Tests) — storage-recommendation
// thresholds (W.1–W.4) — E2E
// ============================================================================
//
// Mirrors the Scenario 110 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO113_LIVE-gated live Part B. Part B is the COMPLETE,
// systematic LIVE reject matrix: for EACH W.x it builds a base-valid
// CloudberryCluster YAML (recommendationScan ENABLED, in-range thresholds) with
// EXACTLY ONE out-of-range threshold, `kubectl apply`s it, and asserts:
//
//   (a) apply FAILS (non-zero exit / admission denied),
//   (b) the stderr contains OUR descriptive webhook error — all four rules are
//       WEBHOOK-sourced (no CRD enum/Min/Max), so the user always sees our message
//       (e.g. "storage.recommendationScan.bloatThreshold" + the bad value),
//   (c) NO-PERSIST: a follow-up `kubectl get cloudberrycluster <name>` returns
//       NotFound (the rejected CR did not persist).
//
//   113-CONTROL-admit: a fully-valid enabled-scan CR APPLIES successfully (then is
//   deleted) — proving the webhook is not rejecting everything (no false-positive).
//
// Vault-PKI webhook cert health: if an apply fails with a TLS/connection error
// (NOT a validation error) the webhook is unhealthy — Part B distinguishes that
// and SKIPS cleanly with a CONFIG-ONLY message (it does NOT count an unhealthy
// webhook as a validation denial). Self-contained; the CONTROL CR is cleaned up;
// generous timeouts; SKIPS cleanly when KUBECONFIG/the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO113_LIVE=1    — gates the live apply matrix.
//   SCENARIO113_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS113 = "KUBECONFIG"
	envS113Live       = "SCENARIO113_LIVE"
	envS113Namespace  = "SCENARIO113_NAMESPACE"

	s113DefaultNamespace = "cloudberry-test"

	s113ExecTimeout = 2 * time.Minute
)

// Scenario113E2ESuite verifies the storage-recommendation webhook validation
// reject matrix end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B
// that applies each invalid CR and asserts reject + no-persist).
type Scenario113E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario113(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario113E2ESuite))
}

func (s *Scenario113E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario113_PartA_CatalogHonest iterates the full Scenario 113 catalog
// and asserts it is well-formed: unique IDs, every W.1–W.4 + BOUNDARY + CONTROL +
// NOPERSIST family present, every row carries a non-empty Layer/Source/Expected/
// Description with known tokens, and every -L reject row carries the NoPersist
// contract.
//
//nolint:gocyclo // a single catalog-well-formedness walk.
func (s *Scenario113E2ESuite) TestE2E_Scenario113_PartA_CatalogHonest() {
	catalog := cases.Scenario113Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario113LayerUnit,
		cases.Scenario113LayerFunctional,
		cases.Scenario113LayerLive,
	}
	knownSources := []string{
		cases.Scenario113SourceWebhook,
		cases.Scenario113SourceNone,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)
			assert.Containsf(s.T(), knownSources, tc.Source, "%s Source must be a known token", tc.ID)

			// Reject rows (not BOUNDARY/CONTROL/NOPERSIST) must carry substrings.
			if tc.Req != "BOUNDARY" && tc.Req != "CONTROL" && tc.Req != "NOPERSIST" {
				assert.NotEmptyf(s.T(), tc.ErrorSubstrings,
					"%s must carry descriptive error substrings", tc.ID)
			}
			// Every live reject row carries the no-persist contract.
			if tc.Layer == cases.Scenario113LayerLive &&
				tc.Req != "CONTROL" && tc.Req != "NOPERSIST" {
				assert.Truef(s.T(), tc.NoPersist, "%s (live reject) must carry NoPersist", tc.ID)
				s.T().Logf("scenario113 %s (%s, source=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Source, tc.Expected)
			}
		})
	}
	for i := 1; i <= 4; i++ {
		req := fmt.Sprintf("W.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover rule family %s", req)
	}
	assert.True(s.T(), reqs["BOUNDARY"], "catalog must cover the BOUNDARY family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["NOPERSIST"], "catalog must cover the NOPERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO113_LIVE gated live apply-and-reject matrix
// ----------------------------------------------------------------------------

func s113Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s113Namespace() string { return s113Env(envS113Namespace, s113DefaultNamespace) }

// s113RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO113_LIVE=1.
func (s *Scenario113E2ESuite) s113RequireLive() {
	if os.Getenv(envKubeconfigS113) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 113 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 113 live Part B")
	}
	if os.Getenv(envS113Live) != "1" {
		s.T().Skip("SCENARIO113_LIVE not set, skipping the live apply-and-reject matrix " +
			"(the deployed cluster + the Vault-PKI webhook must be reachable)")
	}
}

// s113Kubectl runs a kubectl subcommand bounded by a short timeout, returning the
// combined output and error.
func (s *Scenario113E2ESuite) s113Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s113ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s113ApplyYAML pipes a manifest to `kubectl apply -f -` and returns the combined
// output + error.
func (s *Scenario113E2ESuite) s113ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s113ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s113Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s113LooksLikeWebhookUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the webhook (NOT a validation denial). When true,
// Part B SKIPS cleanly: an unhealthy Vault-PKI webhook cert must not be counted
// as a validation rejection.
func s113LooksLikeWebhookUnhealthy(out string) bool {
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

// s113RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served (so a reject is genuinely an admission decision,
// not a missing-resource error).
func (s *Scenario113E2ESuite) s113RequireNamespace() {
	if out, err := s.s113Kubectl("get", "namespace", s113Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s113Namespace(), out)
	}
	if out, err := s.s113Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s113CRExists reports whether a CloudberryCluster with the given name exists in
// the deploy namespace (used for the no-persist GET).
func (s *Scenario113E2ESuite) s113CRExists(name string) bool {
	_, err := s.s113Kubectl("get", "cloudberrycluster", name, "-n", s113Namespace())
	return err == nil
}

// s113validBaseYAML returns a base-valid CloudberryCluster manifest (HA mirrored)
// with the recommendation scan ENABLED and all four thresholds in range, with the
// placeholder name filled. Each reject case is produced by injecting EXACTLY ONE
// out-of-range threshold via a targeted string swap.
func s113validBaseYAML(name string) string {
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

// s113NegativeManifests returns, for each Scenario 113 live (-L) reject rule ID, a
// base-valid manifest mutated to carry EXACTLY ONE out-of-range threshold. The
// mutation is a targeted string swap on the base YAML; the resulting manifest
// matches the catalog's OffendingField.
func (s *Scenario113E2ESuite) s113NegativeManifests(name string) map[string]string {
	base := s113validBaseYAML(name)
	out := map[string]string{}

	// W.1 — bloatThreshold above the upper bound (WEBHOOK).
	out["113-W1-150-L"] = strings.Replace(base, "bloatThreshold: 20", "bloatThreshold: 150", 1)
	// W.1 — bloatThreshold below the lower bound (WEBHOOK).
	out["113-W1-neg1-L"] = strings.Replace(base, "bloatThreshold: 20", "bloatThreshold: -1", 1)
	// W.2 — skewThreshold above the upper bound (WEBHOOK).
	out["113-W2-L"] = strings.Replace(base, "skewThreshold: 50", "skewThreshold: 101", 1)
	// W.3 — indexBloatThreshold above the upper bound (WEBHOOK).
	out["113-W3-L"] = strings.Replace(base, "indexBloatThreshold: 30", "indexBloatThreshold: 200", 1)
	// W.4 — ageThreshold negative (WEBHOOK).
	out["113-W4-L"] = strings.Replace(base, "ageThreshold: 500000000", "ageThreshold: -5", 1)

	return out
}

// TestE2E_Scenario113_LiveRejectMatrix is the core live proof: for EACH W.x it
// applies a base-valid CR carrying one out-of-range threshold and asserts the
// apply is DENIED with OUR descriptive webhook error AND the CR did NOT persist
// (113-NOPERSIST-L). It distinguishes an unhealthy Vault-PKI webhook (TLS /
// connection failure → SKIP CONFIG-ONLY) from a genuine validation denial. SKIPS
// cleanly when the live env is absent.
//
//nolint:gocyclo,funlen // a self-contained per-rule apply→reject→no-persist matrix.
func (s *Scenario113E2ESuite) TestE2E_Scenario113_LiveRejectMatrix() {
	s.s113RequireLive()
	s.s113RequireNamespace()

	ns := s113Namespace()

	// Index the live (-L) reject rows by ID for substring/source lookup.
	liveRows := map[string]cases.Scenario113Case{}
	for _, c := range cases.Scenario113Cases() {
		if c.Layer == cases.Scenario113LayerLive && c.Req != "CONTROL" && c.Req != "NOPERSIST" {
			liveRows[c.ID] = c
		}
	}
	require.NotEmpty(s.T(), liveRows, "the catalog must enumerate live reject rows")

	for id, row := range liveRows {
		id, row := id, row
		s.Run(id, func() {
			// SHORT, unique CR name per rule (e.g. s113-neg-w1-150-l).
			name := "s113-neg-" + strings.ToLower(strings.TrimPrefix(id, "113-"))
			name = strings.ReplaceAll(name, ".", "-")

			manifests := s.s113NegativeManifests(name)
			manifest, ok := manifests[id]
			require.Truef(s.T(), ok, "no manifest wired for live rule %s", id)

			// Best-effort cleanup in case a prior run leaked the name.
			defer func() {
				_, _ = s.s113Kubectl("delete", "cloudberrycluster", name, "-n", ns,
					"--ignore-not-found", "--wait=false")
			}()

			out, applyErr := s.s113ApplyYAML(manifest)

			// (Webhook health) distinguish a TLS/connection failure from a denial.
			if applyErr != nil && s113LooksLikeWebhookUnhealthy(out) {
				s.T().Skipf("%s: webhook appears UNHEALTHY (TLS/connection), not a validation "+
					"denial [CONFIG-ONLY]: %s", id, out)
			}

			// (a) apply must FAIL.
			require.Errorf(s.T(), applyErr, "%s: apply must be DENIED (source=%s); out=%q",
				id, row.Source, out)

			// (b) the error must be descriptive (our webhook message).
			for _, substr := range row.ErrorSubstrings {
				assert.Containsf(s.T(), out, substr,
					"%s (source=%s): rejection must contain %q; got %q",
					id, row.Source, substr, out)
			}

			// (c) NO-PERSIST: the rejected CR must not exist.
			assert.Falsef(s.T(), s.s113CRExists(name),
				"113-NOPERSIST-L: %s rejected CR %q must NOT persist", id, name)

			s.T().Logf("scenario113 %s (source=%s): apply denied + no-persist OK", id, row.Source)
		})
	}
}

// TestE2E_Scenario113_LiveControlAdmits covers 113-CONTROL-admit-L: a fully-valid
// enabled-scan CR APPLIES successfully on the LIVE apiserver (proving the webhook
// is not rejecting everything — no false-positive), then is cleaned up. SKIPS
// cleanly when the live env is absent or the webhook is unhealthy.
func (s *Scenario113E2ESuite) TestE2E_Scenario113_LiveControlAdmits() {
	s.s113RequireLive()
	s.s113RequireNamespace()

	ns := s113Namespace()
	name := "s113-control-l"
	manifest := s113validBaseYAML(name)

	// Always clean up the CONTROL CR.
	defer func() {
		_, _ = s.s113Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s113ApplyYAML(manifest)
	if applyErr != nil && s113LooksLikeWebhookUnhealthy(out) {
		s.T().Skipf("113-CONTROL-admit-L: webhook appears UNHEALTHY (TLS/connection) "+
			"[CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"113-CONTROL-admit-L: a fully-valid CR must APPLY (no false-positive); out=%q", out)

	assert.Truef(s.T(), s.s113CRExists(name),
		"113-CONTROL-admit-L: the valid CR %q must persist after apply", name)
	s.T().Logf("scenario113 113-CONTROL-admit-L: valid CR applied + persisted OK; cleaning up")
}

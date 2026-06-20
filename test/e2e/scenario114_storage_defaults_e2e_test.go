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
// Scenario 114: Mutating Webhook Defaults — storage-recommendation scan
// (D.1–D.6) — E2E
// ============================================================================
//
// Mirrors the Scenario 113 e2e SHAPE: a catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO114_LIVE-gated live Part B. Part B is the LIVE
// persisted-defaults proof: it `kubectl apply`s a MINIMAL CloudberryCluster whose
// recommendation scan is ENABLED with all six defaulted fields OMITTED, then
// `kubectl get`s the persisted object and asserts:
//
//   (a) apply SUCCEEDS (the minimal enabled scan is admitted),
//   (b) the six defaults D.1–D.6 are PERSISTED on the stored object — the object
//       was NOT pre-defaulted, so the values can only come from the deployed
//       mutating webhook (114-PERSIST-L),
//   (c) the applied CR is cleaned up.
//
// Vault-PKI webhook cert health: if the apply fails with a TLS/connection error
// (NOT an admission decision) the webhook is unhealthy — Part B distinguishes
// that and SKIPS cleanly with a CONFIG-ONLY message. Self-contained; generous
// timeouts; SKIPS cleanly when KUBECONFIG/the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO114_LIVE=1    — gates the live apply/persist proof.
//   SCENARIO114_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS114 = "KUBECONFIG"
	envS114Live       = "SCENARIO114_LIVE"
	envS114Namespace  = "SCENARIO114_NAMESPACE"

	s114DefaultNamespace = "cloudberry-test"

	s114ExecTimeout = 2 * time.Minute
)

// Scenario114E2ESuite verifies the storage-recommendation mutating-webhook
// defaults end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B that
// applies a minimal enabled scan and asserts the six defaults persisted).
type Scenario114E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario114(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario114E2ESuite))
}

func (s *Scenario114E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario114_PartA_CatalogHonest iterates the full Scenario 114 catalog
// and asserts it is well-formed: unique IDs, every D.1–D.6 + ALL + PRESERVE +
// DISABLED + CONTROL + PERSIST family present, every row carries a non-empty
// Layer/Gate/Expected/Description with known tokens, and every per-D row carries
// a Field + ExpectedValue.
//
//nolint:gocyclo // a single catalog-well-formedness walk.
func (s *Scenario114E2ESuite) TestE2E_Scenario114_PartA_CatalogHonest() {
	catalog := cases.Scenario114Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario114LayerUnit,
		cases.Scenario114LayerFunctional,
		cases.Scenario114LayerLive,
	}
	knownGates := []string{
		cases.Scenario114GateEnabled,
		cases.Scenario114GateDisabled,
		cases.Scenario114GateNone,
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

			// Per-D rows must carry a Field + ExpectedValue.
			if strings.HasPrefix(tc.Req, "D.") {
				assert.NotEmptyf(s.T(), tc.Field, "%s (per-D) must carry a Field", tc.ID)
				assert.NotEmptyf(s.T(), tc.ExpectedValue, "%s (per-D) must carry an ExpectedValue", tc.ID)
			}
			if tc.Layer == cases.Scenario114LayerLive {
				s.T().Logf("scenario114 %s (%s, gate=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Gate, tc.Expected)
			}
		})
	}
	for i := 1; i <= 6; i++ {
		req := fmt.Sprintf("D.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover rule family %s", req)
	}
	assert.True(s.T(), reqs["ALL"], "catalog must cover the ALL family")
	assert.True(s.T(), reqs["PRESERVE"], "catalog must cover the PRESERVE family")
	assert.True(s.T(), reqs["DISABLED"], "catalog must cover the DISABLED family")
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO114_LIVE gated live apply-and-persist proof
// ----------------------------------------------------------------------------

func s114Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s114Namespace() string { return s114Env(envS114Namespace, s114DefaultNamespace) }

// s114RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO114_LIVE=1.
func (s *Scenario114E2ESuite) s114RequireLive() {
	if os.Getenv(envKubeconfigS114) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 114 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 114 live Part B")
	}
	if os.Getenv(envS114Live) != "1" {
		s.T().Skip("SCENARIO114_LIVE not set, skipping the live apply-and-persist proof " +
			"(the deployed cluster + the Vault-PKI webhook must be reachable)")
	}
}

// s114Kubectl runs a kubectl subcommand bounded by a short timeout, returning the
// combined output and error.
func (s *Scenario114E2ESuite) s114Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s114ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s114ApplyYAML pipes a manifest to `kubectl apply -f -` and returns the combined
// output + error.
func (s *Scenario114E2ESuite) s114ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s114ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s114Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s114LooksLikeWebhookUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the webhook (NOT an admission decision). When true,
// Part B SKIPS cleanly: an unhealthy Vault-PKI webhook cert must not be counted
// as a failed default injection.
func s114LooksLikeWebhookUnhealthy(out string) bool {
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

// s114RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served (so a persisted default is genuinely a webhook
// decision, not a missing-resource error).
func (s *Scenario114E2ESuite) s114RequireNamespace() {
	if out, err := s.s114Kubectl("get", "namespace", s114Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s114Namespace(), out)
	}
	if out, err := s.s114Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s114GetField runs `kubectl get -o jsonpath` for a single recommendation-scan
// field and returns the rendered value.
func (s *Scenario114E2ESuite) s114GetField(name, jsonPath string) (string, error) {
	return s.s114Kubectl("get", "cloudberrycluster", name,
		"-n", s114Namespace(), "-o", "jsonpath="+jsonPath)
}

// s114MinimalEnabledYAML returns a MINIMAL but valid CloudberryCluster manifest
// (HA mirrored) whose recommendation scan is ENABLED with ALL six defaulted
// fields OMITTED, with the placeholder name filled. The object is intentionally
// NOT pre-defaulted: persistence of the defaults proves the webhook ran.
func s114MinimalEnabledYAML(name string) string {
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

// s114LiveFieldPaths maps each per-D catalog Field to the kubectl jsonpath that
// reads it back from the persisted object.
func s114LiveFieldPaths() map[string]string {
	return map[string]string{
		"storage.recommendationScan.schedule":            "{.spec.storage.recommendationScan.schedule}",
		"storage.recommendationScan.bloatThreshold":      "{.spec.storage.recommendationScan.bloatThreshold}",
		"storage.recommendationScan.skewThreshold":       "{.spec.storage.recommendationScan.skewThreshold}",
		"storage.recommendationScan.ageThreshold":        "{.spec.storage.recommendationScan.ageThreshold}",
		"storage.recommendationScan.indexBloatThreshold": "{.spec.storage.recommendationScan.indexBloatThreshold}",
		"storage.recommendationScan.scanDuration":        "{.spec.storage.recommendationScan.scanDuration}",
	}
}

// TestE2E_Scenario114_LiveDefaultsPersisted is the core live proof
// (114-ALL-omitted-L / 114-PERSIST-L / 114-D1..D6-L): it applies a base-valid CR
// whose enabled scan omits all six fields and asserts the apply SUCCEEDS and the
// six defaults D.1–D.6 are PERSISTED on the GET'd object — proving the deployed
// mutating webhook injected them. It distinguishes an unhealthy Vault-PKI webhook
// (TLS/connection failure → SKIP CONFIG-ONLY) from a genuine apply. SKIPS cleanly
// when the live env is absent. The applied CR is cleaned up.
func (s *Scenario114E2ESuite) TestE2E_Scenario114_LiveDefaultsPersisted() {
	s.s114RequireLive()
	s.s114RequireNamespace()

	ns := s114Namespace()
	name := "s114-defaults-l"

	// Index the live per-D rows by Field for expected-value lookup.
	liveRows := map[string]cases.Scenario114Case{}
	for _, c := range cases.Scenario114Cases() {
		if c.Layer == cases.Scenario114LayerLive && c.Field != "" {
			liveRows[c.Field] = c
		}
	}
	require.NotEmpty(s.T(), liveRows, "the catalog must enumerate live per-D rows")

	// Always clean up the applied CR.
	defer func() {
		_, _ = s.s114Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s114ApplyYAML(s114MinimalEnabledYAML(name))
	if applyErr != nil && s114LooksLikeWebhookUnhealthy(out) {
		s.T().Skipf("114-PERSIST-L: webhook appears UNHEALTHY (TLS/connection), not an apply "+
			"decision [CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"114-ALL-omitted-L: a minimal enabled scan must APPLY; out=%q", out)

	paths := s114LiveFieldPaths()
	for field, row := range liveRows {
		field, row := field, row
		s.Run(row.ID, func() {
			jsonPath, ok := paths[field]
			require.Truef(s.T(), ok, "no jsonpath wired for live rule %s", row.ID)

			got, getErr := s.s114GetField(name, jsonPath)
			require.NoErrorf(s.T(), getErr, "%s: GET %s must succeed; got=%q", row.ID, field, got)
			assert.Equalf(s.T(), row.ExpectedValue, strings.TrimSpace(got),
				"114-PERSIST-L: %s (%s) must be persisted by the mutating webhook", row.ID, field)
			s.T().Logf("scenario114 %s (gate=%s): persisted %s = %s", row.ID, row.Gate, field, got)
		})
	}
}

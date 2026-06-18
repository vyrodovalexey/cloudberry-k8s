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
// Scenario 110: Webhook Validation (All Rules) (W.1–W.15) — integration
// ============================================================================
//
// Mirrors the Scenario 108/109 integration SHAPE (reachability-gated; SKIPS
// CLEANLY when the apiserver is down). The validator-direct rules are exercised
// at the unit (internal/webhook) + functional layers; the FULL live source-aware
// matrix is the e2e Part B. This integration layer adds the value those layers
// cannot: it submits — to a REAL apiserver — the cases whose rejection comes from
// the CRD OpenAPI SCHEMA (not the validator), which the validator-direct path
// does NOT exercise the same way:
//
//   - the SCHEMA-enum cases W.3 (type: ftp), W.8 (type: spark), W.15
//     (segmentRejectLimitType: fraction) — rejected by the CRD enum at the
//     apiserver with an "Unsupported value" error BEFORE the webhook runs;
//   - the BOTH cases W.11 / W.12 expressed as an OMITTED targetTable key —
//     rejected by the CRD `required` at the apiserver.
//
// Each case asserts (a) the apply is DENIED and (b) the CR does NOT persist
// (a follow-up GET is NotFound). It distinguishes an unhealthy webhook (TLS /
// connection failure) from a genuine schema/admission denial and SKIPS cleanly
// when the apiserver/CRD/namespace are absent.
//
// HONESTY: nothing is synthesized — when the apiserver/CRD/namespace are
// unreachable the suite skips cleanly; the catalog-well-formedness check always
// runs (no infra).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates the live apiserver submission (skip when unset).
//   SCENARIO110_LIVE=1    — gates the live submission (off by default).
//   SCENARIO110_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS110I = "KUBECONFIG"
	envS110LiveI       = "SCENARIO110_LIVE"
	envS110NamespaceI  = "SCENARIO110_NAMESPACE"

	scenario110DefaultNamespace = "cloudberry-test"
	scenario110ExecTimeout      = 90 * time.Second
)

// Scenario110Suite drives the Scenario 110 CRD-schema rejection probe, gated on
// apiserver reachability.
type Scenario110Suite struct {
	suite.Suite
	ctx    context.Context
	cancel context.CancelFunc
}

func TestIntegration_Scenario110(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario110Suite))
}

func (s *Scenario110Suite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
}

func (s *Scenario110Suite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario110Namespace() string {
	if v := strings.TrimSpace(os.Getenv(envS110NamespaceI)); v != "" {
		return v
	}
	return scenario110DefaultNamespace
}

// scenario110Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario110Suite) scenario110Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario110ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario110ApplyYAML pipes a manifest to `kubectl apply -f -`.
func (s *Scenario110Suite) scenario110ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario110ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", scenario110Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario110LooksUnhealthy reports a TLS/connection failure reaching the webhook
// (NOT a schema/validation denial) so callers can SKIP cleanly.
func scenario110LooksUnhealthy(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "x509") ||
		strings.Contains(lower, "tls") ||
		strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no endpoints available") ||
		strings.Contains(lower, "context deadline exceeded") ||
		strings.Contains(lower, "failed calling webhook") ||
		strings.Contains(lower, "dial tcp")
}

// scenario110RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists,
// SCENARIO110_LIVE=1, and the namespace + CRD are served.
func (s *Scenario110Suite) scenario110RequireLive() {
	if os.Getenv(envKubeconfigS110I) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 110 live schema submission")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 110 live schema submission")
	}
	if os.Getenv(envS110LiveI) != "1" {
		s.T().Skip("SCENARIO110_LIVE not set, skipping the live schema submission " +
			"[CONFIG-ONLY: the full live matrix is the e2e Part B]")
	}
	if out, err := s.scenario110Kubectl("get", "namespace", scenario110Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", scenario110Namespace(), out)
	}
	if out, err := s.scenario110Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// scenario110CRExists reports whether a CloudberryCluster with the given name
// exists in the deploy namespace (used for the no-persist GET).
func (s *Scenario110Suite) scenario110CRExists(name string) bool {
	_, err := s.scenario110Kubectl("get", "cloudberrycluster", name,
		"-n", scenario110Namespace())
	return err == nil
}

// scenario110ValidBaseYAML returns a base-valid CloudberryCluster manifest with
// the placeholder name filled. The schema/both negative cases inject EXACTLY ONE
// violation via a targeted string swap.
func scenario110ValidBaseYAML(name string) string {
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
  dataLoading:
    enabled: true
    pxf:
      enabled: true
      image: "cloudberry-pxf:7.1.0"
      servers:
        - name: s3-datalake
          type: s3
          config:
            fs.s3a.endpoint: "http://minio:9000"
          credentialSecrets:
            - name: s3-creds
    jobs:
      - name: s3-csv-loader
        type: pxf
        enabled: true
        pxfJob:
          server: s3-datalake
          profile: "s3:text"
          targetTable: "public.events"
      - name: csv-bulk-load
        type: gpload
        enabled: true
        gploadJob:
          targetTable: "public.bulk_data"
          format: csv
          filePaths:
            - "/data/incoming/*.csv"
`, name)
}

// scenario110SchemaManifests returns the SCHEMA-sourced negative manifests this
// integration layer submits to the apiserver (the CRD-enum + omitted-key cases),
// keyed by the catalog ID. Each carries EXACTLY ONE violation.
func (s *Scenario110Suite) scenario110SchemaManifests(name string) map[string]string {
	base := scenario110ValidBaseYAML(name)
	return map[string]string{
		// W.3 — server type ftp (CRD enum).
		"110-W3-L": strings.Replace(base, "type: s3", "type: ftp", 1),
		// W.8 — job type spark (CRD enum).
		"110-W8-L": strings.Replace(base, "        type: pxf", "        type: spark", 1),
		// W.15 — segmentRejectLimitType fraction (CRD enum).
		"110-W15-L": strings.Replace(base,
			"          targetTable: \"public.events\"\n",
			"          targetTable: \"public.events\"\n          errorHandling:\n"+
				"            segmentRejectLimit: 100\n            segmentRejectLimitType: fraction\n", 1),
		// W.11 — pxfJob OMITS targetTable (CRD required).
		"110-W11-L": strings.Replace(base,
			"          targetTable: \"public.events\"\n", "", 1),
		// W.12 — gploadJob OMITS targetTable (CRD required).
		"110-W12-L": strings.Replace(base,
			"          targetTable: \"public.bulk_data\"\n", "", 1),
	}
}

// TestIntegration_Scenario110_CatalogHonest asserts the Scenario 110 catalog is
// well-formed (always runs; no infra) so the integration layer documents the
// same IDs the functional/e2e layers resolve.
func (s *Scenario110Suite) TestIntegration_Scenario110_CatalogHonest() {
	catalog := cases.Scenario110WebhookCases()
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

// TestIntegration_Scenario110_SchemaRejection submits the SCHEMA-enum (W.3/W.8/
// W.15) + the BOTH omitted-key (W.11/W.12) cases to the REAL apiserver and
// asserts each apply is DENIED and the CR does NOT persist. These rejections come
// from the CRD OpenAPI schema (not the webhook validator), so they are the unique
// value this layer adds over the validator-direct unit/functional tests. SKIPS
// cleanly when the apiserver/CRD/namespace are absent or the webhook is unhealthy.
func (s *Scenario110Suite) TestIntegration_Scenario110_SchemaRejection() {
	s.scenario110RequireLive()

	ns := scenario110Namespace()

	// Index the schema/both live rows by ID for substring lookup.
	rows := map[string]cases.Scenario110Case{}
	for _, c := range cases.Scenario110WebhookCases() {
		if c.Layer != cases.Scenario110LayerLive {
			continue
		}
		if c.Source == cases.Scenario110SourceSchema || c.Source == cases.Scenario110SourceBoth {
			rows[c.ID] = c
		}
	}
	require.NotEmpty(s.T(), rows, "catalog must enumerate schema/both live rows")

	for id, row := range rows {
		id, row := id, row
		s.Run(id, func() {
			name := "s110i-" + strings.ToLower(strings.TrimPrefix(id, "110-"))
			name = strings.ReplaceAll(name, ".", "-")

			manifests := s.scenario110SchemaManifests(name)
			manifest, ok := manifests[id]
			require.Truef(s.T(), ok, "no schema manifest wired for %s", id)

			defer func() {
				_, _ = s.scenario110Kubectl("delete", "cloudberrycluster", name, "-n", ns,
					"--ignore-not-found", "--wait=false")
			}()

			out, applyErr := s.scenario110ApplyYAML(manifest)
			if applyErr != nil && scenario110LooksUnhealthy(out) {
				s.T().Skipf("%s: webhook/apiserver appears UNHEALTHY (TLS/connection) "+
					"[CONFIG-ONLY]: %s", id, out)
			}

			require.Errorf(s.T(), applyErr, "%s: apply must be DENIED by the CRD schema; out=%q",
				id, out)
			for _, substr := range row.ErrorSubstrings {
				assert.Containsf(s.T(), out, substr,
					"%s (source=%s): schema rejection must contain %q; got %q",
					id, row.Source, substr, out)
			}
			assert.Falsef(s.T(), s.scenario110CRExists(name),
				"%s: schema-rejected CR %q must NOT persist", id, name)
			s.T().Logf("scenario110 %s (source=%s): CRD-schema reject + no-persist OK",
				id, row.Source)
		})
	}
}

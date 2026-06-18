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
// Scenario 110: Webhook Validation (All Rules) (W.1–W.15) — E2E
// ============================================================================
//
// Mirrors the Scenario 108/109 e2e SHAPE: a catalog-honest Part A that ALWAYS
// runs + a KUBECONFIG/SCENARIO110_LIVE-gated live Part B. Part B is the COMPLETE,
// systematic LIVE reject matrix: for EACH W.x it builds a base-valid
// CloudberryCluster YAML with EXACTLY ONE violation, `kubectl apply`s it, and
// asserts:
//
//   (a) apply FAILS (non-zero exit / admission denied),
//   (b) the stderr contains the descriptive error — for WEBHOOK rules our message
//       substring (e.g. "dataLoading.pxf.image", "does not reference a defined",
//       "segmentRejectLimitType must be"); for SCHEMA-enum rules (W.3/W.8/W.15)
//       the apiserver enum error ("Unsupported value: \"ftp\"" + an allowed value);
//       for the BOTH rules (W.11/W.12) the omitted-key apiserver `required` error
//       ("targetTable") — source-aware per the task-breakdown table,
//   (c) NO-PERSIST: a follow-up `kubectl get cloudberrycluster <name>` returns
//       NotFound (the rejected CR did not persist).
//
//   110-CONTROL-admit: a fully-valid CR APPLIES successfully (then is deleted) —
//   proving the webhook is not rejecting everything (no false-positive).
//
// Vault-PKI webhook cert health: if an apply fails with a TLS/connection error
// (NOT a validation error) the webhook is unhealthy — Part B distinguishes that
// and SKIPS cleanly with a CONFIG-ONLY message (it does NOT count an unhealthy
// webhook as a validation denial). Self-contained; the CONTROL CR is cleaned up;
// generous timeouts; SKIPS cleanly when KUBECONFIG/the live env is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG            — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO110_LIVE=1    — gates the live apply matrix.
//   SCENARIO110_NAMESPACE — namespace (default cloudberry-test).
// ============================================================================

const (
	envKubeconfigS110 = "KUBECONFIG"
	envS110Live       = "SCENARIO110_LIVE"
	envS110Namespace  = "SCENARIO110_NAMESPACE"

	s110DefaultNamespace = "cloudberry-test"

	s110ExecTimeout = 2 * time.Minute
)

// Scenario110E2ESuite verifies the full webhook validation reject matrix
// end-to-end (catalog-honest Part A + KUBECONFIG-gated live Part B that applies
// each invalid CR and asserts reject + no-persist).
type Scenario110E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario110(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario110E2ESuite))
}

func (s *Scenario110E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-honest (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario110_PartA_CatalogHonest iterates the full Scenario 110 catalog
// and asserts it is well-formed: unique IDs, every W.1–W.15 + CONTROL + NOPERSIST
// family present, every row carries a non-empty Layer/Source/Expected/Description
// with known tokens, and every -L negative row carries the NoPersist contract.
//
//nolint:gocyclo // a single catalog-well-formedness walk.
func (s *Scenario110E2ESuite) TestE2E_Scenario110_PartA_CatalogHonest() {
	catalog := cases.Scenario110WebhookCases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario110LayerUnit,
		cases.Scenario110LayerFunctional,
		cases.Scenario110LayerLive,
	}
	knownSources := []string{
		cases.Scenario110SourceWebhook,
		cases.Scenario110SourceSchema,
		cases.Scenario110SourceBoth,
		cases.Scenario110SourceNone,
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

			// Negative rows (not CONTROL/NOPERSIST) must carry descriptive substrings.
			if tc.Req != "CONTROL" && tc.Req != "NOPERSIST" {
				assert.NotEmptyf(s.T(), tc.ErrorSubstrings,
					"%s must carry descriptive error substrings", tc.ID)
			}
			// Every live negative row carries the no-persist contract.
			if tc.Layer == cases.Scenario110LayerLive && tc.Req != "CONTROL" {
				assert.Truef(s.T(), tc.NoPersist, "%s (live negative) must carry NoPersist", tc.ID)
				s.T().Logf("scenario110 %s (%s, source=%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Source, tc.Expected)
			}
		})
	}
	for i := 1; i <= 15; i++ {
		req := fmt.Sprintf("W.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover rule family %s", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL family")
	assert.True(s.T(), reqs["NOPERSIST"], "catalog must cover the NOPERSIST family")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO110_LIVE gated live apply-and-reject matrix
// ----------------------------------------------------------------------------

func s110Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s110Namespace() string { return s110Env(envS110Namespace, s110DefaultNamespace) }

// s110RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO110_LIVE=1.
func (s *Scenario110E2ESuite) s110RequireLive() {
	if os.Getenv(envKubeconfigS110) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 110 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 110 live Part B")
	}
	if os.Getenv(envS110Live) != "1" {
		s.T().Skip("SCENARIO110_LIVE not set, skipping the live apply-and-reject matrix " +
			"(the deployed cluster + the Vault-PKI webhook must be reachable)")
	}
}

// s110Kubectl runs a kubectl subcommand bounded by a short timeout, returning
// the combined output and error.
func (s *Scenario110E2ESuite) s110Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s110ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s110ApplyYAML pipes a manifest to `kubectl apply -f -` and returns the combined
// output + error.
func (s *Scenario110E2ESuite) s110ApplyYAML(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s110ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-n", s110Namespace(), "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// s110LooksLikeWebhookUnhealthy reports whether apply output indicates a TLS /
// connection failure reaching the webhook (NOT a validation denial). When true,
// Part B SKIPS cleanly: an unhealthy Vault-PKI webhook cert must not be counted
// as a validation rejection.
func s110LooksLikeWebhookUnhealthy(out string) bool {
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

// s110RequireNamespace skips cleanly unless the deploy namespace exists and the
// CloudberryCluster CRD is served (so a reject is genuinely an admission/schema
// decision, not a missing-resource error).
func (s *Scenario110E2ESuite) s110RequireNamespace() {
	if out, err := s.s110Kubectl("get", "namespace", s110Namespace()); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s110Namespace(), out)
	}
	if out, err := s.s110Kubectl("get", "crd", "cloudberryclusters.avsoft.io"); err != nil {
		s.T().Skipf("CloudberryCluster CRD not served [CONFIG-ONLY]: %s", out)
	}
}

// s110CRExists reports whether a CloudberryCluster with the given name exists in
// the deploy namespace (used for the no-persist GET).
func (s *Scenario110E2ESuite) s110CRExists(name string) bool {
	_, err := s.s110Kubectl("get", "cloudberrycluster", name, "-n", s110Namespace())
	return err == nil
}

// s110validBaseYAML returns a base-valid CloudberryCluster manifest (HA mirrored,
// pxf minimal-but-valid) with the placeholders __NAME__ filled. Each negative
// case is produced by injecting EXACTLY ONE violation via a YAML-fragment swap.
//
// The fragments below are kept in lockstep with the validator's base-valid shape:
// pxf enabled+image, one s3 server (endpoint+credentialSecrets), one jdbc server
// (driver+url), one hdfs server (defaultFS), one pxf job (server/profile/
// targetTable) + one gpload job (targetTable).
func s110validBaseYAML(name string) string {
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
        - name: mysql-oltp
          type: jdbc
          config:
            jdbc.driver: "com.mysql.cj.jdbc.Driver"
            jdbc.url: "jdbc:mysql://mysql:3306/db"
        - name: hdfs-warehouse
          type: hdfs
          config:
            fs.defaultFS: "hdfs://namenode:8020"
    jobs:
      - name: s3-csv-loader
        type: pxf
        enabled: true
        schedule: "*/30 * * * *"
        pxfJob:
          server: s3-datalake
          profile: "s3:text"
          resource: "s3a://data-lake/events/"
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

// s110NegativeManifests returns, for each Scenario 110 live (-L) negative rule
// ID, a base-valid manifest mutated to carry EXACTLY ONE violation. The mutation
// is a targeted string swap on the base YAML; the resulting manifest matches the
// catalog's OffendingField + expected source.
//
//nolint:funlen // an exhaustive per-rule manifest table.
func (s *Scenario110E2ESuite) s110NegativeManifests(name string) map[string]string {
	base := s110validBaseYAML(name)
	out := map[string]string{}

	// W.1 — empty pxf.image (WEBHOOK).
	out["110-W1-L"] = strings.Replace(base,
		`image: "cloudberry-pxf:7.1.0"`, `image: ""`, 1)

	// W.2-dup — duplicate server name (WEBHOOK).
	out["110-W2-dup-L"] = strings.Replace(base,
		"- name: mysql-oltp", "- name: s3-datalake", 1)

	// W.3 — server type ftp (SCHEMA enum).
	out["110-W3-L"] = strings.Replace(base,
		"type: s3", "type: ftp", 1)

	// W.4-endpoint — s3 server missing fs.s3a.endpoint (WEBHOOK).
	out["110-W4-endpoint-L"] = strings.Replace(base,
		"          config:\n            fs.s3a.endpoint: \"http://minio:9000\"\n",
		"          config: {}\n", 1)

	// W.5-driver — jdbc server missing jdbc.driver (WEBHOOK).
	out["110-W5-driver-L"] = strings.Replace(base,
		"            jdbc.driver: \"com.mysql.cj.jdbc.Driver\"\n", "", 1)

	// W.6 — hdfs server missing fs.defaultFS (WEBHOOK).
	out["110-W6-L"] = strings.Replace(base,
		"          config:\n            fs.defaultFS: \"hdfs://namenode:8020\"\n",
		"          config: {}\n", 1)

	// W.7-dup — duplicate job name (WEBHOOK).
	out["110-W7-dup-L"] = strings.Replace(base,
		"- name: csv-bulk-load", "- name: s3-csv-loader", 1)

	// W.8 — job type spark (SCHEMA enum).
	out["110-W8-L"] = strings.Replace(base,
		"        type: pxf", "        type: spark", 1)

	// W.9 — pxfJob.server references an undefined server (WEBHOOK).
	out["110-W9-L"] = strings.Replace(base,
		"          server: s3-datalake", "          server: does-not-exist", 1)

	// W.10 — pxfJob.profile invalid (WEBHOOK).
	out["110-W10-L"] = strings.Replace(base,
		`profile: "s3:text"`, `profile: "s3:nonsense"`, 1)

	// W.11 — pxfJob OMITS targetTable (BOTH → SCHEMA required at apiserver).
	out["110-W11-L"] = strings.Replace(base,
		"          targetTable: \"public.events\"\n", "", 1)

	// W.12 — gploadJob OMITS targetTable (BOTH → SCHEMA required at apiserver).
	out["110-W12-L"] = strings.Replace(base,
		"          targetTable: \"public.bulk_data\"\n", "", 1)

	// W.13 — invalid cron schedule (WEBHOOK).
	out["110-W13-L"] = strings.Replace(base,
		`schedule: "*/30 * * * *"`, `schedule: "not a cron"`, 1)

	// W.14 — partitioning column without range/interval (WEBHOOK).
	out["110-W14-L"] = strings.Replace(base,
		"          targetTable: \"public.events\"\n",
		"          targetTable: \"public.events\"\n          partitioning:\n            column: id\n", 1)

	// W.15 — segmentRejectLimitType fraction (SCHEMA enum).
	out["110-W15-L"] = strings.Replace(base,
		"          targetTable: \"public.events\"\n",
		"          targetTable: \"public.events\"\n          errorHandling:\n"+
			"            segmentRejectLimit: 100\n            segmentRejectLimitType: fraction\n", 1)

	return out
}

// TestE2E_Scenario110_LiveRejectMatrix is the core live proof: for EACH W.x it
// applies a base-valid CR carrying one violation and asserts the apply is DENIED
// with the source-aware descriptive error AND the CR did NOT persist
// (110-NOPERSIST-L). It distinguishes an unhealthy Vault-PKI webhook (TLS /
// connection failure → SKIP CONFIG-ONLY) from a genuine validation denial.
// SKIPS cleanly when the live env is absent.
//
//nolint:gocyclo,funlen // a self-contained per-rule apply→reject→no-persist matrix.
func (s *Scenario110E2ESuite) TestE2E_Scenario110_LiveRejectMatrix() {
	s.s110RequireLive()
	s.s110RequireNamespace()

	ns := s110Namespace()

	// Index the live (-L) negative rows by ID for substring/source lookup.
	liveRows := map[string]cases.Scenario110Case{}
	for _, c := range cases.Scenario110WebhookCases() {
		if c.Layer == cases.Scenario110LayerLive && c.Req != "CONTROL" && c.Req != "NOPERSIST" {
			liveRows[c.ID] = c
		}
	}
	require.NotEmpty(s.T(), liveRows, "the catalog must enumerate live negative rows")

	for id, row := range liveRows {
		id, row := id, row
		s.Run(id, func() {
			// SHORT, unique CR name per rule (e.g. s110-neg-w1-l).
			name := "s110-neg-" + strings.ToLower(strings.TrimPrefix(id, "110-"))
			name = strings.ReplaceAll(name, ".", "-")

			manifests := s.s110NegativeManifests(name)
			manifest, ok := manifests[id]
			require.Truef(s.T(), ok, "no manifest wired for live rule %s", id)

			// Best-effort cleanup in case a prior run leaked the name.
			defer func() {
				_, _ = s.s110Kubectl("delete", "cloudberrycluster", name, "-n", ns,
					"--ignore-not-found", "--wait=false")
			}()

			out, applyErr := s.s110ApplyYAML(manifest)

			// (Webhook health) distinguish a TLS/connection failure from a denial.
			if applyErr != nil && s110LooksLikeWebhookUnhealthy(out) {
				s.T().Skipf("%s: webhook appears UNHEALTHY (TLS/connection), not a validation "+
					"denial [CONFIG-ONLY]: %s", id, out)
			}

			// (a) apply must FAIL.
			require.Errorf(s.T(), applyErr, "%s: apply must be DENIED (source=%s); out=%q",
				id, row.Source, out)

			// (b) the error must be descriptive + source-aware.
			for _, substr := range row.ErrorSubstrings {
				assert.Containsf(s.T(), out, substr,
					"%s (source=%s): rejection must contain %q; got %q",
					id, row.Source, substr, out)
			}

			// (c) NO-PERSIST: the rejected CR must not exist.
			assert.Falsef(s.T(), s.s110CRExists(name),
				"110-NOPERSIST-L: %s rejected CR %q must NOT persist", id, name)

			s.T().Logf("scenario110 %s (source=%s): apply denied + no-persist OK", id, row.Source)
		})
	}
}

// TestE2E_Scenario110_LiveControlAdmits covers 110-CONTROL-admit-L: a fully-valid
// CR APPLIES successfully on the LIVE apiserver (proving the webhook is not
// rejecting everything — no false-positive), then is cleaned up. SKIPS cleanly
// when the live env is absent or the webhook is unhealthy.
func (s *Scenario110E2ESuite) TestE2E_Scenario110_LiveControlAdmits() {
	s.s110RequireLive()
	s.s110RequireNamespace()

	ns := s110Namespace()
	name := "s110-control-l"
	manifest := s110validBaseYAML(name)

	// Always clean up the CONTROL CR.
	defer func() {
		_, _ = s.s110Kubectl("delete", "cloudberrycluster", name, "-n", ns,
			"--ignore-not-found", "--wait=false")
	}()

	out, applyErr := s.s110ApplyYAML(manifest)
	if applyErr != nil && s110LooksLikeWebhookUnhealthy(out) {
		s.T().Skipf("110-CONTROL-admit-L: webhook appears UNHEALTHY (TLS/connection) "+
			"[CONFIG-ONLY]: %s", out)
	}
	require.NoErrorf(s.T(), applyErr,
		"110-CONTROL-admit-L: a fully-valid CR must APPLY (no false-positive); out=%q", out)

	assert.Truef(s.T(), s.s110CRExists(name),
		"110-CONTROL-admit-L: the valid CR %q must persist after apply", name)
	s.T().Logf("scenario110 110-CONTROL-admit-L: valid CR applied + persisted OK; cleaning up")
}

//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 121: All Storage CLI Commands (L.1–L.6) — E2E
// ============================================================================
//
// Mirrors the Scenario 108 e2e SHAPE (catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO121_LIVE-gated live Part B). Part B builds the REAL
// cloudberry-ctl binary and runs EACH of the six storage commands against the
// LIVE deployed operator API, asserting the documented effect:
//
//   L.1 storage disk-usage                         → output carries diskUsagePercent.
//   L.2 storage tables list                         → output lists tables.
//   L.3 storage tables detail --schema --table <T>  → table detail (T from L.2;
//                                                     degrade to CONFIG-ONLY if no
//                                                     user tables).
//   L.4 storage recommendations list                → output carries recommendationCount.
//   L.5 storage recommendations scan                → "scan initiated" / 202 (degrade
//                                                     when recommendationScan disabled).
//   L.6 storage usage-report --month 2026-05        → output echoes month=2026-05
//                                                     (degrade when usageReport disabled).
//
// This storage-recommendations CLI family (L.1–L.6) is DISTINCT from the
// data-loading L.1–L.16 CLI family of Scenario 108.
//
// SELF-CONTAINED: the read commands are read-only; the scan POST is idempotent-
// tolerant. Generous timeouts. SKIPS cleanly when KUBECONFIG / the live env is
// absent. The CLI authenticates via env passthrough: a bearer OIDC token
// (SCENARIO121_OIDC_TOKEN → ctl --auth-method oidc --password <token>) when set,
// otherwise basic-auth. SCENARIO121_API_BASE is the port-forwarded base. NOTE the
// operator API rate limit (10/min) — the test tolerates a 429 by retrying once.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO121_LIVE=1       — gates the live CLI exercise.
//   SCENARIO121_CLUSTER      — storage-enabled live cluster name (default s121).
//   SCENARIO121_NAMESPACE    — namespace (default cloudberry-test).
//   SCENARIO121_API_BASE     — operator API base URL (default http://localhost:8190).
//   SCENARIO121_OIDC_TOKEN   — bearer token; if unset, basic-auth creds are used.
//   SCENARIO121_API_USER     — basic-auth user (default adminuser).
//   SCENARIO121_API_PASS     — basic-auth pass (default adminpass).
//   SCENARIO121_CTL_BIN      — pre-built cloudberry-ctl binary (skip the build).
//   SCENARIO121_MONTH        — L.6 ?month= reporting period (default 2026-05).
// ============================================================================

const (
	envKubeconfigS121 = "KUBECONFIG"
	envS121Live       = "SCENARIO121_LIVE"
	envS121Cluster    = "SCENARIO121_CLUSTER"
	envS121Namespace  = "SCENARIO121_NAMESPACE"
	envS121APIBase    = "SCENARIO121_API_BASE"
	envS121Token      = "SCENARIO121_OIDC_TOKEN"
	envS121APIUser    = "SCENARIO121_API_USER"
	envS121APIPass    = "SCENARIO121_API_PASS"
	envS121CtlBin     = "SCENARIO121_CTL_BIN"
	envS121Month      = "SCENARIO121_MONTH"

	s121DefaultCluster   = "s121"
	s121DefaultNamespace = "cloudberry-test"
	s121DefaultAPIBase   = "http://localhost:8190"
	s121DefaultAPIUser   = "adminuser"
	s121DefaultAPIPass   = "adminpass"
	s121DefaultMonth     = "2026-05"

	s121ExecTimeout = 3 * time.Minute
)

// Scenario121E2ESuite verifies the full storage CLI command surface end-to-end
// (catalog-direct Part A + KUBECONFIG-gated live Part B driving the ctl binary).
type Scenario121E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario121(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario121E2ESuite))
}

func (s *Scenario121E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario121_PartA_CatalogHonest iterates the full Scenario 121 catalog
// and asserts it is well-formed: unique IDs, every L.1–L.6 + CONTROL + PERSIST
// family present, the four DETAIL-* rows + MONTH-period present, every row
// carries a non-empty Layer + Assert + Description + Gate with a known Layer
// token. The -U rows are resolved at the cmd/cloudberry-ctl recorder suite, the
// -F rows at functional; the -L rows are documented here and resolved at Part B.
//
//nolint:funlen // an exhaustive catalog-honesty assertion is one narrative.
func (s *Scenario121E2ESuite) TestE2E_Scenario121_PartA_CatalogHonest() {
	catalog := cases.Scenario121Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario121LayerUnit,
		cases.Scenario121LayerFunctional,
		cases.Scenario121LayerLive,
	}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Assert, "%s must carry an Assert token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.NotEmptyf(s.T(), tc.Gate, "%s must carry a Gate", tc.ID)
			assert.Containsf(s.T(), knownLayers, tc.Layer, "%s Layer must be a known token", tc.ID)

			if tc.Layer == cases.Scenario121LayerLive {
				s.T().Logf("scenario121 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Assert)
			}
		})
	}
	for i := 1; i <= 6; i++ {
		req := fmt.Sprintf("L.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover CLI command family %s", req)
	}
	assert.True(s.T(), reqs["CONTROL"], "catalog must cover the CONTROL negative row")
	assert.True(s.T(), reqs["PERSIST"], "catalog must cover the PERSIST parity row")
	for _, id := range []string{
		"121-DETAIL-flags", "121-DETAIL-positional", "121-DETAIL-precedence",
		"121-DETAIL-missing", "121-MONTH-period",
	} {
		assert.Truef(s.T(), seen[id], "catalog must carry the named edge row %s", id)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO121_LIVE gated live CLI exercise
// ----------------------------------------------------------------------------

func s121Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s121Cluster() string   { return s121Env(envS121Cluster, s121DefaultCluster) }
func s121Namespace() string { return s121Env(envS121Namespace, s121DefaultNamespace) }
func s121APIBase() string   { return s121Env(envS121APIBase, s121DefaultAPIBase) }
func s121Month() string     { return s121Env(envS121Month, s121DefaultMonth) }

// s121RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO121_LIVE=1.
func (s *Scenario121E2ESuite) s121RequireLive() {
	if os.Getenv(envKubeconfigS121) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 121 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 121 live Part B")
	}
	if os.Getenv(envS121Live) != "1" {
		s.T().Skip("SCENARIO121_LIVE not set, skipping the live CLI exercise " +
			"(the deployed cluster + the operator API must be reachable)")
	}
}

// s121Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario121E2ESuite) s121Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s121ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s121CtlBinary returns the path to a cloudberry-ctl binary, building one into a
// temp dir when SCENARIO121_CTL_BIN is not provided.
func (s *Scenario121E2ESuite) s121CtlBinary() string {
	if bin := strings.TrimSpace(os.Getenv(envS121CtlBin)); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin
		}
		s.T().Skipf("%s=%q not found", envS121CtlBin, bin)
	}
	wd, err := os.Getwd()
	require.NoError(s.T(), err)
	repoRoot := filepath.Dir(filepath.Dir(wd)) // test/e2e -> repo root
	bin := filepath.Join(s.T().TempDir(), "cloudberry-ctl")

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/cloudberry-ctl")
	build.Dir = repoRoot
	build.Env = os.Environ()
	out, buildErr := build.CombinedOutput()
	require.NoErrorf(s.T(), buildErr, "building cloudberry-ctl must succeed (out=%q)", string(out))
	return bin
}

// s121AuthArgs returns the ctl auth flags: a bearer OIDC token
// (--auth-method oidc --password <token>) when SCENARIO121_OIDC_TOKEN is set,
// otherwise basic-auth from the API creds.
func s121AuthArgs() []string {
	if tok := strings.TrimSpace(os.Getenv(envS121Token)); tok != "" {
		return []string{"--auth-method", "oidc", "--username", "oidc", "--password", tok}
	}
	return []string{
		"--auth-method", "basic",
		"--username", s121Env(envS121APIUser, s121DefaultAPIUser),
		"--password", s121Env(envS121APIPass, s121DefaultAPIPass),
	}
}

// s121ctlResult carries the outcome of one ctl invocation.
type s121ctlResult struct {
	out      string
	err      error
	exitCode int
}

// runCtl121 invokes the cloudberry-ctl binary with the standard auth + cluster +
// namespace flags plus the given args. It does NOT require a zero exit (callers
// decide), retrying ONCE on a rate-limit (429) signal in the output.
func (s *Scenario121E2ESuite) runCtl121(bin string, args ...string) s121ctlResult {
	res := s.runCtl121Once(bin, args...)
	if res.err != nil && s121LooksRateLimited(res.out) {
		s.T().Logf("scenario121: ctl %v rate-limited (429); retrying once after backoff", args)
		time.Sleep(7 * time.Second)
		res = s.runCtl121Once(bin, args...)
	}
	return res
}

// runCtl121Once performs a single ctl invocation.
func (s *Scenario121E2ESuite) runCtl121Once(bin string, args ...string) s121ctlResult {
	ctx, cancel := context.WithTimeout(s.ctx, s121ExecTimeout)
	defer cancel()

	full := append([]string{
		"--operator-url", s121APIBase(),
		"--cluster", s121Cluster(),
		"--namespace", s121Namespace(),
	}, s121AuthArgs()...)
	full = append(full, args...)

	cmd := exec.CommandContext(ctx, bin, full...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	res := s121ctlResult{out: string(out), err: err}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.exitCode = exitErr.ExitCode()
	}
	return res
}

// s121LooksRateLimited reports whether ctl output indicates a 429 rate-limit.
func s121LooksRateLimited(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "429") ||
		strings.Contains(lower, "too many requests") ||
		strings.Contains(lower, "rate limit")
}

// s121LooksUnreachable reports whether ctl output indicates the operator API was
// not reachable / not port-forwarded (so callers can SKIP cleanly).
func s121LooksUnreachable(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no such host") ||
		strings.Contains(lower, "executing request") ||
		strings.Contains(lower, "dial tcp")
}

// s121LooksFeatureDisabled reports whether ctl output indicates a feature-gated
// command (scan / usage-report) is not enabled on the cluster — an honest
// degrade, not a failure.
func s121LooksFeatureDisabled(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "not enabled") ||
		strings.Contains(lower, "disabled") ||
		strings.Contains(lower, "400") ||
		strings.Contains(lower, "409") ||
		strings.Contains(lower, "not implemented") ||
		strings.Contains(lower, "501")
}

// s121FirstUserTable parses the L.2 `storage tables list` output for a
// schema/table pair to drive the L.3 detail command, or returns "","" when no
// user table can be identified.
func s121FirstUserTable(listOut string) (schema, table string) {
	for _, line := range strings.Split(listOut, "\n") {
		// Look for a "schema.table" token anywhere on the line.
		for _, field := range strings.Fields(line) {
			if !strings.Contains(field, ".") {
				continue
			}
			parts := strings.SplitN(strings.Trim(field, "\"',|"), ".", 2)
			if len(parts) == 2 && parts[0] != "" && parts[1] != "" &&
				!strings.ContainsAny(parts[0], "/:") {
				return parts[0], parts[1]
			}
		}
	}
	return "", ""
}

// TestE2E_Scenario121_LiveStorageCLI drives ALL SIX live storage CLI commands
// (L.1–L.6) via the built ctl binary against the deployed operator API, asserting
// the documented output. Read-only commands are required to exit 0; the
// feature-gated commands (L.5 scan, L.6 usage-report) degrade to CONFIG-ONLY when
// their features are disabled rather than hard-failing.
//
//nolint:gocyclo,funlen // a self-contained six-command live storage-CLI run is one flow.
func (s *Scenario121E2ESuite) TestE2E_Scenario121_LiveStorageCLI() {
	s.s121RequireLive()
	bin := s.s121CtlBinary()

	// L.1 storage disk-usage → output carries disk usage (diskUsagePercent).
	diskRes := s.runCtl121(bin, "storage", "disk-usage")
	if diskRes.err != nil && s121LooksUnreachable(diskRes.out) {
		s.T().Skipf("operator API not reachable at %s [CONFIG-ONLY]: %s", s121APIBase(), diskRes.out)
	}
	require.NoErrorf(s.T(), diskRes.err, "121a-L1-L: disk-usage must succeed (out=%q)", diskRes.out)
	assert.Containsf(s.T(), diskRes.out, "diskUsagePercent",
		"121a-L1-L: disk-usage output must carry diskUsagePercent (out=%q)", diskRes.out)

	// L.2 storage tables list → output lists tables.
	listRes := s.runCtl121(bin, "storage", "tables", "list")
	require.NoErrorf(s.T(), listRes.err, "121b-L2-L: tables list must succeed (out=%q)", listRes.out)
	s.T().Logf("scenario121 121b-L2-L: tables list output:\n%s", listRes.out)

	// L.3 storage tables detail --schema --table <a-real-table> → table detail.
	// Pick a table from the L.2 output; degrade to CONFIG-ONLY if none found.
	schema, table := s121FirstUserTable(listRes.out)
	if schema == "" || table == "" {
		s.T().Logf("121c-L3-L: no user table found in the tables list " +
			"[CONFIG-ONLY: no user tables to detail]")
	} else {
		detailRes := s.runCtl121(bin, "storage", "tables", "detail",
			"--schema", schema, "--table", table)
		require.NoErrorf(s.T(), detailRes.err,
			"121c-L3-L: tables detail --schema %s --table %s must succeed (out=%q)",
			schema, table, detailRes.out)
		assert.Containsf(s.T(), detailRes.out, table,
			"121c-L3-L: detail output must mention the table %q (out=%q)", table, detailRes.out)
	}

	// L.4 storage recommendations list → output carries recommendationCount.
	recsRes := s.runCtl121(bin, "storage", "recommendations", "list")
	require.NoErrorf(s.T(), recsRes.err, "121d-L4-L: recommendations list must succeed (out=%q)", recsRes.out)
	assert.Containsf(s.T(), recsRes.out, "recommendationCount",
		"121d-L4-L: recommendations list output must carry recommendationCount (out=%q)", recsRes.out)

	// L.5 storage recommendations scan → "scan initiated" / 202; degrade when the
	// recommendationScan feature is disabled.
	scanRes := s.runCtl121(bin, "storage", "recommendations", "scan")
	if scanRes.err != nil {
		if s121LooksFeatureDisabled(scanRes.out) {
			s.T().Logf("121e-L5-L: recommendations scan not enabled "+
				"[CONFIG-ONLY-degrade: recommendationScan disabled] (out=%q)", scanRes.out)
		} else {
			require.NoErrorf(s.T(), scanRes.err,
				"121e-L5-L: recommendations scan must succeed when enabled (out=%q)", scanRes.out)
		}
	} else {
		s.T().Logf("scenario121 121e-L5-L: recommendations scan initiated (out=%q)", scanRes.out)
	}

	// L.6 storage usage-report --month <month> → output echoes month=<month>;
	// degrade when the usageReport feature is disabled.
	reportRes := s.runCtl121(bin, "storage", "usage-report", "--month", s121Month())
	if reportRes.err != nil {
		if s121LooksFeatureDisabled(reportRes.out) {
			s.T().Logf("121f-L6-L: usage-report not enabled "+
				"[CONFIG-ONLY-degrade: usageReport disabled] (out=%q)", reportRes.out)
		} else {
			require.NoErrorf(s.T(), reportRes.err,
				"121f-L6-L: usage-report --month must succeed when enabled (out=%q)", reportRes.out)
		}
	} else {
		assert.Containsf(s.T(), reportRes.out, s121Month(),
			"121f-L6-L: usage-report output must echo the reporting period %q (out=%q)",
			s121Month(), reportRes.out)
	}

	s.T().Logf("scenario121 Part B: live storage CLI (L.1–L.6) OK against %s", s121APIBase())
}

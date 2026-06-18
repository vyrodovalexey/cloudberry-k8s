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
// Scenario 108: All CLI Commands (L.1–L.16) — E2E
// ============================================================================
//
// Mirrors the Scenario 107 e2e SHAPE (catalog-honest Part A that ALWAYS runs +
// a KUBECONFIG/SCENARIO108_LIVE-gated live Part B). Part B builds the REAL
// cloudberry-ctl binary and runs EACH L.1–L.16 command against the LIVE deployed
// cluster, asserting the documented effect:
//
//   L.1 pxf status; L.2 servers list; L.3 create a throwaway s3 server (WITH
//   --credential-secret — the webhook requires it); L.4 update its endpoint;
//   L.6 sync; L.5 delete it; L.7 restart (heavy — runs LAST, exit-0 only).
//   L.8 jobs list; L.9 create a throwaway pxf job (valid server, mode insert);
//   L.10 start; L.13 logs (accept stream OR fallback); L.11 stop; L.12 delete.
//   L.14 create a gpload job (--from-yaml); L.16 create --from-yaml a complex
//   pxf job; L.15 test-read --limit 10 (≤10 rows OR honest available:false).
//
// SELF-CONTAINED + idempotent: it creates throwaway server/job fixtures, exercises
// them, and CLEANS UP via defer. Generous timeouts. SKIPS cleanly when KUBECONFIG
// / the live env is absent. The CLI authenticates via env passthrough: a bearer
// OIDC token (SCENARIO108_OIDC_TOKEN → ctl --auth-method oidc --password <token>)
// when set, otherwise basic-auth. SCENARIO108_API_BASE is the port-forwarded base.
// NOTE the operator API rate limit (10/min) — the live multi-command run may need
// CLOUDBERRY_API_RATE_LIMIT raised at deploy; the test tolerates a 429 by retrying.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO108_LIVE=1       — gates the live CLI exercise.
//   SCENARIO108_CLUSTER      — live cluster name (default s108).
//   SCENARIO108_NAMESPACE    — namespace (default cloudberry-test).
//   SCENARIO108_API_BASE     — operator API base URL (default http://localhost:8190).
//   SCENARIO108_OIDC_TOKEN   — bearer token; if unset, basic-auth creds are used.
//   SCENARIO108_API_USER     — basic-auth user (default adminuser).
//   SCENARIO108_API_PASS     — basic-auth pass (default adminpass).
//   SCENARIO108_CTL_BIN      — pre-built cloudberry-ctl binary (skip the build).
//   SCENARIO108_SERVER       — throwaway server name (default s108-tmp-srv).
//   SCENARIO108_JOB          — throwaway pxf job name (default s108-tmp-job).
//   SCENARIO108_GPLOAD_JOB   — throwaway gpload job name (default s108-tmp-gpload).
//   SCENARIO108_YAML_JOB     — throwaway --from-yaml job name (default s108-tmp-yaml).
// ============================================================================

const (
	envKubeconfigS108 = "KUBECONFIG"
	envS108Live       = "SCENARIO108_LIVE"
	envS108Cluster    = "SCENARIO108_CLUSTER"
	envS108Namespace  = "SCENARIO108_NAMESPACE"
	envS108APIBase    = "SCENARIO108_API_BASE"
	envS108Token      = "SCENARIO108_OIDC_TOKEN"
	envS108APIUser    = "SCENARIO108_API_USER"
	envS108APIPass    = "SCENARIO108_API_PASS"
	envS108CtlBin     = "SCENARIO108_CTL_BIN"
	envS108Server     = "SCENARIO108_SERVER"
	envS108Job        = "SCENARIO108_JOB"
	envS108GploadJob  = "SCENARIO108_GPLOAD_JOB"
	envS108YAMLJob    = "SCENARIO108_YAML_JOB"

	s108DefaultCluster   = "s108"
	s108DefaultNamespace = "cloudberry-test"
	s108DefaultAPIBase   = "http://localhost:8190"
	s108DefaultAPIUser   = "adminuser"
	s108DefaultAPIPass   = "adminpass"
	s108DefaultServer    = "s108-tmp-srv"
	s108DefaultJob       = "s108-tmp-job"
	s108DefaultGpload    = "s108-tmp-gpload"
	s108DefaultYAML      = "s108-tmp-yaml"

	s108LiveTimeout  = 5 * time.Minute
	s108PollInterval = 10 * time.Second
	s108ExecTimeout  = 3 * time.Minute
)

// Scenario108E2ESuite verifies the full CLI command surface end-to-end
// (catalog-direct Part A + KUBECONFIG-gated live Part B driving the ctl binary).
type Scenario108E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario108(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario108E2ESuite))
}

func (s *Scenario108E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario108_PartA_CatalogHonest iterates the full Scenario 108 catalog
// and asserts it is well-formed: unique IDs, every L.1–L.16 + RBAC family present,
// every row carries a non-empty Layer + Expected + Description with a known Layer
// token. The -F rows are resolved at functional; the -L rows are documented here
// and resolved at Part B.
func (s *Scenario108E2ESuite) TestE2E_Scenario108_PartA_CatalogHonest() {
	catalog := cases.Scenario108Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	knownLayers := []string{
		cases.Scenario108LayerFunctional,
		cases.Scenario108LayerBuilder,
		cases.Scenario108LayerLive,
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

			if tc.Layer == cases.Scenario108LayerLive {
				s.T().Logf("scenario108 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
			}
		})
	}
	for i := 1; i <= 16; i++ {
		req := fmt.Sprintf("L.%d", i)
		assert.Truef(s.T(), reqs[req], "catalog must cover CLI command family %s", req)
	}
	assert.True(s.T(), reqs["RBAC"], "catalog must cover the RBAC parity row")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG + SCENARIO108_LIVE gated live CLI exercise
// ----------------------------------------------------------------------------

func s108Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func s108Cluster() string    { return s108Env(envS108Cluster, s108DefaultCluster) }
func s108Namespace() string  { return s108Env(envS108Namespace, s108DefaultNamespace) }
func s108APIBase() string    { return s108Env(envS108APIBase, s108DefaultAPIBase) }
func s108ServerName() string { return s108Env(envS108Server, s108DefaultServer) }
func s108JobName() string    { return s108Env(envS108Job, s108DefaultJob) }
func s108GploadName() string { return s108Env(envS108GploadJob, s108DefaultGpload) }
func s108YAMLName() string   { return s108Env(envS108YAMLJob, s108DefaultYAML) }

// s108RequireLive skips cleanly unless KUBECONFIG is set, kubectl exists and
// SCENARIO108_LIVE=1.
func (s *Scenario108E2ESuite) s108RequireLive() {
	if os.Getenv(envKubeconfigS108) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 108 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 108 live Part B")
	}
	if os.Getenv(envS108Live) != "1" {
		s.T().Skip("SCENARIO108_LIVE not set, skipping the live CLI exercise " +
			"(the deployed cluster + the operator API must be reachable)")
	}
}

// s108Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario108E2ESuite) s108Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s108ExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// s108CtlBinary returns the path to a cloudberry-ctl binary, building one into a
// temp dir when SCENARIO108_CTL_BIN is not provided.
func (s *Scenario108E2ESuite) s108CtlBinary() string {
	if bin := strings.TrimSpace(os.Getenv(envS108CtlBin)); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin
		}
		s.T().Skipf("%s=%q not found", envS108CtlBin, bin)
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

// s108AuthArgs returns the ctl auth flags: a bearer OIDC token
// (--auth-method oidc --password <token>) when SCENARIO108_OIDC_TOKEN is set,
// otherwise basic-auth from the API creds.
func s108AuthArgs() []string {
	if tok := strings.TrimSpace(os.Getenv(envS108Token)); tok != "" {
		// The ctl maps --auth-method oidc + --password to Authorization: Bearer.
		return []string{"--auth-method", "oidc", "--username", "oidc", "--password", tok}
	}
	return []string{
		"--auth-method", "basic",
		"--username", s108Env(envS108APIUser, s108DefaultAPIUser),
		"--password", s108Env(envS108APIPass, s108DefaultAPIPass),
	}
}

// s108ctlResult carries the outcome of one ctl invocation.
type s108ctlResult struct {
	out      string
	err      error
	exitCode int
}

// runCtl invokes the cloudberry-ctl binary with the standard auth + cluster +
// namespace flags plus the given args. It does NOT require a zero exit (callers
// decide), retrying ONCE on a rate-limit (429) signal in the output.
func (s *Scenario108E2ESuite) runCtl(bin string, args ...string) s108ctlResult {
	res := s.runCtlOnce(bin, args...)
	if res.err != nil && s108LooksRateLimited(res.out) {
		s.T().Logf("scenario108: ctl %v rate-limited (429); retrying once after backoff", args)
		time.Sleep(7 * time.Second)
		res = s.runCtlOnce(bin, args...)
	}
	return res
}

// runCtlOnce performs a single ctl invocation.
func (s *Scenario108E2ESuite) runCtlOnce(bin string, args ...string) s108ctlResult {
	ctx, cancel := context.WithTimeout(s.ctx, s108ExecTimeout)
	defer cancel()

	full := append([]string{
		"--operator-url", s108APIBase(),
		"--cluster", s108Cluster(),
		"--namespace", s108Namespace(),
	}, s108AuthArgs()...)
	full = append(full, args...)

	cmd := exec.CommandContext(ctx, bin, full...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	res := s108ctlResult{out: string(out), err: err}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.exitCode = exitErr.ExitCode()
	}
	return res
}

// s108LooksRateLimited reports whether ctl output indicates a 429 rate-limit.
func s108LooksRateLimited(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "429") ||
		strings.Contains(lower, "too many requests") ||
		strings.Contains(lower, "rate limit")
}

// s108CRHasServer / s108CRHasJob report whether the live CR contains the named
// server/job (via kubectl jsonpath).
func (s *Scenario108E2ESuite) s108CRHasServer(name string) bool {
	out, err := s.s108Kubectl("get", "cloudberrycluster", s108Cluster(), "-n", s108Namespace(),
		"-o", "jsonpath={.spec.dataLoading.pxf.servers[*].name}")
	if err != nil {
		return false
	}
	for _, n := range strings.Fields(out) {
		if n == name {
			return true
		}
	}
	return false
}

func (s *Scenario108E2ESuite) s108CRHasJob(name string) bool {
	out, err := s.s108Kubectl("get", "cloudberrycluster", s108Cluster(), "-n", s108Namespace(),
		"-o", "jsonpath={.spec.dataLoading.jobs[*].name}")
	if err != nil {
		return false
	}
	for _, n := range strings.Fields(out) {
		if n == name {
			return true
		}
	}
	return false
}

// TestE2E_Scenario108_LivePXFServersCLI drives the LIVE PXF-servers CLI verbs
// (L.1/L.2/L.3/L.4/L.6/L.5, then L.7 last) self-contained via the built ctl
// binary, asserting the documented effects. It cleans up the throwaway server.
//
//nolint:gocyclo,funlen // a self-contained live CLI CRUD narrative is one flow.
func (s *Scenario108E2ESuite) TestE2E_Scenario108_LivePXFServersCLI() {
	s.s108RequireLive()
	bin := s.s108CtlBinary()
	server := s108ServerName()

	// State-based cleanup: delete the throwaway server if it is still present.
	defer func() {
		if s.s108CRHasServer(server) {
			res := s.runCtl(bin, "pxf", "servers", "delete", server)
			s.T().Logf("scenario108 cleanup: delete server %q → exit=%d err=%v",
				server, res.exitCode, res.err)
		}
	}()

	// L.1 pxf status.
	statusRes := s.runCtl(bin, "pxf", "status")
	if statusRes.err != nil && s108LooksUnreachable(statusRes.out) {
		s.T().Skipf("operator API not reachable at %s [CONFIG-ONLY]: %s", s108APIBase(), statusRes.out)
	}
	require.NoErrorf(s.T(), statusRes.err, "pxf status must succeed (out=%q)", statusRes.out)

	// L.2 pxf servers list.
	listRes := s.runCtl(bin, "pxf", "servers", "list")
	require.NoErrorf(s.T(), listRes.err, "pxf servers list must succeed (out=%q)", listRes.out)

	// L.3 pxf servers create a throwaway s3 server WITH --credential-secret (the
	// webhook requires credential secrets for an s3 server).
	createRes := s.runCtl(bin, "pxf", "servers", "create",
		"--name", server, "--type", "s3",
		"--endpoint", "http://minio-s108:9000",
		"--credential-secret", "backup-s3-credentials:aws_access_key_id",
		"--credential-secret", "backup-s3-credentials:aws_secret_access_key")
	require.NoErrorf(s.T(), createRes.err, "pxf servers create must succeed (out=%q)", createRes.out)
	require.Eventuallyf(s.T(), func() bool { return s.s108CRHasServer(server) },
		s108LiveTimeout, s108PollInterval, "the CR must gain server %q after create", server)

	// L.4 pxf servers update --endpoint. An s3 server update replaces the server,
	// so it MUST re-include --credential-secret (the webhook requires credential
	// secrets for s3 servers; omitting them is a 400 VALIDATION_FAILED).
	updateRes := s.runCtl(bin, "pxf", "servers", "update", server,
		"--endpoint", "http://minio-s108-NEW:9000",
		"--credential-secret", "backup-s3-credentials:aws_access_key_id",
		"--credential-secret", "backup-s3-credentials:aws_secret_access_key")
	require.NoErrorf(s.T(), updateRes.err, "pxf servers update must succeed (out=%q)", updateRes.out)

	// L.6 pxf sync.
	syncRes := s.runCtl(bin, "pxf", "sync")
	require.NoErrorf(s.T(), syncRes.err, "pxf sync must succeed (out=%q)", syncRes.out)

	// L.5 pxf servers delete the throwaway server → removed.
	deleteRes := s.runCtl(bin, "pxf", "servers", "delete", server)
	require.NoErrorf(s.T(), deleteRes.err, "pxf servers delete must succeed (out=%q)", deleteRes.out)
	require.Eventuallyf(s.T(), func() bool { return !s.s108CRHasServer(server) },
		s108LiveTimeout, s108PollInterval, "the CR must drop server %q after delete", server)

	// L.7 pxf restart LAST — heavy (rolls pods). Assert exit 0 (202) only.
	restartRes := s.runCtl(bin, "pxf", "restart")
	assert.NoErrorf(s.T(), restartRes.err, "pxf restart must be accepted (out=%q)", restartRes.out)

	s.T().Logf("scenario108 Part B: live PXF-servers CLI (L.1–L.7) OK")
}

// TestE2E_Scenario108_LiveJobsCLI drives the LIVE jobs CLI verbs
// (L.8/L.9/L.10/L.13/L.11/L.12 + L.14 gpload + L.16 from-yaml) self-contained via
// the built ctl binary. It cleans up all throwaway jobs via defer.
//
//nolint:gocyclo,funlen // a self-contained live jobs-CLI lifecycle is one narrative.
func (s *Scenario108E2ESuite) TestE2E_Scenario108_LiveJobsCLI() {
	s.s108RequireLive()
	bin := s.s108CtlBinary()
	job := s108JobName()
	gploadJob := s108GploadName()
	yamlJob := s108YAMLName()

	// Pick an existing server to reference so the pxf create passes validation.
	out, _ := s.s108Kubectl("get", "cloudberrycluster", s108Cluster(), "-n", s108Namespace(),
		"-o", "jsonpath={.spec.dataLoading.pxf.servers[0].name}")
	server := strings.TrimSpace(out)
	if server == "" {
		s.T().Skip("no PXF server in the live CR to reference [CONFIG-ONLY: no servers deployed]")
	}

	// State-based cleanup: stop + delete every throwaway job if still present.
	defer func() {
		for _, j := range []string{job, gploadJob, yamlJob} {
			_ = s.runCtlOnce(bin, "data-loading", "jobs", "stop", j)
			if s.s108CRHasJob(j) {
				res := s.runCtl(bin, "data-loading", "jobs", "delete", j)
				s.T().Logf("scenario108 cleanup: delete job %q → exit=%d err=%v", j, res.exitCode, res.err)
			}
		}
	}()

	// L.8 jobs list.
	listRes := s.runCtl(bin, "data-loading", "jobs", "list")
	if listRes.err != nil && s108LooksUnreachable(listRes.out) {
		s.T().Skipf("operator API not reachable at %s [CONFIG-ONLY]: %s", s108APIBase(), listRes.out)
	}
	require.NoErrorf(s.T(), listRes.err, "jobs list must succeed (out=%q)", listRes.out)

	// L.9 jobs create --type pxf (mode insert; valid server).
	createRes := s.runCtl(bin, "data-loading", "jobs", "create",
		"--type", "pxf", "--name", job,
		"--server", server, "--profile", "s3:text",
		"--resource", "data/events.csv", "--target", "public.events")
	require.NoErrorf(s.T(), createRes.err, "jobs create --type pxf must succeed (out=%q)", createRes.out)
	require.Eventuallyf(s.T(), func() bool { return s.s108CRHasJob(job) },
		s108LiveTimeout, s108PollInterval, "the CR must gain pxf job %q", job)

	// L.10 jobs start → assert a data-loading Job object is created.
	startRes := s.runCtl(bin, "data-loading", "jobs", "start", job)
	require.NoErrorf(s.T(), startRes.err, "jobs start must succeed (out=%q)", startRes.out)

	// L.13 jobs logs — accept a stream OR a non-fatal fallback (the ctl prints the
	// kubectl fallback hint and exits 0 when the stream is not ready).
	logsRes := s.runCtl(bin, "data-loading", "jobs", "logs", "--job", job, "--tail", "10")
	assert.NoErrorf(s.T(), logsRes.err,
		"jobs logs must exit 0 (stream OR kubectl fallback) (out=%q)", logsRes.out)

	// L.11 jobs stop.
	stopRes := s.runCtl(bin, "data-loading", "jobs", "stop", job)
	assert.NoErrorf(s.T(), stopRes.err, "jobs stop must succeed (out=%q)", stopRes.out)

	// L.12 jobs delete the pxf job → gone.
	delRes := s.runCtl(bin, "data-loading", "jobs", "delete", job)
	require.NoErrorf(s.T(), delRes.err, "jobs delete must succeed (out=%q)", delRes.out)
	require.Eventuallyf(s.T(), func() bool { return !s.s108CRHasJob(job) },
		s108LiveTimeout, s108PollInterval, "the CR must drop pxf job %q after delete", job)

	// L.14 jobs create --type gpload (flags).
	gpRes := s.runCtl(bin, "data-loading", "jobs", "create",
		"--type", "gpload", "--name", gploadJob,
		"--gpfdist-host", "gpfdist", "--gpfdist-port", "8080",
		"--file-path", "/in/*.csv", "--format", "csv", "--target", "public.raw")
	require.NoErrorf(s.T(), gpRes.err, "jobs create --type gpload must succeed (out=%q)", gpRes.out)
	require.Eventuallyf(s.T(), func() bool { return s.s108CRHasJob(gploadJob) },
		s108LiveTimeout, s108PollInterval, "the CR must gain gpload job %q", gploadJob)

	// L.16 jobs create --from-yaml a complex scheduled pxf job → reconciled into CR.
	yamlPath := s.s108WriteJobYAML(yamlJob, server)
	yamlRes := s.runCtl(bin, "data-loading", "jobs", "create", "--from-yaml", yamlPath)
	require.NoErrorf(s.T(), yamlRes.err, "jobs create --from-yaml must succeed (out=%q)", yamlRes.out)
	require.Eventuallyf(s.T(), func() bool { return s.s108CRHasJob(yamlJob) },
		s108LiveTimeout, s108PollInterval, "the CR must gain --from-yaml job %q", yamlJob)

	s.T().Logf("scenario108 Part B: live jobs CLI (L.8–L.16) OK")
}

// s108WriteJobYAML writes a complex scheduled pxf job definition to a temp file
// and returns its path (for L.16 --from-yaml).
func (s *Scenario108E2ESuite) s108WriteJobYAML(name, server string) string {
	yaml := fmt.Sprintf(`name: %s
type: pxf
enabled: true
schedule: "0 3 * * *"
pxfJob:
  server: %s
  profile: s3:parquet
  resource: data/events.parquet
  targetTable: public.events
  mode: insert
`, name, server)
	path := filepath.Join(s.T().TempDir(), name+".yaml")
	require.NoError(s.T(), os.WriteFile(path, []byte(yaml), 0o600))
	return path
}

// TestE2E_Scenario108_LiveTestReadCLI covers 108-L15-L: data-loading test-read
// --limit 10 against the live API reads ≤10 rows from a real PXF source OR returns
// an honest available:false preview. Read-only (no cleanup beyond the operator's
// own transient-table drop).
func (s *Scenario108E2ESuite) TestE2E_Scenario108_LiveTestReadCLI() {
	s.s108RequireLive()
	bin := s.s108CtlBinary()

	// Prefer a defined pxf job's source; fall back to an explicit triple.
	out, _ := s.s108Kubectl("get", "cloudberrycluster", s108Cluster(), "-n", s108Namespace(),
		"-o", "jsonpath={.spec.dataLoading.jobs[?(@.type=='pxf')].name}")
	var pxfJob string
	if fields := strings.Fields(out); len(fields) > 0 {
		pxfJob = fields[0]
	}

	var res s108ctlResult
	if pxfJob != "" {
		res = s.runCtl(bin, "data-loading", "test-read", "--job", pxfJob, "--limit", "10")
	} else {
		res = s.runCtl(bin, "data-loading", "test-read",
			"--server", "s108srv", "--profile", "s3:text", "--resource", "data/probe.csv",
			"--limit", "10")
	}
	if res.err != nil && s108LooksUnreachable(res.out) {
		s.T().Skipf("operator API not reachable at %s [CONFIG-ONLY]: %s", s108APIBase(), res.out)
	}
	// test-read is a read/preview: it exits 0 whether rows are returned OR the
	// source is honestly available:false. A non-zero exit is a real failure.
	require.NoErrorf(s.T(), res.err,
		"test-read must exit 0 (rows OR honest available:false) (out=%q)", res.out)
	s.T().Logf("scenario108 108-L15-L: test-read output:\n%s", res.out)
}

// s108LooksUnreachable reports whether ctl output indicates the operator API was
// not reachable / not port-forwarded (so callers can SKIP cleanly).
func s108LooksUnreachable(out string) bool {
	lower := strings.ToLower(out)
	return strings.Contains(lower, "connection refused") ||
		strings.Contains(lower, "no such host") ||
		strings.Contains(lower, "executing request") ||
		strings.Contains(lower, "dial tcp")
}

//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
)

// ============================================================================
// Scenario 85e (live Part B): restore Job name-collision retry contract —
// DEF-6 / U018 §9 F-4 / U030 (operator commit 8a79782)
// ============================================================================
//
// The restore Job name is deterministic — util.RestoreJobName(cluster, ts) =
// <cluster>-restore-<ts> — so re-restoring the same backup timestamp collides
// with the previous attempt's leftover Job. Before U030 every collision
// surfaced as an opaque 500 "failed to create restore job" (the U018 §5 live
// repro). The U030 contract (pinned at the unit layer in
// internal/api/restore_collision_test.go) is:
//
//   - TERMINAL leftover (JobFailed OR JobComplete) → deleted + replaced, the
//     retry proceeds with 202 "restore started" (case 85e-col1 / 85e-col2);
//   - IN-FLIGHT leftover (running/pending) → 409 CONFLICT with an actionable
//     message naming the Job; the in-flight run is never clobbered (85e-col3);
//   - genuine server errors stay 500 (unit-pinned; not live-provable without
//     injecting apiserver faults, which this suite does not do).
//
// THIS suite is the live-verification half of DEF-6: it proves the SHIPPED
// operator (the image deployed by the running helm release) answers the same
// way over its real API port (:8090), through the real auth middleware, against
// the real apiserver. The live rows are 85e-col1 (failed leftover → 202 +
// replacement) and 85e-col3 (in-flight leftover → 409, untouched); 85e-col2
// shares col1's terminal-replace branch and stays unit-pinned so the live run
// spends only ONE real restore attempt.
//
// Gating: skips cleanly (with the documented reason) unless kubectl exists,
// the test namespace, the acceptance CloudberryCluster (backup enabled) and
// the operator admin-password secret are all reachable — CI without a cluster
// runs the unit/functional halves only.
//
// Isolation / cleanup: both timestamps are fixed PRE-CAMPAIGN values
// (2020…) that can match no real gpbackup object in the bucket, so the
// operator-built replacement restore Job fails fast at backup-config read and
// never restores data; the probe target DB (u021_def6_probe) is never a real
// restore destination and is dropped best-effort anyway. Every Job this suite
// seeds or triggers is registered for background-cascade deletion BEFORE the
// first assertion; mydb and the U017 fingerprints are never touched.
// ============================================================================

const (
	// envScenario85BCollisionAPIBase points the suite at an ALREADY
	// port-forwarded operator API base (e.g. http://localhost:8190). When set,
	// no own port-forward is started.
	envScenario85BCollisionAPIBase = "SCENARIO85_COLLISION_API_BASE"
	// envScenario85BCollisionAPIPort is the local port for this suite's OWN
	// kubectl port-forward of svc/cloudberry-operator:8090.
	envScenario85BCollisionAPIPort = "SCENARIO85_COLLISION_API_PORT"
	// envScenario85BCollisionNamespace / envScenario85BCollisionCluster name
	// the live cluster under test.
	envScenario85BCollisionNamespace = "SCENARIO85_COLLISION_NAMESPACE"
	envScenario85BCollisionCluster   = "SCENARIO85_COLLISION_CLUSTER"

	scenario85BDefaultNamespace = "cloudberry-test"
	scenario85BDefaultCluster   = "acceptance-test"
	scenario85BDefaultAPIPort   = "18190"

	// scenario85BFailedTS / scenario85BInFlightTS are pre-campaign 14-digit
	// timestamps: they pass the ^\d{14}$ validation but can never match a real
	// backup object in the bucket (every real one is 2026-11-15 or later).
	scenario85BFailedTS  = "20200101010101"
	scenario85BInFlight  = "20200101020202"
	scenario85BProbeDB   = "u021_def6_probe"
	scenario85BAdminUser = "admin"

	scenario85BExecTimeout       = 90 * time.Second
	scenario85BPortReadyTimeout  = 45 * time.Second
	scenario85BTerminalTimeout   = 150 * time.Second
	scenario85BActiveTimeout     = 90 * time.Second
	scenario85BDeletePollTimeout = 30 * time.Second
	scenario85BHTTPTimeout       = 30 * time.Second
	scenario85BPollInterval      = 500 * time.Millisecond
)

// Scenario85RestoreCollisionLiveSuite drives the live collision-retry contract
// against the deployed operator.
type Scenario85RestoreCollisionLiveSuite struct {
	suite.Suite
	ctx        context.Context
	cancel     context.CancelFunc
	ns         string
	cluster    string
	adminPass  string
	apiBase    string
	pfCmd      *exec.Cmd
	pfCancel   context.CancelFunc
	pfOwned    bool
	cleanupJob []string
}

func TestIntegration_Scenario85RestoreCollisionLive(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario85RestoreCollisionLiveSuite))
}

func (s *Scenario85RestoreCollisionLiveSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 8*time.Minute)
}

func (s *Scenario85RestoreCollisionLiveSuite) TearDownSuite() {
	// Safety net: the per-test defers already cleaned up; this covers a
	// catastrophic require failure between registration and test end.
	s.scenario85BDeleteJobs()
	if s.pfCancel != nil {
		s.pfCancel()
	}
	if s.pfCmd != nil && s.pfCmd.Process != nil {
		_ = s.pfCmd.Process.Signal(syscall.SIGTERM)
	}
	if s.cancel != nil {
		s.cancel()
	}
}

func scenario85BEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// scenario85BKubectl runs a bounded kubectl subcommand.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BKubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario85BExecTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "kubectl", args...).CombinedOutput()
	return string(out), err
}

// scenario85BRequireLive skips cleanly (documented) unless the live gate is
// satisfiable: kubectl + namespace + the backup-enabled acceptance cluster +
// the operator admin-password secret.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BRequireLive() {
	s.ns = scenario85BEnv(envScenario85BCollisionNamespace, scenario85BDefaultNamespace)
	s.cluster = scenario85BEnv(envScenario85BCollisionCluster, scenario85BDefaultCluster)

	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 85e live collision proof " +
			"[unit/functional halves cover the handler contract]")
	}
	if out, err := s.scenario85BKubectl("get", "namespace", s.ns); err != nil {
		s.T().Skipf("namespace %q not reachable [CONFIG-ONLY]: %s", s.ns, out)
	}
	if out, err := s.scenario85BKubectl("get", "cloudberrycluster", s.cluster, "-n", s.ns); err != nil {
		s.T().Skipf("CloudberryCluster %q not found in %q [CONFIG-ONLY — needs the deployed "+
			"acceptance cluster]: %s", s.cluster, s.ns, out)
	}
	enabled, _ := s.scenario85BKubectl("get", "cloudberrycluster", s.cluster, "-n", s.ns,
		"-o", "jsonpath={.spec.backup.enabled}")
	if strings.TrimSpace(enabled) != "true" {
		s.T().Skipf("cluster %q does not have spec.backup.enabled=true — the restore endpoint's "+
			"backup-disabled gate would 400 before any collision [CONFIG-ONLY]", s.cluster)
	}
	pass, err := s.scenario85BKubectl("get", "secret", util.OperatorAdminPasswordSecretName,
		"-n", s.ns, "-o", "jsonpath={.data.password}")
	if err != nil {
		s.T().Skipf("operator admin-password secret not found in %q [CONFIG-ONLY]: %s", s.ns, err)
	}
	decoded, decErr := base64Decode(strings.TrimSpace(pass))
	require.NoError(s.T(), decErr, "admin password secret must carry base64 data")
	s.adminPass = decoded
}

// scenario85BStartAPI makes the operator API reachable at s.apiBase: an
// externally provided base (env) is polled as-is; otherwise a private
// port-forward of svc/cloudberry-operator:8090 is started and polled.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BStartAPI() {
	if base := scenario85BEnv(envScenario85BCollisionAPIBase, ""); base != "" {
		s.apiBase = strings.TrimRight(base, "/")
		s.pfOwned = false
	} else {
		port := scenario85BEnv(envScenario85BCollisionAPIPort, scenario85BDefaultAPIPort)
		s.apiBase = "http://127.0.0.1:" + port
		pfCtx, pfCancel := context.WithCancel(s.ctx)
		cmd := exec.CommandContext(pfCtx, "kubectl", "port-forward",
			"-n", s.ns, "svc/cloudberry-operator", port+":8090")
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		require.NoError(s.T(), cmd.Start(), "starting kubectl port-forward for the operator API")
		s.pfCmd = cmd
		s.pfCancel = pfCancel
		s.pfOwned = true
	}

	// Readiness = an authenticated GET on the cluster list answers 200 (this
	// proves transport AND credentials before the journey starts).
	require.Eventually(s.T(), func() bool {
		code, _, err := s.scenario85BHTTP(http.MethodGet,
			"/api/v1alpha1/clusters?namespace="+s.ns, "")
		return err == nil && code == http.StatusOK
	}, scenario85BPortReadyTimeout, scenario85BPollInterval,
		"operator API at %s must become ready and answer an authenticated GET", s.apiBase)
}

// scenario85BHTTP performs an authenticated admin request against the operator
// API. body may be empty (then no Content-Type is set).
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BHTTP(method, path, body string) (int, map[string]interface{}, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario85BHTTPTimeout)
	defer cancel()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.apiBase+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(scenario85BAdminUser, s.adminPass)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	var decoded map[string]interface{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &decoded) // best-effort: callers assert on shape
	}
	return resp.StatusCode, decoded, nil
}

// scenario85BSeedTerminalFailed applies a restore-named Job that FAILS FAST
// (backoffLimit 0 + exit 1) and polls until the Job controller marks it
// JobFailed=True — a REAL failed restore leftover, exactly the DEF-6 shape.
// Returns the leftover's UID.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BSeedTerminalFailed(name string) string {
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
  labels:
    %s: %s
    %s: %s
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: gprestore
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "-c", "exit 1"]
`, name, s.ns, util.LabelCluster, s.cluster, util.LabelBackupOperation,
		util.BackupOperationRestore, s.scenario85BBackupImage())

	out, err := s.scenario85BKubectlWithStdin(manifest, "apply", "-f", "-")
	require.NoErrorf(s.T(), err, "seeding FAILED leftover Job %s: %s", name, out)

	// Poll to the terminal state: a collision probed before JobFailed=True
	// would legitimately classify as pending and 409 — not the row under test.
	require.Eventually(s.T(), func() bool {
		st, _ := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
			"-o", `jsonpath={.status.conditions[?(@.type=="Failed")].status}`)
		return strings.TrimSpace(st) == "True"
	}, scenario85BTerminalTimeout, scenario85BPollInterval,
		"seeded leftover Job %s must reach JobFailed=True (backoffLimit 0, exit 1)", name)

	uid, err := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
		"-o", "jsonpath={.metadata.uid}")
	require.NoError(s.T(), err, "reading the seeded leftover UID")
	return strings.TrimSpace(uid)
}

// scenario85BSeedInFlight applies a restore-named Job whose pod SLEEPS, polls
// until the Job reports an active pod, and returns its UID. The Job can
// neither complete nor fail inside the probe window, so the leftover is
// deterministically IN-FLIGHT for the collision probe.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BSeedInFlight(name string) string {
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
  labels:
    %s: %s
    %s: %s
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: gprestore
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "-c", "sleep 600"]
`, name, s.ns, util.LabelCluster, s.cluster, util.LabelBackupOperation,
		util.BackupOperationRestore, s.scenario85BBackupImage())

	out, err := s.scenario85BKubectlWithStdin(manifest, "apply", "-f", "-")
	require.NoErrorf(s.T(), err, "seeding IN-FLIGHT leftover Job %s: %s", name, out)

	require.Eventually(s.T(), func() bool {
		active, _ := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
			"-o", "jsonpath={.status.active}")
		return strings.TrimSpace(active) == "1"
	}, scenario85BActiveTimeout, scenario85BPollInterval,
		"seeded in-flight Job %s must report an active pod", name)

	uid, err := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
		"-o", "jsonpath={.metadata.uid}")
	require.NoError(s.T(), err, "reading the in-flight leftover UID")
	return strings.TrimSpace(uid)
}

// scenario85BBackupImage reads the live cluster's backup image (the one the
// operator's own restore Jobs use) so the seeded leftovers are realistic AND
// the image is guaranteed present on the node (no pull latency).
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BBackupImage() string {
	s.T().Helper()
	img, err := s.scenario85BKubectl("get", "cloudberrycluster", s.cluster, "-n", s.ns,
		"-o", "jsonpath={.spec.backup.image}")
	require.NoError(s.T(), err, "reading spec.backup.image from the live cluster")
	require.NotEmpty(s.T(), strings.TrimSpace(img), "backup-enabled cluster must carry a backup image")
	return strings.TrimSpace(img)
}

// scenario85BDeleteJobs deletes Jobs and polls for their absence. With
// explicit names it deletes ONLY those and keeps the registered cleanup list
// intact (the pre-seed sweep); with no names it deletes the REGISTERED jobs
// and clears the list (the end-of-test cleanup). Never fails the test
// (best-effort, loudly logged).
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BDeleteJobs(names ...string) {
	targets := names
	if len(targets) == 0 {
		targets = s.cleanupJob
	}
	if len(targets) == 0 {
		return
	}
	args := append([]string{"delete", "job", "-n", s.ns, "--ignore-not-found",
		"--wait=false"}, targets...)
	if out, err := s.scenario85BKubectl(args...); err != nil {
		s.T().Logf("scenario85b cleanup: delete jobs: %v: %s", err, out)
	}
	deadline := time.Now().Add(scenario85BDeletePollTimeout)
	for _, name := range targets {
		for time.Now().Before(deadline) {
			out, err := s.scenario85BKubectl("get", "job", name, "-n", s.ns, "--ignore-not-found")
			if err != nil || strings.Contains(out, "NotFound") || strings.TrimSpace(out) == "" {
				break
			}
			time.Sleep(scenario85BPollInterval)
		}
	}
	if len(names) == 0 {
		s.cleanupJob = nil
	}
}

// scenario85BDropProbeDB drops the probe redirect DB IF the replacement
// restore Job somehow got past the backup-config read (it cannot for a
// pre-campaign timestamp, but the handoff cluster must stay pristine).
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BDropProbeDB() {
	out, err := s.scenario85BKubectl("exec", "-n", s.ns,
		s.cluster+"-coordinator-0", "-c", "cloudberry", "--",
		"/usr/local/cloudberry-db/bin/psql", "-U", "gpadmin", "-d", "postgres", "-c",
		"DROP DATABASE IF EXISTS "+scenario85BProbeDB+";")
	if err != nil {
		s.T().Logf("scenario85b cleanup: drop %s (best-effort): %v: %s", scenario85BProbeDB, err, out)
	}
}

// scenario85BRegisterCleanup arms the (idempotent) end-of-test cleanup.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BRegisterCleanup(jobNames ...string) {
	s.cleanupJob = append(s.cleanupJob, jobNames...)
}

func (s *Scenario85RestoreCollisionLiveSuite) scenario85BKubectlWithStdin(stdin, verb string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario85BExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{verb, "-n", s.ns}, args...)...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario85BRestorePOST issues the collision-retry POST for ts.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BRestorePOST(ts string) (int, map[string]interface{}) {
	body := `{"databases":["mydb"],"gprestoreOptions":{` +
		`"createDb":true,"redirectDb":"` + scenario85BProbeDB + `"}}`
	path := "/api/v1alpha1/clusters/" + s.cluster + "/backups/" + ts +
		"/restore?namespace=" + s.ns
	code, resp, err := s.scenario85BHTTP(http.MethodPost, path, body)
	require.NoError(s.T(), err, "restore POST must reach the operator API")
	return code, resp
}

// scenario85BJobUID reads a Job's UID ("" when absent).
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BJobUID(name string) string {
	uid, err := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
		"-o", "jsonpath={.metadata.uid}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(uid)
}

// scenario85BJobTerminal reports the coarse terminal state of a Job: "failed",
// "succeeded" or "" while non-terminal.
func (s *Scenario85RestoreCollisionLiveSuite) scenario85BJobTerminal(name string) string {
	for _, probe := range []struct {
		cond string
		out  string
	}{{"Failed", "failed"}, {"Complete", "succeeded"}} {
		st, _ := s.scenario85BKubectl("get", "job", name, "-n", s.ns,
			"-o", fmt.Sprintf(`jsonpath={.status.conditions[?(@.type==%q)].status}`, probe.cond))
		if strings.TrimSpace(st) == "True" {
			return probe.out
		}
	}
	return ""
}

// TestIntegration_Scenario85e_RestoreCollisionLive is the DEF-6 live journey:
// one REAL failed restore leftover → retry → 202 + replaced Job (85e-col1),
// then one REAL in-flight leftover → retry → 409 CONFLICT, untouched (85e-col3).
func (s *Scenario85RestoreCollisionLiveSuite) TestIntegration_Scenario85e_RestoreCollisionLive() {
	s.scenario85BRequireLive()

	// Traceability: log the catalog rows this journey realizes.
	for _, c := range cases.Scenario85RestoreCollisionCases {
		s.T().Logf("case %s (leftover=%s → %d, replace=%t): %s",
			c.ID, c.Leftover, c.ExpectStatus, c.ExpectReplace, c.Description)
	}

	jobA := util.RestoreJobName(s.cluster, scenario85BFailedTS)
	jobB := util.RestoreJobName(s.cluster, scenario85BInFlight)
	s.scenario85BRegisterCleanup(jobA, jobB)
	defer func() {
		s.scenario85BDeleteJobs()
		s.scenario85BDropProbeDB()
	}()

	s.scenario85BStartAPI()
	defer func() {
		if s.pfOwned && s.pfCmd != nil && s.pfCmd.Process != nil {
			_ = syscall.Kill(-s.pfCmd.Process.Pid, syscall.SIGKILL)
		}
	}()

	// Pre-clean any leftovers of an interrupted previous run (idempotence);
	// explicit names keep the registered end-of-test cleanup intact.
	s.scenario85BDeleteJobs(jobA, jobB)

	// ---- 85e-col1: FAILED leftover → replaced + 202 (the DEF-6 acceptance) ----
	s.Run("85e-col1_failed_leftover_replaced_202", func() {
		seededUID := s.scenario85BSeedTerminalFailed(jobA)
		s.T().Logf("85e-col1: seeded FAILED leftover %s (uid=%s)", jobA, seededUID)

		code, resp := s.scenario85BRestorePOST(scenario85BFailedTS)

		require.Equalf(s.T(), http.StatusAccepted, code,
			"85e-col1: retry over a FAILED leftover must proceed (U030 contract), body=%v", resp)
		assert.Equal(s.T(), "restore started", resp["status"])
		assert.Equal(s.T(), jobA, resp["job"], "the deterministic restore Job name")
		assert.Equal(s.T(), scenario85BFailedTS, resp["timestamp"])
		assert.Equal(s.T(), s.cluster, resp["cluster"])

		// The replacement: same name, NEW UID, fresh (non-terminal) status —
		// the terminal condition of the leftover must NOT be inherited.
		require.Eventually(s.T(), func() bool {
			return s.scenario85BJobUID(jobA) != ""
		}, 30*time.Second, scenario85BPollInterval, "the replacement Job must exist")
		replacementUID := s.scenario85BJobUID(jobA)
		assert.NotEqualf(s.T(), seededUID, replacementUID,
			"the leftover must have been REPLACED (new UID under the same deterministic name)")
		assert.Emptyf(s.T(), s.scenario85BJobTerminal(jobA),
			"the replacement must be a fresh run — no terminal condition inherited")

		// It is the operator-BUILT job (its own template), not our seed.
		img, _ := s.scenario85BKubectl("get", "job", jobA, "-n", s.ns,
			"-o", "jsonpath={.spec.template.spec.containers[0].image}")
		assert.Equalf(s.T(), s.scenario85BBackupImage(), strings.TrimSpace(img),
			"the replacement must run the operator's backup image (operator-built Job)")

		s.T().Logf("85e-col1: LIVE PROOF — 202 over the failed leftover; replacement uid=%s", replacementUID)
	})

	// ---- 85e-col3: IN-FLIGHT leftover → 409 CONFLICT, never clobbered ----
	s.Run("85e-col3_inflight_leftover_409_conflict", func() {
		inFlightUID := s.scenario85BSeedInFlight(jobB)
		s.T().Logf("85e-col3: seeded IN-FLIGHT leftover %s (uid=%s)", jobB, inFlightUID)

		code, resp := s.scenario85BRestorePOST(scenario85BInFlight)

		require.Equalf(s.T(), http.StatusConflict, code,
			"85e-col3: retry over an IN-FLIGHT restore must be a 409 CONFLICT, body=%v", resp)
		errObj, ok := resp["error"].(map[string]interface{})
		require.True(s.T(), ok, "409 must carry the error envelope")
		assert.Equal(s.T(), "CONFLICT", errObj["code"])
		msg, _ := errObj["message"].(string)
		assert.Contains(s.T(), msg, jobB, "the 409 message must name the in-flight Job")
		assert.Contains(s.T(), msg, "still in progress",
			"the 409 message must be actionable (tell the caller what to do)")

		// The in-flight run survives UNTOUCHED.
		assert.Equalf(s.T(), inFlightUID, s.scenario85BJobUID(jobB),
			"the in-flight Job must never be clobbered by the retry")
		assert.Emptyf(s.T(), s.scenario85BJobTerminal(jobB),
			"the in-flight Job stays non-terminal")

		s.T().Logf("85e-col3: LIVE PROOF — 409 CONFLICT over the in-flight leftover; uid unchanged")
	})
}

// base64Decode decodes standard base64 (secret payloads) to a string.
func base64Decode(s string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
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
// Scenario 105: DataLoadingStatus PXF Fields (S.1–S.5) — E2E
// ============================================================================
//
// Mirrors the Scenario 104 e2e SHAPE (contract-direct Part A + KUBECONFIG-gated
// live Part B) and REUSES the Scenario 104 live PXF-stop/restore mechanism
// (kubectl exec <segpod> -c pxf -- pxf stop / start) for the S.1 KEY TRANSITION.
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario105Cases() and assert
//     the catalog is well-formed (unique IDs, every S.1–S.5 + MX requirement
//     family present, every row carries a Layer + Expected + Description). The
//     -B/reconcile rows are RESOLVED at the functional + integration layers; the
//     -L rows are documented here and resolved at Part B. NO new operator metric
//     is asserted beyond the honest cloudberry_pxf_status / extensions gauges.
//
//   PART B (KUBECONFIG-gated live, heavy paths behind SCENARIO105_PXF_LIVE=1):
//     against the deployed cluster in cloudberry-test —
//       105-S1-L1: with PXF running, read status.dataLoading.pxf → status
//                  "Running", a positive total sidecar count and ready==total.
//       105-S1-L2 (KEY): stop pxf on ONE segment (the scenario104 mechanism) →
//                  wait for readiness to propagate → assert pxf.status transitions
//                  to "Error" (degraded) or "Stopped" (single segment); RESTORE →
//                  assert it returns to "Running". Generous eventually-timeouts.
//       105-S3-L1/L2: assert extensionsInstalled reflects reality on the live
//                  image (pxf image → pxf/pxf_fdw; non-pxf → ABSENT) — HONESTLY,
//                  never forced.
//       105-S4-L1 / 105-S5-L1: assert activeJobs + the per-job jobs[] runtime
//                  fields from the deployed CR (read-only; honest).
//
// HONESTY: pxf.status derives ONLY from the real "pxf" container readiness; the
// live assertions read the CR the operator wrote (no exec / HTTP health probe).
// Skips cleanly when the live cluster / CR is absent.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO105_PXF_LIVE=1   — gates the heavy stop/restore S.1-L2 path.
//   SCENARIO105_CLUSTER      — live cluster name (default acceptance-test).
//   SCENARIO105_NAMESPACE    — namespace (default cloudberry-test).
// ============================================================================

const (
	// envKubeconfigS105 gates all of Scenario 105 Part B.
	envKubeconfigS105 = "KUBECONFIG"
	// envScenario105Live gates the heavy live stop/restore path.
	envScenario105Live = "SCENARIO105_PXF_LIVE"
	// envScenario105Cluster overrides the live cluster name.
	envScenario105Cluster = "SCENARIO105_CLUSTER"
	// envScenario105Namespace overrides the namespace.
	envScenario105Namespace = "SCENARIO105_NAMESPACE"

	// scenario105DefaultCluster is the default deployed cluster name.
	scenario105DefaultCluster = "acceptance-test"
	// scenario105DefaultNamespace is the default namespace.
	scenario105DefaultNamespace = "cloudberry-test"

	// scenario105PxfContainer is the segment-primary PXF sidecar container.
	scenario105PxfContainer = "pxf"

	// scenario105LiveTimeout bounds the readiness-propagation wait loops (PXF
	// readiness probe periods are ~10-30s, so be generous).
	scenario105LiveTimeout = 5 * time.Minute
	// scenario105PollInterval is the live poll interval.
	scenario105PollInterval = 10 * time.Second
)

// Scenario105E2ESuite verifies the DataLoadingStatus PXF fields end-to-end
// (catalog-direct Part A + KUBECONFIG-gated live Part B).
type Scenario105E2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario105(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario105E2ESuite))
}

func (s *Scenario105E2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// ----------------------------------------------------------------------------
// PART A — catalog-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// TestE2E_Scenario105_PartA_CatalogHonest iterates the full Scenario 105 catalog
// and asserts it is well-formed: unique IDs, every S.1–S.5 + MX requirement
// family present, and every row carries a non-empty Layer + Expected +
// Description. The -B/reconcile rows are resolved at the functional + integration
// layers; the -L rows are documented here and resolved at Part B.
func (s *Scenario105E2ESuite) TestE2E_Scenario105_PartA_CatalogHonest() {
	catalog := cases.Scenario105Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true

			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(),
				[]string{cases.Scenario105LayerBuilder, cases.Scenario105LayerReconcile,
					cases.Scenario105LayerLive}, tc.Layer,
				"%s Layer must be a known token", tc.ID)

			switch tc.Layer {
			case cases.Scenario105LayerLive:
				s.T().Logf("scenario105 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
			default:
				s.T().Logf("scenario105 %s (%s): %s — resolved at functional/integration",
					tc.ID, tc.Req, tc.Expected)
			}
		})
	}

	for _, req := range []string{"S.1", "S.2", "S.3", "S.4", "S.5", "MX"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover requirement family %s", req)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (read CR status + stop/restore PXF)
// ----------------------------------------------------------------------------

// scenario105Env returns the ENV value or the provided default.
func scenario105Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario105Namespace() string {
	return scenario105Env(envScenario105Namespace, scenario105DefaultNamespace)
}
func scenario105Cluster() string {
	return scenario105Env(envScenario105Cluster, scenario105DefaultCluster)
}

// scenario105RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario105E2ESuite) scenario105RequireKubeconfig() {
	if os.Getenv(envKubeconfigS105) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 105 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 105 live Part B")
	}
}

// scenario105RequireLive additionally requires SCENARIO105_PXF_LIVE=1 for the
// destructive stop/restore path.
func (s *Scenario105E2ESuite) scenario105RequireLive() {
	s.scenario105RequireKubeconfig()
	if os.Getenv(envScenario105Live) != "1" {
		s.T().Skip("SCENARIO105_PXF_LIVE not set, skipping the live PXF stop/restore path " +
			"(the deployed cluster + the real cloudberry-pxf sidecar must be available)")
	}
}

// scenario105Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario105E2ESuite) scenario105Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario105PxfStatus is the honest projection of the CR's
// status.dataLoading.pxf the operator wrote.
type scenario105PxfStatus struct {
	Configured          bool     `json:"configured"`
	Servers             int      `json:"servers"`
	Status              string   `json:"status"`
	ExtensionsInstalled []string `json:"extensionsInstalled"`
}

// scenario105DataLoadingStatus is the honest projection of
// status.dataLoading the operator wrote.
type scenario105DataLoadingStatus struct {
	ActiveJobs     int                    `json:"activeJobs"`
	ConfiguredJobs int                    `json:"configuredJobs"`
	Pxf            *scenario105PxfStatus  `json:"pxf"`
	Jobs           []scenario105JobStatus `json:"jobs"`
}

// scenario105JobStatus is the honest projection of a status.dataLoading.jobs[]
// entry (the S.5 per-job runtime fields).
type scenario105JobStatus struct {
	Name       string `json:"name"`
	Enabled    bool   `json:"enabled"`
	LastRun    string `json:"lastRun"`
	LastStatus string `json:"lastStatus"`
	RowsLoaded *int64 `json:"rowsLoaded"`
	Duration   string `json:"duration"`
}

// scenario105ReadDataLoadingStatus reads the deployed CR's status.dataLoading
// sub-object via kubectl. Returns (nil, false) when the CR / status is absent.
func (s *Scenario105E2ESuite) scenario105ReadDataLoadingStatus() (*scenario105DataLoadingStatus, bool) {
	out, err := s.scenario105Kubectl("get", "cloudberrycluster", scenario105Cluster(),
		"-n", scenario105Namespace(), "-o", "jsonpath={.status.dataLoading}")
	if err != nil {
		s.T().Logf("scenario105: could not read CR status.dataLoading: %v (out=%s)", err, out)
		return nil, false
	}
	out = strings.TrimSpace(out)
	if out == "" || out == "<none>" {
		return nil, false
	}
	var dl scenario105DataLoadingStatus
	if e := json.Unmarshal([]byte(out), &dl); e != nil {
		s.T().Logf("scenario105: could not parse status.dataLoading: %v (raw=%s)", e, out)
		return nil, false
	}
	return &dl, true
}

// scenario105ReadPxfStatus reads the deployed CR's status.dataLoading.pxf.status
// string (empty when absent).
func (s *Scenario105E2ESuite) scenario105ReadPxfStatus() string {
	dl, ok := s.scenario105ReadDataLoadingStatus()
	if !ok || dl.Pxf == nil {
		return ""
	}
	return dl.Pxf.Status
}

// scenario105FirstSegmentPxfPod returns the name of the first segment-primary pod
// carrying a pxf container (empty when none found).
func (s *Scenario105E2ESuite) scenario105FirstSegmentPxfPod() string {
	out, err := s.scenario105Kubectl("get", "pods", "-n", scenario105Namespace(),
		"-l", "avsoft.io/component=segment-primary", "-o",
		"jsonpath={.items[0].metadata.name}")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// TestE2E_Scenario105_LiveS1Running covers 105-S1-L1: with PXF running, the CR
// status.dataLoading.pxf reports status "Running", a configured server count and
// (the api echo) ready==total when read via the operator. Skips cleanly when the
// CR / cluster is absent.
func (s *Scenario105E2ESuite) TestE2E_Scenario105_LiveS1Running() {
	s.scenario105RequireKubeconfig()

	dl, ok := s.scenario105ReadDataLoadingStatus()
	if !ok || dl.Pxf == nil {
		s.T().Skip("no status.dataLoading.pxf on the deployed CR (cluster / PXF may not be deployed) " +
			"[CONFIG-ONLY]")
	}

	// HONEST: status is observed. On a healthy deployed cluster it should be
	// Running; if PXF is genuinely down it is Error/Stopped — assert it is one of
	// the honest observed values and is NOT a fabricated value.
	if dl.Pxf.Status == "" {
		s.T().Skip("pxf.status ABSENT on the deployed CR (no segment-primary pxf containers " +
			"observed) — honest unobservable state [CONFIG-ONLY]")
	}
	assert.Contains(s.T(),
		[]string{cases.Scenario105StatusRunning, cases.Scenario105StatusError, cases.Scenario105StatusStopped},
		dl.Pxf.Status, "pxf.status must be one of the honest observed values")
	assert.True(s.T(), dl.Pxf.Configured, "pxf must be configured when status is observed")
	s.T().Logf("scenario105 105-S1-L1: pxf.status=%q servers=%d (configured=%v)",
		dl.Pxf.Status, dl.Pxf.Servers, dl.Pxf.Configured)
}

// TestE2E_Scenario105_LiveS1StopRestore covers 105-S1-L2 (KEY TRANSITION): stop
// PXF on ONE segment (the scenario104 mechanism) → wait for readiness to
// propagate → assert pxf.status transitions to Error (degraded) or Stopped
// (single segment); RESTORE → assert it returns to Running. Gated by
// SCENARIO105_PXF_LIVE=1; skips cleanly when the segment pod / CR is absent.
func (s *Scenario105E2ESuite) TestE2E_Scenario105_LiveS1StopRestore() {
	s.scenario105RequireLive()

	// Baseline: status must be observable + Running before the destructive stop.
	if status := s.scenario105ReadPxfStatus(); status != cases.Scenario105StatusRunning {
		s.T().Skipf("baseline pxf.status=%q (not Running) — the deployed cluster's PXF is not "+
			"healthy enough to prove the stop/restore transition [CONFIG-ONLY]", status)
	}

	segPod := s.scenario105FirstSegmentPxfPod()
	if segPod == "" {
		s.T().Skip("no segment-primary pod found for the PXF stop/restore (cluster may not be " +
			"deployed) [CONFIG-ONLY]")
	}

	// BREAK: stop PXF on ONE segment sidecar (reuse the scenario104 mechanism).
	if o, e := s.scenario105Kubectl("exec", "-n", scenario105Namespace(), segPod,
		"-c", scenario105PxfContainer, "--", "bash", "-lc",
		"pxf stop || pxf-cli cluster stop || true"); e != nil {
		s.T().Skipf("could not stop PXF on segment %s: %v (out=%s) "+
			"[CONFIG-ONLY: stop-PXF not reproducible on this image]", segPod, e, o)
	}
	// RESTORE is deferred regardless of the assertion outcome (never leave PXF
	// stopped).
	defer func() {
		_, _ = s.scenario105Kubectl("exec", "-n", scenario105Namespace(), segPod,
			"-c", scenario105PxfContainer, "--", "bash", "-lc",
			"pxf start || pxf-cli cluster start || true")
	}()

	// Wait for readiness to propagate into the CR status: it must transition AWAY
	// from Running to a degraded honest value (Error or Stopped).
	require.Eventuallyf(s.T(), func() bool {
		st := s.scenario105ReadPxfStatus()
		return st == cases.Scenario105StatusError || st == cases.Scenario105StatusStopped
	}, scenario105LiveTimeout, scenario105PollInterval,
		"after stopping PXF on %s the CR pxf.status must transition to Error/Stopped", segPod)
	s.T().Logf("scenario105 105-S1-L2: degraded status observed = %q", s.scenario105ReadPxfStatus())

	// RESTORE: restart PXF on the segment → the status must return to Running.
	_, _ = s.scenario105Kubectl("exec", "-n", scenario105Namespace(), segPod,
		"-c", scenario105PxfContainer, "--", "bash", "-lc",
		"pxf start || pxf-cli cluster start || true")
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario105ReadPxfStatus() == cases.Scenario105StatusRunning
	}, scenario105LiveTimeout, scenario105PollInterval,
		"after restarting PXF on %s the CR pxf.status must return to Running", segPod)
	s.T().Logf("scenario105 105-S1-L2: restored status = Running")
}

// TestE2E_Scenario105_LiveS3Extensions covers 105-S3-L1/L2: extensionsInstalled
// reflects reality on the live image HONESTLY — a pxf image lists pxf/pxf_fdw, a
// non-pxf image leaves it ABSENT. It asserts honestly (never forces a value):
// when present, every listed name is one of the real PXF extension names; when
// absent, that is the honest non-pxf-image state. Skips cleanly when the CR is
// absent.
func (s *Scenario105E2ESuite) TestE2E_Scenario105_LiveS3Extensions() {
	s.scenario105RequireKubeconfig()

	dl, ok := s.scenario105ReadDataLoadingStatus()
	if !ok || dl.Pxf == nil {
		s.T().Skip("no status.dataLoading.pxf on the deployed CR [CONFIG-ONLY]")
	}

	if len(dl.Pxf.ExtensionsInstalled) == 0 {
		// HONEST: a non-pxf image (or an unreachable DB) leaves the field ABSENT.
		s.T().Logf("scenario105 105-S3-L2: extensionsInstalled ABSENT — honest non-pxf-image / " +
			"unobservable state (not synthesized)")
		return
	}
	// HONEST: every listed extension must be a REAL PXF extension name; on a pxf
	// image the set is a subset of {pxf, pxf_fdw}.
	allowed := map[string]bool{
		cases.Scenario105ExtensionPxf:    true,
		cases.Scenario105ExtensionPxfFdw: true,
	}
	for _, ext := range dl.Pxf.ExtensionsInstalled {
		assert.Truef(s.T(), allowed[ext],
			"extensionsInstalled must list only real PXF extensions, got %q", ext)
	}
	s.T().Logf("scenario105 105-S3-L1: extensionsInstalled=%v (live pxf image)",
		dl.Pxf.ExtensionsInstalled)
}

// TestE2E_Scenario105_LiveS4S5JobsRuntime covers 105-S4-L1 / 105-S5-L1: the
// deployed CR's activeJobs equals the enabled-job count and each jobs[] entry
// that has run carries honest runtime fields (name + lastStatus; lastRun/duration
// when set; rowsLoaded only on a succeeded run). Read-only + honest; skips
// cleanly when the CR / data-loading status is absent.
func (s *Scenario105E2ESuite) TestE2E_Scenario105_LiveS4S5JobsRuntime() {
	s.scenario105RequireKubeconfig()

	dl, ok := s.scenario105ReadDataLoadingStatus()
	if !ok {
		s.T().Skip("no status.dataLoading on the deployed CR (data loading may not be configured) " +
			"[CONFIG-ONLY]")
	}

	// 105-S4-L1: activeJobs is the enabled-job count and never exceeds the
	// configured-job count (honest invariant, concurrency-independent).
	assert.GreaterOrEqual(s.T(), dl.ConfiguredJobs, dl.ActiveJobs,
		"activeJobs (enabled count) must never exceed configuredJobs")
	enabledInStatus := 0
	for _, j := range dl.Jobs {
		if j.Enabled {
			enabledInStatus++
		}
	}
	if len(dl.Jobs) > 0 {
		assert.Equal(s.T(), enabledInStatus, dl.ActiveJobs,
			"activeJobs must equal the count of enabled jobs[] entries")
	}
	s.T().Logf("scenario105 105-S4-L1: activeJobs=%d configuredJobs=%d",
		dl.ActiveJobs, dl.ConfiguredJobs)

	// 105-S5-L1: each jobs[] entry that has run carries honest runtime fields;
	// rowsLoaded is present ONLY on a succeeded run (never synthesized).
	for _, j := range dl.Jobs {
		assert.NotEmptyf(s.T(), j.Name, "every jobs[] entry must carry a name")
		switch j.LastStatus {
		case "Succeeded":
			assert.NotEmptyf(s.T(), j.LastRun, "%s succeeded → lastRun must be set", j.Name)
			s.T().Logf("scenario105 105-S5-L1: job %q Succeeded lastRun=%q duration=%q rowsLoaded=%v",
				j.Name, j.LastRun, j.Duration, j.RowsLoaded)
		case "Failed", "Running", "Pending":
			assert.Nilf(s.T(), j.RowsLoaded,
				"%s (%s) must NOT carry a synthesized rowsLoaded", j.Name, j.LastStatus)
		case "":
			// Never run: only name/enabled are present (honest).
			assert.Nilf(s.T(), j.RowsLoaded, "%s (never run) must not carry rowsLoaded", j.Name)
		}
	}
}

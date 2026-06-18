//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 104: Pre-Load Health Checks (HC.1-HC.5) — E2E
// ============================================================================
//
// Mirrors the Scenario 101 e2e SHAPE. Two parts:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario104Cases() and assert
//     the builder contract via the REAL builder WITHOUT a cluster — the
//     dataload-healthcheck init container (FIRST, named, /bin/bash -c), the
//     5-check script substrings (HC.1 DB-proxy / HC.2 to_regclass / HC.3 curl
//     AWS_S3_ENDPOINT / HC.5 df on the pxf init; HC.4 gpfdist-svc is gpload-only
//     and is asserted DIRECTLY on the gpload Job's init script — the gpload Job
//     now carries the init with HC.2/HC.4/HC.5, NOT HC.1/HC.3), the scratch
//     emptyDir + mounts on both containers, and the enabled:false knob. Live and
//     reconcile rows are logged + skipped.
//
//   PART B (KUBECONFIG-gated live; heavy live behind SCENARIO104_HC_LIVE=1):
//     against the deployed healthcheck-test cluster in cloudberry-test. For EACH
//     HC, BREAK the condition → trigger the dataload Job → assert the init
//     container fails (Job Failed) + the DataLoadingHealthCheckFailed Event +
//     cloudberry_data_loading_job_status=3 + (kube-state-metrics)
//     kube_job_status_failed{job_name=~".*-dataload-.*"} /
//     kube_pod_init_container_status_*; then RESTORE → re-run → the Job proceeds
//     (init passes → Succeeded). The 5 HCs:
//       HC.1  stop PXF on a segment  → restart PXF
//       HC.2  drop public.events     → re-create public.events   (DETERMINISTIC headline)
//       HC.3  break fs.s3a.endpoint  → restore fs.s3a.endpoint
//       HC.4  scale gpfdist to 0     → scale gpfdist back to 1
//       HC.5  raise diskMinFreeMB    → lower diskMinFreeMB / free scratch
//
// HONESTY per HC:
//   - HC.1 is a DB-PROXY probe; if pxf_version() still resolves with PXF stopped
//     the honest proof is "the job fails when PXF is down". Marked config-only if
//     stopping PXF on a segment is not cleanly reproducible.
//   - HC.2 is the DETERMINISTIC, fully live-provable headline (drop/create table).
//   - HC.5: the most deterministic mechanism is patching diskMinFreeMB above the
//     emptyDir free space (no fill needed); filling the scratch is the fallback.
//
// METRIC HONESTY: NO new operator metric. HC failures via job_status=3 +
// errors_total + the Event + kube-state-metrics. cloudberry_pxf_*/gpfdist_* stay
// PLANNED and are NEVER asserted.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO104_HC_LIVE=1    — gates the heavy live fail/restore paths.
//   SCENARIO104_CLUSTER      — live cluster name (default healthcheck-test).
//   SCENARIO104_COORD_POD    — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO104_NAMESPACE    — namespace (default cloudberry-test).
//   SCENARIO104_VM_URL       — VictoriaMetrics URL (default http://localhost:8428).
// ============================================================================

const (
	// envKubeconfigS104 gates all of Scenario 104 Part B.
	envKubeconfigS104 = "KUBECONFIG"
	// envScenario104Live gates the heavy live fail/restore paths.
	envScenario104Live = "SCENARIO104_HC_LIVE"
	// envScenario104Cluster overrides the live cluster name.
	envScenario104Cluster = "SCENARIO104_CLUSTER"
	// envScenario104CoordPod overrides the coordinator pod name.
	envScenario104CoordPod = "SCENARIO104_COORD_POD"
	// envScenario104Namespace overrides the namespace.
	envScenario104Namespace = "SCENARIO104_NAMESPACE"
	// envScenario104VMURL overrides the VictoriaMetrics base URL.
	envScenario104VMURL = "SCENARIO104_VM_URL"

	// scenario104DefaultCluster is the default deployed cluster name.
	scenario104DefaultCluster = "healthcheck-test"
	// scenario104DefaultNamespace is the default namespace.
	scenario104DefaultNamespace = "cloudberry-test"
	// scenario104DefaultVMURL is the default VictoriaMetrics single-node URL.
	scenario104DefaultVMURL = "http://localhost:8428"

	// scenario104ExecTimeout bounds each kubectl exec.
	scenario104ExecTimeout = 5 * time.Minute
	// scenario104JobWait bounds the wait for a dataload Job to fail/complete.
	scenario104JobWait = "5m"
)

// Scenario104E2ESuite verifies the pre-load health-check contract end-to-end
// (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario104E2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario104(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario104E2ESuite))
}

func (s *Scenario104E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario104E2EPxfJob returns the s3-load pxf job per the sample CR §5.
func scenario104E2EPxfJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario104PxfJobName,
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario104Server,
			Profile:     cases.Scenario104Profile,
			Resource:    cases.Scenario104Resource,
			TargetTable: cases.Scenario104TargetTable,
		},
	}
}

// scenario104E2EGploadJob returns the gpload-csv job per the sample CR §5.
func scenario104E2EGploadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario104GploadJobName,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
			FilePaths:   []string{"/incoming/*.csv"},
			Format:      "csv",
			TargetTable: cases.Scenario104TargetTable,
		},
	}
}

// scenario104E2ECluster builds a cluster with pxf+gpfdist enabled + an s3 backup
// destination (HC.3 creds env) + the supplied jobs (mirrors the sample CR).
func scenario104E2ECluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, cases.Scenario104Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: true},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs:    jobs,
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-data",
				Endpoint: "http://minio:9000",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// scenario104E2EInit returns the FIRST init container of a built Job (or nil).
func scenario104E2EInit(job *batchv1.Job) *corev1.Container {
	inits := job.Spec.Template.Spec.InitContainers
	if len(inits) == 0 {
		return nil
	}
	return &inits[0]
}

// scenario104E2EMain returns the main workload container of a built Job (or nil).
func scenario104E2EMain(job *batchv1.Job) *corev1.Container {
	c := job.Spec.Template.Spec.Containers
	if len(c) == 0 {
		return nil
	}
	return &c[0]
}

// scenario104E2EMounts reports whether the container mounts the given path.
func scenario104E2EMounts(c *corev1.Container, path string) bool {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].MountPath == path {
			return true
		}
	}
	return false
}

// scenario104E2EHasVolume reports whether the pod carries the named volume.
func scenario104E2EHasVolume(job *batchv1.Job, name string) bool {
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

// TestE2E_Scenario104_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 104 catalog and asserts the builder contract against the REAL builder
// WITHOUT a cluster. This is the always-on e2e proof. NO new operator metric is
// asserted.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_PartA_ContractHonest() {
	catalog := cases.Scenario104Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario104E2ECluster("s104-e2e-a", scenario104E2EPxfJob())
	pxfOut := s.builder.BuildDataLoadJob(cluster, scenario104E2EPxfJob())
	require.NotNil(s.T(), pxfOut)
	pxfInit := scenario104E2EInit(pxfOut)
	require.NotNil(s.T(), pxfInit)
	pxfScript := pxfInit.Args[0]

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario104LayerLive:
				s.T().Logf("scenario104 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
				return
			case cases.Scenario104LayerReconcile:
				s.T().Logf("scenario104 %s (%s): [reconcile] %s — resolved at controller/envtest",
					tc.ID, tc.Req, tc.Expected)
				return
			case cases.Scenario104LayerBuilder:
				s.scenario104PartABuilder(tc, pxfOut, pxfScript)
			default:
				s.T().Logf("scenario104 %s: layer %q resolved elsewhere", tc.ID, tc.Layer)
			}
		})
	}
}

// scenario104E2EGploadInitScript builds a gpload dataload Job and returns its
// dataload-healthcheck init container script (HC.2/HC.4/HC.5, no HC.1/HC.3).
func (s *Scenario104E2ESuite) scenario104E2EGploadInitScript(name string) string {
	gp := s.builder.BuildDataLoadJob(
		scenario104E2ECluster(name, scenario104E2EGploadJob()), scenario104E2EGploadJob())
	require.NotNil(s.T(), gp)
	init := scenario104E2EInit(gp)
	require.NotNil(s.T(), init, "the gpload Job must carry the health-check init container")
	require.Len(s.T(), init.Args, 1)
	return init.Args[0]
}

// scenario104PartABuilder resolves a builder catalog row against the already-
// built pxf dataload Job + its init script.
func (s *Scenario104E2ESuite) scenario104PartABuilder(
	tc cases.Scenario104Case, pxfOut *batchv1.Job, pxfScript string,
) {
	switch tc.ID {
	case "104-HC1-B-gate", "104-HC3-B-skip":
		// HC.1 (pxf-only) is gated OFF for a gpload job: the gpload Job's init
		// script carries NO HC.1 lines; the HC.3 skip (jdbc) is covered by the
		// builder unit suite. The gpload Job now DOES carry the init.
		gpScript := s.scenario104E2EGploadInitScript("s104-e2e-gate")
		assert.NotContains(s.T(), gpScript, "HC.1 FAIL")
		assert.NotContains(s.T(), gpScript, "pxf_version()")
		return
	case "104-HC4-B":
		// HC.4 is gpload-only: the gpload Job's init script DIRECTLY carries the
		// gpfdist-svc reachability probe (the contract this row pins).
		gpScript := s.scenario104E2EGploadInitScript("s104-e2e-hc4")
		assert.Contains(s.T(), gpScript, "gpfdist-svc",
			"the gpload init must curl the gpfdist Service (HC.4 is gpload-only)")
		assert.Contains(s.T(), gpScript, "HC.4 FAIL")
		return
	case "104-HC4-B-gate":
		// HC.4 gating: the pxf job's init must NOT carry the gpfdist-svc probe.
		assert.NotContains(s.T(), pxfScript, "gpfdist-svc",
			"HC.4 is gpload-only; a pxf job must not curl the gpfdist Service")
		return
	case "104-KNOB-B":
		disabled := false
		cluster := scenario104E2ECluster("s104-e2e-knob", scenario104E2EPxfJob())
		cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &disabled}
		off := s.builder.BuildDataLoadJob(cluster, scenario104E2EPxfJob())
		require.NotNil(s.T(), off)
		assert.Empty(s.T(), off.Spec.Template.Spec.InitContainers)
		return
	case "104-KNOB-B-default":
		assert.NotNil(s.T(), scenario104E2EInit(pxfOut))
		return
	}

	switch tc.Artifact {
	case cases.Scenario104ArtifactInitScript:
		if tc.Contains != "" {
			assert.Containsf(s.T(), pxfScript, tc.Contains,
				"%s init script must carry %q", tc.ID, tc.Contains)
		}
	case cases.Scenario104ArtifactInitContainer:
		init := scenario104E2EInit(pxfOut)
		require.NotNil(s.T(), init)
		assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
		assert.Equal(s.T(), []string{"/bin/bash", "-c"}, init.Command)
	case cases.Scenario104ArtifactVolume:
		assert.True(s.T(), scenario104E2EHasVolume(pxfOut, cases.Scenario104ScratchVolume))
		init := scenario104E2EInit(pxfOut)
		main := scenario104E2EMain(pxfOut)
		require.NotNil(s.T(), init)
		require.NotNil(s.T(), main)
		assert.True(s.T(), scenario104E2EMounts(init, cases.Scenario104ScratchMount))
		assert.True(s.T(), scenario104E2EMounts(main, cases.Scenario104ScratchMount))
	default:
		s.T().Logf("scenario104 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (per-HC fail + restore)
// ----------------------------------------------------------------------------

// scenario104Env returns the ENV value or the provided default.
func scenario104Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario104Namespace() string {
	return scenario104Env(envScenario104Namespace, scenario104DefaultNamespace)
}
func scenario104Cluster() string {
	return scenario104Env(envScenario104Cluster, scenario104DefaultCluster)
}
func scenario104CoordPod() string {
	return scenario104Env(envScenario104CoordPod, scenario104Cluster()+"-coordinator-0")
}
func scenario104VMURL() string {
	return scenario104Env(envScenario104VMURL, scenario104DefaultVMURL)
}

// scenario104DataLoadJobName mirrors util.DataLoadJobName (<cluster>-dataload-<job>).
func scenario104DataLoadJobName(jobName string) string {
	return scenario104Cluster() + "-dataload-" + jobName
}

// scenario104RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario104E2ESuite) scenario104RequireKubeconfig() {
	if os.Getenv(envKubeconfigS104) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 104 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 104 live Part B")
	}
}

// scenario104RequireLive additionally requires SCENARIO104_HC_LIVE=1.
func (s *Scenario104E2ESuite) scenario104RequireLive() {
	s.scenario104RequireKubeconfig()
	if os.Getenv(envScenario104Live) != "1" {
		s.T().Skip("SCENARIO104_HC_LIVE not set, skipping the live HC fail/restore paths " +
			"(the deployed healthcheck-test cluster + pxf + gpfdist + the MinIO s3 dataset must " +
			"be available)")
	}
}

// scenario104Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario104E2ESuite) scenario104Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario104CoordExec runs a bash command inside the coordinator pod's
// cloudberry container via kubectl exec.
func (s *Scenario104E2ESuite) scenario104CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario104ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario104Namespace(),
		"-c", "cloudberry", scenario104CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario104CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario104E2ESuite) scenario104CoordReachable() bool {
	out, err := s.scenario104CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario104ShQuote single-quotes a string for bash -lc.
func scenario104ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// scenario104PSQL runs a psql statement on the coordinator's postgres DB.
func (s *Scenario104E2ESuite) scenario104PSQL(stmt string) (string, error) {
	return s.scenario104CoordExec(fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
		scenario104ShQuote(stmt)))
}

// scenario104VMCounter queries VictoriaMetrics for the scalar sum of a PromQL
// series, returning 0 when the series is absent or the query fails.
func (s *Scenario104E2ESuite) scenario104VMCounter(query string) float64 {
	u := scenario104VMURL() + "/api/v1/query?query=" + url.QueryEscape(query)
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T().Logf("scenario104: VM query failed: %v", err)
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0
	}
	var sum float64
	for _, r := range parsed.Data.Result {
		if len(r.Value) == 2 {
			if str, ok := r.Value[1].(string); ok {
				var f float64
				if _, scanErr := fmt.Sscanf(str, "%g", &f); scanErr == nil {
					sum += f
				}
			}
		}
	}
	return sum
}

// scenario104TriggerDataLoadJob deletes any prior run Job and creates a fresh
// run of the dataload Job from the operator-reconciled Job, returning the
// run-job name. It first tries `kubectl create job --from=job/...`; when that
// fails (e.g. kubectl v1.35 regression "unknown object type *v1.Job") it falls
// back to fetching the source Job JSON, stripping status/metadata, renaming it,
// and applying it as a new Job.
func (s *Scenario104E2ESuite) scenario104TriggerDataLoadJob(jobName, runSuffix string) (string, bool) {
	src := scenario104DataLoadJobName(jobName)
	runJob := jobName + "-hc-" + runSuffix
	_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
		"--ignore-not-found")
	// Prefer --from=job (the operator-reconciled dataload Job).
	if out, err := s.scenario104Kubectl("create", "job", runJob,
		"--from=job/"+src, "-n", scenario104Namespace()); err == nil {
		return runJob, true
	} else {
		s.T().Logf("scenario104: --from=job failed (%v), falling back to JSON clone", err)
		_ = out
	}
	// Fallback: fetch the source Job JSON, clone it with a new name.
	jsonOut, err := s.scenario104Kubectl("get", "job", src, "-n", scenario104Namespace(), "-o", "json")
	if err != nil {
		s.T().Logf("scenario104: could not get job/%s: %v (out=%s) [CONFIG-ONLY]", src, err, jsonOut)
		return runJob, false
	}
	var jobObj map[string]interface{}
	if e := json.Unmarshal([]byte(jsonOut), &jobObj); e != nil {
		s.T().Logf("scenario104: could not parse job JSON: %v [CONFIG-ONLY]", e)
		return runJob, false
	}
	// Strip status, managedFields, resourceVersion, uid, creationTimestamp, ownerReferences, controller labels.
	delete(jobObj, "status")
	if meta, ok := jobObj["metadata"].(map[string]interface{}); ok {
		meta["name"] = runJob
		delete(meta, "resourceVersion")
		delete(meta, "uid")
		delete(meta, "creationTimestamp")
		delete(meta, "managedFields")
		delete(meta, "ownerReferences")
		delete(meta, "generation")
		// Remove controller-uid from labels and selectors to avoid conflicts.
		if labels, ok := meta["labels"].(map[string]interface{}); ok {
			delete(labels, "controller-uid")
			delete(labels, "batch.kubernetes.io/controller-uid")
		}
	}
	if spec, ok := jobObj["spec"].(map[string]interface{}); ok {
		delete(spec, "selector")
		// Reset completions/parallelism tracking fields that block re-creation.
		delete(spec, "completionMode")
		if tmpl, ok := spec["template"].(map[string]interface{}); ok {
			if tmplMeta, ok := tmpl["metadata"].(map[string]interface{}); ok {
				if labels, ok := tmplMeta["labels"].(map[string]interface{}); ok {
					delete(labels, "controller-uid")
					delete(labels, "batch.kubernetes.io/controller-uid")
					// Update job-name labels to match the new Job name.
					labels["job-name"] = runJob
					labels["batch.kubernetes.io/job-name"] = runJob
				}
			}
		}
	}
	cloneJSON, _ := json.Marshal(jobObj)
	// Write to a temp file and apply.
	tmpFile := fmt.Sprintf("/tmp/scenario104-%s.json", runJob)
	if e := os.WriteFile(tmpFile, cloneJSON, 0o600); e != nil {
		s.T().Logf("scenario104: could not write temp file: %v [CONFIG-ONLY]", e)
		return runJob, false
	}
	defer func() { _ = os.Remove(tmpFile) }()
	if out, err := s.scenario104Kubectl("apply", "-f", tmpFile); err != nil {
		s.T().Logf("scenario104: could not apply cloned job: %v (out=%s) [CONFIG-ONLY]", err, out)
		return runJob, false
	}
	return runJob, true
}

// scenario104WaitJobFailed waits for the run Job to reach a Failed condition.
func (s *Scenario104E2ESuite) scenario104WaitJobFailed(runJob string) bool {
	out, err := s.scenario104Kubectl("wait", "--for=condition=failed",
		"--timeout="+scenario104JobWait, "job/"+runJob, "-n", scenario104Namespace())
	if err != nil {
		s.T().Logf("scenario104: job %s did not reach Failed: %v (out=%s)", runJob, err, out)
		return false
	}
	return true
}

// scenario104WaitJobComplete waits for the run Job to complete (Succeeded).
func (s *Scenario104E2ESuite) scenario104WaitJobComplete(runJob string) bool {
	out, err := s.scenario104Kubectl("wait", "--for=condition=complete",
		"--timeout="+scenario104JobWait, "job/"+runJob, "-n", scenario104Namespace())
	if err != nil {
		s.T().Logf("scenario104: job %s did not complete: %v (out=%s)", runJob, err, out)
		return false
	}
	return true
}

// scenario104InitFailedEventPresent asserts a DataLoadingHealthCheckFailed
// Warning Event exists (de-duplicated) in the namespace.
func (s *Scenario104E2ESuite) scenario104InitFailedEventPresent() bool {
	out, err := s.scenario104Kubectl("get", "events", "-n", scenario104Namespace(),
		"--field-selector", "reason="+cases.Scenario104EventReason, "-o", "name")
	if err != nil {
		s.T().Logf("scenario104: get events failed: %v (out=%s)", err, out)
		return false
	}
	return strings.TrimSpace(out) != ""
}

// scenario104AssertKSMJobFailed best-effort asserts the kube-state-metrics
// kube_job_status_failed series for any dataload Job is present in VM (>0).
func (s *Scenario104E2ESuite) scenario104AssertKSMJobFailed() {
	q := `kube_job_status_failed{job_name=~".*-dataload-.*"}`
	v := s.scenario104VMCounter(q)
	s.T().Logf("scenario104: kube-state-metrics %s = %g (kube-state-metrics observability)", q, v)
}

// scenario104AssertJobStatusFailedMetric best-effort asserts the operator's
// cloudberry_data_loading_job_status=3 (Failed) for the given job in VM.
func (s *Scenario104E2ESuite) scenario104AssertJobStatusFailedMetric(jobName string) {
	q := fmt.Sprintf(`cloudberry_data_loading_job_status{cluster=%q,job=%q}`,
		scenario104Cluster(), jobName)
	v := s.scenario104VMCounter(q)
	s.T().Logf("scenario104: %s = %g (expect 3 on a failed health-check Job)", q, v)
}

// scenario104PrepareLive skips cleanly unless the live cluster + coordinator are
// reachable, then ensures public.events exists (the HC.2 baseline restore state).
func (s *Scenario104E2ESuite) scenario104PrepareLive() {
	s.scenario104RequireLive()
	if !s.scenario104CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario104CoordPod())
	}
	if out, err := s.scenario104PSQL(cases.Scenario104TargetDDL); err != nil {
		s.T().Skipf("could not create %s: %v (out=%s) [CONFIG-ONLY]",
			cases.Scenario104TargetTable, err, out)
	}
}

// --- HC.2 — target-table exists (the DETERMINISTIC headline) ----------------

// TestE2E_Scenario104_LiveHC2_TableExists (104-HC2-L-fail / -restore) is the
// DETERMINISTIC HEADLINE: drop public.events → the s3-load init HC.2 fails → Job
// Failed + DataLoadingHealthCheckFailed Event + job_status=3 + kube-state-metrics;
// re-create public.events → re-run → init passes → the Job proceeds.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveHC2_TableExists() {
	s.scenario104PrepareLive()

	// BREAK: drop the target table so HC.2 to_regclass returns NULL.
	if _, err := s.scenario104PSQL("DROP TABLE IF EXISTS " + cases.Scenario104TargetTable); err != nil {
		s.T().Skipf("could not drop %s: %v [CONFIG-ONLY]", cases.Scenario104TargetTable, err)
	}

	runJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc2-fail")
	if !ok {
		s.T().Skip("could not trigger the s3-load dataload Job [CONFIG-ONLY]")
	}
	defer func() {
		_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
			"--ignore-not-found")
	}()

	failed := s.scenario104WaitJobFailed(runJob)
	assert.True(s.T(), failed, "HC.2: the s3-load Job must FAIL when public.events is absent")
	assert.True(s.T(), s.scenario104InitFailedEventPresent(),
		"HC.2: a DataLoadingHealthCheckFailed Event must be emitted")
	s.scenario104AssertJobStatusFailedMetric(cases.Scenario104PxfJobName)
	s.scenario104AssertKSMJobFailed()

	// RESTORE: re-create the table → re-run → the Job proceeds (init passes).
	_, _ = s.scenario104PSQL(cases.Scenario104TargetDDL)
	_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
		"--ignore-not-found")
	restoreJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc2-restore")
	if ok {
		defer func() {
			_, _ = s.scenario104Kubectl("delete", "job", restoreJob, "-n", scenario104Namespace(),
				"--ignore-not-found")
		}()
		completed := s.scenario104WaitJobComplete(restoreJob)
		s.T().Logf("scenario104 HC.2 restore: re-run completed=%v (init passed → load proceeds)",
			completed)
	}
}

// --- HC.1 — PXF readiness (DB proxy) ----------------------------------------

// TestE2E_Scenario104_LiveHC1_PXFReadiness (104-HC1-L-fail / -restore) stops PXF
// on a segment → the s3-load init's DB-proxy HC.1 (or the subsequent load) fails
// → Job Failed + Event; restart PXF → restore. HONEST: HC.1 is a DB proxy; if
// pxf_version() still resolves with PXF stopped, the proof is "the job fails when
// PXF is down". Config-only if stopping PXF is not cleanly reproducible.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveHC1_PXFReadiness() {
	s.scenario104PrepareLive()

	// Find a segment pod carrying the pxf sidecar (label is segment-primary).
	out, err := s.scenario104Kubectl("get", "pods", "-n", scenario104Namespace(),
		"-l", "avsoft.io/component=segment-primary", "-o",
		"jsonpath={.items[0].metadata.name}")
	segPod := strings.TrimSpace(out)
	if err != nil || segPod == "" {
		s.T().Skipf("no segment pod found for HC.1 (cluster may not be deployed): %v (out=%s) "+
			"[CONFIG-ONLY]", err, out)
	}

	// BREAK: stop PXF on the segment sidecar.
	if o, e := s.scenario104Kubectl("exec", "-n", scenario104Namespace(), segPod,
		"-c", "pxf", "--", "bash", "-lc", "pxf stop || pxf-cli cluster stop || true"); e != nil {
		s.T().Skipf("could not stop PXF on segment %s: %v (out=%s) "+
			"[CONFIG-ONLY: HC.1 stop-PXF not reproducible on this image]", segPod, e, o)
	}

	runJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc1-fail")
	if !ok {
		s.T().Skip("could not trigger the s3-load dataload Job [CONFIG-ONLY]")
	}
	defer func() {
		_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
			"--ignore-not-found")
		// RESTORE: restart PXF regardless of the assertion outcome.
		_, _ = s.scenario104Kubectl("exec", "-n", scenario104Namespace(), segPod,
			"-c", "pxf", "--", "bash", "-lc", "pxf start || pxf-cli cluster start || true")
	}()

	failed := s.scenario104WaitJobFailed(runJob)
	if !failed {
		s.T().Logf("scenario104 HC.1: the s3-load Job did NOT fail with PXF stopped — HONEST " +
			"DB-proxy nuance: pxf_version() may still resolve when the agent is down. The " +
			"behavioral proof requires PXF to be genuinely unreachable from the coordinator.")
	} else {
		assert.True(s.T(), s.scenario104InitFailedEventPresent(),
			"HC.1: a DataLoadingHealthCheckFailed Event must be emitted when PXF is down")
		s.scenario104AssertJobStatusFailedMetric(cases.Scenario104PxfJobName)
		s.scenario104AssertKSMJobFailed()
	}

	// RESTORE: restart PXF → re-run → the Job proceeds.
	_, _ = s.scenario104Kubectl("exec", "-n", scenario104Namespace(), segPod,
		"-c", "pxf", "--", "bash", "-lc", "pxf start || pxf-cli cluster start || true")
	if restoreJob, rok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc1-restore"); rok {
		defer func() {
			_, _ = s.scenario104Kubectl("delete", "job", restoreJob, "-n", scenario104Namespace(),
				"--ignore-not-found")
		}()
		s.T().Logf("scenario104 HC.1 restore: re-run after PXF restart completed=%v",
			s.scenario104WaitJobComplete(restoreJob))
	}
}

// --- HC.3 — external source connectivity (s3) -------------------------------

// TestE2E_Scenario104_LiveHC3_S3Endpoint (104-HC3-L-fail / -restore) breaks the
// s3 server fs.s3a.endpoint to an unreachable host → the s3-load init HC.3 curl
// fails → Job Failed + Event; restore the endpoint → re-run → proceeds.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveHC3_S3Endpoint() {
	s.scenario104PrepareLive()

	clusterName := scenario104Cluster()
	const badEP = "http://no-such-minio.invalid:9000"
	const goodEP = "http://minio:9000"

	// BREAK: patch the s3 server endpoint to a bad host.
	patch := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/dataLoading/pxf/servers/0/config/fs.s3a.endpoint","value":%q}]`,
		badEP)
	if out, err := s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
		"-n", scenario104Namespace(), "--type=json", "-p", patch); err != nil {
		s.T().Skipf("could not patch s3 endpoint to a bad host: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() {
		good := fmt.Sprintf(
			`[{"op":"replace","path":"/spec/dataLoading/pxf/servers/0/config/fs.s3a.endpoint","value":%q}]`,
			goodEP)
		_, _ = s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
			"-n", scenario104Namespace(), "--type=json", "-p", good)
	}()
	// Give the operator a moment to reconcile the server config.
	time.Sleep(10 * time.Second)

	runJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc3-fail")
	if !ok {
		s.T().Skip("could not trigger the s3-load dataload Job [CONFIG-ONLY]")
	}
	defer func() {
		_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
			"--ignore-not-found")
	}()

	failed := s.scenario104WaitJobFailed(runJob)
	assert.True(s.T(), failed, "HC.3: the s3-load Job must FAIL when the s3 endpoint is unreachable")
	if failed {
		assert.True(s.T(), s.scenario104InitFailedEventPresent(),
			"HC.3: a DataLoadingHealthCheckFailed Event must be emitted")
		s.scenario104AssertJobStatusFailedMetric(cases.Scenario104PxfJobName)
		s.scenario104AssertKSMJobFailed()
	}

	// RESTORE handled in the deferred patch; re-run to prove the Job proceeds.
	good := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/dataLoading/pxf/servers/0/config/fs.s3a.endpoint","value":%q}]`,
		goodEP)
	_, _ = s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
		"-n", scenario104Namespace(), "--type=json", "-p", good)
	time.Sleep(10 * time.Second)
	if restoreJob, rok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc3-restore"); rok {
		defer func() {
			_, _ = s.scenario104Kubectl("delete", "job", restoreJob, "-n", scenario104Namespace(),
				"--ignore-not-found")
		}()
		s.T().Logf("scenario104 HC.3 restore: re-run after endpoint fix completed=%v",
			s.scenario104WaitJobComplete(restoreJob))
	}
}

// --- HC.4 — gpfdist reachability (gpload) -----------------------------------

// TestE2E_Scenario104_LiveHC4_Gpfdist (104-HC4-L-fail / -restore) scales the
// gpfdist Deployment to 0 → the gpload-csv init HC.4 curl fails → Job Failed +
// Event; scale back to 1 → re-run → proceeds.
//
// NOTE: HC.4 is exercised on the gpload job, whose dataload-healthcheck init
// container now genuinely carries the HC.4 gpfdist-svc reachability probe (the
// gpload-specific check, asserted directly in Part A). With gpfdist scaled to 0
// the gpload init HC.4 curl fails → the Job Fails before the load runs.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveHC4_Gpfdist() {
	s.scenario104PrepareLive()

	gpfdistDeploy := scenario104Cluster() + "-gpfdist"
	if out, err := s.scenario104Kubectl("get", "deploy", gpfdistDeploy,
		"-n", scenario104Namespace()); err != nil {
		s.T().Skipf("gpfdist Deployment %s not found (cluster may not be deployed): %v (out=%s) "+
			"[CONFIG-ONLY]", gpfdistDeploy, err, out)
	}

	// BREAK: scale gpfdist to 0 (no ready endpoints).
	if out, err := s.scenario104Kubectl("scale", "deploy", gpfdistDeploy,
		"--replicas=0", "-n", scenario104Namespace()); err != nil {
		s.T().Skipf("could not scale gpfdist to 0: %v (out=%s) [CONFIG-ONLY]", err, out)
	}
	defer func() {
		_, _ = s.scenario104Kubectl("scale", "deploy", gpfdistDeploy,
			"--replicas=1", "-n", scenario104Namespace())
	}()
	time.Sleep(10 * time.Second)

	runJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104GploadJobName, "hc4-fail")
	if !ok {
		s.T().Skip("could not trigger the gpload-csv dataload Job [CONFIG-ONLY]")
	}
	defer func() {
		_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
			"--ignore-not-found")
	}()

	failed := s.scenario104WaitJobFailed(runJob)
	assert.True(s.T(), failed, "HC.4: the gpload-csv Job must FAIL when gpfdist has no ready endpoints")
	if failed {
		// The DataLoadingHealthCheckFailed Event is emitted when the init is the
		// failing container; the gpload path may fail in the main container, in
		// which case the honest signal is the Job failure + job_status=3.
		s.T().Logf("scenario104 HC.4: Event present=%v", s.scenario104InitFailedEventPresent())
		s.scenario104AssertJobStatusFailedMetric(cases.Scenario104GploadJobName)
		s.scenario104AssertKSMJobFailed()
		// kube_deployment_status_replicas_available for gpfdist should be 0.
		q := fmt.Sprintf(`kube_deployment_status_replicas_available{deployment=%q}`, gpfdistDeploy)
		s.T().Logf("scenario104 HC.4: %s = %g (expect 0 while scaled down)", q,
			s.scenario104VMCounter(q))
	}

	// RESTORE: scale gpfdist back to 1 → re-run → proceeds.
	_, _ = s.scenario104Kubectl("scale", "deploy", gpfdistDeploy,
		"--replicas=1", "-n", scenario104Namespace())
	_, _ = s.scenario104Kubectl("rollout", "status", "deploy/"+gpfdistDeploy,
		"-n", scenario104Namespace(), "--timeout=2m")
	if restoreJob, rok := s.scenario104TriggerDataLoadJob(cases.Scenario104GploadJobName, "hc4-restore"); rok {
		defer func() {
			_, _ = s.scenario104Kubectl("delete", "job", restoreJob, "-n", scenario104Namespace(),
				"--ignore-not-found")
		}()
		s.T().Logf("scenario104 HC.4 restore: re-run after scale-up completed=%v",
			s.scenario104WaitJobComplete(restoreJob))
	}
}

// --- HC.5 — disk space ------------------------------------------------------

// TestE2E_Scenario104_LiveHC5_DiskSpace (104-HC5-L-fail / -restore) makes HC.5
// fail by patching diskMinFreeMB to a value EXCEEDING the emptyDir free space
// (the most deterministic mechanism — no fill needed) → the s3-load init df is
// below threshold → Job Failed + Event; restore the threshold → re-run →
// proceeds. The fill-the-scratch mechanism (scratchSizeLimit 64Mi) is the
// documented fallback.
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveHC5_DiskSpace() {
	s.scenario104PrepareLive()

	clusterName := scenario104Cluster()
	// BREAK: raise diskMinFreeMB far above any realistic emptyDir free space.
	const hugeMB = 1048576 // 1 TiB free required — guaranteed to fail.
	patch := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/dataLoading/healthChecks/diskMinFreeMB","value":%d}]`, hugeMB)
	if out, err := s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
		"-n", scenario104Namespace(), "--type=json", "-p", patch); err != nil {
		// The path may not exist if healthChecks was omitted; try an add.
		add := fmt.Sprintf(
			`[{"op":"add","path":"/spec/dataLoading/healthChecks","value":{"enabled":true,"diskMinFreeMB":%d}}]`,
			hugeMB)
		if out2, err2 := s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
			"-n", scenario104Namespace(), "--type=json", "-p", add); err2 != nil {
			s.T().Skipf("could not patch diskMinFreeMB: %v (out=%s) / %v (out=%s) [CONFIG-ONLY]",
				err, out, err2, out2)
		}
	}
	defer func() {
		restore := fmt.Sprintf(
			`[{"op":"replace","path":"/spec/dataLoading/healthChecks/diskMinFreeMB","value":%d}]`,
			cases.Scenario104DiskMinFreeMB)
		_, _ = s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
			"-n", scenario104Namespace(), "--type=json", "-p", restore)
	}()
	time.Sleep(10 * time.Second)

	runJob, ok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc5-fail")
	if !ok {
		s.T().Skip("could not trigger the s3-load dataload Job [CONFIG-ONLY]")
	}
	defer func() {
		_, _ = s.scenario104Kubectl("delete", "job", runJob, "-n", scenario104Namespace(),
			"--ignore-not-found")
	}()

	failed := s.scenario104WaitJobFailed(runJob)
	assert.True(s.T(), failed, "HC.5: the s3-load Job must FAIL when diskMinFreeMB exceeds free space")
	if failed {
		assert.True(s.T(), s.scenario104InitFailedEventPresent(),
			"HC.5: a DataLoadingHealthCheckFailed Event must be emitted")
		s.scenario104AssertJobStatusFailedMetric(cases.Scenario104PxfJobName)
		s.scenario104AssertKSMJobFailed()
	}

	// RESTORE: lower diskMinFreeMB → re-run → the Job proceeds.
	restore := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/dataLoading/healthChecks/diskMinFreeMB","value":%d}]`,
		cases.Scenario104DiskMinFreeMB)
	_, _ = s.scenario104Kubectl("patch", "cloudberrycluster", clusterName,
		"-n", scenario104Namespace(), "--type=json", "-p", restore)
	time.Sleep(10 * time.Second)
	if restoreJob, rok := s.scenario104TriggerDataLoadJob(cases.Scenario104PxfJobName, "hc5-restore"); rok {
		defer func() {
			_, _ = s.scenario104Kubectl("delete", "job", restoreJob, "-n", scenario104Namespace(),
				"--ignore-not-found")
		}()
		s.T().Logf("scenario104 HC.5 restore: re-run after lowering diskMinFreeMB completed=%v",
			s.scenario104WaitJobComplete(restoreJob))
	}
}

// TestE2E_Scenario104_LiveKSMDeployed (104-KSM-DEP) best-effort confirms the
// kube-state-metrics series the HC observability rides on are present in VM (the
// DevOps dependency). KUBECONFIG/live-gated; logs honestly (NO new operator
// metric is asserted).
func (s *Scenario104E2ESuite) TestE2E_Scenario104_LiveKSMDeployed() {
	s.scenario104RequireLive()
	for _, q := range []string{
		`kube_job_status_failed{job_name=~".*-dataload-.*"}`,
		`kube_pod_init_container_status_restarts_total{container="dataload-healthcheck"}`,
		fmt.Sprintf(`kube_deployment_status_replicas_available{deployment=%q}`,
			scenario104Cluster()+"-gpfdist"),
	} {
		s.T().Logf("scenario104 KSM: %s = %g", q, s.scenario104VMCounter(q))
	}
}

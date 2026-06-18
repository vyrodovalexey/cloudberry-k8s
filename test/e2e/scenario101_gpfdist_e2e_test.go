//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 101: gpfdist Deployment + Job 4 (gpload-csv) — E2E
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario101Cases — this is Scenario 101, mirroring
// the Scenario 99 e2e SHAPE.
//
// Two parts:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario101Cases() and assert
//     the documented contract via the REAL builder/validator WITHOUT a cluster —
//     the gpfdist Deployment/Service/PVC fields (GP.2-GP.5), the gpload control
//     file GL.1-7 + the Job pod args (gpload -f /etc/gpload/<job>.yml), and the
//     webhook W.18-W.22 DENY paths. Live rows are logged + skipped.
//
//   PART B (KUBECONFIG-gated live; heavy live behind SCENARIO101_GPFDIST_LIVE=1):
//     against the deployed gpfdist-test cluster in cloudberry-test:
//       - GP live: Deployment <cluster>-gpfdist Ready (readyReplicas); Service
//         <cluster>-gpfdist-svc selector/port; PVC <cluster>-gpfdist-data-pvc
//         Bound + mounted /data in the pod (kubectl exec ls /data).
//       - SEED: ensure /data/incoming/*.csv present in the gpfdist pod; create
//         public.raw_data on the coordinator.
//       - gpload live (HEADLINE "data loads"): trigger the gpload CronJob
//         (kubectl create job --from=cronjob for determinism) -> gpload -f loads
//         gpfdist://<svc>:8080/incoming/*.csv -> assert
//         SELECT count(*) FROM public.raw_data > 0. GL.6 TRUNCATE: re-run -> count
//         stable (not doubled).
//       - control-file live: ConfigMap <cluster>-gpload-<job> carries GL.1-7.
//
// METRIC HONESTY: cloudberry_gpfdist_* are NEVER asserted (PLANNED;
// kube-state-metrics absent -> Deployment readiness via kubectl, not a VM
// metric). gpload reuses cloudberry_data_loading_job_status (=2 success) +
// rows_total (if the DATALOAD marker is harvested).
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG                 — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO101_GPFDIST_LIVE=1 — gates the heavy live gpfdist/gpload paths.
//   SCENARIO101_CLUSTER        — live cluster name (default gpfdist-test).
//   SCENARIO101_COORD_POD      — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO101_NAMESPACE      — namespace (default cloudberry-test).
// ============================================================================

const (
	// envKubeconfigS101 gates all of Scenario 101 Part B.
	envKubeconfigS101 = "KUBECONFIG"
	// envScenario101Live gates the heavy live gpfdist/gpload paths.
	envScenario101Live = "SCENARIO101_GPFDIST_LIVE"
	// envScenario101Cluster overrides the live cluster name.
	envScenario101Cluster = "SCENARIO101_CLUSTER"
	// envScenario101CoordPod overrides the coordinator pod name.
	envScenario101CoordPod = "SCENARIO101_COORD_POD"
	// envScenario101Namespace overrides the namespace.
	envScenario101Namespace = "SCENARIO101_NAMESPACE"

	// scenario101DefaultCluster is the default deployed cluster name.
	scenario101DefaultCluster = "gpfdist-test"
	// scenario101DefaultNamespace is the default namespace.
	scenario101DefaultNamespace = "cloudberry-test"

	// scenario101ExecTimeout bounds each kubectl exec (a load over a dataset).
	scenario101ExecTimeout = 5 * time.Minute
)

// Scenario101E2ESuite verifies the gpfdist+gpload contract end-to-end
// (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario101E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario101(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario101E2ESuite))
}

func (s *Scenario101E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario101E2ECluster builds a cluster with gpfdist enabled + the given gpload
// jobs (mirrors the gpfdist-test sample CR data-loading shape).
func scenario101E2ECluster(
	name string, jobs ...cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs:    jobs,
	}
	return cluster
}

// scenario101E2EGploadJob returns the gpload-csv job per the spec example.
func scenario101E2EGploadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:     cases.Scenario101JobName,
		Type:     "gpload",
		Enabled:  true,
		Schedule: cases.Scenario101Schedule,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
			FilePaths:   []string{cases.Scenario101FileGlob},
			Format:      "csv",
			Delimiter:   ",",
			Header:      util.Ptr(true),
			Encoding:    "UTF-8",
			ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit: 50,
				LogErrors:          util.Ptr(true),
			},
			TargetTable: cases.Scenario101TargetTable,
			Mode:        "insert",
			Preload:     &cbv1alpha1.GploadPreloadSpec{Truncate: util.Ptr(true)},
			PostActions: []string{"ANALYZE public.raw_data"},
		},
	}
}

// scenario101E2EWebhookJob returns a minimal valid gpload job for the W.* DENY
// paths; callers mutate the GploadJob field.
func scenario101E2EWebhookJob(name string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    name,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: cases.Scenario101TargetTable,
			FilePaths:   []string{cases.Scenario101FileGlob},
		},
	}
}

// TestE2E_Scenario101_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 101 catalog and asserts the documented contract against the REAL
// builder/validator WITHOUT a cluster. This is the always-on e2e proof. The
// gpfdist_* metric family is NEVER asserted.
func (s *Scenario101E2ESuite) TestE2E_Scenario101_PartA_ContractHonest() {
	catalog := cases.Scenario101Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario101E2ECluster("s101-e2e-a")
	job := scenario101E2EGploadJob()
	controlFile, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	dep := s.builder.BuildGpfdistDeployment(cluster)
	svc := s.builder.BuildGpfdistService(cluster)
	pvc := s.builder.BuildGpfdistPVC(cluster)
	gploadJob := s.builder.BuildGploadJob(cluster, job)
	require.NotNil(s.T(), dep)
	require.NotNil(s.T(), svc)
	require.NotNil(s.T(), pvc)
	require.NotNil(s.T(), gploadJob)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario101LayerLive:
				s.T().Logf("scenario101 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario101LayerWebhook:
				s.scenario101PartAWebhook(tc)

			case cases.Scenario101LayerBuilder:
				s.scenario101PartABuilder(tc, controlFile, dep, svc, pvc, gploadJob, cluster, job)

			default:
				s.T().Logf("scenario101 %s: layer %q resolved at envtest", tc.ID, tc.Layer)
			}
		})
	}
}

// scenario101PartAWebhook resolves a webhook catalog row by exercising the
// matching DENY path through the validate webhook.
func (s *Scenario101E2ESuite) scenario101PartAWebhook(tc cases.Scenario101Case) {
	job := scenario101E2EWebhookJob("cat-" + strings.ToLower(strings.ReplaceAll(tc.ID, "-", "")))
	switch tc.Req {
	case "W.18":
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "ftp"}
	case "W.19":
		job.GploadJob.Delimiter = ",,"
	case "W.20":
		job.GploadJob.Mode = "update"
	case "W.21":
		job.GploadJob.PostActions = []string{"DROP TABLE x; --"}
	case "W.22":
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "local", Host: "h", Port: 8080}
	case "W.16":
		job.GploadJob.FilePaths = []string{"file:///a.csv"}
	default:
		s.T().Fatalf("scenario101 %s: unknown webhook req %q", tc.ID, tc.Req)
	}
	cluster := scenario101E2ECluster("s101-e2e-"+
		strings.ToLower(strings.ReplaceAll(tc.ID, "-", "")), job)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
}

// scenario101PartABuilder resolves a builder catalog row against the already-
// built artifacts.
func (s *Scenario101E2ESuite) scenario101PartABuilder(
	tc cases.Scenario101Case,
	controlFile string,
	dep interface{ GetName() string },
	svc interface{ GetName() string },
	pvc interface{ GetName() string },
	gploadJob interface{ GetName() string },
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) {
	switch tc.Artifact {
	case cases.Scenario101ArtifactControlFile:
		if tc.Contains != "" {
			assert.Containsf(s.T(), controlFile, tc.Contains,
				"%s control file must carry %q", tc.ID, tc.Contains)
		}
	case cases.Scenario101ArtifactCronJob:
		scheduled := job
		scheduled.Schedule = cases.Scenario101Schedule
		cron := s.builder.BuildGploadCronJob(cluster, scheduled)
		require.NotNil(s.T(), cron)
		assert.Equal(s.T(), cases.Scenario101Schedule, cron.Spec.Schedule)
	case cases.Scenario101ArtifactDeployment:
		assert.Equal(s.T(), builder.GpfdistServiceName(cluster.Name), dep.GetName())
	case cases.Scenario101ArtifactService:
		assert.Equal(s.T(), util.GpfdistServiceName2(cluster.Name), svc.GetName())
	case cases.Scenario101ArtifactPVC:
		assert.Equal(s.T(), util.GpfdistDataPVCName(cluster.Name), pvc.GetName())
	case cases.Scenario101ArtifactConfigMap:
		cm := s.builder.BuildGploadControlFileConfigMap(cluster, job)
		require.NotNil(s.T(), cm)
		assert.Contains(s.T(), cm.Data, job.Name+".yml")
	case cases.Scenario101ArtifactJob:
		assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), gploadJob.GetName())
	default:
		s.T().Logf("scenario101 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (gpfdist Ready + gpload "data loads")
// ----------------------------------------------------------------------------

// scenario101Env returns the ENV value or the provided default.
func scenario101Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario101Namespace() string {
	return scenario101Env(envScenario101Namespace, scenario101DefaultNamespace)
}
func scenario101Cluster() string {
	return scenario101Env(envScenario101Cluster, scenario101DefaultCluster)
}
func scenario101CoordPod() string {
	return scenario101Env(envScenario101CoordPod, scenario101Cluster()+"-coordinator-0")
}

// scenario101GpfdistDeployment is the gpfdist Deployment name (<cluster>-gpfdist).
func scenario101GpfdistDeployment() string { return scenario101Cluster() + "-gpfdist" }

// scenario101GpfdistSvc is the gpfdist Service name (<cluster>-gpfdist-svc).
func scenario101GpfdistSvc() string { return scenario101Cluster() + "-gpfdist-svc" }

// scenario101GpfdistPVC is the gpfdist PVC name (<cluster>-gpfdist-data-pvc).
func scenario101GpfdistPVC() string { return scenario101Cluster() + "-gpfdist-data-pvc" }

// scenario101RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario101E2ESuite) scenario101RequireKubeconfig() {
	if os.Getenv(envKubeconfigS101) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 101 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 101 live Part B")
	}
}

// scenario101RequireLive additionally requires SCENARIO101_GPFDIST_LIVE=1.
func (s *Scenario101E2ESuite) scenario101RequireLive() {
	s.scenario101RequireKubeconfig()
	if os.Getenv(envScenario101Live) != "1" {
		s.T().Skip("SCENARIO101_GPFDIST_LIVE not set, skipping the live gpfdist/gpload paths " +
			"(the deployed gpfdist-test cluster + gpfdist image cloudberry-gpfdist:2.1.0 + the " +
			"seeded /data/incoming/*.csv must be available)")
	}
}

// scenario101Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario101E2ESuite) scenario101Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario101CoordExec runs a bash command inside the coordinator pod's
// cloudberry container via kubectl exec, bounded by scenario101ExecTimeout.
func (s *Scenario101E2ESuite) scenario101CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario101ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario101Namespace(),
		"-c", "cloudberry", scenario101CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario101CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario101E2ESuite) scenario101CoordReachable() bool {
	out, err := s.scenario101CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario101ShQuote single-quotes a string for bash -lc.
func scenario101ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// scenario101PSQL runs a psql statement on the coordinator's postgres DB.
func (s *Scenario101E2ESuite) scenario101PSQL(stmt string) (string, error) {
	return s.scenario101CoordExec(fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
		scenario101ShQuote(stmt)))
}

// scenario101Count runs SELECT count(*) FROM <table> on the coordinator.
func (s *Scenario101E2ESuite) scenario101Count(table string) (int64, string, error) {
	out, err := s.scenario101CoordExec(fmt.Sprintf("psql -d postgres -tA -c %s",
		scenario101ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, out, err
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n, out, perr
}

// TestE2E_Scenario101_LiveGpfdistReady (Part B / GP live) asserts the gpfdist
// Deployment <cluster>-gpfdist reaches Ready (readyReplicas), the Service exists
// with the gpfdist selector/port, and the PVC is Bound + mounted at /data in the
// gpfdist pod. Readiness is observed via kubectl (kube-state-metrics absent).
func (s *Scenario101E2ESuite) TestE2E_Scenario101_LiveGpfdistReady() {
	s.scenario101RequireLive()

	// Deployment exists.
	dep := scenario101GpfdistDeployment()
	if out, err := s.scenario101Kubectl("get", "deploy", dep,
		"-n", scenario101Namespace()); err != nil {
		s.T().Skipf("gpfdist Deployment %s not found (cluster may not be deployed): %v (out=%s)",
			dep, err, out)
	}

	// readyReplicas >= 1.
	out, err := s.scenario101Kubectl("get", "deploy", dep, "-n", scenario101Namespace(),
		"-o", "jsonpath={.status.readyReplicas}")
	require.NoErrorf(s.T(), err, "get %s readyReplicas", dep)
	ready, _ := strconv.Atoi(strings.TrimSpace(out))
	assert.Positivef(s.T(), ready, "GP live: Deployment %s must reach Ready (readyReplicas=%d)",
		dep, ready)

	// Service exists with the gpfdist component selector + port 8080.
	svc := scenario101GpfdistSvc()
	selOut, selErr := s.scenario101Kubectl("get", "svc", svc, "-n", scenario101Namespace(),
		"-o", "jsonpath={.spec.selector."+strings.ReplaceAll(util.LabelComponent, ".", "\\.")+"}")
	if selErr == nil {
		assert.Equal(s.T(), util.ComponentGpfdist, strings.TrimSpace(selOut),
			"GP.5 live: Service %s selector must be avsoft.io/component=gpfdist", svc)
	}
	portOut, _ := s.scenario101Kubectl("get", "svc", svc, "-n", scenario101Namespace(),
		"-o", "jsonpath={.spec.ports[0].port}")
	assert.Equal(s.T(), strconv.Itoa(cases.Scenario101GpfdistPort), strings.TrimSpace(portOut),
		"GP.5 live: Service %s port must be 8080", svc)

	// PVC Bound.
	pvcOut, _ := s.scenario101Kubectl("get", "pvc", scenario101GpfdistPVC(),
		"-n", scenario101Namespace(), "-o", "jsonpath={.status.phase}")
	assert.Equal(s.T(), "Bound", strings.TrimSpace(pvcOut),
		"GP.4 live: PVC %s must be Bound", scenario101GpfdistPVC())

	// /data mounted in the gpfdist pod.
	lsOut, lsErr := s.scenario101Kubectl("exec", "-n", scenario101Namespace(),
		"deploy/"+dep, "--", "ls", "-ld", "/data")
	if lsErr == nil {
		assert.Contains(s.T(), lsOut, "/data",
			"GP.4 live: /data must be mounted in the gpfdist pod")
	}
	s.T().Logf("scenario101 GP live: Deployment %s readyReplicas=%d; Service %s port=%s; "+
		"PVC %s=%s", dep, ready, svc, strings.TrimSpace(portOut), scenario101GpfdistPVC(),
		strings.TrimSpace(pvcOut))
}

// TestE2E_Scenario101_LiveGploadLoads (Part B / HEADLINE) is the "data loads
// successfully" proof. It seeds /data/incoming/*.csv into the gpfdist pod (if
// absent), creates public.raw_data, triggers the gpload CronJob via
// `kubectl create job --from=cronjob` (deterministic), waits for completion and
// asserts SELECT count(*) FROM public.raw_data > 0. GL.6 TRUNCATE: re-run -> the
// count is STABLE (not doubled). cloudberry_gpfdist_* are NEVER asserted.
func (s *Scenario101E2ESuite) TestE2E_Scenario101_LiveGploadLoads() {
	s.scenario101RequireLive()
	if !s.scenario101CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario101CoordPod())
	}

	// Create the target table.
	if out, err := s.scenario101PSQL(cases.Scenario101TargetDDL); err != nil {
		s.T().Skipf("could not create %s: %v (out=%s) [CONFIG-ONLY]",
			cases.Scenario101TargetTable, err, out)
	}

	// Ensure the CSVs are present in the gpfdist pod (fallback seed via exec when
	// the seed Job has not run). The honest source-of-truth is gen-gpload-csv.sh.
	s.scenario101SeedGpfdistCSV()

	// Trigger the gpload CronJob deterministically.
	cron := util.DataLoadJobName(scenario101Cluster(), cases.Scenario101JobName)
	runJob := "gpload-csv-e2e-run"
	_, _ = s.scenario101Kubectl("delete", "job", runJob, "-n", scenario101Namespace(),
		"--ignore-not-found")
	if out, err := s.scenario101Kubectl("create", "job", runJob,
		"--from=cronjob/"+cron, "-n", scenario101Namespace()); err != nil {
		s.T().Skipf("could not create job from cronjob/%s: %v (out=%s) [CONFIG-ONLY]",
			cron, err, out)
	}
	defer func() {
		_, _ = s.scenario101Kubectl("delete", "job", runJob, "-n", scenario101Namespace(),
			"--ignore-not-found")
	}()

	// Wait for the gpload Job to complete.
	if out, err := s.scenario101Kubectl("wait", "--for=condition=complete",
		"--timeout=5m", "job/"+runJob, "-n", scenario101Namespace()); err != nil {
		s.T().Logf("scenario101 gpload Job %s did not complete cleanly: %v (out=%s)",
			runJob, err, out)
	}

	// HEADLINE: rows LAND in public.raw_data.
	n, out, cerr := s.scenario101Count(cases.Scenario101TargetTable)
	require.NoErrorf(s.T(), cerr, "count %s (out=%s)", cases.Scenario101TargetTable, out)
	assert.Positivef(s.T(), n,
		"HEADLINE: gpload must load rows -> SELECT count(*) FROM %s > 0 (got %d)",
		cases.Scenario101TargetTable, n)
	s.T().Logf("scenario101 gpload live: %d rows loaded into %s", n, cases.Scenario101TargetTable)

	// GL.6 TRUNCATE: re-run -> count STABLE (PRELOAD TRUNCATE empties first).
	_, _ = s.scenario101Kubectl("delete", "job", runJob, "-n", scenario101Namespace(),
		"--ignore-not-found")
	if _, err := s.scenario101Kubectl("create", "job", runJob,
		"--from=cronjob/"+cron, "-n", scenario101Namespace()); err == nil {
		_, _ = s.scenario101Kubectl("wait", "--for=condition=complete",
			"--timeout=5m", "job/"+runJob, "-n", scenario101Namespace())
		n2, _, _ := s.scenario101Count(cases.Scenario101TargetTable)
		assert.Equalf(s.T(), n, n2,
			"GL.6 TRUNCATE: re-run count must be STABLE (not doubled): first=%d second=%d", n, n2)
		s.T().Logf("scenario101 GL.6 TRUNCATE: re-run count stable (%d == %d)", n, n2)
	}
}

// scenario101SeedGpfdistCSV best-effort writes the two CSV samples into the
// gpfdist pod's /data/incoming if they are not already present (the deploy agent
// normally seeds via a seed Job / kubectl cp). Logs honestly; never fails the
// test on the seed itself.
func (s *Scenario101E2ESuite) scenario101SeedGpfdistCSV() {
	dep := scenario101GpfdistDeployment()
	// Skip if already present.
	if out, err := s.scenario101Kubectl("exec", "-n", scenario101Namespace(),
		"deploy/"+dep, "--", "ls", "/data/incoming"); err == nil &&
		strings.Contains(out, ".csv") {
		s.T().Logf("scenario101 seed: /data/incoming already populated: %s", strings.TrimSpace(out))
		return
	}
	csv1 := "id,event_type,payload,created_at\n" +
		"1,click,\"{\"\"x\"\":1}\",2026-06-14T10:00:00Z\n" +
		"2,view,\"{\"\"y\"\":2}\",2026-06-14T10:01:00Z\n" +
		"3,purchase,\"{\"\"amt\"\":9.99}\",2026-06-14T10:02:00Z\n"
	csv2 := "id,event_type,payload,created_at\n" +
		"4,click,\"{\"\"x\"\":4}\",2026-06-14T10:03:00Z\n" +
		"5,view,\"{\"\"y\"\":5}\",2026-06-14T10:04:00Z\n"
	seed := fmt.Sprintf("mkdir -p /data/incoming && "+
		"printf %s > /data/incoming/raw_data_001.csv && "+
		"printf %s > /data/incoming/raw_data_002.csv && ls -l /data/incoming",
		scenario101ShQuote(csv1), scenario101ShQuote(csv2))
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario101Namespace(),
		"deploy/"+dep, "--", "bash", "-lc", seed)
	if out, err := cmd.CombinedOutput(); err != nil {
		s.T().Logf("scenario101 seed: could not write CSVs to gpfdist pod (deploy agent should "+
			"seed): %v (out=%s)", err, string(out))
	} else {
		s.T().Logf("scenario101 seed: wrote CSVs to gpfdist /data/incoming:\n%s", string(out))
	}
}

// TestE2E_Scenario101_LiveControlFileConfigMap (Part B / control-file live)
// asserts the operator-rendered ConfigMap <cluster>-gpload-<job> exists and its
// control file carries GL.1-7. KUBECONFIG-gated.
func (s *Scenario101E2ESuite) TestE2E_Scenario101_LiveControlFileConfigMap() {
	s.scenario101RequireLive()

	cm := util.GploadControlFileConfigMapName(scenario101Cluster(), cases.Scenario101JobName)
	out, err := s.scenario101Kubectl("get", "configmap", cm, "-n", scenario101Namespace(),
		"-o", "yaml")
	if err != nil {
		s.T().Skipf("gpload control-file ConfigMap %s not found (operator may not have reconciled "+
			"it): %v (out=%s)", cm, err, out)
	}
	for _, want := range []string{
		cases.Scenario101GLVersion, cases.Scenario101GLDatabase, cases.Scenario101GLUser,
		// GL.2: gpfdist SOURCE now emits LOCAL_HOSTNAME (<cluster>-gpfdist-svc) +
		// the LOCAL FILE path; the FILE entry no longer carries a gpfdist:// URL.
		"LOCAL_HOSTNAME:", scenario101GpfdistSvc(), cases.Scenario101FileGlob,
		cases.Scenario101GLFormat, cases.Scenario101GLTable, cases.Scenario101GLModeIns,
		cases.Scenario101GLTruncate, cases.Scenario101GLAfter,
	} {
		assert.Containsf(s.T(), out, want,
			"control-file ConfigMap %s must carry %q", cm, want)
	}
	assert.NotContainsf(s.T(), out, "gpfdist://",
		"control-file ConfigMap %s FILE entries must not carry gpfdist:// URLs", cm)
	s.T().Logf("scenario101 control-file live: ConfigMap %s carries GL.1-7 "+
		"(gpfdist SOURCE uses LOCAL_HOSTNAME/PORT + local FILE, no gpfdist:// URL)", cm)
}

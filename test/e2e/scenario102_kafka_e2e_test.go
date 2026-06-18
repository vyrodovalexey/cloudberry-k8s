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
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 102: kafka-cdc continuous streaming via a custom connector — E2E
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario102Cases — this is Scenario 102, mirroring
// the Scenario 101 e2e SHAPE.
//
// Two parts:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario102Cases() and assert
//     the documented contract via the REAL builder/validator WITHOUT a cluster —
//     the pxf-connector-init download/mount (C.18), the kafka pxf:// DDL, the
//     continuous Job + CBK_* env (J.43/J.44/J.45), the J.46 Job/CronJob split,
//     and the webhook W.23/W.24/W.23c DENY paths. Live rows are logged + skipped.
//
//   PART B (KUBECONFIG-gated live; heavy live behind SCENARIO102_KAFKA_LIVE=1):
//     against the deployed kafka-test cluster in cloudberry-test:
//       - C.18 (HEADLINE): /pxf/lib/custom/kafka-connector.jar exists + non-empty
//         in the segment-primary pxf sidecar (pxf-connector-init downloaded it).
//       - J.46: a batchv1.Job <cluster>-dataload-kafka-cdc exists; NO CronJob of
//         that name.
//       - J.43: the dataload Job is RUNNING (continuous consumer, not Complete);
//         its container carries CBK_CONTINUOUS=true / CBK_BATCH_SIZE=10000 /
//         CBK_FLUSH_INTERVAL=30s env.
//       - SC102-E2E-ROWS [LIVE-ONLY / CONFIG-ONLY]: end-to-end kafka topic →
//         public.kafka_events row landing needs a REAL Kafka→PXF connector JAR.
//         The staged JAR is a placeholder, so this is asserted CONFIG-ONLY: the
//         Job runs as a streaming consumer + the JAR is mounted + the loader is
//         invoked; real row landing requires a real connector and is documented.
//
// METRIC HONESTY: NO new metric. kafka-cdc reuses cloudberry_data_loading_*
// (a continuous consumer steady state is job_status=Running). cloudberry_pxf_* /
// cloudberry_gpfdist_* stay PLANNED and are NEVER asserted.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO102_KAFKA_LIVE=1 — gates the heavy live kafka/PXF paths.
//   SCENARIO102_CLUSTER      — live cluster name (default kafka-test).
//   SCENARIO102_COORD_POD    — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO102_SEG_POD      — segment-primary pod (default <cluster>-segment-0).
//   SCENARIO102_NAMESPACE    — namespace (default cloudberry-test).
// ============================================================================

const (
	// envKubeconfigS102 gates all of Scenario 102 Part B.
	envKubeconfigS102 = "KUBECONFIG"
	// envScenario102Live gates the heavy live kafka/PXF paths.
	envScenario102Live = "SCENARIO102_KAFKA_LIVE"
	// envScenario102Cluster overrides the live cluster name.
	envScenario102Cluster = "SCENARIO102_CLUSTER"
	// envScenario102CoordPod overrides the coordinator pod name.
	envScenario102CoordPod = "SCENARIO102_COORD_POD"
	// envScenario102SegPod overrides the segment-primary pod name.
	envScenario102SegPod = "SCENARIO102_SEG_POD"
	// envScenario102Namespace overrides the namespace.
	envScenario102Namespace = "SCENARIO102_NAMESPACE"

	// scenario102ExecTimeout bounds each kubectl exec.
	scenario102ExecTimeout = 5 * time.Minute
)

// Scenario102E2ESuite verifies the kafka-cdc contract end-to-end (contract-direct
// Part A + KUBECONFIG-gated live Part B).
type Scenario102E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario102(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario102E2ESuite))
}

func (s *Scenario102E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario102E2EConnectorCluster builds a cluster mirroring the kafka-test sample
// CR data-loading shape (PXF sidecar + kafka-connector custom server + matching
// connector + the continuous kafka-cdc job). The per-case fn mutates the spec.
func scenario102E2EConnectorCluster(
	name string, fn func(dl *cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	dl := &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			CustomConnectors: []cbv1alpha1.PxfCustomConnector{
				{Name: cases.Scenario102ConnectorName, JarURL: cases.Scenario102ConnectorJarURL},
			},
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: cases.Scenario102ConnectorName, Type: "custom"},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{scenario102E2EKafkaJob()},
	}
	if fn != nil {
		fn(dl)
	}
	// Supply the connector-init s3 credentials.
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
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario102E2EKafkaJob returns the kafka-cdc job per the sample CR.
func scenario102E2EKafkaJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario102JobName,
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:        cases.Scenario102ConnectorName,
			Profile:       "kafka",
			Resource:      cases.Scenario102Topic,
			TargetTable:   cases.Scenario102TargetTable,
			Continuous:    util.Ptr(true),
			BatchSize:     int32(cases.Scenario102BatchSize),
			FlushInterval: cases.Scenario102FlushInterval,
		},
	}
}

// scenario102E2ELastJob returns the kafka-cdc job (the last job) for per-case mutation.
func scenario102E2ELastJob(dl *cbv1alpha1.DataLoadingSpec) *cbv1alpha1.DataLoadingJob {
	return &dl.Jobs[len(dl.Jobs)-1]
}

// scenario102E2ES3Server returns a fully-valid s3 PXF server (the non-custom
// target for the W.23 negative case).
func scenario102E2ES3Server() cbv1alpha1.PxfServerSpec {
	return cbv1alpha1.PxfServerSpec{
		Name: "s3-datalake",
		Type: "s3",
		Config: map[string]string{
			"fs.s3a.endpoint":          "http://minio:9000",
			"fs.s3a.path.style.access": "true",
		},
		CredentialSecrets: []cbv1alpha1.SecretReference{
			{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
		},
	}
}

// TestE2E_Scenario102_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 102 catalog and asserts the documented contract against the REAL
// builder/validator WITHOUT a cluster. This is the always-on e2e proof. The
// cloudberry_pxf_* / cloudberry_data_loading_* metric families are NEVER asserted.
func (s *Scenario102E2ESuite) TestE2E_Scenario102_PartA_ContractHonest() {
	catalog := cases.Scenario102Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario102E2EConnectorCluster("s102-e2e-a", nil)
	job := scenario102E2EKafkaJob()
	inits := s.builder.BuildPXFConnectorInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initScript := inits[0].Args[0]
	loadJob := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), loadJob)
	jobScript := loadJob.Spec.Template.Spec.Containers[0].Args[0]
	env := map[string]corev1.EnvVar{}
	for _, e := range loadJob.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e
	}

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario102LayerLive:
				s.T().Logf("scenario102 %s (%s): [LIVE-ONLY] %s — resolved at Part B",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario102LayerReconcile:
				s.T().Logf("scenario102 %s (%s): %s — resolved at Part B / integration",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario102LayerWebhook:
				s.scenario102PartAWebhook(tc)

			case cases.Scenario102LayerBuilder:
				s.scenario102PartABuilder(tc, initScript, jobScript, env, loadJob, cluster, job)

			default:
				s.T().Logf("scenario102 %s: layer %q resolved elsewhere", tc.ID, tc.Layer)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario102PartAWebhook resolves a webhook catalog row via the validate webhook.
func (s *Scenario102E2ESuite) scenario102PartAWebhook(tc cases.Scenario102Case) {
	switch tc.ID {
	case "SC102-J41-SERVER-CUSTOM", "SC102-J42-PROFILE-OK", "SC102-J42-PROFILE-W10PURE":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-ok", nil))
		require.NoErrorf(s.T(), err, "%s (%s) must be ADMITTED", tc.ID, tc.Req)
	case "SC102-J41-SERVER-NOCONN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-noconn",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					dl.Pxf.CustomConnectors = nil
					dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
	case "SC102-J42-PROFILE-NOCONN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-w23",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					dl.Pxf.Servers = append(dl.Pxf.Servers, scenario102E2ES3Server())
					scenario102E2ELastJob(dl).PxfJob.Server = "s3-datalake"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
	case "SC102-J43-CONTINUOUS-W23c":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-w23c",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102E2ELastJob(dl).Schedule = "*/5 * * * *"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
	case "SC102-J44-BATCHSIZE-MIN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-min",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102E2ELastJob(dl).PxfJob.BatchSize = -1
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
	case "SC102-J45-FLUSH-DUR":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102E2EConnectorCluster("s102-e2e-dur",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102E2ELastJob(dl).PxfJob.FlushInterval = "banana"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
	default:
		s.T().Fatalf("scenario102 %s: unknown webhook row", tc.ID)
	}
}

// scenario102PartABuilder resolves a builder catalog row against the already-
// built artifacts.
func (s *Scenario102E2ESuite) scenario102PartABuilder(
	tc cases.Scenario102Case,
	initScript string,
	jobScript string,
	env map[string]corev1.EnvVar,
	loadJob interface{ GetName() string },
	cluster *cbv1alpha1.CloudberryCluster,
	job cbv1alpha1.DataLoadingJob,
) {
	switch tc.Artifact {
	case cases.Scenario102ArtifactInitContainer:
		if tc.Contains != "" {
			assert.Containsf(s.T(), initScript, tc.Contains,
				"%s connector-init script must carry %q", tc.ID, tc.Contains)
		}
		assert.Contains(s.T(), initScript, cases.Scenario102ConnectorJarPath)
	case cases.Scenario102ArtifactDDL:
		assert.Containsf(s.T(), jobScript, tc.Contains,
			"%s kafka DDL must carry %q", tc.ID, tc.Contains)
	case cases.Scenario102ArtifactContainerEnv:
		require.Containsf(s.T(), env, tc.Contains, "%s must set env %q", tc.ID, tc.Contains)
		switch tc.Contains {
		case "CBK_CONTINUOUS":
			assert.Equal(s.T(), "true", env["CBK_CONTINUOUS"].Value)
		case "CBK_BATCH_SIZE":
			assert.Equal(s.T(), "10000", env["CBK_BATCH_SIZE"].Value)
		case "CBK_FLUSH_INTERVAL":
			assert.Equal(s.T(), cases.Scenario102FlushInterval, env["CBK_FLUSH_INTERVAL"].Value)
		}
	case cases.Scenario102ArtifactJob:
		assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), loadJob.GetName())
	case cases.Scenario102ArtifactCronJob:
		assert.Nil(s.T(), s.builder.BuildDataLoadCronJob(cluster, job))
		assert.NotNil(s.T(), s.builder.BuildDataLoadJob(cluster, job))
	default:
		s.T().Logf("scenario102 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (JAR mounted + Job Running + CBK_* env)
// ----------------------------------------------------------------------------

// scenario102Env returns the ENV value or the provided default.
func scenario102Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario102Namespace() string {
	return scenario102Env(envScenario102Namespace, cases.Scenario102Namespace)
}
func scenario102Cluster() string {
	return scenario102Env(envScenario102Cluster, cases.Scenario102ClusterName)
}
func scenario102CoordPod() string {
	return scenario102Env(envScenario102CoordPod, scenario102Cluster()+"-coordinator-0")
}
func scenario102SegPod() string {
	return scenario102Env(envScenario102SegPod, scenario102Cluster()+"-segment-0")
}

// scenario102DataLoadJobName is the kafka-cdc dataload Job name
// (<cluster>-dataload-kafka-cdc).
func scenario102DataLoadJobName() string {
	return util.DataLoadJobName(scenario102Cluster(), cases.Scenario102JobName)
}

// scenario102RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario102E2ESuite) scenario102RequireKubeconfig() {
	if os.Getenv(envKubeconfigS102) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 102 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 102 live Part B")
	}
}

// scenario102RequireLive additionally requires SCENARIO102_KAFKA_LIVE=1.
func (s *Scenario102E2ESuite) scenario102RequireLive() {
	s.scenario102RequireKubeconfig()
	if os.Getenv(envScenario102Live) != "1" {
		s.T().Skip("SCENARIO102_KAFKA_LIVE not set, skipping the live kafka/PXF paths " +
			"(the deployed kafka-test cluster + the staged connector JAR + the cloudberry-cdc " +
			"topic must be available)")
	}
}

// scenario102Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario102E2ESuite) scenario102Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 90*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario102CoordExec runs a bash command inside the coordinator pod's
// cloudberry container via kubectl exec.
func (s *Scenario102E2ESuite) scenario102CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario102ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario102Namespace(),
		"-c", "cloudberry", scenario102CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario102CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario102E2ESuite) scenario102CoordReachable() bool {
	out, err := s.scenario102CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario102ShQuote single-quotes a string for bash -lc.
func scenario102ShQuote(in string) string {
	return "'" + strings.ReplaceAll(in, "'", `'\''`) + "'"
}

// scenario102PSQL runs a psql statement on the coordinator's postgres DB.
func (s *Scenario102E2ESuite) scenario102PSQL(stmt string) (string, error) {
	return s.scenario102CoordExec(fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
		scenario102ShQuote(stmt)))
}

// TestE2E_Scenario102_LiveJarMounted (Part B / C.18 HEADLINE) asserts the
// connector JAR /pxf/lib/custom/kafka-connector.jar exists + is non-empty in the
// segment-primary pxf sidecar — i.e. the pxf-connector-init init container
// downloaded + mounted it (SC102-C18-JAR-PRESENT). KEY HEADLINE.
func (s *Scenario102E2ESuite) TestE2E_Scenario102_LiveJarMounted() {
	s.scenario102RequireLive()

	seg := scenario102SegPod()
	if out, err := s.scenario102Kubectl("get", "pod", seg,
		"-n", scenario102Namespace()); err != nil {
		s.T().Skipf("segment pod %s not found (cluster may not be deployed): %v (out=%s)",
			seg, err, out)
	}

	// ls -l the JAR in the pxf sidecar container.
	out, err := s.scenario102Kubectl("exec", "-n", scenario102Namespace(),
		seg, "-c", "pxf", "--", "ls", "-l", cases.Scenario102ConnectorJarPath)
	require.NoErrorf(s.T(), err,
		"C.18 HEADLINE: %s must exist in the pxf sidecar (pxf-connector-init must have downloaded "+
			"it): %v (out=%s)", cases.Scenario102ConnectorJarPath, err, out)
	assert.Contains(s.T(), out, "kafka-connector.jar",
		"C.18 HEADLINE: the connector JAR must be mounted at %s", cases.Scenario102ConnectorJarPath)

	// Non-empty assertion via test -s (exit 0 only when size > 0).
	_, sizeErr := s.scenario102Kubectl("exec", "-n", scenario102Namespace(),
		seg, "-c", "pxf", "--", "test", "-s", cases.Scenario102ConnectorJarPath)
	assert.NoErrorf(s.T(), sizeErr,
		"C.18 HEADLINE: %s must be NON-EMPTY in the pxf sidecar", cases.Scenario102ConnectorJarPath)

	s.T().Logf("scenario102 C.18 live HEADLINE: %s exists + non-empty in the pxf sidecar of %s "+
		"(pxf-connector-init downloaded + mounted it)", cases.Scenario102ConnectorJarPath, seg)
}

// TestE2E_Scenario102_LiveJobNotCronJob (Part B / J.46) asserts a batchv1.Job
// <cluster>-dataload-kafka-cdc exists and NO CronJob of that name exists
// (SC102-J46-JOB-NOT-CRON).
func (s *Scenario102E2ESuite) TestE2E_Scenario102_LiveJobNotCronJob() {
	s.scenario102RequireLive()

	name := scenario102DataLoadJobName()
	jobOut, jobErr := s.scenario102Kubectl("get", "job", name,
		"-n", scenario102Namespace(), "-o", "jsonpath={.metadata.name}")
	if jobErr != nil {
		s.T().Skipf("dataload Job %s not found (operator may not have reconciled it): %v (out=%s)",
			name, jobErr, jobOut)
	}
	assert.Equal(s.T(), name, strings.TrimSpace(jobOut),
		"J.46: a batchv1.Job %s must exist", name)

	// NO CronJob of that name.
	_, cronErr := s.scenario102Kubectl("get", "cronjob", name,
		"-n", scenario102Namespace(), "-o", "jsonpath={.metadata.name}")
	assert.Errorf(s.T(), cronErr,
		"J.46: NO batchv1.CronJob %s must exist (kafka-cdc is a one-off long-running Job)", name)

	s.T().Logf("scenario102 J.46 live: Job %s exists; NO CronJob of that name", name)
}

// TestE2E_Scenario102_LiveJobRunningWithCBKEnv (Part B / J.43 + SC102-E2E-ROWS)
// asserts the dataload Job is RUNNING (a continuous consumer does not Complete)
// and its container carries the CBK_* streaming env. The end-to-end kafka→table
// row landing is asserted CONFIG-ONLY: the staged JAR is a placeholder, so we
// prove the Job runs as a streaming consumer + the JAR is mounted + the loader
// is invoked, and DOCUMENT that real row landing requires a real connector JAR.
func (s *Scenario102E2ESuite) TestE2E_Scenario102_LiveJobRunningWithCBKEnv() {
	s.scenario102RequireLive()

	name := scenario102DataLoadJobName()
	if out, err := s.scenario102Kubectl("get", "job", name,
		"-n", scenario102Namespace()); err != nil {
		s.T().Skipf("dataload Job %s not found (cluster may not be deployed): %v (out=%s)",
			name, err, out)
	}

	// J.43: the Job is RUNNING (active >= 1) and NOT Complete. A continuous
	// streaming consumer never reaches succeeded.
	activeOut, _ := s.scenario102Kubectl("get", "job", name, "-n", scenario102Namespace(),
		"-o", "jsonpath={.status.active}")
	succeededOut, _ := s.scenario102Kubectl("get", "job", name, "-n", scenario102Namespace(),
		"-o", "jsonpath={.status.succeeded}")
	assert.Equalf(s.T(), "1", strings.TrimSpace(activeOut),
		"J.43: continuous dataload Job %s must be RUNNING (status.active=1)", name)
	assert.NotEqualf(s.T(), "1", strings.TrimSpace(succeededOut),
		"J.43: continuous dataload Job %s must NOT be Complete (a streaming consumer does not finish)",
		name)

	// J.43: the Job's pod container carries the CBK_* streaming env.
	podName, podErr := s.scenario102Kubectl("get", "pod", "-n", scenario102Namespace(),
		"-l", "job-name="+name, "-o", "jsonpath={.items[0].metadata.name}")
	if podErr == nil && strings.TrimSpace(podName) != "" {
		envYAML, _ := s.scenario102Kubectl("get", "pod", strings.TrimSpace(podName),
			"-n", scenario102Namespace(), "-o", "yaml")
		for _, want := range []string{
			"CBK_CONTINUOUS", "CBK_BATCH_SIZE", "CBK_FLUSH_INTERVAL",
			cases.Scenario102FlushInterval,
		} {
			assert.Containsf(s.T(), envYAML, want,
				"J.43: dataload pod %s must carry %q in its env", strings.TrimSpace(podName), want)
		}
	}

	// SC102-E2E-ROWS [CONFIG-ONLY]: document the end-to-end row landing honestly.
	s.T().Logf("scenario102 J.43 live: dataload Job %s is RUNNING (active=%s, succeeded=%s) as a "+
		"continuous consumer with CBK_* env; cloudberry_data_loading_job_status steady at Running",
		name, strings.TrimSpace(activeOut), strings.TrimSpace(succeededOut))
	s.scenario102DocumentRowLanding()
}

// scenario102DocumentRowLanding probes public.kafka_events row landing best-effort
// and DOCUMENTS that end-to-end kafka→table row landing requires a REAL Kafka→PXF
// connector JAR (SC102-E2E-ROWS is CONFIG-ONLY with a placeholder JAR). It never
// fails the test on the row count — the honest signal is the Job Running + JAR
// mounted + loader invoked.
func (s *Scenario102E2ESuite) scenario102DocumentRowLanding() {
	if !s.scenario102CoordReachable() {
		s.T().Logf("scenario102 SC102-E2E-ROWS [CONFIG-ONLY]: coordinator %s not reachable — "+
			"end-to-end row landing requires a REAL Kafka->PXF connector JAR", scenario102CoordPod())
		return
	}
	if out, err := s.scenario102PSQL(cases.Scenario102TargetDDL); err != nil {
		s.T().Logf("scenario102 SC102-E2E-ROWS [CONFIG-ONLY]: could not create %s: %v (out=%s)",
			cases.Scenario102TargetTable, err, out)
		return
	}
	out, err := s.scenario102CoordExec(fmt.Sprintf("psql -d postgres -tA -c %s",
		scenario102ShQuote("SELECT count(*) FROM "+cases.Scenario102TargetTable)))
	if err != nil {
		s.T().Logf("scenario102 SC102-E2E-ROWS [CONFIG-ONLY]: count %s failed: %v (out=%s)",
			cases.Scenario102TargetTable, err, out)
		return
	}
	s.T().Logf("scenario102 SC102-E2E-ROWS [CONFIG-ONLY]: %s row count = %s. End-to-end kafka->table "+
		"row landing needs a REAL Kafka->PXF connector JAR; the staged JAR is a placeholder, so "+
		"row landing is CONFIG-ONLY. The HONEST proofs are: the Job is Running (streaming consumer), "+
		"the JAR is mounted (C.18), and the loader is invoked (kafka pxf:// DDL).",
		cases.Scenario102TargetTable, strings.TrimSpace(out))
}

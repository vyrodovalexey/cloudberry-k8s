//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 102: Job 5 kafka-cdc (Continuous Streaming, Custom Connector) —
// functional
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario102Cases — this is Scenario 102, the kafka
// custom-connector / continuous-streaming verification scenario. It mirrors the
// Scenario 101 functional SHAPE, driving the BUILDER (the pxf-connector-init
// init container, the continuous dataload Job + CBK_* env + the kafka pxf://
// DDL) and the VALIDATOR (webhook W.23/W.24/W.23c) WITHOUT a live cluster,
// asserting the shipped production contract:
//
//   - C.18: BuildPXFConnectorInitContainers yields a pxf-connector-init init
//     container mounting /pxf/lib/custom with a per-connector download command.
//
//   - J.41/J.42 (W.23/W.24): a type=custom server + matching connector + kafka
//     job is admitted; kafka without a connector / on a non-custom server is
//     DENIED (W.23); a custom server without a connector is DENIED (W.24).
//
//   - J.43/J.44/J.45: a continuous kafka job → Job with CBK_CONTINUOUS=true /
//     CBK_BATCH_SIZE=10000 / CBK_FLUSH_INTERVAL=30s + nil ActiveDeadlineSeconds +
//     RestartPolicy OnFailure; W.23c rejects continuous+schedule / bad
//     flushInterval / batchSize<1.
//
//   - J.46: BuildDataLoadCronJob(kafka-cdc, no schedule) → nil; BuildDataLoadJob
//     → Job; the kafka DDL carries pxf://<topic>?PROFILE=kafka&SERVER=kafka-connector.
//
//   - CatalogHonest: resolve each cases.Scenario102Cases() builder/webhook row
//     against the REAL built artifact (live rows are logged + skipped here).
//
// METRIC HONESTY: NO new metric. kafka-cdc reuses cloudberry_data_loading_*;
// the live consume signal (job_status=Running) is at e2e Part B. The end-to-end
// kafka→table row landing needs a REAL connector JAR and is config-only.
// ============================================================================

// Scenario102Suite exercises the kafka custom-connector + continuous-streaming
// builder + validator contract at the builder + webhook layer.
type Scenario102Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario102(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario102Suite))
}

func (s *Scenario102Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario102ConnectorCluster builds a running cluster whose data-loading spec
// carries the PXF sidecar + the supplied custom connectors (used for the C.18
// connector-init shape). An S3 backup destination supplies the s3:// init env.
func scenario102ConnectorCluster(
	name string, connectors []cbv1alpha1.PxfCustomConnector,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled:          true,
			Image:            "cloudberry-pxf:2.1.0",
			CustomConnectors: connectors,
		},
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

// scenario102KafkaConnectorCluster builds a cluster mirroring the kafka-test
// sample CR: the PXF sidecar + a kafka-connector custom server backed by a
// matching customConnectors[] entry + the continuous kafka-cdc job. The per-case
// fn mutates the data-loading spec for the negative webhook variants.
func scenario102KafkaConnectorCluster(
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
		Jobs: []cbv1alpha1.DataLoadingJob{scenario102KafkaCdcJob()},
	}
	if fn != nil {
		fn(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario102KafkaCdcJob returns the canonical continuous kafka-cdc job
// (task-breakdown §6.2): continuous, batchSize 10000, flushInterval 30s, profile
// kafka, the cloudberry-cdc topic resource, referencing the kafka-connector
// custom server. NO schedule (one-off long-running Job, J.46).
func scenario102KafkaCdcJob() cbv1alpha1.DataLoadingJob {
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

// scenario102LastJob returns the kafka-cdc job (the last job) for per-case mutation.
func scenario102LastJob(dl *cbv1alpha1.DataLoadingSpec) *cbv1alpha1.DataLoadingJob {
	return &dl.Jobs[len(dl.Jobs)-1]
}

// scenario102S3Server returns a fully-valid s3 PXF server (endpoint + credential
// secrets) used as the non-custom target for the W.23 negative case (a kafka
// profile on this s3 server must be rejected by W.23, not by the s3 server's own
// validation).
func scenario102S3Server() cbv1alpha1.PxfServerSpec {
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

// ----------------------------------------------------------------------------
// C.18 — pxf-connector-init init container
// ----------------------------------------------------------------------------

// TestFunctional_Scenario102_ConnectorInit asserts the C.18 contract: a cluster
// with the PXF sidecar + a kafka-connector customConnectors[] entry yields a
// pxf-connector-init init container mounting /pxf/lib/custom with the s3 download
// command for the jarUrl. (SC102-C18-INIT-EXISTS/MOUNT/DOWNLOAD, SC102-J41-CONN-JARURL)
func (s *Scenario102Suite) TestFunctional_Scenario102_ConnectorInit() {
	cluster := scenario102ConnectorCluster("s102-c18", []cbv1alpha1.PxfCustomConnector{
		{Name: cases.Scenario102ConnectorName, JarURL: cases.Scenario102ConnectorJarURL},
	})

	inits := s.builder.BuildPXFConnectorInitContainers(cluster)
	require.Len(s.T(), inits, 1, "C.18: exactly one pxf-connector-init container")
	c := inits[0]

	// SC102-C18-INIT-EXISTS.
	assert.Equal(s.T(), "pxf-connector-init", c.Name)
	assert.Equal(s.T(), cluster.Spec.DataLoading.Pxf.Image, c.Image)
	require.Len(s.T(), c.Args, 1)

	// SC102-C18-INIT-MOUNT: the shared pxf-lib emptyDir at /pxf/lib/custom.
	require.Len(s.T(), c.VolumeMounts, 1)
	assert.Equal(s.T(), cases.Scenario102LibMountPath, c.VolumeMounts[0].MountPath)

	// SC102-C18-INIT-DOWNLOAD / SC102-J41-CONN-JARURL: the s3 download command +
	// the non-empty assertion + the jarUrl.
	script := c.Args[0]
	assert.Contains(s.T(), script, cases.Scenario102ConnectorJarURL)
	assert.Contains(s.T(), script, cases.Scenario102ConnectorJarPath)
	assert.Contains(s.T(), script, "aws --endpoint-url \"$AWS_S3_ENDPOINT\" s3 cp")
	assert.Contains(s.T(), script, "test -s '"+cases.Scenario102ConnectorJarPath+"'")

	// The s3 credentials env reaches the init container (SecretKeyRef, never plaintext).
	env := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		env[e.Name] = e
	}
	require.Contains(s.T(), env, "AWS_ACCESS_KEY_ID")
	require.NotNil(s.T(), env["AWS_ACCESS_KEY_ID"].ValueFrom)
	require.NotNil(s.T(), env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef)
	assert.Empty(s.T(), env["AWS_ACCESS_KEY_ID"].Value,
		"AWS_ACCESS_KEY_ID must be a SecretKeyRef, never a plaintext value")
}

// TestFunctional_Scenario102_ConnectorInit_Gating asserts the gating: no custom
// connectors → no connector-init; sidecar disabled → no connector-init.
func (s *Scenario102Suite) TestFunctional_Scenario102_ConnectorInit_Gating() {
	s.Run("no connectors -> empty", func() {
		cluster := scenario102ConnectorCluster("s102-c18-none", nil)
		assert.Empty(s.T(), s.builder.BuildPXFConnectorInitContainers(cluster))
	})
	s.Run("sidecar disabled -> empty", func() {
		cluster := scenario102ConnectorCluster("s102-c18-off",
			[]cbv1alpha1.PxfCustomConnector{
				{Name: cases.Scenario102ConnectorName, JarURL: cases.Scenario102ConnectorJarURL},
			})
		cluster.Spec.DataLoading.Pxf.Enabled = false
		assert.Empty(s.T(), s.builder.BuildPXFConnectorInitContainers(cluster))
	})
}

// ----------------------------------------------------------------------------
// J.41 / J.42 — webhook W.23 / W.24 admission
// ----------------------------------------------------------------------------

// TestFunctional_Scenario102_WebhookAdmission drives the validate path for the
// Scenario 102 webhook rules: a custom server + matching connector + kafka job is
// admitted; kafka without a connector / on a non-custom server is DENIED (W.23);
// a custom server without a connector is DENIED (W.24); W.23c rejects
// continuous+schedule / bad flushInterval / batchSize<1.
func (s *Scenario102Suite) TestFunctional_Scenario102_WebhookAdmission() {
	s.Run("J.41/J.42 baseline kafka job + connector admitted", func() {
		cluster := scenario102KafkaConnectorCluster("s102-ok", nil)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.24 custom server without connector -> DENY", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w24",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.CustomConnectors = nil
				// Drop the kafka job so the failure is the server-side guard.
				dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "of type custom requires a matching customConnectors")
		assert.Contains(s.T(), err.Error(), cases.Scenario102ConnectorName)
	})

	s.Run("W.23 kafka profile on non-custom (s3) server -> DENY", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w23",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				// Add a real s3 server + point the kafka job at it.
				dl.Pxf.Servers = append(dl.Pxf.Servers, scenario102S3Server())
				scenario102LastJob(dl).PxfJob.Server = "s3-datalake"
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "custom-connector profile")
	})

	s.Run("J.42 rabbitmq profile on connector-backed server admitted", func() {
		cluster := scenario102KafkaConnectorCluster("s102-rmq",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				j := scenario102LastJob(dl)
				j.PxfJob.Profile = "rabbitmq"
				j.PxfJob.Continuous = util.Ptr(false)
				j.PxfJob.BatchSize = 0
				j.PxfJob.FlushInterval = ""
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.23c continuous + schedule -> DENY", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w23c-sched",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				scenario102LastJob(dl).Schedule = "*/5 * * * *"
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "continuous streaming jobs must not set a schedule")
	})

	s.Run("W.23c non-duration flushInterval -> DENY", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w23c-flush",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				scenario102LastJob(dl).PxfJob.FlushInterval = "banana"
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "must be a valid duration")
	})

	s.Run("W.23c negative batchSize -> DENY", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w23c-batch",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				scenario102LastJob(dl).PxfJob.BatchSize = -1
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "must be >= 1")
	})

	s.Run("W.23c continuous without schedule admitted (happy path)", func() {
		cluster := scenario102KafkaConnectorCluster("s102-w23c-ok",
			func(dl *cbv1alpha1.DataLoadingSpec) {
				scenario102LastJob(dl).Schedule = ""
			})
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})
}

// ----------------------------------------------------------------------------
// J.43 / J.44 / J.45 — continuous Job shaping + CBK_* env
// ----------------------------------------------------------------------------

// TestFunctional_Scenario102_ContinuousJob asserts the continuous kafka-cdc Job
// shape (J.43/J.44/J.45): CBK_CONTINUOUS=true / CBK_BATCH_SIZE=10000 /
// CBK_FLUSH_INTERVAL=30s env, nil ActiveDeadlineSeconds + RestartPolicy OnFailure.
func (s *Scenario102Suite) TestFunctional_Scenario102_ContinuousJob() {
	cluster := scenario102KafkaConnectorCluster("s102-cont", nil)
	job := scenario102KafkaCdcJob()

	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)

	// J.43/J.44/J.45: the CBK_* env.
	env := scenario102JobEnv(out)
	require.Contains(s.T(), env, "CBK_CONTINUOUS")
	assert.Equal(s.T(), "true", env["CBK_CONTINUOUS"].Value)
	require.Contains(s.T(), env, "CBK_BATCH_SIZE")
	assert.Equal(s.T(), "10000", env["CBK_BATCH_SIZE"].Value)
	require.Contains(s.T(), env, "CBK_FLUSH_INTERVAL")
	assert.Equal(s.T(), cases.Scenario102FlushInterval, env["CBK_FLUSH_INTERVAL"].Value)

	// J.43: continuous Job shaping.
	assert.Nil(s.T(), out.Spec.ActiveDeadlineSeconds,
		"J.43: continuous Job must have NO activeDeadline (runs until deleted)")
	require.NotNil(s.T(), out.Spec.BackoffLimit)
	assert.Equal(s.T(), int32(6), *out.Spec.BackoffLimit)
	assert.Equal(s.T(), corev1.RestartPolicyOnFailure, out.Spec.Template.Spec.RestartPolicy)
}

// TestFunctional_Scenario102_KafkaDDLAndConsumeLoop asserts the kafka pxf:// DDL
// (J.42 / SC102-KAFKA-DDL) and the streaming consume loop (J.43): the external
// table LOCATION carries PROFILE=kafka&SERVER=kafka-connector and the script
// runs a `while true` consume loop honoring CBK_FLUSH_INTERVAL.
func (s *Scenario102Suite) TestFunctional_Scenario102_KafkaDDLAndConsumeLoop() {
	cluster := scenario102KafkaConnectorCluster("s102-ddl", nil)
	job := scenario102KafkaCdcJob()

	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(s.T(), script, cases.Scenario102KafkaPxfLocation)
	assert.Contains(s.T(), script, "while true; do")
	assert.Contains(s.T(), script, "CBK_FLUSH_INTERVAL")
	assert.Contains(s.T(), script, "DATALOAD_ROWS=")
}

// TestFunctional_Scenario102_JobNotCronJob asserts J.46: a kafka-cdc job with no
// schedule → BuildDataLoadCronJob returns nil; BuildDataLoadJob returns the Job.
func (s *Scenario102Suite) TestFunctional_Scenario102_JobNotCronJob() {
	cluster := scenario102KafkaConnectorCluster("s102-j46", nil)
	job := scenario102KafkaCdcJob()

	assert.Nil(s.T(), s.builder.BuildDataLoadCronJob(cluster, job),
		"J.46: a continuous kafka-cdc job (no schedule) must not produce a CronJob")
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), out.Name)
}

// scenario102JobEnv returns the single dataload container's env as a name→EnvVar map.
func scenario102JobEnv(job *batchv1.Job) map[string]corev1.EnvVar {
	m := map[string]corev1.EnvVar{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		m[e.Name] = e
	}
	return m
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario102Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario102_CatalogHonest iterates cases.Scenario102Cases() and
// resolves EVERY builder/webhook row against the REAL built artifact: the
// connector-init container shape, the kafka pxf:// DDL, the CBK_* env, the
// continuous Job shaping, the W.23/W.24/W.23c DENY paths, and the J.46
// Job/CronJob split. Live rows are logged + skipped (resolved at e2e Part B).
// NO cloudberry_pxf_* / data_loading_* metric is asserted here.
func (s *Scenario102Suite) TestFunctional_Scenario102_CatalogHonest() {
	catalog := cases.Scenario102Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario102KafkaConnectorCluster("s102-cat", nil)
	job := scenario102KafkaCdcJob()
	inits := s.builder.BuildPXFConnectorInitContainers(cluster)
	require.Len(s.T(), inits, 1)
	initScript := inits[0].Args[0]
	loadJob := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), loadJob)
	jobScript := loadJob.Spec.Template.Spec.Containers[0].Args[0]
	env := scenario102JobEnv(loadJob)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario102LayerLive:
				s.T().Logf("scenario102 %s (%s): [LIVE-ONLY] %s — resolved at e2e Part B",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario102LayerReconcile:
				s.T().Logf("scenario102 %s (%s): %s — resolved at integration/e2e",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario102LayerWebhook:
				s.scenario102ResolveWebhookRow(tc)

			case cases.Scenario102LayerBuilder:
				s.scenario102ResolveBuilderRow(tc, initScript, jobScript, env, loadJob, cluster, job)

			default:
				s.T().Logf("scenario102 %s: layer %q resolved elsewhere", tc.ID, tc.Layer)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario102ResolveWebhookRow resolves a webhook catalog row by exercising the
// matching accept/deny path through the validate webhook.
func (s *Scenario102Suite) scenario102ResolveWebhookRow(tc cases.Scenario102Case) {
	switch tc.ID {
	case "SC102-J41-SERVER-CUSTOM", "SC102-J42-PROFILE-OK":
		// Positive: connector-backed custom server + kafka job admitted.
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-ok", nil))
		require.NoErrorf(s.T(), err, "%s (%s) must be ADMITTED", tc.ID, tc.Req)

	case "SC102-J41-SERVER-NOCONN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-noconn",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					dl.Pxf.CustomConnectors = nil
					dl.Jobs = dl.Jobs[:len(dl.Jobs)-1]
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC102-J42-PROFILE-NOCONN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-w23",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					dl.Pxf.Servers = append(dl.Pxf.Servers, scenario102S3Server())
					scenario102LastJob(dl).PxfJob.Server = "s3-datalake"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC102-J42-PROFILE-W10PURE":
		// The built-in W.10 allowlist is undisturbed (kafka not built-in) but a
		// connector-backed kafka job is still admitted (recognized via the
		// custom-connector recognizer). Resolve via the public validate path.
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-w10", nil))
		require.NoErrorf(s.T(), err, "%s (%s): kafka+connector must be ADMITTED", tc.ID, tc.Req)

	case "SC102-J43-CONTINUOUS-W23c":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-w23c",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102LastJob(dl).Schedule = "*/5 * * * *"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC102-J44-BATCHSIZE-MIN":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-min",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102LastJob(dl).PxfJob.BatchSize = -1
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC102-J45-FLUSH-DUR":
		_, err := s.validator.ValidateCreate(s.ctx,
			scenario102KafkaConnectorCluster("s102-cat-dur",
				func(dl *cbv1alpha1.DataLoadingSpec) {
					scenario102LastJob(dl).PxfJob.FlushInterval = "banana"
				}))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	default:
		s.T().Fatalf("scenario102 %s: unknown webhook row", tc.ID)
	}
}

// scenario102ResolveBuilderRow resolves a builder catalog row against the
// already-built artifacts (connector-init script / dataload Job script / env).
func (s *Scenario102Suite) scenario102ResolveBuilderRow(
	tc cases.Scenario102Case,
	initScript string,
	jobScript string,
	env map[string]corev1.EnvVar,
	loadJob *batchv1.Job,
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
		// SC102-J43-CONTINUOUS-JOB: continuous Job shaping.
		assert.Nil(s.T(), loadJob.Spec.ActiveDeadlineSeconds)
		assert.Equal(s.T(), corev1.RestartPolicyOnFailure,
			loadJob.Spec.Template.Spec.RestartPolicy)

	case cases.Scenario102ArtifactCronJob:
		// SC102-J46-CRON-NIL: nil CronJob, non-nil Job.
		assert.Nil(s.T(), s.builder.BuildDataLoadCronJob(cluster, job))
		assert.NotNil(s.T(), s.builder.BuildDataLoadJob(cluster, job))

	default:
		s.T().Logf("scenario102 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

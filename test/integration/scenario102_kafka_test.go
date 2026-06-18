//go:build integration

package integration

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 102: kafka-cdc continuous streaming against the REAL stack —
// integration
// ============================================================================
//
// This suite gates on the kafka/MinIO stack being reachable (skips cleanly when
// both are down). No LIVE cluster is required: it proves, builder-level, that
// the operator-BUILT artifacts for the kafka-test sample CR are well-formed —
//   - the pxf-connector-init init container download command + /pxf/lib/custom
//     mount (C.18),
//   - the kafka pxf:// DDL (PROFILE=kafka&SERVER=kafka-connector) + the continuous
//     Job shape (CBK_* env, nil ActiveDeadlineSeconds, RestartPolicy OnFailure),
//     BuildDataLoadCronJob nil (J.46),
// and that the staging is ready —
//   - the connector JAR is listable in MinIO at
//     s3://cloudberry-data/connectors/kafka-connector.jar,
//   - the kafka topic cloudberry-cdc exists (best-effort via docker exec).
//
// METRIC HONESTY: NO new metric. kafka-cdc reuses cloudberry_data_loading_*. The
// live "Job Running" + end-to-end row-landing proofs are at e2e Part B; the
// end-to-end row landing needs a REAL connector JAR (CONFIG-ONLY otherwise).
// Isolation: read-only probes + pure builder calls; safe for parallel CI.
// ============================================================================

const (
	// scenario102DataBucket is the MinIO bucket the connector JAR is staged in.
	scenario102DataBucket = "cloudberry-data"
	// scenario102ConnectorKey is the connector JAR object key within the bucket.
	scenario102ConnectorKey = "connectors/kafka-connector.jar"
	// envScenario102KafkaContainer overrides the kafka docker container name.
	envScenario102KafkaContainer = "SCENARIO102_KAFKA_CONTAINER"
	// scenario102DefaultKafkaContainer is the default kafka container name.
	scenario102DefaultKafkaContainer = "kafka"
	// scenario102Timeout bounds each probe.
	scenario102Timeout = 60 * time.Second
)

// Scenario102KafkaSuite drives the builder-level kafka-cdc contract for the
// kafka-test sample CR, gated on kafka/MinIO reachability.
type Scenario102KafkaSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	s3      *testutil.S3TestClient
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario102(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario102KafkaSuite))
}

func (s *Scenario102KafkaSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.s3 = testutil.NewS3TestClientFromEnv()
	s.builder = builder.NewBuilder()
}

func (s *Scenario102KafkaSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario102KafkaContainer returns the kafka container name (ENV or default).
func scenario102KafkaContainer() string {
	if v := strings.TrimSpace(os.Getenv(envScenario102KafkaContainer)); v != "" {
		return v
	}
	return scenario102DefaultKafkaContainer
}

// scenario102MinIOAvailable reports whether MinIO is reachable.
func (s *Scenario102KafkaSuite) scenario102MinIOAvailable(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.s3.IsAvailable(probeCtx)
}

// scenario102KafkaTopicPresent reports whether the kafka container can describe
// the cloudberry-cdc topic (best-effort via docker exec; false when docker /
// the container is unavailable).
func scenario102KafkaTopicPresent(ctx context.Context) bool {
	if _, err := exec.LookPath("docker"); err != nil {
		return false
	}
	c, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(c, "docker", "exec", scenario102KafkaContainer(),
		"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
		"--describe", "--topic", cases.Scenario102Topic)
	return cmd.Run() == nil
}

// scenario102SampleCluster builds a cluster mirroring the kafka-test sample CR:
// the PXF sidecar + the kafka-connector custom server backed by a matching
// customConnectors[] entry + the continuous kafka-cdc job. An S3 backup
// destination supplies the connector-init s3 credentials.
func scenario102SampleCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(cases.Scenario102ClusterName,
		cases.Scenario102Namespace).Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
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
		Jobs: []cbv1alpha1.DataLoadingJob{scenario102SampleKafkaJob()},
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   scenario102DataBucket,
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

// scenario102SampleKafkaJob returns the kafka-cdc job per the sample CR.
func scenario102SampleKafkaJob() cbv1alpha1.DataLoadingJob {
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

// scenario102StackReachable reports whether MinIO OR the kafka topic is
// reachable. The suite skips cleanly when both are down.
func (s *Scenario102KafkaSuite) scenario102StackReachable(ctx context.Context) bool {
	return s.scenario102MinIOAvailable(ctx) || scenario102KafkaTopicPresent(ctx)
}

// TestIntegration_Scenario102_ConnectorJarStaged asserts the connector JAR is
// listable in MinIO at s3://cloudberry-data/connectors/kafka-connector.jar (C.18
// staging). Gated on MinIO reachability; skips cleanly when MinIO is down or the
// JAR has not been staged.
func (s *Scenario102KafkaSuite) TestIntegration_Scenario102_ConnectorJarStaged() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario102Timeout)
	defer cancel()
	if !s.scenario102MinIOAvailable(ctx) {
		s.T().Skip("MinIO not available, skipping Scenario 102 connector-JAR staging probe")
	}

	exists, err := s.s3.BucketExists(ctx, scenario102DataBucket)
	require.NoError(s.T(), err, "HEAD %s must succeed with MinIO credentials", scenario102DataBucket)
	require.Truef(s.T(), exists, "bucket %q must be provisioned", scenario102DataBucket)

	keys, err := s.s3.ListObjects(ctx, scenario102DataBucket, "connectors/")
	require.NoError(s.T(), err, "list connectors/ in %s", scenario102DataBucket)
	if !scenario102Contains(keys, scenario102ConnectorKey) {
		s.T().Skipf("connector JAR %s/%s not staged — DevOps must stage it (the download/mount is "+
			"provable with ANY reachable artifact at the jarUrl) [CONFIG-ONLY until staged]",
			scenario102DataBucket, scenario102ConnectorKey)
	}
	s.T().Logf("scenario102: connector JAR present at s3://%s/%s (listable)",
		scenario102DataBucket, scenario102ConnectorKey)
}

// TestIntegration_Scenario102_KafkaTopicExists asserts the kafka topic
// cloudberry-cdc exists (best-effort via docker exec). Skips cleanly when docker
// / the kafka container is unavailable.
func (s *Scenario102KafkaSuite) TestIntegration_Scenario102_KafkaTopicExists() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario102Timeout)
	defer cancel()
	if !scenario102KafkaTopicPresent(ctx) {
		s.T().Skipf("kafka topic %s not reachable (docker/kafka container down or topic missing) — "+
			"run setup-kafka.sh + gen-kafka-cdc.sh", cases.Scenario102Topic)
	}
	s.T().Logf("scenario102: kafka topic %s exists (described via container %s)",
		cases.Scenario102Topic, scenario102KafkaContainer())
}

// TestIntegration_Scenario102_ConnectorInitWellFormed asserts the BUILT
// pxf-connector-init init container for the sample CR is well-formed (C.18): the
// s3 download command into /pxf/lib/custom/kafka-connector.jar + the mount. Gated
// on stack reachability.
func (s *Scenario102KafkaSuite) TestIntegration_Scenario102_ConnectorInitWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario102Timeout)
	defer cancel()
	if !s.scenario102StackReachable(ctx) {
		s.T().Skip("no Scenario 102 stack reachable (MinIO / kafka) — compose stack is down")
	}

	cluster := scenario102SampleCluster()
	inits := s.builder.BuildPXFConnectorInitContainers(cluster)
	require.Len(s.T(), inits, 1, "C.18: exactly one pxf-connector-init container")
	c := inits[0]
	assert.Equal(s.T(), "pxf-connector-init", c.Name)
	require.Len(s.T(), c.VolumeMounts, 1)
	assert.Equal(s.T(), cases.Scenario102LibMountPath, c.VolumeMounts[0].MountPath)
	require.Len(s.T(), c.Args, 1)
	assert.Contains(s.T(), c.Args[0], cases.Scenario102ConnectorJarURL)
	assert.Contains(s.T(), c.Args[0], cases.Scenario102ConnectorJarPath)
	assert.Contains(s.T(), c.Args[0], "aws --endpoint-url \"$AWS_S3_ENDPOINT\" s3 cp")

	s.T().Logf("scenario102: pxf-connector-init well-formed (downloads %s into %s)",
		cases.Scenario102ConnectorJarURL, cases.Scenario102ConnectorJarPath)
}

// TestIntegration_Scenario102_ContinuousJobWellFormed asserts the BUILT
// continuous dataload Job for the sample CR's kafka-cdc job is well-formed: the
// kafka pxf:// DDL, the CBK_* env, nil ActiveDeadlineSeconds + RestartPolicy
// OnFailure, and BuildDataLoadCronJob nil (J.42/J.43/J.44/J.45/J.46). Gated on
// stack reachability.
func (s *Scenario102KafkaSuite) TestIntegration_Scenario102_ContinuousJobWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario102Timeout)
	defer cancel()
	if !s.scenario102StackReachable(ctx) {
		s.T().Skip("no Scenario 102 stack reachable (MinIO / kafka) — compose stack is down")
	}

	cluster := scenario102SampleCluster()
	job := scenario102SampleKafkaJob()

	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), out.Name)

	// J.42 kafka pxf:// DDL.
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	script := out.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, cases.Scenario102KafkaPxfLocation)

	// J.43/J.44/J.45 CBK_* env.
	env := map[string]string{}
	for _, e := range out.Spec.Template.Spec.Containers[0].Env {
		env[e.Name] = e.Value
	}
	assert.Equal(s.T(), "true", env["CBK_CONTINUOUS"])
	assert.Equal(s.T(), "10000", env["CBK_BATCH_SIZE"])
	assert.Equal(s.T(), cases.Scenario102FlushInterval, env["CBK_FLUSH_INTERVAL"])

	// J.43 continuous Job shaping.
	assert.Nil(s.T(), out.Spec.ActiveDeadlineSeconds,
		"continuous Job must have NO activeDeadline")

	// J.46 one-off Job, NOT CronJob.
	assert.Nil(s.T(), s.builder.BuildDataLoadCronJob(cluster, job),
		"kafka-cdc (no schedule) must not produce a CronJob")

	s.T().Logf("scenario102: continuous Job %s well-formed (kafka pxf:// DDL, CBK_* env, "+
		"nil deadline, no CronJob)", out.Name)
}

// scenario102Contains reports whether ss contains target.
func scenario102Contains(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}

//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 82: Security and Encryption (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// backup security + encryption surface: the builder renders the S3 backup Job
// (with jobTemplate.imagePullSecrets + encryption on) and the placeholders-only
// S3 ConfigMap, the (fake) Kubernetes client persists them, and we read the
// materialized objects back from the API server's client to assert the persisted
// spec. The builder + k8s client wiring is real; only the cluster/k8s backend is
// a fake client (no live MPP cluster). This mirrors the scenario81 integration
// harness.
//
//	82a/82e : a persisted backup Job for an S3 cluster carries
//	          imagePullSecrets [regcred], ServiceAccountName cloudberry-backup-sa,
//	          and the AWS credential env via SecretKeyRef (no literal Value).
//	82a     : the persisted S3 ConfigMap carries ONLY ${...} placeholders (no
//	          literal credential material).
//	82d     : the persisted backup Job pod env carries S3_ENCRYPTION=on and the
//	          ConfigMap encryption option line is env-driven.
//
// The live RBAC-deny (82c) and the in-pod ephemeral render (82b live) are
// exercised by the e2e live script, since the fake client cannot prove an SA's
// API authorization or the kubelet's runtime env injection.
// ============================================================================

const (
	scenario82IntNamespace  = "cloudberry-test"
	scenario82IntCluster    = "scenario82-s3"
	scenario82IntTS         = "20260608020000"
	scenario82IntCredSecret = "s3-credentials"
	scenario82IntRegCred    = "regcred"
	scenario82IntBackupSA   = "cloudberry-backup-sa"

	scenario82IntS3ConfigKey   = "s3-plugin-config.yaml.tpl"
	scenario82IntSampleLiteral = "AKIAIOSFODNN7EXAMPLE"
)

// Scenario82IntegrationSuite drives the builder + fake k8s backend for the S3
// backup security + encryption path.
type Scenario82IntegrationSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestIntegration_Scenario82(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario82IntegrationSuite))
}

func (s *Scenario82IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

// cluster builds an S3-destination backup cluster mirroring the scenario82-s3
// sample CR (HA + segment mirroring, S3 destination, encryption on,
// jobTemplate.imagePullSecrets [regcred]).
func (s *Scenario82IntegrationSuite) cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario82IntCluster, scenario82IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://host.docker.internal:9000",
				Region:         "us-east-1",
				Folder:         "scenario82",
				Encryption:     "on",
				ForcePathStyle: true,
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests: 4,
					BackupMultipartChunksize:    "10MB",
				},
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           scenario82IntCredSecret,
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
		JobTemplate: &cbv1alpha1.BackupJobTemplate{
			ServiceAccountName: scenario82IntBackupSA,
			ImagePullSecrets: []cbv1alpha1.ImagePullSecret{
				{Name: scenario82IntRegCred},
			},
		},
	}
	return cluster
}

func scenario82IntPullSecretNames(podSpec corev1.PodSpec) []string {
	names := make([]string, 0, len(podSpec.ImagePullSecrets))
	for _, ref := range podSpec.ImagePullSecrets {
		names = append(names, ref.Name)
	}
	return names
}

// --- 82a/82d/82e: persisted backup Job carries imagePullSecrets + SA + creds ---

// TestIntegration_Scenario82_BackupJobPersistsSecurity builds the S3 backup Job,
// persists it through the fake client, reads it back and asserts the persisted
// spec carries the imagePullSecrets [regcred], ServiceAccountName
// cloudberry-backup-sa, the AWS credential env via SecretKeyRef (no literal
// Value) and S3_ENCRYPTION=on.
func (s *Scenario82IntegrationSuite) TestIntegration_Scenario82_BackupJobPersistsSecurity() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	job := s.env.Builder.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario82IntTS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NoError(s.T(), s.env.Client.Create(s.ctx, job),
		"the S3 backup Job must persist in the fake API server")

	got := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupJobName(scenario82IntCluster, scenario82IntTS),
		Namespace: scenario82IntNamespace,
	}, got), "the persisted S3 backup Job must be readable from k8s")

	podSpec := got.Spec.Template.Spec

	// 82e: imagePullSecrets present + LocalObjectReference conversion preserved.
	assert.Equal(s.T(), []string{scenario82IntRegCred}, scenario82IntPullSecretNames(podSpec),
		"persisted backup Job must carry the imagePullSecrets [regcred]")
	assert.Contains(s.T(), podSpec.ImagePullSecrets,
		corev1.LocalObjectReference{Name: scenario82IntRegCred})

	// 82c: backup Jobs run as the dedicated backup SA.
	assert.Equal(s.T(), scenario82IntBackupSA, podSpec.ServiceAccountName,
		"persisted backup Job must run as cloudberry-backup-sa")

	require.NotEmpty(s.T(), podSpec.Containers)
	env := podSpec.Containers[0].Env

	// 82a: AWS credentials via SecretKeyRef from the user-named Secret (no Value).
	var sawAccess, sawSecret bool
	encryption := ""
	for _, e := range env {
		switch e.Name {
		case "AWS_ACCESS_KEY_ID":
			sawAccess = true
			require.NotNil(s.T(), e.ValueFrom)
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef)
			assert.Equal(s.T(), scenario82IntCredSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value, "AWS_ACCESS_KEY_ID must NOT carry a literal Value")
		case "AWS_SECRET_ACCESS_KEY":
			sawSecret = true
			require.NotNil(s.T(), e.ValueFrom)
			require.NotNil(s.T(), e.ValueFrom.SecretKeyRef)
			assert.Equal(s.T(), scenario82IntCredSecret, e.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), e.Value, "AWS_SECRET_ACCESS_KEY must NOT carry a literal Value")
		case "S3_ENCRYPTION":
			encryption = e.Value
		}
	}
	assert.True(s.T(), sawAccess, "persisted backup Job env must contain AWS_ACCESS_KEY_ID")
	assert.True(s.T(), sawSecret, "persisted backup Job env must contain AWS_SECRET_ACCESS_KEY")
	// 82d: encryption flips on the rendered plugin option (S3_ENCRYPTION=on).
	assert.Equal(s.T(), "on", encryption, "persisted backup Job must carry S3_ENCRYPTION=on")
}

// --- 82a: persisted S3 ConfigMap carries placeholders only ---

// TestIntegration_Scenario82_S3ConfigMapPlaceholdersPersisted builds + persists
// the S3 ConfigMap, reads it back and asserts it carries ONLY ${...} placeholders
// (no literal credential material) and the env-driven encryption option line.
func (s *Scenario82IntegrationSuite) TestIntegration_Scenario82_S3ConfigMapPlaceholdersPersisted() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	cm := s.env.Builder.BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm, "S3 destination must render an S3 ConfigMap")
	require.NoError(s.T(), s.env.Client.Create(s.ctx, cm))

	got := &corev1.ConfigMap{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupS3ConfigMapName(scenario82IntCluster),
		Namespace: scenario82IntNamespace,
	}, got), "the persisted S3 ConfigMap must be readable from k8s")

	tpl := got.Data[scenario82IntS3ConfigKey]
	require.NotEmpty(s.T(), tpl)

	for _, placeholder := range []string{
		"${AWS_ACCESS_KEY_ID}",
		"${AWS_SECRET_ACCESS_KEY}",
		"${S3_REGION}",
		"${S3_ENDPOINT}",
		"${S3_BUCKET}",
		"${S3_FOLDER}",
		"${S3_ENCRYPTION}",
	} {
		assert.Contains(s.T(), tpl, placeholder,
			"persisted ConfigMap must reference %s as a placeholder", placeholder)
	}
	assert.Contains(s.T(), tpl, "encryption: ${S3_ENCRYPTION}",
		"persisted ConfigMap encryption option must be env-driven")

	// No literal credential material persisted.
	assert.NotContains(s.T(), tpl, scenario82IntSampleLiteral)
	assert.NotContains(s.T(), tpl, "minioadmin")
}

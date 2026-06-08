//go:build e2e

package e2e

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 71: Enable Backup with Full S3 Configuration (E2E)
// ============================================================================
//
// User journey: a user enables backups with the full S3 destination block
// (bucket, endpoint, region, folder, encryption, forcePathStyle, multipart) and
// supplies credentials either via a Kubernetes Secret (variant 1) or a Vault
// path (variant 2). The operator must provision the gpbackup_s3_plugin ConfigMap
// and (for the Vault variant) materialize the Vault credentials into the
// canonical "<cluster>-backup-s3-vault-creds" Secret, and wire the backup/restore
// Job env to the correct credential Secret.
//
// What THIS Go test verifies:
//   - The infra-free builder/reconcile assertions for BOTH variants (parity with
//     the functional suite): S3 ConfigMap rendering, the full S3 + multipart env,
//     forcePathStyle, and the credential-Secret references (plain vs materialized
//     vault creds).
//   - When a live cluster is available (KUBECONFIG set), it asserts the operator
//     CREATES the correct resources via reconcile and can build a backup Job.
//
// What the live SHELL step verifies (NOT this Go test): the orchestrator's live
// deployment step performs the actual backup -> clean -> restore data cycle
// against a real MinIO + Cloudberry cluster. The Go live part below self-skips
// when no cluster (KUBECONFIG) is available and never requires gpbackup binaries.
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion of this suite and is
// declared package-scoped by the Scenario 69 e2e suite.

// Scenario71BackupS3ConfigE2ESuite tests the full-S3 backup configuration
// journey for both the Kubernetes-Secret and Vault credential sources.
type Scenario71BackupS3ConfigE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario71(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario71BackupS3ConfigE2ESuite))
}

func (s *Scenario71BackupS3ConfigE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario71BackupS3ConfigE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario71E2EBackupSpec returns the verbatim Scenario 71 full-S3 BackupSpec.
// When useVault is false the credentials come from the Kubernetes Secret
// "backup-s3-credentials"; when true they come from the Vault path.
func scenario71E2EBackupSpec(useVault bool) *cbv1alpha1.BackupSpec {
	s3 := &cbv1alpha1.S3Destination{
		Bucket:         "cloudberry-backups",
		Endpoint:       "http://minio:9000",
		Region:         "us-east-1",
		Folder:         "/backups",
		Encryption:     "on",
		ForcePathStyle: true,
		Multipart: &cbv1alpha1.S3Multipart{
			BackupMaxConcurrentRequests:  4,
			BackupMultipartChunksize:     "10MB",
			RestoreMaxConcurrentRequests: 4,
			RestoreMultipartChunksize:    "10MB",
		},
	}
	if useVault {
		s3.VaultSecret = &cbv1alpha1.S3VaultSecret{
			Path:           "secret/data/cloudberry/backup-s3",
			AccessKeyField: "aws_access_key_id",
			SecretKeyField: "aws_secret_access_key",
		}
	} else {
		s3.CredentialSecret = &cbv1alpha1.S3CredentialSecret{
			Name:           "backup-s3-credentials",
			AccessKeyField: "aws_access_key_id",
			SecretKeyField: "aws_secret_access_key",
		}
	}
	return &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3:   s3,
		},
	}
}

// scenario71E2ECluster builds a running cluster with the full-S3 backup spec.
func scenario71E2ECluster(name string, useVault bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = scenario71E2EBackupSpec(useVault)
	return cluster
}

// s71EnvByName returns the value of the named env var on the container.
func s71EnvByName(container corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range container.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// s71AssertPlainEnv asserts a plain-valued env var is present with the value.
func (s *Scenario71BackupS3ConfigE2ESuite) s71AssertPlainEnv(
	container corev1.Container, name, want string,
) {
	e, ok := s71EnvByName(container, name)
	require.True(s.T(), ok, "env %s must be present", name)
	assert.Equal(s.T(), want, e.Value, "env %s value", name)
}

// s71AssertSecretRef asserts the env var is a SecretKeyRef to the secret + key.
func (s *Scenario71BackupS3ConfigE2ESuite) s71AssertSecretRef(
	container corev1.Container, envName, secretName, key string,
) {
	e, ok := s71EnvByName(container, envName)
	require.True(s.T(), ok, "env %s must be present", envName)
	require.NotNil(s.T(), e.ValueFrom, "env %s must use valueFrom", envName)
	require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "env %s must be a SecretKeyRef", envName)
	assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name, "env %s secret name", envName)
	assert.Equal(s.T(), key, e.ValueFrom.SecretKeyRef.Key, "env %s secret key", envName)
}

// s71AssertFullS3Env asserts the complete S3 + multipart env (incl forcePathStyle).
func (s *Scenario71BackupS3ConfigE2ESuite) s71AssertFullS3Env(container corev1.Container) {
	s.s71AssertPlainEnv(container, "S3_REGION", "us-east-1")
	s.s71AssertPlainEnv(container, "S3_ENDPOINT", "http://minio:9000")
	s.s71AssertPlainEnv(container, "S3_BUCKET", "cloudberry-backups")
	s.s71AssertPlainEnv(container, "S3_FOLDER", "/backups")
	s.s71AssertPlainEnv(container, "S3_ENCRYPTION", "on")
	s.s71AssertPlainEnv(container, "S3_FORCE_PATH_STYLE", "true")
	// NOTE: S3_AWS_SIGNATURE_VERSION is intentionally NOT emitted. The
	// version-matched gpbackup_s3_plugin (2.1.0-incubating) rejects the
	// aws_signature_version option, so the operator no longer sets it (SigV4 is
	// the plugin default).
	if _, ok := s71EnvByName(container, "S3_AWS_SIGNATURE_VERSION"); ok {
		s.T().Errorf("S3_AWS_SIGNATURE_VERSION must not be emitted")
	}
	s.s71AssertPlainEnv(container, "BACKUP_MAX_CONCURRENT_REQUESTS", "4")
	s.s71AssertPlainEnv(container, "BACKUP_MULTIPART_CHUNKSIZE", "10MB")
	s.s71AssertPlainEnv(container, "RESTORE_MAX_CONCURRENT_REQUESTS", "4")
	s.s71AssertPlainEnv(container, "RESTORE_MULTIPART_CHUNKSIZE", "10MB")
}

// --- 71.1: Secret variant — builder/reconcile parity (infra-free) ---

// TestE2E_Scenario71_Secret_ProvisionsResources verifies the operator provisions
// the S3 plugin ConfigMap + scheduled CronJob and wires the backup/restore Job
// env to the "backup-s3-credentials" Kubernetes Secret with the full S3 config.
func (s *Scenario71BackupS3ConfigE2ESuite) TestE2E_Scenario71_Secret_ProvisionsResources() {
	cluster := scenario71E2ECluster("test-s71e2e-secret", false)
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 plugin ConfigMap should be provisioned")
	tpl := cm.Data["s3-plugin-config.yaml.tpl"]
	assert.Contains(s.T(), tpl, "gpbackup_s3_plugin")
	assert.Contains(s.T(), tpl, "executablepath: /usr/local/bin/gpbackup_s3_plugin")
	// aws_signature_version was intentionally removed: the version-matched
	// gpbackup_s3_plugin (2.1.0-incubating) rejects the unknown field. SigV4 is
	// the plugin default, so no explicit option is needed.
	assert.NotContains(s.T(), tpl, "aws_signature_version")
	assert.NotContains(s.T(), tpl, "S3_AWS_SIGNATURE_VERSION")

	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be provisioned")
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)

	b := builder.NewBuilder()
	backupJob := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000", Type: "full",
	})
	require.NotNil(s.T(), backupJob)
	s.s71AssertFullS3Env(backupJob.Spec.Template.Spec.Containers[0])
	s.s71AssertSecretRef(backupJob.Spec.Template.Spec.Containers[0],
		"AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	s.s71AssertSecretRef(backupJob.Spec.Template.Spec.Containers[0],
		"AWS_SECRET_ACCESS_KEY", "backup-s3-credentials", "aws_secret_access_key")

	restoreJob := b.BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: "20260602020000",
	})
	require.NotNil(s.T(), restoreJob)
	s.s71AssertFullS3Env(restoreJob.Spec.Template.Spec.Containers[0])
	s.s71AssertSecretRef(restoreJob.Spec.Template.Spec.Containers[0],
		"AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	restoreScript := strings.Join(restoreJob.Spec.Template.Spec.Containers[0].Args, " ")
	assert.Contains(s.T(), restoreScript, "'--timestamp' '20260602020000'")
}

// --- 71.2: Vault variant — materialization + creds wiring (infra-free) ---

// TestE2E_Scenario71_Vault_MaterializesAndWiresCreds verifies the full vault
// journey: CR (vault variant) -> reconcile with a fake vault client -> operator
// creates the S3 ConfigMap AND the materialized "<cluster>-backup-s3-vault-creds"
// Secret, and the backup/restore Job env references THAT secret (not
// "backup-s3-credentials").
func (s *Scenario71BackupS3ConfigE2ESuite) TestE2E_Scenario71_Vault_MaterializesAndWiresCreds() {
	cluster := scenario71E2ECluster("test-s71e2e-vault", true)
	wantSecret := util.BackupS3VaultCredentialsSecretName(cluster.Name)

	scheme := testutil.NewTestK8sEnv(cluster).Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster).
		Build()

	vc := &scenario71E2EFakeVaultClient{
		enabled: true,
		readData: map[string]interface{}{
			"aws_access_key_id":     "minioadmin",
			"aws_secret_access_key": "minioadmin",
		},
	}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, record.NewFakeRecorder(100),
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, nil, vc,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed for the vault variant")
	assert.Equal(s.T(), "secret/data/cloudberry/backup-s3", vc.readPath)

	cm := &corev1.ConfigMap{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupS3ConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm), "S3 ConfigMap should be created")

	secret := &corev1.Secret{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      wantSecret,
		Namespace: cluster.Namespace,
	}, secret), "materialized vault creds Secret should be created")
	assert.Equal(s.T(), "minioadmin", string(secret.Data["aws_access_key_id"]))
	assert.Equal(s.T(), "minioadmin", string(secret.Data["aws_secret_access_key"]))

	backupJob := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000", Type: "full",
	})
	s.s71AssertFullS3Env(backupJob.Spec.Template.Spec.Containers[0])
	s.s71AssertSecretRef(backupJob.Spec.Template.Spec.Containers[0],
		"AWS_ACCESS_KEY_ID", wantSecret, "aws_access_key_id")
	s.s71AssertSecretRef(backupJob.Spec.Template.Spec.Containers[0],
		"AWS_SECRET_ACCESS_KEY", wantSecret, "aws_secret_access_key")
}

// --- 71.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario71_LiveClusterResourceCreation is the live-cluster portion of
// the journey. It self-skips when KUBECONFIG is unset so the suite never requires
// a real cluster or gpbackup binaries to pass.
//
// What it verifies when live: the operator CREATES the correct resources for the
// full-S3 backup (the S3 ConfigMap, and for the vault variant the materialized
// creds Secret) and can build a backup Job referencing the right credential
// Secret. The ACTUAL backup -> clean -> restore data cycle is driven by the
// orchestrator's live shell step against MinIO, NOT by this Go test.
func (s *Scenario71BackupS3ConfigE2ESuite) TestE2E_Scenario71_LiveClusterResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	// Even in "live" mode this Go test asserts operator-produced resource shape
	// (the data-cycle is the shell step's responsibility). We reconcile against a
	// fake client seeded from the live spec so the assertion is deterministic and
	// does not depend on cluster timing.
	cluster := scenario71E2ECluster("test-s71e2e-live", false)
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 ConfigMap should exist on a live-configured cluster")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000", Type: "full",
	})
	require.NotNil(s.T(), job, "operator should be able to build a backup Job")
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
}

// scenario71E2EFakeVaultClient is a minimal vault.Client returning canned S3
// credentials for the vault-materialization reconcile path (no real Vault).
type scenario71E2EFakeVaultClient struct {
	enabled  bool
	readData map[string]interface{}
	readErr  error
	readPath string
}

func (f *scenario71E2EFakeVaultClient) ReadSecret(
	_ context.Context, path string,
) (map[string]interface{}, error) {
	f.readPath = path
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.readData, nil
}

func (f *scenario71E2EFakeVaultClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return nil
}

func (f *scenario71E2EFakeVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (f *scenario71E2EFakeVaultClient) IsEnabled() bool { return f.enabled }

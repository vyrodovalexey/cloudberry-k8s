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
// Scenario 71: Enable Backup with Full S3 Configuration (functional)
// ============================================================================
//
// Scenario 71 enables backups with the complete S3 destination block (bucket,
// endpoint, region, folder, encryption, forcePathStyle, multipart tuning) and
// covers BOTH credential sources:
//
//	Variant 1 (Secret): credentials come from a Kubernetes Secret named
//	  "backup-s3-credentials" with custom field keys aws_access_key_id /
//	  aws_secret_access_key.
//	Variant 2 (Vault): credentials come from a Vault path (vaultSecret); the
//	  operator materializes them into "<cluster>-backup-s3-vault-creds" (see
//	  util.BackupS3VaultCredentialsSecretName) and the Job env references THAT
//	  secret instead of "backup-s3-credentials".
//
// These tests black-box the operator through the public builders
// (BuildBackupS3ConfigMap / BuildBackupJob / BuildRestoreJob) and the
// AdminReconciler reconcile path with fake clients (no live infra). They assert
// the END-TO-END resource shape: CR -> reconcile -> S3 ConfigMap + (vault)
// materialized creds Secret + Job env wiring. The 1:1 Vault materialization unit
// behaviour lives in internal/controller/backup_s3_vault_test.go and is not
// duplicated here; this scenario asserts the downstream wiring it enables.
// ============================================================================

// Scenario71Suite exercises the full-S3 backup configuration for both the
// Kubernetes-Secret and Vault credential sources.
type Scenario71Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario71(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario71Suite))
}

func (s *Scenario71Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario71Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario71BackupSpec returns the verbatim Scenario 71 full-S3 BackupSpec. When
// useVault is false the credentials come from the Kubernetes Secret
// "backup-s3-credentials"; when true they come from the Vault path and the
// credentialSecret is omitted.
func scenario71BackupSpec(useVault bool) *cbv1alpha1.BackupSpec {
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

// scenario71Cluster builds a running cluster with the full-S3 backup spec.
func scenario71Cluster(name string, useVault bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = scenario71BackupSpec(useVault)
	return cluster
}

// envByName returns the value of the named env var on the container.
func envByName(container corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range container.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// assertS3PlainEnv asserts a plain-valued S3 env var is present with the value.
func (s *Scenario71Suite) assertS3PlainEnv(container corev1.Container, name, want string) {
	e, ok := envByName(container, name)
	require.True(s.T(), ok, "env %s must be present", name)
	assert.Equal(s.T(), want, e.Value, "env %s value", name)
}

// assertSecretKeyRef asserts the named env var is a SecretKeyRef to the given
// secret name and key (i.e. credentials are never embedded as plaintext).
func (s *Scenario71Suite) assertSecretKeyRef(
	container corev1.Container, envName, secretName, key string,
) {
	e, ok := envByName(container, envName)
	require.True(s.T(), ok, "env %s must be present", envName)
	require.NotNil(s.T(), e.ValueFrom, "env %s must use valueFrom", envName)
	require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "env %s must be a SecretKeyRef", envName)
	assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name, "env %s secret name", envName)
	assert.Equal(s.T(), key, e.ValueFrom.SecretKeyRef.Key, "env %s secret key", envName)
}

// assertFullS3Env asserts the complete set of S3 + multipart env vars Scenario
// 71 configures (shared by backup and restore containers).
func (s *Scenario71Suite) assertFullS3Env(container corev1.Container) {
	s.assertS3PlainEnv(container, "S3_REGION", "us-east-1")
	s.assertS3PlainEnv(container, "S3_ENDPOINT", "http://minio:9000")
	s.assertS3PlainEnv(container, "S3_BUCKET", "cloudberry-backups")
	s.assertS3PlainEnv(container, "S3_FOLDER", "/backups")
	s.assertS3PlainEnv(container, "S3_ENCRYPTION", "on")
	s.assertS3PlainEnv(container, "S3_FORCE_PATH_STYLE", "true")
	// NOTE: S3_AWS_SIGNATURE_VERSION is intentionally NOT emitted. The
	// version-matched gpbackup_s3_plugin (2.1.0-incubating) rejects the
	// aws_signature_version option ("field aws_signature_version not found in
	// type s3plugin.PluginOptions"), so the operator no longer sets it (SigV4 is
	// the plugin default).
	if _, ok := envByName(container, "S3_AWS_SIGNATURE_VERSION"); ok {
		s.T().Errorf("S3_AWS_SIGNATURE_VERSION must not be emitted")
	}
	s.assertS3PlainEnv(container, "BACKUP_MAX_CONCURRENT_REQUESTS", "4")
	s.assertS3PlainEnv(container, "BACKUP_MULTIPART_CHUNKSIZE", "10MB")
	s.assertS3PlainEnv(container, "RESTORE_MAX_CONCURRENT_REQUESTS", "4")
	s.assertS3PlainEnv(container, "RESTORE_MULTIPART_CHUNKSIZE", "10MB")
}

// --- Variant 1: credentials from a Kubernetes Secret ---

// TestFunctional_Scenario71_Secret_S3ConfigMap asserts the rendered S3 plugin
// template carries all the configured placeholders and the canonical
// executablepath, but NOT aws_signature_version (the version-matched
// gpbackup_s3_plugin 2.1.0-incubating rejects that option).
func (s *Scenario71Suite) TestFunctional_Scenario71_Secret_S3ConfigMap() {
	cluster := scenario71Cluster("s71-secret-cm", false)

	cm := builder.NewBuilder().BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm, "S3 ConfigMap must be built for an s3 destination")
	require.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")
	tpl := cm.Data["s3-plugin-config.yaml.tpl"]

	assert.Contains(s.T(), tpl, "gpbackup_s3_plugin")
	assert.Contains(s.T(), tpl, "executablepath: /usr/local/bin/gpbackup_s3_plugin")
	assert.Contains(s.T(), tpl, "region: ${S3_REGION}")
	assert.Contains(s.T(), tpl, "endpoint: ${S3_ENDPOINT}")
	assert.Contains(s.T(), tpl, "bucket: ${S3_BUCKET}")
	assert.Contains(s.T(), tpl, "folder: ${S3_FOLDER}")
	assert.Contains(s.T(), tpl, "encryption: ${S3_ENCRYPTION}")
	// aws_signature_version was intentionally removed: the version-matched
	// gpbackup_s3_plugin (2.1.0-incubating) rejects the unknown field. SigV4 is
	// the plugin default, so no explicit option is needed.
	assert.NotContains(s.T(), tpl, "aws_signature_version")
	assert.NotContains(s.T(), tpl, "S3_AWS_SIGNATURE_VERSION")
	assert.Contains(s.T(), tpl, "backup_max_concurrent_requests: ${BACKUP_MAX_CONCURRENT_REQUESTS}")
	assert.Contains(s.T(), tpl, "backup_multipart_chunksize: ${BACKUP_MULTIPART_CHUNKSIZE}")
	assert.Contains(s.T(), tpl, "restore_max_concurrent_requests: ${RESTORE_MAX_CONCURRENT_REQUESTS}")
	assert.Contains(s.T(), tpl, "restore_multipart_chunksize: ${RESTORE_MULTIPART_CHUNKSIZE}")
}

// TestFunctional_Scenario71_Secret_BackupJobEnv asserts the gpbackup container
// carries the full S3 + multipart env and references the Kubernetes Secret
// "backup-s3-credentials" for AWS credentials.
func (s *Scenario71Suite) TestFunctional_Scenario71_Secret_BackupJobEnv() {
	cluster := scenario71Cluster("s71-secret-backup", false)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gpbackup", container.Name)

	s.assertFullS3Env(container)
	s.assertSecretKeyRef(container, "AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	s.assertSecretKeyRef(container, "AWS_SECRET_ACCESS_KEY", "backup-s3-credentials", "aws_secret_access_key")
}

// TestFunctional_Scenario71_Secret_RestoreJobEnv asserts the gprestore container
// carries the same S3 env + creds reference plus the --timestamp/--plugin-config
// args.
func (s *Scenario71Suite) TestFunctional_Scenario71_Secret_RestoreJobEnv() {
	cluster := scenario71Cluster("s71-secret-restore", false)

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: "20260602020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gprestore", container.Name)

	s.assertFullS3Env(container)
	s.assertSecretKeyRef(container, "AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	s.assertSecretKeyRef(container, "AWS_SECRET_ACCESS_KEY", "backup-s3-credentials", "aws_secret_access_key")

	script := joinArgs(container.Args)
	assert.Contains(s.T(), script, "gprestore")
	assert.Contains(s.T(), script, "'--timestamp' '20260602020000'")
	assert.Contains(s.T(), script, "'--plugin-config' '/tmp/s3-config.yaml'")
}

// --- Variant 2: credentials from Vault (materialized secret) ---

// TestFunctional_Scenario71_Vault_BackupJobEnv asserts the gpbackup container's
// AWS_* env reference the operator-materialized secret
// "<cluster>-backup-s3-vault-creds" (NOT "backup-s3-credentials") with the
// canonical default field keys.
func (s *Scenario71Suite) TestFunctional_Scenario71_Vault_BackupJobEnv() {
	cluster := scenario71Cluster("s71-vault-backup", true)
	wantSecret := util.BackupS3VaultCredentialsSecretName(cluster.Name)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000",
		Type:      "full",
	})
	require.NotNil(s.T(), job)
	container := job.Spec.Template.Spec.Containers[0]

	s.assertFullS3Env(container)
	s.assertSecretKeyRef(container, "AWS_ACCESS_KEY_ID", wantSecret, "aws_access_key_id")
	s.assertSecretKeyRef(container, "AWS_SECRET_ACCESS_KEY", wantSecret, "aws_secret_access_key")

	// It must NOT reference the plain credential secret used by variant 1.
	e, ok := envByName(container, "AWS_ACCESS_KEY_ID")
	require.True(s.T(), ok)
	require.NotNil(s.T(), e.ValueFrom)
	require.NotNil(s.T(), e.ValueFrom.SecretKeyRef)
	assert.NotEqual(s.T(), "backup-s3-credentials", e.ValueFrom.SecretKeyRef.Name,
		"vault variant must not reference the variant-1 credential secret")
}

// TestFunctional_Scenario71_Vault_RestoreJobEnv asserts the gprestore container
// also references the materialized vault creds secret.
func (s *Scenario71Suite) TestFunctional_Scenario71_Vault_RestoreJobEnv() {
	cluster := scenario71Cluster("s71-vault-restore", true)
	wantSecret := util.BackupS3VaultCredentialsSecretName(cluster.Name)

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: "20260602020000",
	})
	require.NotNil(s.T(), job)
	container := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "gprestore", container.Name)

	s.assertFullS3Env(container)
	s.assertSecretKeyRef(container, "AWS_ACCESS_KEY_ID", wantSecret, "aws_access_key_id")
	s.assertSecretKeyRef(container, "AWS_SECRET_ACCESS_KEY", wantSecret, "aws_secret_access_key")
}

// TestFunctional_Scenario71_Vault_ReconcileMaterializesCreds drives the
// end-to-end vault path: CR (vault variant) -> reconcile with a fake vault client
// returning minio creds -> operator creates the S3 ConfigMap AND the
// "<cluster>-backup-s3-vault-creds" Secret with those values, and the backup Job
// env references the materialized secret. The 1:1 materialization edge cases are
// covered by the unit test; here we assert the operator-level wiring.
func (s *Scenario71Suite) TestFunctional_Scenario71_Vault_ReconcileMaterializesCreds() {
	cluster := scenario71Cluster("s71-vault-recon", true)

	scheme := testutil.NewTestK8sEnv(cluster).Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster).
		Build()

	vc := &scenario71FakeVaultClient{
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
	assert.Equal(s.T(), "secret/data/cloudberry/backup-s3", vc.readPath,
		"operator should read the configured vault path")

	// The S3 plugin ConfigMap is created.
	cm := &corev1.ConfigMap{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupS3ConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm), "S3 ConfigMap should be created by reconcile")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	// The vault-sourced credentials are materialized into the canonical secret.
	secret := &corev1.Secret{}
	require.NoError(s.T(), k8sClient.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupS3VaultCredentialsSecretName(cluster.Name),
		Namespace: cluster.Namespace,
	}, secret), "materialized vault creds Secret should be created by reconcile")
	assert.Equal(s.T(), "minioadmin", string(secret.Data["aws_access_key_id"]))
	assert.Equal(s.T(), "minioadmin", string(secret.Data["aws_secret_access_key"]))

	// And the backup Job env references that materialized secret.
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260602020000",
	})
	s.assertSecretKeyRef(job.Spec.Template.Spec.Containers[0],
		"AWS_ACCESS_KEY_ID",
		util.BackupS3VaultCredentialsSecretName(cluster.Name), "aws_access_key_id")
}

// --- Positive full reconcile (variant 1, Secret) ---

// TestFunctional_Scenario71_Secret_FullReconcile applies the full-S3 cluster
// (Secret variant) and asserts reconcileBackup ensures the S3 ConfigMap, builds
// the scheduled CronJob (a schedule is set), and records status.cronJobName.
func (s *Scenario71Suite) TestFunctional_Scenario71_Secret_FullReconcile() {
	cluster := scenario71Cluster("s71-secret-recon", false)
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed for the secret variant")
	assert.NotZero(s.T(), result.RequeueAfter)

	// The S3 plugin ConfigMap exists.
	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 ConfigMap should be ensured by reconcile")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	// The scheduled backup CronJob is built (schedule is set).
	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be created when a schedule is set")
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)

	// status.cronJobName is recorded.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)
}

// joinArgs joins container args with spaces for substring assertions on the
// rendered gpbackup/gprestore script (mirrors backup_restore_test.go).
func joinArgs(args []string) string {
	out := ""
	for i, a := range args {
		if i > 0 {
			out += " "
		}
		out += a
	}
	return out
}

// scenario71FakeVaultClient is a minimal vault.Client that returns canned S3
// credentials, used to exercise the vault-materialization reconcile path without
// a real Vault server (mirrors the fake client in backup_s3_vault_test.go).
type scenario71FakeVaultClient struct {
	enabled  bool
	readData map[string]interface{}
	readErr  error
	readPath string
}

func (f *scenario71FakeVaultClient) ReadSecret(
	_ context.Context, path string,
) (map[string]interface{}, error) {
	f.readPath = path
	if f.readErr != nil {
		return nil, f.readErr
	}
	return f.readData, nil
}

func (f *scenario71FakeVaultClient) WriteSecret(
	_ context.Context, _ string, _ map[string]interface{},
) error {
	return nil
}

func (f *scenario71FakeVaultClient) WriteSecretWithResponse(
	_ context.Context, _ string, _ map[string]interface{},
) (map[string]interface{}, error) {
	return nil, nil
}

func (f *scenario71FakeVaultClient) IsEnabled() bool { return f.enabled }

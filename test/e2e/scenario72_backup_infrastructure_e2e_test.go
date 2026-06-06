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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 72: Backup Infrastructure Deployment (E2E)
// ============================================================================
//
// User journey: a user enables backups with the full S3 destination block and an
// explicit jobTemplate (resources, nodeSelector, tolerations, serviceAccountName,
// backoffLimit/activeDeadlineSeconds/ttlSecondsAfterFinished). The operator must
// deploy the backup infrastructure: the toolchain image, the backup
// ServiceAccount (RBAC), the gpbackup_s3_plugin ConfigMap, and Jobs in the
// cluster namespace carrying the operator labels, the inspectable env (incl.
// envsubst rendering) and the jobTemplate overrides.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - The six verifications at the builder/reconcile level (image, SA, ConfigMap,
//     labels/ns, env/envsubst, jobTemplate) — parity with the functional suite.
//   - A live-cluster portion gated on KUBECONFIG that reconciles against a fake
//     client seeded from the Scenario 72 spec and asserts resource shape.
//
// What the live SHELL step verifies (NOT this Go test): image binaries via
// `docker run cloudberry-backup:2.1.0`, the rendered Role/RoleBinding, and the
// live Job inspection. This Go test never requires gpbackup binaries.
// ============================================================================

// envKubeconfig (KUBECONFIG) gates the live-cluster portion and is declared
// package-scoped by the Scenario 69 e2e suite.

const (
	scenario72E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario72E2EBackupSA    = "cloudberry-backup-sa"
)

// Scenario72BackupInfraE2ESuite tests the backup-infrastructure deployment.
type Scenario72BackupInfraE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario72(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario72BackupInfraE2ESuite))
}

func (s *Scenario72BackupInfraE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario72BackupInfraE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario72E2EBackupSpec returns the Scenario 72 BackupSpec (full S3 + explicit
// jobTemplate). When withJobTemplate is false the jobTemplate is omitted.
func scenario72E2EBackupSpec(withJobTemplate bool) *cbv1alpha1.BackupSpec {
	spec := &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    scenario72E2EBackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 5,
			CompressionType:  "gzip",
			Jobs:             4,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "/backups",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
				Multipart: &cbv1alpha1.S3Multipart{
					BackupMaxConcurrentRequests:  4,
					BackupMultipartChunksize:     "10MB",
					RestoreMaxConcurrentRequests: 4,
					RestoreMultipartChunksize:    "10MB",
				},
			},
		},
	}
	if withJobTemplate {
		backoff := int32(2)
		deadline := int64(7200)
		ttl := int32(86400)
		spec.JobTemplate = &cbv1alpha1.BackupJobTemplate{
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "500m", Memory: "512Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "2Gi"},
			},
			NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
			Tolerations: []cbv1alpha1.Toleration{
				{Key: "dedicated", Operator: "Equal", Value: "backup", Effect: "NoSchedule"},
			},
			ServiceAccountName:      scenario72E2EBackupSA,
			BackoffLimit:            &backoff,
			ActiveDeadlineSeconds:   &deadline,
			TTLSecondsAfterFinished: &ttl,
		}
	}
	return spec
}

// scenario72E2ECluster builds a running cluster with the Scenario 72 backup spec.
func scenario72E2ECluster(name string, withJobTemplate bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = scenario72E2EBackupSpec(withJobTemplate)
	return cluster
}

// s72E2EEnvByName returns the value of the named env var on the container.
func s72E2EEnvByName(container corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range container.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// s72E2EAssertPlainEnv asserts a plain-valued env var is present with the value.
func (s *Scenario72BackupInfraE2ESuite) s72E2EAssertPlainEnv(
	container corev1.Container, name, want string,
) {
	e, ok := s72E2EEnvByName(container, name)
	require.True(s.T(), ok, "env %s must be present", name)
	assert.Equal(s.T(), want, e.Value, "env %s value", name)
}

// s72E2EAssertSecretRef asserts the env var is a SecretKeyRef to the secret + key.
func (s *Scenario72BackupInfraE2ESuite) s72E2EAssertSecretRef(
	container corev1.Container, envName, secretName, key string,
) {
	e, ok := s72E2EEnvByName(container, envName)
	require.True(s.T(), ok, "env %s must be present", envName)
	require.NotNil(s.T(), e.ValueFrom, "env %s must use valueFrom", envName)
	require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "env %s must be a SecretKeyRef", envName)
	assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name, "env %s secret name", envName)
	assert.Equal(s.T(), key, e.ValueFrom.SecretKeyRef.Key, "env %s secret key", envName)
}

// s72E2EAssertBackupLabels asserts the operator's actual backup label set.
func (s *Scenario72BackupInfraE2ESuite) s72E2EAssertBackupLabels(
	labels map[string]string, cluster string,
) {
	assert.Equal(s.T(), "cloudberry-operator", labels["app.kubernetes.io/managed-by"])
	assert.Equal(s.T(), cluster, labels["avsoft.io/cluster"])
	assert.Equal(s.T(), "backup", labels["avsoft.io/backup-operation"])
	assert.Equal(s.T(), util.ComponentBackup, labels[util.LabelComponent])
}

// --- 72.1: builder/reconcile parity (infra-free) — all six verifications ---

// TestE2E_Scenario72_BuilderParity verifies the six Scenario 72 verifications at
// the builder/reconcile level: image (V1), serviceAccountName (V2), S3 ConfigMap
// placeholders (V3), Job labels/namespace (V4), env + envsubst (V5) and the
// jobTemplate overrides (V6).
func (s *Scenario72BackupInfraE2ESuite) TestE2E_Scenario72_BuilderParity() {
	cluster := scenario72E2ECluster("test-s72e2e", true)
	b := builder.NewBuilder()

	// V3: S3 plugin ConfigMap.
	cm := b.BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm)
	tpl := cm.Data["s3-plugin-config.yaml.tpl"]
	assert.Contains(s.T(), tpl, "executablepath: /usr/local/bin/gpbackup_s3_plugin")
	assert.Contains(s.T(), tpl, "region: ${S3_REGION}")
	assert.Contains(s.T(), tpl, "endpoint: ${S3_ENDPOINT}")
	assert.Contains(s.T(), tpl, "aws_access_key_id: ${AWS_ACCESS_KEY_ID}")
	assert.Contains(s.T(), tpl, "aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}")
	assert.Contains(s.T(), tpl, "bucket: ${S3_BUCKET}")
	assert.Contains(s.T(), tpl, "folder: ${S3_FOLDER}")
	assert.Contains(s.T(), tpl, "encryption: ${S3_ENCRYPTION}")
	assert.Contains(s.T(), tpl, "backup_max_concurrent_requests: ${BACKUP_MAX_CONCURRENT_REQUESTS}")
	assert.Contains(s.T(), tpl, "backup_multipart_chunksize: ${BACKUP_MULTIPART_CHUNKSIZE}")
	assert.Contains(s.T(), tpl, "restore_max_concurrent_requests: ${RESTORE_MAX_CONCURRENT_REQUESTS}")
	assert.Contains(s.T(), tpl, "restore_multipart_chunksize: ${RESTORE_MULTIPART_CHUNKSIZE}")
	assert.NotContains(s.T(), tpl, "aws_signature_version")

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	container := job.Spec.Template.Spec.Containers[0]

	// V1: image.
	assert.Equal(s.T(), scenario72E2EBackupImage, container.Image)

	// V2: serviceAccountName.
	assert.Equal(s.T(), scenario72E2EBackupSA, job.Spec.Template.Spec.ServiceAccountName)

	// V4: labels + namespace.
	assert.Equal(s.T(), cluster.Namespace, job.Namespace)
	s.s72E2EAssertBackupLabels(job.Labels, cluster.Name)
	s.s72E2EAssertBackupLabels(job.Spec.Template.Labels, cluster.Name)

	// V5: env + envsubst.
	s.s72E2EAssertPlainEnv(container, "CBDB_DATABASE", "mydb")
	s.s72E2EAssertPlainEnv(container, "COMPRESSION_LEVEL", "5")
	s.s72E2EAssertPlainEnv(container, "COMPRESSION_TYPE", "gzip")
	s.s72E2EAssertPlainEnv(container, "BACKUP_JOBS", "4")
	s.s72E2EAssertPlainEnv(container, "PGHOST", util.CoordinatorServiceName(cluster.Name))
	_, ok := s72E2EEnvByName(container, "PGPORT")
	require.True(s.T(), ok, "PGPORT must be present")
	s.s72E2EAssertSecretRef(container, "AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	s.s72E2EAssertSecretRef(container, "AWS_SECRET_ACCESS_KEY", "backup-s3-credentials", "aws_secret_access_key")
	script := strings.Join(container.Args, " ")
	assert.Contains(s.T(), script, "envsubst")
	assert.Contains(s.T(), script, "/tmp/s3-config.yaml")

	// V6: jobTemplate overrides.
	res := container.Resources
	assert.True(s.T(), res.Requests.Cpu().Equal(resource.MustParse("500m")))
	assert.True(s.T(), res.Requests.Memory().Equal(resource.MustParse("512Mi")))
	assert.True(s.T(), res.Limits.Cpu().Equal(resource.MustParse("2")))
	assert.True(s.T(), res.Limits.Memory().Equal(resource.MustParse("2Gi")))
	assert.Equal(s.T(), "linux", job.Spec.Template.Spec.NodeSelector["kubernetes.io/os"])
	require.Len(s.T(), job.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(s.T(), "dedicated", job.Spec.Template.Spec.Tolerations[0].Key)
	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), int32(2), *job.Spec.BackoffLimit)
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), int64(7200), *job.Spec.ActiveDeadlineSeconds)
	require.NotNil(s.T(), job.Spec.TTLSecondsAfterFinished)
	assert.Equal(s.T(), int32(86400), *job.Spec.TTLSecondsAfterFinished)
}

// TestE2E_Scenario72_JobTemplateDefaults asserts the operator defaults
// (backoff 2 / deadline 7200 / ttl 86400) apply with no jobTemplate, and the SA
// resolves to the canonical cloudberry-backup-sa.
func (s *Scenario72BackupInfraE2ESuite) TestE2E_Scenario72_JobTemplateDefaults() {
	cluster := scenario72E2ECluster("test-s72e2e-default", false)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)

	assert.Equal(s.T(), util.BackupServiceAccountName(cluster.Name),
		job.Spec.Template.Spec.ServiceAccountName)
	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), int32(2), *job.Spec.BackoffLimit)
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), int64(7200), *job.Spec.ActiveDeadlineSeconds)
	require.NotNil(s.T(), job.Spec.TTLSecondsAfterFinished)
	assert.Equal(s.T(), int32(86400), *job.Spec.TTLSecondsAfterFinished)
}

// --- 72.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario72_LiveResourceCreation is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries to pass. When live, it reconciles against a fake client
// seeded from the Scenario 72 spec and asserts the operator-produced resource
// shape (S3 ConfigMap, scheduled CronJob, and a backup Job carrying the
// jobTemplate overrides + labels/ns). The actual data cycle is the shell step.
func (s *Scenario72BackupInfraE2ESuite) TestE2E_Scenario72_LiveResourceCreation() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live cluster resource-creation check")
	}

	cluster := scenario72E2ECluster("test-s72e2e-live", true)
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

	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be provisioned")
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job, "operator should be able to build a backup Job")
	assert.Equal(s.T(), cluster.Namespace, job.Namespace)
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation])
	assert.Equal(s.T(), scenario72E2EBackupSA, job.Spec.Template.Spec.ServiceAccountName)
}

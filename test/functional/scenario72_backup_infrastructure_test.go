//go:build functional

package functional

import (
	"context"
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
// Scenario 72: Backup Infrastructure Deployment (functional)
// ============================================================================
//
// Scenario 72 covers the backup INFRASTRUCTURE the operator deploys for a
// cluster with backups enabled — the toolchain image, the backup ServiceAccount
// (RBAC), the gpbackup_s3_plugin ConfigMap, the Job labels/namespace, the Job
// container env (incl. envsubst rendering) and the jobTemplate pod-template
// overrides. These tests black-box the operator through the public builders
// (BuildBackupS3ConfigMap / BuildBackupJob) and the AdminReconciler with fake
// clients (no live infra). They are deterministic and self-contained.
//
// The six verifications map to the test funcs below:
//
//	V1 Image binaries  -> TestFunctional_Scenario72_ImageDefault (image assertion
//	                      only; binary presence is verified live + by `docker run`)
//	V2 RBAC            -> TestFunctional_Scenario72_ServiceAccount (Job references
//	                      cloudberry-backup-sa; the Role/RoleBinding are a helm
//	                      template verified live + by `helm template`)
//	V3 ConfigMap       -> TestFunctional_Scenario72_S3ConfigMapPlaceholders
//	V4 labels/ns       -> TestFunctional_Scenario72_JobLabelsAndNamespace
//	V5 env/envsubst    -> TestFunctional_Scenario72_JobEnvAndEnvsubst
//	V6 jobTemplate     -> TestFunctional_Scenario72_JobTemplateOverrides (+ a
//	                      default-values case)
// ============================================================================

const (
	scenario72BackupImage = "cloudberry-backup:2.1.0"
	scenario72BackupSA    = "cloudberry-backup-sa"
)

// Scenario72Suite exercises the backup-infrastructure deployment.
type Scenario72Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario72(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario72Suite))
}

func (s *Scenario72Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario72Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario72BackupSpec returns the Scenario 72 BackupSpec: the full S3
// destination (Secret credentials) plus the explicit jobTemplate from
// scenario72-backup-infrastructure.yaml. When withJobTemplate is false the
// jobTemplate is omitted so the builder defaults can be asserted.
func scenario72BackupSpec(withJobTemplate bool) *cbv1alpha1.BackupSpec {
	spec := &cbv1alpha1.BackupSpec{
		Enabled:  true,
		Schedule: "0 2 * * *",
		Image:    scenario72BackupImage,
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
		spec.JobTemplate = scenario72JobTemplate()
	}
	return spec
}

// scenario72JobTemplate mirrors the explicit jobTemplate of
// scenario72-backup-infrastructure.yaml.
func scenario72JobTemplate() *cbv1alpha1.BackupJobTemplate {
	backoff := int32(2)
	deadline := int64(7200)
	ttl := int32(86400)
	return &cbv1alpha1.BackupJobTemplate{
		Resources: &cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "500m", Memory: "512Mi"},
			Limits:   &cbv1alpha1.ResourceList{CPU: "2", Memory: "2Gi"},
		},
		NodeSelector: map[string]string{"kubernetes.io/os": "linux"},
		Tolerations: []cbv1alpha1.Toleration{
			{Key: "dedicated", Operator: "Equal", Value: "backup", Effect: "NoSchedule"},
		},
		ServiceAccountName:      scenario72BackupSA,
		BackoffLimit:            &backoff,
		ActiveDeadlineSeconds:   &deadline,
		TTLSecondsAfterFinished: &ttl,
	}
}

// scenario72Cluster builds a running cluster with the Scenario 72 backup spec.
func scenario72Cluster(name string, withJobTemplate bool) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = scenario72BackupSpec(withJobTemplate)
	return cluster
}

// s72EnvByName returns the value of the named env var on the container.
func s72EnvByName(container corev1.Container, name string) (corev1.EnvVar, bool) {
	for _, e := range container.Env {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

// s72AssertPlainEnv asserts a plain-valued env var is present with the value.
func (s *Scenario72Suite) s72AssertPlainEnv(container corev1.Container, name, want string) {
	e, ok := s72EnvByName(container, name)
	require.True(s.T(), ok, "env %s must be present", name)
	assert.Equal(s.T(), want, e.Value, "env %s value", name)
}

// s72AssertSecretRef asserts the env var is a SecretKeyRef to the secret + key.
func (s *Scenario72Suite) s72AssertSecretRef(
	container corev1.Container, envName, secretName, key string,
) {
	e, ok := s72EnvByName(container, envName)
	require.True(s.T(), ok, "env %s must be present", envName)
	require.NotNil(s.T(), e.ValueFrom, "env %s must use valueFrom", envName)
	require.NotNil(s.T(), e.ValueFrom.SecretKeyRef, "env %s must be a SecretKeyRef", envName)
	assert.Equal(s.T(), secretName, e.ValueFrom.SecretKeyRef.Name, "env %s secret name", envName)
	assert.Equal(s.T(), key, e.ValueFrom.SecretKeyRef.Key, "env %s secret key", envName)
}

// --- V1: image binaries (image assertion; binaries verified live) ---

// TestFunctional_Scenario72_ImageDefault asserts the backup Job container uses
// the configured cloudberry-backup:2.1.0 toolchain image. The presence of the
// gpbackup/gprestore/gpbackup_s3_plugin binaries inside the image is verified
// live (`docker run cloudberry-backup:2.1.0`) and is NOT testable in Go.
func (s *Scenario72Suite) TestFunctional_Scenario72_ImageDefault() {
	cluster := scenario72Cluster("s72-image", true)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	assert.Equal(s.T(), scenario72BackupImage, job.Spec.Template.Spec.Containers[0].Image)
}

// --- V2: RBAC (Job references the backup ServiceAccount) ---

// TestFunctional_Scenario72_ServiceAccount asserts the backup Job pod references
// the cloudberry-backup-sa ServiceAccount when the jobTemplate sets it. The Role
// (`cloudberry-backup-role`: get secrets/configmaps, create/patch events) and
// RoleBinding are rendered from the helm template
// (deploy/helm/.../templates/backup-rbac.yaml) and are verified live and by
// `helm template`, not by this Go unit test.
func (s *Scenario72Suite) TestFunctional_Scenario72_ServiceAccount() {
	cluster := scenario72Cluster("s72-sa", true)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), scenario72BackupSA, job.Spec.Template.Spec.ServiceAccountName)

	// The default (no jobTemplate override) still resolves to the canonical SA.
	defCluster := scenario72Cluster("s72-sa-default", false)
	defJob := builder.NewBuilder().BuildBackupJob(defCluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000",
	})
	assert.Equal(s.T(), util.BackupServiceAccountName(defCluster.Name),
		defJob.Spec.Template.Spec.ServiceAccountName)
}

// --- V3: S3 plugin ConfigMap placeholders ---

// TestFunctional_Scenario72_S3ConfigMapPlaceholders asserts the rendered S3
// plugin template carries the canonical executablepath and every option
// placeholder (region/endpoint/creds/bucket/folder/encryption + 4 multipart
// vars), and that aws_signature_version is NOT present.
func (s *Scenario72Suite) TestFunctional_Scenario72_S3ConfigMapPlaceholders() {
	cluster := scenario72Cluster("s72-cm", true)

	cm := builder.NewBuilder().BuildBackupS3ConfigMap(cluster)
	require.NotNil(s.T(), cm, "S3 ConfigMap must be built for an s3 destination")
	require.Equal(s.T(), util.BackupS3ConfigMapName(cluster.Name), cm.Name)
	require.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")
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
	// aws_signature_version is intentionally absent (version-matched plugin
	// rejects the unknown field; SigV4 is the default).
	assert.NotContains(s.T(), tpl, "aws_signature_version")
}

// --- V4: Job labels + namespace ---

// TestFunctional_Scenario72_JobLabelsAndNamespace asserts the backup Job lives in
// the cluster namespace and carries the operator's labels:
// app.kubernetes.io/managed-by=cloudberry-operator, avsoft.io/cluster=<cluster>,
// avsoft.io/backup-operation=backup (+ avsoft.io/component=backup).
func (s *Scenario72Suite) TestFunctional_Scenario72_JobLabelsAndNamespace() {
	cluster := scenario72Cluster("s72-labels", true)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)

	assert.Equal(s.T(), cluster.Namespace, job.Namespace, "Job.ns must equal cluster.ns")

	s.assertBackupLabels(job.Labels, cluster.Name)
	// The pod template carries the same labels.
	s.assertBackupLabels(job.Spec.Template.Labels, cluster.Name)
}

// assertBackupLabels asserts the operator's actual backup label set.
func (s *Scenario72Suite) assertBackupLabels(labels map[string]string, cluster string) {
	assert.Equal(s.T(), util.LabelManagedByValue, labels[util.LabelManagedBy])
	assert.Equal(s.T(), cluster, labels[util.LabelCluster])
	assert.Equal(s.T(), util.BackupOperationBackup, labels[util.LabelBackupOperation])
	assert.Equal(s.T(), util.ComponentBackup, labels[util.LabelComponent])
	// Cross-check the literal keys/values the verification requires.
	assert.Equal(s.T(), "cloudberry-operator", labels["app.kubernetes.io/managed-by"])
	assert.Equal(s.T(), cluster, labels["avsoft.io/cluster"])
	assert.Equal(s.T(), "backup", labels["avsoft.io/backup-operation"])
}

// --- V5: Job env + envsubst ---

// TestFunctional_Scenario72_JobEnvAndEnvsubst asserts the backup Job container
// carries CBDB_DATABASE, PGHOST, PGPORT, COMPRESSION_LEVEL, COMPRESSION_TYPE and
// BACKUP_JOBS; that AWS_* are SecretKeyRefs to backup-s3-credentials; and that
// the container args run envsubst to render /tmp/s3-config.yaml.
func (s *Scenario72Suite) TestFunctional_Scenario72_JobEnvAndEnvsubst() {
	cluster := scenario72Cluster("s72-env", true)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	container := job.Spec.Template.Spec.Containers[0]
	require.Equal(s.T(), "gpbackup", container.Name)

	// Informational/inspectable gpbackup env (spec 11). Values come from the
	// cluster's gpbackupOptions (level 5, gzip, jobs 4) and the request database.
	s.s72AssertPlainEnv(container, "CBDB_DATABASE", "mydb")
	s.s72AssertPlainEnv(container, "COMPRESSION_LEVEL", "5")
	s.s72AssertPlainEnv(container, "COMPRESSION_TYPE", "gzip")
	s.s72AssertPlainEnv(container, "BACKUP_JOBS", "4")

	// PGHOST/PGPORT remain present.
	s.s72AssertPlainEnv(container, "PGHOST", util.CoordinatorServiceName(cluster.Name))
	_, ok := s72EnvByName(container, "PGPORT")
	require.True(s.T(), ok, "PGPORT must be present")

	// AWS credentials are SecretKeyRefs (never plaintext).
	s.s72AssertSecretRef(container, "AWS_ACCESS_KEY_ID", "backup-s3-credentials", "aws_access_key_id")
	s.s72AssertSecretRef(container, "AWS_SECRET_ACCESS_KEY", "backup-s3-credentials", "aws_secret_access_key")

	// The container args run envsubst and produce /tmp/s3-config.yaml; the
	// existing gpbackup CLI args remain unchanged (Scenario 71 parity).
	script := strings.Join(container.Args, " ")
	assert.Contains(s.T(), script, "envsubst")
	assert.Contains(s.T(), script, "/tmp/s3-config.yaml")
	assert.Contains(s.T(), script, "'--plugin-config' '/tmp/s3-config.yaml'")
	assert.Contains(s.T(), script, "'--dbname' 'mydb'")
	assert.Contains(s.T(), script, "'--compression-level' '5'")
	assert.Contains(s.T(), script, "'--compression-type' 'gzip'")
	assert.Contains(s.T(), script, "'--jobs' '4'")
}

// TestFunctional_Scenario72_JobEnvDefaults asserts the env defaults (1/gzip/1)
// apply when no gpbackupOptions are configured, and CBDB_DATABASE is empty when
// no database is requested.
func (s *Scenario72Suite) TestFunctional_Scenario72_JobEnvDefaults() {
	cluster := scenario72Cluster("s72-env-default", false)
	cluster.Spec.Backup.Gpbackup = nil

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)
	container := job.Spec.Template.Spec.Containers[0]

	s.s72AssertPlainEnv(container, "CBDB_DATABASE", "")
	s.s72AssertPlainEnv(container, "COMPRESSION_LEVEL", "1")
	s.s72AssertPlainEnv(container, "COMPRESSION_TYPE", "gzip")
	s.s72AssertPlainEnv(container, "BACKUP_JOBS", "1")
}

// TestFunctional_Scenario72_CronJobEnv asserts the scheduled CronJob's backup
// container carries the same informational env, with CBDB_DATABASE empty (the
// CronJob's databases are resolved at runtime).
func (s *Scenario72Suite) TestFunctional_Scenario72_CronJobEnv() {
	cluster := scenario72Cluster("s72-cron-env", true)

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron, "CronJob must be built when a schedule is set")
	container := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]

	s.s72AssertPlainEnv(container, "CBDB_DATABASE", "")
	s.s72AssertPlainEnv(container, "COMPRESSION_LEVEL", "5")
	s.s72AssertPlainEnv(container, "COMPRESSION_TYPE", "gzip")
	s.s72AssertPlainEnv(container, "BACKUP_JOBS", "4")
}

// --- V6: jobTemplate overrides ---

// TestFunctional_Scenario72_JobTemplateOverrides asserts every jobTemplate
// override propagates to the built Job: container resources (req 500m/512Mi,
// lim 2/2Gi), nodeSelector, tolerations, serviceAccountName, and the JobSpec
// backoffLimit=2/activeDeadlineSeconds=7200/ttlSecondsAfterFinished=86400.
func (s *Scenario72Suite) TestFunctional_Scenario72_JobTemplateOverrides() {
	cluster := scenario72Cluster("s72-jobtmpl", true)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)

	// Container resources.
	res := job.Spec.Template.Spec.Containers[0].Resources
	assert.True(s.T(), res.Requests.Cpu().Equal(resource.MustParse("500m")),
		"requests.cpu should be 500m, got %s", res.Requests.Cpu().String())
	assert.True(s.T(), res.Requests.Memory().Equal(resource.MustParse("512Mi")),
		"requests.memory should be 512Mi, got %s", res.Requests.Memory().String())
	assert.True(s.T(), res.Limits.Cpu().Equal(resource.MustParse("2")),
		"limits.cpu should be 2, got %s", res.Limits.Cpu().String())
	assert.True(s.T(), res.Limits.Memory().Equal(resource.MustParse("2Gi")),
		"limits.memory should be 2Gi, got %s", res.Limits.Memory().String())

	// Pod-level overrides.
	podSpec := job.Spec.Template.Spec
	assert.Equal(s.T(), "linux", podSpec.NodeSelector["kubernetes.io/os"])
	require.Len(s.T(), podSpec.Tolerations, 1)
	tol := podSpec.Tolerations[0]
	assert.Equal(s.T(), "dedicated", tol.Key)
	assert.Equal(s.T(), corev1.TolerationOpEqual, tol.Operator)
	assert.Equal(s.T(), "backup", tol.Value)
	assert.Equal(s.T(), corev1.TaintEffectNoSchedule, tol.Effect)
	assert.Equal(s.T(), scenario72BackupSA, podSpec.ServiceAccountName)

	// JobSpec-level overrides.
	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), int32(2), *job.Spec.BackoffLimit)
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), int64(7200), *job.Spec.ActiveDeadlineSeconds)
	require.NotNil(s.T(), job.Spec.TTLSecondsAfterFinished)
	assert.Equal(s.T(), int32(86400), *job.Spec.TTLSecondsAfterFinished)
}

// TestFunctional_Scenario72_JobTemplateDefaults asserts that, with NO
// jobTemplate, the JobSpec still carries the operator defaults
// backoffLimit=2/activeDeadlineSeconds=7200/ttlSecondsAfterFinished=86400.
func (s *Scenario72Suite) TestFunctional_Scenario72_JobTemplateDefaults() {
	cluster := scenario72Cluster("s72-jobtmpl-default", false)

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260606020000", Type: "full",
	})
	require.NotNil(s.T(), job)

	require.NotNil(s.T(), job.Spec.BackoffLimit)
	assert.Equal(s.T(), int32(2), *job.Spec.BackoffLimit)
	require.NotNil(s.T(), job.Spec.ActiveDeadlineSeconds)
	assert.Equal(s.T(), int64(7200), *job.Spec.ActiveDeadlineSeconds)
	require.NotNil(s.T(), job.Spec.TTLSecondsAfterFinished)
	assert.Equal(s.T(), int32(86400), *job.Spec.TTLSecondsAfterFinished)
}

// --- Full reconcile (parity with Scenario 71) ---

// TestFunctional_Scenario72_FullReconcile applies the Scenario 72 cluster and
// asserts reconcileBackup ensures the S3 ConfigMap, builds the scheduled CronJob,
// and records status.cronJobName.
func (s *Scenario72Suite) TestFunctional_Scenario72_FullReconcile() {
	cluster := scenario72Cluster("s72-recon", true)
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	result, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")
	assert.NotZero(s.T(), result.RequeueAfter)

	cm, err := env.GetConfigMap(s.ctx, util.BackupS3ConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "S3 ConfigMap should be ensured by reconcile")
	assert.Contains(s.T(), cm.Data, "s3-plugin-config.yaml.tpl")

	cron := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupCronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron), "backup CronJob should be created when a schedule is set")
	assert.Equal(s.T(), "0 2 * * *", cron.Spec.Schedule)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.BackupCronJobName(cluster.Name), updated.Status.CronJobName)
}

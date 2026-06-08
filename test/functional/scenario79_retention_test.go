//go:build functional

package functional

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// Scenario 79: Retention Cleanup, All Policies (functional)
// ============================================================================
//
// Scenario 79 covers retention cleanup at the builder/controller layer
// (deterministic, no live cluster). The operator enforces three retention
// policies through a single gpbackman-driven cleanup Job and feeds the deletion
// count into the cloudberry_backup_retention_deleted_total metric:
//
//	79a fullCount=3        : the rendered cleanup script enumerates the Success
//	    full backups (gpbackman backup-info --type full) and deletes the oldest
//	    excess beyond FullCount via backup-delete --timestamp <ts> --cascade.
//	79b incrementalCount=10: same enforcement for --type incremental backups,
//	    re-enumerating after each delete so a cascaded full delete never
//	    over/under-counts.
//	79c maxAge="30d"       : time-based retention runs
//	    gpbackman backup-clean --older-than-days 30 --cascade.
//	79d cleanup placement   : the operator creates EXACTLY ONE cleanup Job per
//	    latest Succeeded backup (idempotent), patches the
//	    avsoft.io/backup-retention-deleted annotation from the cleanup pod's
//	    RETENTION_DELETED terminated message, and the metrics loop increments
//	    cloudberry_backup_retention_deleted_total by that count.
//
// These tests black-box the operator through the public builder
// (BuildRetentionCleanupJob / buildGpbackmanRetentionScript rendered args) and
// the AdminReconciler with a fake client. They are deterministic and
// self-contained (no live infra). The live count-based deletions are exercised
// by the e2e live script via coordinator-exec, since the standalone cleanup Job
// pod does not carry the coordinator's gpbackup_history.db.
// ============================================================================

const (
	scenario79BackupImage = "cloudberry-backup:2.1.0"
	// scenario79LatestTS is the pinned latest-backup timestamp used to key the
	// deterministic cleanup Job name.
	scenario79LatestTS = "20260608060000"
)

// scenario79CountingMetrics embeds NoopRecorder and counts only the retention
// deletion records so the functional suite can assert the metric delta without a
// live Prometheus registry (mirrors the controller's countingBackupMetrics).
type scenario79CountingMetrics struct {
	metrics.NoopRecorder
	retentionDeleted int
}

func (m *scenario79CountingMetrics) RecordBackupRetentionDeleted(_, _ string, n int) {
	m.retentionDeleted += n
}

// Scenario79Suite exercises the retention cleanup script rendering, the cleanup
// Job spec, the idempotent cleanup-Job creation, the annotation patch and the
// metric increment.
type Scenario79Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario79(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario79Suite))
}

func (s *Scenario79Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario79Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario79RetentionSpec returns an S3 (MinIO) destination BackupSpec with the
// given retention policy, mirroring the scenario79-s3 sample CR.
func scenario79RetentionSpec(retention cbv1alpha1.BackupRetention) *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:   true,
		Image:     scenario79BackupImage,
		Retention: retention,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
			Incremental:      true,
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
}

// scenario79AllPoliciesRetention returns the full retention policy used by the
// scenario79-s3 sample CR (fullCount=3, incrementalCount=10, maxAge=30d).
func scenario79AllPoliciesRetention() cbv1alpha1.BackupRetention {
	return cbv1alpha1.BackupRetention{
		FullCount:        3,
		IncrementalCount: 10,
		MaxAge:           "30d",
	}
}

// scenario79Cluster builds a Running cluster (pending generation) with the given
// backup spec.
func scenario79Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	// WithPendingGeneration drives the full spec-driven reconcile path
	// (reconcileBackup -> ensureRetentionCleanup); a steady-state generation
	// short-circuits to refreshBackupStatusOnSteadyState which skips cleanup.
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario79CleanupScript renders the gpbackman retention cleanup script for a
// cleanup Job built from the given retention policy.
func (s *Scenario79Suite) scenario79CleanupScript(
	retention cbv1alpha1.BackupRetention,
) (*batchv1.Job, string) {
	cluster := scenario79Cluster("s79-script", scenario79RetentionSpec(retention))
	job := builder.NewBuilder().BuildRetentionCleanupJob(cluster, scenario79LatestTS)
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.Len(s.T(), container.Args, 1)
	return job, container.Args[0]
}

// scenario79SucceededBackupJob builds a Succeeded backup-operation Job fixture
// named like an on-demand backup so the operator parses the 14-digit timestamp.
func scenario79SucceededBackupJob(cluster, ts string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.BackupJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
				util.LabelBackupType:      util.BackupTypeFull,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
}

// scenario79SucceededCleanupJob builds a Succeeded cleanup-operation Job fixture
// keyed off the latest backup timestamp.
func scenario79SucceededCleanupJob(cluster, ts string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-1 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.RetentionCleanupJobName(cluster, ts),
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationCleanup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded:      1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
}

// scenario79CleanupPod builds a terminated cleanup pod carrying the
// RETENTION_DELETED marker so the operator can recover the deletion count.
func scenario79CleanupPod(cluster, ts string, deleted int) *corev1.Pod {
	cleanupName := util.RetentionCleanupJobName(cluster, ts)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cleanupName + "-abcde",
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				"job-name": cleanupName,
			},
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "gpbackman",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Message: retentionDeletedMessage(deleted),
					},
				},
			}},
		},
	}
}

// retentionDeletedMessage renders a RETENTION_DELETED marker line.
func retentionDeletedMessage(n int) string {
	return "RETENTION_DELETED=" + strconv.Itoa(n) + "\n"
}

// countCleanupJobs counts the cleanup-operation Jobs in a Job list.
func countCleanupJobs(jobs []batchv1.Job) int {
	n := 0
	for i := range jobs {
		if jobs[i].Labels[util.LabelBackupOperation] == util.BackupOperationCleanup {
			n++
		}
	}
	return n
}

// --- 79a: fullCount retention renders backup-delete for the oldest excess ---

// TestFunctional_Scenario79_FullCountScript asserts the rendered cleanup script
// for fullCount=3 enumerates full backups (backup-info --type full), deletes the
// oldest excess with backup-delete --timestamp ... --cascade, keeps the newest 3,
// and carries NO incremental enumeration or backup-clean step.
func (s *Scenario79Suite) TestFunctional_Scenario79_FullCountScript() {
	_, script := s.scenario79CleanupScript(cbv1alpha1.BackupRetention{FullCount: 3})

	assert.Contains(s.T(), script, "backup-info --type \"$1\"",
		"the cleanup script must enumerate via gpbackman backup-info")
	assert.Contains(s.T(), script, "_gpbackman_timestamps 'full'",
		"fullCount retention must enumerate the full backups")
	assert.Contains(s.T(), script, "KEEP=3",
		"fullCount=3 must render KEEP=3")
	assert.Contains(s.T(), script, "backup-delete --timestamp \"$1\" --cascade",
		"excess fulls must be removed via backup-delete --cascade")
	assert.Contains(s.T(), script, "RETENTION_DELETED=",
		"the script must emit the RETENTION_DELETED marker")

	// fullCount-only: no incremental enumeration, no time-based backup-clean.
	assert.NotContains(s.T(), script, "_gpbackman_timestamps 'incremental'",
		"fullCount-only retention must NOT enumerate incrementals")
	assert.NotContains(s.T(), script, "backup-clean --older-than-days",
		"fullCount-only retention must NOT run a time-based backup-clean")
	// No legacy/invalid gpbackman tokens.
	assert.NotContains(s.T(), script, "gpbackman delete")
	assert.NotContains(s.T(), script, "--keep-full")
}

// --- 79b: incrementalCount retention renders the incremental enforcement loop ---

// TestFunctional_Scenario79_IncrementalCountScript asserts the rendered cleanup
// script for incrementalCount=10 enumerates incremental backups and deletes the
// oldest excess via backup-delete --cascade, with KEEP=10.
func (s *Scenario79Suite) TestFunctional_Scenario79_IncrementalCountScript() {
	_, script := s.scenario79CleanupScript(cbv1alpha1.BackupRetention{IncrementalCount: 10})

	assert.Contains(s.T(), script, "_gpbackman_timestamps 'incremental'",
		"incrementalCount retention must enumerate the incremental backups")
	assert.Contains(s.T(), script, "KEEP=10",
		"incrementalCount=10 must render KEEP=10")
	assert.Contains(s.T(), script, "backup-delete --timestamp \"$1\" --cascade",
		"excess incrementals must be removed via backup-delete --cascade")
	// The loop re-enumerates Success timestamps before each delete (cascade safe).
	assert.Contains(s.T(), script, "while :; do",
		"incrementalCount retention must use a re-enumerating loop")
	assert.Contains(s.T(), script, "tail -n 1",
		"the oldest (last, newest-first) Success timestamp must be selected")

	assert.NotContains(s.T(), script, "_gpbackman_timestamps 'full'",
		"incrementalCount-only retention must NOT enumerate fulls")
	assert.NotContains(s.T(), script, "backup-clean --older-than-days",
		"incrementalCount-only retention must NOT run a time-based backup-clean")
}

// --- 79c: maxAge retention renders backup-clean --older-than-days ---

// TestFunctional_Scenario79_MaxAgeScript asserts the rendered cleanup script for
// maxAge=30d runs gpbackman backup-clean --older-than-days 30 --cascade and emits
// no count-based enforcement.
func (s *Scenario79Suite) TestFunctional_Scenario79_MaxAgeScript() {
	_, script := s.scenario79CleanupScript(cbv1alpha1.BackupRetention{MaxAge: "30d"})

	assert.Contains(s.T(), script, "backup-clean --older-than-days 30",
		"maxAge=30d must render backup-clean --older-than-days 30")
	assert.Contains(s.T(), script, "--cascade",
		"the time-based backup-clean must cascade dependent incrementals")
	assert.Contains(s.T(), script, "RETENTION_DELETED=")

	// maxAge-only: no count-based KEEP loops.
	assert.NotContains(s.T(), script, "KEEP=",
		"maxAge-only retention must NOT render a count-based KEEP loop")
	// No legacy/invalid gpbackman tokens.
	assert.NotContains(s.T(), script, "--older-than ")
	assert.NotContains(s.T(), script, "gpbackman delete")
}

// TestFunctional_Scenario79_AllPoliciesScript asserts that when all three
// policies are set the rendered script renders count enforcement for BOTH types
// and the time-based step, each exactly once.
func (s *Scenario79Suite) TestFunctional_Scenario79_AllPoliciesScript() {
	_, script := s.scenario79CleanupScript(scenario79AllPoliciesRetention())

	assert.Equal(s.T(), 1, strings.Count(script, "_gpbackman_timestamps 'full'"),
		"all-policy retention enumerates fulls once")
	assert.Equal(s.T(), 1, strings.Count(script, "_gpbackman_timestamps 'incremental'"),
		"all-policy retention enumerates incrementals once")
	assert.Equal(s.T(), 1, strings.Count(script, "backup-clean --older-than-days 30"),
		"all-policy retention runs backup-clean once")
	assert.Contains(s.T(), script, "KEEP=3")
	assert.Contains(s.T(), script, "KEEP=10")
}

// --- 79a/b/c: cleanup Job spec (operation label, sh -c, termination policy) ---

// TestFunctional_Scenario79_CleanupJobSpec asserts the cleanup Job carries the
// operation=cleanup label, the deterministic name, an sh -c invocation of the
// retention script, and TerminationMessagePolicy=FallbackToLogsOnError (so the
// deletion count is recoverable from the pod log).
func (s *Scenario79Suite) TestFunctional_Scenario79_CleanupJobSpec() {
	job, _ := s.scenario79CleanupScript(scenario79AllPoliciesRetention())

	assert.Equal(s.T(),
		util.RetentionCleanupJobName("s79-script", scenario79LatestTS), job.Name,
		"the cleanup Job name must be deterministic (cluster-cleanup-ts)")
	assert.Equal(s.T(), util.BackupOperationCleanup,
		job.Labels[util.LabelBackupOperation],
		"the cleanup Job must carry operation=cleanup")

	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers)
	container := job.Spec.Template.Spec.Containers[0]
	require.GreaterOrEqual(s.T(), len(container.Command), 2,
		"the cleanup container must run a shell command")
	assert.Contains(s.T(), container.Command, "-c",
		"the cleanup container must invoke the script via sh -c")
	assert.Equal(s.T(), corev1.TerminationMessageFallbackToLogsOnError,
		container.TerminationMessagePolicy,
		"the cleanup container must fall back to logs for the deletion count")
}

// --- 79d: ensureRetentionCleanup creates exactly one cleanup Job (idempotent) ---

// TestFunctional_Scenario79_EnsureCleanupCreatesOnce reconciles a cluster with a
// Succeeded backup and a retention policy, then asserts exactly one cleanup Job
// (named <cluster>-cleanup-<latest-ts>) is created and a second reconcile does
// NOT create a duplicate.
func (s *Scenario79Suite) TestFunctional_Scenario79_EnsureCleanupCreatesOnce() {
	cluster := scenario79Cluster("s79-ensure", scenario79RetentionSpec(scenario79AllPoliciesRetention()))
	backup := scenario79SucceededBackupJob(cluster.Name, scenario79LatestTS)

	env := testutil.NewTestK8sEnv(cluster, backup)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "first reconcile should succeed")

	cleanupName := util.RetentionCleanupJobName(cluster.Name, scenario79LatestTS)
	created, err := env.GetJob(s.ctx, cleanupName, cluster.Namespace)
	require.NoError(s.T(), err, "a cleanup Job keyed off the latest backup ts must exist")
	assert.Equal(s.T(), util.BackupOperationCleanup,
		created.Labels[util.LabelBackupOperation])

	// A second reconcile must NOT create a duplicate cleanup Job.
	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "second reconcile should succeed")

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	assert.Equal(s.T(), 1, countCleanupJobs(jobs.Items),
		"reconcile must be idempotent: exactly one cleanup Job for the latest backup")
}

// TestFunctional_Scenario79_NoCleanupWithoutPolicy asserts that with no retention
// policy set, no cleanup Job is created even after a Succeeded backup.
func (s *Scenario79Suite) TestFunctional_Scenario79_NoCleanupWithoutPolicy() {
	cluster := scenario79Cluster("s79-nopolicy",
		scenario79RetentionSpec(cbv1alpha1.BackupRetention{}))
	backup := scenario79SucceededBackupJob(cluster.Name, scenario79LatestTS)

	env := testutil.NewTestK8sEnv(cluster, backup)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	assert.Equal(s.T(), 0, countCleanupJobs(jobs.Items),
		"no retention policy => no cleanup Job")
}

// --- 79d: annotation patch from the RETENTION_DELETED pod message ---

// TestFunctional_Scenario79_AnnotatesFromPodMessage seeds a Succeeded cleanup Job
// + a terminated pod carrying RETENTION_DELETED=2 and asserts the operator
// patches avsoft.io/backup-retention-deleted=2 onto the cleanup Job.
func (s *Scenario79Suite) TestFunctional_Scenario79_AnnotatesFromPodMessage() {
	cluster := scenario79Cluster("s79-annotate",
		scenario79RetentionSpec(scenario79AllPoliciesRetention()))
	backup := scenario79SucceededBackupJob(cluster.Name, scenario79LatestTS)
	cleanup := scenario79SucceededCleanupJob(cluster.Name, scenario79LatestTS)
	pod := scenario79CleanupPod(cluster.Name, scenario79LatestTS, 2)

	// The cleanup pod requires the corev1 scheme; testutil registers it.
	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, backup, cleanup, pod).
		Build()
	recorder := record.NewFakeRecorder(50)
	m := &scenario79CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, recorder,
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cleanupName := util.RetentionCleanupJobName(cluster.Name, scenario79LatestTS)
	got := &batchv1.Job{}
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: cleanupName, Namespace: cluster.Namespace}, got))
	assert.Equal(s.T(), "2", got.Annotations[util.AnnotationBackupRetentionDeleted],
		"the operator must patch the deletion count from the RETENTION_DELETED pod message")
}

// TestFunctional_Scenario79_SucceededCleanupDrivesMetric seeds a Succeeded
// cleanup Job already carrying avsoft.io/backup-retention-deleted=3 and asserts a
// reconcile records RecordBackupRetentionDeleted(...,3) via the counting recorder
// (the annotation->metric plumbing).
func (s *Scenario79Suite) TestFunctional_Scenario79_SucceededCleanupDrivesMetric() {
	cluster := scenario79Cluster("s79-metric",
		scenario79RetentionSpec(scenario79AllPoliciesRetention()))
	backup := scenario79SucceededBackupJob(cluster.Name, scenario79LatestTS)
	cleanup := scenario79SucceededCleanupJob(cluster.Name, scenario79LatestTS)
	cleanup.Annotations = map[string]string{
		util.AnnotationBackupRetentionDeleted: "3",
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cbv1alpha1.CloudberryCluster{}).
		WithObjects(cluster, backup, cleanup).
		Build()
	m := &scenario79CountingMetrics{}
	reconciler := controller.NewAdminReconciler(
		k8sClient, scheme, record.NewFakeRecorder(50),
		builder.NewBuilder(), nil, m, nil,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	assert.Equal(s.T(), 3, m.retentionDeleted,
		"a Succeeded annotated cleanup Job must drive RecordBackupRetentionDeleted by its count")
}

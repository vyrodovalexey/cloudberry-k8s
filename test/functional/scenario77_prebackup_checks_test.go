//go:build functional

package functional

import (
	"context"
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

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 77: Pre-Backup Health Checks (functional)
// ============================================================================
//
// Scenario 77 hardens the backup Job's `pre-backup-check` init container so it
// validates cluster + destination health BEFORE the backup proceeds. The init
// container exits non-zero (init-container semantics => the main gpbackup
// container never starts) when any of four sub-checks fails:
//
//	77a Segments-up   : a segment is down (gp_segment_configuration status='d').
//	77b Long-running  : a transaction older than the threshold is open.
//	77c S3 reachability: a SigV4 HEAD against the bucket returns non-2xx/3xx.
//	77d Local disk    : the backup PVC free space is below minBackupDiskFreeKB.
//
// When a backup Job is observed Failed the operator records
// status.lastBackupStatus=Failed AND emits a Warning Kubernetes Event with
// reason BackupFailed (de-duplicated per Job).
//
// These tests black-box the operator through the public builder
// (BuildBackupJob / BuildBackupCronJob) and the AdminReconciler with a fake
// client. They are deterministic and self-contained (no live infra). Matrix:
//
//	init container present + ordered   -> TestFunctional_Scenario77_InitContainerPresent
//	77a segments-up check wiring        -> TestFunctional_Scenario77_SegmentsUpCheck
//	77b long-running-txn check wiring   -> TestFunctional_Scenario77_LongRunningTxnCheck
//	77c S3 reachability check wiring    -> TestFunctional_Scenario77_S3ReachabilityCheck
//	77d local disk-space check wiring   -> TestFunctional_Scenario77_LocalDiskSpaceCheck
//	Failed -> lastBackupStatus + event  -> TestFunctional_Scenario77_FailedBackupEmitsWarning
//	dedup of the failure warning        -> TestFunctional_Scenario77_FailedBackupWarningDeDup
//	success emits no warning            -> TestFunctional_Scenario77_SucceededBackupNoWarning
// ============================================================================

const (
	scenario77BackupImage = "cloudberry-backup:2.1.0"
	scenario77LocalPath   = "/backups"
)

// Scenario77Suite exercises the pre-backup-check init container wiring + the
// backup-failure Warning Event/status flow.
type Scenario77Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario77(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario77Suite))
}

func (s *Scenario77Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario77Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario77S3BackupSpec returns an S3 (MinIO) destination BackupSpec mirroring
// the Scenario 76 backup block (used for 77a/77b/77c).
func scenario77S3BackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario77BackupImage,
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

// scenario77LocalBackupSpec returns a local (PVC-backed) destination BackupSpec
// used for the 77d disk-space check.
func scenario77LocalBackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario77BackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			Local: &cbv1alpha1.LocalDestination{
				Path:                  scenario77LocalPath,
				PersistentVolumeClaim: "scenario77-backup-pvc",
			},
		},
	}
}

// scenario77Cluster builds a Running cluster (pending generation) with the given
// backup spec.
func scenario77Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// scenario77PreBackupScript returns the rendered pre-backup-check init container
// script for an on-demand backup Job built from the given backup spec.
func (s *Scenario77Suite) scenario77PreBackupScript(backup *cbv1alpha1.BackupSpec) string {
	cluster := scenario77Cluster("s77-script", backup)
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260608070000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	podSpec := job.Spec.Template.Spec
	require.NotEmpty(s.T(), podSpec.InitContainers,
		"pre-backup-check init container must be present")
	init := podSpec.InitContainers[0]
	require.Equal(s.T(), "pre-backup-check", init.Name)
	return strings.Join(init.Args, "\n")
}

// scenario77FailedBackupJob builds a Failed backup-operation Job fixture.
func scenario77FailedBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Failed:         1,
			StartTime:      &start,
			CompletionTime: &completion,
		},
	}
}

// scenario77SucceededBackupJob builds a Succeeded backup-operation Job fixture.
func scenario77SucceededBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
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

// drainWarningBackupFailed drains the FakeRecorder channel and counts the
// Warning events carrying the BackupFailed reason.
func drainWarningBackupFailed(recorder *record.FakeRecorder) (count int, all []string) {
	for {
		select {
		case e := <-recorder.Events:
			all = append(all, e)
			if strings.Contains(e, corev1.EventTypeWarning) &&
				strings.Contains(e, cbv1alpha1.EventReasonBackupFailed) {
				count++
			}
		default:
			return count, all
		}
	}
}

// --- init container presence + ordering ---

// TestFunctional_Scenario77_InitContainerPresent asserts both the on-demand
// BuildBackupJob and the scheduled BuildBackupCronJob prepend the
// pre-backup-check init container (so it runs BEFORE the gpbackup container).
func (s *Scenario77Suite) TestFunctional_Scenario77_InitContainerPresent() {
	cluster := scenario77Cluster("s77-init", scenario77S3BackupSpec())
	cluster.Spec.Backup.Schedule = "0 2 * * *"

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260608070000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	jobPod := job.Spec.Template.Spec
	require.Len(s.T(), jobPod.InitContainers, 1,
		"backup Job must carry exactly the pre-backup-check init container")
	assert.Equal(s.T(), "pre-backup-check", jobPod.InitContainers[0].Name)
	require.Len(s.T(), jobPod.Containers, 1)
	assert.Equal(s.T(), "gpbackup", jobPod.Containers[0].Name)

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	cronPod := cron.Spec.JobTemplate.Spec.Template.Spec
	require.Len(s.T(), cronPod.InitContainers, 1)
	assert.Equal(s.T(), "pre-backup-check", cronPod.InitContainers[0].Name)
}

// --- 77a: segments-up check ---

// TestFunctional_Scenario77_SegmentsUpCheck asserts the init script queries
// gp_segment_configuration for status='d' segments and exits non-zero (blocks
// the backup) when any are down.
func (s *Scenario77Suite) TestFunctional_Scenario77_SegmentsUpCheck() {
	script := s.scenario77PreBackupScript(scenario77S3BackupSpec())

	assert.Contains(s.T(), script, "gp_segment_configuration",
		"77a must query gp_segment_configuration")
	assert.Contains(s.T(), script, "status='d'",
		"77a must look for down (status='d') segments")
	assert.Contains(s.T(), script, "${down:-0}",
		"77a must guard on the down-segment count")
	assert.Contains(s.T(), script, "exit 1",
		"77a must exit non-zero (block the backup) when a segment is down")
}

// --- 77b: long-running transaction check ---

// TestFunctional_Scenario77_LongRunningTxnCheck asserts the init script scans
// pg_stat_activity for a transaction older than the threshold and exits
// non-zero when one is found.
func (s *Scenario77Suite) TestFunctional_Scenario77_LongRunningTxnCheck() {
	script := s.scenario77PreBackupScript(scenario77S3BackupSpec())

	assert.Contains(s.T(), script, "pg_stat_activity",
		"77b must query pg_stat_activity")
	assert.Contains(s.T(), script, "xact_start",
		"77b must compare against the transaction start time")
	assert.Contains(s.T(), script, "now() - xact_start",
		"77b must compute the open-transaction age")
	assert.Contains(s.T(), script, "interval '3600 seconds'",
		"77b must use the 3600s long-running-txn threshold")
	assert.Contains(s.T(), script, "${longtx:-0}",
		"77b must guard on the long-running-txn count")
}

// --- 77c: S3 reachability check (fail-closed SigV4 HEAD) ---

// TestFunctional_Scenario77_S3ReachabilityCheck asserts the S3-destination init
// script performs a fail-closed SigV4-signed HEAD against ${S3_ENDPOINT}/
// ${S3_BUCKET} that exits non-zero on any non-2xx/3xx response.
func (s *Scenario77Suite) TestFunctional_Scenario77_S3ReachabilityCheck() {
	script := s.scenario77PreBackupScript(scenario77S3BackupSpec())

	assert.Contains(s.T(), script, "s3 bucket reachability",
		"77c must announce the S3 reachability check")
	assert.Contains(s.T(), script, "AWS4-HMAC-SHA256",
		"77c must use SigV4 signing")
	assert.Contains(s.T(), script, "${S3_ENDPOINT", "77c must use the S3 endpoint env var")
	assert.Contains(s.T(), script, "${S3_BUCKET}", "77c must target the S3 bucket env var")
	assert.Contains(s.T(), script, "${AWS_ACCESS_KEY_ID}", "77c must sign with the access key id")
	assert.Contains(s.T(), script, "AWS_SECRET_ACCESS_KEY",
		"77c must sign with the secret access key")
	assert.Contains(s.T(), script, "-X HEAD", "77c must issue an HTTP HEAD request")
	assert.Contains(s.T(), script, "--max-time",
		"77c must bound the request so an unreachable endpoint fails closed")
	// Fail-closed: any non-2xx/3xx response -> exit 1.
	assert.Contains(s.T(), script, "2??|3??",
		"77c must treat only 2xx/3xx as reachable")
	assert.Contains(s.T(), script, "exit 1",
		"77c must exit non-zero (block the backup) on an unreachable bucket")

	// Negative control: a local-destination backup must NOT carry the S3 HEAD.
	localScript := s.scenario77PreBackupScript(scenario77LocalBackupSpec())
	assert.NotContains(s.T(), localScript, "s3 bucket reachability",
		"a local-destination backup must not run the S3 reachability check")
}

// --- 77d: local disk-space check ---

// TestFunctional_Scenario77_LocalDiskSpaceCheck asserts the local-destination
// init script runs a df free-space check on the backup mount and exits
// non-zero when free space is below minBackupDiskFreeKB (1 GiB).
func (s *Scenario77Suite) TestFunctional_Scenario77_LocalDiskSpaceCheck() {
	script := s.scenario77PreBackupScript(scenario77LocalBackupSpec())

	assert.Contains(s.T(), script, "free disk space",
		"77d must announce the disk-space check")
	assert.Contains(s.T(), script, "df -Pk", "77d must use df -Pk for free space")
	assert.Contains(s.T(), script, scenario77LocalPath,
		"77d must check the configured local backup path")
	assert.Contains(s.T(), script, "${free:-0}",
		"77d must guard on the free-space value")
	assert.Contains(s.T(), script, "1048576",
		"77d must compare against minBackupDiskFreeKB (1 GiB in KiB)")
	assert.Contains(s.T(), script, "exit 1",
		"77d must exit non-zero (block the backup) on insufficient free space")

	// Negative control: a local-destination backup must NOT carry the S3 HEAD.
	assert.NotContains(s.T(), script, "AWS4-HMAC-SHA256",
		"a local-destination backup must not run the S3 SigV4 HEAD")
}

// --- Failed backup -> lastBackupStatus=Failed + BackupFailed Warning event ---

// TestFunctional_Scenario77_FailedBackupEmitsWarning seeds a Failed backup Job
// and asserts the operator's status reconcile records
// status.lastBackupStatus=Failed AND emits exactly one Warning/BackupFailed
// Kubernetes Event referencing the failed Job.
func (s *Scenario77Suite) TestFunctional_Scenario77_FailedBackupEmitsWarning() {
	cluster := scenario77Cluster("s77-failed", scenario77S3BackupSpec())
	job := scenario77FailedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608070000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok, "test recorder must be a FakeRecorder")
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err, "reconcile should succeed")

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)
	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	assert.Equal(s.T(), "Failed", updated.Status.BackupHistory[0].Status)

	count, all := drainWarningBackupFailed(recorder)
	assert.Equal(s.T(), 1, count,
		"expected exactly one Warning/BackupFailed event: %v", all)
	require.NotEmpty(s.T(), all)
	found := false
	for _, e := range all {
		if strings.Contains(e, corev1.EventTypeWarning) &&
			strings.Contains(e, cbv1alpha1.EventReasonBackupFailed) &&
			strings.Contains(e, job.Name) {
			found = true
		}
	}
	assert.True(s.T(), found,
		"the Warning/BackupFailed event must reference the failed Job %s: %v", job.Name, all)
}

// TestFunctional_Scenario77_FailedBackupWarningDeDup asserts a second reconcile
// of the SAME unchanged failed backup Job does NOT emit a new Warning (the event
// is de-duplicated per Job to avoid an event storm under periodic reconcile).
func (s *Scenario77Suite) TestFunctional_Scenario77_FailedBackupWarningDeDup() {
	cluster := scenario77Cluster("s77-dedup", scenario77S3BackupSpec())
	job := scenario77FailedBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608070000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	first, _ := drainWarningBackupFailed(recorder)
	require.Equal(s.T(), 1, first, "first reconcile must emit one Warning")

	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)
	second, all := drainWarningBackupFailed(recorder)
	assert.Equal(s.T(), 0, second,
		"second reconcile of the unchanged failed Job must not emit a new Warning: %v", all)
}

// TestFunctional_Scenario77_SucceededBackupNoWarning asserts a Succeeded backup
// Job records status.lastBackupStatus=Success and emits NO BackupFailed Warning.
func (s *Scenario77Suite) TestFunctional_Scenario77_SucceededBackupNoWarning() {
	cluster := scenario77Cluster("s77-success", scenario77S3BackupSpec())
	job := scenario77SucceededBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608070000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	recorder, ok := env.Recorder.(*record.FakeRecorder)
	require.True(s.T(), ok)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)

	count, all := drainWarningBackupFailed(recorder)
	assert.Equal(s.T(), 0, count,
		"a succeeded backup must not emit a BackupFailed Warning: %v", all)
}

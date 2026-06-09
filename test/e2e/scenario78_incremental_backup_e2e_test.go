//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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
// Scenario 78: Incremental Backup Lifecycle (E2E)
// ============================================================================
//
// User journey: an operator-managed cluster with gpbackup.incremental:true takes
// a FULL backup, then (after modifying an append-optimized table) takes one or
// more INCREMENTAL backups — auto-locating the base from the most recent
// compatible backup OR pinning it via --from-timestamp — and finally restores
// from the latest incremental. gprestore validates the FULL incremental set
// (full + every intermediate incremental): a complete chain restores; a chain
// missing an intermediate incremental is REFUSED and surfaced as a Failed restore
// (status.lastBackupStatus=Failed + a Warning/RestoreFailed Event).
//
//	78a Incremental flag wiring : Job AND CronJob args carry
//	    `--incremental --leaf-partition-data` (each exactly once).
//	78b Auto-locate base        : an incremental-labelled Succeeded Job drives
//	    status.lastBackupType=incremental (Job-label precedence over spec).
//	78c Pinned base             : a per-Job FromTimestamp renders --from-timestamp.
//	78d Restore completeness    : a Failed restore Job => lastBackupStatus=Failed +
//	    exactly one Warning/RestoreFailed Event (and NO BackupFailed Warning).
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob/BuildBackupCronJob render the incremental
//     flags once each, label Job/CronJob with avsoft.io/backup-type, and emit
//     --from-timestamp for a pinned per-Job request.
//   - Controller parity: an incremental-labelled Succeeded Job yields
//     lastBackupType=incremental; a Failed restore Job yields
//     lastBackupStatus=Failed + one RestoreFailed Warning and zero BackupFailed.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario78-incremental-backup.sh, which drives the full
//     12-step incremental lifecycle on a running cluster (real gpbackup/gprestore
//     via coordinator-exec; arg assertion from the rendered operator spec).
//
// This Go test never requires gpbackup binaries or a real cluster for its
// deterministic parts; the actual full->incremental->restore->refuse cycle is the
// live shell step (scenario78-incremental-backup.sh).
// ============================================================================

// envKubeconfig (KUBECONFIG) is declared by the Scenario 69 e2e suite and reused
// here to gate the live-cluster portion. envS78Cluster / envS78Script override
// the live target name + script path.
const (
	envS78Cluster = "SCENARIO78_S3_CLUSTER"
	envS78Script  = "SCENARIO78_SCRIPT"

	scenario78E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario78E2EFullTS      = "20260608060000"
)

// Scenario78IncrementalBackupE2ESuite tests the incremental backup arg wiring,
// the Job-label derived status type, and the restore-failure Warning Event flow.
type Scenario78IncrementalBackupE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario78(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario78IncrementalBackupE2ESuite))
}

func (s *Scenario78IncrementalBackupE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario78IncrementalBackupE2ESuite) reqFor(
	cluster *cbv1alpha1.CloudberryCluster,
) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario78E2EIncrementalCluster builds a running cluster with an S3 (MinIO)
// backup destination and incremental backups enabled (mirrors scenario78-s3).
func scenario78E2EIncrementalCluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario78E2EBackupImage,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
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
	return cluster
}

// scenario78E2EIncrementalBackupJob builds a Succeeded incremental-labelled
// backup-operation Job fixture.
func scenario78E2EIncrementalBackupJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationBackup,
				util.LabelBackupType:      util.BackupTypeIncremental,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1, StartTime: &start, CompletionTime: &completion,
		},
	}
}

// scenario78E2EFailedRestoreJob builds a Failed restore-operation Job fixture.
func scenario78E2EFailedRestoreJob(cluster, name string) *batchv1.Job {
	start := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	completion := metav1.NewTime(time.Now())
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "cloudberry-test",
			Labels: map[string]string{
				util.LabelCluster:         cluster,
				util.LabelBackupOperation: util.BackupOperationRestore,
			},
			CreationTimestamp: completion,
		},
		Status: batchv1.JobStatus{
			Failed: 1, StartTime: &start, CompletionTime: &completion,
		},
	}
}

// scenario78E2EWarning drains the FakeRecorder and counts the Warning events
// carrying the given reason.
func scenario78E2EWarning(recorder *record.FakeRecorder, reason string) (int, []string) {
	count := 0
	var all []string
	for {
		select {
		case e := <-recorder.Events:
			all = append(all, e)
			if strings.Contains(e, corev1.EventTypeWarning) && strings.Contains(e, reason) {
				count++
			}
		default:
			return count, all
		}
	}
}

// --- 78.1: builder parity (infra-free) — incremental flag wiring + labels ---

// TestE2E_Scenario78_IncrementalArgsParity verifies BuildBackupJob and
// BuildBackupCronJob render `--incremental --leaf-partition-data` exactly once
// each for an incremental cluster, and that a pinned per-Job request renders
// --from-timestamp.
func (s *Scenario78IncrementalBackupE2ESuite) TestE2E_Scenario78_IncrementalArgsParity() {
	cluster := scenario78E2EIncrementalCluster("test-s78e2e-args")
	cluster.Spec.Backup.Schedule = "0 2 * * *"

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	jobScript := strings.Join(job.Spec.Template.Spec.Containers[0].Args, "\n")
	assert.Equal(s.T(), 1, strings.Count(jobScript, "'--incremental'"),
		"incremental Job must render --incremental once")
	assert.Equal(s.T(), 1, strings.Count(jobScript, "'--leaf-partition-data'"),
		"incremental Job must render --leaf-partition-data once")

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	cronScript := strings.Join(
		cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args, "\n")
	assert.Equal(s.T(), 1, strings.Count(cronScript, "'--incremental'"),
		"incremental CronJob must render --incremental once")
	assert.Equal(s.T(), 1, strings.Count(cronScript, "'--leaf-partition-data'"),
		"incremental CronJob must render --leaf-partition-data once")

	// 78c pinned base.
	pinned := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp:     "20260608080000",
		Type:          util.BackupTypeIncremental,
		Databases:     []string{"mydb"},
		FromTimestamp: scenario78E2EFullTS,
	})
	require.NotNil(s.T(), pinned)
	pinnedScript := strings.Join(pinned.Spec.Template.Spec.Containers[0].Args, "\n")
	assert.Contains(s.T(), pinnedScript, "'--from-timestamp'")
	assert.Contains(s.T(), pinnedScript, "'"+scenario78E2EFullTS+"'")
}

// TestE2E_Scenario78_BackupTypeLabelParity verifies the incremental Job +
// CronJob carry avsoft.io/backup-type=incremental on metadata + pod template.
func (s *Scenario78IncrementalBackupE2ESuite) TestE2E_Scenario78_BackupTypeLabelParity() {
	cluster := scenario78E2EIncrementalCluster("test-s78e2e-label")
	cluster.Spec.Backup.Schedule = "0 2 * * *"

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260608070000", Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	assert.Equal(s.T(), util.BackupTypeIncremental, job.Labels[util.LabelBackupType])
	assert.Equal(s.T(), util.BackupTypeIncremental,
		job.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])

	cron := builder.NewBuilder().BuildBackupCronJob(cluster)
	require.NotNil(s.T(), cron)
	assert.Equal(s.T(), util.BackupTypeIncremental, cron.Labels[util.LabelBackupType])
	assert.Equal(s.T(), util.BackupTypeIncremental,
		cron.Spec.JobTemplate.Spec.Template.ObjectMeta.Labels[util.LabelBackupType])
}

// --- 78.2: controller parity (infra-free) — status type + restore failure ---

// TestE2E_Scenario78_IncrementalStatusParity reconciles against a fake client
// seeded with a Succeeded incremental-labelled Job (on a full spec) and asserts
// the operator records lastBackupType=incremental (Job-label precedence).
func (s *Scenario78IncrementalBackupE2ESuite) TestE2E_Scenario78_IncrementalStatusParity() {
	cluster := scenario78E2EIncrementalCluster("test-s78e2e-status")
	cluster.Spec.Backup.Gpbackup.Incremental = false // spec is full; the Job label must win
	job := scenario78E2EIncrementalBackupJob(cluster.Name,
		util.BackupJobName(cluster.Name, "20260608090000"))

	env := testutil.NewTestK8sEnv(cluster, job)
	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "Success", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), util.BackupTypeIncremental, updated.Status.LastBackupType,
		"lastBackupType must derive from the incremental Job label")
	require.NotEmpty(s.T(), updated.Status.BackupHistory)
	assert.Equal(s.T(), util.BackupTypeIncremental, updated.Status.BackupHistory[0].Type)
}

// TestE2E_Scenario78_FailedRestoreEventParity reconciles against a fake client
// seeded with a Failed restore Job and asserts the operator records
// lastBackupStatus=Failed and emits exactly one Warning/RestoreFailed Event and
// NO BackupFailed Warning (Scenario 78d / restore-only).
func (s *Scenario78IncrementalBackupE2ESuite) TestE2E_Scenario78_FailedRestoreEventParity() {
	cluster := scenario78E2EIncrementalCluster("test-s78e2e-restore-fail")
	job := scenario78E2EFailedRestoreJob(cluster.Name,
		util.RestoreJobName(cluster.Name, "20260608100000"))

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
	assert.Equal(s.T(), "Failed", updated.Status.LastBackupStatus)
	assert.Equal(s.T(), job.Name, updated.Status.LastBackupJobName)

	restoreCount, all := scenario78E2EWarning(recorder, cbv1alpha1.EventReasonRestoreFailed)
	assert.Equal(s.T(), 1, restoreCount,
		"expected exactly one Warning/RestoreFailed event: %v", all)
	backupCount, _ := scenario78E2EWarning(recorder, cbv1alpha1.EventReasonBackupFailed)
	assert.Equal(s.T(), 0, backupCount,
		"a restore failure must NOT emit a BackupFailed Warning")
}

// --- 78.3: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario78_LiveIncrementalLifecycle is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup binaries. When live, it shells out to the scenario78 live script,
// which drives the full 12-step incremental lifecycle: 78a arg assertion;
// FULL backup; modify the AO table; auto-base incremental (status incremental);
// pinned --from-timestamp incremental; restore from latest incremental (success);
// delete an intermediate incremental from S3 and retry (gprestore refuses =>
// Failed restore + RestoreFailed Warning).
func (s *Scenario78IncrementalBackupE2ESuite) TestE2E_Scenario78_LiveIncrementalLifecycle() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live incremental-backup verification")
	}

	cluster := os.Getenv(envS78Cluster)
	if cluster == "" {
		cluster = "scenario78-s3"
	}

	script := os.Getenv(envS78Script)
	if script == "" {
		// Resolve the live script relative to this test file's package dir.
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario78-incremental-backup.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full incremental lifecycle and prints a per-check PASS/FAIL summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario78 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario78 live script must pass all incremental-lifecycle checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}

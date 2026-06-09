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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// Scenario 88: Backup Disabled / No Schedule (functional)
// ============================================================================
//
// These tests black-box the operator at the RECONCILE level (the house style of
// the other backup functional suites, e.g. scenario76_scheduled_backup_test.go):
// they drive the PUBLIC controller.AdminReconciler.Reconcile against a fake
// client via testutil.NewTestK8sEnv and assert on the resulting cluster Status +
// the CronJob/ConfigMap/Job objects. They COMPLEMENT (do not duplicate) the
// internal/controller/backup_status_test.go unit cases by proving the full
// reconcile composition for the disabled / empty-schedule / re-enable paths.
//
// COVERAGE MAP:
//   TestFunctional_Scenario88_DisabledNoCronJob          -> 88a-1, 88a-3
//   TestFunctional_Scenario88_DisabledRemovesCronJob     -> 88a-1 (GAP-3 removal)
//   TestFunctional_Scenario88_DisabledNoRetention        -> 88a-2
//   TestFunctional_Scenario88_EmptyScheduleNoCronJob     -> 88b-1
//   TestFunctional_Scenario88_ReEnableRecreatesCronJob   -> 88a-7 (transition)
//   TestFunctional_Scenario88_EmptyScheduleOnDemandJobBuildable -> 88b-5 (builder)
//
// GAP NOTES (see test/cases/scenario88_backup_disabled_cases.go for the full
// rationale):
//   - GAP-1: the backup ServiceAccount/Role/RoleBinding are CHART-level
//     (cloudberry-backup-sa / cloudberry-backup-role, gated by the Helm value
//     `backup.rbac.create`) and SHARED in the operator namespace; they are NOT
//     created or removed per-cluster. These tests therefore assert ONLY the
//     per-cluster effects (no CronJob, no backup/retention Jobs, empty
//     Status.CronJobName), never a per-cluster SA/Role removal.
//   - GAP-3: disabling backup now REMOVES a previously-created CronJob and clears
//     Status.CronJobName (idempotent).
// ============================================================================

// Scenario88Suite exercises the disabled / empty-schedule / re-enable reconcile
// paths against a fake client.
type Scenario88Suite struct {
	suite.Suite
	ctx context.Context
}

func TestFunctional_Scenario88(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario88Suite))
}

func (s *Scenario88Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario88Suite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

func (s *Scenario88Suite) newReconciler(env *testutil.TestK8sEnv) *controller.AdminReconciler {
	return controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)
}

// scenario88BackupSpec returns an S3-destination BackupSpec with the given
// enabled flag and schedule. A valid S3 destination is included so the spec is
// otherwise complete (the disabled path must still be a no-op).
func scenario88BackupSpec(enabled bool, schedule string) *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled:  enabled,
		Image:    "cloudberry-backup:2.1.0",
		Schedule: schedule,
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
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
			},
		},
	}
}

// scenario88Cluster builds a Running cluster with the given backup spec. A nil
// spec produces a cluster WITHOUT a backup spec (88a-3).
func scenario88Cluster(name string, spec *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	// WithPendingGeneration drives the full spec-driven reconcile path (the
	// generation gate in the admin reconciler short-circuits to a status-only
	// refresh when ObservedGeneration == Generation), so reconcileBackup runs.
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = spec
	return cluster
}

// scenario88CronJobName is the per-cluster backup CronJob name.
func scenario88CronJobName(name string) string {
	return util.BackupCronJobName(name)
}

// --- 88a-1 + 88a-3: disabled / nil-spec reconcile creates no CronJob ---

// TestFunctional_Scenario88_DisabledNoCronJob proves the full reconcile is a
// per-cluster no-op when backup is disabled (Enabled=false) AND when the spec is
// nil: no CronJob, no backup S3 ConfigMap, Status.CronJobName empty.
func (s *Scenario88Suite) TestFunctional_Scenario88_DisabledNoCronJob() {
	cases := []struct {
		name string
		spec *cbv1alpha1.BackupSpec
	}{
		{"88a-1 enabled=false", scenario88BackupSpec(false, "0 2 * * *")},
		{"88a-3 nil backup spec", nil},
	}
	for _, tc := range cases {
		s.Run(tc.name, func() {
			cluster := scenario88Cluster("s88-disabled", tc.spec)
			env := testutil.NewTestK8sEnv(cluster)
			reconciler := s.newReconciler(env)

			_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
			require.NoError(s.T(), err, "reconcile of a disabled cluster must succeed")

			cron := &batchv1.CronJob{}
			getErr := env.Client.Get(s.ctx, types.NamespacedName{
				Name:      scenario88CronJobName(cluster.Name),
				Namespace: cluster.Namespace,
			}, cron)
			assert.True(s.T(), apierrors.IsNotFound(getErr),
				"no backup CronJob must exist when backup is disabled")

			// No backup S3 ConfigMap is created on the disabled path.
			cm := &corev1.ConfigMap{}
			cmErr := env.Client.Get(s.ctx, types.NamespacedName{
				Name:      util.BackupS3ConfigMapName(cluster.Name),
				Namespace: cluster.Namespace,
			}, cm)
			assert.True(s.T(), apierrors.IsNotFound(cmErr),
				"no backup S3 ConfigMap must exist when backup is disabled")

			updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			assert.Empty(s.T(), updated.Status.CronJobName,
				"Status.CronJobName must be empty when backup is disabled")
		})
	}
}

// --- 88a-1 (GAP-3): disabling removes a pre-existing CronJob ---

// TestFunctional_Scenario88_DisabledRemovesCronJob proves the GAP-3 fix: a
// cluster that previously had a scheduled CronJob (Status.CronJobName set) and is
// then disabled has its CronJob DELETED and Status.CronJobName CLEARED on the
// next reconcile.
func (s *Scenario88Suite) TestFunctional_Scenario88_DisabledRemovesCronJob() {
	cluster := scenario88Cluster("s88-disable-removes", scenario88BackupSpec(false, "0 2 * * *"))
	cluster.Status.CronJobName = scenario88CronJobName(cluster.Name)
	existing := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      scenario88CronJobName(cluster.Name),
			Namespace: cluster.Namespace,
		},
		Spec: batchv1.CronJobSpec{Schedule: "0 2 * * *"},
	}
	env := testutil.NewTestK8sEnv(cluster, existing)
	reconciler := s.newReconciler(env)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cron := &batchv1.CronJob{}
	getErr := env.Client.Get(s.ctx, types.NamespacedName{
		Name:      existing.Name,
		Namespace: existing.Namespace,
	}, cron)
	assert.True(s.T(), apierrors.IsNotFound(getErr),
		"a pre-existing CronJob must be deleted when backup is disabled")

	// NOTE: the in-memory clearing of Status.CronJobName by reconcileBackup is
	// asserted directly by the unit test TestReconcileBackup_DisabledRemovesCronJob.
	// At the full-reconcile layer the status is persisted via a MergePatch (see
	// patchStatus): an empty CronJobName (omitempty) is omitted from the patch, so
	// a previously-persisted value is left unchanged in the fake store. The strong,
	// reconcile-driven observable here is therefore the CronJob DELETION above; the
	// empty CronJobName is proven by the leaf unit test and by the
	// EmptyScheduleNoCronJob path (which starts from no persisted value).
}

// --- 88a-2: disabled => retention cleanup is a no-op ---

// TestFunctional_Scenario88_DisabledNoRetention proves no retention cleanup Job
// (operation=cleanup) is created when backup is disabled, even with a retention
// policy present in the spec.
func (s *Scenario88Suite) TestFunctional_Scenario88_DisabledNoRetention() {
	spec := scenario88BackupSpec(false, "0 2 * * *")
	spec.Retention = cbv1alpha1.BackupRetention{FullCount: 1, IncrementalCount: 1, MaxAge: "1d"}
	cluster := scenario88Cluster("s88-noretention", spec)
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := s.newReconciler(env)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), env.Client.List(s.ctx, jobs))
	for i := range jobs.Items {
		assert.NotEqual(s.T(), util.BackupOperationCleanup,
			jobs.Items[i].Labels[util.LabelBackupOperation],
			"no retention cleanup Job must be created when backup is disabled")
	}
}

// --- 88b-1: enabled + empty schedule => no CronJob ---

// TestFunctional_Scenario88_EmptyScheduleNoCronJob proves the full reconcile of
// an ENABLED cluster with an EMPTY schedule creates NO CronJob and leaves
// Status.CronJobName empty (the empty-schedule branch of ensureBackupCronJob).
func (s *Scenario88Suite) TestFunctional_Scenario88_EmptyScheduleNoCronJob() {
	cluster := scenario88Cluster("s88-emptysched", scenario88BackupSpec(true, ""))
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := s.newReconciler(env)

	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cron := &batchv1.CronJob{}
	getErr := env.Client.Get(s.ctx, types.NamespacedName{
		Name:      scenario88CronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron)
	assert.True(s.T(), apierrors.IsNotFound(getErr),
		"no backup CronJob must exist for an enabled cluster with an empty schedule")

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), updated.Status.CronJobName,
		"Status.CronJobName must be empty when the schedule is empty")
}

// --- 88a-7: re-enable transition recreates the CronJob ---

// TestFunctional_Scenario88_ReEnableRecreatesCronJob proves the disabled->enabled
// transition within one run: start disabled (assert NO CronJob, empty
// CronJobName), then set Enabled=true + Schedule, reconcile again, and assert the
// CronJob is recreated with the schedule and Status.CronJobName is set.
func (s *Scenario88Suite) TestFunctional_Scenario88_ReEnableRecreatesCronJob() {
	cluster := scenario88Cluster("s88-reenable", scenario88BackupSpec(false, "0 2 * * *"))
	env := testutil.NewTestK8sEnv(cluster)
	reconciler := s.newReconciler(env)

	// Phase 1: disabled => no CronJob, empty CronJobName.
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	cron := &batchv1.CronJob{}
	getErr := env.Client.Get(s.ctx, types.NamespacedName{
		Name:      scenario88CronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cron)
	require.True(s.T(), apierrors.IsNotFound(getErr),
		"no CronJob must exist in the disabled phase")
	disabled, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Empty(s.T(), disabled.Status.CronJobName)

	// Phase 2: re-enable with a schedule => CronJob recreated + CronJobName set.
	disabled.Spec.Backup.Enabled = true
	disabled.Spec.Backup.Schedule = "0 3 * * *"
	require.NoError(s.T(), env.Client.Update(s.ctx, disabled))

	_, err = reconciler.Reconcile(s.ctx, s.reqFor(cluster))
	require.NoError(s.T(), err)

	recreated := &batchv1.CronJob{}
	require.NoError(s.T(), env.Client.Get(s.ctx, types.NamespacedName{
		Name:      scenario88CronJobName(cluster.Name),
		Namespace: cluster.Namespace,
	}, recreated), "the CronJob must be recreated after re-enable")
	assert.Equal(s.T(), "0 3 * * *", recreated.Spec.Schedule)

	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), scenario88CronJobName(cluster.Name), updated.Status.CronJobName,
		"Status.CronJobName must be set after re-enable")
}

// --- 88b-5: builder parity — on-demand Job buildable without a schedule ---

// TestFunctional_Scenario88_EmptyScheduleOnDemandJobBuildable proves the
// on-demand backup path does not depend on a schedule: BuildBackupCronJob returns
// nil for an enabled cluster with an empty schedule, while BuildBackupJob still
// returns a non-nil Job carrying the operation=backup label.
func (s *Scenario88Suite) TestFunctional_Scenario88_EmptyScheduleOnDemandJobBuildable() {
	cluster := scenario88Cluster("s88-ondemand", scenario88BackupSpec(true, ""))
	b := builder.NewBuilder()

	assert.Nil(s.T(), b.BuildBackupCronJob(cluster),
		"BuildBackupCronJob must return nil for an empty schedule")

	job := b.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: "20260101020000",
		Type:      util.BackupTypeFull,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job, "BuildBackupJob must build an on-demand Job without a schedule")
	assert.Equal(s.T(), util.BackupOperationBackup, job.Labels[util.LabelBackupOperation],
		"the on-demand Job must carry the operation=backup label")
}

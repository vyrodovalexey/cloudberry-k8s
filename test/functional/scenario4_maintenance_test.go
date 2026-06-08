//go:build functional

package functional

import (
	"context"
	"fmt"
	"log/slog"
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
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario4ClusterName = "scenario4-cluster"
	scenario4Namespace   = "cloudberry-test"
)

// Scenario4MaintenanceSuite tests maintenance operations (vacuum, analyze, reindex)
// via the admin controller, verifying that Jobs are created with correct SQL commands.
type Scenario4MaintenanceSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario4_Maintenance(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario4MaintenanceSuite))
}

func (s *Scenario4MaintenanceSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario4MaintenanceSuite) TearDownTest() {
	s.cancel()
}

// buildScenario4Cluster constructs a Running cluster with pending generation for maintenance tests.
func buildScenario4Cluster(name string) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(name, scenario4Namespace).
		WithVersion("7.1.0").
		WithImage("cloudberrydb/cloudberry:7.1.0").
		WithSegments(4).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario4Req creates a reconcile request for the given cluster.
func scenario4Req(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// newScenario4AdminReconciler creates an AdminReconciler for scenario 4 tests.
func newScenario4AdminReconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	logger *slog.Logger,
) *controller.AdminReconciler {
	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}
	return controller.NewAdminReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, dbFactory, env.Metrics, logger,
	)
}

// setMaintenanceAnnotation sets the maintenance annotation on the cluster.
func (s *Scenario4MaintenanceSuite) setMaintenanceAnnotation(
	cluster *cbv1alpha1.CloudberryCluster,
	operation string,
) {
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.AnnotationMaintenance] = operation
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "setting maintenance annotation should succeed")
}

// findMaintenanceJob searches for a Job whose name contains the given operation substring.
func (s *Scenario4MaintenanceSuite) findMaintenanceJob(
	namespace, operationSubstr string,
) *batchv1.Job {
	jobList := &batchv1.JobList{}
	err := s.env.Client.List(s.ctx, jobList, client.InNamespace(namespace))
	require.NoError(s.T(), err, "listing jobs should succeed")

	for i := range jobList.Items {
		if strings.Contains(jobList.Items[i].Name, operationSubstr) {
			return &jobList.Items[i]
		}
	}
	return nil
}

// getJobCommand extracts the command from the first container of a Job's pod template.
func getJobCommand(job *batchv1.Job) []string {
	if len(job.Spec.Template.Spec.Containers) == 0 {
		return nil
	}
	return job.Spec.Template.Spec.Containers[0].Command
}

// --- Test: Vacuum Variants (Direct DB Execution) ---

func (s *Scenario4MaintenanceSuite) TestScenario4a_VacuumVariants() {
	tests := []struct {
		name      string
		operation string
	}{
		{"vacuum", util.MaintenanceVacuum},
		{"vacuum-analyze", util.MaintenanceVacuumAnalyze},
		{"vacuum-full", util.MaintenanceVacuumFull},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			clusterName := scenario4ClusterName + "-" + tt.name
			cluster := buildScenario4Cluster(clusterName)
			cluster.Annotations[util.AnnotationMaintenance] = tt.operation
			s.env = testutil.NewTestK8sEnv(cluster)

			fakeRecorder := record.NewFakeRecorder(100)
			reconciler := newScenario4AdminReconciler(s.env, fakeRecorder, s.logger)

			// Act: reconcile with maintenance annotation.
			result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
			require.NoError(s.T(), err, "reconciliation should succeed for %s", tt.name)
			assert.NotZero(s.T(), result.RequeueAfter, "should requeue after maintenance")

			// With a working DB client, the operation completes directly (no Job needed).
			// Verify MaintenanceCompleted event was emitted.
			events := collectEvents(fakeRecorder)
			maintenanceCompletedFound := false
			for _, event := range events {
				if containsSubstring(event, "MaintenanceCompleted") {
					maintenanceCompletedFound = true
					break
				}
			}
			assert.True(s.T(), maintenanceCompletedFound,
				"MaintenanceCompleted event should be emitted for %s; events: %v", tt.name, events)

			// Verify annotation was removed.
			updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
			require.NoError(s.T(), err)
			_, hasMaintenance := updated.Annotations[util.AnnotationMaintenance]
			assert.False(s.T(), hasMaintenance,
				"maintenance annotation should be removed after processing %s", tt.name)
		})
	}
}

// --- Test: Analyze ---

func (s *Scenario4MaintenanceSuite) TestScenario4b_Analyze() {
	clusterName := scenario4ClusterName + "-analyze"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = util.MaintenanceAnalyze
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario4AdminReconciler(s.env, fakeRecorder, s.logger)

	// Act.
	result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed for analyze")
	assert.NotZero(s.T(), result.RequeueAfter)

	// With a working DB client, the operation completes directly.
	events := collectEvents(fakeRecorder)
	completedFound := false
	for _, event := range events {
		if containsSubstring(event, "MaintenanceCompleted") {
			completedFound = true
			break
		}
	}
	assert.True(s.T(), completedFound,
		"MaintenanceCompleted event should be emitted for analyze; events: %v", events)
}

// --- Test: Reindex ---

func (s *Scenario4MaintenanceSuite) TestScenario4c_Reindex() {
	clusterName := scenario4ClusterName + "-reindex"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = util.MaintenanceReindex
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario4AdminReconciler(s.env, fakeRecorder, s.logger)

	// Act.
	result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed for reindex")
	assert.NotZero(s.T(), result.RequeueAfter)

	// With a working DB client, the operation completes directly.
	events := collectEvents(fakeRecorder)
	completedFound := false
	for _, event := range events {
		if containsSubstring(event, "MaintenanceCompleted") {
			completedFound = true
			break
		}
	}
	assert.True(s.T(), completedFound,
		"MaintenanceCompleted event should be emitted for reindex; events: %v", events)
}

// --- Test: Unknown Maintenance ---

func (s *Scenario4MaintenanceSuite) TestScenario4d_UnknownMaintenance() {
	clusterName := scenario4ClusterName + "-unknown"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = "unknown-operation"
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario4AdminReconciler(s.env, fakeRecorder, s.logger)

	// Act.
	result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed for unknown operation")
	assert.Zero(s.T(), result.RequeueAfter,
		"should not requeue for unknown maintenance operation")

	// Verify no Job was created.
	jobList := &batchv1.JobList{}
	err = s.env.Client.List(s.ctx, jobList, client.InNamespace(cluster.Namespace))
	require.NoError(s.T(), err)
	assert.Empty(s.T(), jobList.Items,
		"no jobs should be created for unknown maintenance operation")

	// Verify MaintenanceUnknown warning event was emitted.
	events := collectEvents(fakeRecorder)
	unknownEventFound := false
	for _, event := range events {
		if containsSubstring(event, "MaintenanceUnknown") {
			unknownEventFound = true
			break
		}
	}
	assert.True(s.T(), unknownEventFound,
		"MaintenanceUnknown warning event should be emitted; events: %v", events)
}

// --- Test: Job Properties (Fallback when DB client fails) ---

func (s *Scenario4MaintenanceSuite) TestScenario4_JobProperties() {
	clusterName := scenario4ClusterName + "-props"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = util.MaintenanceVacuum
	s.env = testutil.NewTestK8sEnv(cluster)

	// Use a DB client that fails, forcing the Job fallback path.
	failingDB := &testutil.MockDBClient{
		VacuumFunc: func(_ context.Context, _ db.VacuumOptions) error {
			return fmt.Errorf("simulated vacuum failure")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: failingDB}

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.logger,
	)

	// Act.
	_, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Find the created Job (fallback path).
	job := s.findMaintenanceJob(cluster.Namespace, util.MaintenanceVacuum)
	require.NotNil(s.T(), job, "maintenance job should be created when DB client fails")

	// Verify labels.
	assert.Equal(s.T(), util.LabelManagedByValue, job.Labels[util.LabelManagedBy],
		"job should have managed-by label")
	assert.Equal(s.T(), clusterName, job.Labels[util.LabelCluster],
		"job should have cluster label")
	assert.Equal(s.T(), util.ComponentCoordinator, job.Labels[util.LabelComponent],
		"job should have component label")
	assert.Equal(s.T(), util.MaintenanceVacuum, job.Labels[util.LabelOperation],
		"job should have operation label")

	// Verify owner reference.
	require.Len(s.T(), job.OwnerReferences, 1,
		"job should have exactly one owner reference")
	assert.Equal(s.T(), clusterName, job.OwnerReferences[0].Name,
		"owner reference should point to the cluster")
	assert.Equal(s.T(), "CloudberryCluster", job.OwnerReferences[0].Kind,
		"owner reference kind should be CloudberryCluster")
	require.NotNil(s.T(), job.OwnerReferences[0].Controller)
	assert.True(s.T(), *job.OwnerReferences[0].Controller,
		"owner reference should be a controller reference")

	// Verify backoff limit.
	require.NotNil(s.T(), job.Spec.BackoffLimit,
		"backoff limit should be set")
	assert.Equal(s.T(), int32(1), *job.Spec.BackoffLimit,
		"backoff limit should be 1")

	// Verify TTL.
	require.NotNil(s.T(), job.Spec.TTLSecondsAfterFinished,
		"TTL should be set")
	assert.Equal(s.T(), int32(3600), *job.Spec.TTLSecondsAfterFinished,
		"TTL should be 3600 seconds (1 hour)")

	// Verify pod template.
	podSpec := job.Spec.Template.Spec
	assert.Equal(s.T(), corev1.RestartPolicyNever, podSpec.RestartPolicy,
		"restart policy should be Never")

	// Verify container.
	require.Len(s.T(), podSpec.Containers, 1,
		"job should have exactly one container")
	container := podSpec.Containers[0]
	assert.Equal(s.T(), "cloudberrydb/cloudberry:7.1.0", container.Image,
		"container image should match cluster image")

	// Verify PGPASSWORD env var.
	require.Len(s.T(), container.Env, 1,
		"container should have exactly one env var")
	assert.Equal(s.T(), "PGPASSWORD", container.Env[0].Name,
		"env var should be PGPASSWORD")
	require.NotNil(s.T(), container.Env[0].ValueFrom,
		"PGPASSWORD should come from a secret")
	require.NotNil(s.T(), container.Env[0].ValueFrom.SecretKeyRef,
		"PGPASSWORD should reference a secret key")
	assert.Equal(s.T(), util.AdminPasswordSecretName(clusterName),
		container.Env[0].ValueFrom.SecretKeyRef.Name,
		"PGPASSWORD secret name should match admin password secret")
	assert.Equal(s.T(), "password",
		container.Env[0].ValueFrom.SecretKeyRef.Key,
		"PGPASSWORD secret key should be 'password'")

	// Verify command includes coordinator service name.
	cmd := container.Command
	require.NotEmpty(s.T(), cmd, "command should not be empty")
	assert.Equal(s.T(), "psql", cmd[0], "command should start with psql")
	assert.Contains(s.T(), cmd, util.CoordinatorServiceName(clusterName),
		"command should reference coordinator service")
	assert.Contains(s.T(), cmd, util.DefaultAdminUser,
		"command should use admin user")

	// Verify MaintenanceStarted event was emitted (fallback path).
	events := collectEvents(fakeRecorder)
	startedFound := false
	for _, event := range events {
		if containsSubstring(event, "MaintenanceStarted") {
			startedFound = true
			break
		}
	}
	assert.True(s.T(), startedFound,
		"MaintenanceStarted event should be emitted for fallback; events: %v", events)
}

// --- Test: Log Rotate ---

func (s *Scenario4MaintenanceSuite) TestScenario4e_LogRotate() {
	clusterName := scenario4ClusterName + "-logrotate"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = util.MaintenanceLogRotate
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario4AdminReconciler(s.env, fakeRecorder, s.logger)

	// Act.
	result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed for log-rotate")
	assert.NotZero(s.T(), result.RequeueAfter)

	// With a working DB client, log rotation completes directly.
	events := collectEvents(fakeRecorder)
	completedFound := false
	for _, event := range events {
		if containsSubstring(event, "MaintenanceCompleted") {
			completedFound = true
			break
		}
	}
	assert.True(s.T(), completedFound,
		"MaintenanceCompleted event should be emitted for log-rotate; events: %v", events)

	// Verify annotation was removed.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasMaintenance := updated.Annotations[util.AnnotationMaintenance]
	assert.False(s.T(), hasMaintenance,
		"maintenance annotation should be removed after log-rotate")
}

// --- Test: Log Rotate Job Fallback ---

func (s *Scenario4MaintenanceSuite) TestScenario4f_LogRotateJobFallback() {
	clusterName := scenario4ClusterName + "-logrotate-fb"
	cluster := buildScenario4Cluster(clusterName)
	cluster.Annotations[util.AnnotationMaintenance] = util.MaintenanceLogRotate
	s.env = testutil.NewTestK8sEnv(cluster)

	// Use a DB client that fails log rotation, forcing the Job fallback.
	failingDB := &testutil.MockDBClient{
		LogRotateFunc: func(_ context.Context) error {
			return fmt.Errorf("simulated log rotate failure")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: failingDB}

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.logger,
	)

	// Act.
	result, err := reconciler.Reconcile(s.ctx, scenario4Req(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed for log-rotate fallback")
	assert.NotZero(s.T(), result.RequeueAfter)

	// Verify a Job was created with the log-rotate operation.
	job := s.findMaintenanceJob(cluster.Namespace, util.MaintenanceLogRotate)
	require.NotNil(s.T(), job, "maintenance job should be created when log-rotate DB call fails")

	// Verify the Job's command contains pg_rotate_logfile.
	cmd := getJobCommand(job)
	require.NotEmpty(s.T(), cmd, "job command should not be empty")
	cmdStr := strings.Join(cmd, " ")
	assert.Contains(s.T(), cmdStr, "pg_rotate_logfile",
		"job command should contain pg_rotate_logfile")
}

//go:build functional

package functional

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario3ClusterName = "scenario3-cluster"
	scenario3Namespace   = "cloudberry-test"
)

// Scenario3StopStartSuite tests stop/start/restart cluster lifecycle scenarios.
type Scenario3StopStartSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario3_StopStart(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario3StopStartSuite))
}

func (s *Scenario3StopStartSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario3StopStartSuite) TearDownTest() {
	s.cancel()
}

// buildScenario3Cluster constructs a Running cluster with 4 segments, standby, and mirroring.
func buildScenario3Cluster() *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario3ClusterName, scenario3Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario3Req creates a reconcile request for the scenario 3 cluster.
func scenario3Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario3ClusterName,
			Namespace: scenario3Namespace,
		},
	}
}

// createScenario3ReadyStatefulSet creates a StatefulSet with ready replicas.
func (s *Scenario3StopStartSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "db", Image: "cloudberrydb/cloudberry:7.1.0"},
					},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:      replicas,
			ReadyReplicas: replicas,
		},
	}
	err := s.env.Client.Create(s.ctx, sts)
	require.NoError(s.T(), err, "creating statefulset %s should succeed", name)
}

// createAllStatefulSets creates all StatefulSets for a cluster with ready replicas.
func (s *Scenario3StopStartSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	}
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		s.createReadyStatefulSet(util.StandbyName(cluster.Name), cluster.Namespace, 1)
	}
}

// simulateAllPodsTerminated sets all StatefulSet statuses to 0 replicas.
func (s *Scenario3StopStartSuite) simulateAllPodsTerminated(cluster *cbv1alpha1.CloudberryCluster) {
	stsNames := []string{
		util.CoordinatorName(cluster.Name),
		util.SegmentPrimaryName(cluster.Name),
	}
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		stsNames = append(stsNames, util.SegmentMirrorName(cluster.Name))
	}
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		stsNames = append(stsNames, util.StandbyName(cluster.Name))
	}

	for _, name := range stsNames {
		sts := &appsv1.StatefulSet{}
		err := s.env.Client.Get(s.ctx, types.NamespacedName{
			Name:      name,
			Namespace: cluster.Namespace,
		}, sts)
		if err != nil {
			continue
		}
		sts.Status.Replicas = 0
		sts.Status.ReadyReplicas = 0
		err = s.env.Client.Status().Update(s.ctx, sts)
		require.NoError(s.T(), err, "updating statefulset %s status should succeed", name)
	}
}

// simulateCoordinatorReady sets the coordinator StatefulSet status to 1 ready replica.
func (s *Scenario3StopStartSuite) simulateCoordinatorReady(cluster *cbv1alpha1.CloudberryCluster) {
	coordName := util.CoordinatorName(cluster.Name)
	sts := &appsv1.StatefulSet{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      coordName,
		Namespace: cluster.Namespace,
	}, sts)
	require.NoError(s.T(), err, "getting coordinator statefulset should succeed")

	sts.Status.Replicas = 1
	sts.Status.ReadyReplicas = 1
	err = s.env.Client.Status().Update(s.ctx, sts)
	require.NoError(s.T(), err, "updating coordinator status should succeed")
}

// setActionAnnotation sets the action annotation on the cluster.
func (s *Scenario3StopStartSuite) setActionAnnotation(cluster *cbv1alpha1.CloudberryCluster, action string) {
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.AnnotationAction] = action
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "setting action annotation should succeed")
}

// getStatefulSetReplicas returns the spec replicas for a StatefulSet.
func (s *Scenario3StopStartSuite) getStatefulSetReplicas(name, namespace string) int32 {
	sts := &appsv1.StatefulSet{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts)
	if err != nil {
		return -1
	}
	if sts.Spec.Replicas == nil {
		return 1
	}
	return *sts.Spec.Replicas
}

// newScenario3Reconciler creates a ClusterReconciler for scenario 3 tests.
func newScenario3Reconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	logger *slog.Logger,
) *controller.ClusterReconciler {
	return controller.NewClusterReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, env.Metrics, logger,
	)
}

// --- Test: Smart Stop + Normal Start ---

func (s *Scenario3StopStartSuite) TestScenario3a_SmartStop_NormalStart() {
	// Arrange: create a Running cluster with all StatefulSets ready.
	cluster := buildScenario3Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario3Reconciler(s.env, fakeRecorder, s.logger)

	// Act: annotate with stop action.
	s.setActionAnnotation(cluster, util.ActionStop)

	// First reconcile: should set phase to Stopping and scale down.
	result, err := reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "stop reconciliation should succeed")

	// Verify phase is Stopping (pods not yet terminated).
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopping, current.Status.Phase,
		"phase should be Stopping after stop action")

	// Verify all StatefulSets are scaled to 0.
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.CoordinatorName(cluster.Name), cluster.Namespace),
		"coordinator should be scaled to 0")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 0")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 0")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.StandbyName(cluster.Name), cluster.Namespace),
		"standby should be scaled to 0")

	// Should requeue since pods are not yet terminated.
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue while stopping")

	// Simulate all pods terminated.
	s.simulateAllPodsTerminated(cluster)

	// Second reconcile: should transition to Stopped.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "second stop reconciliation should succeed")

	// Verify phase is Stopped.
	stopped, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopped, stopped.Status.Phase,
		"phase should be Stopped after all pods terminated")
	assert.False(s.T(), stopped.Status.CoordinatorReady,
		"coordinator should not be ready when stopped")
	assert.Equal(s.T(), int32(0), stopped.Status.SegmentsReady,
		"segments ready should be 0 when stopped")

	// Verify Stopped event was emitted.
	events := collectEvents(fakeRecorder)
	stoppedEventFound := false
	for _, event := range events {
		if containsSubstring(event, "Stopped") {
			stoppedEventFound = true
			break
		}
	}
	assert.True(s.T(), stoppedEventFound,
		"Stopped event should be emitted; events: %v", events)

	// Act: annotate with start action.
	s.setActionAnnotation(cluster, util.ActionStart)

	// Reconcile: should start the cluster back up.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "start reconciliation should succeed")

	// Verify StatefulSets are scaled back up.
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.CoordinatorName(cluster.Name), cluster.Namespace), int32(0),
		"coordinator should be scaled back up")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace), int32(0),
		"primary segments should be scaled back up")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace), int32(0),
		"mirror segments should be scaled back up")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.StandbyName(cluster.Name), cluster.Namespace), int32(0),
		"standby should be scaled back up")
}

// --- Test: Fast Stop + Restricted Start ---

func (s *Scenario3StopStartSuite) TestScenario3b_FastStop_RestrictedStart() {
	// Arrange: create a Running cluster with all StatefulSets ready.
	cluster := buildScenario3Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario3Reconciler(s.env, fakeRecorder, s.logger)

	// Act: annotate with stop-fast action.
	s.setActionAnnotation(cluster, util.ActionStopFast)

	// Reconcile: should set phase to Stopping.
	_, err := reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "stop-fast reconciliation should succeed")

	// Verify phase is Stopping.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopping, current.Status.Phase,
		"phase should be Stopping after stop-fast action")

	// Simulate all pods terminated.
	s.simulateAllPodsTerminated(cluster)

	// Reconcile again: should transition to Stopped.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	stopped, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopped, stopped.Status.Phase,
		"phase should be Stopped")

	// Act: annotate with start-restricted action.
	s.setActionAnnotation(cluster, util.ActionStartRestricted)

	// Reconcile: should start in restricted mode.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "start-restricted reconciliation should succeed")

	// Verify phase is Restricted.
	restricted, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRestricted, restricted.Status.Phase,
		"phase should be Restricted after start-restricted")

	// Verify only coordinator is scaled up, segments remain at 0.
	assert.Equal(s.T(), int32(1), s.getStatefulSetReplicas(util.CoordinatorName(cluster.Name), cluster.Namespace),
		"coordinator should be scaled to 1 in restricted mode")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 0 in restricted mode")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should remain at 0 in restricted mode")

	// Verify events.
	events := collectEvents(fakeRecorder)
	restrictedStartFound := false
	for _, event := range events {
		if containsSubstring(event, "restricted") {
			restrictedStartFound = true
			break
		}
	}
	assert.True(s.T(), restrictedStartFound,
		"restricted start event should be emitted; events: %v", events)
}

// --- Test: Immediate Stop + Maintenance Start ---

func (s *Scenario3StopStartSuite) TestScenario3c_ImmediateStop_MaintenanceStart() {
	// Arrange: create a Running cluster with all StatefulSets ready.
	cluster := buildScenario3Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario3Reconciler(s.env, fakeRecorder, s.logger)

	// Act: annotate with stop-immediate action.
	s.setActionAnnotation(cluster, util.ActionStopImmediate)

	// Reconcile: should set phase to Stopping.
	_, err := reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "stop-immediate reconciliation should succeed")

	// Verify phase is Stopping.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopping, current.Status.Phase,
		"phase should be Stopping after stop-immediate action")

	// Simulate all pods terminated.
	s.simulateAllPodsTerminated(cluster)

	// Reconcile again: should transition to Stopped.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	stopped, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopped, stopped.Status.Phase,
		"phase should be Stopped")

	// Act: annotate with start-maintenance action.
	s.setActionAnnotation(cluster, util.ActionStartMaintenance)

	// Reconcile: should start in maintenance mode.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "start-maintenance reconciliation should succeed")

	// Verify phase is Maintenance.
	maintenance, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseMaintenance, maintenance.Status.Phase,
		"phase should be Maintenance after start-maintenance")

	// Verify only coordinator is running, segments not started.
	assert.Equal(s.T(), int32(1), s.getStatefulSetReplicas(util.CoordinatorName(cluster.Name), cluster.Namespace),
		"coordinator should be scaled to 1 in maintenance mode")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 0 in maintenance mode")
	assert.Equal(s.T(), int32(0), s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should remain at 0 in maintenance mode")

	// Verify events.
	events := collectEvents(fakeRecorder)
	maintenanceStartFound := false
	for _, event := range events {
		if containsSubstring(event, "maintenance") {
			maintenanceStartFound = true
			break
		}
	}
	assert.True(s.T(), maintenanceStartFound,
		"maintenance start event should be emitted; events: %v", events)
}

// --- Test: Restart ---

func (s *Scenario3StopStartSuite) TestScenario3d_Restart() {
	// Arrange: create a Running cluster with all StatefulSets ready.
	cluster := buildScenario3Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	reconciler := newScenario3Reconciler(s.env, fakeRecorder, s.logger)

	// Act: annotate with restart action.
	s.setActionAnnotation(cluster, util.ActionRestart)

	// First reconcile: should set phase to Stopping and scale down.
	result, err := reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "restart reconciliation should succeed")

	// Verify phase is Stopping (pods not yet terminated).
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopping, current.Status.Phase,
		"phase should be Stopping during restart")

	// Should requeue since pods are not yet terminated.
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue while stopping during restart")

	// Simulate all pods terminated.
	s.simulateAllPodsTerminated(cluster)

	// Second reconcile: should transition through Stopped and start back up.
	_, err = reconciler.Reconcile(s.ctx, scenario3Req())
	require.NoError(s.T(), err, "second restart reconciliation should succeed")

	// Verify all StatefulSets are scaled back up.
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.CoordinatorName(cluster.Name), cluster.Namespace), int32(0),
		"coordinator should be scaled back up after restart")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace), int32(0),
		"primary segments should be scaled back up after restart")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace), int32(0),
		"mirror segments should be scaled back up after restart")
	assert.Greater(s.T(), s.getStatefulSetReplicas(util.StandbyName(cluster.Name), cluster.Namespace), int32(0),
		"standby should be scaled back up after restart")

	// Verify events.
	events := collectEvents(fakeRecorder)
	restartEventFound := false
	restartedEventFound := false
	for _, event := range events {
		if containsSubstring(event, "Restarting") {
			restartEventFound = true
		}
		if containsSubstring(event, "Restarted") {
			restartedEventFound = true
		}
	}
	assert.True(s.T(), restartEventFound,
		"Restarting event should be emitted; events: %v", events)
	assert.True(s.T(), restartedEventFound,
		"Restarted event should be emitted; events: %v", events)
}

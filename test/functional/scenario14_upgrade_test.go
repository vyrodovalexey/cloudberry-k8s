//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario14ClusterName = "scenario14-cluster"
	scenario14Namespace   = "cloudberry-test"
	scenario14OldImage    = "postgres:16"
	scenario14NewImage    = "postgres:17"
	scenario14OldVersion  = "7.1.0"
	scenario14NewVersion  = "7.2.0"
	scenario14SegCount    = int32(4)
)

// Scenario14UpgradeSuite tests cluster upgrade with rollback scenarios.
type Scenario14UpgradeSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario14_Upgrade(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario14UpgradeSuite))
}

func (s *Scenario14UpgradeSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario14UpgradeSuite) TearDownTest() {
	s.cancel()
}

// buildScenario14Cluster constructs a Running cluster with the old image/version.
func buildScenario14Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario14ClusterName, scenario14Namespace).
		WithVersion(scenario14OldVersion).
		WithImage(scenario14OldImage).
		WithSegments(scenario14SegCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Status.ClusterVersion = scenario14OldVersion
	return cluster
}

// scenario14Req creates a reconcile request for the scenario 14 cluster.
func scenario14Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario14ClusterName,
			Namespace: scenario14Namespace,
		},
	}
}

// newScenario14Reconciler creates a ClusterReconciler for scenario 14 tests.
func newScenario14Reconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	metricsRec metrics.Recorder,
	logger *slog.Logger,
) *controller.ClusterReconciler {
	return controller.NewClusterReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, metricsRec, logger,
	)
}

// createReadyStatefulSet14 creates a StatefulSet with ready replicas and a specific image.
func (s *Scenario14UpgradeSuite) createReadyStatefulSet(name, namespace, image string, replicas int32) {
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
						{Name: "db", Image: image},
					},
				},
			},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			UpdatedReplicas: replicas,
		},
	}
	err := s.env.Client.Create(s.ctx, sts)
	require.NoError(s.T(), err, "creating statefulset %s should succeed", name)
}

// createAllStatefulSets14 creates all StatefulSets for a cluster with the given image.
func (s *Scenario14UpgradeSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster, image string) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, image, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, image, cluster.Spec.Segments.Count)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, image, cluster.Spec.Segments.Count)
	}
}

// getStatefulSetImage returns the first container image of a StatefulSet.
func (s *Scenario14UpgradeSuite) getStatefulSetImage(name, namespace string) string {
	sts := &appsv1.StatefulSet{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts)
	if err != nil {
		return ""
	}
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		return sts.Spec.Template.Spec.Containers[0].Image
	}
	return ""
}

// --- Test: Upgrade Happy Path ---

func (s *Scenario14UpgradeSuite) TestScenario14_UpgradeHappyPath() {
	// Arrange: create a Running cluster with old image.
	cluster := buildScenario14Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation since the fake client's status subresource
	// does not persist status fields set during object creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.ClusterVersion = scenario14OldVersion
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario14SegCount
	current.Status.SegmentsTotal = scenario14SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSets(cluster, scenario14OldImage)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario14Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch spec to new image/version and bump generation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Image = scenario14NewImage
	current.Spec.Version = scenario14NewVersion
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	// First reconciliation: should detect upgrade needed and start it.
	_, err = reconciler.Reconcile(s.ctx, scenario14Req())
	require.NoError(s.T(), err)

	// Verify phase -> Updating.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseUpdating, updated.Status.Phase,
		"phase should be Updating after upgrade started")

	// Verify UpgradeStarted event.
	events := collectEvents(fakeRecorder)
	upgradeStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "UpgradeStarted") {
			upgradeStartedFound = true
			break
		}
	}
	assert.True(s.T(), upgradeStartedFound,
		"UpgradeStarted event should be emitted; events: %v", events)

	// Verify upgrade annotation is set with previousImage and previousVersion.
	assert.NotEmpty(s.T(), updated.Annotations[util.AnnotationUpgrade],
		"upgrade annotation should be set")

	var state map[string]interface{}
	unmarshalErr := json.Unmarshal([]byte(updated.Annotations[util.AnnotationUpgrade]), &state)
	require.NoError(s.T(), unmarshalErr, "upgrade annotation should be valid JSON")
	assert.Equal(s.T(), scenario14OldImage, state["previousImage"],
		"previousImage should be the old image")
	assert.Equal(s.T(), scenario14OldVersion, state["previousVersion"],
		"previousVersion should be the old version")

	// Verify upgrade order: mirrors should be updated first.
	// Since all StatefulSets are ready, the upgrade should progress through all phases.
	// Run reconciliation multiple times to advance through phases.
	for i := 0; i < 10; i++ {
		_, err = reconciler.Reconcile(s.ctx, scenario14Req())
		require.NoError(s.T(), err)

		updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
		require.NoError(s.T(), err)
		if updated.Status.Phase == cbv1alpha1.ClusterPhaseRunning {
			break
		}
	}

	// Verify final state.
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after upgrade completes")
	assert.Equal(s.T(), scenario14NewVersion, updated.Status.ClusterVersion,
		"clusterVersion should be updated to new version")

	// Verify upgrade annotation removed.
	assert.Empty(s.T(), updated.Annotations[util.AnnotationUpgrade],
		"upgrade annotation should be removed after completion")

	// Verify UpgradeCompleted event.
	events = collectEvents(fakeRecorder)
	upgradeCompletedFound := false
	for _, event := range events {
		if containsSubstring(event, "UpgradeCompleted") {
			upgradeCompletedFound = true
			break
		}
	}
	assert.True(s.T(), upgradeCompletedFound,
		"UpgradeCompleted event should be emitted; events: %v", events)

	// Verify all StatefulSets have the new image.
	assert.Equal(s.T(), scenario14NewImage,
		s.getStatefulSetImage(util.CoordinatorName(cluster.Name), cluster.Namespace),
		"coordinator should have new image")
	assert.Equal(s.T(), scenario14NewImage,
		s.getStatefulSetImage(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should have new image")
	assert.Equal(s.T(), scenario14NewImage,
		s.getStatefulSetImage(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should have new image")
}

// --- Test: Upgrade Rollback ---

func (s *Scenario14UpgradeSuite) TestScenario14_UpgradeRollback() {
	// Arrange: create a Running cluster with old image.
	cluster := buildScenario14Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.ClusterVersion = scenario14OldVersion
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario14SegCount
	current.Status.SegmentsTotal = scenario14SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSets(cluster, scenario14OldImage)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario14Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Simulate an in-progress upgrade that has timed out in the coordinator phase.
	// Set the upgrade annotation with phaseStartedAt > 10 minutes ago.
	timedOutStart := time.Now().Add(-11 * time.Minute).Format(time.RFC3339)
	upgradeState := map[string]string{
		"previousImage":   scenario14OldImage,
		"previousVersion": scenario14OldVersion,
		"phase":           "coordinator",
		"startedAt":       timedOutStart,
		"phaseStartedAt":  timedOutStart,
	}
	stateJSON, err := json.Marshal(upgradeState)
	require.NoError(s.T(), err)

	// Set the upgrade annotation and Updating phase.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Patch annotation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.AnnotationUpgrade] = string(stateJSON)
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Also update the spec to have a new (broken) image.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Image = "postgres:broken"
	current.Spec.Version = "7.2.0-broken"
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Act: reconcile — should detect timeout and rollback.
	_, err = reconciler.Reconcile(s.ctx, scenario14Req())
	require.NoError(s.T(), err)

	// Verify rollback.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Phase should be back to Running.
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after rollback")

	// ClusterVersion should be the old version.
	assert.Equal(s.T(), scenario14OldVersion, updated.Status.ClusterVersion,
		"clusterVersion should be reverted to old version")

	// UpgradeFailed condition should be set.
	failedCond := util.FindCondition(updated.Status.Conditions, "UpgradeFailed")
	require.NotNil(s.T(), failedCond, "UpgradeFailed condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, failedCond.Status,
		"UpgradeFailed should be True")
	assert.Equal(s.T(), "RolledBack", failedCond.Reason,
		"UpgradeFailed reason should be RolledBack")

	// Upgrade annotation should be removed.
	assert.Empty(s.T(), updated.Annotations[util.AnnotationUpgrade],
		"upgrade annotation should be removed after rollback")

	// UpgradeRollback event should be emitted.
	events := collectEvents(fakeRecorder)
	rollbackFound := false
	for _, event := range events {
		if containsSubstring(event, "UpgradeRollback") {
			rollbackFound = true
			break
		}
	}
	assert.True(s.T(), rollbackFound,
		"UpgradeRollback event should be emitted; events: %v", events)

	// All StatefulSets should be reverted to the old image.
	assert.Equal(s.T(), scenario14OldImage,
		s.getStatefulSetImage(util.CoordinatorName(cluster.Name), cluster.Namespace),
		"coordinator should be reverted to old image")
	assert.Equal(s.T(), scenario14OldImage,
		s.getStatefulSetImage(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be reverted to old image")
	assert.Equal(s.T(), scenario14OldImage,
		s.getStatefulSetImage(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be reverted to old image")
}

// --- Test: Upgrade Blocked When Not Running ---

func (s *Scenario14UpgradeSuite) TestScenario14_UpgradeBlockedWhenNotRunning() {
	// Arrange: create a cluster in Stopped phase.
	cluster := testutil.NewClusterBuilder(scenario14ClusterName, scenario14Namespace).
		WithVersion(scenario14OldVersion).
		WithImage(scenario14OldImage).
		WithSegments(scenario14SegCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseStopped).
		WithPendingGeneration().
		Build()
	cluster.Status.ClusterVersion = scenario14OldVersion

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	current.Status.ClusterVersion = scenario14OldVersion
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Change version to trigger upgrade detection.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Version = scenario14NewVersion
	current.Spec.Image = scenario14NewImage
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario14Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile — should be blocked by Stopped phase (lifecycle handler).
	_, err = reconciler.Reconcile(s.ctx, scenario14Req())
	require.NoError(s.T(), err)

	// Verify phase is still Stopped (lifecycle handler short-circuits).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopped, updated.Status.Phase,
		"phase should remain Stopped")

	// Verify no upgrade annotation was set.
	assert.Empty(s.T(), updated.Annotations[util.AnnotationUpgrade],
		"upgrade annotation should not be set when cluster is Stopped")
}

// --- Test: No Upgrade When Version Unchanged ---

func (s *Scenario14UpgradeSuite) TestScenario14_NoUpgradeWhenVersionUnchanged() {
	// Arrange: create a Running cluster where spec.version matches status.clusterVersion.
	cluster := testutil.NewClusterBuilder(scenario14ClusterName, scenario14Namespace).
		WithVersion(scenario14OldVersion).
		WithImage(scenario14OldImage).
		WithSegments(scenario14SegCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Status.ClusterVersion = scenario14OldVersion

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.ClusterVersion = scenario14OldVersion
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = scenario14SegCount
	current.Status.SegmentsTotal = scenario14SegCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSets(cluster, scenario14OldImage)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario14Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile without changing version.
	_, err = reconciler.Reconcile(s.ctx, scenario14Req())
	require.NoError(s.T(), err)

	// Verify no upgrade was triggered.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Phase should not be Updating.
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseUpdating, updated.Status.Phase,
		"phase should not be Updating when version is unchanged")

	// No upgrade annotation should be set.
	assert.Empty(s.T(), updated.Annotations[util.AnnotationUpgrade],
		"upgrade annotation should not be set when version is unchanged")

	// No UpgradeStarted event.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "UpgradeStarted"),
			"UpgradeStarted event should not be emitted; events: %v", events)
	}
}

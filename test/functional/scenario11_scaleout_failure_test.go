//go:build functional

package functional

import (
	"context"
	"fmt"
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
	scenario11ClusterName = "scenario11-cluster"
	scenario11Namespace   = "cloudberry-test"
	scenario11InitCount   = int32(4)
	scenario11ScaleCount  = int32(6)
)

// Scenario11ScaleOutFailureSuite tests scale-out failure and rollback scenarios.
type Scenario11ScaleOutFailureSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario11_ScaleOutFailure(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario11ScaleOutFailureSuite))
}

func (s *Scenario11ScaleOutFailureSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario11ScaleOutFailureSuite) TearDownTest() {
	s.cancel()
}

// scenario11Req creates a reconcile request for the scenario 11 cluster.
func scenario11Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario11ClusterName,
			Namespace: scenario11Namespace,
		},
	}
}

// newScenario11Reconciler creates a ClusterReconciler for scenario 11 tests.
func newScenario11Reconciler(
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

// createReadyStatefulSet11 creates a StatefulSet with ready replicas.
func (s *Scenario11ScaleOutFailureSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
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

// createAllStatefulSets11 creates all StatefulSets for a cluster with ready replicas.
func (s *Scenario11ScaleOutFailureSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count,
	)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		s.createReadyStatefulSet(
			util.SegmentMirrorName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count,
		)
	}
}

// getStatefulSetReplicas11 returns the spec replicas for a StatefulSet.
func (s *Scenario11ScaleOutFailureSuite) getStatefulSetReplicas(name, namespace string) int32 {
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

// simulateSegmentsReady11 sets segment StatefulSet statuses to the desired ready replicas.
func (s *Scenario11ScaleOutFailureSuite) simulateSegmentsReady(
	cluster *cbv1alpha1.CloudberryCluster, count int32,
) {
	stsNames := []string{util.SegmentPrimaryName(cluster.Name)}
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		stsNames = append(stsNames, util.SegmentMirrorName(cluster.Name))
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
		sts.Status.Replicas = count
		sts.Status.ReadyReplicas = count
		err = s.env.Client.Status().Update(s.ctx, sts)
		require.NoError(s.T(), err, "updating statefulset %s status should succeed", name)
	}
}

// --- Test: Scale-Out Blocked When Not Running ---

func (s *Scenario11ScaleOutFailureSuite) TestScenario11a_ScaleOutBlockedWhenNotRunning() {
	// Arrange: create a Stopped cluster with 4 segments.
	cluster := testutil.NewClusterBuilder(scenario11ClusterName, scenario11Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario11InitCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseStopped).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create primary StatefulSet with 4 replicas.
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario11InitCount,
	)
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)

	// Update status to Stopped (fake client doesn't persist status from object creation).
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario11Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 6 and bump generation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario11ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation — Stopped phase should short-circuit via handleLifecyclePhase.
	// The reconciler won't reach reconcileSegments because Stopped is handled first.
	// So we need to test the pre-flight check directly by calling reconcileSegments
	// on a cluster that is NOT in Running phase but has a scale-out pending.
	// To do this, we set the phase to something that doesn't short-circuit (e.g., Initializing)
	// but is not Running.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	// The Stopped phase short-circuits in handleLifecyclePhase, so reconcileSegments
	// is never called. This is correct behavior — the cluster is stopped.
	// Let's verify the lifecycle handler returns without error.
	_, err = reconciler.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase stays Stopped (not Scaling).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseStopped, updated.Status.Phase,
		"phase should stay Stopped when cluster is not Running")

	// Verify StatefulSet replicas unchanged at 4.
	assert.Equal(s.T(), scenario11InitCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 4 when scale-out is blocked")

	// Now test the pre-flight check directly: set phase to Initializing (not Running)
	// and trigger reconcileSegments through a full reconcile.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder2 := record.NewFakeRecorder(100)
	reconciler2 := newScenario11Reconciler(s.env, fakeRecorder2, mockMetrics, s.logger)

	_, err = reconciler2.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify ScaleOutBlocked warning event.
	events := collectEvents(fakeRecorder2)
	scaleOutBlockedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleOutBlocked") {
			scaleOutBlockedFound = true
			break
		}
	}
	assert.True(s.T(), scaleOutBlockedFound,
		"ScaleOutBlocked event should be emitted; events: %v", events)

	// Verify phase is NOT Scaling.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when scale-out is blocked")

	// Verify StatefulSet replicas unchanged at 4.
	assert.Equal(s.T(), scenario11InitCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 4 when scale-out is blocked")
}

// --- Test: Scale-Out Failure Timeout ---

func (s *Scenario11ScaleOutFailureSuite) TestScenario11b_ScaleOutFailureTimeout() {
	// Arrange: create a Running cluster with 4 segments.
	cluster := testutil.NewClusterBuilder(scenario11ClusterName, scenario11Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario11InitCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create primary StatefulSet with 4 replicas (ready).
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario11InitCount,
	)

	// Update status to Running.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario11Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 6 and bump generation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario11ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation → triggers handleScaleOut → phase becomes Scaling.
	_, err = reconciler.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling after scale-out detected")

	// Verify scale-started annotation is set.
	assert.NotEmpty(s.T(), updated.Annotations[util.AnnotationScaleStarted],
		"scale-started annotation should be set")

	// Simulate timeout: set the scale-started annotation to a time > 10 minutes ago.
	pastTime := time.Now().Add(-15 * time.Minute).Format(time.RFC3339)
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.AnnotationScaleStarted] = pastTime
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "setting past scale-started annotation should succeed")

	// The primary StatefulSet was scaled to 6 but only 4 are ready.
	// Simulate that segments 4,5 are NOT ready by setting ReadyReplicas to 4.
	primarySts := &appsv1.StatefulSet{}
	err = s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primarySts)
	require.NoError(s.T(), err)
	primarySts.Status.Replicas = scenario11ScaleCount
	primarySts.Status.ReadyReplicas = scenario11InitCount // Only 4 of 6 ready.
	err = s.env.Client.Status().Update(s.ctx, primarySts)
	require.NoError(s.T(), err)

	// Run reconciliation again — should detect Scaling phase and check progress.
	fakeRecorder2 := record.NewFakeRecorder(100)
	reconciler2 := newScenario11Reconciler(s.env, fakeRecorder2, mockMetrics, s.logger)

	_, err = reconciler2.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	// Verify status.failedSegments contains entries for segments 4,5.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), updated.Status.FailedSegments,
		"failedSegments should not be empty after timeout")

	// Verify failed segments contain the expected content IDs.
	failedContentIDs := make(map[int32]bool)
	for _, fs := range updated.Status.FailedSegments {
		failedContentIDs[fs.ContentID] = true
	}
	assert.True(s.T(), failedContentIDs[4],
		"segment with contentID 4 should be in failedSegments")
	assert.True(s.T(), failedContentIDs[5],
		"segment with contentID 5 should be in failedSegments")

	// Verify all failed segments have role "primary" and status "NotReady".
	for _, fs := range updated.Status.FailedSegments {
		assert.Equal(s.T(), "primary", fs.Role,
			"failed segment role should be primary")
		assert.Equal(s.T(), "NotReady", fs.Status,
			"failed segment status should be NotReady")
	}

	// Verify Condition ScaleOutFailed is True with reason "SegmentsNotReady".
	scaleFailedCond := util.FindCondition(updated.Status.Conditions, "ScaleOutFailed")
	require.NotNil(s.T(), scaleFailedCond, "ScaleOutFailed condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, scaleFailedCond.Status,
		"ScaleOutFailed should be True")
	assert.Equal(s.T(), "SegmentsNotReady", scaleFailedCond.Reason,
		"ScaleOutFailed reason should be SegmentsNotReady")

	// Verify Warning event ScaleOutFailed emitted.
	events := collectEvents(fakeRecorder2)
	scaleOutFailedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleOutFailed") {
			scaleOutFailedFound = true
			break
		}
	}
	assert.True(s.T(), scaleOutFailedFound,
		"ScaleOutFailed event should be emitted; events: %v", events)

	// Verify phase stays Scaling (no automatic rollback).
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should stay Scaling after failure (no automatic rollback)")

	// Verify StatefulSet replicas NOT reverted to 4.
	assert.Equal(s.T(), scenario11ScaleCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should NOT be reverted to 4 after failure")
}

// --- Test: Pre-flight Also Blocks Scale-In ---

func (s *Scenario11ScaleOutFailureSuite) TestScenario11_PreflightAlsoBlocksScaleIn() {
	// Arrange: create a cluster in Initializing phase with 6 segments.
	cluster := testutil.NewClusterBuilder(scenario11ClusterName, scenario11Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario11ScaleCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseInitializing).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create primary StatefulSet with 6 replicas.
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario11ScaleCount,
	)

	// Update status to Initializing.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseInitializing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario11Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 4 (scale-in) and bump generation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario11InitCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify ScaleInBlocked warning event.
	events := collectEvents(fakeRecorder)
	scaleInBlockedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInBlocked") {
			scaleInBlockedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInBlockedFound,
		"ScaleInBlocked event should be emitted; events: %v", events)

	// Verify phase is NOT Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when scale-in is blocked")

	// Verify StatefulSet replicas unchanged at 6.
	assert.Equal(s.T(), scenario11ScaleCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 6 when scale-in is blocked")
}

// --- Test: Scale-Started Annotation Cleaned On Success ---

func (s *Scenario11ScaleOutFailureSuite) TestScenario11_ScaleStartedAnnotationCleanedOnSuccess() {
	// Arrange: create a cluster in Scaling phase with all segments ready at 6.
	cluster := testutil.NewClusterBuilder(scenario11ClusterName, scenario11Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario11ScaleCount).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseScaling).
		WithPendingGeneration().
		WithAnnotation(util.AnnotationScaleStarted, time.Now().Format(time.RFC3339)).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create StatefulSets with 6 replicas (already scaled).
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario11ScaleCount,
	)

	// Simulate all 6 primary pods ready.
	s.simulateSegmentsReady(cluster, scenario11ScaleCount)

	// Update status to Scaling with scale-started annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario11Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile — should detect Scaling phase and check progress.
	_, err = reconciler.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify scale-started annotation is removed.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasAnnotation := updated.Annotations[util.AnnotationScaleStarted]
	assert.False(s.T(), hasAnnotation,
		"scale-started annotation should be removed after successful scale completion")

	// Verify phase -> Running.
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after scale-out completes")

	// Verify ScaleOutCompleted event.
	events := collectEvents(fakeRecorder)
	scaleCompletedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleOutCompleted") {
			scaleCompletedFound = true
			break
		}
	}
	assert.True(s.T(), scaleCompletedFound,
		"ScaleOutCompleted event should be emitted; events: %v", events)
}

// --- Test: Scale-Out Failure With Mirroring ---

func (s *Scenario11ScaleOutFailureSuite) TestScenario11_ScaleOutFailureWithMirroring() {
	// Arrange: create a Running cluster with 4 segments + mirroring.
	cluster := testutil.NewClusterBuilder(scenario11ClusterName, scenario11Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario11InitCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create all StatefulSets with 4 replicas (ready).
	s.createAllStatefulSets(cluster)

	// Update status to Running.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario11Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 6 and bump generation.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario11ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation → triggers handleScaleOut → phase becomes Scaling.
	_, err = reconciler.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling after scale-out detected")

	// Simulate timeout: set the scale-started annotation to a time > 10 minutes ago.
	pastTime := time.Now().Add(-15 * time.Minute).Format(time.RFC3339)
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if current.Annotations == nil {
		current.Annotations = make(map[string]string)
	}
	current.Annotations[util.AnnotationScaleStarted] = pastTime
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Simulate that primary segments 4,5 are NOT ready (only 4 of 6 ready).
	primarySts := &appsv1.StatefulSet{}
	err = s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.SegmentPrimaryName(cluster.Name),
		Namespace: cluster.Namespace,
	}, primarySts)
	require.NoError(s.T(), err)
	primarySts.Status.Replicas = scenario11ScaleCount
	primarySts.Status.ReadyReplicas = scenario11InitCount
	err = s.env.Client.Status().Update(s.ctx, primarySts)
	require.NoError(s.T(), err)

	// Simulate that mirror segments 4,5 are NOT ready (only 4 of 6 ready).
	mirrorSts := &appsv1.StatefulSet{}
	err = s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.SegmentMirrorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, mirrorSts)
	require.NoError(s.T(), err)
	mirrorSts.Status.Replicas = scenario11ScaleCount
	mirrorSts.Status.ReadyReplicas = scenario11InitCount
	err = s.env.Client.Status().Update(s.ctx, mirrorSts)
	require.NoError(s.T(), err)

	// Run reconciliation again — should detect timeout and mark as failed.
	fakeRecorder2 := record.NewFakeRecorder(100)
	reconciler2 := newScenario11Reconciler(s.env, fakeRecorder2, mockMetrics, s.logger)

	_, err = reconciler2.Reconcile(s.ctx, scenario11Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	// Verify status.failedSegments contains entries for both primary and mirror.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), updated.Status.FailedSegments,
		"failedSegments should not be empty after timeout")

	// Count primary and mirror failures.
	primaryFailures := 0
	mirrorFailures := 0
	for _, fs := range updated.Status.FailedSegments {
		switch fs.Role {
		case "primary":
			primaryFailures++
		case "mirror":
			mirrorFailures++
		}
	}
	assert.Equal(s.T(), 2, primaryFailures,
		"should have 2 primary segment failures (segments 4,5)")
	assert.Equal(s.T(), 2, mirrorFailures,
		"should have 2 mirror segment failures (segments 4,5)")

	// Verify ScaleOutFailed condition.
	scaleFailedCond := util.FindCondition(updated.Status.Conditions, "ScaleOutFailed")
	require.NotNil(s.T(), scaleFailedCond, "ScaleOutFailed condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, scaleFailedCond.Status,
		"ScaleOutFailed should be True")

	// Verify the message mentions the correct number of failed segments.
	assert.Contains(s.T(), scaleFailedCond.Message,
		fmt.Sprintf("%d segments not ready", len(updated.Status.FailedSegments)),
		"ScaleOutFailed message should mention the number of failed segments")
}

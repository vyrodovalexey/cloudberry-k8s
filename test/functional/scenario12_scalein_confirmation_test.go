//go:build functional

package functional

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario12ClusterName = "scenario12-cluster"
	scenario12Namespace   = "cloudberry-test"
	scenario12InitCount   = int32(8)
	scenario12ScaleCount  = int32(3)
)

// Scenario12ScaleInConfirmationSuite tests the >50% scale-in confirmation requirement.
type Scenario12ScaleInConfirmationSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario12_ScaleInConfirmation(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario12ScaleInConfirmationSuite))
}

func (s *Scenario12ScaleInConfirmationSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario12ScaleInConfirmationSuite) TearDownTest() {
	s.cancel()
}

// buildScenario12Cluster constructs a Running cluster with the given segment count + mirroring.
func buildScenario12Cluster(segmentCount int32) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario12ClusterName, scenario12Namespace).
		WithVersion("7.1.0").
		WithSegments(segmentCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario12Req creates a reconcile request for the scenario 12 cluster.
func scenario12Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario12ClusterName,
			Namespace: scenario12Namespace,
		},
	}
}

// newScenario12Reconciler creates a ClusterReconciler for scenario 12 tests.
func newScenario12Reconciler(
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

// createReadyStatefulSet12 creates a StatefulSet with ready replicas.
func (s *Scenario12ScaleInConfirmationSuite) createReadyStatefulSet(
	name, namespace string, replicas int32,
) {
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

// createAllStatefulSets12 creates all StatefulSets for a cluster with ready replicas.
func (s *Scenario12ScaleInConfirmationSuite) createAllStatefulSets(
	cluster *cbv1alpha1.CloudberryCluster,
) {
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

// getStatefulSetReplicas12 returns the spec replicas for a StatefulSet.
func (s *Scenario12ScaleInConfirmationSuite) getStatefulSetReplicas(
	name, namespace string,
) int32 {
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

// simulateSegmentsReady12 sets segment StatefulSet statuses to the desired ready replicas.
func (s *Scenario12ScaleInConfirmationSuite) simulateSegmentsReady(
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

// createSegmentPVCs12 creates PVCs for segments 0..count-1 for both primary and mirror.
func (s *Scenario12ScaleInConfirmationSuite) createSegmentPVCs(
	cluster *cbv1alpha1.CloudberryCluster, count int32,
) {
	components := []string{util.ComponentSegmentPrimary, util.ComponentSegmentMirror}
	for _, component := range components {
		stsName := util.SanitizeK8sName(fmt.Sprintf("%s-%s", cluster.Name, component))
		for i := int32(0); i < count; i++ {
			pvcName := fmt.Sprintf("data-%s-%d", stsName, i)
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pvcName,
					Namespace: cluster.Namespace,
					Labels: map[string]string{
						util.LabelCluster:   cluster.Name,
						util.LabelComponent: component,
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("20Gi"),
						},
					},
				},
			}
			err := s.env.Client.Create(s.ctx, pvc)
			require.NoError(s.T(), err, "creating PVC %s should succeed", pvcName)
		}
	}
}

// --- Test: Scale-In Blocked Without 50% Confirmation (8→3, 62.5% reduction) ---

func (s *Scenario12ScaleInConfirmationSuite) TestScenario12a_ScaleInBlockedWithout50PercentConfirmation() {
	// Arrange: create a Running cluster with 8 segments + mirroring.
	cluster := buildScenario12Cluster(scenario12InitCount)
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario12Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 3 (62.5% reduction, >50%) WITHOUT confirmation annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario12ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario12Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify ScaleInBlocked warning event emitted.
	events := collectEvents(fakeRecorder)
	scaleInBlockedFound := false
	var blockedEventMsg string
	for _, event := range events {
		if containsSubstring(event, "ScaleInBlocked") {
			scaleInBlockedFound = true
			blockedEventMsg = event
			break
		}
	}
	assert.True(s.T(), scaleInBlockedFound,
		"ScaleInBlocked event should be emitted; events: %v", events)

	// Verify event message contains the annotation requirement.
	assert.Contains(s.T(), blockedEventMsg, util.AnnotationConfirmScaleIn,
		"ScaleInBlocked event should mention the required annotation")

	// Verify phase stays Running (NOT Scaling).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when scale-in is blocked")

	// Verify primary StatefulSet replicas unchanged at 8.
	assert.Equal(s.T(), scenario12InitCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 8 when scale-in is blocked")

	// Verify mirror StatefulSet replicas unchanged at 8.
	assert.Equal(s.T(), scenario12InitCount,
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should remain at 8 when scale-in is blocked")

	// Verify no redistribution Job created.
	jobList := &batchv1.JobList{}
	err = s.env.Client.List(s.ctx, jobList, client.InNamespace(scenario12Namespace))
	require.NoError(s.T(), err, "listing jobs should succeed")
	redistributeJobFound := false
	for i := range jobList.Items {
		if containsSubstring(jobList.Items[i].Name, "redistribute") {
			redistributeJobFound = true
			break
		}
	}
	assert.False(s.T(), redistributeJobFound,
		"no redistribution Job should be created when scale-in is blocked")
}

// --- Test: Scale-In Proceeds With Confirmation (8→3) ---

func (s *Scenario12ScaleInConfirmationSuite) TestScenario12b_ScaleInProceedsWithConfirmation() {
	// Arrange: create a Running cluster with 8 segments + mirroring + confirmation annotation.
	cluster := testutil.NewClusterBuilder(scenario12ClusterName, scenario12Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario12InitCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithAnnotation(util.AnnotationConfirmScaleIn, "true").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario12Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch segments.count to 3 (62.5% reduction) WITH confirmation annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario12ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario12Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase → Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling when scale-in is confirmed")

	// Verify ScaleInStarted event emitted (from 8 to 3).
	events := collectEvents(fakeRecorder)
	scaleInStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInStarted") {
			scaleInStartedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInStartedFound,
		"ScaleInStarted event should be emitted; events: %v", events)

	// Verify primary StatefulSet replicas updated to 3.
	assert.Equal(s.T(), scenario12ScaleCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 3")

	// Verify mirror StatefulSet replicas updated to 3.
	assert.Equal(s.T(), scenario12ScaleCount,
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 3")

	// Verify redistribution Job created.
	jobList := &batchv1.JobList{}
	err = s.env.Client.List(s.ctx, jobList, client.InNamespace(scenario12Namespace))
	require.NoError(s.T(), err, "listing jobs should succeed")
	redistributeJobFound := false
	for i := range jobList.Items {
		if containsSubstring(jobList.Items[i].Name, "redistribute") {
			redistributeJobFound = true
			break
		}
	}
	assert.True(s.T(), redistributeJobFound,
		"a redistribution Job should be created; jobs: %v", jobNames(jobList))

	// Verify DataRedistribution condition set to InProgress.
	redistCond := util.FindCondition(updated.Status.Conditions, "DataRedistribution")
	require.NotNil(s.T(), redistCond, "DataRedistribution condition should exist")
	assert.Equal(s.T(), "InProgress", redistCond.Reason,
		"DataRedistribution reason should be InProgress")

	// Verify scale-started annotation set.
	assert.NotEmpty(s.T(), updated.Annotations[util.AnnotationScaleStarted],
		"scale-started annotation should be set")
}

// --- Test: Scale-In Completion Cleans Confirmation Annotation ---

func (s *Scenario12ScaleInConfirmationSuite) TestScenario12_ScaleInCompletionCleansConfirmation() {
	// Arrange: create a cluster in Scaling phase with 3 desired segments,
	// simulating the state after TestScenario12b completes the scale-in initiation.
	cluster := testutil.NewClusterBuilder(scenario12ClusterName, scenario12Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario12ScaleCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseScaling).
		WithPendingGeneration().
		WithAnnotation(util.AnnotationConfirmScaleIn, "true").
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create StatefulSets with 3 replicas (already scaled down).
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario12ScaleCount,
	)
	s.createReadyStatefulSet(
		util.SegmentMirrorName(cluster.Name), cluster.Namespace, scenario12ScaleCount,
	)

	// Simulate all StatefulSets ready at 3 replicas.
	s.simulateSegmentsReady(cluster, scenario12ScaleCount)

	// Set SegmentsTotal to 8 to simulate scale-in from 8 to 3.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.SegmentsTotal = scenario12InitCount
	current.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster status should succeed")

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario12Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile — should detect Scaling phase and check progress.
	_, err = reconciler.Reconcile(s.ctx, scenario12Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase → Running.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after scale-in completes")

	// Verify ScaleInCompleted event.
	events := collectEvents(fakeRecorder)
	scaleInCompletedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInCompleted") {
			scaleInCompletedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInCompletedFound,
		"ScaleInCompleted event should be emitted; events: %v", events)

	// Verify avsoft.io/confirm-scale-in annotation removed.
	_, hasConfirmAnnotation := updated.Annotations[util.AnnotationConfirmScaleIn]
	assert.False(s.T(), hasConfirmAnnotation,
		"confirm-scale-in annotation should be removed after successful scale-in completion")

	// Verify avsoft.io/scale-started annotation removed.
	_, hasScaleStarted := updated.Annotations[util.AnnotationScaleStarted]
	assert.False(s.T(), hasScaleStarted,
		"scale-started annotation should be removed after successful scale-in completion")

	// Verify segmentsReady=3, segmentsTotal=3.
	assert.Equal(s.T(), scenario12ScaleCount, updated.Status.SegmentsReady,
		"segmentsReady should be 3")
	assert.Equal(s.T(), scenario12ScaleCount, updated.Status.SegmentsTotal,
		"segmentsTotal should be 3")
}

// --- Test: Exactly At 50% Not Blocked ---

func (s *Scenario12ScaleInConfirmationSuite) TestScenario12_ExactlyAt50PercentNotBlocked() {
	// Arrange: create a Running cluster with 8 segments + mirroring (no confirmation annotation).
	cluster := buildScenario12Cluster(scenario12InitCount)
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario12Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch to 4 segments (exactly 50% — 4/8 = 0.5, check is < 0.5, so NOT blocked).
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = 4
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario12Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase → Scaling (scale-in proceeds without confirmation).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling — exactly 50% should NOT be blocked")

	// Verify ScaleInStarted event emitted.
	events := collectEvents(fakeRecorder)
	scaleInStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInStarted") {
			scaleInStartedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInStartedFound,
		"ScaleInStarted event should be emitted for exactly 50%% reduction; events: %v", events)

	// Verify no ScaleInBlocked event.
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "ScaleInBlocked"),
			"ScaleInBlocked event should NOT be emitted for exactly 50%% reduction; events: %v", events)
	}

	// Verify StatefulSets scaled to 4.
	assert.Equal(s.T(), int32(4),
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 4")
	assert.Equal(s.T(), int32(4),
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 4")
}

// --- Test: Just Over 50% Blocked ---

func (s *Scenario12ScaleInConfirmationSuite) TestScenario12_JustOver50PercentBlocked() {
	// Arrange: create a Running cluster with 10 segments + mirroring (no confirmation annotation).
	cluster := testutil.NewClusterBuilder(scenario12ClusterName, scenario12Namespace).
		WithVersion("7.1.0").
		WithSegments(10).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario12Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch to 4 segments (60% reduction, >50%) WITHOUT confirmation annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = 4
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario12Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify ScaleInBlocked warning event emitted.
	events := collectEvents(fakeRecorder)
	scaleInBlockedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInBlocked") {
			scaleInBlockedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInBlockedFound,
		"ScaleInBlocked event should be emitted for >50%% reduction without confirmation; events: %v", events)

	// Verify phase stays Running (NOT Scaling).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when scale-in is blocked")

	// Verify StatefulSets unchanged at 10.
	assert.Equal(s.T(), int32(10),
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 10 when scale-in is blocked")
	assert.Equal(s.T(), int32(10),
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should remain at 10 when scale-in is blocked")
}

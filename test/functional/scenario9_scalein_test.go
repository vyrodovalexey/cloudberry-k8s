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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	scenario9ClusterName = "scenario9-cluster"
	scenario9Namespace   = "cloudberry-test"
	scenario9InitCount   = int32(6)
	scenario9ScaleCount  = int32(4)
)

// Scenario9ScaleInSuite tests scale-in with both PVC policies.
type Scenario9ScaleInSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario9_ScaleIn(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario9ScaleInSuite))
}

func (s *Scenario9ScaleInSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario9ScaleInSuite) TearDownTest() {
	s.cancel()
}

// buildScenario9Cluster constructs a Running cluster with 6 segments + mirroring.
func buildScenario9Cluster(deletionPolicy cbv1alpha1.DeletionPolicy) *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario9ClusterName, scenario9Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario9InitCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(deletionPolicy).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario9Req creates a reconcile request for the scenario 9 cluster.
func scenario9Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario9ClusterName,
			Namespace: scenario9Namespace,
		},
	}
}

// createReadyStatefulSet9 creates a StatefulSet with ready replicas.
func (s *Scenario9ScaleInSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
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

// createAllStatefulSets9 creates all StatefulSets for a cluster with ready replicas.
func (s *Scenario9ScaleInSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
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

// createSegmentPVCs creates PVCs for segments 0..count-1 for both primary and mirror.
func (s *Scenario9ScaleInSuite) createSegmentPVCs(cluster *cbv1alpha1.CloudberryCluster, count int32) {
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

// getStatefulSetReplicas9 returns the spec replicas for a StatefulSet.
func (s *Scenario9ScaleInSuite) getStatefulSetReplicas(name, namespace string) int32 {
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

// simulateSegmentsReady9 sets segment StatefulSet statuses to the desired ready replicas.
func (s *Scenario9ScaleInSuite) simulateSegmentsReady(
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

// pvcExists checks whether a PVC exists.
func (s *Scenario9ScaleInSuite) pvcExists(name, namespace string) bool {
	pvc := &corev1.PersistentVolumeClaim{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, pvc)
	return err == nil
}

// newScenario9Reconciler creates a ClusterReconciler for scenario 9 tests.
func newScenario9Reconciler(
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

// --- Test: Scale-In with Retain Policy ---

func (s *Scenario9ScaleInSuite) TestScenario9a_ScaleInRetainPolicy() {
	// Arrange: create a Running cluster with 6 segments + mirroring, deletionPolicy=Retain.
	cluster := buildScenario9Cluster(cbv1alpha1.DeletionPolicyRetain)
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)
	s.createSegmentPVCs(cluster, scenario9InitCount)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario9Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch cluster spec to segments.count=4 and bump generation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario9ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation — should detect scale-in.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling after scale-in detected")

	// Verify ScaleInStarted event.
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

	// Verify mirror StatefulSet scaled to 4.
	assert.Equal(s.T(), scenario9ScaleCount,
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 4")

	// Verify primary StatefulSet scaled to 4.
	assert.Equal(s.T(), scenario9ScaleCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 4")

	// Simulate pods ready at new count.
	s.simulateSegmentsReady(updated, scenario9ScaleCount)

	// Update cluster status to reflect the old total (6) so checkScaleProgress
	// can detect this was a scale-in.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Status.SegmentsTotal = scenario9InitCount
	err = s.env.Client.Status().Update(s.ctx, updated)
	require.NoError(s.T(), err, "updating cluster status should succeed")

	// Run reconciliation again — should detect Scaling phase and check progress.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	// Verify phase -> Running.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after scale-in completes")

	// Verify ScaleInCompleted event.
	events = collectEvents(fakeRecorder)
	scaleInCompletedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleInCompleted") {
			scaleInCompletedFound = true
			break
		}
	}
	assert.True(s.T(), scaleInCompletedFound,
		"ScaleInCompleted event should be emitted; events: %v", events)

	// Verify segmentsReady=4, segmentsTotal=4.
	assert.Equal(s.T(), scenario9ScaleCount, updated.Status.SegmentsReady,
		"segmentsReady should be 4")
	assert.Equal(s.T(), scenario9ScaleCount, updated.Status.SegmentsTotal,
		"segmentsTotal should be 4")

	// Verify PVCs for segments 4,5 still exist (Retain policy).
	primaryStsName := util.SanitizeK8sName(
		fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentPrimary),
	)
	mirrorStsName := util.SanitizeK8sName(
		fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentMirror),
	)
	for i := scenario9ScaleCount; i < scenario9InitCount; i++ {
		assert.True(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", primaryStsName, i), cluster.Namespace),
			"primary PVC for segment %d should still exist with Retain policy", i)
		assert.True(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", mirrorStsName, i), cluster.Namespace),
			"mirror PVC for segment %d should still exist with Retain policy", i)
	}
}

// --- Test: Scale-In with Delete Policy ---

func (s *Scenario9ScaleInSuite) TestScenario9b_ScaleInDeletePolicy() {
	// Arrange: create a Running cluster with 6 segments + mirroring, deletionPolicy=Delete.
	cluster := buildScenario9Cluster(cbv1alpha1.DeletionPolicyDelete)
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)
	s.createSegmentPVCs(cluster, scenario9InitCount)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario9Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch cluster spec to segments.count=4 and bump generation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario9ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation — should detect scale-in.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling after scale-in detected")

	// Simulate pods ready at new count.
	s.simulateSegmentsReady(updated, scenario9ScaleCount)

	// Update cluster status to reflect the old total (6) so checkScaleProgress
	// can detect this was a scale-in.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Status.SegmentsTotal = scenario9InitCount
	err = s.env.Client.Status().Update(s.ctx, updated)
	require.NoError(s.T(), err, "updating cluster status should succeed")

	// Run reconciliation again — should complete scale-in.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "second reconciliation should succeed")

	// Verify phase -> Running.
	updated, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after scale-in completes")

	// Verify PVCs for segments 4,5 are deleted (Delete policy).
	primaryStsName := util.SanitizeK8sName(
		fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentPrimary),
	)
	mirrorStsName := util.SanitizeK8sName(
		fmt.Sprintf("%s-%s", cluster.Name, util.ComponentSegmentMirror),
	)
	for i := scenario9ScaleCount; i < scenario9InitCount; i++ {
		assert.False(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", primaryStsName, i), cluster.Namespace),
			"primary PVC for segment %d should be deleted with Delete policy", i)
		assert.False(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", mirrorStsName, i), cluster.Namespace),
			"mirror PVC for segment %d should be deleted with Delete policy", i)
	}

	// Verify PVCs for segments 0-3 still exist.
	for i := int32(0); i < scenario9ScaleCount; i++ {
		assert.True(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", primaryStsName, i), cluster.Namespace),
			"primary PVC for segment %d should still exist", i)
		assert.True(s.T(),
			s.pvcExists(fmt.Sprintf("data-%s-%d", mirrorStsName, i), cluster.Namespace),
			"mirror PVC for segment %d should still exist", i)
	}
}

// --- Test: Scale-In Blocked Without 50% Confirmation ---

func (s *Scenario9ScaleInSuite) TestScenario9_ScaleInBlockedWithout50PercentConfirmation() {
	// Arrange: create a Running cluster with 6 segments.
	cluster := buildScenario9Cluster(cbv1alpha1.DeletionPolicyRetain)
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario9Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch to 2 segments (>50% reduction) WITHOUT confirmation annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = 2
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
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

	// Verify no scale-down occurs — StatefulSets should remain at 6.
	assert.Equal(s.T(), scenario9InitCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should remain at 6 when scale-in is blocked")
	assert.Equal(s.T(), scenario9InitCount,
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should remain at 6 when scale-in is blocked")

	// Verify phase is NOT Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when scale-in is blocked")
}

// --- Test: Scale-In With Confirmation ---

func (s *Scenario9ScaleInSuite) TestScenario9_ScaleInWithConfirmation() {
	// Arrange: create a Running cluster with 6 segments + confirmation annotation.
	cluster := testutil.NewClusterBuilder(scenario9ClusterName, scenario9Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario9InitCount).
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
	reconciler := newScenario9Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch to 2 segments (>50% reduction) WITH confirmation annotation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = 2
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling (scale-in proceeds).
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling when scale-in is confirmed")

	// Verify ScaleInStarted event.
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

	// Verify StatefulSets scaled to 2.
	assert.Equal(s.T(), int32(2),
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 2")
	assert.Equal(s.T(), int32(2),
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 2")
}

// --- Test: Scale-In Metrics ---

func (s *Scenario9ScaleInSuite) TestScenario9_ScaleMetrics() {
	// Arrange: create a cluster in Scaling phase with all segments ready at 4.
	cluster := testutil.NewClusterBuilder(scenario9ClusterName, scenario9Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario9ScaleCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseScaling).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create StatefulSets with 4 replicas (already scaled down).
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(
		util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario9ScaleCount,
	)
	s.createReadyStatefulSet(
		util.SegmentMirrorName(cluster.Name), cluster.Namespace, scenario9ScaleCount,
	)
	s.simulateSegmentsReady(cluster, scenario9ScaleCount)

	// Set SegmentsTotal to 6 to simulate scale-in from 6 to 4.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.SegmentsTotal = scenario9InitCount
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster status should succeed")

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario9Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile to complete scale-in.
	_, err = reconciler.Reconcile(s.ctx, scenario9Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify RecordScaleOperation called with "scale-in".
	require.Len(s.T(), mockMetrics.ScaleOperationCalls, 1,
		"RecordScaleOperation should be called once")
	assert.Equal(s.T(), "scale-in", mockMetrics.ScaleOperationCalls[0].Operation,
		"operation should be scale-in")
	assert.Equal(s.T(), scenario9ClusterName, mockMetrics.ScaleOperationCalls[0].Cluster,
		"cluster name should match")
	assert.Equal(s.T(), scenario9Namespace, mockMetrics.ScaleOperationCalls[0].Namespace,
		"namespace should match")
}

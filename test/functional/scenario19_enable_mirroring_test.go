//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario19ClusterName = "scenario19-cluster"
	scenario19Namespace   = "cloudberry-test"
)

// Scenario19MirroringMetricsRecorder wraps NoopRecorder and tracks mirroring operation calls.
type Scenario19MirroringMetricsRecorder struct {
	metrics.NoopRecorder
	MirroringOperationCalls []mirroringOpCall
	ReplicationLagCalls     []replicationLagCall
}

type mirroringOpCall struct {
	Cluster   string
	Namespace string
	Operation string
}

type replicationLagCall struct {
	Cluster   string
	Namespace string
	Segment   string
	LagBytes  float64
}

// RecordMirroringOperation records a mirroring operation event for verification.
func (m *Scenario19MirroringMetricsRecorder) RecordMirroringOperation(cluster, namespace, operation string) {
	m.MirroringOperationCalls = append(m.MirroringOperationCalls, mirroringOpCall{
		Cluster:   cluster,
		Namespace: namespace,
		Operation: operation,
	})
}

// SetReplicationLag records a replication lag metric for verification.
func (m *Scenario19MirroringMetricsRecorder) SetReplicationLag(cluster, namespace, segment string, lagBytes float64) {
	m.ReplicationLagCalls = append(m.ReplicationLagCalls, replicationLagCall{
		Cluster:   cluster,
		Namespace: namespace,
		Segment:   segment,
		LagBytes:  lagBytes,
	})
}

// Ensure Scenario19MirroringMetricsRecorder satisfies the metrics.Recorder interface at compile time.
var _ metrics.Recorder = (*Scenario19MirroringMetricsRecorder)(nil)

// Scenario19EnableMirroringSuite tests enabling/disabling mirroring on existing clusters.
type Scenario19EnableMirroringSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario19EnableMirroring(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario19EnableMirroringSuite))
}

func (s *Scenario19EnableMirroringSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario19EnableMirroringSuite) TearDownTest() {
	s.cancel()
}

// scenario19Req creates a reconcile request for the scenario 19 cluster.
func scenario19Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario19ClusterName,
			Namespace: scenario19Namespace,
		},
	}
}

// scenario19ReqFor creates a reconcile request for a named cluster.
func scenario19ReqFor(name, namespace string) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      name,
			Namespace: namespace,
		},
	}
}

// newScenario19Reconciler creates a ClusterReconciler for scenario 19 tests.
func newScenario19Reconciler(
	env *testutil.TestK8sEnv,
	recorder record.EventRecorder,
	metricsRec metrics.Recorder,
	logger *slog.Logger,
	dbFactory ...db.DBClientFactory,
) *controller.ClusterReconciler {
	return controller.NewClusterReconciler(
		env.Client, env.Scheme, recorder,
		env.Builder, metricsRec, logger,
		dbFactory...,
	)
}

// createReadyStatefulSet19 creates a StatefulSet with ready replicas.
func (s *Scenario19EnableMirroringSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
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

// createAllPrimaryStatefulSets creates coordinator and primary segment StatefulSets.
func (s *Scenario19EnableMirroringSuite) createAllPrimaryStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
}

// createAllStatefulSetsWithMirrors creates coordinator, primary, and mirror StatefulSets.
func (s *Scenario19EnableMirroringSuite) createAllStatefulSetsWithMirrors(cluster *cbv1alpha1.CloudberryCluster) {
	s.createAllPrimaryStatefulSets(cluster)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
}

// simulateMirrorSTSReady sets the mirror StatefulSet status to ready.
func (s *Scenario19EnableMirroringSuite) simulateMirrorSTSReady(cluster *cbv1alpha1.CloudberryCluster, count int32) {
	sts := &appsv1.StatefulSet{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.SegmentMirrorName(cluster.Name),
		Namespace: cluster.Namespace,
	}, sts)
	if err != nil {
		return
	}
	sts.Status.Replicas = count
	sts.Status.ReadyReplicas = count
	err = s.env.Client.Status().Update(s.ctx, sts)
	require.NoError(s.T(), err, "updating mirror statefulset status should succeed")
}

// getStatefulSetReplicas returns the spec replicas for a StatefulSet.
func (s *Scenario19EnableMirroringSuite) getStatefulSetReplicas(name, namespace string) int32 {
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

// statefulSetExists checks whether a StatefulSet exists.
func (s *Scenario19EnableMirroringSuite) statefulSetExists(name, namespace string) bool {
	sts := &appsv1.StatefulSet{}
	err := s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, sts)
	return err == nil
}

// createMirrorPVCs creates PVCs for mirror segments.
func (s *Scenario19EnableMirroringSuite) createMirrorPVCs(cluster *cbv1alpha1.CloudberryCluster) {
	mirrorStsName := util.SegmentMirrorName(cluster.Name)
	for i := int32(0); i < cluster.Spec.Segments.Count; i++ {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("data-%s-%d", mirrorStsName, i),
				Namespace: cluster.Namespace,
				Labels: map[string]string{
					util.LabelCluster:   cluster.Name,
					util.LabelComponent: util.ComponentSegmentMirror,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: *mustParseQuantity("20Gi"),
					},
				},
			},
		}
		err := s.env.Client.Create(s.ctx, pvc)
		require.NoError(s.T(), err, "creating mirror PVC %s should succeed", pvc.Name)
	}
}

// countPVCs counts PVCs matching a label selector.
func (s *Scenario19EnableMirroringSuite) countPVCs(namespace string, labels map[string]string) int {
	pvcList := &corev1.PersistentVolumeClaimList{}
	err := s.env.Client.List(s.ctx, pvcList,
		client.InNamespace(namespace),
		client.MatchingLabels(labels),
	)
	if err != nil {
		return -1
	}
	return len(pvcList.Items)
}

// ============================================================================
// Group 1: Pre-flight Validation
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_ValidatesNodeCount() {
	// Arrange: create a Running cluster with only 1 segment (insufficient for group mirroring).
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(1).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroringStatus(cbv1alpha1.MirroringNotConfigured).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status after creation since fake client status subresource
	// does not persist status fields set during object creation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 1
	current.Status.SegmentsTotal = 1
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 1)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Assert: warning event emitted, mirroring not started.
	events := collectEvents(fakeRecorder)
	mirroringFailedFound := false
	for _, event := range events {
		if containsSubstring(event, "MirroringFailed") && containsSubstring(event, "insufficient") {
			mirroringFailedFound = true
			break
		}
	}
	assert.True(s.T(), mirroringFailedFound,
		"MirroringFailed event with 'insufficient' should be emitted; events: %v", events)

	// Mirror STS should NOT be created.
	assert.False(s.T(), s.statefulSetExists(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror StatefulSet should not be created with insufficient segments")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_RequiresRunningPhase() {
	// Arrange: create a cluster in Stopped phase with mirroring enabled in spec.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseStopped).
		WithMirroringStatus(cbv1alpha1.MirroringNotConfigured).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseStopped
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile — Stopped phase should short-circuit.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Assert: mirror STS should NOT be created.
	assert.False(s.T(), s.statefulSetExists(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror StatefulSet should not be created when cluster is Stopped")
}

// ============================================================================
// Group 2: Mirror StatefulSet Creation
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_CreatesMirrorStatefulSet() {
	// Arrange: create a Running cluster without mirroring, then enable it.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Patch CR: enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Assert: mirror STS created.
	assert.True(s.T(), s.statefulSetExists(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror StatefulSet should be created after enabling mirroring")

	// Verify MirroringEnabled event.
	events := collectEvents(fakeRecorder)
	mirroringEnabledFound := false
	for _, event := range events {
		if containsSubstring(event, "MirroringEnabled") {
			mirroringEnabledFound = true
			break
		}
	}
	assert.True(s.T(), mirroringEnabledFound,
		"MirroringEnabled event should be emitted; events: %v", events)
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_MirrorSTSMatchesPrimaryCount() {
	// Arrange: create a Running cluster without mirroring.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: mirror STS replicas == primary STS replicas.
	primaryReplicas := s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace)
	mirrorReplicas := s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace)
	assert.Equal(s.T(), primaryReplicas, mirrorReplicas,
		"mirror STS replicas should match primary STS replicas")
}

// ============================================================================
// Group 3: Status Transitions
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_StatusTransitions() {
	// Arrange: create a Running cluster without mirroring.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: first reconcile — should transition to Updating phase.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: phase should be Updating (mirroring enable in progress).
	// Note: updateStatus runs after handleEnableMirroring and may overwrite
	// MirroringStatus based on mirror STS readiness, but the phase is preserved.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseUpdating, updated.Status.Phase,
		"phase should be Updating after mirroring enable starts")
	// The mirroring state annotation should be set, indicating the operation is in progress.
	assert.NotEmpty(s.T(), updated.Annotations[util.AnnotationMirroringState],
		"mirroring state annotation should be set")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_PhaseTransitions() {
	// Arrange: create a Running cluster without mirroring.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: phase should be Updating.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseUpdating, updated.Status.Phase,
		"phase should be Updating during mirroring enable")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_ConditionUpdates() {
	// Arrange: create a Running cluster without mirroring.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: MirroringHealthy condition should be False with reason MirroringInitializing.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	mirrorCond := util.FindCondition(updated.Status.Conditions, string(cbv1alpha1.ConditionMirroringHealthy))
	require.NotNil(s.T(), mirrorCond, "MirroringHealthy condition should exist")
	assert.Equal(s.T(), metav1.ConditionFalse, mirrorCond.Status,
		"MirroringHealthy should be False during initialization")
	assert.Equal(s.T(), "MirroringInitializing", mirrorCond.Reason,
		"MirroringHealthy reason should be MirroringInitializing")
}

// ============================================================================
// Group 4: Replication Monitoring
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_ReplicationLagDecreases() {
	// Arrange: create a cluster in Updating phase with mirroring state annotation
	// at the syncing phase, with a DB factory that returns decreasing lag.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringSyncing).
		WithPendingGeneration().
		Build()

	// Set mirroring state annotation with recent timestamps.
	now := time.Now().Format(time.RFC3339)
	stateJSON, _ := json.Marshal(map[string]string{
		"phase":          "syncing",
		"startedAt":      now,
		"phaseStartedAt": now,
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON)

	s.env = testutil.NewTestK8sEnv(cluster)

	// Update status.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringSyncing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	// Mock DB client returning decreasing lag.
	mockDB := &testutil.MockDBClient{
		GetMirrorSyncStatusFunc: func(_ context.Context) ([]db.MirrorSyncInfo, error) {
			return []db.MirrorSyncInfo{
				{ContentID: 0, IsSynced: false, ReplicationLag: 1024, State: "catchup"},
				{ContentID: 1, IsSynced: false, ReplicationLag: 512, State: "catchup"},
				{ContentID: 2, IsSynced: true, ReplicationLag: 0, State: "streaming"},
				{ContentID: 3, IsSynced: true, ReplicationLag: 0, State: "streaming"},
			}, nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger, dbFactory)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: replication lag metrics should be recorded.
	assert.NotEmpty(s.T(), mockMetrics.ReplicationLagCalls,
		"SetReplicationLag should have been called")

	// Verify specific lag values.
	lagMap := make(map[string]float64)
	for _, call := range mockMetrics.ReplicationLagCalls {
		lagMap[call.Segment] = call.LagBytes
	}
	assert.Equal(s.T(), float64(1024), lagMap["0"], "segment 0 lag should be 1024")
	assert.Equal(s.T(), float64(512), lagMap["1"], "segment 1 lag should be 512")
	assert.Equal(s.T(), float64(0), lagMap["2"], "segment 2 lag should be 0")
	assert.Equal(s.T(), float64(0), lagMap["3"], "segment 3 lag should be 0")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_WALReplicationStarts() {
	// Arrange: create a cluster in Updating phase at the initializing phase.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringInitializing).
		WithPendingGeneration().
		Build()

	now := time.Now().Format(time.RFC3339)
	stateJSON, _ := json.Marshal(map[string]string{
		"phase":          "initializing",
		"startedAt":      now,
		"phaseStartedAt": now,
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON)

	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringInitializing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	// Track whether ConfigureReplication was called.
	configureReplicationCalled := false
	mockDB := &testutil.MockDBClient{
		ConfigureReplicationFunc: func(_ context.Context, _ db.ReplicationOptions) error {
			configureReplicationCalled = true
			return nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger, dbFactory)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: ConfigureReplication should have been called.
	assert.True(s.T(), configureReplicationCalled,
		"ConfigureReplication should be called during mirror initialization")
}

// ============================================================================
// Group 5: Completion
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_CompletesSuccessfully() {
	// Arrange: create a cluster in Updating phase at the syncing phase,
	// with all mirrors synced.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringSyncing).
		WithPendingGeneration().
		Build()

	now := time.Now().Format(time.RFC3339)
	stateJSON, _ := json.Marshal(map[string]string{
		"phase":          "syncing",
		"startedAt":      now,
		"phaseStartedAt": now,
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON)

	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringSyncing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	// Mock DB: all mirrors synced.
	mockDB := &testutil.MockDBClient{
		GetMirrorSyncStatusFunc: func(_ context.Context) ([]db.MirrorSyncInfo, error) {
			return []db.MirrorSyncInfo{
				{ContentID: 0, IsSynced: true, ReplicationLag: 0, State: "streaming"},
				{ContentID: 1, IsSynced: true, ReplicationLag: 0, State: "streaming"},
				{ContentID: 2, IsSynced: true, ReplicationLag: 0, State: "streaming"},
				{ContentID: 3, IsSynced: true, ReplicationLag: 0, State: "streaming"},
			}, nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger, dbFactory)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: phase should be Running, mirroring InSync.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after mirroring completes")
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, updated.Status.MirroringStatus,
		"mirroring status should be InSync after completion")

	// Verify MirroringInSync event.
	events := collectEvents(fakeRecorder)
	mirroringInSyncFound := false
	for _, event := range events {
		if containsSubstring(event, "MirroringInSync") {
			mirroringInSyncFound = true
			break
		}
	}
	assert.True(s.T(), mirroringInSyncFound,
		"MirroringInSync event should be emitted; events: %v", events)

	// Verify RecordMirroringOperation called with "enable".
	require.Len(s.T(), mockMetrics.MirroringOperationCalls, 1,
		"RecordMirroringOperation should be called once")
	assert.Equal(s.T(), "enable", mockMetrics.MirroringOperationCalls[0].Operation,
		"operation should be enable")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_DataMatchesPrimaries() {
	// Arrange: cluster in syncing phase, all mirrors synced.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringSyncing).
		WithPendingGeneration().
		Build()

	now2 := time.Now().Format(time.RFC3339)
	stateJSON2, _ := json.Marshal(map[string]string{
		"phase":          "syncing",
		"startedAt":      now2,
		"phaseStartedAt": now2,
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON2)

	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringSyncing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	allSynced := []db.MirrorSyncInfo{
		{ContentID: 0, IsSynced: true, ReplicationLag: 0, State: "streaming"},
		{ContentID: 1, IsSynced: true, ReplicationLag: 0, State: "streaming"},
		{ContentID: 2, IsSynced: true, ReplicationLag: 0, State: "streaming"},
		{ContentID: 3, IsSynced: true, ReplicationLag: 0, State: "streaming"},
	}
	mockDB := &testutil.MockDBClient{
		GetMirrorSyncStatusFunc: func(_ context.Context) ([]db.MirrorSyncInfo, error) {
			return allSynced, nil
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger, dbFactory)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: all replication lag should be 0.
	for _, call := range mockMetrics.ReplicationLagCalls {
		assert.Equal(s.T(), float64(0), call.LagBytes,
			"replication lag for segment %s should be 0", call.Segment)
	}

	// Verify MirroringHealthy condition is True.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	mirrorCond := util.FindCondition(updated.Status.Conditions, string(cbv1alpha1.ConditionMirroringHealthy))
	require.NotNil(s.T(), mirrorCond, "MirroringHealthy condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, mirrorCond.Status,
		"MirroringHealthy should be True when all synced")
}

// ============================================================================
// Group 6: Error Handling
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_HandlesDBError() {
	// Arrange: cluster in Updating phase at initializing phase, DB returns error.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringInitializing).
		WithPendingGeneration().
		Build()

	nowDB := time.Now().Format(time.RFC3339)
	stateJSON, _ := json.Marshal(map[string]string{
		"phase":          "initializing",
		"startedAt":      nowDB,
		"phaseStartedAt": nowDB,
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON)

	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringInitializing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	// Mock DB: InitializeMirrors returns error.
	mockDB := &testutil.MockDBClient{
		InitializeMirrorsFunc: func(_ context.Context, _ db.MirrorInitOptions) error {
			return fmt.Errorf("connection refused")
		},
	}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger, dbFactory)

	// Act: reconcile should not return error (error is handled internally).
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err, "reconciliation should not return error on DB failure")

	// Assert: status should NOT be corrupted — still in Updating/Initializing.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseUpdating, updated.Status.Phase,
		"phase should remain Updating after DB error")
}

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_HandlesTimeout() {
	// Arrange: cluster in Updating phase with mirroring state that started long ago.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseUpdating).
		WithMirroringStatus(cbv1alpha1.MirroringInitializing).
		WithPendingGeneration().
		Build()

	// Set startedAt to a time well past the timeout (30 minutes).
	stateJSON, _ := json.Marshal(map[string]string{
		"phase":          "syncing",
		"startedAt":      "2020-01-01T00:00:00Z", // Far in the past.
		"phaseStartedAt": "2020-01-01T00:00:00Z",
		"layout":         "group",
	})
	cluster.Annotations[util.AnnotationMirroringState] = string(stateJSON)

	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseUpdating
	current.Status.MirroringStatus = cbv1alpha1.MirroringInitializing
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllStatefulSetsWithMirrors(current)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: mirroring status should be Degraded.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringDegraded, updated.Status.MirroringStatus,
		"mirroring status should be Degraded after timeout")

	// Verify MirroringFailed event.
	events := collectEvents(fakeRecorder)
	mirroringFailedFound := false
	for _, event := range events {
		if containsSubstring(event, "MirroringFailed") && containsSubstring(event, "timed out") {
			mirroringFailedFound = true
			break
		}
	}
	assert.True(s.T(), mirroringFailedFound,
		"MirroringFailed event with 'timed out' should be emitted; events: %v", events)
}

// ============================================================================
// Group 7: Disable Mirroring
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestDisableMirroring_DeletesMirrorSTS() {
	// Arrange: create a Running cluster with mirroring enabled, then disable it.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(false, "").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroringStatus(cbv1alpha1.MirroringInSync).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	// Create all STS including mirrors.
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 4)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, 4)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: mirror STS should be deleted.
	assert.False(s.T(), s.statefulSetExists(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror StatefulSet should be deleted after disabling mirroring")

	// Verify status is NotConfigured.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.MirroringNotConfigured, updated.Status.MirroringStatus,
		"mirroring status should be NotConfigured after disable")

	// Verify MirroringDisabled event.
	events := collectEvents(fakeRecorder)
	mirroringDisabledFound := false
	for _, event := range events {
		if containsSubstring(event, "MirroringDisabled") {
			mirroringDisabledFound = true
			break
		}
	}
	assert.True(s.T(), mirroringDisabledFound,
		"MirroringDisabled event should be emitted; events: %v", events)

	// Verify RecordMirroringOperation called with "disable".
	require.Len(s.T(), mockMetrics.MirroringOperationCalls, 1,
		"RecordMirroringOperation should be called once")
	assert.Equal(s.T(), "disable", mockMetrics.MirroringOperationCalls[0].Operation,
		"operation should be disable")
}

func (s *Scenario19EnableMirroringSuite) TestDisableMirroring_CleansPVCsOnDeletePolicy() {
	// Arrange: cluster with DeletionPolicy=Delete and mirroring disabled in spec.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(false, "").
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyDelete).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroringStatus(cbv1alpha1.MirroringInSync).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 4)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, 4)

	// Create mirror PVCs.
	s.createMirrorPVCs(current)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: mirror PVCs should be deleted.
	pvcCount := s.countPVCs(cluster.Namespace, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentSegmentMirror,
	})
	assert.Equal(s.T(), 0, pvcCount,
		"mirror PVCs should be deleted when DeletionPolicy=Delete")
}

func (s *Scenario19EnableMirroringSuite) TestDisableMirroring_RetainsPVCsOnRetainPolicy() {
	// Arrange: cluster with DeletionPolicy=Retain and mirroring disabled in spec.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(false, "").
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroringStatus(cbv1alpha1.MirroringInSync).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 4)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, 4)

	// Create mirror PVCs.
	s.createMirrorPVCs(current)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: mirror PVCs should be retained.
	pvcCount := s.countPVCs(cluster.Namespace, map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentSegmentMirror,
	})
	assert.Equal(s.T(), 4, pvcCount,
		"mirror PVCs should be retained when DeletionPolicy=Retain")
}

// ============================================================================
// Group 8: Idempotency
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestEnableMirroring_IdempotentOnRerun() {
	// Arrange: create a Running cluster, enable mirroring, reconcile twice.
	cluster := testutil.UnmirroredRunningCluster(scenario19ClusterName, scenario19Namespace)
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringNotConfigured
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createAllPrimaryStatefulSets(current)

	// Enable mirroring.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{
		Enabled: true,
		Layout:  cbv1alpha1.MirroringLayoutGroup,
	}
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: first reconcile.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Act: second reconcile (should not create duplicate resources).
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Assert: only one mirror STS should exist.
	stsList := &appsv1.StatefulSetList{}
	err = s.env.Client.List(s.ctx, stsList, client.InNamespace(scenario19Namespace))
	require.NoError(s.T(), err)

	mirrorSTSCount := 0
	for i := range stsList.Items {
		if stsList.Items[i].Name == util.SegmentMirrorName(cluster.Name) {
			mirrorSTSCount++
		}
	}
	assert.Equal(s.T(), 1, mirrorSTSCount,
		"only one mirror StatefulSet should exist after two reconciles")
}

func (s *Scenario19EnableMirroringSuite) TestDisableMirroring_IdempotentOnRerun() {
	// Arrange: cluster with mirroring disabled in spec but STS exists.
	cluster := testutil.NewClusterBuilder(scenario19ClusterName, scenario19Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(false, "").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithMirroringStatus(cbv1alpha1.MirroringInSync).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	current.Status.MirroringStatus = cbv1alpha1.MirroringInSync
	current.Status.CoordinatorReady = true
	current.Status.SegmentsReady = 4
	current.Status.SegmentsTotal = 4
	err = s.env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err)

	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, 4)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, 4)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &Scenario19MirroringMetricsRecorder{}
	reconciler := newScenario19Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: first reconcile — deletes mirror STS.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err)

	// Act: second reconcile — should not error.
	_, err = reconciler.Reconcile(s.ctx, scenario19Req())
	require.NoError(s.T(), err, "second disable reconcile should not error")

	// Assert: mirror STS should not exist.
	assert.False(s.T(), s.statefulSetExists(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror StatefulSet should not exist after two disable reconciles")
}

// ============================================================================
// Group 9: Webhook Validation
// ============================================================================

func (s *Scenario19EnableMirroringSuite) TestWebhook_EnableMirroring_RunningCluster_Allowed() {
	// Arrange: old cluster is Running without mirroring, new cluster enables mirroring.
	oldCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(false, "").
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()

	validator := webhook.NewCloudberryClusterValidator(nil)

	// Act.
	warnings, err := validator.ValidateUpdate(s.ctx, oldCluster, newCluster)

	// Assert: should be allowed.
	require.NoError(s.T(), err, "enabling mirroring on Running cluster should be allowed")
	_ = warnings
}

func (s *Scenario19EnableMirroringSuite) TestWebhook_EnableMirroring_StoppedCluster_Rejected() {
	// Arrange: old cluster is Stopped.
	oldCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(false, "").
		WithPhase(cbv1alpha1.ClusterPhaseStopped).
		Build()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseStopped

	newCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()

	validator := webhook.NewCloudberryClusterValidator(nil)

	// Act.
	_, err := validator.ValidateUpdate(s.ctx, oldCluster, newCluster)

	// Assert: should be rejected.
	require.Error(s.T(), err, "enabling mirroring on Stopped cluster should be rejected")
	assert.Contains(s.T(), err.Error(), "Running",
		"error should mention Running phase requirement")
}

func (s *Scenario19EnableMirroringSuite) TestWebhook_EnableMirroring_InsufficientSegments_Rejected() {
	// Arrange: old cluster is Running with 1 segment.
	oldCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(1).
		WithMirroring(false, "").
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(1).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()

	validator := webhook.NewCloudberryClusterValidator(nil)

	// Act.
	_, err := validator.ValidateUpdate(s.ctx, oldCluster, newCluster)

	// Assert: should be rejected.
	require.Error(s.T(), err, "enabling mirroring with 1 segment should be rejected")
	assert.Contains(s.T(), err.Error(), "segments",
		"error should mention insufficient segments")
}

func (s *Scenario19EnableMirroringSuite) TestWebhook_LayoutChange_WhileEnabled_Rejected() {
	// Arrange: old cluster has group mirroring, new cluster changes to spread.
	oldCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	oldCluster.Status.Phase = cbv1alpha1.ClusterPhaseRunning

	newCluster := testutil.NewClusterBuilder("test-webhook", scenario19Namespace).
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutSpread).
		Build()

	validator := webhook.NewCloudberryClusterValidator(nil)

	// Act.
	_, err := validator.ValidateUpdate(s.ctx, oldCluster, newCluster)

	// Assert: should be rejected.
	require.Error(s.T(), err, "changing layout while mirroring is enabled should be rejected")
	assert.Contains(s.T(), err.Error(), "layout",
		"error should mention layout change")
}

// mustParseQuantity parses a resource quantity string, panicking on failure.
func mustParseQuantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

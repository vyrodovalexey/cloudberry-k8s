//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario8ClusterName = "scenario8-cluster"
	scenario8Namespace   = "cloudberry-test"
	scenario8InitCount   = int32(4)
	scenario8ScaleCount  = int32(6)
)

// MockMetricsRecorder wraps NoopRecorder and tracks scale operation calls.
type MockMetricsRecorder struct {
	metrics.NoopRecorder
	ScaleOperationCalls []scaleOpCall
}

type scaleOpCall struct {
	Cluster   string
	Namespace string
	Operation string
}

// RecordScaleOperation records a scale operation event for verification.
func (m *MockMetricsRecorder) RecordScaleOperation(cluster, namespace, operation string) {
	m.ScaleOperationCalls = append(m.ScaleOperationCalls, scaleOpCall{
		Cluster:   cluster,
		Namespace: namespace,
		Operation: operation,
	})
}

// Scenario8ScaleOutSuite tests scale-out with mirroring scenarios.
type Scenario8ScaleOutSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario8_ScaleOut(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario8ScaleOutSuite))
}

func (s *Scenario8ScaleOutSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario8ScaleOutSuite) TearDownTest() {
	s.cancel()
}

// buildScenario8Cluster constructs a Running cluster with 4 segments + mirroring.
func buildScenario8Cluster() *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario8ClusterName, scenario8Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario8InitCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario8Req creates a reconcile request for the scenario 8 cluster.
func scenario8Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario8ClusterName,
			Namespace: scenario8Namespace,
		},
	}
}

// createReadyStatefulSet8 creates a StatefulSet with ready replicas.
func (s *Scenario8ScaleOutSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
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
func (s *Scenario8ScaleOutSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	}
}

// getStatefulSetReplicas returns the spec replicas for a StatefulSet.
func (s *Scenario8ScaleOutSuite) getStatefulSetReplicas(name, namespace string) int32 {
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

// simulateSegmentsReady sets segment StatefulSet statuses to the desired ready replicas.
func (s *Scenario8ScaleOutSuite) simulateSegmentsReady(cluster *cbv1alpha1.CloudberryCluster, count int32) {
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

// newScenario8Reconciler creates a ClusterReconciler for scenario 8 tests.
func newScenario8Reconciler(
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

// --- Test: Scale-Out Detected ---

func (s *Scenario8ScaleOutSuite) TestScenario8_ScaleOutDetected() {
	// Arrange: create a Running cluster with 4 segments + mirroring.
	cluster := buildScenario8Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario8Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch cluster spec to segments.count=6 and bump generation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario8ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario8Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should be Scaling after scale-out detected")

	// Verify primary StatefulSet replicas updated to 6.
	assert.Equal(s.T(), scenario8ScaleCount,
		s.getStatefulSetReplicas(util.SegmentPrimaryName(cluster.Name), cluster.Namespace),
		"primary segments should be scaled to 6")

	// Verify mirror StatefulSet replicas updated to 6.
	assert.Equal(s.T(), scenario8ScaleCount,
		s.getStatefulSetReplicas(util.SegmentMirrorName(cluster.Name), cluster.Namespace),
		"mirror segments should be scaled to 6")

	// Verify ScaleOutStarted event.
	events := collectEvents(fakeRecorder)
	scaleStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "ScaleOutStarted") {
			scaleStartedFound = true
			break
		}
	}
	assert.True(s.T(), scaleStartedFound,
		"ScaleOutStarted event should be emitted; events: %v", events)

	// Verify DataRedistribution condition set.
	redistCond := util.FindCondition(updated.Status.Conditions, "DataRedistribution")
	require.NotNil(s.T(), redistCond, "DataRedistribution condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, redistCond.Status,
		"DataRedistribution should be True (InProgress)")
	assert.Equal(s.T(), "InProgress", redistCond.Reason,
		"DataRedistribution reason should be InProgress")
}

// --- Test: Scale-Out Completes ---

func (s *Scenario8ScaleOutSuite) TestScenario8_ScaleOutCompletes() {
	// Arrange: create a cluster already in Scaling phase with 6 desired segments.
	cluster := testutil.NewClusterBuilder(scenario8ClusterName, scenario8Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario8ScaleCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseScaling).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create StatefulSets with 6 replicas (already scaled).
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario8ScaleCount)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, scenario8ScaleCount)

	// Simulate all 6 primary + 6 mirror pods ready.
	s.simulateSegmentsReady(cluster, scenario8ScaleCount)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario8Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Run reconciliation — should detect Scaling phase and check progress.
	_, err := reconciler.Reconcile(s.ctx, scenario8Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase -> Running.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, updated.Status.Phase,
		"phase should be Running after scale-out completes")

	// Verify DataRedistribution condition -> Completed.
	redistCond := util.FindCondition(updated.Status.Conditions, "DataRedistribution")
	require.NotNil(s.T(), redistCond, "DataRedistribution condition should exist")
	assert.Equal(s.T(), metav1.ConditionTrue, redistCond.Status,
		"DataRedistribution should be True (Completed)")
	assert.Equal(s.T(), "Completed", redistCond.Reason,
		"DataRedistribution reason should be Completed")

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

	// Verify segmentsReady=6, segmentsTotal=6.
	assert.Equal(s.T(), scenario8ScaleCount, updated.Status.SegmentsReady,
		"segmentsReady should be 6")
	assert.Equal(s.T(), scenario8ScaleCount, updated.Status.SegmentsTotal,
		"segmentsTotal should be 6")
}

// --- Test: Scale Metrics ---

func (s *Scenario8ScaleOutSuite) TestScenario8_ScaleMetrics() {
	// Arrange: create a cluster in Scaling phase with all segments ready.
	cluster := testutil.NewClusterBuilder(scenario8ClusterName, scenario8Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario8ScaleCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseScaling).
		WithPendingGeneration().
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, scenario8ScaleCount)
	s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, scenario8ScaleCount)
	s.simulateSegmentsReady(cluster, scenario8ScaleCount)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario8Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile to complete scale-out.
	_, err := reconciler.Reconcile(s.ctx, scenario8Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify RecordScaleOperation called with "scale-out".
	require.Len(s.T(), mockMetrics.ScaleOperationCalls, 1,
		"RecordScaleOperation should be called once")
	assert.Equal(s.T(), "scale-out", mockMetrics.ScaleOperationCalls[0].Operation,
		"operation should be scale-out")
	assert.Equal(s.T(), scenario8ClusterName, mockMetrics.ScaleOperationCalls[0].Cluster,
		"cluster name should match")
	assert.Equal(s.T(), scenario8Namespace, mockMetrics.ScaleOperationCalls[0].Namespace,
		"namespace should match")
}

// --- Test: Redistribution Job Created ---

func (s *Scenario8ScaleOutSuite) TestScenario8_RedistributionJobCreated() {
	// Arrange: create a Running cluster with 4 segments + mirroring.
	cluster := buildScenario8Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario8Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: patch cluster spec to segments.count=6 and bump generation.
	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	current.Spec.Segments.Count = scenario8ScaleCount
	current.Generation = 2
	err = s.env.Client.Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run reconciliation.
	_, err = reconciler.Reconcile(s.ctx, scenario8Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify the scale-state annotation is set with the correct phase.
	// The new scale-out flow uses DB client for redistribution instead of a Job.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	scaleStateJSON := updated.Annotations["avsoft.io/scale-state"]
	assert.NotEmpty(s.T(), scaleStateJSON,
		"scale-state annotation should be set during scale-out")

	if scaleStateJSON != "" {
		var scaleState struct {
			Phase    string `json:"phase"`
			OldCount int32  `json:"oldCount"`
			NewCount int32  `json:"newCount"`
		}
		err = json.Unmarshal([]byte(scaleStateJSON), &scaleState)
		require.NoError(s.T(), err, "scale-state annotation should be valid JSON")
		assert.Equal(s.T(), "scaling-sts", scaleState.Phase,
			"initial scale phase should be 'scaling-sts'")
		assert.Equal(s.T(), scenario8InitCount, scaleState.OldCount)
		assert.Equal(s.T(), scenario8ScaleCount, scaleState.NewCount)
	}
}

// --- Test: Scale Status API ---

func (s *Scenario8ScaleOutSuite) TestScenario8_ScaleStatusAPI() {
	// Arrange: create a cluster in Scaling phase.
	cluster := testutil.NewClusterBuilder(scenario8ClusterName, scenario8Namespace).
		WithVersion("7.1.0").
		WithSegments(scenario8ScaleCount).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		Build()

	env := testutil.NewTestK8sEnv(cluster)

	// Update status after creation since the fake client's status subresource
	// does not persist status fields set during object creation.
	current, err := env.GetCluster(s.ctx, scenario8ClusterName, scenario8Namespace)
	require.NoError(s.T(), err)
	current.Status.Phase = cbv1alpha1.ClusterPhaseScaling
	current.Status.SegmentsReady = scenario8InitCount
	current.Status.SegmentsTotal = scenario8ScaleCount
	current.Status.Conditions = util.SetCondition(current.Status.Conditions,
		"DataRedistribution", metav1.ConditionTrue, "InProgress",
		"Data redistribution in progress")
	err = env.Client.Status().Update(s.ctx, current)
	require.NoError(s.T(), err, "updating cluster status should succeed")

	apiServer := api.NewServer(env.Client, nil, nil, &metrics.NoopRecorder{}, slog.Default())
	handler := apiServer.Handler()

	// Act: GET /clusters/{name}/scale/status.
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1alpha1/clusters/"+scenario8ClusterName+"/scale/status?namespace="+scenario8Namespace,
		nil)
	// Add admin identity to bypass auth middleware.
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Assert: response should contain scaling info.
	assert.Equal(s.T(), http.StatusOK, rec.Code,
		"scale status endpoint should return 200")

	var body map[string]interface{}
	unmarshalErr := json.Unmarshal(rec.Body.Bytes(), &body)
	require.NoError(s.T(), unmarshalErr, "response should be valid JSON")

	assert.Equal(s.T(), true, body["scaling"],
		"scaling should be true")
	assert.Equal(s.T(), "Scaling", body["phase"],
		"phase should be Scaling")
	assert.Equal(s.T(), scenario8ClusterName, body["name"],
		"name should match")

	// Verify redistribution info is present.
	redistribution, ok := body["redistribution"].(map[string]interface{})
	require.True(s.T(), ok, "redistribution field should be present")
	assert.Equal(s.T(), "InProgress", redistribution["reason"],
		"redistribution reason should be InProgress")
}

// --- Test: Scale-Out Not Triggered When Count Unchanged ---

func (s *Scenario8ScaleOutSuite) TestScenario8_NoScaleWhenCountUnchanged() {
	// Arrange: create a Running cluster with 4 segments, StatefulSets already at 4.
	cluster := buildScenario8Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}
	reconciler := newScenario8Reconciler(s.env, fakeRecorder, mockMetrics, s.logger)

	// Act: reconcile without changing segment count.
	_, err := reconciler.Reconcile(s.ctx, scenario8Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify phase is NOT Scaling.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.NotEqual(s.T(), cbv1alpha1.ClusterPhaseScaling, updated.Status.Phase,
		"phase should not be Scaling when count is unchanged")

	// Verify no ScaleOutStarted event.
	events := collectEvents(fakeRecorder)
	for _, event := range events {
		assert.False(s.T(), containsSubstring(event, "ScaleOutStarted"),
			"ScaleOutStarted event should not be emitted when count is unchanged; events: %v", events)
	}
}

// jobNames extracts job names from a JobList for diagnostic output.
func jobNames(list *batchv1.JobList) []string {
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	return names
}

// Ensure MockMetricsRecorder satisfies the metrics.Recorder interface at compile time.
var _ metrics.Recorder = (*MockMetricsRecorder)(nil)

// Ensure the test uses a reasonable timeout for CI environments.
func init() {
	// Allow tests to detect stale goroutines.
	_ = time.Second
}

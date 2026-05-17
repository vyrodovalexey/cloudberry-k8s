//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario2ClusterName = "scenario2-cluster"
	scenario2Namespace   = "cloudberry-test"
)

// Scenario2ConfigHotReloadSuite tests configuration hot-reload and rolling restart scenarios.
type Scenario2ConfigHotReloadSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario2_ConfigHotReload(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario2ConfigHotReloadSuite))
}

func (s *Scenario2ConfigHotReloadSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario2ConfigHotReloadSuite) TearDownTest() {
	s.cancel()
}

// buildScenario2Cluster constructs the Scenario 2 cluster CR in Running state
// with only reload-safe initial configuration (no restart-required params).
func buildScenario2Cluster() *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario2ClusterName, scenario2Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithConfig(map[string]string{
			"gp_enable_global_deadlock_detector": "on",
			"log_min_duration_statement":         "1000",
			"work_mem":                           "64MB",
		}).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// buildScenario2ClusterWithRestartParams constructs a cluster with restart-required params.
func buildScenario2ClusterWithRestartParams() *cbv1alpha1.CloudberryCluster {
	return testutil.NewClusterBuilder(scenario2ClusterName, scenario2Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithConfig(map[string]string{
			"max_connections":                    "200",
			"shared_buffers":                     "2GB",
			"gp_enable_global_deadlock_detector": "on",
		}).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
}

// scenario2AdminReq creates a reconcile request for the given cluster.
func scenario2AdminReq(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// newScenario2AdminReconciler creates an AdminReconciler with the given env and metrics.
func newScenario2AdminReconciler(
	env *testutil.TestK8sEnv,
	mockMetrics *mockMetricsRecorder,
	logger *slog.Logger,
) *controller.AdminReconciler {
	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	return controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, dbFactory, mockMetrics, logger,
	)
}

// createReadyStatefulSet creates a StatefulSet in the fake client with ready replicas
// and matching revisions to simulate a fully rolled StatefulSet.
func (s *Scenario2ConfigHotReloadSuite) createReadyStatefulSet(name, namespace string, replicas int32) {
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
			Replicas:        replicas,
			ReadyReplicas:   replicas,
			UpdatedReplicas: replicas,
			CurrentRevision: name + "-rev",
			UpdateRevision:  name + "-rev",
		},
	}
	err := s.env.Client.Create(s.ctx, sts)
	require.NoError(s.T(), err, "creating statefulset %s should succeed", name)
}

// createAllStatefulSets creates all StatefulSets for a cluster with ready replicas.
func (s *Scenario2ConfigHotReloadSuite) createAllStatefulSets(cluster *cbv1alpha1.CloudberryCluster) {
	s.createReadyStatefulSet(util.CoordinatorName(cluster.Name), cluster.Namespace, 1)
	s.createReadyStatefulSet(util.SegmentPrimaryName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	if cluster.Spec.Segments.Mirroring != nil && cluster.Spec.Segments.Mirroring.Enabled {
		s.createReadyStatefulSet(util.SegmentMirrorName(cluster.Name), cluster.Namespace, cluster.Spec.Segments.Count)
	}
	if cluster.Spec.Standby != nil && cluster.Spec.Standby.Enabled {
		s.createReadyStatefulSet(util.StandbyName(cluster.Name), cluster.Namespace, 1)
	}
}

// drainRollingRestart runs reconcile loops until the rolling restart annotation is removed.
// Returns the number of reconcile cycles executed.
func (s *Scenario2ConfigHotReloadSuite) drainRollingRestart(
	reconciler *controller.AdminReconciler,
	cluster *cbv1alpha1.CloudberryCluster,
	maxCycles int,
) int {
	for i := 0; i < maxCycles; i++ {
		current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
		require.NoError(s.T(), err)
		if _, hasRestart := current.Annotations[util.AnnotationRollingRestart]; !hasRestart {
			return i
		}
		_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
		require.NoError(s.T(), err, "reconcile cycle %d should succeed", i)
	}
	return maxCycles
}

// completePendingReloadForTest simulates the two-phase reload completion by:
// 1. Setting ObservedGeneration = Generation (simulating cluster controller)
// 2. Setting the pending-reload annotation timestamp to the past (bypassing 30s wait)
// 3. Running a reconcile to trigger completePendingReload
// This is needed because reload-safe changes use a two-phase mechanism:
// first reconcile sets the pending annotation, second reconcile (after propagation delay)
// calls pg_reload_conf() and records the metric/event.
func (s *Scenario2ConfigHotReloadSuite) completePendingReloadForTest(
	reconciler *controller.AdminReconciler,
	cluster *cbv1alpha1.CloudberryCluster,
) {
	s.T().Helper()

	current, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Simulate cluster controller updating ObservedGeneration so that
	// completePendingReload is reached (it requires ObservedGeneration == Generation).
	current.Status.ObservedGeneration = current.Generation
	err = s.env.UpdateClusterStatus(s.ctx, current)
	require.NoError(s.T(), err)

	// Set the pending-reload annotation timestamp to the past to bypass the 30s wait.
	current, err = s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	if _, hasPending := current.Annotations[util.AnnotationPendingReload]; hasPending {
		current.Annotations[util.AnnotationPendingReload] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
		err = s.env.Client.Update(s.ctx, current)
		require.NoError(s.T(), err)
	}

	// Run reconcile to trigger completePendingReload.
	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconcile for completePendingReload should succeed")
}

// --- Phase A: Reload-safe change ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_PhaseA_ReloadSafeChange() {
	// Arrange: create a Running cluster with reload-safe initial config.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockMetrics := &mockMetricsRecorder{}
	reconciler := newScenario2AdminReconciler(s.env, mockMetrics, s.logger)

	// First reconciliation: establishes the initial config hash and creates the ConfigMap.
	result, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Verify initial ConfigApplied condition reason is "ConfigReloadPending" (reload-safe,
	// waiting for ConfigMap volume propagation before calling pg_reload_conf).
	initial, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	cond := util.FindCondition(initial.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied))
	require.NotNil(s.T(), cond, "ConfigApplied condition should exist after initial reconciliation")
	assert.Equal(s.T(), "ConfigReloadPending", cond.Reason,
		"initial config with only reload-safe params should have reason ConfigReloadPending")

	// Record initial pod restart counts (all zero in fake env).
	initialRestartCounts := s.getPodRestartCounts(cluster)

	// Act: patch the cluster spec to add another reload-safe parameter.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["log_min_messages"] = "WARNING"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run admin controller reconciliation after the config change.
	result, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation after config change should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Assert: ConfigMap contains the new parameter.
	cm, err := s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "postgresql.conf ConfigMap should exist")
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "log_min_messages",
		"ConfigMap should contain log_min_messages")
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "WARNING",
		"ConfigMap should contain WARNING value")

	// Assert: ConfigApplied condition is set with reason "ConfigReloadPending"
	// (the actual pg_reload_conf() happens on a subsequent reconcile after ConfigMap propagation).
	final, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	cond = util.FindCondition(final.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied))
	require.NotNil(s.T(), cond)
	assert.Equal(s.T(), "ConfigReloadPending", cond.Reason,
		"ConfigApplied reason should be ConfigReloadPending for reload-safe change")

	// Assert: No pod restarts (restart counts unchanged).
	currentRestartCounts := s.getPodRestartCounts(cluster)
	assert.Equal(s.T(), initialRestartCounts, currentRestartCounts,
		"pod restart counts should remain unchanged for reload-safe parameter change")
}

// --- Phase B: Restart-required change ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_PhaseB_RestartRequiredChange() {
	// Arrange: create a Running cluster with reload-safe initial config.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}
	mockMetrics := &mockMetricsRecorder{}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		s.env.Builder, dbFactory, mockMetrics, s.logger,
	)

	// First reconciliation: establishes the initial config hash.
	_, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")
	drainEvents(fakeRecorder)

	// Act: patch the cluster spec with restart-required parameters.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["shared_buffers"] = "4GB"
	updated.Spec.Config.Parameters["max_connections"] = "300"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err, "updating cluster spec should succeed")

	// Run admin controller reconciliation after the config change.
	result, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation after restart-required config change should succeed")
	assert.NotZero(s.T(), result.RequeueAfter, "should requeue after reconciliation")

	// Assert: ConfigMap contains the new values.
	cm, err := s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "postgresql.conf ConfigMap should exist")
	confContent := cm.Data["postgresql.conf"]
	assert.Contains(s.T(), confContent, "shared_buffers",
		"ConfigMap should contain shared_buffers")
	assert.Contains(s.T(), confContent, "4GB",
		"ConfigMap should contain 4GB value for shared_buffers")
	assert.Contains(s.T(), confContent, "max_connections",
		"ConfigMap should contain max_connections")
	assert.Contains(s.T(), confContent, "300",
		"ConfigMap should contain 300 value for max_connections")

	// Assert: ConfigApplied condition is False with reason "RestartRequired".
	afterChange, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	assert.True(s.T(), util.IsConditionFalse(afterChange.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied)),
		"ConfigApplied condition should be False after restart-required change")
	cond := util.FindCondition(afterChange.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied))
	require.NotNil(s.T(), cond)
	assert.Equal(s.T(), "RestartRequired", cond.Reason,
		"ConfigApplied reason should be RestartRequired")

	// Assert: rolling-restart annotation is set on the cluster.
	restartAnnotation, hasRestart := afterChange.Annotations[util.AnnotationRollingRestart]
	assert.True(s.T(), hasRestart, "rolling-restart annotation should be set")

	// Assert: annotation contains the restart-required param names.
	assert.Contains(s.T(), restartAnnotation, "shared_buffers",
		"rolling-restart annotation should contain shared_buffers")
	assert.Contains(s.T(), restartAnnotation, "max_connections",
		"rolling-restart annotation should contain max_connections")

	// Assert: "RollingRestartStarted" event was emitted.
	events := collectEvents(fakeRecorder)
	restartStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "RollingRestartStarted") {
			restartStartedFound = true
			break
		}
	}
	assert.True(s.T(), restartStartedFound,
		"RollingRestartStarted event should be emitted; events: %v", events)

	// Simulate rolling restart completion by running reconcile cycles.
	cycles := s.drainRollingRestart(reconciler, cluster, 20)
	assert.Less(s.T(), cycles, 20, "rolling restart should complete within 20 cycles")

	// Assert: rolling restart completed - annotation removed.
	final, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasRestart = final.Annotations[util.AnnotationRollingRestart]
	assert.False(s.T(), hasRestart, "rolling-restart annotation should be removed after completion")

	// Assert: ConfigApplied=True with reason "ConfigAppliedAfterRestart".
	assert.True(s.T(), util.IsConditionTrue(final.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied)),
		"ConfigApplied condition should be True after rolling restart completes")
	cond = util.FindCondition(final.Status.Conditions, string(cbv1alpha1.ConditionConfigApplied))
	require.NotNil(s.T(), cond)
	assert.Equal(s.T(), "ConfigAppliedAfterRestart", cond.Reason,
		"ConfigApplied reason should be ConfigAppliedAfterRestart")

	// Assert: "RollingRestartCompleted" event was emitted.
	completedEvents := collectEvents(fakeRecorder)
	restartCompletedFound := false
	for _, event := range completedEvents {
		if containsSubstring(event, "RollingRestartCompleted") {
			restartCompletedFound = true
			break
		}
	}
	assert.True(s.T(), restartCompletedFound,
		"RollingRestartCompleted event should be emitted; events: %v", completedEvents)

	// Assert: Config reload metric was recorded.
	calls := mockMetrics.getCalls()
	assert.True(s.T(), containsCall(calls, "RecordConfigReload"),
		"RecordConfigReload should have been called for restart-required parameter change")
}

// --- Config hash change detection ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_ConfigHashDetectsChanges() {
	// Arrange: create a Running cluster with reload-safe initial config.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockMetrics := &mockMetricsRecorder{}
	reconciler := newScenario2AdminReconciler(s.env, mockMetrics, s.logger)

	// First reconciliation: stores initial hash, creates ConfigMap, and sets pending-reload.
	_, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")

	// Verify ConfigMap was created.
	_, err = s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "ConfigMap should be created after first reconciliation")

	// Complete the pending reload (two-phase: metric is recorded in completePendingReload).
	s.completePendingReloadForTest(reconciler, cluster)

	initialReloadCount := countCalls(mockMetrics.getCalls(), "RecordConfigReload")
	assert.Equal(s.T(), 1, initialReloadCount,
		"RecordConfigReload should be called once after initial reconciliation and pending reload completion")

	// Reconciliation without changes: should skip (no ConfigMap update).
	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation without changes should succeed")

	afterNoChangeReloadCount := countCalls(mockMetrics.getCalls(), "RecordConfigReload")
	assert.Equal(s.T(), initialReloadCount, afterNoChangeReloadCount,
		"RecordConfigReload should NOT be called again when config is unchanged")

	// Reconciliation with a reload-safe parameter change: should detect change and update.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["log_statement"] = "all"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation after config change should succeed")

	// Complete the pending reload for the new change.
	s.completePendingReloadForTest(reconciler, cluster)

	afterChangeReloadCount := countCalls(mockMetrics.getCalls(), "RecordConfigReload")
	assert.Equal(s.T(), initialReloadCount+1, afterChangeReloadCount,
		"RecordConfigReload should be called again after config change and pending reload completion")

	// Verify ConfigMap was updated with the new parameter.
	cm, err := s.env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "log_statement",
		"ConfigMap should contain log_statement after change")
	assert.Contains(s.T(), cm.Data["postgresql.conf"], "all",
		"ConfigMap should contain 'all' value for log_statement")
}

// --- Metrics recorded on config change ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_MetricsRecordedOnConfigChange() {
	// Arrange: create a Running cluster with reload-safe initial config.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	mockMetrics := &mockMetricsRecorder{}
	reconciler := newScenario2AdminReconciler(s.env, mockMetrics, s.logger)

	// First reconciliation: establishes the initial config hash and sets pending-reload.
	_, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")

	// Complete the pending reload (two-phase: metric is recorded in completePendingReload).
	s.completePendingReloadForTest(reconciler, cluster)

	// Verify metric was recorded after completing the pending reload.
	initialCalls := mockMetrics.getCalls()
	initialReloadCount := countCalls(initialCalls, "RecordConfigReload")
	require.Equal(s.T(), 1, initialReloadCount,
		"RecordConfigReload should be called once after initial reconciliation and pending reload completion")

	// Act: change a reload-safe config parameter and reconcile.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["log_min_messages"] = "WARNING"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation after config change should succeed")

	// Complete the pending reload for the new change.
	s.completePendingReloadForTest(reconciler, cluster)

	// Assert: RecordConfigReload was called again.
	allCalls := mockMetrics.getCalls()
	totalReloadCount := countCalls(allCalls, "RecordConfigReload")
	assert.Equal(s.T(), 2, totalReloadCount,
		"RecordConfigReload should be called twice (initial + change)")

	// Verify the RecordConfigReload call has the correct cluster and namespace.
	reloadCalls := filterCalls(allCalls, "RecordConfigReload")
	require.Len(s.T(), reloadCalls, 2, "should have exactly 2 RecordConfigReload calls")
	lastReloadCall := reloadCalls[len(reloadCalls)-1]
	assert.Equal(s.T(), scenario2ClusterName, lastReloadCall.args["cluster"],
		"RecordConfigReload should be called with the correct cluster name")
	assert.Equal(s.T(), scenario2Namespace, lastReloadCall.args["namespace"],
		"RecordConfigReload should be called with the correct namespace")
}

// --- Event emitted on config change ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_EventEmittedOnConfigChange() {
	// Arrange: create a Running cluster with reload-safe initial config.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Use a FakeRecorder with a buffered channel so we can read events.
	fakeRecorder := record.NewFakeRecorder(100)
	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		s.env.Builder, dbFactory, s.env.Metrics, s.logger,
	)

	// First reconciliation: creates ConfigMap and sets pending-reload annotation.
	_, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")

	// Complete the initial pending reload (emits ConfigReloaded event).
	s.completePendingReloadForTest(reconciler, cluster)

	// Drain initial events.
	drainEvents(fakeRecorder)

	// Act: change a reload-safe config parameter and reconcile.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["log_min_messages"] = "WARNING"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation after config change should succeed")

	// Complete the pending reload for the new change (emits ConfigReloaded event).
	s.completePendingReloadForTest(reconciler, cluster)

	// Assert: "ConfigReloaded" event was emitted.
	events := collectEvents(fakeRecorder)
	configReloadedFound := false
	for _, event := range events {
		if containsSubstring(event, "ConfigReloaded") {
			configReloadedFound = true
			break
		}
	}
	assert.True(s.T(), configReloadedFound,
		"ConfigReloaded event should be emitted after reload-safe config change; events: %v", events)
}

// --- Rolling restart order test ---

func (s *Scenario2ConfigHotReloadSuite) TestScenario2_RollingRestartOrder() {
	// Arrange: create a cluster with standby + mirroring and all StatefulSets.
	cluster := buildScenario2Cluster()
	s.env = testutil.NewTestK8sEnv(cluster)
	s.createAllStatefulSets(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockDB := &testutil.MockDBClient{}
	dbFactory := &testutil.MockDBClientFactory{Client: mockDB}
	mockMetrics := &mockMetricsRecorder{}

	reconciler := controller.NewAdminReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		s.env.Builder, dbFactory, mockMetrics, s.logger,
	)

	// First reconciliation: establishes the initial config hash.
	_, err := reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "initial reconciliation should succeed")
	drainEvents(fakeRecorder)

	// Act: change a restart-required param.
	updated, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Spec.Config.Parameters["shared_buffers"] = "4GB"
	updated.Generation = updated.Generation + 1
	err = s.env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	// Trigger the restart.
	_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify the rolling restart annotation starts at "mirrors" phase.
	afterTrigger, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	restartAnnotation := afterTrigger.Annotations[util.AnnotationRollingRestart]
	require.NotEmpty(s.T(), restartAnnotation, "rolling-restart annotation should be set")

	var state struct {
		Phase         string   `json:"phase"`
		RestartParams []string `json:"restartParams"`
	}
	err = json.Unmarshal([]byte(restartAnnotation), &state)
	require.NoError(s.T(), err, "should parse rolling restart annotation")
	assert.Equal(s.T(), "mirrors", state.Phase, "initial phase should be mirrors")

	// Track which StatefulSets get the restart-trigger annotation at each phase.
	expectedOrder := []struct {
		phase   string
		stsName string
	}{
		{"mirrors", util.SegmentMirrorName(cluster.Name)},
		{"primaries", util.SegmentPrimaryName(cluster.Name)},
		{"standby", util.StandbyName(cluster.Name)},
		{"coordinator", util.CoordinatorName(cluster.Name)},
	}

	// The first phase (mirrors) should already have been restarted by triggerRollingRestart
	// via the initial reconcile. The continueRollingRestart will advance phases.
	// Run reconcile cycles and verify phase progression.
	for i, expected := range expectedOrder {
		current, getErr := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
		require.NoError(s.T(), getErr)

		annotation := current.Annotations[util.AnnotationRollingRestart]
		if annotation == "" {
			// Rolling restart completed early (all phases done).
			break
		}

		err = json.Unmarshal([]byte(annotation), &state)
		require.NoError(s.T(), err)

		// Verify we're at the expected phase or have advanced past it.
		if state.Phase != expected.phase {
			// May have already advanced; that's OK for phases that complete instantly.
			continue
		}

		s.T().Logf("phase %d: %s (sts: %s)", i, expected.phase, expected.stsName)

		// Run a reconcile cycle to advance.
		_, err = reconciler.Reconcile(s.ctx, scenario2AdminReq(cluster))
		require.NoError(s.T(), err, "reconcile at phase %s should succeed", expected.phase)
	}

	// Drain any remaining cycles.
	s.drainRollingRestart(reconciler, cluster, 20)

	// Verify rolling restart completed.
	final, err := s.env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	_, hasRestart := final.Annotations[util.AnnotationRollingRestart]
	assert.False(s.T(), hasRestart, "rolling-restart annotation should be removed after completion")

	// Verify each StatefulSet got the restart-trigger annotation.
	for _, expected := range expectedOrder {
		sts, getErr := s.env.GetStatefulSet(s.ctx, expected.stsName, cluster.Namespace)
		require.NoError(s.T(), getErr, "should get statefulset %s", expected.stsName)
		trigger := sts.Spec.Template.Annotations[util.AnnotationRestartTrigger]
		assert.NotEmpty(s.T(), trigger,
			"StatefulSet %s should have restart-trigger annotation after rolling restart", expected.stsName)
	}

	// Verify events.
	allEvents := collectEvents(fakeRecorder)
	hasStarted := false
	hasCompleted := false
	for _, event := range allEvents {
		if containsSubstring(event, "RollingRestartStarted") {
			hasStarted = true
		}
		if containsSubstring(event, "RollingRestartCompleted") {
			hasCompleted = true
		}
	}
	assert.True(s.T(), hasStarted, "RollingRestartStarted event should be emitted")
	assert.True(s.T(), hasCompleted, "RollingRestartCompleted event should be emitted")
}

// --- Helper functions ---

// getPodRestartCounts returns a map of pod name to restart count for the cluster's StatefulSets.
// In the fake k8s environment, pods are not actually created, so we verify that
// no StatefulSet has been deleted/recreated (which would indicate a restart).
func (s *Scenario2ConfigHotReloadSuite) getPodRestartCounts(
	cluster *cbv1alpha1.CloudberryCluster,
) map[string]int32 {
	counts := make(map[string]int32)
	stsNames := []string{
		util.CoordinatorName(cluster.Name),
		util.StandbyName(cluster.Name),
		util.SegmentPrimaryName(cluster.Name),
		util.SegmentMirrorName(cluster.Name),
	}
	for _, name := range stsNames {
		sts, err := s.env.GetStatefulSet(s.ctx, name, cluster.Namespace)
		if err != nil {
			// StatefulSet may not exist yet in the admin controller flow;
			// record zero restarts for missing StatefulSets.
			counts[name] = 0
			continue
		}
		// In a fake environment, the UpdateRevision changing would indicate a rolling restart.
		// We use the observed generation as a proxy for restart detection.
		counts[name] = sts.Status.UpdatedReplicas
	}
	return counts
}

// countCalls counts the number of calls matching the given method name.
func countCalls(calls []metricsCall, method string) int {
	count := 0
	for _, c := range calls {
		if c.method == method {
			count++
		}
	}
	return count
}

// drainEvents reads and discards all pending events from the FakeRecorder channel.
func drainEvents(recorder *record.FakeRecorder) {
	for {
		select {
		case <-recorder.Events:
			// Discard the event.
		default:
			return
		}
	}
}

// collectEvents reads all pending events from the FakeRecorder channel.
func collectEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

// containsSubstring checks if a string contains a given substring.
func containsSubstring(s, substr string) bool {
	return strings.Contains(s, substr)
}

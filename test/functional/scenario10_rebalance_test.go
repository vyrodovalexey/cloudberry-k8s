//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario10ClusterName = "scenario10-cluster"
	scenario10Namespace   = "cloudberry-test"
)

// Scenario10RebalanceSuite tests manual segment rebalancing with full configuration.
type Scenario10RebalanceSuite struct {
	suite.Suite
	env    *testutil.TestK8sEnv
	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

func TestScenario10_Rebalance(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario10RebalanceSuite))
}

func (s *Scenario10RebalanceSuite) SetupTest() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.logger = slog.Default()
}

func (s *Scenario10RebalanceSuite) TearDownTest() {
	s.cancel()
}

// scenario10Req creates a reconcile request for the scenario 10 cluster.
func scenario10Req() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      scenario10ClusterName,
			Namespace: scenario10Namespace,
		},
	}
}

// --- Test: Rebalance via annotation with full config ---

func (s *Scenario10RebalanceSuite) TestScenario10a_RebalanceViaAnnotation() {
	// Arrange: create a Running cluster with rebalance config.
	cluster := testutil.NewClusterBuilder(scenario10ClusterName, scenario10Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithRebalance(10, 2, []string{"audit_log", "temp_*"}).
		WithAnnotation(util.AnnotationAction, util.ActionRebalance).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		nil, s.env.Builder, mockMetrics, s.logger,
	)

	// Act: run HA controller reconciliation. Without a DB factory the
	// controller falls back to a tracked rebalance Job, so the first
	// reconcile only STARTS the rebalance.
	result, err := reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.Positive(s.T(), result.RequeueAfter,
		"controller should requeue to track the rebalance Job")

	// Verify annotation was removed and the tracking annotation was stamped.
	updated, err := s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	_, hasAction := updated.Annotations[util.AnnotationAction]
	assert.False(s.T(), hasAction, "action annotation should be removed after rebalance")
	jobName := updated.Annotations[util.AnnotationRebalanceJob]
	require.NotEmpty(s.T(), jobName, "rebalance-job tracking annotation should be set")

	// Simulate the rebalance Job reaching a successful terminal state, then
	// reconcile again so the controller observes completion.
	completeScenario10RebalanceJob(s.ctx, s.T(), s.env, jobName)
	_, err = reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "completion reconcile should succeed")

	updated, err = s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	_, hasJobAnnotation := updated.Annotations[util.AnnotationRebalanceJob]
	assert.False(s.T(), hasJobAnnotation,
		"rebalance-job tracking annotation should be cleared after completion")

	// Verify DataRedistribution condition set to RebalanceCompleted.
	redistCond := util.FindCondition(updated.Status.Conditions, "DataRedistribution")
	require.NotNil(s.T(), redistCond, "DataRedistribution condition should be set")
	assert.Equal(s.T(), metav1.ConditionTrue, redistCond.Status,
		"DataRedistribution condition should be True")
	assert.Equal(s.T(), "RebalanceCompleted", redistCond.Reason,
		"DataRedistribution reason should be RebalanceCompleted")

	// Verify RebalanceStarted and RebalanceCompleted events.
	events := collectEvents(fakeRecorder)
	rebalanceStartedFound := false
	rebalanceCompletedFound := false
	for _, event := range events {
		if containsSubstring(event, "RebalanceStarted") {
			rebalanceStartedFound = true
		}
		if containsSubstring(event, "RebalanceCompleted") {
			rebalanceCompletedFound = true
		}
	}
	assert.True(s.T(), rebalanceStartedFound,
		"RebalanceStarted event should be emitted; events: %v", events)
	assert.True(s.T(), rebalanceCompletedFound,
		"RebalanceCompleted event should be emitted; events: %v", events)

	// Verify rebalance Job created.
	jobList, err := listJobsByLabel(s.ctx, s.env, scenario10Namespace,
		util.LabelCluster, scenario10ClusterName)
	require.NoError(s.T(), err)
	rebalanceJobFound := false
	for _, job := range jobList {
		if labels := job.Labels; labels != nil {
			if labels[util.LabelOperation] == util.MaintenanceRebalance {
				rebalanceJobFound = true
				break
			}
		}
	}
	assert.True(s.T(), rebalanceJobFound,
		"rebalance Job should be created")

	// Verify RecordScaleOperation called with "rebalance".
	require.Len(s.T(), mockMetrics.ScaleOperationCalls, 1,
		"RecordScaleOperation should be called once")
	assert.Equal(s.T(), "rebalance", mockMetrics.ScaleOperationCalls[0].Operation,
		"operation should be rebalance")
}

// --- Test: Rebalance status API ---

func (s *Scenario10RebalanceSuite) TestScenario10b_RebalanceStatusAPI() {
	// Arrange: create cluster with DataRedistribution condition and rebalance config.
	cluster := testutil.NewClusterBuilder(scenario10ClusterName, scenario10Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithRebalance(15, 4, []string{"temp_data"}).
		Build()
	cluster.Status.Conditions = util.SetCondition(cluster.Status.Conditions,
		"DataRedistribution", metav1.ConditionTrue, "RebalanceCompleted",
		"Rebalance completed successfully")
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create API server.
	srv := api.NewServer(s.env.Client, nil, nil, &metrics.NoopRecorder{}, s.logger, 0)

	// Act: GET /clusters/{name}/rebalance/status.
	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/api/v1alpha1/clusters/%s/rebalance/status?namespace=%s",
			scenario10ClusterName, scenario10Namespace), nil)
	// Add admin identity to bypass auth middleware.
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Assert.
	assert.Equal(s.T(), http.StatusOK, w.Code,
		"rebalance status should return 200")

	var resp map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &resp)
	require.NoError(s.T(), err, "response should be valid JSON")

	assert.Equal(s.T(), scenario10ClusterName, resp["name"],
		"response should contain cluster name")

	// Verify config is present.
	config, ok := resp["config"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain config")
	assert.Equal(s.T(), float64(15), config["skewThreshold"],
		"skewThreshold should be 15")
	assert.Equal(s.T(), float64(4), config["parallelism"],
		"parallelism should be 4")

	// Verify redistribution status is present.
	redistribution, ok := resp["redistribution"].(map[string]interface{})
	require.True(s.T(), ok, "response should contain redistribution status")
	assert.Equal(s.T(), "True", redistribution["status"],
		"redistribution status should be True")
	assert.Equal(s.T(), "RebalanceCompleted", redistribution["reason"],
		"redistribution reason should be RebalanceCompleted")
}

// --- Test: Rebalance with specific tables ---

func (s *Scenario10RebalanceSuite) TestScenario10c_RebalanceSpecificTables() {
	// Arrange: create a Running cluster.
	cluster := testutil.NewClusterBuilder(scenario10ClusterName, scenario10Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	// Create API server.
	srv := api.NewServer(s.env.Client, nil, nil, &metrics.NoopRecorder{}, s.logger, 0)

	// Act: POST /clusters/{name}/rebalance (sets annotation).
	req := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/v1alpha1/clusters/%s/rebalance?namespace=%s",
			scenario10ClusterName, scenario10Namespace), nil)
	// Add admin identity to bypass auth middleware.
	identity := &auth.Identity{Username: "admin", Permission: auth.PermissionAdmin}
	req = req.WithContext(auth.ContextWithIdentity(req.Context(), identity))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)

	// Assert: annotation should be set.
	assert.Equal(s.T(), http.StatusAccepted, w.Code,
		"rebalance POST should return 202")

	updated, err := s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), util.ActionRebalance, updated.Annotations[util.AnnotationAction],
		"rebalance annotation should be set on cluster")
}

// --- Test: Rebalance metrics ---

func (s *Scenario10RebalanceSuite) TestScenario10_RebalanceMetrics() {
	// Arrange: create a Running cluster with rebalance annotation.
	cluster := testutil.NewClusterBuilder(scenario10ClusterName, scenario10Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithAnnotation(util.AnnotationAction, util.ActionRebalance).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		nil, s.env.Builder, mockMetrics, s.logger,
	)

	// Act: first reconcile starts the tracked rebalance Job; the metric is
	// recorded only when the Job reaches a successful terminal state.
	_, err := reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "reconciliation should succeed")
	assert.Empty(s.T(), mockMetrics.ScaleOperationCalls,
		"metric must not be recorded before the rebalance Job completes")

	updated, err := s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	jobName := updated.Annotations[util.AnnotationRebalanceJob]
	require.NotEmpty(s.T(), jobName, "rebalance-job tracking annotation should be set")

	completeScenario10RebalanceJob(s.ctx, s.T(), s.env, jobName)
	_, err = reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "completion reconcile should succeed")

	// Verify RecordScaleOperation called with "rebalance".
	require.Len(s.T(), mockMetrics.ScaleOperationCalls, 1,
		"RecordScaleOperation should be called once")
	assert.Equal(s.T(), "rebalance", mockMetrics.ScaleOperationCalls[0].Operation,
		"operation should be rebalance")
	assert.Equal(s.T(), scenario10ClusterName, mockMetrics.ScaleOperationCalls[0].Cluster,
		"cluster name should match")
	assert.Equal(s.T(), scenario10Namespace, mockMetrics.ScaleOperationCalls[0].Namespace,
		"namespace should match")
}

// --- Test: Default rebalance config ---

func (s *Scenario10RebalanceSuite) TestScenario10_DefaultRebalanceConfig() {
	// Arrange: create a Running cluster WITHOUT rebalance config.
	cluster := testutil.NewClusterBuilder(scenario10ClusterName, scenario10Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithAnnotation(util.AnnotationAction, util.ActionRebalance).
		Build()
	s.env = testutil.NewTestK8sEnv(cluster)

	fakeRecorder := record.NewFakeRecorder(100)
	mockMetrics := &MockMetricsRecorder{}

	reconciler := controller.NewHAReconciler(
		s.env.Client, s.env.Scheme, fakeRecorder,
		nil, s.env.Builder, mockMetrics, s.logger,
	)

	// Act: first reconcile starts the tracked rebalance Job.
	_, err := reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify defaults used: check events contain default values.
	events := collectEvents(fakeRecorder)
	rebalanceStartedFound := false
	for _, event := range events {
		if containsSubstring(event, "RebalanceStarted") &&
			containsSubstring(event, "threshold=10%") &&
			containsSubstring(event, "parallelism=2") {
			rebalanceStartedFound = true
			break
		}
	}
	assert.True(s.T(), rebalanceStartedFound,
		"RebalanceStarted event should contain default threshold=10%% and parallelism=2; events: %v", events)

	// Simulate the rebalance Job succeeding, then reconcile to observe it.
	updated, err := s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	jobName := updated.Annotations[util.AnnotationRebalanceJob]
	require.NotEmpty(s.T(), jobName, "rebalance-job tracking annotation should be set")

	completeScenario10RebalanceJob(s.ctx, s.T(), s.env, jobName)
	_, err = reconciler.Reconcile(s.ctx, scenario10Req())
	require.NoError(s.T(), err, "completion reconcile should succeed")

	// Verify DataRedistribution condition set.
	updated, err = s.env.GetCluster(s.ctx, scenario10ClusterName, scenario10Namespace)
	require.NoError(s.T(), err)
	redistCond := util.FindCondition(updated.Status.Conditions, "DataRedistribution")
	require.NotNil(s.T(), redistCond, "DataRedistribution condition should be set")
	assert.Equal(s.T(), "RebalanceCompleted", redistCond.Reason,
		"DataRedistribution reason should be RebalanceCompleted")

	// Verify RecordScaleOperation called.
	require.Len(s.T(), mockMetrics.ScaleOperationCalls, 1,
		"RecordScaleOperation should be called once")
	assert.Equal(s.T(), "rebalance", mockMetrics.ScaleOperationCalls[0].Operation,
		"operation should be rebalance")
}

// completeScenario10RebalanceJob marks the tracked rebalance Job as
// succeeded so the next reconcile observes a successful terminal state.
func completeScenario10RebalanceJob(
	ctx context.Context,
	t *testing.T,
	env *testutil.TestK8sEnv,
	jobName string,
) {
	t.Helper()
	job := &batchv1.Job{}
	require.NoError(t, env.Client.Get(ctx, types.NamespacedName{
		Name:      jobName,
		Namespace: scenario10Namespace,
	}, job), "tracked rebalance Job should exist")
	job.Status.Succeeded = 1
	require.NoError(t, env.Client.Status().Update(ctx, job),
		"updating rebalance Job status to succeeded")
}

// listJobsByLabel lists Jobs in a namespace matching a label key/value.
func listJobsByLabel(
	ctx context.Context,
	env *testutil.TestK8sEnv,
	namespace, labelKey, labelValue string,
) ([]struct{ Labels map[string]string }, error) {
	// Use the fake client to list all jobs and filter by label.
	jobList := &batchv1.JobList{}
	if err := env.Client.List(ctx, jobList,
		client.InNamespace(namespace),
		client.MatchingLabels{labelKey: labelValue},
	); err != nil {
		return nil, fmt.Errorf("listing jobs: %w", err)
	}

	var result []struct{ Labels map[string]string }
	for i := range jobList.Items {
		result = append(result, struct{ Labels map[string]string }{
			Labels: jobList.Items[i].Labels,
		})
	}
	return result, nil
}

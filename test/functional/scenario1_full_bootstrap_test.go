//go:build functional

package functional

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

const (
	scenario1ClusterName = "scenario1-cluster"
	scenario1Namespace   = "cloudberry-test"
)

// Scenario1FullBootstrapSuite tests the full cluster bootstrap scenario end-to-end.
type Scenario1FullBootstrapSuite struct {
	suite.Suite
	ctx context.Context
}

func TestScenario1_FullBootstrap(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario1FullBootstrapSuite))
}

func (s *Scenario1FullBootstrapSuite) SetupTest() {
	s.ctx = context.Background()
}

// buildScenario1Cluster constructs the Scenario 1 cluster CR with all required configuration.
func buildScenario1Cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario1ClusterName, scenario1Namespace).
		WithVersion("7.1.0").
		WithSegments(4).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		WithConfig(map[string]string{
			"max_connections":                    "200",
			"shared_buffers":                     "2GB",
			"gp_enable_global_deadlock_detector": "on",
		}).
		WithDeletionPolicy(cbv1alpha1.DeletionPolicyRetain).
		WithBasicAuth(true, "gpadmin").
		WithMonitoring(true, 9187).
		WithPendingGeneration().
		Build()

	cluster.Spec.Config.CoordinatorParameters = map[string]string{
		"log_min_duration_statement": "500",
	}
	cluster.Spec.Config.DatabaseParameters = map[string]map[string]string{
		"mydb": {"work_mem": "256MB"},
	}
	cluster.Spec.Config.RoleParameters = map[string]map[string]string{
		"analyst": {"statement_mem": "1GB"},
	}
	cluster.Spec.BackupOnDelete = true

	return cluster
}

// reqFor creates a reconcile request for the given cluster.
func reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// reconcileUntilStable runs reconciliation cycles until the cluster reaches a stable state
// or the maximum number of iterations is reached.
func reconcileUntilStable(
	ctx context.Context,
	reconciler *controller.ClusterReconciler,
	req ctrl.Request,
	maxIterations int,
) error {
	for i := range maxIterations {
		result, err := reconciler.Reconcile(ctx, req)
		if err != nil {
			return err
		}
		// If no requeue is requested, the reconciler has reached a stable state.
		if !result.Requeue && result.RequeueAfter == 0 {
			break
		}
		_ = i
	}
	return nil
}

// simulateStatefulSetReady updates a StatefulSet's status to simulate readiness.
func simulateStatefulSetReady(
	ctx context.Context,
	env *testutil.TestK8sEnv,
	name, namespace string,
) error {
	sts := &appsv1.StatefulSet{}
	if err := env.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return err
	}
	if sts.Spec.Replicas != nil {
		sts.Status.ReadyReplicas = *sts.Spec.Replicas
		sts.Status.Replicas = *sts.Spec.Replicas
	}
	return env.Client.Status().Update(ctx, sts)
}

// --- Test Methods ---

func (s *Scenario1FullBootstrapSuite) TestScenario1_WebhookValidation_NegativeTest() {
	// Create an INVALID cluster: segments.count = 0.
	invalidCluster := testutil.NewClusterBuilder("invalid-cluster", scenario1Namespace).
		WithSegments(0).
		Build()

	validator := webhook.NewCloudberryClusterValidator(nil)

	// Assert ValidateCreate returns an error for the invalid cluster.
	_, err := validator.ValidateCreate(s.ctx, invalidCluster)
	require.Error(s.T(), err, "validation should fail for segments.count = 0")
	assert.Contains(s.T(), err.Error(), "segments.count")

	// Create another INVALID cluster: OIDC enabled without issuerURL.
	invalidOIDCCluster := testutil.NewClusterBuilder("invalid-oidc-cluster", scenario1Namespace).
		WithOIDC(true, "", "client-id").
		Build()

	_, err = validator.ValidateCreate(s.ctx, invalidOIDCCluster)
	require.Error(s.T(), err, "validation should fail for OIDC without issuerURL")
	assert.Contains(s.T(), err.Error(), "issuerURL")

	// Now create the VALID Scenario 1 cluster and assert ValidateCreate succeeds.
	validCluster := buildScenario1Cluster()
	warnings, err := validator.ValidateCreate(s.ctx, validCluster)
	require.NoError(s.T(), err, "validation should succeed for valid Scenario 1 cluster")
	assert.Empty(s.T(), warnings, "no warnings expected for valid cluster")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_ResourceCreation() {
	cluster := buildScenario1Cluster()
	cluster.Finalizers = append(cluster.Finalizers, util.FinalizerName)
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	// Run reconciliation to create resources.
	err := reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify admin password Secret was created.
	secret, err := env.GetSecret(s.ctx, util.AdminPasswordSecretName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "admin password Secret should be created")
	assert.NotEmpty(s.T(), secret.Data["password"], "admin password should not be empty")

	// Verify ConfigMap for postgresql.conf was created.
	pgConfCM, err := env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "postgresql.conf ConfigMap should be created")
	assert.Contains(s.T(), pgConfCM.Data, "postgresql.conf", "ConfigMap should contain postgresql.conf key")

	// Verify ConfigMap for pg_hba.conf was created.
	_, err = env.GetConfigMap(s.ctx, util.PgHBAConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "pg_hba.conf ConfigMap should be created")

	// Verify coordinator StatefulSet was created.
	coordSts, err := env.GetStatefulSet(s.ctx, util.CoordinatorName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "coordinator StatefulSet should be created")

	// Verify init container is present in coordinator StatefulSet.
	require.NotEmpty(s.T(), coordSts.Spec.Template.Spec.InitContainers,
		"coordinator StatefulSet should have init containers")
	assert.Equal(s.T(), "init-cloudberry", coordSts.Spec.Template.Spec.InitContainers[0].Name,
		"init container should be named init-cloudberry")

	// Verify standby StatefulSet was created (standby is enabled).
	_, err = env.GetStatefulSet(s.ctx, util.StandbyName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "standby StatefulSet should be created")

	// Verify primary segment StatefulSet was created.
	primarySts, err := env.GetStatefulSet(s.ctx, util.SegmentPrimaryName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "primary segment StatefulSet should be created")
	assert.Equal(s.T(), int32(4), *primarySts.Spec.Replicas,
		"primary segment StatefulSet should have 4 replicas")

	// Verify mirror segment StatefulSet was created (mirroring is enabled).
	mirrorSts, err := env.GetStatefulSet(s.ctx, util.SegmentMirrorName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "mirror segment StatefulSet should be created")
	assert.Equal(s.T(), int32(4), *mirrorSts.Spec.Replicas,
		"mirror segment StatefulSet should have 4 replicas")

	// Verify headless Services were created.
	_, err = env.GetService(s.ctx, util.CoordinatorServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "coordinator service should be created")

	_, err = env.GetService(s.ctx, util.StandbyServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "standby service should be created")

	_, err = env.GetService(s.ctx, util.SegmentServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "segment service should be created")

	_, err = env.GetService(s.ctx, util.ClientServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "client service should be created")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_ConfigLayersApplied() {
	cluster := buildScenario1Cluster()
	cluster.Finalizers = append(cluster.Finalizers, util.FinalizerName)
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	// Run reconciliation to create ConfigMaps.
	err := reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err, "reconciliation should succeed")

	// Verify the postgresql.conf ConfigMap contains cluster-wide parameters.
	pgConfCM, err := env.GetConfigMap(s.ctx, util.PostgresqlConfConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), err, "postgresql.conf ConfigMap should exist")

	confContent := pgConfCM.Data["postgresql.conf"]
	require.NotEmpty(s.T(), confContent, "postgresql.conf content should not be empty")

	// Cluster-wide params.
	assert.Contains(s.T(), confContent, "max_connections", "should contain max_connections")
	assert.Contains(s.T(), confContent, "200", "max_connections should be 200")
	assert.Contains(s.T(), confContent, "shared_buffers", "should contain shared_buffers")
	assert.Contains(s.T(), confContent, "2GB", "shared_buffers should be 2GB")
	assert.Contains(s.T(), confContent, "gp_enable_global_deadlock_detector",
		"should contain gp_enable_global_deadlock_detector")
	assert.Contains(s.T(), confContent, "on",
		"gp_enable_global_deadlock_detector should be on")

	// Verify coordinator-only, database, and role parameters are stored in the cluster spec.
	// These are applied at runtime via the admin controller, but the spec should carry them.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	require.NotNil(s.T(), updated.Spec.Config, "config spec should not be nil")

	// Coordinator-only params.
	assert.Equal(s.T(), "500", updated.Spec.Config.CoordinatorParameters["log_min_duration_statement"],
		"coordinator parameter log_min_duration_statement should be 500")

	// Database params.
	require.Contains(s.T(), updated.Spec.Config.DatabaseParameters, "mydb",
		"database parameters should contain mydb")
	assert.Equal(s.T(), "256MB", updated.Spec.Config.DatabaseParameters["mydb"]["work_mem"],
		"mydb.work_mem should be 256MB")

	// Role params.
	require.Contains(s.T(), updated.Spec.Config.RoleParameters, "analyst",
		"role parameters should contain analyst")
	assert.Equal(s.T(), "1GB", updated.Spec.Config.RoleParameters["analyst"]["statement_mem"],
		"analyst.statement_mem should be 1GB")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_StatusPopulated() {
	cluster := buildScenario1Cluster()
	cluster.Finalizers = append(cluster.Finalizers, util.FinalizerName)
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	// First reconciliation pass: create resources.
	err := reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err, "initial reconciliation should succeed")

	// Simulate all StatefulSets being ready.
	stsNames := []string{
		util.CoordinatorName(cluster.Name),
		util.StandbyName(cluster.Name),
		util.SegmentPrimaryName(cluster.Name),
		util.SegmentMirrorName(cluster.Name),
	}
	for _, name := range stsNames {
		err := simulateStatefulSetReady(s.ctx, env, name, cluster.Namespace)
		require.NoError(s.T(), err, "simulating readiness for %s should succeed", name)
	}

	// Bump generation to force re-reconciliation after status simulation.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Generation = updated.Status.ObservedGeneration + 1
	err = env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	// Second reconciliation pass: update status.
	err = reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err, "status reconciliation should succeed")

	// Fetch the final cluster state.
	final, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	// Verify status fields.
	assert.Equal(s.T(), cbv1alpha1.ClusterPhaseRunning, final.Status.Phase,
		"cluster phase should be Running")
	assert.True(s.T(), final.Status.CoordinatorReady,
		"coordinator should be ready")
	assert.True(s.T(), final.Status.StandbyReady,
		"standby should be ready")
	assert.Equal(s.T(), int32(4), final.Status.SegmentsReady,
		"segments ready should be 4")
	assert.Equal(s.T(), int32(4), final.Status.SegmentsTotal,
		"segments total should be 4")
	assert.Equal(s.T(), cbv1alpha1.MirroringInSync, final.Status.MirroringStatus,
		"mirroring status should be InSync")
	assert.Equal(s.T(), "7.1.0", final.Status.ClusterVersion,
		"cluster version should be 7.1.0")
	assert.Equal(s.T(), final.Generation, final.Status.ObservedGeneration,
		"observed generation should match generation")

	// Verify conditions.
	assert.True(s.T(), util.IsConditionTrue(final.Status.Conditions, string(cbv1alpha1.ConditionClusterReady)),
		"ClusterReady condition should be True")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_MetricsRecorded() {
	cluster := buildScenario1Cluster()
	cluster.Finalizers = append(cluster.Finalizers, util.FinalizerName)
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	mockMetrics := &mockMetricsRecorder{}
	env := testutil.NewTestK8sEnv(cluster)
	env.Metrics = mockMetrics

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, mockMetrics, env.Logger,
	)

	// First reconciliation: create resources.
	err := reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err)

	// Simulate StatefulSets being ready.
	stsNames := []string{
		util.CoordinatorName(cluster.Name),
		util.StandbyName(cluster.Name),
		util.SegmentPrimaryName(cluster.Name),
		util.SegmentMirrorName(cluster.Name),
	}
	for _, name := range stsNames {
		err := simulateStatefulSetReady(s.ctx, env, name, cluster.Namespace)
		require.NoError(s.T(), err)
	}

	// Bump generation to trigger re-reconciliation.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)
	updated.Generation = updated.Status.ObservedGeneration + 1
	err = env.Client.Update(s.ctx, updated)
	require.NoError(s.T(), err)

	// Second reconciliation: update status and record metrics.
	err = reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err)

	// Verify metrics were recorded.
	calls := mockMetrics.getCalls()

	assert.True(s.T(), containsCall(calls, "UpdateClusterInfo"),
		"UpdateClusterInfo should have been called")
	assert.True(s.T(), containsCall(calls, "SetCoordinatorUp"),
		"SetCoordinatorUp should have been called")
	assert.True(s.T(), containsCall(calls, "SetSegmentsReady"),
		"SetSegmentsReady should have been called")
	assert.True(s.T(), containsCall(calls, "SetSegmentsTotal"),
		"SetSegmentsTotal should have been called")
	assert.True(s.T(), containsCall(calls, "RecordReconcile"),
		"RecordReconcile should have been called")

	// Verify specific metric values.
	clusterInfoCalls := filterCalls(calls, "UpdateClusterInfo")
	require.NotEmpty(s.T(), clusterInfoCalls, "UpdateClusterInfo should have been called at least once")
	lastClusterInfo := clusterInfoCalls[len(clusterInfoCalls)-1]
	assert.Equal(s.T(), scenario1ClusterName, lastClusterInfo.args["cluster"],
		"cluster name should match")
	assert.Equal(s.T(), scenario1Namespace, lastClusterInfo.args["namespace"],
		"namespace should match")

	segReadyCalls := filterCalls(calls, "SetSegmentsReady")
	require.NotEmpty(s.T(), segReadyCalls, "SetSegmentsReady should have been called")
	lastSegReady := segReadyCalls[len(segReadyCalls)-1]
	assert.Equal(s.T(), float64(4), lastSegReady.args["count"],
		"segments ready count should be 4")

	segTotalCalls := filterCalls(calls, "SetSegmentsTotal")
	require.NotEmpty(s.T(), segTotalCalls, "SetSegmentsTotal should have been called")
	lastSegTotal := segTotalCalls[len(segTotalCalls)-1]
	assert.Equal(s.T(), float64(4), lastSegTotal.args["count"],
		"segments total count should be 4")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_StructuredLogging() {
	cluster := buildScenario1Cluster()
	cluster.Finalizers = append(cluster.Finalizers, util.FinalizerName)
	cluster.Status.Phase = cbv1alpha1.ClusterPhasePending

	// Create a buffer to capture log output.
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, env.Metrics, logger,
	)

	// Run reconciliation to generate log output.
	err := reconcileUntilStable(s.ctx, reconciler, reqFor(cluster), 5)
	require.NoError(s.T(), err)

	logOutput := logBuf.String()
	require.NotEmpty(s.T(), logOutput, "log output should not be empty")

	// Verify structured log fields are present.
	assert.Contains(s.T(), logOutput, "cluster",
		"log output should contain 'cluster' field")
	assert.Contains(s.T(), logOutput, "namespace",
		"log output should contain 'namespace' field")
	assert.Contains(s.T(), logOutput, "controller",
		"log output should contain 'controller' field")
	assert.Contains(s.T(), logOutput, scenario1ClusterName,
		"log output should contain the cluster name")
	assert.Contains(s.T(), logOutput, scenario1Namespace,
		"log output should contain the namespace")
}

func (s *Scenario1FullBootstrapSuite) TestScenario1_DeletionPolicy() {
	cluster := buildScenario1Cluster()

	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewClusterReconciler(
		env.Client, env.Scheme, env.Recorder,
		env.Builder, env.Metrics, env.Logger,
	)

	// First reconcile: adds finalizer.
	result, err := reconciler.Reconcile(s.ctx, reqFor(cluster))
	require.NoError(s.T(), err)
	assert.Positive(s.T(), result.RequeueAfter, "first reconcile should requeue to add finalizer")

	// Verify the cluster has DeletionPolicy=Retain and BackupOnDelete=true.
	updated, err := env.GetCluster(s.ctx, cluster.Name, cluster.Namespace)
	require.NoError(s.T(), err)

	assert.Equal(s.T(), cbv1alpha1.DeletionPolicyRetain, updated.Spec.DeletionPolicy,
		"deletion policy should be Retain")
	assert.True(s.T(), updated.Spec.BackupOnDelete,
		"backup on delete should be true")

	// Verify the finalizer was added.
	assert.Contains(s.T(), updated.Finalizers, util.FinalizerName,
		"finalizer should be present")
}

// --- Mock Metrics Recorder ---

// metricsCall records a single metrics method invocation.
type metricsCall struct {
	method string
	args   map[string]interface{}
}

// mockMetricsRecorder tracks all metrics method calls for verification.
type mockMetricsRecorder struct {
	mu    sync.Mutex
	calls []metricsCall
}

func (m *mockMetricsRecorder) record(method string, args map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, metricsCall{method: method, args: args})
}

func (m *mockMetricsRecorder) getCalls() []metricsCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]metricsCall, len(m.calls))
	copy(result, m.calls)
	return result
}

// containsCall checks if any call matches the given method name.
func containsCall(calls []metricsCall, method string) bool {
	for _, c := range calls {
		if c.method == method {
			return true
		}
	}
	return false
}

// filterCalls returns all calls matching the given method name.
func filterCalls(calls []metricsCall, method string) []metricsCall {
	var result []metricsCall
	for _, c := range calls {
		if c.method == method {
			result = append(result, c)
		}
	}
	return result
}

// Implement the metrics.Recorder interface.
var _ metrics.Recorder = (*mockMetricsRecorder)(nil)

func (m *mockMetricsRecorder) RecordReconcile(cluster, namespace, result string, duration time.Duration) {
	m.record("RecordReconcile", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result, "duration": duration,
	})
}

func (m *mockMetricsRecorder) UpdateClusterInfo(cluster, namespace, version, phase string, segments float64) {
	m.record("UpdateClusterInfo", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "version": version, "phase": phase, "segments": segments,
	})
}

func (m *mockMetricsRecorder) SetCoordinatorUp(cluster, namespace string, up bool) {
	m.record("SetCoordinatorUp", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "up": up,
	})
}

func (m *mockMetricsRecorder) SetStandbyUp(cluster, namespace string, up bool) {
	m.record("SetStandbyUp", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "up": up,
	})
}

func (m *mockMetricsRecorder) SetSegmentsReady(cluster, namespace string, count float64) {
	m.record("SetSegmentsReady", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetSegmentsTotal(cluster, namespace string, count float64) {
	m.record("SetSegmentsTotal", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetSegmentsFailed(cluster, namespace string, count float64) {
	m.record("SetSegmentsFailed", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetMirroringInSync(cluster, namespace string, inSync bool) {
	m.record("SetMirroringInSync", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "inSync": inSync,
	})
}

func (m *mockMetricsRecorder) RecordFTSProbe(cluster, namespace, result string, duration time.Duration) {
	m.record("RecordFTSProbe", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result, "duration": duration,
	})
}

func (m *mockMetricsRecorder) RecordFTSFailover(cluster, namespace string) {
	m.record("RecordFTSFailover", map[string]interface{}{
		"cluster": cluster, "namespace": namespace,
	})
}

func (m *mockMetricsRecorder) SetSegmentStatus(cluster, namespace, segment string, up bool) {
	m.record("SetSegmentStatus", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "segment": segment, "up": up,
	})
}

func (m *mockMetricsRecorder) SetReplicationLag(cluster, namespace, segment string, lagBytes float64) {
	m.record("SetReplicationLag", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "segment": segment, "bytes": lagBytes,
	})
}

func (m *mockMetricsRecorder) SetStandbyReplicationLag(cluster, namespace string, lagBytes float64) {
	m.record("SetStandbyReplicationLag", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "bytes": lagBytes,
	})
}

func (m *mockMetricsRecorder) RecordConfigReload(cluster, namespace string) {
	m.record("RecordConfigReload", map[string]interface{}{
		"cluster": cluster, "namespace": namespace,
	})
}

func (m *mockMetricsRecorder) SetConnectionsActive(cluster, namespace string, count float64) {
	m.record("SetConnectionsActive", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetConnectionsMax(cluster, namespace string, count float64) {
	m.record("SetConnectionsMax", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetDiskUsageBytes(cluster, namespace, database string, diskBytes float64) {
	m.record("SetDiskUsageBytes", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "database": database, "bytes": diskBytes,
	})
}

func (m *mockMetricsRecorder) RecordAuthAttempt(method, result string) {
	m.record("RecordAuthAttempt", map[string]interface{}{
		"method": method, "result": result,
	})
}

func (m *mockMetricsRecorder) SetActiveQueries(cluster, namespace string, count float64) {
	m.record("SetActiveQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetQueuedQueries(cluster, namespace string, count float64) {
	m.record("SetQueuedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) SetBlockedQueries(cluster, namespace string, count float64) {
	m.record("SetBlockedQueries", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) RecordWorkloadRuleAction(cluster, namespace, rule, action string) {
	m.record("RecordWorkloadRuleAction", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "rule": rule, "action": action,
	})
}

func (m *mockMetricsRecorder) SetResourceGroupUsage(cluster, namespace, group string, cpu, memory float64) {
	m.record("SetResourceGroupUsage", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "group": group, "cpu": cpu, "memory": memory,
	})
}

func (m *mockMetricsRecorder) RecordIdleSessionTermination(cluster, namespace, rule string) {
	m.record("RecordIdleSessionTermination", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "rule": rule,
	})
}

func (m *mockMetricsRecorder) RecordSlowQuery(cluster, namespace string) {
	m.record("RecordSlowQuery", map[string]interface{}{
		"cluster": cluster, "namespace": namespace,
	})
}

func (m *mockMetricsRecorder) RecordBackup(cluster, namespace, backupType, status string) {
	m.record("RecordBackup", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "type": backupType, "status": status,
	})
}

func (m *mockMetricsRecorder) ObserveBackupDuration(cluster, namespace, backupType string, duration time.Duration) {
	m.record("ObserveBackupDuration", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "type": backupType, "duration": duration,
	})
}

func (m *mockMetricsRecorder) SetBackupSizeBytes(cluster, namespace, timestamp string, sizeBytes float64) {
	m.record("SetBackupSizeBytes", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "timestamp": timestamp, "bytes": sizeBytes,
	})
}

func (m *mockMetricsRecorder) SetBackupLastSuccessTimestamp(cluster, namespace string, ts float64) {
	m.record("SetBackupLastSuccessTimestamp", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "ts": ts,
	})
}

func (m *mockMetricsRecorder) SetBackupLastStatus(cluster, namespace string, status float64) {
	m.record("SetBackupLastStatus", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "status": status,
	})
}

func (m *mockMetricsRecorder) ObserveRestoreDuration(cluster, namespace string, duration time.Duration) {
	m.record("ObserveRestoreDuration", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "duration": duration,
	})
}

func (m *mockMetricsRecorder) RecordBackupRetentionDeleted(cluster, namespace string, n int) {
	m.record("RecordBackupRetentionDeleted", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "n": n,
	})
}

func (m *mockMetricsRecorder) SetBackupJobStatus(cluster, namespace, job, operation string, status float64) {
	m.record("SetBackupJobStatus", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "job": job, "operation": operation, "status": status,
	})
}

func (m *mockMetricsRecorder) RecordRestore(cluster, namespace, status string) {
	m.record("RecordRestore", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "status": status,
	})
}

func (m *mockMetricsRecorder) RecordRestoreValidation(cluster, namespace, result string) {
	m.record("RecordRestoreValidation", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result,
	})
}

func (m *mockMetricsRecorder) SetDataLoadingJobsActive(cluster, namespace string, count float64) {
	m.record("SetDataLoadingJobsActive", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "count": count,
	})
}

func (m *mockMetricsRecorder) RecordDataLoadingRows(cluster, namespace, job, sourceType string, count float64) {
	m.record("RecordDataLoadingRows", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "job": job, "sourceType": sourceType, "count": count,
	})
}

func (m *mockMetricsRecorder) SetDiskUsagePercent(cluster, namespace string, percent float64) {
	m.record("SetDiskUsagePercent", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "percent": percent,
	})
}

func (m *mockMetricsRecorder) SetRecommendationsTotal(cluster, namespace, recType string, count float64) {
	m.record("SetRecommendationsTotal", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "type": recType, "count": count,
	})
}

func (m *mockMetricsRecorder) ObserveRecommendationScanDuration(cluster, namespace string, duration time.Duration) {
	m.record("ObserveRecommendationScanDuration", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "duration": duration,
	})
}

func (m *mockMetricsRecorder) SetTableBloatRatio(cluster, namespace, table string, ratio float64) {
	m.record("SetTableBloatRatio", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "table": table, "ratio": ratio,
	})
}

func (m *mockMetricsRecorder) RecordScaleOperation(cluster, namespace, operation string) {
	m.record("RecordScaleOperation", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "operation": operation,
	})
}

func (m *mockMetricsRecorder) SetRedistributionProgress(cluster, namespace string, progress float64) {
	m.record("SetRedistributionProgress", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "progress": progress,
	})
}

func (m *mockMetricsRecorder) SetDataSkewCoefficient(cluster, namespace string, coefficient float64) {
	m.record("SetDataSkewCoefficient", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "coefficient": coefficient,
	})
}

func (m *mockMetricsRecorder) SetPVCSizeBytes(cluster, namespace, component string, sizeBytes float64) {
	m.record("SetPVCSizeBytes", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "component": component, "bytes": sizeBytes,
	})
}

func (m *mockMetricsRecorder) RecordMirroringOperation(cluster, namespace, operation string) {
	m.record("RecordMirroringOperation", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "operation": operation,
	})
}

func (m *mockMetricsRecorder) RecordMaintenanceOperation(cluster, namespace, operation, result string) {
	m.record("RecordMaintenanceOperation", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "operation": operation, "result": result,
	})
}

func (m *mockMetricsRecorder) RecordPasswordRotation() {
	m.record("RecordPasswordRotation", nil)
}

func (m *mockMetricsRecorder) RecordQueryHistoryInsert(_, _ string) {}
func (m *mockMetricsRecorder) ObserveQueryHistorySearchDuration(_, _ string, _ time.Duration) {
}
func (m *mockMetricsRecorder) RecordQueryHistoryExport(_, _, _ string)                 {}
func (m *mockMetricsRecorder) RecordQueryHistoryRetentionCleanup(_, _ string, _ int64) {}
func (m *mockMetricsRecorder) SetQueryHistorySizeBytes(_, _ string, _ float64)         {}
func (m *mockMetricsRecorder) RecordPlanCheck(_, _ string)                             {}
func (m *mockMetricsRecorder) RecordPlanCheckIssue(_, _, _, _ string)                  {}
func (m *mockMetricsRecorder) ObservePlanCheckDuration(_, _ string, _ time.Duration) {
}
func (m *mockMetricsRecorder) RecordQueryCancel(_, _ string)                           {}
func (m *mockMetricsRecorder) RecordQueryMove(_, _ string)                             {}
func (m *mockMetricsRecorder) RecordExporterHealthCheck(_, _ string)                   {}
func (m *mockMetricsRecorder) RecordActiveQueryExport()                                {}
func (m *mockMetricsRecorder) RecordGuestAccess(_, _ string, _ bool)                   {}
func (m *mockMetricsRecorder) RecordMonitorPause(_, _ string)                          {}
func (m *mockMetricsRecorder) RecordMonitorResume(_, _ string)                         {}
func (m *mockMetricsRecorder) RecordMonitoringDisabledAccess(_, _ string)              {}
func (m *mockMetricsRecorder) RecordCertRotation(_, _, _ string)                       {}
func (m *mockMetricsRecorder) SetCertExpirySeconds(_ string, _ float64)                {}
func (m *mockMetricsRecorder) RecordClusterCertIssuance(_, _, _ string)                {}
func (m *mockMetricsRecorder) RecordVaultOperation(_, _ string)                        {}
func (m *mockMetricsRecorder) ObserveVaultOperationDuration(_ string, _ time.Duration) {}
func (m *mockMetricsRecorder) RecordWebhookAdmission(_, _, _ string)                   {}
func (m *mockMetricsRecorder) RecordUpgradeOperation(_, _, _ string)                   {}

// --- Methods added for the new metrics.Recorder surface (API middleware,
// DB instrumentation, idle daemons, backup-on-delete, OIDC, log streaming).
// Controller-relevant ones record their calls for assertions; pure API/DB
// plumbing metrics are no-ops in functional controller tests.

func (m *mockMetricsRecorder) RecordAPIRequest(_, _, _ string, _ time.Duration) {}
func (m *mockMetricsRecorder) AddAPIRequestsInFlight(_ float64)                 {}
func (m *mockMetricsRecorder) RecordRateLimitRejection(_ string)                {}

func (m *mockMetricsRecorder) RecordDBConnect(_, _, _ string, _ time.Duration) {}
func (m *mockMetricsRecorder) ObserveDBQueryDuration(_ string, _ time.Duration) {
}

func (m *mockMetricsRecorder) RegisterDBPoolStats(_, _ string, _ metrics.DBPoolStatsFunc) func() {
	return func() {}
}

func (m *mockMetricsRecorder) SetIdleDaemonUp(cluster, namespace string, up bool) {
	m.record("SetIdleDaemonUp", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "up": up,
	})
}

func (m *mockMetricsRecorder) RecordIdleScanFailure(cluster, namespace string) {
	m.record("RecordIdleScanFailure", map[string]interface{}{
		"cluster": cluster, "namespace": namespace,
	})
}

func (m *mockMetricsRecorder) RecordIdleReconnectAttempt(cluster, namespace, result string) {
	m.record("RecordIdleReconnectAttempt", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result,
	})
}

func (m *mockMetricsRecorder) RecordSessionTermination(cluster, namespace, result string) {
	m.record("RecordSessionTermination", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result,
	})
}

func (m *mockMetricsRecorder) RecordStorageExpansion(cluster, namespace, result string) {
	m.record("RecordStorageExpansion", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result,
	})
}

func (m *mockMetricsRecorder) RecordBackupOnDelete(cluster, namespace, result string) {
	m.record("RecordBackupOnDelete", map[string]interface{}{
		"cluster": cluster, "namespace": namespace, "result": result,
	})
}

func (m *mockMetricsRecorder) ObserveScalePhaseDuration(direction, phase string, d time.Duration) {
	m.record("ObserveScalePhaseDuration", map[string]interface{}{
		"direction": direction, "phase": phase, "duration": d,
	})
}

func (m *mockMetricsRecorder) RecordMigrateOperation(result string) {
	m.record("RecordMigrateOperation", map[string]interface{}{"result": result})
}

func (m *mockMetricsRecorder) RecordAPIClusterOperation(_, _ string)          {}
func (m *mockMetricsRecorder) RecordLogStreamSession(_ string)                {}
func (m *mockMetricsRecorder) AddLogStreamBytes(_ float64)                    {}
func (m *mockMetricsRecorder) RecordOIDCDiscovery(_ string)                   {}
func (m *mockMetricsRecorder) ObserveAuthTokenVerifyDuration(_ time.Duration) {}
func (m *mockMetricsRecorder) RecordRollingRestart(_, _, _ string)            {}
func (m *mockMetricsRecorder) RecordRecoveryOperation(_, _, _, _ string)      {}

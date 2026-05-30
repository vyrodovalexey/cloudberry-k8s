//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/controller"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 54: Exporter Deployment (E2E)
// ============================================================================
//
// This scenario verifies that the exporter deployment resources — credentials
// Secret, queries ConfigMap, sidecar containers, node exporter DaemonSet,
// exporter Service, ServiceMonitor, and PrometheusRule — are correctly built
// and created by the reconciler when query monitoring with full exporter
// configuration is enabled.
//
// ============================================================================

// Scenario54ExporterDeploymentE2ESuite tests the exporter deployment resources
// through the AdminReconciler and builder.
type Scenario54ExporterDeploymentE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario54(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario54ExporterDeploymentE2ESuite))
}

func (s *Scenario54ExporterDeploymentE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario54ExporterDeploymentE2ESuite) reqFor(cluster *cbv1alpha1.CloudberryCluster) ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
}

// scenario54E2EFullExporterCluster returns a cluster with full exporter configuration
// for testing exporter deployment resources.
func scenario54E2EFullExporterCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "30d",
		SamplingInterval:   5,
		GuestAccess:        false,
		PlanCollection:     true,
		SlowQueryThreshold: "1000ms",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "prometheuscommunity/postgres-exporter:0.16.0",
				Port:    9187,
				Resources: &cbv1alpha1.ResourceRequirements{
					Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
					Limits:   &cbv1alpha1.ResourceList{CPU: "500m", Memory: "256Mi"},
				},
			},
			NodeExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "prom/node-exporter:1.8.2",
				Port:    9100,
			},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberry-query-exporter:1.0.0",
				Port:    9188,
				Resources: &cbv1alpha1.ResourceRequirements{
					Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
					Limits:   &cbv1alpha1.ResourceList{CPU: "500m", Memory: "256Mi"},
				},
			},
			ServiceMonitor: &cbv1alpha1.QueryServiceMonitorSpec{
				Enabled:       true,
				Interval:      "15s",
				ScrapeTimeout: "10s",
				Labels:        map[string]string{"team": "platform"},
			},
			PrometheusRule: &cbv1alpha1.QueryPrometheusRuleSpec{
				Enabled: true,
				Labels:  map[string]string{"team": "platform"},
			},
		},
	}
	return cluster
}

// --- 54.1: Exporter credentials Secret is created ---

// TestE2E_Scenario54_ExporterCredentialsSecret_Created verifies that
// after reconciliation, the exporter credentials Secret exists with the
// expected keys (password and dsn).
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_ExporterCredentialsSecret_Created() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	secret, getErr := env.GetSecret(s.ctx,
		util.ExporterCredentialsSecretName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), getErr, "exporter credentials secret should exist")
	assert.Contains(s.T(), secret.Data, "password",
		"secret should contain 'password' key")
	assert.Contains(s.T(), secret.Data, "dsn",
		"secret should contain 'dsn' key")
	assert.NotEmpty(s.T(), secret.Data["password"],
		"password should not be empty")
	assert.NotEmpty(s.T(), secret.Data["dsn"],
		"dsn should not be empty")
}

// --- 54.2: Exporter queries ConfigMap is created ---

// TestE2E_Scenario54_ExporterQueriesConfigMap_Created verifies that
// after reconciliation, the exporter queries ConfigMap exists with the
// expected queries.yaml key.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_ExporterQueriesConfigMap_Created() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	cm, getErr := env.GetConfigMap(s.ctx,
		util.ExporterQueriesConfigMapName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), getErr, "exporter queries configmap should exist")
	assert.Contains(s.T(), cm.Data, "queries.yaml",
		"configmap should contain 'queries.yaml' key")
	assert.NotEmpty(s.T(), cm.Data["queries.yaml"],
		"queries.yaml should not be empty")
}

// --- 54.3: Coordinator StatefulSet has sidecar containers ---

// TestE2E_Scenario54_CoordinatorStatefulSet_HasSidecars verifies that
// the coordinator StatefulSet built by the builder includes the main cloudberry
// container plus the postgres-exporter and cloudberry-query-exporter sidecars.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_CoordinatorStatefulSet_HasSidecars() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	sts, err := b.BuildCoordinatorStatefulSet(cluster)

	// Assert
	require.NoError(s.T(), err, "building coordinator statefulset should succeed")
	require.NotNil(s.T(), sts, "statefulset should not be nil")

	containers := sts.Spec.Template.Spec.Containers
	require.Len(s.T(), containers, 3,
		"coordinator should have 3 containers (cloudberry + 2 exporter sidecars)")

	containerNames := make([]string, len(containers))
	for i, c := range containers {
		containerNames[i] = c.Name
	}
	assert.Contains(s.T(), containerNames, "cloudberry",
		"should contain main cloudberry container")
	assert.Contains(s.T(), containerNames, "postgres-exporter",
		"should contain postgres-exporter sidecar")
	assert.Contains(s.T(), containerNames, "cloudberry-query-exporter",
		"should contain cloudberry-query-exporter sidecar")
}

// --- 54.4: Postgres exporter container args ---

// TestE2E_Scenario54_PostgresExporter_ContainerArgs verifies that the
// postgres-exporter sidecar container has the correct command-line arguments.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_PostgresExporter_ContainerArgs() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	sts, err := b.BuildCoordinatorStatefulSet(cluster)

	// Assert
	require.NoError(s.T(), err, "building coordinator statefulset should succeed")

	var pgExporter *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == "postgres-exporter" {
			pgExporter = &sts.Spec.Template.Spec.Containers[i]
			break
		}
	}
	require.NotNil(s.T(), pgExporter, "postgres-exporter container should exist")

	assert.Contains(s.T(), pgExporter.Args,
		"--extend.query-path=/etc/postgres-exporter/queries.yaml",
		"should have extend.query-path arg")
	assert.Contains(s.T(), pgExporter.Args,
		"--auto-discover-databases",
		"should have auto-discover-databases arg")
	assert.Contains(s.T(), pgExporter.Args,
		"--web.listen-address=:9187",
		"should have web.listen-address arg with port 9187")
}

// --- 54.5: Cloudberry query exporter container args ---

// TestE2E_Scenario54_CloudberryQueryExporter_ContainerArgs verifies that
// the cloudberry-query-exporter sidecar container has the correct arguments
// derived from the cluster's query monitoring configuration.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_CloudberryQueryExporter_ContainerArgs() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	sts, err := b.BuildCoordinatorStatefulSet(cluster)

	// Assert
	require.NoError(s.T(), err, "building coordinator statefulset should succeed")

	var cbdbExporter *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == "cloudberry-query-exporter" {
			cbdbExporter = &sts.Spec.Template.Spec.Containers[i]
			break
		}
	}
	require.NotNil(s.T(), cbdbExporter, "cloudberry-query-exporter container should exist")

	assert.Contains(s.T(), cbdbExporter.Args,
		"--listen-address=:9188",
		"should have listen-address arg with port 9188")
	assert.Contains(s.T(), cbdbExporter.Args,
		"--sampling-interval=5s",
		"should have sampling-interval arg matching cluster config")
	assert.Contains(s.T(), cbdbExporter.Args,
		"--slow-query-threshold=1000ms",
		"should have slow-query-threshold arg matching cluster config")
}

// --- 54.6: Exporter env from Secret ---

// TestE2E_Scenario54_ExporterEnvFromSecret verifies that both exporter
// sidecar containers have the DATA_SOURCE_NAME environment variable sourced
// from the exporter credentials Secret.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_ExporterEnvFromSecret() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	sts, err := b.BuildCoordinatorStatefulSet(cluster)

	// Assert
	require.NoError(s.T(), err, "building coordinator statefulset should succeed")

	expectedSecretName := util.ExporterCredentialsSecretName(cluster.Name)
	exporterContainerNames := []string{"postgres-exporter", "cloudberry-query-exporter"}

	for _, name := range exporterContainerNames {
		var container *corev1.Container
		for i := range sts.Spec.Template.Spec.Containers {
			if sts.Spec.Template.Spec.Containers[i].Name == name {
				container = &sts.Spec.Template.Spec.Containers[i]
				break
			}
		}
		require.NotNil(s.T(), container, "%s container should exist", name)

		var dsnEnv *corev1.EnvVar
		for i := range container.Env {
			if container.Env[i].Name == "DATA_SOURCE_NAME" {
				dsnEnv = &container.Env[i]
				break
			}
		}
		require.NotNil(s.T(), dsnEnv,
			"%s should have DATA_SOURCE_NAME env var", name)
		require.NotNil(s.T(), dsnEnv.ValueFrom,
			"%s DATA_SOURCE_NAME should use ValueFrom", name)
		require.NotNil(s.T(), dsnEnv.ValueFrom.SecretKeyRef,
			"%s DATA_SOURCE_NAME should reference a Secret", name)
		assert.Equal(s.T(), expectedSecretName,
			dsnEnv.ValueFrom.SecretKeyRef.LocalObjectReference.Name,
			"%s DATA_SOURCE_NAME should reference the exporter credentials secret", name)
		assert.Equal(s.T(), "dsn",
			dsnEnv.ValueFrom.SecretKeyRef.Key,
			"%s DATA_SOURCE_NAME should use the 'dsn' key", name)
	}
}

// --- 54.7: Node exporter DaemonSet is created ---

// TestE2E_Scenario54_NodeExporterDaemonSet_Created verifies that after
// reconciliation, the node exporter DaemonSet exists with the correct labels,
// hostPID, hostNetwork, args, and volumes.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_NodeExporterDaemonSet_Created() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	dsName := util.NodeExporterDaemonSetName(cluster.Name)
	ds := &appsv1.DaemonSet{}
	getErr := env.Client.Get(s.ctx, types.NamespacedName{
		Name: dsName, Namespace: cluster.Namespace,
	}, ds)
	require.NoError(s.T(), getErr, "node exporter daemonset should exist")

	// Verify labels
	expectedLabels := util.CommonLabels(cluster.Name, util.ComponentNodeExporter)
	for k, v := range expectedLabels {
		assert.Equal(s.T(), v, ds.Labels[k],
			"daemonset label %s should match", k)
	}

	// Verify hostPID and hostNetwork
	assert.True(s.T(), ds.Spec.Template.Spec.HostPID,
		"daemonset should have hostPID enabled")
	assert.True(s.T(), ds.Spec.Template.Spec.HostNetwork,
		"daemonset should have hostNetwork enabled")

	// Verify container
	require.Len(s.T(), ds.Spec.Template.Spec.Containers, 1,
		"daemonset should have exactly 1 container")
	container := ds.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "node-exporter", container.Name,
		"container name should be 'node-exporter'")
	assert.Equal(s.T(), "prom/node-exporter:1.8.2", container.Image,
		"container image should match")

	// Verify args
	assert.Contains(s.T(), container.Args, "--path.rootfs=/host",
		"should have rootfs path arg")
	assert.Contains(s.T(), container.Args, "--web.listen-address=:9100",
		"should have listen address arg")

	// Verify volumes
	require.NotEmpty(s.T(), ds.Spec.Template.Spec.Volumes,
		"daemonset should have volumes")
	var rootfsVolume *corev1.Volume
	for i := range ds.Spec.Template.Spec.Volumes {
		if ds.Spec.Template.Spec.Volumes[i].Name == "rootfs" {
			rootfsVolume = &ds.Spec.Template.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(s.T(), rootfsVolume, "should have 'rootfs' volume")
	require.NotNil(s.T(), rootfsVolume.HostPath, "rootfs volume should be HostPath")
	assert.Equal(s.T(), "/", rootfsVolume.HostPath.Path,
		"rootfs volume should mount host root")
}

// --- 54.8: Exporter Service is created ---

// TestE2E_Scenario54_ExporterService_Created verifies that after
// reconciliation, the exporter metrics Service exists with ports 9187 and 9188.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_ExporterService_Created() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	env := testutil.NewTestK8sEnv(cluster)

	reconciler := controller.NewAdminReconciler(
		env.Client, env.Scheme, env.Recorder,
		builder.NewBuilder(), nil, &metrics.NoopRecorder{}, env.Logger,
	)

	// Act
	_, err := reconciler.Reconcile(s.ctx, s.reqFor(cluster))

	// Assert
	require.NoError(s.T(), err, "reconcile should succeed")

	svc, getErr := env.GetService(s.ctx,
		util.ExporterMetricsServiceName(cluster.Name), cluster.Namespace)
	require.NoError(s.T(), getErr, "exporter metrics service should exist")

	// Verify service has both exporter ports
	require.Len(s.T(), svc.Spec.Ports, 2,
		"exporter service should have 2 ports")

	portMap := make(map[int32]string)
	for _, p := range svc.Spec.Ports {
		portMap[p.Port] = p.Name
	}
	assert.Contains(s.T(), portMap, int32(9187),
		"service should expose postgres-exporter port 9187")
	assert.Contains(s.T(), portMap, int32(9188),
		"service should expose cloudberry-query-exporter port 9188")
}

// --- 54.9: ServiceMonitor is built correctly ---

// TestE2E_Scenario54_ServiceMonitor_Built verifies that the builder
// produces a correctly structured ServiceMonitor unstructured object with
// the expected kind, API version, and name.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_ServiceMonitor_Built() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	sm := b.BuildQueryMetricsServiceMonitor(cluster)

	// Assert
	require.NotNil(s.T(), sm, "ServiceMonitor should not be nil")
	assert.Equal(s.T(), "ServiceMonitor", sm.GetKind(),
		"kind should be ServiceMonitor")
	assert.Equal(s.T(), "monitoring.coreos.com/v1", sm.GetAPIVersion(),
		"apiVersion should be monitoring.coreos.com/v1")
	assert.Equal(s.T(), util.QueryMetricsServiceMonitorName(cluster.Name), sm.GetName(),
		"name should match expected ServiceMonitor name")
	assert.Equal(s.T(), cluster.Namespace, sm.GetNamespace(),
		"namespace should match cluster namespace")

	// Verify labels include team label from spec
	labels := sm.GetLabels()
	assert.Equal(s.T(), "platform", labels["team"],
		"ServiceMonitor should have team=platform label from spec")

	// Verify spec contains endpoints
	spec, ok := sm.Object["spec"].(map[string]interface{})
	require.True(s.T(), ok, "ServiceMonitor should have spec")
	endpoints, ok := spec["endpoints"].([]interface{})
	require.True(s.T(), ok, "ServiceMonitor spec should have endpoints")
	assert.Len(s.T(), endpoints, 3,
		"ServiceMonitor should have 3 endpoints (pg-exporter, cbdb-exporter, node-metrics)")
}

// --- 54.10: PrometheusRule is built correctly ---

// TestE2E_Scenario54_PrometheusRule_Built verifies that the builder
// produces a correctly structured PrometheusRule unstructured object with
// the expected kind, API version, name, and alert groups.
func (s *Scenario54ExporterDeploymentE2ESuite) TestE2E_Scenario54_PrometheusRule_Built() {
	// Arrange
	cluster := scenario54E2EFullExporterCluster()
	b := builder.NewBuilder()

	// Act
	pr := b.BuildQueryAlertsPrometheusRule(cluster)

	// Assert
	require.NotNil(s.T(), pr, "PrometheusRule should not be nil")
	assert.Equal(s.T(), "PrometheusRule", pr.GetKind(),
		"kind should be PrometheusRule")
	assert.Equal(s.T(), "monitoring.coreos.com/v1", pr.GetAPIVersion(),
		"apiVersion should be monitoring.coreos.com/v1")
	assert.Equal(s.T(), util.QueryAlertsPrometheusRuleName(cluster.Name), pr.GetName(),
		"name should match expected PrometheusRule name")
	assert.Equal(s.T(), cluster.Namespace, pr.GetNamespace(),
		"namespace should match cluster namespace")

	// Verify labels include team label from spec
	labels := pr.GetLabels()
	assert.Equal(s.T(), "platform", labels["team"],
		"PrometheusRule should have team=platform label from spec")

	// Verify spec contains alert groups
	spec, ok := pr.Object["spec"].(map[string]interface{})
	require.True(s.T(), ok, "PrometheusRule should have spec")
	groups, ok := spec["groups"].([]interface{})
	require.True(s.T(), ok, "PrometheusRule spec should have groups")
	assert.NotEmpty(s.T(), groups,
		"PrometheusRule should have at least one alert group")
}

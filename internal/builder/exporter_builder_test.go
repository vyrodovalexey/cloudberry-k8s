package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// newExporterCluster returns a test cluster with query monitoring and all
// exporters enabled so the exporter builders can be exercised end-to-end.
func newExporterCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled:            true,
		SamplingInterval:   5,
		SlowQueryThreshold: "1000ms",
		Exporters: &cbv1alpha1.QueryMonitoringExportersSpec{
			PostgresExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "quay.io/prometheuscommunity/postgres-exporter:v0.15.0",
			},
			NodeExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "quay.io/prometheus/node-exporter:v1.8.0",
			},
			CloudberryQueryExporter: &cbv1alpha1.ExporterSpec{
				Enabled: true,
				Image:   "cloudberrydb/query-exporter:latest",
			},
		},
	}
	return cluster
}

// containerArgs returns the Args slice for the named container, or nil.
func containerArgs(containers []corev1.Container, name string) []string {
	for _, c := range containers {
		if c.Name == name {
			return c.Args
		}
	}
	return nil
}

func TestBuildExporterCredentialsSecret(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	secret := b.BuildExporterCredentialsSecret(cluster, "pw", "host=localhost user=exporter")
	require.NotNil(t, secret)

	assert.Equal(t, util.ExporterCredentialsSecretName("test-cluster"), secret.Name)
	assert.Equal(t, corev1.SecretTypeOpaque, secret.Type)
	assert.Equal(t, []byte("pw"), secret.Data[secretKeyPassword])
	assert.Equal(t, []byte("host=localhost user=exporter"), secret.Data[secretKeyDSN])
	require.Len(t, secret.OwnerReferences, 1)
	assert.Equal(t, "test-cluster", secret.OwnerReferences[0].Name)
}

func TestBuildExporterQueriesConfigMap(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	cm := b.BuildExporterQueriesConfigMap(cluster)
	require.NotNil(t, cm)

	assert.Equal(t, util.ExporterQueriesConfigMapName("test-cluster"), cm.Name)
	queries := cm.Data["queries.yaml"]
	require.NotEmpty(t, queries)

	// The custom queries must be cluster/global-scoped so they can run only once
	// against the default connection database (which is why --auto-discover-databases
	// is unsafe). Assert the global views these queries depend on are present.
	assert.Contains(t, queries, "gp_segment_configuration")
	assert.Contains(t, queries, "pg_stat_replication")
	assert.Contains(t, queries, "pg_stat_activity")

	// The default test cluster declares no resource groups, so the resgroup
	// status query (which references the resource-group-only
	// gp_toolkit.gp_resgroup_status view) must be omitted to avoid the
	// per-scrape parse error on resource-queue clusters.
	assert.NotContains(t, queries, "gp_toolkit.gp_resgroup_status")
	assert.NotContains(t, queries, "cloudberry_resgroup_status")
}

// TestBuildExporterQueriesConfigMap_ResourceGroupsGate verifies the
// resource-group status query is emitted ONLY when the cluster declares
// resource groups. On resource-queue clusters the gp_toolkit.gp_resgroup_status
// view does not exist, so emitting the query causes a parse error on every
// scrape (queryNamespaceMappings returned 1 errors / pg_exporter_last_scrape_error=1).
func TestBuildExporterQueriesConfigMap_ResourceGroupsGate(t *testing.T) {
	b := NewBuilder()

	assertBaseQueries := func(t *testing.T, queries string) {
		t.Helper()
		require.NotEmpty(t, queries)
		// Always-safe queries are present regardless of the resource manager.
		assert.Contains(t, queries, "cloudberry_segment_status")
		assert.Contains(t, queries, "cloudberry_database_stats")
	}

	t.Run("nil Workload omits resgroup query", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.Workload = nil

		queries := b.BuildExporterQueriesConfigMap(cluster).Data["queries.yaml"]
		assertBaseQueries(t, queries)
		assert.NotContains(t, queries, "cloudberry_resgroup_status")
		assert.NotContains(t, queries, "gp_resgroup_status")
	})

	t.Run("empty ResourceGroups omits resgroup query", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled:        true,
			ResourceGroups: []cbv1alpha1.ResourceGroupSpec{},
		}

		queries := b.BuildExporterQueriesConfigMap(cluster).Data["queries.yaml"]
		assertBaseQueries(t, queries)
		assert.NotContains(t, queries, "cloudberry_resgroup_status")
		assert.NotContains(t, queries, "gp_resgroup_status")
	})

	t.Run("resource groups present includes resgroup query", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
			Enabled: true,
			ResourceGroups: []cbv1alpha1.ResourceGroupSpec{
				{Name: "rg_etl"},
			},
		}

		queries := b.BuildExporterQueriesConfigMap(cluster).Data["queries.yaml"]
		assertBaseQueries(t, queries)
		assert.Contains(t, queries, "cloudberry_resgroup_status")
		assert.Contains(t, queries, "gp_toolkit.gp_resgroup_status")
	})
}

func TestBuildExporterSidecarContainers_AllEnabled(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	containers := b.BuildExporterSidecarContainers(cluster)
	require.Len(t, containers, 2)
	assert.Equal(t, pgExporterContainerName, containers[0].Name)
	assert.Equal(t, cbdbExporterContainerName, containers[1].Name)
}

func TestBuildExporterSidecarContainers_PostgresOnly(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter.Enabled = false

	containers := b.BuildExporterSidecarContainers(cluster)
	require.Len(t, containers, 1)
	assert.Equal(t, pgExporterContainerName, containers[0].Name)
}

func TestBuildExporterSidecarContainers_CloudberryOnly(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.PostgresExporter.Enabled = false

	containers := b.BuildExporterSidecarContainers(cluster)
	require.Len(t, containers, 1)
	assert.Equal(t, cbdbExporterContainerName, containers[0].Name)
}

func TestBuildExporterSidecarContainers_NoneEnabled(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.PostgresExporter = nil
	cluster.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter = nil

	containers := b.BuildExporterSidecarContainers(cluster)
	assert.Empty(t, containers)
}

// hasContainer reports whether a container with the given name is present.
func hasContainer(containers []corev1.Container, name string) bool {
	for _, c := range containers {
		if c.Name == name {
			return true
		}
	}
	return false
}

// TestBuildPostgresExporterSidecarContainers verifies the standby-only builder
// returns ONLY the postgres-exporter container and never the
// cloudberry-query-exporter (which is coordinator-only to avoid duplicate
// cluster-global metric series from a non-promoted standby).
func TestBuildPostgresExporterSidecarContainers(t *testing.T) {
	b := NewBuilder()

	t.Run("postgres enabled returns only postgres-exporter", func(t *testing.T) {
		cluster := newExporterCluster()

		containers := b.BuildPostgresExporterSidecarContainers(cluster)
		require.Len(t, containers, 1)
		assert.Equal(t, pgExporterContainerName, containers[0].Name)
		assert.False(t, hasContainer(containers, cbdbExporterContainerName),
			"cloudberry-query-exporter must never be added to the standby")
		assert.Contains(t, containers[0].Args, "--web.listen-address=:9187")
	})

	t.Run("postgres disabled returns empty", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.QueryMonitoring.Exporters.PostgresExporter.Enabled = false

		containers := b.BuildPostgresExporterSidecarContainers(cluster)
		assert.Empty(t, containers)
	})

	t.Run("postgres nil returns empty", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.QueryMonitoring.Exporters.PostgresExporter = nil

		containers := b.BuildPostgresExporterSidecarContainers(cluster)
		assert.Empty(t, containers)
	})

	t.Run("cloudberry enabled is still excluded", func(t *testing.T) {
		cluster := newExporterCluster()
		// Even with the cloudberry-query-exporter enabled, the standby builder
		// must not include it.
		cluster.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter.Enabled = true

		containers := b.BuildPostgresExporterSidecarContainers(cluster)
		require.Len(t, containers, 1)
		assert.Equal(t, pgExporterContainerName, containers[0].Name)
	})
}

// TestPostgresExporterContainerArgs verifies the postgres-exporter sidecar Args
// are correct after the duplicate-database fix.
func TestPostgresExporterContainerArgs(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	containers := b.BuildExporterSidecarContainers(cluster)
	args := containerArgs(containers, pgExporterContainerName)
	require.NotNil(t, args)

	// The custom queries and listen address must still be configured.
	assert.Contains(t, args, "--extend.query-path=/etc/postgres-exporter/queries.yaml")
	assert.Contains(t, args, "--web.listen-address=:9187")
	// The redundant built-in stat_user_tables collector must be disabled: on
	// Cloudberry it always errors ("query plan with multiple segworker groups
	// is not supported") and the custom cloudberry_table_stats query already
	// covers per-table stats.
	assert.Contains(t, args, "--no-collector.stat_user_tables")
	// The redundant built-in settings collector must be disabled: on Cloudberry
	// it always errors ("Scan error on column index 3, name \"short_desc\":
	// converting NULL to string is unsupported") because Cloudberry's
	// pg_settings view exposes NULL short_desc values. The custom
	// cloudberry_connections_max query already exposes max_connections.
	assert.Contains(t, args, "--disable-settings-metrics")
}

// TestPostgresExporterContainer_DisablesStatUserTables verifies the redundant
// built-in stat_user_tables and settings collectors are disabled for the
// coordinator, standby and segment (utility-mode) postgres-exporter variants.
// Both collectors always error on Cloudberry (stat_user_tables: "query plan
// with multiple segworker groups is not supported"; settings: NULL short_desc)
// and their useful metrics are covered by the custom queries.
func TestPostgresExporterContainer_DisablesStatUserTables(t *testing.T) {
	b := NewBuilder()

	t.Run("coordinator", func(t *testing.T) {
		cluster := newExporterCluster()
		args := containerArgs(b.BuildExporterSidecarContainers(cluster), pgExporterContainerName)
		require.NotNil(t, args)
		assert.Contains(t, args, argNoCollectorStatUserTables)
		assert.Contains(t, args, argDisableSettingsMetrics)
	})

	t.Run("standby", func(t *testing.T) {
		cluster := newExporterCluster()
		args := containerArgs(b.BuildPostgresExporterSidecarContainers(cluster), pgExporterContainerName)
		require.NotNil(t, args)
		assert.Contains(t, args, argNoCollectorStatUserTables)
		assert.Contains(t, args, argDisableSettingsMetrics)
	})

	t.Run("segment", func(t *testing.T) {
		cluster := newExporterCluster()
		args := containerArgs(b.BuildSegmentPostgresExporterSidecarContainers(cluster), pgExporterContainerName)
		require.NotNil(t, args)
		assert.Contains(t, args, argNoCollectorStatUserTables)
		assert.Contains(t, args, argDisableSettingsMetrics)
	})

	t.Run("segment-utility-mode", func(t *testing.T) {
		cluster := newExporterCluster()
		container := buildPostgresExporterContainer(cluster, true)
		assert.Contains(t, container.Args, argNoCollectorStatUserTables)
		assert.Contains(t, container.Args, argDisableSettingsMetrics)
	})
}

// TestPostgresExporterContainer_NoAutoDiscoverDatabases is a regression guard for
// the duplicate-metric (HTTP 500) bug. With --auto-discover-databases the global
// custom queries were run per-database and collided in the Prometheus collector
// whenever the cluster had more than one user database. The arg must stay absent.
func TestPostgresExporterContainer_NoAutoDiscoverDatabases(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	containers := b.BuildExporterSidecarContainers(cluster)
	args := containerArgs(containers, pgExporterContainerName)
	require.NotNil(t, args)

	assert.NotContains(t, args, "--auto-discover-databases",
		"--auto-discover-databases must not be set: it re-runs global queries per "+
			"database and causes duplicate-metric collisions (HTTP 500)")
}

func TestBuildPostgresExporterContainer_Fields(t *testing.T) {
	cluster := newExporterCluster()
	container := buildPostgresExporterContainer(cluster, false)

	assert.Equal(t, pgExporterContainerName, container.Name)
	assert.Equal(t, "quay.io/prometheuscommunity/postgres-exporter:v0.15.0", container.Image)

	require.Len(t, container.Ports, 1)
	assert.Equal(t, pgExporterPort, container.Ports[0].ContainerPort)

	// Non-utility (coordinator/standby) exporters carry ONLY the DSN env var and
	// must NOT have PGOPTIONS, which would force utility mode and break the
	// coordinator connection.
	require.Len(t, container.Env, 1)
	assert.Equal(t, "DATA_SOURCE_NAME", container.Env[0].Name)
	require.NotNil(t, container.Env[0].ValueFrom)
	require.NotNil(t, container.Env[0].ValueFrom.SecretKeyRef)
	assert.Equal(t, secretKeyDSN, container.Env[0].ValueFrom.SecretKeyRef.Key)
	assert.False(t, hasEnvVar(container.Env, envPGOptions),
		"coordinator/standby exporter must not set PGOPTIONS")

	require.Len(t, container.VolumeMounts, 1)
	assert.Equal(t, exporterQueriesVolumeName, container.VolumeMounts[0].Name)
	assert.Equal(t, exporterQueriesMountPath, container.VolumeMounts[0].MountPath)
	assert.True(t, container.VolumeMounts[0].ReadOnly)
}

// TestBuildPostgresExporterContainer_UtilityMode verifies the segment variant
// injects PGOPTIONS=-c gp_role=utility so libpq connects to primary segments in
// utility mode (Cloudberry rejects normal client connections to segments). The
// DSN env var must still be present and unchanged.
func TestBuildPostgresExporterContainer_UtilityMode(t *testing.T) {
	cluster := newExporterCluster()
	container := buildPostgresExporterContainer(cluster, true)

	assert.Equal(t, pgExporterContainerName, container.Name)

	require.Len(t, container.Env, 2)
	assert.Equal(t, "DATA_SOURCE_NAME", container.Env[0].Name)
	assert.Equal(t, envPGOptions, container.Env[1].Name)
	assert.Equal(t, "-c gp_role=utility", container.Env[1].Value)
	assert.Equal(t, segmentExporterPGOptions, container.Env[1].Value)
}

// hasEnvVar reports whether an env var with the given name is present.
func hasEnvVar(env []corev1.EnvVar, name string) bool {
	for _, e := range env {
		if e.Name == name {
			return true
		}
	}
	return false
}

// envVarValue returns the literal Value of the named env var, or "".
func envVarValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// containerByName returns the container with the given name, or nil.
func containerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

// TestBuildSegmentPostgresExporterSidecarContainers verifies the SEGMENT builder
// returns ONLY the postgres-exporter container in utility mode (with PGOPTIONS),
// and never the cloudberry-query-exporter.
func TestBuildSegmentPostgresExporterSidecarContainers(t *testing.T) {
	b := NewBuilder()

	t.Run("postgres enabled returns utility-mode exporter", func(t *testing.T) {
		cluster := newExporterCluster()

		containers := b.BuildSegmentPostgresExporterSidecarContainers(cluster)
		require.Len(t, containers, 1)
		assert.Equal(t, pgExporterContainerName, containers[0].Name)
		assert.False(t, hasContainer(containers, cbdbExporterContainerName),
			"cloudberry-query-exporter must never be added to segments")
		assert.Equal(t, segmentExporterPGOptions,
			envVarValue(containers[0].Env, envPGOptions),
			"segment exporter must connect in utility mode via PGOPTIONS")
	})

	t.Run("postgres disabled returns empty", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.QueryMonitoring.Exporters.PostgresExporter.Enabled = false

		containers := b.BuildSegmentPostgresExporterSidecarContainers(cluster)
		assert.Empty(t, containers)
	})

	t.Run("postgres nil returns empty", func(t *testing.T) {
		cluster := newExporterCluster()
		cluster.Spec.QueryMonitoring.Exporters.PostgresExporter = nil

		containers := b.BuildSegmentPostgresExporterSidecarContainers(cluster)
		assert.Empty(t, containers)
	})
}

// TestPostgresExporterUtilityMode_NoLeakToCoordinatorStandby is the regression
// guard that utility mode must NOT leak to the coordinator/standby exporters.
// The standby builder (BuildPostgresExporterSidecarContainers) must produce a
// postgres-exporter WITHOUT PGOPTIONS, while the segment builder must include it.
func TestPostgresExporterUtilityMode_NoLeakToCoordinatorStandby(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	standby := b.BuildPostgresExporterSidecarContainers(cluster)
	require.Len(t, standby, 1)
	assert.False(t, hasEnvVar(standby[0].Env, envPGOptions),
		"standby exporter must NOT set PGOPTIONS (utility mode must not leak)")

	coordinator := b.BuildExporterSidecarContainers(cluster)
	pg := containerByName(coordinator, pgExporterContainerName)
	require.NotNil(t, pg)
	assert.False(t, hasEnvVar(pg.Env, envPGOptions),
		"coordinator exporter must NOT set PGOPTIONS (utility mode must not leak)")

	segment := b.BuildSegmentPostgresExporterSidecarContainers(cluster)
	require.Len(t, segment, 1)
	assert.True(t, hasEnvVar(segment[0].Env, envPGOptions),
		"segment exporter MUST set PGOPTIONS for utility mode")
}

func TestBuildPostgresExporterContainer_WithResources(t *testing.T) {
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.PostgresExporter.Resources =
		&cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
			Limits:   &cbv1alpha1.ResourceList{CPU: "200m", Memory: "256Mi"},
		}

	container := buildPostgresExporterContainer(cluster, false)
	assert.NotEmpty(t, container.Resources.Requests)
	assert.NotEmpty(t, container.Resources.Limits)
}

func TestBuildPostgresExporterContainer_InvalidResources(t *testing.T) {
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.PostgresExporter.Resources =
		&cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "invalid-cpu"},
		}

	// Invalid resources are skipped (not applied), the container is still built.
	container := buildPostgresExporterContainer(cluster, false)
	assert.Empty(t, container.Resources.Requests)
}

func TestBuildCloudberryQueryExporterContainer_Defaults(t *testing.T) {
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.SamplingInterval = 0
	cluster.Spec.QueryMonitoring.SlowQueryThreshold = ""

	container := buildCloudberryQueryExporterContainer(cluster)
	assert.Equal(t, cbdbExporterContainerName, container.Name)
	assert.Contains(t, container.Args, "--listen-address=:9188")
	assert.Contains(t, container.Args, "--sampling-interval=5s")
	assert.Contains(t, container.Args, "--slow-query-threshold=1000ms")
}

func TestBuildCloudberryQueryExporterContainer_CustomArgs(t *testing.T) {
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.SamplingInterval = 10
	cluster.Spec.QueryMonitoring.SlowQueryThreshold = "500ms"
	cluster.Spec.QueryMonitoring.PlanCollection = true
	cluster.Spec.QueryMonitoring.HistoryRetention = "30d"

	container := buildCloudberryQueryExporterContainer(cluster)
	assert.Contains(t, container.Args, "--sampling-interval=10s")
	assert.Contains(t, container.Args, "--slow-query-threshold=500ms")
	assert.Contains(t, container.Args, "--plan-collection")
	assert.Contains(t, container.Args, "--history-retention=30d")
}

func TestBuildCloudberryQueryExporterContainer_WithResources(t *testing.T) {
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter.Resources =
		&cbv1alpha1.ResourceRequirements{
			Requests: &cbv1alpha1.ResourceList{CPU: "100m", Memory: "128Mi"},
		}

	container := buildCloudberryQueryExporterContainer(cluster)
	assert.NotEmpty(t, container.Resources.Requests)
}

func TestBuildExporterSidecarVolumes(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	volumes := b.BuildExporterSidecarVolumes(cluster)
	require.Len(t, volumes, 1)
	assert.Equal(t, exporterQueriesVolumeName, volumes[0].Name)
	require.NotNil(t, volumes[0].ConfigMap)
	assert.Equal(t, util.ExporterQueriesConfigMapName("test-cluster"),
		volumes[0].ConfigMap.Name)
	require.NotNil(t, volumes[0].ConfigMap.Optional)
	assert.True(t, *volumes[0].ConfigMap.Optional)
}

func TestBuildNodeExporterDaemonSet(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	ds := b.BuildNodeExporterDaemonSet(cluster)
	require.NotNil(t, ds)

	assert.Equal(t, util.NodeExporterDaemonSetName("test-cluster"), ds.Name)
	assert.True(t, ds.Spec.Template.Spec.HostPID)
	assert.True(t, ds.Spec.Template.Spec.HostNetwork)

	require.Len(t, ds.Spec.Template.Spec.Containers, 1)
	container := ds.Spec.Template.Spec.Containers[0]
	assert.Equal(t, nodeExporterContainerName, container.Name)
	assert.Equal(t, "quay.io/prometheus/node-exporter:v1.8.0", container.Image)
	assert.Contains(t, container.Args, "--web.listen-address=:9100")
	require.Len(t, container.Ports, 1)
	assert.Equal(t, nodeExporterPort, container.Ports[0].ContainerPort)
	require.NotNil(t, container.SecurityContext.ReadOnlyRootFilesystem)
	assert.True(t, *container.SecurityContext.ReadOnlyRootFilesystem)

	require.Len(t, ds.Spec.Template.Spec.Volumes, 1)
	assert.Equal(t, "rootfs", ds.Spec.Template.Spec.Volumes[0].Name)
	require.NotNil(t, ds.Spec.Template.Spec.Volumes[0].HostPath)
}

func TestBuildExporterService(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	svc := b.BuildExporterService(cluster)
	require.NotNil(t, svc)

	assert.Equal(t, util.ExporterMetricsServiceName("test-cluster"), svc.Name)
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, promScrapeTrue, svc.Annotations["prometheus.io/scrape"])

	require.Len(t, svc.Spec.Ports, 2)
	assert.Equal(t, pgExporterPortName, svc.Spec.Ports[0].Name)
	assert.Equal(t, pgExporterPort, svc.Spec.Ports[0].Port)
	assert.Equal(t, cbdbExporterPortName, svc.Spec.Ports[1].Name)
	assert.Equal(t, cbdbExporterPort, svc.Spec.Ports[1].Port)
}

func TestBuildQueryMetricsServiceMonitor_Defaults(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	sm := b.BuildQueryMetricsServiceMonitor(cluster)
	require.NotNil(t, sm)

	assert.Equal(t, serviceMonitorKind, sm.Object[keyKind])
	metadata, ok := sm.Object["metadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "default", metadata["namespace"])
	assert.Equal(t, util.QueryMetricsServiceMonitorName("test-cluster"), metadata[keyName])

	spec, ok := sm.Object["spec"].(map[string]interface{})
	require.True(t, ok)
	endpoints, ok := spec["endpoints"].([]interface{})
	require.True(t, ok)
	require.Len(t, endpoints, 3)
}

func TestBuildQueryMetricsServiceMonitor_CustomNamespaceLabelsInterval(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.ServiceMonitor = &cbv1alpha1.QueryServiceMonitorSpec{
		Enabled:   true,
		Namespace: "monitoring",
		Interval:  "30s",
		Labels:    map[string]string{"team": "platform"},
	}

	sm := b.BuildQueryMetricsServiceMonitor(cluster)
	require.NotNil(t, sm)

	metadata, ok := sm.Object["metadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "monitoring", metadata["namespace"])

	labels, ok := metadata[keyLabels].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "platform", labels["team"])

	spec, ok := sm.Object["spec"].(map[string]interface{})
	require.True(t, ok)
	endpoints, ok := spec["endpoints"].([]interface{})
	require.True(t, ok)
	first, ok := endpoints[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "30s", first[keyInterval])
}

func TestBuildQueryAlertsPrometheusRule_Defaults(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()

	pr := b.BuildQueryAlertsPrometheusRule(cluster)
	require.NotNil(t, pr)

	assert.Equal(t, prometheusRuleKind, pr.Object[keyKind])
	metadata, ok := pr.Object["metadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "default", metadata["namespace"])
	assert.Equal(t, util.QueryAlertsPrometheusRuleName("test-cluster"), metadata[keyName])

	spec, ok := pr.Object["spec"].(map[string]interface{})
	require.True(t, ok)
	groups, ok := spec["groups"].([]interface{})
	require.True(t, ok)
	require.Len(t, groups, 4)
}

func TestBuildQueryAlertsPrometheusRule_CustomNamespaceAndLabels(t *testing.T) {
	b := NewBuilder()
	cluster := newExporterCluster()
	cluster.Spec.QueryMonitoring.Exporters.PrometheusRule = &cbv1alpha1.QueryPrometheusRuleSpec{
		Enabled:   true,
		Namespace: "monitoring",
		Labels:    map[string]string{"role": "alert"},
	}

	pr := b.BuildQueryAlertsPrometheusRule(cluster)
	require.NotNil(t, pr)

	metadata, ok := pr.Object["metadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "monitoring", metadata["namespace"])
	labels, ok := metadata[keyLabels].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "alert", labels["role"])
}

func TestOwnerRefMap(t *testing.T) {
	cluster := newExporterCluster()
	ref := ownerRefMap(cluster)
	assert.Equal(t, "CloudberryCluster", ref["kind"])
	assert.Equal(t, "test-cluster", ref["name"])
	assert.Equal(t, true, ref["controller"])
	assert.Equal(t, true, ref["blockOwnerDeletion"])
}

func TestToStringInterfaceMap(t *testing.T) {
	result := toStringInterfaceMap(map[string]string{"a": "1", "b": "2"})
	require.Len(t, result, 2)
	assert.Equal(t, "1", result["a"])
	assert.Equal(t, "2", result["b"])
}

func TestBuildEndpointMap(t *testing.T) {
	ep := buildEndpointMap(pgExporterPortName, "15s")
	assert.Equal(t, pgExporterPortName, ep[keyPort])
	assert.Equal(t, "15s", ep[keyInterval])
	assert.Equal(t, keyMetricsPath, ep["path"])
}

func TestBuildAlertRule(t *testing.T) {
	rule := buildAlertRule("MyAlert", "up == 0", "5m", "critical", "summary text", "desc text")
	assert.Equal(t, "MyAlert", rule[keyAlert])
	assert.Equal(t, "up == 0", rule[keyExpr])
	assert.Equal(t, "5m", rule["for"])

	labels, ok := rule[keyLabels].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "critical", labels[keySeverity])

	annotations, ok := rule[keyAnnotations].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "summary text", annotations[keySummary])
	assert.Equal(t, "desc text", annotations[keyDescription])
}

func TestBuildAlertGroups(t *testing.T) {
	groups := buildAlertGroups("test-cluster")
	require.Len(t, groups, 4)

	first, ok := groups[0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "cloudberry-exporter-health", first[keyName])
}

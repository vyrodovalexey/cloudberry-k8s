package builder

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// optionalEnv is used to mark Secret-backed env vars as optional so pods can
// start before the Secret is created by the admin-controller.
var optionalEnv = true //nolint:gochecknoglobals // shared by sidecar builders

const (
	// promScrapeTrue is the annotation value for enabling Prometheus scraping.
	promScrapeTrue = "true"

	// exporterQueriesVolumeName is the volume name for the exporter queries ConfigMap.
	exporterQueriesVolumeName = "exporter-queries"
	// exporterQueriesMountPath is the mount path for the exporter queries ConfigMap.
	exporterQueriesMountPath = "/etc/postgres-exporter"

	// secretKeyDSN is the key used for the DSN data in the exporter credentials Secret.
	secretKeyDSN = "dsn"

	// pgExporterContainerName is the container name for the postgres exporter sidecar.
	pgExporterContainerName = "postgres-exporter"
	// pgExporterPortName is the port name for the postgres exporter.
	pgExporterPortName = "pg-exporter"
	// pgExporterPort is the default port for the postgres exporter.
	pgExporterPort int32 = 9187

	// cbdbExporterContainerName is the container name for the cloudberry query exporter sidecar.
	cbdbExporterContainerName = "cloudberry-query-exporter"
	// cbdbExporterPortName is the port name for the cloudberry query exporter.
	cbdbExporterPortName = "cbdb-exporter"
	// cbdbExporterPort is the default port for the cloudberry query exporter.
	cbdbExporterPort int32 = 9188

	// nodeExporterContainerName is the container name for the node exporter.
	nodeExporterContainerName = "node-exporter"
	// nodeExporterPortName is the port name for the node exporter.
	nodeExporterPortName = "node-metrics"
	// nodeExporterPort is the default port for the node exporter.
	nodeExporterPort int32 = 9100

	// defaultSamplingInterval is the default sampling interval in seconds.
	defaultSamplingInterval int32 = 5
	// defaultSlowQueryThreshold is the default slow query threshold.
	defaultSlowQueryThreshold = "1000ms"

	// serviceMonitorAPIVersion is the API version for ServiceMonitor/PrometheusRule resources.
	serviceMonitorAPIVersion = "monitoring.coreos.com/v1"
	// serviceMonitorKind is the kind for ServiceMonitor resources.
	serviceMonitorKind = "ServiceMonitor"
	// prometheusRuleKind is the kind for PrometheusRule resources.
	prometheusRuleKind = "PrometheusRule"

	// defaultScrapeInterval is the default Prometheus scrape interval.
	defaultScrapeInterval = "15s"

	// Unstructured map keys used in ServiceMonitor and PrometheusRule construction.
	keyAPIVersion   = "apiVersion"
	keyKind         = "kind"
	keyName         = "name"
	keyLabels       = "labels"
	keyPort         = "port"
	keyInterval     = "interval"
	keyMetricsPath  = "/metrics"
	keyAlert        = "alert"
	keyExpr         = "expr"
	keyRules        = "rules"
	keySeverity     = "severity"
	keyAnnotations  = "annotations"
	keySummary      = "summary"
	keyDescription  = "description"
	severityWarning = "warning"
	alertDuration5m = "5m"
)

// BuildExporterCredentialsSecret builds the exporter credentials Secret containing
// the password and DSN for the monitoring exporters.
func (b *DefaultBuilder) BuildExporterCredentialsSecret(
	cluster *cbv1alpha1.CloudberryCluster, password, dsn string,
) *corev1.Secret {
	labels := util.CommonLabels(cluster.Name, util.ComponentExporter)

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.ExporterCredentialsSecretName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			secretKeyPassword: []byte(password),
			secretKeyDSN:      []byte(dsn),
		},
	}
}

// BuildExporterQueriesConfigMap builds the ConfigMap containing custom queries
// for postgres_exporter to monitor Cloudberry-specific metrics.
func (b *DefaultBuilder) BuildExporterQueriesConfigMap(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.ConfigMap {
	labels := util.CommonLabels(cluster.Name, util.ComponentExporter)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.ExporterQueriesConfigMapName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Data: map[string]string{
			"queries.yaml": exporterQueriesYAML,
		},
	}
}

// BuildExporterSidecarContainers returns the exporter sidecar containers based on
// the cluster's query monitoring configuration. Returns up to 2 containers:
// postgres-exporter and cloudberry-query-exporter.
func (b *DefaultBuilder) BuildExporterSidecarContainers(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Container {
	exporters := cluster.Spec.QueryMonitoring.Exporters
	containers := make([]corev1.Container, 0, 2)

	if exporters.PostgresExporter != nil && exporters.PostgresExporter.Enabled {
		containers = append(containers, buildPostgresExporterContainer(cluster))
	}

	if exporters.CloudberryQueryExporter != nil && exporters.CloudberryQueryExporter.Enabled {
		containers = append(containers, buildCloudberryQueryExporterContainer(cluster))
	}

	return containers
}

// BuildExporterSidecarVolumes returns the volumes required by the exporter sidecar containers.
func (b *DefaultBuilder) BuildExporterSidecarVolumes(
	cluster *cbv1alpha1.CloudberryCluster,
) []corev1.Volume {
	optional := true
	return []corev1.Volume{
		{
			Name: exporterQueriesVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: util.ExporterQueriesConfigMapName(cluster.Name),
					},
					Optional: &optional,
				},
			},
		},
	}
}

// BuildNodeExporterDaemonSet builds the node_exporter DaemonSet for collecting
// OS-level metrics from segment hosts.
func (b *DefaultBuilder) BuildNodeExporterDaemonSet(
	cluster *cbv1alpha1.CloudberryCluster,
) *appsv1.DaemonSet {
	labels := util.CommonLabels(cluster.Name, util.ComponentNodeExporter)
	readOnlyFS := true
	hostPathRoot := "/"

	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.NodeExporterDaemonSetName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					HostPID:     true,
					HostNetwork: true,
					Containers: []corev1.Container{
						{
							Name:  nodeExporterContainerName,
							Image: cluster.Spec.QueryMonitoring.Exporters.NodeExporter.Image,
							Args: []string{
								"--path.rootfs=/host",
								"--web.listen-address=:9100",
								"--collector.filesystem.mount-points-exclude=" +
									"^/(dev|proc|sys|var/lib/docker/.+|var/lib/kubelet/.+)($|/)",
							},
							Ports: []corev1.ContainerPort{
								{
									Name:          nodeExporterPortName,
									ContainerPort: nodeExporterPort,
									HostPort:      nodeExporterPort,
									Protocol:      corev1.ProtocolTCP,
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "rootfs",
									MountPath: "/host",
									ReadOnly:  true,
								},
							},
							SecurityContext: &corev1.SecurityContext{
								ReadOnlyRootFilesystem: &readOnlyFS,
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "rootfs",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: hostPathRoot,
								},
							},
						},
					},
				},
			},
		},
	}
}

// BuildExporterService builds the exporter metrics Service that exposes
// the postgres-exporter and cloudberry-query-exporter sidecar ports.
func (b *DefaultBuilder) BuildExporterService(
	cluster *cbv1alpha1.CloudberryCluster,
) *corev1.Service {
	labels := util.CommonLabels(cluster.Name, util.ComponentExporter)
	selectorLabels := util.CommonLabels(cluster.Name, util.ComponentCoordinator)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.ExporterMetricsServiceName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			Annotations: map[string]string{
				"prometheus.io/scrape": promScrapeTrue,
				"prometheus.io/path":   "/metrics",
			},
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: selectorLabels,
			Ports: []corev1.ServicePort{
				{
					Name:       pgExporterPortName,
					Port:       pgExporterPort,
					TargetPort: intstr.FromInt32(pgExporterPort),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       cbdbExporterPortName,
					Port:       cbdbExporterPort,
					TargetPort: intstr.FromInt32(cbdbExporterPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
}

// BuildQueryMetricsServiceMonitor builds an unstructured ServiceMonitor resource
// for Prometheus Operator to discover the exporter endpoints.
func (b *DefaultBuilder) BuildQueryMetricsServiceMonitor(
	cluster *cbv1alpha1.CloudberryCluster,
) *unstructured.Unstructured {
	exporters := cluster.Spec.QueryMonitoring.Exporters

	namespace := cluster.Namespace
	if exporters.ServiceMonitor != nil && exporters.ServiceMonitor.Namespace != "" {
		namespace = exporters.ServiceMonitor.Namespace
	}

	// Build labels: merge spec labels with common labels.
	labels := util.CommonLabels(cluster.Name, util.ComponentExporter)
	if exporters.ServiceMonitor != nil {
		for k, v := range exporters.ServiceMonitor.Labels {
			labels[k] = v
		}
	}

	// Determine scrape interval.
	interval := defaultScrapeInterval
	if exporters.ServiceMonitor != nil && exporters.ServiceMonitor.Interval != "" {
		interval = exporters.ServiceMonitor.Interval
	}

	// Build endpoints for each exporter.
	endpoints := []interface{}{
		buildEndpointMap(pgExporterPortName, interval),
		buildEndpointMap(cbdbExporterPortName, interval),
		buildEndpointMap(nodeExporterPortName, interval),
	}

	sm := &unstructured.Unstructured{
		Object: map[string]interface{}{
			keyAPIVersion: serviceMonitorAPIVersion,
			keyKind:       serviceMonitorKind,
			"metadata": map[string]interface{}{
				keyName:     util.QueryMetricsServiceMonitorName(cluster.Name),
				"namespace": namespace,
				keyLabels:   toStringInterfaceMap(labels),
				"ownerReferences": []interface{}{
					ownerRefMap(cluster),
				},
			},
			"spec": map[string]interface{}{
				"selector": map[string]interface{}{
					"matchLabels": map[string]interface{}{
						util.LabelCluster: cluster.Name,
					},
				},
				"endpoints": endpoints,
			},
		},
	}

	return sm
}

// BuildQueryAlertsPrometheusRule builds an unstructured PrometheusRule resource
// containing alerting rules for Cloudberry monitoring.
func (b *DefaultBuilder) BuildQueryAlertsPrometheusRule(
	cluster *cbv1alpha1.CloudberryCluster,
) *unstructured.Unstructured {
	exporters := cluster.Spec.QueryMonitoring.Exporters

	namespace := cluster.Namespace
	if exporters.PrometheusRule != nil && exporters.PrometheusRule.Namespace != "" {
		namespace = exporters.PrometheusRule.Namespace
	}

	// Build labels: merge spec labels with common labels.
	labels := util.CommonLabels(cluster.Name, util.ComponentExporter)
	if exporters.PrometheusRule != nil {
		for k, v := range exporters.PrometheusRule.Labels {
			labels[k] = v
		}
	}

	pr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			keyAPIVersion: serviceMonitorAPIVersion,
			keyKind:       prometheusRuleKind,
			"metadata": map[string]interface{}{
				keyName:     util.QueryAlertsPrometheusRuleName(cluster.Name),
				"namespace": namespace,
				keyLabels:   toStringInterfaceMap(labels),
				"ownerReferences": []interface{}{
					ownerRefMap(cluster),
				},
			},
			"spec": map[string]interface{}{
				"groups": buildAlertGroups(cluster.Name),
			},
		},
	}

	return pr
}

// buildPostgresExporterContainer creates the postgres-exporter sidecar container.
func buildPostgresExporterContainer(cluster *cbv1alpha1.CloudberryCluster) corev1.Container {
	spec := cluster.Spec.QueryMonitoring.Exporters.PostgresExporter

	container := corev1.Container{
		Name:  pgExporterContainerName,
		Image: spec.Image,
		Args: []string{
			"--extend.query-path=/etc/postgres-exporter/queries.yaml",
			"--auto-discover-databases",
			"--web.listen-address=:9187",
		},
		Env: []corev1.EnvVar{
			{
				Name: "DATA_SOURCE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: util.ExporterCredentialsSecretName(cluster.Name),
						},
						Key:      secretKeyDSN,
						Optional: &optionalEnv,
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          pgExporterPortName,
				ContainerPort: pgExporterPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      exporterQueriesVolumeName,
				MountPath: exporterQueriesMountPath,
				ReadOnly:  true,
			},
		},
	}

	if spec.Resources != nil {
		k8sRes, err := convertResources(spec.Resources)
		if err == nil {
			container.Resources = k8sRes
		}
	}

	return container
}

// buildCloudberryQueryExporterContainer creates the cloudberry-query-exporter sidecar container.
func buildCloudberryQueryExporterContainer(cluster *cbv1alpha1.CloudberryCluster) corev1.Container {
	spec := cluster.Spec.QueryMonitoring.Exporters.CloudberryQueryExporter

	samplingInterval := defaultSamplingInterval
	if cluster.Spec.QueryMonitoring.SamplingInterval > 0 {
		samplingInterval = cluster.Spec.QueryMonitoring.SamplingInterval
	}

	slowQueryThreshold := defaultSlowQueryThreshold
	if cluster.Spec.QueryMonitoring.SlowQueryThreshold != "" {
		slowQueryThreshold = cluster.Spec.QueryMonitoring.SlowQueryThreshold
	}

	args := []string{
		"--listen-address=:9188",
		fmt.Sprintf("--sampling-interval=%ds", samplingInterval),
		fmt.Sprintf("--slow-query-threshold=%s", slowQueryThreshold),
	}

	if cluster.Spec.QueryMonitoring.PlanCollection {
		args = append(args, "--plan-collection")
	}
	if cluster.Spec.QueryMonitoring.HistoryRetention != "" {
		args = append(args, fmt.Sprintf("--history-retention=%s", cluster.Spec.QueryMonitoring.HistoryRetention))
	}

	container := corev1.Container{
		Name:  cbdbExporterContainerName,
		Image: spec.Image,
		Args:  args,
		Env: []corev1.EnvVar{
			{
				Name: "DATA_SOURCE_NAME",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: util.ExporterCredentialsSecretName(cluster.Name),
						},
						Key:      secretKeyDSN,
						Optional: &optionalEnv,
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          cbdbExporterPortName,
				ContainerPort: cbdbExporterPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
	}

	if spec.Resources != nil {
		k8sRes, err := convertResources(spec.Resources)
		if err == nil {
			container.Resources = k8sRes
		}
	}

	return container
}

// ownerRefMap returns the owner reference as a map for unstructured resources.
func ownerRefMap(cluster *cbv1alpha1.CloudberryCluster) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion":         cbv1alpha1.GroupVersion.String(),
		"kind":               "CloudberryCluster",
		"name":               cluster.Name,
		"uid":                string(cluster.UID),
		"controller":         true,
		"blockOwnerDeletion": true,
	}
}

// toStringInterfaceMap converts a map[string]string to map[string]interface{} for unstructured use.
func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// buildEndpointMap creates a ServiceMonitor endpoint entry.
func buildEndpointMap(portName, interval string) map[string]interface{} {
	return map[string]interface{}{
		keyPort:     portName,
		keyInterval: interval,
		"path":      keyMetricsPath,
	}
}

// buildAlertRule creates a single Prometheus alerting rule map.
func buildAlertRule(
	name, expr, duration, severity, summary, description string,
) map[string]interface{} {
	return map[string]interface{}{
		keyAlert: name,
		keyExpr:  expr,
		"for":    duration,
		keyLabels: map[string]interface{}{
			keySeverity: severity,
		},
		keyAnnotations: map[string]interface{}{
			keySummary:     summary,
			keyDescription: description,
		},
	}
}

// buildAlertGroups returns the Prometheus alerting rule groups for Cloudberry monitoring.
func buildAlertGroups(clusterName string) []interface{} {
	return []interface{}{
		map[string]interface{}{
			keyName: "cloudberry-exporter-health",
			keyRules: []interface{}{
				buildAlertRule(
					"CloudberryExporterDown",
					fmt.Sprintf("up{job=~\"%s-exporter.*\"} == 0", clusterName),
					alertDuration5m,
					"critical",
					"Cloudberry exporter is down on {{ $labels.instance }}",
					"The exporter {{ $labels.job }} has been unreachable for more than 5 minutes.",
				),
			},
		},
		map[string]interface{}{
			keyName: "cloudberry-query-performance",
			keyRules: []interface{}{
				buildAlertRule(
					"CloudberrySlowQueries",
					"rate(cloudberry_queries_slow_total[5m]) > 0.1",
					alertDuration5m,
					severityWarning,
					"High slow query rate on {{ $labels.cluster }}",
					"Slow query rate exceeds 0.1/s over the last 5 minutes.",
				),
				buildAlertRule(
					"CloudberryLongRunningQuery",
					"cloudberry_query_max_duration_seconds > 3600",
					alertDuration5m,
					severityWarning,
					"Query running longer than 1 hour on {{ $labels.cluster }}",
					"A query has been running for more than 1 hour.",
				),
			},
		},
		map[string]interface{}{
			keyName: "cloudberry-connections",
			keyRules: []interface{}{
				buildAlertRule(
					"CloudberryHighConnections",
					"sum(cloudberry_connections) / cloudberry_connections_max > 0.85",
					alertDuration5m,
					severityWarning,
					"Connection pool over 85% utilized on {{ $labels.cluster }}",
					"Database connection pool utilization exceeds 85%.",
				),
			},
		},
		map[string]interface{}{
			keyName: "cloudberry-segment-health",
			keyRules: []interface{}{
				buildAlertRule(
					"CloudberrySegmentDown",
					"cloudberry_segments_down > 0",
					"1m",
					"critical",
					"Cloudberry segment(s) down on {{ $labels.cluster }}",
					"{{ $value }} segment(s) are in down status.",
				),
			},
		},
	}
}

// exporterQueriesYAML contains the custom queries for postgres_exporter
// to monitor Cloudberry-specific metrics including segment status,
// replication lag, connection counts, and resource group usage.
//
//nolint:lll // YAML content requires long lines for readability.
var exporterQueriesYAML = `# Cloudberry custom queries for postgres_exporter
# Generated by cloudberry-operator

# ── Segment Status ──────────────────────────────────────────
cloudberry_segment_status:
  query: |
    SELECT hostname,
           role,
           preferred_role,
           status,
           mode,
           COUNT(*) AS count
    FROM gp_segment_configuration
    GROUP BY hostname, role, preferred_role, status, mode
  metrics:
    - hostname:
        usage: "LABEL"
        description: "Segment hostname"
    - role:
        usage: "LABEL"
        description: "Current role (p=primary, m=mirror)"
    - preferred_role:
        usage: "LABEL"
        description: "Preferred role"
    - status:
        usage: "LABEL"
        description: "Status (u=up, d=down)"
    - mode:
        usage: "LABEL"
        description: "Sync mode (s=synced, n=not synced)"
    - count:
        usage: "GAUGE"
        description: "Number of segments in this state"

cloudberry_segments_down:
  query: |
    SELECT COUNT(*) AS down_count
    FROM gp_segment_configuration
    WHERE status = 'd'
  metrics:
    - down_count:
        usage: "GAUGE"
        description: "Number of segments currently down"

# ── Replication Lag ─────────────────────────────────────────
cloudberry_replication:
  query: |
    SELECT client_addr,
           application_name,
           state,
           sent_lsn - write_lsn AS write_lag_bytes,
           sent_lsn - flush_lsn AS flush_lag_bytes,
           sent_lsn - replay_lsn AS replay_lag_bytes
    FROM pg_stat_replication
  metrics:
    - client_addr:
        usage: "LABEL"
        description: "Replica address"
    - application_name:
        usage: "LABEL"
        description: "Application name"
    - state:
        usage: "LABEL"
        description: "Replication state"
    - write_lag_bytes:
        usage: "GAUGE"
        description: "Write lag in bytes"
    - flush_lag_bytes:
        usage: "GAUGE"
        description: "Flush lag in bytes"
    - replay_lag_bytes:
        usage: "GAUGE"
        description: "Replay lag in bytes"

# ── Connection Counts ───────────────────────────────────────
cloudberry_connections:
  query: |
    SELECT datname,
           usename,
           state,
           COALESCE(application_name, '') AS application_name,
           COUNT(*) AS count
    FROM pg_stat_activity
    WHERE backend_type = 'client backend'
    GROUP BY datname, usename, state, application_name
  metrics:
    - datname:
        usage: "LABEL"
        description: "Database name"
    - usename:
        usage: "LABEL"
        description: "Username"
    - state:
        usage: "LABEL"
        description: "Connection state"
    - application_name:
        usage: "LABEL"
        description: "Application name"
    - count:
        usage: "GAUGE"
        description: "Number of connections"

cloudberry_connections_max:
  query: "SELECT setting::int AS max_connections FROM pg_settings WHERE name = 'max_connections'"
  metrics:
    - max_connections:
        usage: "GAUGE"
        description: "Maximum allowed connections"

# ── Resource Group Usage ────────────────────────────────────
cloudberry_resgroup_status:
  query: |
    SELECT rsgname,
           num_running AS running_queries,
           num_queueing AS queued_queries,
           num_executed AS executed_total,
           total_queue_duration AS queue_duration_seconds_total
    FROM gp_toolkit.gp_resgroup_status
  metrics:
    - rsgname:
        usage: "LABEL"
        description: "Resource group name"
    - running_queries:
        usage: "GAUGE"
        description: "Running queries in resource group"
    - queued_queries:
        usage: "GAUGE"
        description: "Queued queries in resource group"
    - executed_total:
        usage: "COUNTER"
        description: "Total queries executed by resource group"
    - queue_duration_seconds_total:
        usage: "COUNTER"
        description: "Total time queries spent in queue"

# ── Database Statistics ─────────────────────────────────────
cloudberry_database_stats:
  query: |
    SELECT datname,
           numbackends,
           xact_commit,
           xact_rollback,
           blks_read,
           blks_hit,
           tup_returned,
           tup_fetched,
           tup_inserted,
           tup_updated,
           tup_deleted,
           conflicts,
           temp_files,
           temp_bytes,
           deadlocks
    FROM pg_stat_database
    WHERE datname NOT IN ('template0', 'template1')
  metrics:
    - datname:
        usage: "LABEL"
        description: "Database name"
    - numbackends:
        usage: "GAUGE"
        description: "Active backends"
    - xact_commit:
        usage: "COUNTER"
        description: "Transactions committed"
    - xact_rollback:
        usage: "COUNTER"
        description: "Transactions rolled back"
    - blks_read:
        usage: "COUNTER"
        description: "Disk blocks read"
    - blks_hit:
        usage: "COUNTER"
        description: "Buffer cache hits"
    - tup_returned:
        usage: "COUNTER"
        description: "Rows returned by queries"
    - tup_fetched:
        usage: "COUNTER"
        description: "Rows fetched by queries"
    - tup_inserted:
        usage: "COUNTER"
        description: "Rows inserted"
    - tup_updated:
        usage: "COUNTER"
        description: "Rows updated"
    - tup_deleted:
        usage: "COUNTER"
        description: "Rows deleted"
    - conflicts:
        usage: "COUNTER"
        description: "Queries canceled due to conflicts"
    - temp_files:
        usage: "COUNTER"
        description: "Temp files created"
    - temp_bytes:
        usage: "COUNTER"
        description: "Temp bytes written"
    - deadlocks:
        usage: "COUNTER"
        description: "Deadlocks detected"

# ── Lock Metrics ────────────────────────────────────────────
cloudberry_locks:
  query: |
    SELECT mode,
           locktype,
           CASE WHEN granted THEN 'granted' ELSE 'waiting' END AS state,
           COUNT(*) AS count
    FROM pg_locks
    WHERE pid != pg_backend_pid()
    GROUP BY mode, locktype, granted
  metrics:
    - mode:
        usage: "LABEL"
        description: "Lock mode (AccessShareLock, RowExclusiveLock, etc.)"
    - locktype:
        usage: "LABEL"
        description: "Lock type (relation, transactionid, etc.)"
    - state:
        usage: "LABEL"
        description: "Lock state (granted or waiting)"
    - count:
        usage: "GAUGE"
        description: "Number of locks in this state"

# ── Table Statistics ────────────────────────────────────────
cloudberry_table_stats:
  query: |
    SELECT schemaname,
           relname,
           seq_scan,
           seq_tup_read,
           COALESCE(idx_scan, 0) AS idx_scan,
           COALESCE(idx_tup_fetch, 0) AS idx_tup_fetch,
           n_tup_ins,
           n_tup_upd,
           n_tup_del,
           n_live_tup,
           n_dead_tup
    FROM pg_stat_user_tables
    ORDER BY n_live_tup DESC
    LIMIT 100
  metrics:
    - schemaname:
        usage: "LABEL"
        description: "Schema name"
    - relname:
        usage: "LABEL"
        description: "Table name"
    - seq_scan:
        usage: "COUNTER"
        description: "Sequential scans"
    - seq_tup_read:
        usage: "COUNTER"
        description: "Rows read by sequential scans"
    - idx_scan:
        usage: "COUNTER"
        description: "Index scans"
    - idx_tup_fetch:
        usage: "COUNTER"
        description: "Rows fetched by index scans"
    - n_tup_ins:
        usage: "COUNTER"
        description: "Rows inserted"
    - n_tup_upd:
        usage: "COUNTER"
        description: "Rows updated"
    - n_tup_del:
        usage: "COUNTER"
        description: "Rows deleted"
    - n_live_tup:
        usage: "GAUGE"
        description: "Estimated live rows"
    - n_dead_tup:
        usage: "GAUGE"
        description: "Estimated dead rows"

# ── WAL ─────────────────────────────────────────────────────
cloudberry_wal:
  query: |
    SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') AS wal_bytes_total
  metrics:
    - wal_bytes_total:
        usage: "COUNTER"
        description: "Total WAL bytes generated"
`

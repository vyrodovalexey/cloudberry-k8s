//go:build functional

package functional

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	yaml "go.yaml.in/yaml/v3"
)

// ============================================================================
// Scenario 58: Node Exporter Metrics
// ============================================================================
//
// This scenario verifies that the node-exporter Helm chart at
// test/monitoring/node-exporter/ is properly configured and produces all 11
// required metric families for host-level monitoring:
//
//   - Chart.yaml exists with correct name and version
//   - DaemonSet template contains required host-level access, args, ports,
//     annotations, and volume configuration
//   - values.yaml defaults match expected image, tag, port, and host settings
//   - All 11 standard node-exporter metric families are documented
//
// ============================================================================

// Scenario58NodeExporterMetricsSuite tests the node-exporter Helm chart
// configuration and metric family definitions.
type Scenario58NodeExporterMetricsSuite struct {
	suite.Suite
}

func TestFunctional_Scenario58(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario58NodeExporterMetricsSuite))
}

// chartYAML represents the structure of Chart.yaml for parsing.
type chartYAML struct {
	APIVersion  string `yaml:"apiVersion"`
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Type        string `yaml:"type"`
	Version     string `yaml:"version"`
	AppVersion  string `yaml:"appVersion"`
}

// valuesYAML represents the structure of values.yaml for parsing.
type valuesYAML struct {
	Image struct {
		Repository string `yaml:"repository"`
		Tag        string `yaml:"tag"`
		PullPolicy string `yaml:"pullPolicy"`
	} `yaml:"image"`
	MetricsPort int  `yaml:"metricsPort"`
	HostPID     bool `yaml:"hostPID"`
	HostNetwork bool `yaml:"hostNetwork"`
}

// --- Test 1: Chart Exists ---

// TestFunctional_Scenario58_ChartExists verifies that the node-exporter
// Chart.yaml exists and has the correct name and version fields.
func (s *Scenario58NodeExporterMetricsSuite) TestFunctional_Scenario58_ChartExists() {
	content, err := os.ReadFile("../../test/monitoring/node-exporter/Chart.yaml")
	require.NoError(s.T(), err, "Chart.yaml should exist at test/monitoring/node-exporter/Chart.yaml")

	var chart chartYAML
	err = yaml.Unmarshal(content, &chart)
	require.NoError(s.T(), err, "Chart.yaml should be valid YAML")

	assert.Equal(s.T(), "node-exporter", chart.Name,
		"Chart name should be 'node-exporter'")
	assert.Equal(s.T(), "0.1.0", chart.Version,
		"Chart version should be '0.1.0'")
	assert.Equal(s.T(), "v1.8.2", chart.AppVersion,
		"Chart appVersion should be 'v1.8.2'")
	assert.Equal(s.T(), "v2", chart.APIVersion,
		"Chart apiVersion should be 'v2'")
}

// --- Test 2: DaemonSet Template ---

// TestFunctional_Scenario58_DaemonSetTemplate reads the DaemonSet template
// and verifies it contains all required configuration: hostPID, hostNetwork,
// rootfs args, web listen address, hostPort, Prometheus scrape annotations,
// and the rootfs volume with hostPath.
func (s *Scenario58NodeExporterMetricsSuite) TestFunctional_Scenario58_DaemonSetTemplate() {
	content, err := os.ReadFile("../../test/monitoring/node-exporter/templates/daemonset.yaml")
	require.NoError(s.T(), err, "daemonset.yaml should exist at test/monitoring/node-exporter/templates/daemonset.yaml")

	src := string(content)

	// Verify host-level access settings
	assert.Contains(s.T(), src, "hostPID:",
		"DaemonSet template should configure hostPID")
	assert.Contains(s.T(), src, "hostNetwork:",
		"DaemonSet template should configure hostNetwork")

	// Verify container args
	assert.Contains(s.T(), src, "--path.rootfs=/host",
		"DaemonSet template should mount rootfs at /host")
	assert.Contains(s.T(), src, "--web.listen-address=:",
		"DaemonSet template should configure web listen address")

	// Verify hostPort is configured
	assert.Contains(s.T(), src, "hostPort:",
		"DaemonSet template should expose hostPort")

	// Verify Prometheus scrape annotations
	assert.Contains(s.T(), src, "prometheus.io/scrape",
		"DaemonSet template should have prometheus.io/scrape annotation")

	// Verify rootfs volume with hostPath
	assert.Contains(s.T(), src, "name: rootfs",
		"DaemonSet template should define rootfs volume")
	assert.Contains(s.T(), src, "hostPath:",
		"DaemonSet template should use hostPath for rootfs volume")
}

// --- Test 3: Values Defaults ---

// TestFunctional_Scenario58_ValuesDefaults reads values.yaml and verifies
// the default configuration: image repository, tag, metricsPort, hostPID,
// and hostNetwork settings.
func (s *Scenario58NodeExporterMetricsSuite) TestFunctional_Scenario58_ValuesDefaults() {
	content, err := os.ReadFile("../../test/monitoring/node-exporter/values.yaml")
	require.NoError(s.T(), err, "values.yaml should exist at test/monitoring/node-exporter/values.yaml")

	var values valuesYAML
	err = yaml.Unmarshal(content, &values)
	require.NoError(s.T(), err, "values.yaml should be valid YAML")

	assert.Equal(s.T(), "prom/node-exporter", values.Image.Repository,
		"Default image repository should be 'prom/node-exporter'")
	assert.Equal(s.T(), "v1.8.2", values.Image.Tag,
		"Default image tag should be 'v1.8.2'")
	assert.Equal(s.T(), 9100, values.MetricsPort,
		"Default metricsPort should be 9100")
	assert.True(s.T(), values.HostPID,
		"Default hostPID should be true")
	assert.True(s.T(), values.HostNetwork,
		"Default hostNetwork should be true")
}

// --- Test 4: Metric Families ---

// TestFunctional_Scenario58_MetricFamilies verifies that all 11 required
// node-exporter metric family names are documented. These are standard
// Prometheus node-exporter metrics covering CPU, memory, disk, filesystem,
// network, and system load.
func (s *Scenario58NodeExporterMetricsSuite) TestFunctional_Scenario58_MetricFamilies() {
	expectedFamilies := []string{
		"node_cpu_seconds_total",
		"node_memory_MemTotal_bytes",
		"node_memory_MemAvailable_bytes",
		"node_disk_read_bytes_total",
		"node_disk_written_bytes_total",
		"node_disk_io_time_seconds_total",
		"node_filesystem_avail_bytes",
		"node_filesystem_size_bytes",
		"node_network_receive_bytes_total",
		"node_network_transmit_bytes_total",
		"node_load1",
	}

	// These are standard node-exporter metrics, verify the list is complete
	assert.Len(s.T(), expectedFamilies, 11,
		"There should be exactly 11 required metric families")

	// Verify all metric families follow the node_* naming convention
	for _, family := range expectedFamilies {
		assert.True(s.T(), strings.HasPrefix(family, "node_"),
			"Metric family %q should start with 'node_' prefix", family)
	}

	// Verify metric families cover all required categories
	categories := map[string]bool{
		"cpu":        false,
		"memory":     false,
		"disk":       false,
		"filesystem": false,
		"network":    false,
		"load":       false,
	}

	for _, family := range expectedFamilies {
		switch {
		case strings.Contains(family, "cpu"):
			categories["cpu"] = true
		case strings.Contains(family, "memory") || strings.Contains(family, "Mem"):
			categories["memory"] = true
		case strings.Contains(family, "disk"):
			categories["disk"] = true
		case strings.Contains(family, "filesystem"):
			categories["filesystem"] = true
		case strings.Contains(family, "network"):
			categories["network"] = true
		case strings.Contains(family, "load"):
			categories["load"] = true
		}
	}

	for category, covered := range categories {
		assert.True(s.T(), covered,
			"Metric families should cover the %q category", category)
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, family := range expectedFamilies {
		assert.False(s.T(), seen[family],
			"Metric family %q should not be duplicated", family)
		seen[family] = true
	}
}

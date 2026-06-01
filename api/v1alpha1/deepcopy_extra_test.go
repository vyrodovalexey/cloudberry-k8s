package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeepCopy_NilReceivers_QueryMonitoringExporters covers the nil-receiver
// branches of the query monitoring exporter related types that were missing
// from the existing nil-receiver table.
func TestDeepCopy_NilReceivers_QueryMonitoringExporters(t *testing.T) {
	nilTests := []struct {
		name string
		fn   func() interface{}
	}{
		{"ExporterSpec", func() interface{} { var s *ExporterSpec; return s.DeepCopy() }},
		{"QueryMonitoringExportersSpec", func() interface{} {
			var s *QueryMonitoringExportersSpec
			return s.DeepCopy()
		}},
		{"QueryServiceMonitorSpec", func() interface{} { var s *QueryServiceMonitorSpec; return s.DeepCopy() }},
		{"QueryPrometheusRuleSpec", func() interface{} { var s *QueryPrometheusRuleSpec; return s.DeepCopy() }},
		{"RebalanceSpec", func() interface{} { var s *RebalanceSpec; return s.DeepCopy() }},
		{"TablespaceIOLimitSpec", func() interface{} { var s *TablespaceIOLimitSpec; return s.DeepCopy() }},
	}

	for _, tt := range nilTests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fn()
			assert.Nil(t, result)
		})
	}
}

// TestExporterSpec_DeepCopy_WithResources exercises the non-nil Resources
// branch of ExporterSpec.DeepCopyInto and verifies independence.
func TestExporterSpec_DeepCopy_WithResources(t *testing.T) {
	src := &ExporterSpec{
		Enabled: true,
		Image:   "postgres-exporter:latest",
		Port:    9187,
		Resources: &ResourceRequirements{
			Requests: &ResourceList{CPU: "100m", Memory: "128Mi"},
			Limits:   &ResourceList{CPU: "200m", Memory: "256Mi"},
		},
		Segments: true,
		Mirrors:  true,
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.True(t, copied.Mirrors)
	require.NotNil(t, copied.Resources)
	require.NotNil(t, copied.Resources.Requests)
	assert.Equal(t, "100m", copied.Resources.Requests.CPU)

	// Mutating the copy must not affect the source.
	copied.Resources.Requests.CPU = "999m"
	assert.Equal(t, "100m", src.Resources.Requests.CPU)
}

// TestQueryMonitoringExportersSpec_DeepCopy_FullyPopulated exercises every
// non-nil pointer branch of QueryMonitoringExportersSpec.DeepCopyInto.
func TestQueryMonitoringExportersSpec_DeepCopy_FullyPopulated(t *testing.T) {
	src := &QueryMonitoringExportersSpec{
		PostgresExporter: &ExporterSpec{
			Enabled:   true,
			Image:     "postgres-exporter:v1",
			Port:      9187,
			Resources: &ResourceRequirements{Requests: &ResourceList{CPU: "100m"}},
		},
		NodeExporter: &ExporterSpec{
			Enabled:   true,
			Image:     "node-exporter:v1",
			Port:      9100,
			Resources: &ResourceRequirements{Limits: &ResourceList{Memory: "64Mi"}},
		},
		CloudberryQueryExporter: &ExporterSpec{
			Enabled: true,
			Image:   "cbquery-exporter:v1",
			Port:    9188,
		},
		ServiceMonitor: &QueryServiceMonitorSpec{
			Enabled:       true,
			Namespace:     "monitoring",
			Interval:      "30s",
			ScrapeTimeout: "10s",
			Labels:        map[string]string{"release": "prometheus"},
		},
		PrometheusRule: &QueryPrometheusRuleSpec{
			Enabled:   true,
			Namespace: "monitoring",
			Labels:    map[string]string{"role": "alert-rules"},
		},
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	require.NotNil(t, copied.PostgresExporter)
	require.NotNil(t, copied.NodeExporter)
	require.NotNil(t, copied.CloudberryQueryExporter)
	require.NotNil(t, copied.ServiceMonitor)
	require.NotNil(t, copied.PrometheusRule)

	assert.Equal(t, "postgres-exporter:v1", copied.PostgresExporter.Image)
	assert.Equal(t, int32(9100), copied.NodeExporter.Port)
	assert.Equal(t, "cbquery-exporter:v1", copied.CloudberryQueryExporter.Image)

	// Independence checks across the nested pointers and maps.
	copied.PostgresExporter.Resources.Requests.CPU = "changed"
	assert.Equal(t, "100m", src.PostgresExporter.Resources.Requests.CPU)

	copied.ServiceMonitor.Labels["new"] = "label"
	_, exists := src.ServiceMonitor.Labels["new"]
	assert.False(t, exists)

	copied.PrometheusRule.Labels["extra"] = "value"
	_, exists = src.PrometheusRule.Labels["extra"]
	assert.False(t, exists)
}

// TestQueryMonitoringExportersSpec_DeepCopyInto_AllNil exercises the
// all-pointers-nil path of QueryMonitoringExportersSpec.DeepCopyInto.
func TestQueryMonitoringExportersSpec_DeepCopyInto_AllNil(t *testing.T) {
	src := QueryMonitoringExportersSpec{}
	dst := QueryMonitoringExportersSpec{}
	src.DeepCopyInto(&dst)
	assert.Nil(t, dst.PostgresExporter)
	assert.Nil(t, dst.NodeExporter)
	assert.Nil(t, dst.CloudberryQueryExporter)
	assert.Nil(t, dst.ServiceMonitor)
	assert.Nil(t, dst.PrometheusRule)
}

// TestQueryMonitoringSpec_DeepCopy_WithExporters exercises the non-nil
// Exporters branch of QueryMonitoringSpec.DeepCopyInto.
func TestQueryMonitoringSpec_DeepCopy_WithExporters(t *testing.T) {
	src := &QueryMonitoringSpec{
		Enabled:            true,
		HistoryRetention:   "90d",
		SamplingInterval:   5,
		GuestAccess:        true,
		PlanCollection:     true,
		SlowQueryThreshold: "500ms",
		Exporters: &QueryMonitoringExportersSpec{
			PostgresExporter: &ExporterSpec{Enabled: true, Port: 9187},
			ServiceMonitor:   &QueryServiceMonitorSpec{Enabled: true, Labels: map[string]string{"a": "b"}},
		},
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	require.NotNil(t, copied.Exporters)
	require.NotNil(t, copied.Exporters.PostgresExporter)
	assert.Equal(t, int32(9187), copied.Exporters.PostgresExporter.Port)

	copied.Exporters.ServiceMonitor.Labels["c"] = "d"
	_, exists := src.Exporters.ServiceMonitor.Labels["c"]
	assert.False(t, exists)
}

// TestQueryServiceMonitorSpec_DeepCopy exercises the Labels map branch.
func TestQueryServiceMonitorSpec_DeepCopy(t *testing.T) {
	t.Run("with labels", func(t *testing.T) {
		src := &QueryServiceMonitorSpec{
			Enabled:       true,
			Namespace:     "monitoring",
			Interval:      "15s",
			ScrapeTimeout: "5s",
			Labels:        map[string]string{"release": "kube-prometheus"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "15s", copied.Interval)

		copied.Labels["new"] = "v"
		_, exists := src.Labels["new"]
		assert.False(t, exists)
	})

	t.Run("nil labels", func(t *testing.T) {
		src := &QueryServiceMonitorSpec{Enabled: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Nil(t, copied.Labels)
	})
}

// TestQueryPrometheusRuleSpec_DeepCopy exercises the Labels map branch.
func TestQueryPrometheusRuleSpec_DeepCopy(t *testing.T) {
	t.Run("with labels", func(t *testing.T) {
		src := &QueryPrometheusRuleSpec{
			Enabled:   true,
			Namespace: "monitoring",
			Labels:    map[string]string{"role": "alert-rules"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Equal(t, "monitoring", copied.Namespace)

		copied.Labels["new"] = "v"
		_, exists := src.Labels["new"]
		assert.False(t, exists)
	})

	t.Run("nil labels", func(t *testing.T) {
		src := &QueryPrometheusRuleSpec{Enabled: true}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Nil(t, copied.Labels)
	})
}

// TestRebalanceSpec_DeepCopy exercises the ExcludeTables slice branch.
func TestRebalanceSpec_DeepCopy(t *testing.T) {
	t.Run("with exclude tables", func(t *testing.T) {
		src := &RebalanceSpec{
			SkewThreshold: 10,
			Parallelism:   2,
			ExcludeTables: []string{"public.huge", "public.temp_*"},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		require.Len(t, copied.ExcludeTables, 2)
		assert.Equal(t, "public.huge", copied.ExcludeTables[0])

		copied.ExcludeTables[0] = "changed"
		assert.Equal(t, "public.huge", src.ExcludeTables[0])
	})

	t.Run("nil exclude tables", func(t *testing.T) {
		src := &RebalanceSpec{SkewThreshold: 5}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Nil(t, copied.ExcludeTables)
	})
}

// TestTablespaceIOLimitSpec_DeepCopy exercises the simple value type copy.
func TestTablespaceIOLimitSpec_DeepCopy(t *testing.T) {
	src := &TablespaceIOLimitSpec{
		Tablespace:       "pg_default",
		ReadBytesPerSec:  1048576,
		WriteBytesPerSec: 524288,
		ReadIOPS:         1000,
		WriteIOPS:        500,
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Equal(t, "pg_default", copied.Tablespace)
	assert.Equal(t, int64(1048576), copied.ReadBytesPerSec)

	copied.Tablespace = "changed"
	assert.Equal(t, "pg_default", src.Tablespace)
}

// TestTablespaceIOLimitSpec_DeepCopyInto exercises DeepCopyInto directly.
func TestTablespaceIOLimitSpec_DeepCopyInto(t *testing.T) {
	src := TablespaceIOLimitSpec{Tablespace: "*", ReadIOPS: 2000, WriteIOPS: 1000}
	dst := TablespaceIOLimitSpec{}
	src.DeepCopyInto(&dst)
	assert.Equal(t, "*", dst.Tablespace)
	assert.Equal(t, int32(2000), dst.ReadIOPS)
}

// TestResourceGroupSpec_DeepCopy_WithIOLimits exercises the non-nil IOLimits
// branch of ResourceGroupSpec.DeepCopyInto.
func TestResourceGroupSpec_DeepCopy_WithIOLimits(t *testing.T) {
	src := &ResourceGroupSpec{
		Name:          "analytics",
		Concurrency:   20,
		CPUMaxPercent: 60,
		CPUWeight:     100,
		MemoryLimit:   4096,
		MinCost:       500,
		IOLimits: []TablespaceIOLimitSpec{
			{Tablespace: "pg_default", ReadIOPS: 1000, WriteIOPS: 500},
			{Tablespace: "fast_ts", ReadBytesPerSec: 2097152},
		},
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	require.Len(t, copied.IOLimits, 2)
	assert.Equal(t, "pg_default", copied.IOLimits[0].Tablespace)

	copied.IOLimits[0].Tablespace = "changed"
	assert.Equal(t, "pg_default", src.IOLimits[0].Tablespace)
}

// TestSegmentsSpec_DeepCopy_WithRebalance exercises the non-nil Rebalance
// branch of SegmentsSpec.DeepCopyInto that was previously uncovered.
func TestSegmentsSpec_DeepCopy_WithRebalance(t *testing.T) {
	src := &SegmentsSpec{
		Count:            4,
		PrimariesPerHost: 2,
		Mirroring:        &MirroringSpec{Enabled: true, Layout: MirroringLayoutGroup},
		Rebalance: &RebalanceSpec{
			SkewThreshold: 15,
			Parallelism:   4,
			ExcludeTables: []string{"public.staging"},
		},
		Resources:    &ResourceRequirements{Requests: &ResourceList{CPU: "2"}},
		Storage:      StorageSpec{Size: "20Gi"},
		NodeSelector: map[string]string{"role": "segment"},
		Tolerations:  []Toleration{{Key: "seg"}},
		AntiAffinity: AntiAffinityPreferred,
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	require.NotNil(t, copied.Rebalance)
	assert.Equal(t, int32(15), copied.Rebalance.SkewThreshold)
	require.Len(t, copied.Rebalance.ExcludeTables, 1)

	copied.Rebalance.ExcludeTables[0] = "changed"
	assert.Equal(t, "public.staging", src.Rebalance.ExcludeTables[0])
}

// TestConfigSpec_DeepCopy_NilNestedMapValues exercises the nil-valued nested
// map branches of ConfigSpec.DeepCopyInto for DatabaseParameters and
// RoleParameters.
func TestConfigSpec_DeepCopy_NilNestedMapValues(t *testing.T) {
	src := &ConfigSpec{
		Parameters:            map[string]string{"max_connections": "100"},
		CoordinatorParameters: map[string]string{"work_mem": "64MB"},
		DatabaseParameters: map[string]map[string]string{
			"with_values": {"search_path": "public"},
			"nil_db":      nil,
		},
		RoleParameters: map[string]map[string]string{
			"with_values": {"statement_timeout": "60s"},
			"nil_role":    nil,
		},
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)

	assert.Nil(t, copied.DatabaseParameters["nil_db"])
	assert.Nil(t, copied.RoleParameters["nil_role"])
	assert.Equal(t, "public", copied.DatabaseParameters["with_values"]["search_path"])
	assert.Equal(t, "60s", copied.RoleParameters["with_values"]["statement_timeout"])

	// Independence of populated nested maps.
	copied.DatabaseParameters["with_values"]["new"] = "v"
	_, exists := src.DatabaseParameters["with_values"]["new"]
	assert.False(t, exists)
}

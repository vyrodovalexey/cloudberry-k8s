package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoordinatorName(t *testing.T) {
	tests := []struct {
		name     string
		cluster  string
		expected string
	}{
		{
			name:     "simple cluster name",
			cluster:  "my-cluster",
			expected: "my-cluster-coordinator",
		},
		{
			name:     "uppercase cluster name",
			cluster:  "MyCluster",
			expected: "mycluster-coordinator",
		},
		{
			name:     "empty cluster name",
			cluster:  "",
			expected: "coordinator",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CoordinatorName(tt.cluster)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestStandbyName(t *testing.T) {
	tests := []struct {
		name     string
		cluster  string
		expected string
	}{
		{
			name:     "simple cluster name",
			cluster:  "my-cluster",
			expected: "my-cluster-coordinator-standby",
		},
		{
			name:     "empty cluster name",
			cluster:  "",
			expected: "coordinator-standby",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StandbyName(tt.cluster)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSegmentPrimaryName(t *testing.T) {
	result := SegmentPrimaryName("test-cluster")
	assert.Equal(t, "test-cluster-segment-primary", result)
}

func TestSegmentMirrorName(t *testing.T) {
	result := SegmentMirrorName("test-cluster")
	assert.Equal(t, "test-cluster-segment-mirror", result)
}

func TestCoordinatorServiceName(t *testing.T) {
	result := CoordinatorServiceName("test-cluster")
	assert.Equal(t, "test-cluster-coord-hl", result)
}

func TestStandbyServiceName(t *testing.T) {
	result := StandbyServiceName("test-cluster")
	assert.Equal(t, "test-cluster-standby-hl", result)
}

func TestSegmentServiceName(t *testing.T) {
	result := SegmentServiceName("test-cluster")
	assert.Equal(t, "test-cluster-seg-hl", result)
}

func TestClientServiceName(t *testing.T) {
	result := ClientServiceName("test-cluster")
	assert.Equal(t, "test-cluster-client", result)
}

func TestPostgresqlConfConfigMapName(t *testing.T) {
	result := PostgresqlConfConfigMapName("test-cluster")
	assert.Equal(t, "test-cluster-postgresql-conf", result)
}

func TestPgHBAConfConfigMapName(t *testing.T) {
	result := PgHBAConfConfigMapName("test-cluster")
	assert.Equal(t, "test-cluster-pg-hba-conf", result)
}

func TestAdminPasswordSecretName(t *testing.T) {
	result := AdminPasswordSecretName("test-cluster")
	assert.Equal(t, "test-cluster-admin-password", result)
}

func TestRecoveryJobName(t *testing.T) {
	result := RecoveryJobName("test-cluster", "20240101120000")
	assert.Equal(t, "test-cluster-recovery-20240101120000", result)
}

func TestMaintenanceJobName(t *testing.T) {
	result := MaintenanceJobName("test-cluster", "vacuum", "20240101120000")
	assert.Equal(t, "test-cluster-vacuum-20240101120000", result)
}

func TestCommonLabels(t *testing.T) {
	tests := []struct {
		name      string
		cluster   string
		component string
	}{
		{
			name:      "coordinator labels",
			cluster:   "my-cluster",
			component: ComponentCoordinator,
		},
		{
			name:      "segment labels",
			cluster:   "my-cluster",
			component: ComponentSegmentPrimary,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := CommonLabels(tt.cluster, tt.component)
			assert.Equal(t, LabelManagedByValue, labels[LabelManagedBy])
			assert.Equal(t, tt.cluster, labels[LabelCluster])
			assert.Equal(t, tt.component, labels[LabelComponent])
			assert.Len(t, labels, 3)
		})
	}
}

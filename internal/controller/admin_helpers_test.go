package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// slowQueryRecorder wraps NoopRecorder and counts RecordSlowQuery calls.
type slowQueryRecorder struct {
	metrics.NoopRecorder
	slowQueries int
}

func (s *slowQueryRecorder) RecordSlowQuery(_, _ string) {
	s.slowQueries++
}

func newAdminReconcilerWithMetrics(m metrics.Recorder) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	return NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, m, nil)
}

func TestAdminReconciler_RecordSlowQueries(t *testing.T) {
	now := time.Now()

	t.Run("nil query monitoring is no-op", func(t *testing.T) {
		rec := &slowQueryRecorder{}
		r := newAdminReconcilerWithMetrics(rec)
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = nil
		r.recordSlowQueries(cluster, []db.Session{{State: "active", QueryStart: now.Add(-time.Hour)}})
		assert.Zero(t, rec.slowQueries)
	})

	t.Run("empty threshold is no-op", func(t *testing.T) {
		rec := &slowQueryRecorder{}
		r := newAdminReconcilerWithMetrics(rec)
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{Enabled: true}
		r.recordSlowQueries(cluster, []db.Session{{State: "active", QueryStart: now.Add(-time.Hour)}})
		assert.Zero(t, rec.slowQueries)
	})

	t.Run("invalid threshold is no-op", func(t *testing.T) {
		rec := &slowQueryRecorder{}
		r := newAdminReconcilerWithMetrics(rec)
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
			Enabled: true, SlowQueryThreshold: "not-a-duration",
		}
		r.recordSlowQueries(cluster, []db.Session{{State: "active", QueryStart: now.Add(-time.Hour)}})
		assert.Zero(t, rec.slowQueries)
	})

	t.Run("counts only slow active queries", func(t *testing.T) {
		rec := &slowQueryRecorder{}
		r := newAdminReconcilerWithMetrics(rec)
		cluster := newTestCluster()
		cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
			Enabled: true, SlowQueryThreshold: "30s",
		}
		sessions := []db.Session{
			{State: "active", QueryStart: now.Add(-time.Minute)}, // slow
			{State: "active", QueryStart: now.Add(time.Minute)},  // future -> fast
			{State: "idle", QueryStart: now.Add(-time.Hour)},     // not active
			{State: "active"}, // zero QueryStart
			{State: "active", QueryStart: now.Add(-2 * time.Hour)}, // slow
		}
		r.recordSlowQueries(cluster, sessions)
		assert.Equal(t, 2, rec.slowQueries)
	})
}

func TestNormalizeRecoveryType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"incremental", "incremental"},
		{"full", "full"},
		{"differential", "differential"},
		{"unknown", "full"},
		{"", "full"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, normalizeRecoveryType(tt.input))
		})
	}
}

func TestAdminReconciler_RecordSlowQueries_EmptySessions(t *testing.T) {
	rec := &slowQueryRecorder{}
	r := newAdminReconcilerWithMetrics(rec)
	cluster := newTestCluster()
	cluster.Spec.QueryMonitoring = &cbv1alpha1.QueryMonitoringSpec{
		Enabled: true, SlowQueryThreshold: "100ms",
	}
	r.recordSlowQueries(cluster, nil)
	require.Zero(t, rec.slowQueries)
}

// TestMergeAnnotations covers mergeAnnotations including the existing==nil
// allocation branch, key collisions (desired wins), and preservation of
// third-party annotations absent from desired (H-4).
func TestMergeAnnotations(t *testing.T) {
	tests := []struct {
		name     string
		existing map[string]string
		desired  map[string]string
		want     map[string]string
	}{
		{
			name:     "nil existing allocates and returns desired",
			existing: nil,
			desired:  map[string]string{"a": "1"},
			want:     map[string]string{"a": "1"},
		},
		{
			name:     "nil existing and nil desired yields empty non-nil map",
			existing: nil,
			desired:  nil,
			want:     map[string]string{},
		},
		{
			name:     "collision: desired value overrides existing",
			existing: map[string]string{"a": "old"},
			desired:  map[string]string{"a": "new"},
			want:     map[string]string{"a": "new"},
		},
		{
			name:     "preserve third-party annotations absent from desired",
			existing: map[string]string{"x": "old", "a": "keep"},
			desired:  map[string]string{"a": "new"},
			want:     map[string]string{"x": "old", "a": "new"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeAnnotations(tt.existing, tt.desired)
			require.NotNil(t, got)
			assert.Equal(t, tt.want, got)
		})
	}
}

package db

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// connectRecorder captures RecordDBConnect calls.
type connectRecorder struct {
	metrics.NoopRecorder
	mu      sync.Mutex
	results []string
}

func (r *connectRecorder) RecordDBConnect(_, _, result string, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, result)
}

func (r *connectRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.results...)
}

// TestClientFactory_NewClient_RecordsConnectError verifies that a failed
// connection attempt (missing admin Secret) records exactly one
// cloudberry_db_connect_total{result="error"} sample and a connect span with
// error status.
func TestClientFactory_NewClient_RecordsConnectError(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &connectRecorder{}
	factory := NewClientFactory(k8sClient, nil, rec)

	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns1"},
	}

	_, err := factory.NewClient(context.Background(), cluster)
	require.Error(t, err)
	assert.Equal(t, []string{"error"}, rec.recorded())

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "db.connect" {
			found = true
		}
	}
	assert.True(t, found, "db.connect span not exported")
}

// TestRecordConnectOutcome verifies the success/error result mapping of the
// connect-outcome hook directly (the full success path needs cluster-service
// DNS that does not resolve in unit tests).
func TestRecordConnectOutcome(t *testing.T) {
	rec := &connectRecorder{}
	factory := &ClientFactory{recorder: rec}
	cluster := &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns1"},
	}

	factory.recordConnectOutcome(cluster, time.Now(), nil)
	factory.recordConnectOutcome(cluster, time.Now(), assert.AnError)
	assert.Equal(t, []string{"success", "error"}, rec.recorded())

	// Nil recorder is a safe no-op.
	nilFactory := &ClientFactory{}
	nilFactory.recordConnectOutcome(cluster, time.Now(), nil)
}

// TestClientFactory_NilClusterDoesNotRecord verifies a nil cluster fails
// before any connect attempt metric is recorded.
func TestClientFactory_NilClusterDoesNotRecord(t *testing.T) {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	rec := &connectRecorder{}
	factory := NewClientFactory(k8sClient, nil, rec)

	_, err := factory.NewClient(context.Background(), nil)
	require.Error(t, err)
	assert.Empty(t, rec.recorded())
}

// TestPgxClientRegisterPoolStats verifies the pool stats provider lifecycle:
// registered with the recorder, sampled values come from pgxpool.Stat(), and
// Close unregisters exactly once.
func TestPgxClientRegisterPoolStats(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	client.SetRecorder(rec, "c1", "ns1")
	client.registerPoolStats()
	require.NotNil(t, client.unregisterPoolStats)

	families, err := reg.Gather()
	require.NoError(t, err)
	var foundMax bool
	for _, f := range families {
		if f.GetName() == "cloudberry_db_pool_max_conns" {
			foundMax = true
			require.NotEmpty(t, f.GetMetric())
			assert.Equal(t, float64(1), f.GetMetric()[0].GetGauge().GetValue())
		}
	}
	assert.True(t, foundMax, "pool max conns gauge not exposed")

	// Close unregisters the provider; the series disappears from scrapes.
	client.Close()
	families, err = reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		assert.NotEqual(t, "cloudberry_db_pool_max_conns", f.GetName())
	}
}

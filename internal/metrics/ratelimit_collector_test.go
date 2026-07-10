package metrics

// In-package tests for the rateLimitEntriesCollector (handoff task T-5,
// fix E3-adjacent): the collector is verified end-to-end from internal/api,
// but Go coverage is per-package — these tests close add / Collect /
// RegisterRateLimitEntries directly against a real registry.

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rateLimitEntriesFamily = "cloudberry_api_rate_limit_entries"

// gatherRateLimitEntries gathers the single label-less gauge sample.
func gatherRateLimitEntries(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	return valueWithLabels(t, reg, rateLimitEntriesFamily, nil)
}

func TestRegisterRateLimitEntries_MultiProviderSum(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	// No providers: the gauge exists and reports 0 (single collector sample).
	assert.Zero(t, gatherRateLimitEntries(t, reg),
		"the gauge must report 0 before any provider registers")

	// Two live providers (one per API server instance) are summed.
	unregisterA := rec.RegisterRateLimitEntries(func() float64 { return 5 })
	unregisterB := rec.RegisterRateLimitEntries(func() float64 { return 7 })
	assert.InDelta(t, 12.0, gatherRateLimitEntries(t, reg), 0.0001,
		"scrape must sum every live provider")

	// Unregister one: only the survivor is sampled.
	unregisterA()
	assert.InDelta(t, 7.0, gatherRateLimitEntries(t, reg), 0.0001,
		"an unregistered provider must stop counting")

	// Unregister is idempotent (sync.Once): a double call must not remove
	// the OTHER provider or panic.
	assert.NotPanics(t, unregisterA)
	assert.InDelta(t, 7.0, gatherRateLimitEntries(t, reg), 0.0001,
		"a double unregister must be a no-op")

	unregisterB()
	assert.Zero(t, gatherRateLimitEntries(t, reg),
		"the gauge must drop to 0 after the last provider unregisters")
}

func TestRateLimitEntriesCollector_ProvidersSampledLive(t *testing.T) {
	reg := prometheus.NewRegistry()
	rec := NewPrometheusRecorder(reg)

	// The provider is sampled at SCRAPE time, not registration time.
	var mu sync.Mutex
	entries := 3.0
	unregister := rec.RegisterRateLimitEntries(func() float64 {
		mu.Lock()
		defer mu.Unlock()
		return entries
	})
	defer unregister()

	assert.InDelta(t, 3.0, gatherRateLimitEntries(t, reg), 0.0001)

	mu.Lock()
	entries = 9
	mu.Unlock()
	assert.InDelta(t, 9.0, gatherRateLimitEntries(t, reg), 0.0001,
		"each scrape must re-sample the provider")
}

func TestRateLimitEntriesCollector_DescribeEmitsSingleDesc(t *testing.T) {
	c := newRateLimitEntriesCollector()

	ch := make(chan *prometheus.Desc, 2)
	c.Describe(ch)
	close(ch)

	var descs []*prometheus.Desc
	for d := range ch {
		descs = append(descs, d)
	}
	require.Len(t, descs, 1)
	assert.Contains(t, descs[0].String(), rateLimitEntriesFamily,
		"Describe must announce the registered family name")
}

func TestNoopRecorder_RegisterRateLimitEntries(t *testing.T) {
	n := &NoopRecorder{}
	unregister := n.RegisterRateLimitEntries(func() float64 { return 1 })
	require.NotNil(t, unregister,
		"the no-op recorder must keep the non-nil unregister contract")
	assert.NotPanics(t, unregister)
	assert.NotPanics(t, unregister, "the no-op unregister must be repeatable")
}

func TestRateLimitEntriesCollector_CollectDirect(t *testing.T) {
	c := newRateLimitEntriesCollector()
	unregister := c.add(func() float64 { return 4 })
	defer unregister()
	c.add(func() float64 { return 2 })()

	// The second provider unregistered immediately: only 4 remains.
	ch := make(chan prometheus.Metric, 2)
	c.Collect(ch)
	close(ch)

	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	require.Len(t, metrics, 1, "Collect must emit exactly one summed sample")
}

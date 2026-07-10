package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// rateLimitEntriesCollector exposes the process-wide
// cloudberry_api_rate_limit_entries gauge by sampling every registered
// entries provider on each Prometheus scrape and emitting their SUM as a
// single sample. A single collector instance is registered once by the
// PrometheusRecorder; individual API server instances add/remove their
// provider via add(), so two coexisting servers sharing one recorder never
// cause a duplicate-registration panic (mirrors dbPoolStatsCollector).
type rateLimitEntriesCollector struct {
	desc *prometheus.Desc

	mu sync.RWMutex
	// providers maps a registration id to its entries provider. Multiple live
	// providers (one per API server instance) are summed into the single
	// label-less gauge, which keeps the metric honest for the whole process.
	providers map[uint64]func() float64
	// nextID is a monotonically increasing registration counter.
	nextID uint64
}

// newRateLimitEntriesCollector creates an empty rate-limit entries collector.
func newRateLimitEntriesCollector() *rateLimitEntriesCollector {
	return &rateLimitEntriesCollector{
		desc: prometheus.NewDesc(
			metricsNamespace+"_api_rate_limit_entries",
			"Current number of tracked per-client API rate limiter entries.",
			nil, nil,
		),
		providers: make(map[uint64]func() float64),
	}
}

// add registers an entries provider and returns its idempotent unregister
// function.
func (c *rateLimitEntriesCollector) add(fn func() float64) func() {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.providers[id] = fn
	c.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			delete(c.providers, id)
		})
	}
}

// Describe implements prometheus.Collector.
func (c *rateLimitEntriesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector by summing all live providers.
func (c *rateLimitEntriesCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total float64
	for _, fn := range c.providers {
		total += fn()
	}
	ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, total)
}

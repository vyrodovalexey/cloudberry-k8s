package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// dbPoolStatsCollector exposes per-cluster database connection-pool gauges
// (cloudberry_db_pool_acquired_conns / _idle_conns / _max_conns) by sampling
// each registered DBPoolStatsFunc on every Prometheus scrape. A single
// collector instance is registered once by the PrometheusRecorder; individual
// clients add/remove their stats provider via add(), so two coexisting DB
// clients never cause a duplicate-registration panic.
type dbPoolStatsCollector struct {
	acquiredDesc *prometheus.Desc
	idleDesc     *prometheus.Desc
	maxDesc      *prometheus.Desc

	mu sync.RWMutex
	// providers keeps exactly ONE stats provider per cluster/namespace pair
	// (a reconnected client replaces the closed one), so identical label sets
	// can never be emitted twice in a single scrape.
	providers map[poolKey]*poolEntry
	// generation is a monotonically increasing registration counter used to
	// detect stale unregistrations (see add).
	generation uint64
}

// poolEntry pairs a stats provider with a registration generation so a stale
// unregister (old client closed AFTER its replacement registered) cannot
// remove the live provider.
type poolEntry struct {
	stats      DBPoolStatsFunc
	generation uint64
}

// poolKey identifies a pool stats provider by its metric label values.
type poolKey struct {
	cluster   string
	namespace string
}

// newDBPoolStatsCollector creates an empty pool stats collector.
func newDBPoolStatsCollector() *dbPoolStatsCollector {
	return &dbPoolStatsCollector{
		acquiredDesc: prometheus.NewDesc(
			metricsNamespace+"_db_pool_acquired_conns",
			"Number of currently acquired (in-use) connections in the pool.",
			[]string{labelCluster, labelNamespace}, nil,
		),
		idleDesc: prometheus.NewDesc(
			metricsNamespace+"_db_pool_idle_conns",
			"Number of currently idle connections in the pool.",
			[]string{labelCluster, labelNamespace}, nil,
		),
		maxDesc: prometheus.NewDesc(
			metricsNamespace+"_db_pool_max_conns",
			"Maximum number of connections allowed in the pool.",
			[]string{labelCluster, labelNamespace}, nil,
		),
		providers: make(map[poolKey]*poolEntry),
	}
}

// add registers a stats provider for the given cluster, replacing any
// previous provider with the same labels (a reconnected client supersedes the
// closed one). The returned function removes the provider; a stale unregister
// from a superseded client is a no-op (generation check), so closing the old
// client after reconnect never drops the live provider.
func (c *dbPoolStatsCollector) add(cluster, namespace string, stats DBPoolStatsFunc) func() {
	key := poolKey{cluster: cluster, namespace: namespace}

	c.mu.Lock()
	c.generation++
	gen := c.generation
	c.providers[key] = &poolEntry{stats: stats, generation: gen}
	c.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if entry, ok := c.providers[key]; ok && entry.generation == gen {
				delete(c.providers, key)
			}
		})
	}
}

// Describe implements prometheus.Collector.
func (c *dbPoolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.acquiredDesc
	ch <- c.idleDesc
	ch <- c.maxDesc
}

// Collect implements prometheus.Collector by sampling every registered pool.
func (c *dbPoolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for key, entry := range c.providers {
		acquired, idle, maxConns := entry.stats()
		ch <- prometheus.MustNewConstMetric(
			c.acquiredDesc, prometheus.GaugeValue, acquired, key.cluster, key.namespace)
		ch <- prometheus.MustNewConstMetric(
			c.idleDesc, prometheus.GaugeValue, idle, key.cluster, key.namespace)
		ch <- prometheus.MustNewConstMetric(
			c.maxDesc, prometheus.GaugeValue, maxConns, key.cluster, key.namespace)
	}
}

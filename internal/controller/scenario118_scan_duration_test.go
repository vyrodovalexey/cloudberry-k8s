package controller

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// Scenario 118 — Scan Scheduling and Duration Limit (controller layer).
//
// These unit tests cover the C.10 scanDuration cap + truncation signal and the
// M.3 capped-duration histogram, exercised through recordRecommendations /
// resolveScanDuration with:
//   - resolveScanDuration table (118b-resolveScanDuration);
//   - a BLOCKING mock DB whose Get* select on ctx.Done() so a tiny
//     scanDuration ("10ms") deterministically trips context.DeadlineExceeded
//     (118b-C10-cap, 118b-shared-budget);
//   - a fast mock that proves a generous cap does NOT truncate (118b-no-truncate);
//   - a counting recorder that proves ObserveRecommendationScanDuration fires
//     exactly once per scan (118a-M3-duration);
//   - the disabled / nil-dbFactory control rows (118-DISABLED-noop, 118-CONTROL);
//   - the steady-state path also setting the truncation flag when capped.

// ---------------------------------------------------------------------------
// Blocking / fast mock DB clients for the cap tests.
// ---------------------------------------------------------------------------

// blockingScanDBClient is a db.Client whose four Get* recommendation queries
// BLOCK on the passed context (select on ctx.Done() with a long sleep) so a
// tiny shared scanDuration deadline trips context.DeadlineExceeded mid-scan. It
// records, atomically, how many Get* calls were entered and the deadline of the
// context each call observed (so the SHARED single-budget invariant can be
// asserted: every call sees the same/derived deadline, not a fresh per-query
// cap).
type blockingScanDBClient struct {
	*mockDBClient

	// block is how long each Get* sleeps if the context is NOT cancelled
	// (large, so the deadline always wins for a tiny cap).
	block time.Duration

	calls    int32        // number of Get* calls entered.
	deadline atomic.Value // time.Time — the deadline of the last observed ctx.
}

func (m *blockingScanDBClient) wait(ctx context.Context) ([]db.Recommendation, error) {
	atomic.AddInt32(&m.calls, 1)
	if dl, ok := ctx.Deadline(); ok {
		m.deadline.Store(dl)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(m.block):
		return nil, nil
	}
}

func (m *blockingScanDBClient) GetBloatRecommendations(
	ctx context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.wait(ctx)
}

func (m *blockingScanDBClient) GetSkewRecommendations(
	ctx context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.wait(ctx)
}

func (m *blockingScanDBClient) GetAgeRecommendations(
	ctx context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.wait(ctx)
}

func (m *blockingScanDBClient) GetIndexBloatRecommendations(
	ctx context.Context, _ db.RecommendationThresholds,
) ([]db.Recommendation, error) {
	return m.wait(ctx)
}

// countingScanRecorder is a metrics.Recorder that counts the M.3 duration
// observations (and the truncation increments) so the "observed exactly once"
// invariant can be asserted directly, alongside the real registry-based checks.
type countingScanRecorder struct {
	metrics.NoopRecorder

	durationCalls  int
	lastDuration   time.Duration
	truncatedCalls int
}

func (r *countingScanRecorder) ObserveRecommendationScanDuration(
	_, _ string, d time.Duration,
) {
	r.durationCalls++
	r.lastDuration = d
}

func (r *countingScanRecorder) IncRecommendationScanTruncated(_, _ string) {
	r.truncatedCalls++
}

// ---------------------------------------------------------------------------
// 118b-resolveScanDuration — fallback / clamp / verbatim policy (table).
// ---------------------------------------------------------------------------

func TestScenario118_ResolveScanDuration(t *testing.T) {
	tests := []struct {
		name  string
		input string // scanDuration string ("" means empty field).
		want  time.Duration
	}{
		{name: "empty -> default 10s", input: "", want: defaultScanDuration},
		{name: "unparseable -> default 10s", input: "bad", want: defaultScanDuration},
		{name: "zero -> default 10s", input: "0s", want: defaultScanDuration},
		{name: "negative -> default 10s", input: "-5s", want: defaultScanDuration},
		{name: "above ceiling -> clamp 24h", input: "25h", want: maxScanDuration},
		{name: "verbatim 30s", input: "30s", want: 30 * time.Second},
		{name: "verbatim tiny 10ms", input: "10ms", want: 10 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := &cbv1alpha1.RecommendationScanSpec{ScanDuration: tt.input}
			got := resolveScanDuration(scan, slog.Default())
			assert.Equal(t, tt.want, got,
				"resolveScanDuration(%q) must be exactly %s", tt.input, tt.want)
		})
	}

	// Defensive: a nil scan must also fall back to the default cap (never <= 0).
	t.Run("nil scan -> default 10s", func(t *testing.T) {
		assert.Equal(t, defaultScanDuration, resolveScanDuration(nil, slog.Default()))
	})
}

// scanDurationCluster returns a recommendation-scan-enabled cluster with the
// given scanDuration string.
func scanDurationCluster(scanDuration string) *cbv1alpha1.CloudberryCluster {
	c := recScanCluster()
	c.Spec.Storage.RecommendationScan.ScanDuration = scanDuration
	return c
}

// ---------------------------------------------------------------------------
// 118b-C10-cap — a tiny scanDuration against a blocking DB truncates the scan.
// ---------------------------------------------------------------------------

func TestScenario118_C10_Cap_Truncates(t *testing.T) {
	cluster := scanDurationCluster("10ms")
	cluster.Status.RecommendationCount = 42 // stale prior value must be replaced.
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second, // far longer than the 10ms cap.
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	start := time.Now()
	r.recordRecommendations(context.Background(), cluster, slog.Default())
	elapsed := time.Since(start)

	// The truncation flag is set on the status.
	assert.True(t, cluster.Status.RecommendationScanTruncated,
		"a capped scan must set RecommendationScanTruncated=true")

	// The truncation counter incremented exactly once.
	v, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	require.True(t, found, "the truncation counter must be published")
	assert.InDelta(t, 1.0, v, 0.001)

	// Un-run types contribute 0 — no fabrication. The blocking DB returns no
	// rows, so every per-type gauge is 0 and the count is 0.
	assert.Equal(t, int32(0), cluster.Status.RecommendationCount,
		"a truncated scan must not fabricate counts; un-run types count 0")
	for _, recType := range recommendationTypes {
		got, ok := recsTotalGaugeValue(t, reg, "test-cluster", "default", recType)
		require.True(t, ok, "every type gauge must still be published: %s", recType)
		assert.InDelta(t, 0.0, got, 0.001, "type %s must be 0 (not fabricated)", recType)
	}

	// M.3: the duration histogram recorded one (capped) sample.
	cnt, found := histogramSampleCount(t, reg,
		"cloudberry_recommendation_scan_duration_seconds",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	require.True(t, found, "the duration histogram must record the capped run")
	assert.Equal(t, uint64(1), cnt)

	// LastRecommendationScanTime is set each scan.
	require.NotNil(t, cluster.Status.LastRecommendationScanTime,
		"LastRecommendationScanTime must be set on a (capped) scan")

	// The total wall-clock is bounded near the cap, NOT 4x the per-query block:
	// the single shared budget short-circuits the remaining queries.
	assert.Less(t, elapsed, time.Second,
		"the capped scan must finish near the cap, not run the full block")
}

// ---------------------------------------------------------------------------
// 118b-no-truncate — a fast DB + generous cap does NOT truncate.
// ---------------------------------------------------------------------------

func TestScenario118_NoTruncate_FastDB(t *testing.T) {
	cluster := scanDurationCluster("2h")
	cluster.Status.RecommendationScanTruncated = true // sticky prior must reset.
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
		skewRecs:     []db.Recommendation{rec(recTypeSkew, "public", "s1", 40)},
		ageRecs:      []db.Recommendation{rec(recTypeAge, "public", "a1", 0)},
		indexRecs:    []db.Recommendation{rec(recTypeIndexBloat, "public", "i1", 70)},
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	r.recordRecommendations(context.Background(), cluster, slog.Default())

	// Flag resets to false (never sticky).
	assert.False(t, cluster.Status.RecommendationScanTruncated,
		"a completed scan must set RecommendationScanTruncated=false")

	// No truncation counter increment (series absent).
	_, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	assert.False(t, found, "a non-truncated scan must NOT increment the counter")

	// Normal counts: 1 of each type = 4.
	assert.Equal(t, int32(4), cluster.Status.RecommendationCount)
	require.NotNil(t, cluster.Status.LastRecommendationScanTime)
}

// ---------------------------------------------------------------------------
// 118b-shared-budget — all four Get* share ONE dbCtx deadline (not 4x the cap).
// ---------------------------------------------------------------------------

func TestScenario118_SharedBudget_SingleDeadline(t *testing.T) {
	cluster := scanDurationCluster("50ms")
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second,
	}
	r, _ := recScanReconciler(t, cluster, dbClient)

	start := time.Now()
	r.recordRecommendations(context.Background(), cluster, slog.Default())
	elapsed := time.Since(start)

	// One shared 50ms budget bounds the TOTAL scan: total elapsed ~= cap, NOT
	// 4 x block (8s) and NOT 4 x cap. Generous upper bound for CI slack.
	assert.Less(t, elapsed, 500*time.Millisecond,
		"the cap must bound the TOTAL scan (one shared budget), not 4x the timeout")

	// At least one Get* was entered and observed a deadline; the first blocking
	// call trips the deadline and the remaining calls observe the canceled ctx
	// and return fast, so the recorded deadline is the shared one.
	require.GreaterOrEqual(t, atomic.LoadInt32(&dbClient.calls), int32(1),
		"at least the first Get* must run under the shared dbCtx")
	dl, ok := dbClient.deadline.Load().(time.Time)
	require.True(t, ok, "the Get* calls must run under a deadline-bearing ctx")
	// The shared deadline is ~cap from start (within a generous slack window).
	assert.WithinDuration(t, start.Add(50*time.Millisecond), dl, 200*time.Millisecond,
		"every Get* must derive from the single shared scanDuration deadline")
}

// ---------------------------------------------------------------------------
// 118b-budget-split — the CONNECTION budget is SEPARATE from the SCAN budget.
//
// recordRecommendations splits the single legacy dbCtx into:
//   - connectCtx (FIXED connectTimeout) — used ONLY for dbFactory.NewClient;
//   - scanCtx (resolveScanDuration) — used ONLY for the four Get* queries;
// and keys truncation off scanCtx.Err(), NOT the connection. The regression:
// previously a single combined budget meant a tiny scanDuration tripped on
// NewClient FIRST, so the QUERY-phase truncation signal was never observable.
// These tests prove the split directly:
//   - a SLOW (but successful) connect with a fast Get* and a tiny scanDuration
//     must NOT truncate — connection time does not count against the scan
//     budget (the key new coverage the fix enables);
//   - a fast connect with a BLOCKING Get* and a tiny scanDuration DOES trip the
//     scan budget and truncates (already covered by 118b-C10-cap; re-asserted
//     here through the same split path for completeness).
// ---------------------------------------------------------------------------

// delayedConnectDBFactory is a db.DBClientFactory whose NewClient SLEEPS for a
// non-trivial delay (simulating a slow-but-successful connection handshake)
// before returning the wrapped, fast client. It records the deadline observed
// on the context passed to NewClient so the test can assert NewClient runs under
// the FIXED connectTimeout budget — independent of (and far larger than) the
// tiny scanDuration cap. This additive fake exists because the shared
// mockDBClientFactory returns instantly and so cannot model a slow connect.
type delayedConnectDBFactory struct {
	client db.Client
	delay  time.Duration

	connectDeadline atomic.Value // time.Time — deadline of the ctx NewClient saw.
}

func (f *delayedConnectDBFactory) NewClient(
	ctx context.Context, _ *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	if dl, ok := ctx.Deadline(); ok {
		f.connectDeadline.Store(dl)
	}
	// Sleep to model a slow connection handshake. Respect ctx cancellation so a
	// (hypothetically) too-tight connect budget would surface as an error rather
	// than hang — but with the FIXED connectTimeout this always completes.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(f.delay):
	}
	return f.client, nil
}

// 118b-budget-split-slow-connect — a SLOW (but successful) NewClient combined
// with a fast Get* and a SHORT scanDuration must NOT truncate. The connection
// took longer than scanDuration, but because the connection has its own FIXED
// connectTimeout budget (separate from the scan budget), the scan COMPLETES
// NORMALLY: the connect delay does not count against the scan budget. This is
// the live bug the fix repaired — previously a single combined budget made
// NewClient fail FIRST for a tiny cap and truncation was never observable.
func TestScenario118_BudgetSplit_SlowConnect_NoTruncate(t *testing.T) {
	cluster := scanDurationCluster("10ms")
	cluster.Status.RecommendationScanTruncated = true // sticky prior must reset.

	// Fast client: every Get* returns instantly with one bloat row.
	fastClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
	}
	// NewClient blocks ~50ms (>> the 10ms scanDuration) but SUCCEEDS.
	const connectDelay = 50 * time.Millisecond
	dbFactory := &delayedConnectDBFactory{client: fastClient, delay: connectDelay}

	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, recorder, nil)

	start := time.Now()
	r.recordRecommendations(context.Background(), cluster, slog.Default())
	elapsed := time.Since(start)

	// THE KEY ASSERTION: a slow connection (50ms > 10ms scanDuration) must NOT
	// truncate. Only the QUERIES are capped by scanDuration; the connect delay
	// is charged to the separate connectTimeout budget.
	assert.False(t, cluster.Status.RecommendationScanTruncated,
		"a slow-but-successful connect must NOT truncate: the connection delay "+
			"does not count against the scanDuration budget")

	// No truncation counter increment (series absent).
	_, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	assert.False(t, found,
		"a non-truncated (slow-connect) scan must NOT increment the counter")

	// The scan completed normally: the single bloat row is counted (== 1), and
	// the per-type gauges are honest (bloat 1, others 0). A truncation-before-
	// connect bug would have produced no completed scan at all.
	assert.Equal(t, int32(1), cluster.Status.RecommendationCount,
		"the completed scan must count the one bloat row")
	bloat, ok := recsTotalGaugeValue(t, reg, "test-cluster", "default", recTypeBloat)
	require.True(t, ok, "the bloat gauge must be published by the completed scan")
	assert.InDelta(t, 1.0, bloat, 0.001)

	// LastRecommendationScanTime is set on the completed scan.
	require.NotNil(t, cluster.Status.LastRecommendationScanTime,
		"a completed scan must set LastRecommendationScanTime")

	// The total wall-clock is at least the connect delay (NewClient really ran)
	// but the scan still completed — proving the connect time was spent OUTSIDE
	// the scan budget rather than tripping it.
	assert.GreaterOrEqual(t, elapsed, connectDelay,
		"the slow connect must actually have run (>= connectDelay)")

	// connectTimeout (FIXED, 10s) is the budget NewClient observed — NOT the tiny
	// 10ms scanDuration. The deadline NewClient saw is ~connectTimeout from start,
	// independent of scanDuration, confirming the budgets are split.
	dl, dlOK := dbFactory.connectDeadline.Load().(time.Time)
	require.True(t, dlOK, "NewClient must run under the connect deadline")
	assert.WithinDuration(t, start.Add(connectTimeout), dl, 2*time.Second,
		"NewClient must run under the FIXED connectTimeout, independent of scanDuration")
	assert.Greater(t, dl.Sub(start), 10*time.Millisecond,
		"the connect budget must be far larger than the 10ms scanDuration")
}

// 118b-budget-split-fast-connect-slow-scan — an INSTANT NewClient + a BLOCKING
// Get* + a tiny "1ms" scanDuration trips the SCAN context and truncates. This
// exercises the SAME split path from the other side: with a fast connect, the
// scan budget is what trips, so truncation IS observed (mirrors 118b-C10-cap
// but with the smallest cap and asserting the truncation counter increments
// exactly once through the split).
func TestScenario118_BudgetSplit_FastConnect_SlowScan_Truncates(t *testing.T) {
	cluster := scanDurationCluster("1ms")
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second, // far longer than the 1ms scan cap.
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	r.recordRecommendations(context.Background(), cluster, slog.Default())

	assert.True(t, cluster.Status.RecommendationScanTruncated,
		"a blocking Get* under a 1ms scan budget must truncate (scanCtx trips)")
	v, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	require.True(t, found, "the truncation counter must be published")
	assert.InDelta(t, 1.0, v, 0.001,
		"the truncation counter must increment exactly once on the capped scan")
}

// ---------------------------------------------------------------------------
// 118a-M3-duration — ObserveRecommendationScanDuration fires exactly once.
// ---------------------------------------------------------------------------

func TestScenario118_M3_DurationObservedOnce(t *testing.T) {
	cluster := scanDurationCluster("2h")
	dbClient := &recScanDBClient{
		mockDBClient: &mockDBClient{},
		bloatRecs:    []db.Recommendation{rec(recTypeBloat, "public", "t1", 25)},
	}
	recorder := &countingScanRecorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, recorder, nil)

	r.recordRecommendations(context.Background(), cluster, slog.Default())

	assert.Equal(t, 1, recorder.durationCalls,
		"ObserveRecommendationScanDuration must be called exactly once per scan")
	assert.GreaterOrEqual(t, recorder.lastDuration, time.Duration(0),
		"the observed duration must be non-negative")
	assert.Equal(t, 0, recorder.truncatedCalls,
		"a completed scan must not increment the truncation counter")
}

// 118a-M3-capped — under truncation the duration is STILL observed once and is
// bounded near the cap (not the unbounded block).
func TestScenario118_M3_CappedDurationObservedOnce(t *testing.T) {
	cluster := scanDurationCluster("20ms")
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second,
	}
	recorder := &countingScanRecorder{}
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, recorder, nil)

	r.recordRecommendations(context.Background(), cluster, slog.Default())

	require.Equal(t, 1, recorder.durationCalls,
		"the duration must be observed once even on a truncated scan")
	assert.Equal(t, 1, recorder.truncatedCalls,
		"the truncation counter must increment exactly once on a capped scan")
	assert.Less(t, recorder.lastDuration, time.Second,
		"the observed duration must reflect the CAPPED run, not the full block")
}

// ---------------------------------------------------------------------------
// 118-DISABLED-noop / 118-CONTROL — disabled scan and nil dbFactory are no-ops.
// ---------------------------------------------------------------------------

func TestScenario118_Disabled_NoTruncation(t *testing.T) {
	tests := []struct {
		name string
		scan *cbv1alpha1.RecommendationScanSpec
	}{
		{name: "scan nil", scan: nil},
		{name: "scan disabled", scan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false, ScanDuration: "10ms",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newTestCluster()
			cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
				DiskMonitoring:     true,
				RecommendationScan: tt.scan,
			}
			dbClient := &blockingScanDBClient{
				mockDBClient: &mockDBClient{},
				block:        2 * time.Second,
			}
			r, reg := recScanReconciler(t, cluster, dbClient)

			require.NotPanics(t, func() {
				require.NoError(t, r.reconcileStorage(context.Background(), cluster))
			})

			assert.False(t, cluster.Status.RecommendationScanTruncated,
				"a disabled scan must not set the truncation flag")
			assert.Nil(t, cluster.Status.LastRecommendationScanTime,
				"a disabled scan must not set the scan timestamp")
			assert.Equal(t, int32(0), atomic.LoadInt32(&dbClient.calls),
				"a disabled scan must not enter the DB Get* path")
			_, found := counterSeriesValue(t, reg,
				"cloudberry_recommendation_scan_truncated_total",
				map[string]string{"cluster": "test-cluster", "namespace": "default"})
			assert.False(t, found, "a disabled scan must not increment the counter")
		})
	}
}

func TestScenario118_Control_NilDBFactory_NoTruncation(t *testing.T) {
	cluster := scanDurationCluster("10ms")
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	reg := prometheus.NewRegistry()
	recorder := metrics.NewPrometheusRecorder(reg)
	// nil dbFactory → recordRecommendations returns early (safe no-op).
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, recorder, nil)

	require.NotPanics(t, func() {
		r.recordRecommendations(context.Background(), cluster, slog.Default())
	})

	assert.False(t, cluster.Status.RecommendationScanTruncated,
		"a nil dbFactory must not set the truncation flag")
	assert.Nil(t, cluster.Status.LastRecommendationScanTime,
		"a nil dbFactory must not set the scan timestamp")
	_, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	assert.False(t, found, "a nil dbFactory must not increment the counter")
}

// ---------------------------------------------------------------------------
// Steady-state — refreshStorageOnSteadyState also sets the truncation flag.
// ---------------------------------------------------------------------------

func TestScenario118_SteadyState_SetsTruncationFlag(t *testing.T) {
	cluster := scanDurationCluster("10ms")
	dbClient := &blockingScanDBClient{
		mockDBClient: &mockDBClient{},
		block:        2 * time.Second,
	}
	r, reg := recScanReconciler(t, cluster, dbClient)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.True(t, cluster.Status.RecommendationScanTruncated,
		"the steady-state capped scan must set RecommendationScanTruncated=true")
	assert.True(t, getCluster(t, r.client).Status.RecommendationScanTruncated,
		"the steady-state truncation flag must be persisted")
	v, found := counterSeriesValue(t, reg,
		"cloudberry_recommendation_scan_truncated_total",
		map[string]string{"cluster": "test-cluster", "namespace": "default"})
	require.True(t, found, "the steady-state path must increment the truncation counter")
	assert.InDelta(t, 1.0, v, 0.001)
}

// ---------------------------------------------------------------------------
// Registry helpers — labeled counter value and histogram sample count.
// ---------------------------------------------------------------------------

// counterSeriesValue returns the value of the counter series whose labels match
// all of the provided key/value pairs (0/false when absent).
func counterSeriesValue(
	t *testing.T, reg *prometheus.Registry, name string, want map[string]string,
) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), want) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

// histogramSampleCount returns the sample count of the histogram series whose
// labels match all of the provided key/value pairs (0/false when absent).
func histogramSampleCount(
	t *testing.T, reg *prometheus.Registry, name string, want map[string]string,
) (uint64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), want) {
				return m.GetHistogram().GetSampleCount(), true
			}
		}
	}
	return 0, false
}

// labelsMatch reports whether every key/value in want is present in labels.
func labelsMatch(labels []*dto.LabelPair, want map[string]string) bool {
	have := map[string]string{}
	for _, lp := range labels {
		have[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

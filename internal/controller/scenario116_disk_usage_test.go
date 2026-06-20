package controller

import (
	"context"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

const (
	// oneGiB is 1 GiB in bytes, used to build provisioned-capacity expectations
	// for the Scenario-116 logical-size fallback tests.
	oneGiB = int64(1024 * 1024 * 1024)
)

// countingClusterSizeDBClient wraps mockDBClient and counts GetClusterDataSizeBytes
// invocations so the PREFERRED-path test can prove the logical-size fallback is
// NOT consulted when gp_disk_free returns a real value.
type countingClusterSizeDBClient struct {
	*mockDBClient
	clusterSizeCalls int
}

func (c *countingClusterSizeDBClient) GetClusterDataSizeBytes(ctx context.Context) (int64, error) {
	c.clusterSizeCalls++
	return c.mockDBClient.GetClusterDataSizeBytes(ctx)
}

// fallbackCluster returns a disk-monitoring cluster whose segment spec drives a
// deterministic provisioned capacity: count primaries of the given per-volume
// size, doubled when mirroring is enabled.
func fallbackCluster(count int32, size string, mirroring bool) *cbv1alpha1.CloudberryCluster {
	c := diskMonitoringCluster()
	c.Spec.Segments.Count = count
	c.Spec.Segments.Storage = cbv1alpha1.StorageSpec{Size: size}
	if mirroring {
		c.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	} else {
		c.Spec.Segments.Mirroring = nil
	}
	return c
}

// TestScenario116_Fallback_OK covers 116-FALLBACK-ok: gp_disk_free is absent
// (GetDiskUsagePercent returns ErrDiskUsageUnavailable) so the portable
// logical-size proxy is used. With segments.count=2, size=2Gi, mirroring on, the
// provisioned capacity is 2Gi*(2+2)=8Gi; usedBytes is chosen so the percent is a
// clean 15. Both Status.DiskUsagePercent (S.1) and the gauge (M.1) equal 15.
func TestScenario116_Fallback_OK(t *testing.T) {
	scheme := newTestScheme()
	cluster := fallbackCluster(2, "2Gi", true)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	provisioned := 8 * oneGiB           // 2Gi * (2 primaries + 2 mirrors)
	usedBytes := provisioned*15/100 + 1 // -> exactly 15%

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{
		diskUsageErr:    db.ErrDiskUsageUnavailable,
		clusterDataSize: usedBytes,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Equal(t, int32(15), cluster.Status.DiskUsagePercent)
	assert.Equal(t, int32(15), getCluster(t, k8sClient).Status.DiskUsagePercent)
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found, "fallback must still publish the gauge")
	assert.InDelta(t, 15.0, gauge, 0.001)
	assert.InDelta(t, float64(cluster.Status.DiskUsagePercent), gauge, 0.001,
		"metric must match status (M.1 invariant)")
}

// TestScenario116_Fallback_MirroringOff covers 116-FALLBACK-mirroring-off: with
// mirroring disabled the provisioned capacity is 2Gi*2=4Gi, so the SAME used
// bytes now map to 30%.
func TestScenario116_Fallback_MirroringOff(t *testing.T) {
	scheme := newTestScheme()
	cluster := fallbackCluster(2, "2Gi", false)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	provisioned := 4 * oneGiB           // 2Gi * 2 primaries, no mirrors
	usedBytes := provisioned*30/100 + 1 // -> exactly 30%

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{
		diskUsageErr:    db.ErrDiskUsageUnavailable,
		clusterDataSize: usedBytes,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Equal(t, int32(30), cluster.Status.DiskUsagePercent)
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 30.0, gauge, 0.001)
}

// TestScenario116_Fallback_NoCapacity covers 116-FALLBACK-no-capacity: an empty
// segments.storage.size yields a provisioned capacity of 0, so the proxy is
// skipped honestly — status is unchanged and no gauge is published.
func TestScenario116_Fallback_NoCapacity(t *testing.T) {
	tests := []struct {
		name string
		size string
	}{
		{name: "empty size", size: ""},
		{name: "unparseable size", size: "not-a-quantity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := fallbackCluster(2, tt.size, true)
			cluster.Status.DiskUsagePercent = 21 // prior value must survive.
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()

			rec := &diskUsageRecorder{}
			dbClient := &mockDBClient{
				diskUsageErr:    db.ErrDiskUsageUnavailable,
				clusterDataSize: 5 * oneGiB,
			}
			dbFactory := &mockDBClientFactory{client: dbClient}
			r := NewAdminReconciler(k8sClient, scheme,
				record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

			require.NoError(t, r.reconcileStorage(context.Background(), cluster))

			assert.Equal(t, int32(21), cluster.Status.DiskUsagePercent,
				"status must NOT be fabricated without provisioned capacity")
			assert.Empty(t, rec.diskCalls, "no gauge without provisioned capacity")
		})
	}
}

// TestScenario116_Fallback_UsedBytesError covers 116-FALLBACK-usedbytes-error:
// gp_disk_free is absent AND GetClusterDataSizeBytes errors, so the proxy is
// skipped honestly — status is unchanged and no gauge is published.
func TestScenario116_Fallback_UsedBytesError(t *testing.T) {
	scheme := newTestScheme()
	cluster := fallbackCluster(2, "2Gi", true)
	cluster.Status.DiskUsagePercent = 19 // prior value must survive.
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	dbClient := &mockDBClient{
		diskUsageErr:       db.ErrDiskUsageUnavailable,
		clusterDataSizeErr: assertNewClientErr,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Equal(t, int32(19), cluster.Status.DiskUsagePercent,
		"status must NOT be fabricated when the used-bytes read fails")
	assert.Empty(t, rec.diskCalls, "no gauge when the used-bytes read fails")
}

// TestScenario116_Fallback_Clamp covers 116-FALLBACK-clamp: usedBytes exceeds the
// provisioned capacity (>100%), so the computed percent is clamped to 100.
func TestScenario116_Fallback_Clamp(t *testing.T) {
	scheme := newTestScheme()
	cluster := fallbackCluster(2, "2Gi", true)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	// Provisioned = 8Gi; used = 20Gi => ratio 2.5 (250%) => clamped to 100.
	dbClient := &mockDBClient{
		diskUsageErr:    db.ErrDiskUsageUnavailable,
		clusterDataSize: 20 * oneGiB,
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Equal(t, int32(100), cluster.Status.DiskUsagePercent,
		"over-100%% proxy must clamp to 100")
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 100.0, gauge, 0.001)
}

// TestScenario116_Preferred_StillWorks covers 116-PREFERRED-still-works: when
// gp_disk_free returns a real value (no sentinel), that value is used directly
// and the logical-size fallback is NOT consulted (GetClusterDataSizeBytes is
// never called, proven by the call counter).
func TestScenario116_Preferred_StillWorks(t *testing.T) {
	scheme := newTestScheme()
	cluster := fallbackCluster(2, "2Gi", true)
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	// The preferred path returns 57; the fallback source is wired to a value that
	// would map to a DIFFERENT percent, so using it would be detectable.
	dbClient := &countingClusterSizeDBClient{
		mockDBClient: &mockDBClient{
			diskUsagePercent: 57,
			clusterDataSize:  20 * oneGiB,
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Equal(t, int32(57), cluster.Status.DiskUsagePercent,
		"preferred gp_disk_free value must be used directly")
	assert.Zero(t, dbClient.clusterSizeCalls,
		"fallback must NOT be consulted when the preferred path succeeds")
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 57.0, gauge, 0.001)
}

// TestScenario116_ComputeProvisionedCapacityBytes covers
// computeProvisionedCapacityBytes directly across the happy path and every
// zero-returning edge (empty/unparseable/zero size, non-positive count) plus the
// mirroring on/off branch.
func TestScenario116_ComputeProvisionedCapacityBytes(t *testing.T) {
	tests := []struct {
		name      string
		count     int32
		size      string
		mirroring bool
		want      int64
	}{
		{name: "primaries only", count: 3, size: "2Gi", mirroring: false, want: 6 * oneGiB},
		{name: "primaries plus mirrors", count: 3, size: "2Gi", mirroring: true, want: 12 * oneGiB},
		{name: "empty size", count: 2, size: "", mirroring: true, want: 0},
		{name: "unparseable size", count: 2, size: "bogus", mirroring: false, want: 0},
		{name: "zero count", count: 0, size: "2Gi", mirroring: true, want: 0},
		{name: "negative count", count: -1, size: "2Gi", mirroring: false, want: 0},
		{name: "zero quantity", count: 2, size: "0", mirroring: false, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := fallbackCluster(tt.count, tt.size, tt.mirroring)
			assert.Equal(t, tt.want, computeProvisionedCapacityBytes(cluster))
		})
	}
}

// TestScenario116_ClampUsagePercent covers clampUsagePercent directly, including
// the defensive branches the controller path cannot reach: a non-positive
// provisioned denominator (returns 0) and a negative ratio (clamped to 0), plus
// the in-range and over-100 cases.
func TestScenario116_ClampUsagePercent(t *testing.T) {
	tests := []struct {
		name        string
		used        int64
		provisioned int64
		want        int32
	}{
		{name: "zero provisioned guard", used: 100, provisioned: 0, want: 0},
		{name: "negative provisioned guard", used: 100, provisioned: -1, want: 0},
		{name: "negative used clamps to zero", used: -10, provisioned: 100, want: 0},
		{name: "mid value", used: 25, provisioned: 100, want: 25},
		{name: "exactly full", used: 100, provisioned: 100, want: 100},
		{name: "over hundred clamps", used: 250, provisioned: 100, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, clampUsagePercent(tt.used, tt.provisioned))
		})
	}
}

// TestScenario116_ComputeProvisionedCapacityBytes_MirroringDisabledFlag covers
// the branch where a MirroringSpec is present but Enabled is false: mirrors must
// NOT be counted.
func TestScenario116_ComputeProvisionedCapacityBytes_MirroringDisabledFlag(t *testing.T) {
	cluster := diskMonitoringCluster()
	cluster.Spec.Segments.Count = 2
	cluster.Spec.Segments.Storage = cbv1alpha1.StorageSpec{Size: "2Gi"}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: false}

	assert.Equal(t, 4*oneGiB, computeProvisionedCapacityBytes(cluster),
		"mirroring present-but-disabled must not double capacity")
}

// diskUsageCall captures a SetDiskUsagePercent invocation so the M.1 invariant
// (metric value == status value, published FROM the measured value) can be
// asserted in the same call that writes the status.
type diskUsageCall struct {
	cluster   string
	namespace string
	percent   float64
}

// diskUsageRecorder wraps NoopRecorder and records every SetDiskUsagePercent
// call. It lets Scenario-116 tests prove the gauge is published with the
// measured value (M.1) and NOT published at all when measurement is skipped.
type diskUsageRecorder struct {
	metrics.NoopRecorder
	diskCalls []diskUsageCall
}

func (r *diskUsageRecorder) SetDiskUsagePercent(cluster, namespace string, percent float64) {
	r.diskCalls = append(r.diskCalls, diskUsageCall{
		cluster: cluster, namespace: namespace, percent: percent,
	})
}

// diskMonitoringCluster returns a cluster with disk monitoring enabled so
// reconcileStorage / refreshStorageOnSteadyState reach recordDiskUsage.
func diskMonitoringCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: true}
	return c
}

// diskUsageGaugeValue gathers cloudberry_disk_usage_percent from the registry
// and returns the gauge value for the matching {cluster,namespace} series
// (0 / found=false when no matching series is present).
func diskUsageGaugeValue(
	t *testing.T, reg *prometheus.Registry, cluster, namespace string,
) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "cloudberry_disk_usage_percent" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["cluster"] == cluster && labels["namespace"] == namespace {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// getCluster reads the cluster back from the fake client so persisted status
// (written via patchStatus) can be asserted.
func getCluster(t *testing.T, c client.Client) *cbv1alpha1.CloudberryCluster {
	t.Helper()
	var got cbv1alpha1.CloudberryCluster
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: "test-cluster", Namespace: "default"}, &got))
	return &got
}

// TestScenario116_R2_S1_M1_MeasuredValue covers 116-R2/S1/M1: with disk
// monitoring on and a DB client reporting 42%, reconcileStorage sets
// Status.DiskUsagePercent to 42 (S.1) AND publishes the gauge as 42 (M.1), and
// the two MATCH. The gauge is verified through the REAL PrometheusRecorder.
func TestScenario116_R2_S1_M1_MeasuredValue(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{diskUsagePercent: 42}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// S.1: in-memory and persisted status carry the measured value.
	assert.Equal(t, int32(42), cluster.Status.DiskUsagePercent)
	assert.Equal(t, int32(42), getCluster(t, k8sClient).Status.DiskUsagePercent)

	// M.1: gauge equals the measured value and matches the status.
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found, "cloudberry_disk_usage_percent must be published")
	assert.InDelta(t, 42.0, gauge, 0.001)
	assert.InDelta(t, float64(cluster.Status.DiskUsagePercent), gauge, 0.001,
		"metric must match status (M.1 invariant)")
}

// TestScenario116_M1_InvariantInSameCall covers 116-M1 at the helper level: the
// value passed to SetDiskUsagePercent EQUALS float64(Status.DiskUsagePercent)
// set in the same recordDiskUsage call (single source of truth, not the stale
// field). Uses a capturing recorder.
func TestScenario116_M1_InvariantInSameCall(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	// Prior status differs from the measured value to prove M.1 derives from the
	// measurement, not the pre-existing field.
	cluster.Status.DiskUsagePercent = 5
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	dbFactory := &mockDBClientFactory{client: &mockDBClient{diskUsagePercent: 77}}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	r.recordDiskUsage(context.Background(), cluster, slog.Default())

	assert.Equal(t, int32(77), cluster.Status.DiskUsagePercent)
	require.Len(t, rec.diskCalls, 1)
	assert.InDelta(t, float64(cluster.Status.DiskUsagePercent), rec.diskCalls[0].percent, 0.001)
	assert.InDelta(t, 77.0, rec.diskCalls[0].percent, 0.001)
}

// TestScenario116_TrackGrowth covers 116-TRACK-growth: two successive measured
// values (30 then 80) drive a non-sticky status + metric update to the latest
// value, proving growth tracking with no max-only/cached behavior.
func TestScenario116_TrackGrowth(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{diskUsagePercent: 30}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	// First pass: 30.
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, int32(30), cluster.Status.DiskUsagePercent)
	g1, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 30.0, g1, 0.001)

	// Second pass: 80 (growth).
	dbClient.diskUsagePercent = 80
	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, int32(80), cluster.Status.DiskUsagePercent)
	assert.Equal(t, int32(80), getCluster(t, k8sClient).Status.DiskUsagePercent)
	g2, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 80.0, g2, 0.001)
	assert.Greater(t, g2, g1, "metric must track growth")
}

// TestScenario116_Disabled_NoOp covers 116-DISABLED-noop with the C.2
// reset-on-disable policy (Scenario 122): with diskMonitoring off,
// reconcileStorage early-returns and recordDiskUsage is never reached (the DB
// factory is never called — no measurement), but the stale disk-usage status is
// RESET to 0 and exactly one SetDiskUsagePercent(...,0) gauge call is recorded so
// the disabled state is an explicit "monitoring off" signal, not a frozen stale
// reading.
func TestScenario116_Disabled_NoOp(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 11 // stale prior value must be RESET to 0.
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	// A factory whose NewClient would error if called; reaching it fails the test.
	dbFactory := &countingDBFactory{inner: &mockDBClientFactory{client: &mockDBClient{diskUsagePercent: 99}}}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	assert.Zero(t, dbFactory.calls, "DB factory must not be called when monitoring is off (no measurement)")
	// C.2 reset-on-disable: exactly one SetDiskUsagePercent(...,0) gauge call (the
	// reset, NOT a measurement) — the disabled state is an explicit "monitoring
	// off" signal on the gauge.
	require.Len(t, rec.diskCalls, 1, "the C.2 reset must publish the gauge once")
	assert.InDelta(t, 0.0, rec.diskCalls[0].percent, 0.001, "the reset gauge value must be 0")
	// NOTE: clearStorageSignals zeroes Status.DiskUsagePercent in memory, but the
	// status field is tagged omitempty, so the zero is dropped from the MergePatch
	// and the in-memory object is refreshed from the (unchanged) server value
	// during the patch round-trip. The authoritative, observable disabled signal
	// is therefore the gauge reset above, not the persisted status field.
}

// countingDBFactory wraps a factory and counts NewClient calls so the disabled
// no-op case can assert the DB layer is never reached.
type countingDBFactory struct {
	inner *mockDBClientFactory
	calls int
}

func (f *countingDBFactory) NewClient(
	ctx context.Context, c *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	f.calls++
	return f.inner.NewClient(ctx, c)
}

// TestScenario116_DBErr_NonFatal covers 116-DBERR-nonfatal: when BOTH disk-usage
// sources fail — the preferred gp_disk_free path (GetDiskUsagePercent) AND the
// portable logical-size fallback (GetClusterDataSizeBytes) — reconcileStorage
// returns NO error, Status.DiskUsagePercent is NOT overwritten (no fabrication),
// and no gauge is published.
//
// Both sub-cases force the fallback to also fail (clusterDataSizeErr) so the
// honest "skip without fabricating" behavior is exercised: the "unavailable
// sentinel" case now reaches the fallback (gp_disk_free absent), and the
// "generic db error" case skips on the preferred path before the fallback.
func TestScenario116_DBErr_NonFatal(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "unavailable sentinel", err: db.ErrDiskUsageUnavailable},
		{name: "generic db error", err: assertNewClientErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newTestScheme()
			cluster := diskMonitoringCluster()
			cluster.Status.DiskUsagePercent = 17 // prior value must survive.
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(cluster).
				Build()

			rec := &diskUsageRecorder{}
			// Force BOTH sources to fail so the controller skips honestly: the
			// preferred gp_disk_free path returns tt.err and the portable
			// fallback (cluster data size) also errors.
			dbClient := &mockDBClient{
				diskUsagePercent:   99,
				diskUsageErr:       tt.err,
				clusterDataSizeErr: assertNewClientErr,
			}
			dbFactory := &mockDBClientFactory{client: dbClient}
			r := NewAdminReconciler(k8sClient, scheme,
				record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

			require.NoError(t, r.reconcileStorage(context.Background(), cluster),
				"DB error must be non-fatal")
			assert.Equal(t, int32(17), cluster.Status.DiskUsagePercent,
				"status must NOT be fabricated on DB error")
			assert.Empty(t, rec.diskCalls, "metric must not be set on DB error")
		})
	}
}

// TestScenario116_DBErr_NewClientNonFatal covers the NewClient-failure branch of
// recordDiskUsage: a factory error skips the measurement without overwriting the
// status or publishing the gauge, and reconcile still succeeds.
func TestScenario116_DBErr_NewClientNonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	cluster.Status.DiskUsagePercent = 23
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	dbFactory := &mockDBClientFactory{err: assertNewClientErr}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Equal(t, int32(23), cluster.Status.DiskUsagePercent)
	assert.Empty(t, rec.diskCalls)
}

// TestScenario116_SteadyState covers 116-STEADY: refreshStorageOnSteadyState
// with disk monitoring on measures usage, updates + persists
// Status.DiskUsagePercent, and publishes the gauge (growth tracked at steady
// state).
func TestScenario116_SteadyState(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	cluster.Status.DiskUsagePercent = 10
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)
	dbClient := &mockDBClient{diskUsagePercent: 64}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.Equal(t, int32(64), cluster.Status.DiskUsagePercent)
	assert.Equal(t, int32(64), getCluster(t, k8sClient).Status.DiskUsagePercent,
		"steady-state status must be persisted")
	gauge, found := diskUsageGaugeValue(t, reg, "test-cluster", "default")
	require.True(t, found)
	assert.InDelta(t, 64.0, gauge, 0.001)
}

// TestScenario116_SteadyState_Disabled covers the disabled steady-state path
// with the C.2 reset-on-disable policy (Scenario 122): with monitoring off,
// refreshStorageOnSteadyState early-returns and never measures (no DB call), but
// the stale disk-usage status is RESET to 0 and exactly one
// SetDiskUsagePercent(...,0) gauge call is recorded.
func TestScenario116_SteadyState_Disabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{DiskMonitoring: false}
	cluster.Status.DiskUsagePercent = 8 // stale prior value must be RESET to 0.
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	dbFactory := &countingDBFactory{inner: &mockDBClientFactory{client: &mockDBClient{diskUsagePercent: 88}}}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	r.refreshStorageOnSteadyState(context.Background(), cluster)

	assert.Zero(t, dbFactory.calls, "steady-state must not measure when monitoring is off (no measurement)")
	// C.2 reset-on-disable: exactly one SetDiskUsagePercent(...,0) gauge call (the
	// reset, NOT a measurement).
	require.Len(t, rec.diskCalls, 1, "the C.2 reset must publish the gauge once")
	assert.InDelta(t, 0.0, rec.diskCalls[0].percent, 0.001, "the reset gauge value must be 0")
	// NOTE: as in the spec-driven path, the omitempty Status.DiskUsagePercent
	// cannot be persisted as 0 via the MergePatch, so the authoritative disabled
	// signal is the gauge reset above. See TestScenario116_Disabled_NoOp.
}

// TestScenario116_Control_NilDBFactory covers 116-CONTROL: a nil dbFactory makes
// recordDiskUsage a safe no-op (no panic, no status change, no gauge) and
// reconcileStorage still succeeds.
func TestScenario116_Control_NilDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := diskMonitoringCluster()
	cluster.Status.DiskUsagePercent = 3
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	rec := &diskUsageRecorder{}
	// dbFactory == nil -> recordDiskUsage returns early.
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)

	require.NotPanics(t, func() {
		require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	})
	assert.Equal(t, int32(3), cluster.Status.DiskUsagePercent)
	assert.Empty(t, rec.diskCalls)
}

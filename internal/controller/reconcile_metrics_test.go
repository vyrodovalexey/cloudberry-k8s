package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// assertNewClientErr is a sentinel error used to simulate DB client / query
// failures in the best-effort bloat-scan tests.
var assertNewClientErr = errors.New("db unavailable")

// reconcileCall captures a RecordReconcile invocation.
type reconcileCall struct {
	cluster   string
	namespace string
	result    string
	duration  time.Duration
}

// bloatRatioCall captures a SetTableBloatRatio invocation.
type bloatRatioCall struct {
	cluster   string
	namespace string
	table     string
	ratio     float64
}

// reconcileMetricsRecorder wraps NoopRecorder and tracks the reconcile and
// table-bloat-ratio metric calls exercised by the controllers under test.
type reconcileMetricsRecorder struct {
	metrics.NoopRecorder
	reconcileCalls  []reconcileCall
	bloatRatioCalls []bloatRatioCall
}

func (r *reconcileMetricsRecorder) RecordReconcile(cluster, namespace, result string, d time.Duration) {
	r.reconcileCalls = append(r.reconcileCalls, reconcileCall{
		cluster: cluster, namespace: namespace, result: result, duration: d,
	})
}

func (r *reconcileMetricsRecorder) SetTableBloatRatio(cluster, namespace, table string, ratio float64) {
	r.bloatRatioCalls = append(r.bloatRatioCalls, bloatRatioCall{
		cluster: cluster, namespace: namespace, table: table, ratio: ratio,
	})
}

// runningCluster returns a test cluster in the Running phase with its
// ObservedGeneration matching Generation, so reconciles take a fast path that
// still records the reconcile outcome.
func runningCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Status.Phase = cbv1alpha1.ClusterPhaseRunning
	c.Status.ObservedGeneration = c.Generation
	return c
}

func TestAdminReconciler_Reconcile_RecordsReconcileTotal(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, "test-cluster", rec.reconcileCalls[0].cluster)
	assert.Equal(t, "default", rec.reconcileCalls[0].namespace)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

func TestHAReconciler_Reconcile_RecordsReconcileTotal(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	r := NewHAReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), nil, builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

func TestAuthReconciler_Reconcile_RecordsReconcileTotal(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	r := NewAuthReconciler(k8sClient,
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultSuccess, rec.reconcileCalls[0].result)
}

// failingGetClient builds a fake client whose Get always fails with a generic
// (non-NotFound) error so a controller Reconcile returns an error and records
// reconcile_total{result="error"}.
func failingGetClient(scheme *runtime.Scheme, obj client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(obj).
		WithStatusSubresource(obj).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey,
				_ client.Object, _ ...client.GetOption) error {
				return errors.New("simulated get failure")
			},
		}).
		Build()
}

func TestAdminReconciler_Reconcile_RecordsReconcileError(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	rec := &reconcileMetricsRecorder{}
	r := NewAdminReconciler(failingGetClient(scheme, cluster), scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result)
}

func TestHAReconciler_Reconcile_RecordsReconcileError(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	rec := &reconcileMetricsRecorder{}
	r := NewHAReconciler(failingGetClient(scheme, cluster), scheme,
		record.NewFakeRecorder(20), nil, builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result)
}

func TestAuthReconciler_Reconcile_RecordsReconcileError(t *testing.T) {
	scheme := newTestScheme()
	cluster := runningCluster()
	rec := &reconcileMetricsRecorder{}
	r := NewAuthReconciler(failingGetClient(scheme, cluster),
		record.NewFakeRecorder(20), builder.NewBuilder(), rec, nil)

	_, err := r.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	require.Len(t, rec.reconcileCalls, 1)
	assert.Equal(t, reconcileResultError, rec.reconcileCalls[0].result)
}

// bloatDBClient embeds mockDBClient and returns configurable bloat
// recommendations (or an error) to drive the SetTableBloatRatio wiring.
type bloatDBClient struct {
	*mockDBClient
	recs []db.Recommendation
	err  error
}

func (m *bloatDBClient) GetBloatRecommendations(_ context.Context, _ db.RecommendationThresholds) ([]db.Recommendation, error) {
	return m.recs, m.err
}

// bloatStorageCluster returns a cluster wired for the recommendation-scan path
// (DiskMonitoring + RecommendationScan enabled) so reconcileStorage invokes
// recordTableBloatRatios.
func bloatStorageCluster() *cbv1alpha1.CloudberryCluster {
	c := newTestCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true,
		},
	}
	return c
}

func TestAdminReconciler_RecordTableBloatRatios_WiresMetric(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()
	cluster.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	dbClient := &bloatDBClient{
		mockDBClient: &mockDBClient{},
		recs: []db.Recommendation{
			{Type: "bloat", Schema: "public", Table: "orders", Value: 1000, Ratio: 42},
			{Type: "bloat", Schema: "public", Table: "items", Value: 500, Ratio: 21},
		},
	}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	err := r.reconcileStorage(context.Background(), cluster)
	require.NoError(t, err)

	require.Len(t, rec.bloatRatioCalls, 2)
	assert.Equal(t, "public.orders", rec.bloatRatioCalls[0].table)
	assert.InDelta(t, 42.0, rec.bloatRatioCalls[0].ratio, 0.001)
	assert.Equal(t, "public.items", rec.bloatRatioCalls[1].table)
	assert.InDelta(t, 21.0, rec.bloatRatioCalls[1].ratio, 0.001)
}

// TestAdminReconciler_RecordTableBloatRatios_NoDBFactory verifies the bloat
// scan is a safe no-op (no SetTableBloatRatio calls) when no DB factory is
// configured, so disk monitoring still reconciles without a DB connection.
func TestAdminReconciler_RecordTableBloatRatios_NoDBFactory(t *testing.T) {
	scheme := newTestScheme()
	cluster := bloatStorageCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	// dbFactory == nil -> recordTableBloatRatios returns early.
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Empty(t, rec.bloatRatioCalls, "no DB factory must skip the bloat scan")
}

// TestAdminReconciler_RecordTableBloatRatios_NewClientError verifies a DB
// client creation failure skips the scan without surfacing an error (the bloat
// scan is best-effort and non-fatal).
func TestAdminReconciler_RecordTableBloatRatios_NewClientError(t *testing.T) {
	scheme := newTestScheme()
	cluster := bloatStorageCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	dbFactory := &mockDBClientFactory{err: assertNewClientErr}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Empty(t, rec.bloatRatioCalls, "NewClient error must skip the bloat scan")
}

// TestAdminReconciler_RecordTableBloatRatios_QueryError verifies a
// GetBloatRecommendations error is non-fatal and emits no metric.
func TestAdminReconciler_RecordTableBloatRatios_QueryError(t *testing.T) {
	scheme := newTestScheme()
	cluster := bloatStorageCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}
	dbClient := &bloatDBClient{mockDBClient: &mockDBClient{}, err: assertNewClientErr}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))
	assert.Empty(t, rec.bloatRatioCalls, "bloat query error must skip the metric")
}

// TestAdminReconciler_RecordTableBloatRatios_TopNAndBareTable verifies the
// top-N cap (maxBloatRatioTables) bounds the number of published gauges and
// that a recommendation with an empty schema uses the bare table name as the
// metric label (no leading ".").
func TestAdminReconciler_RecordTableBloatRatios_TopNAndBareTable(t *testing.T) {
	scheme := newTestScheme()
	cluster := bloatStorageCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()
	rec := &reconcileMetricsRecorder{}

	// Build more than maxBloatRatioTables recs; the first has an empty schema so
	// the bare table name is used as the label.
	recs := make([]db.Recommendation, 0, maxBloatRatioTables+5)
	recs = append(recs, db.Recommendation{Type: "bloat", Schema: "", Table: "bare_table", Ratio: 99})
	for i := 0; i < maxBloatRatioTables+4; i++ {
		recs = append(recs, db.Recommendation{
			Type: "bloat", Schema: "public", Table: "t", Value: int64(i), Ratio: float64(i),
		})
	}
	dbClient := &bloatDBClient{mockDBClient: &mockDBClient{}, recs: recs}
	dbFactory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), dbFactory, rec, nil)

	require.NoError(t, r.reconcileStorage(context.Background(), cluster))

	// Exactly maxBloatRatioTables gauges published (top-N cap honored).
	require.Len(t, rec.bloatRatioCalls, maxBloatRatioTables)
	// First entry had an empty schema -> bare table name (no leading dot).
	assert.Equal(t, "bare_table", rec.bloatRatioCalls[0].table)
	assert.InDelta(t, 99.0, rec.bloatRatioCalls[0].ratio, 0.001)
}

// reconcileTotalValue gathers cloudberry_reconcile_total from the given registry
// and returns the counter value for the matching {cluster, namespace, result}
// label set (0 when no matching series is present).
func reconcileTotalValue(t *testing.T, reg *prometheus.Registry, cluster, namespace, result string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "cloudberry_reconcile_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["cluster"] == cluster &&
				labels["namespace"] == namespace &&
				labels["result"] == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestAdminReconciler_Reconcile_PrometheusReconcileTotal drives the AdminReconciler
// against the REAL PrometheusRecorder backed by an isolated test registry and
// asserts that cloudberry_reconcile_total increments for BOTH result="success"
// (happy path) and result="error" (Get failure), exercising the actual metric
// vector rather than a stub. This validates the deferred RecordReconcile wiring
// end-to-end against the production recorder.
func TestAdminReconciler_Reconcile_PrometheusReconcileTotal(t *testing.T) {
	scheme := newTestScheme()
	reg := prometheus.NewRegistry()
	rec := metrics.NewPrometheusRecorder(reg)

	// --- success path ---
	successCluster := runningCluster()
	successClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(successCluster).
		WithStatusSubresource(successCluster).
		Build()
	rOK := NewAdminReconciler(successClient, scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)
	_, err := rOK.Reconcile(context.Background(), reconcileRequest())
	require.NoError(t, err)

	assert.Equal(t, 1.0,
		reconcileTotalValue(t, reg, "test-cluster", "default", reconcileResultSuccess),
		"reconcile_total{result=success} must be 1")

	// --- error path (Get fails) ---
	errCluster := runningCluster()
	rErr := NewAdminReconciler(failingGetClient(scheme, errCluster), scheme,
		record.NewFakeRecorder(20), builder.NewBuilder(), nil, rec, nil)
	_, err = rErr.Reconcile(context.Background(), reconcileRequest())
	require.Error(t, err)

	assert.Equal(t, 1.0,
		reconcileTotalValue(t, reg, "test-cluster", "default", reconcileResultError),
		"reconcile_total{result=error} must be 1")
}

func reconcileRequest() ctrl.Request {
	return ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "test-cluster", Namespace: "default"},
	}
}

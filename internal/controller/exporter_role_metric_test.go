package controller

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// T-2 — setupExporterRole metric wiring (cloudberry_exporter_role_setup_total).
//
// These are the exact structural sibling of the SE.6 EnsureDataLoaderRole
// reconcile tests in security_scenario111_test.go: they drive setupExporterRole
// with a prometheus recorder backed by a fresh registry (not NoopRecorder) and
// assert the {result} label is selected honestly — "error" on a DB-boundary
// failure and "success" when the role is set up.
// ============================================================================

// errTestSetupExporter is a sentinel used to exercise the setupExporterRole
// error path (SetupExporterRole failure).
var errTestSetupExporter = errors.New("setup exporter role failed")

// errTestExporterDBClient is a sentinel used to exercise the connectivity-
// boundary error path (db factory NewClient failure).
var errTestExporterDBClient = errors.New("db client unavailable for exporter role")

// exporterRoleSpyClient is a db.Client (via embedded pxfExtDBClient) that
// records every SetupExporterRole call and can be configured to fail, so a
// reconcile test can assert which {result} label setupExporterRole selects.
type exporterRoleSpyClient struct {
	pxfExtDBClient
	setupCalls int
	setupErr   error
}

func (m *exporterRoleSpyClient) SetupExporterRole(_ context.Context, _ string) error {
	m.setupCalls++
	return m.setupErr
}

// exporterRoleSetupCount returns the value of cloudberry_exporter_role_setup_total
// for the given {result} label, and whether a sample with that label was found
// in the registry. It mirrors dataLoaderRoleSetupCount.
func exporterRoleSetupCount(t *testing.T, reg *prometheus.Registry, result string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "cloudberry_exporter_role_setup_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "result" && lp.GetValue() == result {
					return m.GetCounter().GetValue(), true
				}
			}
		}
	}
	return 0, false
}

// TestSetupExporterRole_Success_RecordsSuccess proves that a successful
// SetupExporterRole records cloudberry_exporter_role_setup_total{result=success}
// exactly once (and no error sample), and sets the ready annotation.
func TestSetupExporterRole_Success_RecordsSuccess(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	reg := prometheus.NewRegistry()
	dbClient := &exporterRoleSpyClient{}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, metrics.NewPrometheusRecorder(reg), nil)

	r.setupExporterRole(context.Background(), cluster, "secret-pass", slog.Default())

	require.Equal(t, 1, dbClient.setupCalls, "the exporter role must be set up exactly once")

	v, found := exporterRoleSetupCount(t, reg, "success")
	assert.True(t, found, "exporter_role_setup_total{result=success} must be recorded")
	assert.InDelta(t, 1.0, v, 0.001)
	_, errFound := exporterRoleSetupCount(t, reg, "error")
	assert.False(t, errFound, "no error sample on the success path")

	// On success the ready annotation is set so the controller stops retrying.
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Equal(t, "true", updated.Annotations[util.AnnotationExporterRoleReady])
}

// TestSetupExporterRole_SetupError_RecordsError proves that a SetupExporterRole
// failure records cloudberry_exporter_role_setup_total{result=error} exactly
// once (and no success sample), and does NOT set the ready annotation.
func TestSetupExporterRole_SetupError_RecordsError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	reg := prometheus.NewRegistry()
	dbClient := &exporterRoleSpyClient{setupErr: errTestSetupExporter}
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, metrics.NewPrometheusRecorder(reg), nil)

	r.setupExporterRole(context.Background(), cluster, "secret-pass", slog.Default())

	require.Equal(t, 1, dbClient.setupCalls)

	v, found := exporterRoleSetupCount(t, reg, "error")
	assert.True(t, found, "exporter_role_setup_total{result=error} must be recorded on failure")
	assert.InDelta(t, 1.0, v, 0.001)
	_, okFound := exporterRoleSetupCount(t, reg, "success")
	assert.False(t, okFound, "no success sample on the error path")

	// On failure the ready annotation stays absent so the controller retries.
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.NotContains(t, updated.Annotations, util.AnnotationExporterRoleReady)
}

// TestSetupExporterRole_DBClientError_RecordsError proves that a failure at the
// connectivity boundary (db factory NewClient error) is counted honestly as
// cloudberry_exporter_role_setup_total{result=error} and never reaches the role
// setup, so no success sample is recorded.
func TestSetupExporterRole_DBClientError_RecordsError(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	reg := prometheus.NewRegistry()
	factory := &mockDBClientFactory{err: errTestExporterDBClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, metrics.NewPrometheusRecorder(reg), nil)

	r.setupExporterRole(context.Background(), cluster, "secret-pass", slog.Default())

	v, found := exporterRoleSetupCount(t, reg, "error")
	assert.True(t, found, "exporter_role_setup_total{result=error} must be recorded on DB-client failure")
	assert.InDelta(t, 1.0, v, 0.001)
	_, okFound := exporterRoleSetupCount(t, reg, "success")
	assert.False(t, okFound, "no success sample when the DB client cannot be created")
}

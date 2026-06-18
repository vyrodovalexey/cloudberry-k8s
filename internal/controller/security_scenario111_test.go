package controller

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ============================================================================
// Scenario 111 — SE.5 (ensurePxfNetworkPolicy) + SE.6 (EnsureDataLoaderRole
// wiring) controller-level tests.
//
// Catalog IDs: 111-SE5-F (reconcile ensures the policy), 111-SE6-F (reconcile
// drives the dedicated role only when opted-in; gpadmin path unchanged).
// ============================================================================

// ----------------------------------------------------------------------------
// SE.5 — ensurePxfNetworkPolicy (111-SE5-F)
// ----------------------------------------------------------------------------

// TestEnsurePxfNetworkPolicy_CreatesForPxfCluster proves the controller creates
// the SE.5 NetworkPolicy for a PXF-enabled cluster, with an ownerRef for GC and
// the segment-primary selector — and that the PXF port 5888 is NOT in the
// allowed cross-pod ingress set. (111-SE5-F)
func TestEnsurePxfNetworkPolicy_CreatesForPxfCluster(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfNetworkPolicy(context.Background(), cluster))

	np := &networkingv1.NetworkPolicy{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PxfNetworkPolicyName(cluster.Name),
		Namespace: cluster.Namespace,
	}, np))

	// ownerRef present for GC.
	require.Len(t, np.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, np.OwnerReferences[0].Name)

	// Segment-primary selector.
	assert.Equal(t, util.ComponentSegmentPrimary,
		np.Spec.PodSelector.MatchLabels[util.LabelComponent])

	// 5888 (PXF) is NOT in the allowed ingress ports.
	for _, rule := range np.Spec.Ingress {
		for _, p := range rule.Ports {
			require.NotNil(t, p.Port)
			assert.NotEqual(t, int32(5888), p.Port.IntVal,
				"cross-pod ingress to PXF :5888 must not be allowed")
		}
	}
}

// TestEnsurePxfNetworkPolicy_Idempotent proves a second reconcile is a no-op
// (the policy already exists, no error, still exactly one object).
func TestEnsurePxfNetworkPolicy_Idempotent(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfNetworkPolicy(context.Background(), cluster))
	require.NoError(t, r.ensurePxfNetworkPolicy(context.Background(), cluster))

	list := &networkingv1.NetworkPolicyList{}
	require.NoError(t, k8sClient.List(context.Background(), list))
	assert.Len(t, list.Items, 1, "policy must be created exactly once")
}

// TestEnsurePxfNetworkPolicy_UpdatesDrift proves an existing policy whose spec
// drifted from desired is reconciled back (the update branch). (111-SE5-F)
func TestEnsurePxfNetworkPolicy_UpdatesDrift(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFDataLoadingCluster()

	// Seed an existing policy with a drifted spec (empty ingress + bad label).
	desired := builder.NewBuilder().BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(t, desired)
	stale := desired.DeepCopy()
	stale.Spec.Ingress = nil
	stale.Labels["drift"] = "yes"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster, stale).WithStatusSubresource(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfNetworkPolicy(context.Background(), cluster))

	got := &networkingv1.NetworkPolicy{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      util.PxfNetworkPolicyName(cluster.Name),
		Namespace: cluster.Namespace,
	}, got))
	// Spec reconciled back to desired (ingress restored), labels reconciled.
	require.NotEmpty(t, got.Spec.Ingress, "drifted spec must be reconciled to desired")
	assert.NotContains(t, got.Labels, "drift", "drifted labels must be reconciled")
}

// TestEnsurePxfNetworkPolicy_NoOpWhenPXFDisabled proves no policy is created
// when PXF is disabled (the builder yields nil → controller no-op). (111-SE5-F)
func TestEnsurePxfNetworkPolicy_NoOpWhenPXFDisabled(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster() // no DataLoading/PXF

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).Build()
	r := NewClusterReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), &metrics.NoopRecorder{}, nil)

	require.NoError(t, r.ensurePxfNetworkPolicy(context.Background(), cluster))

	list := &networkingv1.NetworkPolicyList{}
	require.NoError(t, k8sClient.List(context.Background(), list))
	assert.Empty(t, list.Items, "no policy when PXF is disabled")
}

// ----------------------------------------------------------------------------
// SE.6 — EnsureDataLoaderRole wiring (111-SE6-F)
// ----------------------------------------------------------------------------

// errTestEnsureRole is a sentinel error used to exercise the non-fatal SE.6
// EnsureDataLoaderRole failure path.
var errTestEnsureRole = errors.New("ensure data-loader role failed")

// dataLoaderRoleSpyClient is a db.Client (via embedded pxfExtDBClient) that
// records every EnsureDataLoaderRole call so a reconcile test can assert whether
// — and with which role name — the dedicated-role path was driven.
type dataLoaderRoleSpyClient struct {
	pxfExtDBClient
	ensureCalls []string
	ensureErr   error
}

func (m *dataLoaderRoleSpyClient) EnsureDataLoaderRole(_ context.Context, role string) error {
	m.ensureCalls = append(m.ensureCalls, role)
	return m.ensureErr
}

func newPXFRoleCluster(role string) *cbv1alpha1.CloudberryCluster {
	c := newPXFExtCluster()
	c.Spec.DataLoading.Pxf.DataLoaderRole = role
	return c
}

// dataLoaderRoleSetupCount returns the value of
// cloudberry_dataloader_role_setup_total for the given {result} label, and
// whether a sample with that label was found in the registry. It is used by the
// SE.6 tests to assert the result-label selection end-to-end (T-B2).
func dataLoaderRoleSetupCount(t *testing.T, reg *prometheus.Registry, result string) (float64, bool) {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "cloudberry_dataloader_role_setup_total" {
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

// TestSetupPXFExtensions_CallsEnsureDataLoaderRole_WhenOptedIn proves the SE.6
// wiring: with a dedicated DataLoaderRole set (!= gpadmin) and a successful
// extension install, setupPXFExtensions calls EnsureDataLoaderRole with exactly
// that role name. (111-SE6-F)
func TestSetupPXFExtensions_CallsEnsureDataLoaderRole_WhenOptedIn(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFRoleCluster("cb_dataload")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	reg := prometheus.NewRegistry()
	dbClient := &dataLoaderRoleSpyClient{}
	dbClient.pxfInstalled = 2
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, metrics.NewPrometheusRecorder(reg), nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	require.Len(t, dbClient.ensureCalls, 1, "the dedicated role must be ensured exactly once")
	assert.Equal(t, "cb_dataload", dbClient.ensureCalls[0])

	// T-B2: a successful EnsureDataLoaderRole records result="success" exactly
	// once and does NOT record an "error" sample.
	v, found := dataLoaderRoleSetupCount(t, reg, "success")
	assert.True(t, found, "dataloader_role_setup_total{result=success} must be recorded")
	assert.InDelta(t, 1.0, v, 0.001)
	_, errFound := dataLoaderRoleSetupCount(t, reg, "error")
	assert.False(t, errFound, "no error sample on the success path")
}

// TestSetupPXFExtensions_EnsureDataLoaderRoleError_NonFatal proves the SE.6
// wiring is best-effort: an EnsureDataLoaderRole error is logged and tolerated,
// so reconcile still marks PXF ready (the gpadmin load path is unaffected).
func TestSetupPXFExtensions_EnsureDataLoaderRoleError_NonFatal(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFRoleCluster("cb_dataload")

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	reg := prometheus.NewRegistry()
	dbClient := &dataLoaderRoleSpyClient{ensureErr: errTestEnsureRole}
	dbClient.pxfInstalled = 2
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, metrics.NewPrometheusRecorder(reg), nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	require.Len(t, dbClient.ensureCalls, 1)

	// T-B2: a failed EnsureDataLoaderRole records result="error" exactly once
	// and does NOT record a "success" sample.
	v, found := dataLoaderRoleSetupCount(t, reg, "error")
	assert.True(t, found, "dataloader_role_setup_total{result=error} must be recorded on failure")
	assert.InDelta(t, 1.0, v, 0.001)
	_, okFound := dataLoaderRoleSetupCount(t, reg, "success")
	assert.False(t, okFound, "no success sample on the error path")

	// Non-fatal: the ready annotation is still set (extensions installed).
	updated := &cbv1alpha1.CloudberryCluster{}
	require.NoError(t, k8sClient.Get(context.Background(),
		types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, updated))
	assert.Equal(t, "true", updated.Annotations[util.AnnotationPXFExtensionsReady])
}

// TestSetupPXFExtensions_SkipsEnsureDataLoaderRole_WhenUnset proves the honesty /
// back-compat assertion: with DataLoaderRole UNSET, setupPXFExtensions does NOT
// call EnsureDataLoaderRole — the gpadmin RP.11 path is unchanged. (111-SE6-F)
func TestSetupPXFExtensions_SkipsEnsureDataLoaderRole_WhenUnset(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFExtCluster() // DataLoaderRole unset

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &dataLoaderRoleSpyClient{}
	dbClient.pxfInstalled = 2
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	assert.Empty(t, dbClient.ensureCalls,
		"gpadmin default must NOT trigger EnsureDataLoaderRole (back-compat)")
}

// TestSetupPXFExtensions_SkipsEnsureDataLoaderRole_WhenGpadmin proves an explicit
// gpadmin role is treated as the default no-op (no dedicated-role call).
func TestSetupPXFExtensions_SkipsEnsureDataLoaderRole_WhenGpadmin(t *testing.T) {
	scheme := newTestScheme()
	cluster := newPXFRoleCluster(util.DefaultAdminUser)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()

	dbClient := &dataLoaderRoleSpyClient{}
	dbClient.pxfInstalled = 2
	factory := &mockDBClientFactory{client: dbClient}
	r := NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(10),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)

	r.setupPXFExtensions(context.Background(), cluster, slog.Default())

	assert.Empty(t, dbClient.ensureCalls,
		"explicit gpadmin must be treated as the default no-op")
}

// TestPxfDataLoaderRole_Resolution covers the helper directly: a non-empty
// DataLoaderRole is returned verbatim; empty / nil falls back to gpadmin.
func TestPxfDataLoaderRole_Resolution(t *testing.T) {
	assert.Equal(t, "cb_dataload",
		pxfDataLoaderRole(&cbv1alpha1.PxfSpec{DataLoaderRole: "cb_dataload"}))
	assert.Equal(t, util.DefaultAdminUser,
		pxfDataLoaderRole(&cbv1alpha1.PxfSpec{}))
	assert.Equal(t, util.DefaultAdminUser, pxfDataLoaderRole(nil))
}

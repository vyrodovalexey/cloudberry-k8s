package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// Scenario 106 — Server Configuration Update / Delete (SL.7–SL.8) controller
// observability. These -F (functional/reconcile) tests drive
// ensurePxfServersConfigMap over a fake client + a spy metrics recorder + a
// fake EventRecorder and assert the HONEST invariant: the PXFServersChanged
// event AND cloudberry_pxf_servers_changed_total counter fire EXACTLY ONCE on a
// real ConfigMap Data diff (a server added/removed/updated) and NEVER on a
// no-op reconcile, a labels-only change, a create, or PXF-disabled.

// pxfServersChangedCall captures one IncPXFServersChanged invocation so the
// reconcile tests can assert the metric fired with the right {cluster,namespace}
// labels — and, by counting calls, that it fired EXACTLY ONCE (honesty).
type pxfServersChangedCall struct {
	cluster   string
	namespace string
}

// pxfServersChangedSpy is a metrics.Recorder (via embedded NoopRecorder) that
// records every IncPXFServersChanged call. Counting the calls is the core
// honesty assertion: a no-op / labels-only / create reconcile must leave the
// slice empty.
type pxfServersChangedSpy struct {
	metrics.NoopRecorder
	calls []pxfServersChangedCall
}

func (m *pxfServersChangedSpy) IncPXFServersChanged(cluster, namespace string) {
	m.calls = append(m.calls, pxfServersChangedCall{cluster: cluster, namespace: namespace})
}

// seedRenderedPXFConfigMap renders the builder's PXF servers ConfigMap for the
// given cluster and returns it as the "already persisted" object — the realistic
// existing state a later reconcile of a mutated spec must diff against.
func seedRenderedPXFConfigMap(t *testing.T, cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap {
	t.Helper()
	cm := builder.NewBuilder().BuildPXFServersConfigMap(cluster)
	require.NotNil(t, cm, "baseline cluster must render a PXF servers ConfigMap")
	return cm
}

// 106-SL7-F: an existing CM carries the OLD-endpoint s3-a server; reconciling a
// spec with the patched endpoint UPDATES the persisted CM (new endpoint present,
// old absent) AND fires IncPXFServersChanged exactly once AND emits a
// PXFServersChanged Normal event naming the updated server.
func TestEnsurePxfServersConfigMap_SL7_UpdateFiresOnceAndEmitsEvent(t *testing.T) {
	scheme := newTestScheme()

	// Baseline cluster: s3-a has endpoint "https://m". Render & seed it.
	baseline := newPXFDataLoadingCluster()
	existingCM := seedRenderedPXFConfigMap(t, baseline)
	oldEndpoint := "https://m"
	require.Contains(t, existingCM.Data["s3-a__s3-site.xml"], oldEndpoint)

	// Desired cluster: same servers, but s3-a's endpoint is patched.
	cluster := newPXFDataLoadingCluster()
	cluster.Spec.DataLoading.Pxf.Servers[0].Config["fs.s3a.endpoint"] = "https://patched-endpoint"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	// CM Data was updated: new endpoint present, old absent.
	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm))
	assert.Contains(t, cm.Data["s3-a__s3-site.xml"], "https://patched-endpoint")
	assert.NotContains(t, cm.Data["s3-a__s3-site.xml"], oldEndpoint)

	// Metric fired EXACTLY once with the right labels.
	require.Len(t, spy.calls, 1)
	assert.Equal(t, pxfServersChangedCall{cluster: cluster.Name, namespace: cluster.Namespace},
		spy.calls[0])

	// Exactly one PXFServersChanged event, naming the updated server.
	events := drainEvents(recorder)
	require.Len(t, events, 1)
	assert.Contains(t, events[0], cbv1alpha1.EventReasonPXFServersChanged)
	assert.Contains(t, events[0], "updated=[s3-a]")
	assert.Contains(t, events[0], "added=[]")
	assert.Contains(t, events[0], "removed=[]")
}

// 106-SL8-F: an existing CM carries 5 servers; reconciling a spec with one
// server removed drops EXACTLY that server's "<server>__*.xml" keys AND fires
// the metric once AND emits an event with removed=[<server>].
func TestEnsurePxfServersConfigMap_SL8_RemoveFiresOnceAndDropsKeys(t *testing.T) {
	scheme := newTestScheme()

	baseline := newPXFDataLoadingCluster()
	existingCM := seedRenderedPXFConfigMap(t, baseline)
	// Sanity: the soon-to-be-removed server's keys exist in the baseline.
	require.Contains(t, existingCM.Data, "s3-b__s3-site.xml")
	require.Contains(t, existingCM.Data, "hdfs__core-site.xml")

	// Desired cluster: drop the "s3-b" server (index 1).
	cluster := newPXFDataLoadingCluster()
	servers := cluster.Spec.DataLoading.Pxf.Servers
	cluster.Spec.DataLoading.Pxf.Servers = append(servers[:1], servers[2:]...)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm))
	// EXACTLY the removed server's keys are gone; every OTHER server's keys remain.
	assert.NotContains(t, cm.Data, "s3-b__s3-site.xml")
	assert.Contains(t, cm.Data, "s3-a__s3-site.xml")
	assert.Contains(t, cm.Data, "hdfs__core-site.xml")
	assert.Contains(t, cm.Data, "mysql__jdbc-site.xml")

	require.Len(t, spy.calls, 1)

	events := drainEvents(recorder)
	require.Len(t, events, 1)
	assert.Contains(t, events[0], cbv1alpha1.EventReasonPXFServersChanged)
	assert.Contains(t, events[0], "removed=[s3-b]")
	assert.Contains(t, events[0], "added=[]")
	assert.Contains(t, events[0], "updated=[]")
}

// 106-MX-B2 (no-op honesty — the highest-value test): reconciling a spec whose
// rendered Data already equals the persisted CM Data must NOT update, NOT
// increment the metric, and NOT emit an event.
func TestEnsurePxfServersConfigMap_NoOp_FiresNothing(t *testing.T) {
	scheme := newTestScheme()

	cluster := newPXFDataLoadingCluster()
	// Seed the EXACT rendered CM (same spec) → byte-identical Data on reconcile.
	existingCM := seedRenderedPXFConfigMap(t, cluster)

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	// Honesty: no metric, no event on a no-op.
	assert.Empty(t, spy.calls, "no-op reconcile must NOT increment the counter")
	assert.Empty(t, drainEvents(recorder), "no-op reconcile must NOT emit an event")
}

// 106-MX-B3 (labels-only honesty): when ONLY Labels differ (Data identical) the
// CM is updated but the servers-changed signal does NOT fire — the signal
// tracks the SERVER SET, not labels.
func TestEnsurePxfServersConfigMap_LabelsOnly_FiresNothing(t *testing.T) {
	scheme := newTestScheme()

	cluster := newPXFDataLoadingCluster()
	existingCM := seedRenderedPXFConfigMap(t, cluster)
	// Mutate ONLY the labels of the persisted CM so Data stays byte-identical to
	// the desired render but the label set differs (forces the update branch
	// WITHOUT a Data diff).
	existingCM.Labels = map[string]string{"stale-label": "old"}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	// The labels WERE reconciled to the desired set (update branch ran)...
	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm))
	assert.NotContains(t, cm.Labels, "stale-label")

	// ...but the SERVERS-CHANGED signal did NOT fire (Data was unchanged).
	assert.Empty(t, spy.calls, "labels-only change must NOT increment the counter")
	assert.Empty(t, drainEvents(recorder), "labels-only change must NOT emit an event")
}

// 106-MX (create honesty): when the CM is absent the reconcile CREATES it but
// fires NO servers-changed signal — a create is not a change (the signal lives
// only in the update/default branch).
func TestEnsurePxfServersConfigMap_Create_FiresNothing(t *testing.T) {
	scheme := newTestScheme()

	cluster := newPXFDataLoadingCluster()

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster). // NO existing ConfigMap
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))

	// CM created.
	cm := &corev1.ConfigMap{}
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(cluster.Name),
		Namespace: cluster.Namespace,
	}, cm))
	assert.NotEmpty(t, cm.Data)

	// Honesty: create is not a change.
	assert.Empty(t, spy.calls, "create must NOT increment the counter")
	assert.Empty(t, drainEvents(recorder), "create must NOT emit a servers-changed event")
}

// 106-MX-B4 (PXF-disabled honesty): a default (non-PXF) cluster yields a nil
// desired ConfigMap, so the helper is a no-op — no panic, no metric, no event,
// reconcile returns nil.
func TestEnsurePxfServersConfigMap_PXFDisabled_FiresNothing(t *testing.T) {
	scheme := newTestScheme()
	cluster := newTestCluster() // no DataLoading/PXF

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	require.NotPanics(t, func() {
		require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))
	})

	assert.Empty(t, spy.calls)
	assert.Empty(t, drainEvents(recorder))
}

// TestEnsurePxfServersConfigMap_SecondReconcileIsNoOp proves the honesty
// invariant END-TO-END: a real update fires the signal ONCE, then an immediate
// second reconcile of the same (now-persisted) spec is byte-identical and fires
// NOTHING (106-SL7-F2 / 106-SL8-F2 shape).
func TestEnsurePxfServersConfigMap_SecondReconcileIsNoOp(t *testing.T) {
	scheme := newTestScheme()

	baseline := newPXFDataLoadingCluster()
	existingCM := seedRenderedPXFConfigMap(t, baseline)

	cluster := newPXFDataLoadingCluster()
	cluster.Spec.DataLoading.Pxf.Servers[0].Config["fs.s3a.endpoint"] = "https://patched"

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster, existingCM).
		WithStatusSubresource(cluster).
		Build()

	spy := &pxfServersChangedSpy{}
	recorder := record.NewFakeRecorder(10)
	r := NewClusterReconciler(k8sClient, scheme, recorder, builder.NewBuilder(), spy, nil)

	// First reconcile: real diff → fires once.
	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))
	require.Len(t, spy.calls, 1)
	require.Len(t, drainEvents(recorder), 1)

	// Second reconcile of the SAME spec: byte-identical → fires nothing more.
	require.NoError(t, r.ensurePxfServersConfigMap(context.Background(), cluster))
	assert.Len(t, spy.calls, 1, "second identical reconcile must NOT fire again")
	assert.Empty(t, drainEvents(recorder), "second identical reconcile must NOT emit an event")
}

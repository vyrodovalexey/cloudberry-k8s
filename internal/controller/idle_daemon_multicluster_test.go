package controller

// Tests for B-3: per-cluster idle daemons in the admin controller and the
// fresh cluster snapshot used by the daemon reconnect factory (M-3 + M-14).

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/db"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
)

// recordingDBClientFactory records every cluster object passed to NewClient
// so tests can assert which snapshot the idle daemon's reconnects use.
type recordingDBClientFactory struct {
	mu       sync.Mutex
	client   db.Client
	clusters []*cbv1alpha1.CloudberryCluster
}

func (f *recordingDBClientFactory) NewClient(
	_ context.Context, cluster *cbv1alpha1.CloudberryCluster,
) (db.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clusters = append(f.clusters, cluster)
	return f.client, nil
}

func (f *recordingDBClientFactory) lastCluster() *cbv1alpha1.CloudberryCluster {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.clusters) == 0 {
		return nil
	}
	return f.clusters[len(f.clusters)-1]
}

func newIdleRuleCluster(name, namespace, ruleName string) *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Name = name
	cluster.Namespace = namespace
	cluster.Spec.Workload = &cbv1alpha1.WorkloadSpec{
		Enabled: true,
		IdleRules: []cbv1alpha1.IdleSessionRule{
			{Name: ruleName, Enabled: true, ResourceGroup: "grp", IdleTimeout: "5m"},
		},
	}
	return cluster
}

func newIdleTestAdminReconciler(factory db.DBClientFactory) *AdminReconciler {
	scheme := newTestScheme()
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	return NewAdminReconciler(k8sClient, scheme, record.NewFakeRecorder(20),
		builder.NewBuilder(), factory, &metrics.NoopRecorder{}, nil)
}

// stopAllIdleDaemonsForTest stops every running idle daemon (test cleanup).
func stopAllIdleDaemonsForTest(r *AdminReconciler) {
	r.idleDaemonMu.Lock()
	entries := make([]*idleDaemonEntry, 0, len(r.idleDaemons))
	for key, entry := range r.idleDaemons {
		entries = append(entries, entry)
		delete(r.idleDaemons, key)
	}
	r.idleDaemonMu.Unlock()

	for _, entry := range entries {
		entry.daemon.Stop()
	}
}

func TestIdleDaemons_TwoClusters_TwoIndependentDaemons(t *testing.T) {
	factory := &recordingDBClientFactory{client: &mockDBClient{}}
	r := newIdleTestAdminReconciler(factory)
	t.Cleanup(func() { stopAllIdleDaemonsForTest(r) })

	clusterA := newIdleRuleCluster("cluster-a", "ns-a", "rule-a")
	clusterB := newIdleRuleCluster("cluster-b", "ns-b", "rule-b")

	r.startOrUpdateIdleDaemon(context.Background(), clusterA)
	r.startOrUpdateIdleDaemon(context.Background(), clusterB)

	keyA := types.NamespacedName{Name: "cluster-a", Namespace: "ns-a"}
	keyB := types.NamespacedName{Name: "cluster-b", Namespace: "ns-b"}

	r.idleDaemonMu.Lock()
	entryA := r.idleDaemons[keyA]
	entryB := r.idleDaemons[keyB]
	total := len(r.idleDaemons)
	r.idleDaemonMu.Unlock()

	require.Equal(t, 2, total, "each cluster must get its own daemon")
	require.NotNil(t, entryA)
	require.NotNil(t, entryB)
	assert.NotSame(t, entryA.daemon, entryB.daemon)

	// Each daemon enforces ITS OWN rules (a shared daemon would have had
	// cluster A's rules overwritten by B's).
	rulesA := entryA.daemon.Rules()
	rulesB := entryB.daemon.Rules()
	require.Len(t, rulesA, 1)
	require.Len(t, rulesB, 1)
	assert.Equal(t, "rule-a", rulesA[0].Name)
	assert.Equal(t, "rule-b", rulesB[0].Name)
}

func TestIdleDaemons_RulesRemoved_StopsOnlyThatDaemon(t *testing.T) {
	factory := &recordingDBClientFactory{client: &mockDBClient{}}
	r := newIdleTestAdminReconciler(factory)
	t.Cleanup(func() { stopAllIdleDaemonsForTest(r) })

	clusterA := newIdleRuleCluster("cluster-a", "ns-a", "rule-a")
	clusterB := newIdleRuleCluster("cluster-b", "ns-b", "rule-b")
	r.startOrUpdateIdleDaemon(context.Background(), clusterA)
	r.startOrUpdateIdleDaemon(context.Background(), clusterB)

	// Remove A's rules → A's daemon stops, B's keeps running.
	clusterA.Spec.Workload.IdleRules = nil
	r.startOrUpdateIdleDaemon(context.Background(), clusterA)

	keyA := types.NamespacedName{Name: "cluster-a", Namespace: "ns-a"}
	keyB := types.NamespacedName{Name: "cluster-b", Namespace: "ns-b"}

	r.idleDaemonMu.Lock()
	_, hasA := r.idleDaemons[keyA]
	_, hasB := r.idleDaemons[keyB]
	r.idleDaemonMu.Unlock()

	assert.False(t, hasA, "cluster A's daemon must be stopped and removed")
	assert.True(t, hasB, "cluster B's daemon must be untouched")
}

func TestIdleDaemons_ClusterDeletion_StopsDaemon(t *testing.T) {
	factory := &recordingDBClientFactory{client: &mockDBClient{}}
	r := newIdleTestAdminReconciler(factory)
	t.Cleanup(func() { stopAllIdleDaemonsForTest(r) })

	cluster := newIdleRuleCluster("cluster-a", "ns-a", "rule-a")
	r.startOrUpdateIdleDaemon(context.Background(), cluster)

	key := types.NamespacedName{Name: "cluster-a", Namespace: "ns-a"}

	// Reconcile a request for the (now deleted / never persisted) cluster:
	// the NotFound path must stop and remove its daemon.
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)

	r.idleDaemonMu.Lock()
	_, has := r.idleDaemons[key]
	r.idleDaemonMu.Unlock()
	assert.False(t, has, "deletion must stop and remove the cluster's daemon")
}

func TestIdleDaemons_FactorySnapshotRefreshedOnReconcile(t *testing.T) {
	factory := &recordingDBClientFactory{client: &mockDBClient{}}
	r := newIdleTestAdminReconciler(factory)
	t.Cleanup(func() { stopAllIdleDaemonsForTest(r) })

	cluster := newIdleRuleCluster("cluster-a", "ns-a", "rule-a")
	cluster.ResourceVersion = "1"
	r.startOrUpdateIdleDaemon(context.Background(), cluster)

	// Simulate a later reconcile after e.g. a password rotation / port change.
	rotated := cluster.DeepCopy()
	rotated.ResourceVersion = "2"
	rotated.Annotations = map[string]string{"example.com/password-rotated": "true"}
	r.startOrUpdateIdleDaemon(context.Background(), rotated)

	key := types.NamespacedName{Name: "cluster-a", Namespace: "ns-a"}
	r.idleDaemonMu.Lock()
	entry := r.idleDaemons[key]
	r.idleDaemonMu.Unlock()
	require.NotNil(t, entry)

	// A reconnect through the daemon's factory must use the NEW snapshot.
	dbClient, err := entry.factory.NewClient(context.Background())
	require.NoError(t, err)
	require.NotNil(t, dbClient)

	got := factory.lastCluster()
	require.NotNil(t, got)
	assert.Equal(t, "2", got.ResourceVersion,
		"reconnect must use the refreshed cluster snapshot")
	assert.Equal(t, "true", got.Annotations["example.com/password-rotated"])
}

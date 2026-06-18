//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 106: Server Configuration Update / Delete (SL.7–SL.8) — functional
// ============================================================================
//
// Black-boxes the OPERATOR-DRIVEN server-config update/delete signal through the
// REAL api.Server HTTP router + auth/RBAC middleware over a fake k8s client +
// a spy metrics recorder — infra-free, no live cluster. The TOP-LEVEL entrypoint
// driven here is the documented `pxf sync` verb:
//
//	POST /api/v1alpha1/clusters/{name}/data-loading/pxf/sync
//
// whose handler (handlePXFSync → syncPXFServersConfigMap) re-renders the shared
// <cluster>-pxf-servers ConfigMap via the SAME builder the controller uses,
// full-replaces the persisted Data, and — ONLY on a REAL Data diff — fires the
// honest cloudberry_pxf_servers_changed_total signal through the shared
// internal/util.DiffPXFServerNames helper. This exercises the full router →
// handler → diff → metric path rather than the unexported helper.
//
// HONESTY: the metric fires ONLY on a real Data diff (a server added/removed/
// updated), NEVER on a no-op sync or a first-time create. The PXFServersChanged
// Kubernetes EVENT is emitted by the controller reconcile path (the API Server
// has no EventRecorder); the event contract is pinned at the controller-unit
// layer (internal/controller cluster_controller_pxf_servers_test.go). Here the
// event reason + message format is cross-checked through the shared util helpers
// so the two paths never disagree.
//
// Covers: 106-SL7-F1 (patch endpoint → NEW persisted + metric once),
// 106-SL8-F1 (remove server → keys gone + metric once), 106-MX-B2 (no-op → no
// metric), 106-SL7-F2/106-SL8-F2 (second identical sync → no metric), plus a
// catalog-honest cross-check of the shared diff/message helpers.
// ============================================================================

const (
	scenario106Namespace = "cloudberry-test"
	scenario106Cluster   = "scenario106-pxf"
	scenario106Prefix    = "/api/v1alpha1"

	scenario106OperUser = "s106oper"
	scenario106OperPass = "s106operpass"

	// scenario106OldEndpoint / scenario106NewEndpoint are the OLD/NEW
	// minio-warehouse fs.s3a.endpoint values exercised by the SL.7 update rows.
	scenario106OldEndpoint = "http://minio-old:9000"
	scenario106NewEndpoint = "http://minio-new:9000"
)

// scenario106MetricsRecorder embeds NoopRecorder and records every
// IncPXFServersChanged call so the suite can assert the honest
// cloudberry_pxf_servers_changed_total signal fired (or did not) with the right
// {cluster,namespace} labels.
type scenario106MetricsRecorder struct {
	metrics.NoopRecorder
	serversChanged []scenario106ServersChanged
}

type scenario106ServersChanged struct {
	cluster   string
	namespace string
}

func (m *scenario106MetricsRecorder) IncPXFServersChanged(cluster, namespace string) {
	m.serversChanged = append(m.serversChanged, scenario106ServersChanged{
		cluster: cluster, namespace: namespace,
	})
}

// Scenario106Suite drives the pxf/sync server-config update/delete signal through
// the real router over a fake client + spy metrics.
type Scenario106Suite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	client  client.Client
	metrics *scenario106MetricsRecorder
	ctx     context.Context
}

func TestFunctional_Scenario106(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario106Suite))
}

func (s *Scenario106Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario106Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// scenario106PXFCluster builds the scenario106 cluster with PXF data loading
// enabled and the minio-warehouse server carrying the given fs.s3a.endpoint plus
// two stable companion servers (an hdfs multi-file server + a jdbc server) so the
// SL.8 delete leaves a non-trivial remainder to assert intact.
func scenario106PXFCluster(endpoint string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario106Cluster, scenario106Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name:   cases.Scenario106UpdateServer,
					Type:   "s3",
					Config: map[string]string{"fs.s3a.endpoint": endpoint},
				},
				{
					Name:   "hadoop-cluster",
					Type:   "hdfs",
					Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
					Hive:   map[string]string{"hive.metastore.uris": "thrift://hive:9083"},
				},
				{
					Name:   "mysql-oltp",
					Type:   "jdbc",
					Config: map[string]string{"jdbc.driver": "com.mysql.cj.jdbc.Driver"},
				},
			},
		},
	}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{Configured: true, Servers: 3},
	}
	return cluster
}

// scenario106SegmentSTS builds the empty segment-primary StatefulSet the sync
// handler bumps (its presence is required for the 202 sync path).
func scenario106SegmentSTS() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(scenario106Cluster),
			Namespace: scenario106Namespace,
		},
	}
}

// scenario106SeedCM renders the persisted <cluster>-pxf-servers ConfigMap for the
// given cluster via the SAME builder the operator uses — the realistic existing
// state a later sync of a mutated spec must diff against.
func scenario106SeedCM(cluster *cbv1alpha1.CloudberryCluster) *corev1.ConfigMap {
	return builder.NewBuilder().BuildPXFServersConfigMap(cluster)
}

// boot builds the API server (real router + auth/RBAC) over a fake client seeded
// with the cluster + any extra objects, and the spy metrics recorder.
func (s *Scenario106Suite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client
	s.metrics = &scenario106MetricsRecorder{}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario106OperUser, scenario106OperPass, auth.PermissionOperator)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, s.metrics, env.Logger, 0)
	s.handler = s.server.Handler()
}

func scenario106SyncPath() string {
	return scenario106Prefix + "/clusters/" + scenario106Cluster +
		"/data-loading/pxf/sync?namespace=" + scenario106Namespace
}

// sync POSTs the pxf/sync verb as the operator user and returns the recorder.
func (s *Scenario106Suite) sync() *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, scenario106SyncPath(), nil)
	req.SetBasicAuth(scenario106OperUser, scenario106OperPass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

// getCM reads back the persisted <cluster>-pxf-servers ConfigMap.
func (s *Scenario106Suite) getCM() *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	require.NoError(s.T(), s.client.Get(s.ctx, types.NamespacedName{
		Name:      builder.PxfServersConfigMapName(scenario106Cluster),
		Namespace: scenario106Namespace,
	}, cm))
	return cm
}

// --- 106-SL7-F1: patch endpoint → NEW persisted, old absent, metric once -----

// TestSL7_UpdateEndpointPersistsAndFiresOnce covers 106-SL7-F1 / 106-MX-B1: the
// persisted CM carries the OLD endpoint; syncing the cluster whose spec has the
// patched endpoint full-replaces the CM so minio-warehouse__s3-site.xml carries
// the NEW endpoint (old absent), the other servers stay intact, and the
// servers-changed counter fires EXACTLY once with the right labels (updated=
// [minio-warehouse] per the shared diff helper).
func (s *Scenario106Suite) TestSL7_UpdateEndpointPersistsAndFiresOnce() {
	// Seed the persisted CM rendered from the OLD-endpoint spec.
	oldCluster := scenario106PXFCluster(scenario106OldEndpoint)
	seed := scenario106SeedCM(oldCluster)
	require.NotNil(s.T(), seed)
	require.Contains(s.T(), seed.Data[cases.Scenario106UpdateFile], scenario106OldEndpoint)

	// The live cluster spec carries the NEW endpoint.
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	s.boot(cluster, scenario106SegmentSTS(), seed)

	rec := s.sync()
	require.Equal(s.T(), http.StatusAccepted, rec.Code)

	cm := s.getCM()
	// NEW endpoint present, OLD absent — the surgical re-render persisted.
	assert.Contains(s.T(), cm.Data[cases.Scenario106UpdateFile], scenario106NewEndpoint)
	assert.NotContains(s.T(), cm.Data[cases.Scenario106UpdateFile], scenario106OldEndpoint)
	// Other servers' keys remain intact.
	assert.Contains(s.T(), cm.Data, "hadoop-cluster__core-site.xml")
	assert.Contains(s.T(), cm.Data, "mysql-oltp__jdbc-site.xml")

	// Honest counter fired EXACTLY once with the right labels.
	require.Len(s.T(), s.metrics.serversChanged, 1)
	assert.Equal(s.T(),
		scenario106ServersChanged{cluster: scenario106Cluster, namespace: scenario106Namespace},
		s.metrics.serversChanged[0])

	// Cross-check the shared diff/message helpers agree the change is an UPDATE of
	// exactly minio-warehouse (the same helpers the controller event consumes).
	added, removed, updated := util.DiffPXFServerNames(seed.Data, cm.Data)
	assert.Empty(s.T(), added)
	assert.Empty(s.T(), removed)
	assert.Equal(s.T(), []string{cases.Scenario106UpdateServer}, updated)
	msg := util.FormatPXFServersChangedMessage(added, removed, updated)
	assert.Contains(s.T(), msg, "updated=["+cases.Scenario106UpdateServer+"]")
	assert.Contains(s.T(), msg, "added=[]")
	assert.Contains(s.T(), msg, "removed=[]")
}

// --- 106-SL7-F2: second identical sync is a no-op -----------------------------

// TestSL7_SecondIdenticalSyncIsNoOp covers 106-SL7-F2: after the first real
// update, an immediate second sync of the same (now-persisted) spec is
// byte-identical and fires NOTHING more.
func (s *Scenario106Suite) TestSL7_SecondIdenticalSyncIsNoOp() {
	oldCluster := scenario106PXFCluster(scenario106OldEndpoint)
	seed := scenario106SeedCM(oldCluster)
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	s.boot(cluster, scenario106SegmentSTS(), seed)

	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)
	require.Len(s.T(), s.metrics.serversChanged, 1)

	// Second identical sync: byte-identical Data → no further increment.
	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)
	assert.Len(s.T(), s.metrics.serversChanged, 1,
		"second identical sync must NOT increment the servers-changed counter")
}

// --- 106-SL8-F1: remove a server → keys gone, metric once ---------------------

// TestSL8_RemoveServerDropsKeysAndFiresOnce covers 106-SL8-F1: the persisted CM
// carries 3 servers; syncing a SHRUNK spec (mysql-oltp removed) full-replaces the
// CM so EXACTLY that server's keys are gone, the others stay intact, and the
// counter fires EXACTLY once (removed=[mysql-oltp] per the shared diff helper).
func (s *Scenario106Suite) TestSL8_RemoveServerDropsKeysAndFiresOnce() {
	// Seed the persisted CM from the full 3-server spec.
	full := scenario106PXFCluster(scenario106NewEndpoint)
	seed := scenario106SeedCM(full)
	require.NotNil(s.T(), seed)
	require.Contains(s.T(), seed.Data, "mysql-oltp__jdbc-site.xml")

	// Live cluster: drop the mysql-oltp server (index 2).
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	servers := cluster.Spec.DataLoading.Pxf.Servers
	cluster.Spec.DataLoading.Pxf.Servers = servers[:2]
	s.boot(cluster, scenario106SegmentSTS(), seed)

	rec := s.sync()
	require.Equal(s.T(), http.StatusAccepted, rec.Code)

	cm := s.getCM()
	// EXACTLY the removed server's keys are gone; the others remain.
	assert.NotContains(s.T(), cm.Data, "mysql-oltp__jdbc-site.xml")
	assert.Contains(s.T(), cm.Data, cases.Scenario106UpdateFile)
	assert.Contains(s.T(), cm.Data, "hadoop-cluster__core-site.xml")
	assert.Contains(s.T(), cm.Data, "hadoop-cluster__hdfs-site.xml")

	// Honest counter fired EXACTLY once.
	require.Len(s.T(), s.metrics.serversChanged, 1)

	// The shared diff helper reports the removal honestly.
	added, removed, updated := util.DiffPXFServerNames(seed.Data, cm.Data)
	assert.Empty(s.T(), added)
	assert.Equal(s.T(), []string{"mysql-oltp"}, removed)
	assert.Empty(s.T(), updated)
	msg := util.FormatPXFServersChangedMessage(added, removed, updated)
	assert.Contains(s.T(), msg, "removed=[mysql-oltp]")
}

// --- 106-SL8-F2: second identical shrunk sync is a no-op ----------------------

// TestSL8_SecondIdenticalShrunkSyncIsNoOp covers 106-SL8-F2: after the first real
// delete, a second sync of the same shrunk spec is byte-identical → no further
// signal.
func (s *Scenario106Suite) TestSL8_SecondIdenticalShrunkSyncIsNoOp() {
	full := scenario106PXFCluster(scenario106NewEndpoint)
	seed := scenario106SeedCM(full)
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	cluster.Spec.DataLoading.Pxf.Servers = cluster.Spec.DataLoading.Pxf.Servers[:2]
	s.boot(cluster, scenario106SegmentSTS(), seed)

	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)
	require.Len(s.T(), s.metrics.serversChanged, 1)

	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)
	assert.Len(s.T(), s.metrics.serversChanged, 1,
		"second identical shrunk sync must NOT increment the servers-changed counter")
}

// --- 106-MX-B2: no-op honesty -------------------------------------------------

// TestMX_NoOpSyncFiresNothing covers 106-MX-B2 (the core honesty invariant):
// syncing a spec whose rendered Data already equals the persisted CM Data does
// NOT increment the counter.
func (s *Scenario106Suite) TestMX_NoOpSyncFiresNothing() {
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	// Seed the EXACT rendered CM (same spec) → byte-identical Data on sync.
	seed := scenario106SeedCM(cluster)
	s.boot(cluster, scenario106SegmentSTS(), seed)

	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)
	assert.Empty(s.T(), s.metrics.serversChanged,
		"no-op sync (identical Data) must NOT increment the servers-changed counter")
}

// --- 106-MX (create honesty): a first-time create is not a change -------------

// TestMX_CreateSyncFiresNothing covers the create-honesty row: a sync that
// CREATES the ConfigMap (none existed) does NOT increment the counter.
func (s *Scenario106Suite) TestMX_CreateSyncFiresNothing() {
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	s.boot(cluster, scenario106SegmentSTS()) // no ConfigMap seeded

	require.Equal(s.T(), http.StatusAccepted, s.sync().Code)

	cm := s.getCM()
	assert.NotEmpty(s.T(), cm.Data)
	assert.Empty(s.T(), s.metrics.serversChanged,
		"create must NOT increment the servers-changed counter")
}

// --- Catalog-honest cross-check ----------------------------------------------

// TestCatalogHonest iterates cases.Scenario106Cases() and asserts the catalog is
// well-formed (unique IDs, every SL.7/SL.8/MX family present, each row carries a
// Layer + Expected + Description) and that the shared diff/message helpers behave
// per the catalog's honesty contract.
func (s *Scenario106Suite) TestCatalogHonest() {
	catalog := cases.Scenario106Cases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	reqs := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			reqs[tc.Req] = true
			assert.NotEmptyf(s.T(), tc.Layer, "%s must carry a Layer", tc.ID)
			assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
			assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
			assert.Containsf(s.T(),
				[]string{cases.Scenario106LayerBuilder, cases.Scenario106LayerReconcile,
					cases.Scenario106LayerLive}, tc.Layer,
				"%s Layer must be a known token", tc.ID)
		})
	}
	for _, req := range []string{"SL.7", "SL.8", "MX"} {
		assert.Truef(s.T(), reqs[req], "catalog must cover requirement family %s", req)
	}

	// Honesty pin: identical Data → empty diff (no signal); a no-difference map is
	// never reported as a change.
	a, r, u := util.DiffPXFServerNames(
		map[string]string{"srv__s3-site.xml": "x"},
		map[string]string{"srv__s3-site.xml": "x"})
	assert.Empty(s.T(), a)
	assert.Empty(s.T(), r)
	assert.Empty(s.T(), u)
}

// scenario106Marshal is a tiny helper kept for parity with the sibling suites'
// response-decoding pattern; the sync response body is asserted via the
// status-code + persisted-state checks above, but a couple of tests decode it.
func scenario106Marshal(rec *httptest.ResponseRecorder) map[string]interface{} {
	var resp map[string]interface{}
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	return resp
}

// TestSL7_SyncResponseShape sanity-checks the sync response envelope (synced=true
// + the configMap name) so the SL.7/SL.8 rows assert the documented contract.
func (s *Scenario106Suite) TestSL7_SyncResponseShape() {
	oldCluster := scenario106PXFCluster(scenario106OldEndpoint)
	seed := scenario106SeedCM(oldCluster)
	cluster := scenario106PXFCluster(scenario106NewEndpoint)
	s.boot(cluster, scenario106SegmentSTS(), seed)

	rec := s.sync()
	require.Equal(s.T(), http.StatusAccepted, rec.Code)
	resp := scenario106Marshal(rec)
	assert.Equal(s.T(), true, resp["synced"])
	assert.Equal(s.T(), builder.PxfServersConfigMapName(scenario106Cluster), resp["configMap"])
}

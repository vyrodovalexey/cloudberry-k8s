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
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 95: PXF CLI Lifecycle (functional)
// ============================================================================
//
// Scenario 95 black-boxes the OPERATOR-DRIVEN PXF lifecycle (the verbs surfaced
// by `cloudberry-ctl pxf status|restart|sync`) through the REAL api.Server HTTP
// router + auth/RBAC middleware over a fake k8s client — infra-free, no live
// cluster. It proves, deterministically:
//
//   - POST .../pxf/restart  -> 202; the <cluster>-segment-primary StatefulSet
//     gains/changes the restart-trigger annotation (the pod ROLL primitive); a
//     cloudberry_pxf_restart_total{result="started"} is recorded.
//   - POST .../pxf/sync     -> 202; the <cluster>-pxf-servers ConfigMap is
//     (re)created/updated AND the segment-primary restart-trigger is bumped.
//   - GET  .../pxf/status   -> 200; honest readiness aggregation from the real
//     ContainerStatuses of the seeded segment-primary pods (some pxf containers
//     Ready, some not) with the ready/total counts + spec-derived echo.
//   - pxf-not-enabled cluster -> all three return the PXF_NOT_ENABLED error and
//     perform no STS/ConfigMap mutation.
//
// SCENARIO NUMBERING NOTE: Scenario 94 (PXF Sidecar Deployment Verification) is
// RETAINED unchanged; 95 (this suite) is the PXF CLI Lifecycle that follows it.
//
// HONESTY NOTE: pxf status is derived ONLY from the real pxf container readiness
// (no synthetic health, no exec, no cross-pod HTTP). The operator-driven restart
// is a pod ROLL (STS template annotation bump), heavier than an in-place sidecar
// restart — the catalog (cases.Scenario95Cases) documents the full verb contract
// including the exec-only prepare/start/stop verbs exercised in e2e.
// ============================================================================

const (
	scenario95Namespace = "cloudberry-test"
	scenario95Cluster   = "scenario95-pxf"
	scenario95Prefix    = "/api/v1alpha1"

	scenario95BasicUser = "basicuser"
	scenario95BasicPass = "basicpass"
	scenario95OperUser  = "operuser"
	scenario95OperPass  = "operpass"
)

// scenario95MetricsRecorder embeds NoopRecorder and records RecordPXFRestart
// calls so the suite can assert cloudberry_pxf_restart_total emission.
type scenario95MetricsRecorder struct {
	metrics.NoopRecorder
	pxfRestarts []scenario95PXFRestart
}

type scenario95PXFRestart struct {
	cluster   string
	namespace string
	result    string
}

func (m *scenario95MetricsRecorder) RecordPXFRestart(cluster, namespace, result string) {
	m.pxfRestarts = append(m.pxfRestarts, scenario95PXFRestart{
		cluster: cluster, namespace: namespace, result: result,
	})
}

// Scenario95Suite drives the 3 PXF lifecycle endpoints through the real router
// over a fake client with a credential store providing Basic/Operator tiers.
type Scenario95Suite struct {
	suite.Suite
	server  *api.Server
	handler http.Handler
	client  client.Client
	metrics *scenario95MetricsRecorder
	ctx     context.Context
}

func TestFunctional_Scenario95(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario95Suite))
}

func (s *Scenario95Suite) SetupTest() {
	s.ctx = context.Background()
}

func (s *Scenario95Suite) TearDownTest() {
	if s.server != nil {
		s.server.Close()
	}
}

// scenario95PXFCluster builds the scenario95 cluster with PXF data loading
// enabled and a spec-derived status echo.
func scenario95PXFCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario95Cluster, scenario95Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "s3.local"}},
			},
		},
	}
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{Configured: true, Servers: 1},
	}
	return cluster
}

// scenario95PXFDisabledCluster builds a cluster with data loading present but
// PXF NOT enabled.
func scenario95PXFDisabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario95Cluster, scenario95Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true, Pxf: &cbv1alpha1.PxfSpec{Enabled: false}}
	return cluster
}

func scenario95SegmentSTS() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(scenario95Cluster),
			Namespace: scenario95Namespace,
		},
	}
}

func scenario95SegmentPod(name string, ready bool) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: scenario95Namespace,
			Labels: map[string]string{
				util.LabelCluster:   scenario95Cluster,
				util.LabelComponent: util.ComponentSegmentPrimary,
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "segment", Ready: true},
				{Name: "pxf", Ready: ready},
			},
		},
	}
}

// boot builds the API server (real router + auth/RBAC) over a fake client seeded
// with the cluster + any extra objects, and the spy metrics recorder.
func (s *Scenario95Suite) boot(cluster *cbv1alpha1.CloudberryCluster, extra ...client.Object) {
	objs := []client.Object{cluster}
	objs = append(objs, extra...)
	env := testutil.NewTestK8sEnv(objs...)
	s.client = env.Client
	s.metrics = &scenario95MetricsRecorder{}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario95OperUser, scenario95OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario95BasicUser, scenario95BasicPass, auth.PermissionBasic)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	s.server = api.NewServer(env.Client, authMW, nil, s.metrics, env.Logger, 0)
	s.handler = s.server.Handler()
}

func scenario95Path(action string) string {
	return scenario95Prefix + "/clusters/" + scenario95Cluster +
		"/data-loading/pxf/" + action + "?namespace=" + scenario95Namespace
}

func (s *Scenario95Suite) do(user, pass, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	return rec
}

func (s *Scenario95Suite) getSTS() *appsv1.StatefulSet {
	sts := &appsv1.StatefulSet{}
	require.NoError(s.T(), s.client.Get(s.ctx, types.NamespacedName{
		Name: util.SegmentPrimaryName(scenario95Cluster), Namespace: scenario95Namespace,
	}, sts))
	return sts
}

// --- L.4 restart ----------------------------------------------------------

func (s *Scenario95Suite) TestRestartRollsSegmentSTSAndRecordsMetric() {
	s.boot(scenario95PXFCluster(), scenario95SegmentSTS())

	rec := s.do(scenario95OperUser, scenario95OperPass, http.MethodPost, scenario95Path("restart"))
	require.Equal(s.T(), http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), true, resp["restarted"])
	assert.Equal(s.T(), util.SegmentPrimaryName(scenario95Cluster), resp["statefulSet"])

	// The STS gained a restart-trigger annotation (the pod ROLL primitive).
	sts := s.getSTS()
	assert.NotEmpty(s.T(), sts.Spec.Template.Annotations[util.AnnotationRestartTrigger])

	// cloudberry_pxf_restart_total{result="started"} recorded.
	require.Len(s.T(), s.metrics.pxfRestarts, 1)
	assert.Equal(s.T(), scenario95PXFRestart{
		cluster: scenario95Cluster, namespace: scenario95Namespace, result: "started",
	}, s.metrics.pxfRestarts[0])
}

func (s *Scenario95Suite) TestRestartMissingSTSRecordsFailure() {
	s.boot(scenario95PXFCluster()) // no STS seeded

	rec := s.do(scenario95OperUser, scenario95OperPass, http.MethodPost, scenario95Path("restart"))
	// A missing segment-primary StatefulSet is a precondition failure: 409
	// PXF_NOT_READY (Scenario 108 fixed the prior 404/INTERNAL_ERROR mismatch so
	// the status and code agree). The restart is still recorded as failed.
	assert.Equal(s.T(), http.StatusConflict, rec.Code)
	require.Len(s.T(), s.metrics.pxfRestarts, 1)
	assert.Equal(s.T(), "failed", s.metrics.pxfRestarts[0].result)
}

// --- L.5 sync -------------------------------------------------------------

func (s *Scenario95Suite) TestSyncRefreshesConfigMapAndBumpsSTS() {
	s.boot(scenario95PXFCluster(), scenario95SegmentSTS())

	rec := s.do(scenario95OperUser, scenario95OperPass, http.MethodPost, scenario95Path("sync"))
	require.Equal(s.T(), http.StatusAccepted, rec.Code)

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), true, resp["synced"])

	// The <cluster>-pxf-servers ConfigMap was created.
	cmName := builder.PxfServersConfigMapName(scenario95Cluster)
	cm := &corev1.ConfigMap{}
	require.NoError(s.T(), s.client.Get(s.ctx,
		types.NamespacedName{Name: cmName, Namespace: scenario95Namespace}, cm))
	assert.NotEmpty(s.T(), cm.Data)
	assert.Equal(s.T(), cmName, resp["configMap"])

	// The STS restart-trigger was bumped.
	sts := s.getSTS()
	assert.NotEmpty(s.T(), sts.Spec.Template.Annotations[util.AnnotationRestartTrigger])
}

// --- L.2/L.3 status -------------------------------------------------------

func (s *Scenario95Suite) TestStatusAggregatesReadiness() {
	s.boot(scenario95PXFCluster(),
		scenario95SegmentPod("seg-0", true),
		scenario95SegmentPod("seg-1", false))

	rec := s.do(scenario95BasicUser, scenario95BasicPass, http.MethodGet, scenario95Path("status"))
	require.Equal(s.T(), http.StatusOK, rec.Code)

	var resp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(s.T(), float64(1), resp["readySidecars"])
	assert.Equal(s.T(), float64(2), resp["totalSidecars"])
	assert.Equal(s.T(), float64(1), resp["servers"])
	assert.Equal(s.T(), true, resp["configured"])
}

// --- not-enabled gate -----------------------------------------------------

func (s *Scenario95Suite) TestPXFNotEnabledGate() {
	s.boot(scenario95PXFDisabledCluster(), scenario95SegmentSTS())

	cases := []struct {
		action string
		method string
		user   string
		pass   string
	}{
		{"status", http.MethodGet, scenario95BasicUser, scenario95BasicPass},
		{"restart", http.MethodPost, scenario95OperUser, scenario95OperPass},
		{"sync", http.MethodPost, scenario95OperUser, scenario95OperPass},
	}
	for _, c := range cases {
		s.Run(c.action, func() {
			rec := s.do(c.user, c.pass, c.method, scenario95Path(c.action))
			assert.Equal(s.T(), http.StatusBadRequest, rec.Code)
			assert.Contains(s.T(), rec.Body.String(), "PXF_NOT_ENABLED")
		})
	}

	// No STS mutation and no ConfigMap created on the disabled path.
	sts := s.getSTS()
	assert.Empty(s.T(), sts.Spec.Template.Annotations[util.AnnotationRestartTrigger])
	cm := &corev1.ConfigMap{}
	err := s.client.Get(s.ctx, types.NamespacedName{
		Name: builder.PxfServersConfigMapName(scenario95Cluster), Namespace: scenario95Namespace,
	}, cm)
	assert.Error(s.T(), err, "no PXF servers ConfigMap should be created on the disabled path")

	// No restart metric recorded on the disabled path.
	assert.Empty(s.T(), s.metrics.pxfRestarts)
}

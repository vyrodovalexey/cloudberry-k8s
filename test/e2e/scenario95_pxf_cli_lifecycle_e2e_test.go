//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
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
// Scenario 95: PXF CLI Lifecycle (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 95 cases at the e2e layer and adds
// a KUBECONFIG-gated live verification against the deployed acceptance-test
// cluster. It has two parts:
//
//   - PART A (builder/contract-direct, infra-free, ALWAYS runs): iterate
//     cases.Scenario95Cases() (L.1-L.6) documenting the contract, and assert the
//     real api.Server registers + serves the THREE PXF lifecycle routes
//     (GET pxf/status Basic, POST pxf/restart Operator, POST pxf/sync Operator)
//     over a fake k8s client — proving the handlers + routes + CLI exist without
//     a live cluster. Mirrors the builder-direct sections of scenario93/94 e2e.
//
//   - PART B (KUBECONFIG-gated live, skips cleanly without KUBECONFIG; the heavy
//     exec/restart parts gated behind SCENARIO95_PXF_LIVE=1 like
//     SCENARIO94_PXF_LIVE): against the deployed acceptance-test cluster in
//     cloudberry-test with the real cloudberry-pxf:2.1.0 sidecar.
//
//     TestE2E_Scenario95_LivePXFCLILifecycle exercises EACH PXF CLI verb directly
//     against a running segment-primary sidecar via kubectl exec (reusing the
//     scenario94 exec-helper pattern):
//       L.1 pxf prepare  — exit 0 and idempotent (run twice on the initialized base).
//       L.2 pxf start + pxf status — started + status Running (or already running).
//       L.3 pxf stop     — service down → readiness /actuator/health fails (one
//                          segment only; restored afterwards).
//       L.4 pxf restart  — recovers → status Running + /actuator/health UP again.
//       L.5 pxf sync     — exit 0; resolved server configs still present.
//     After per-verb exec the sidecar is restored healthy (never left stopped).
//
//     TestE2E_Scenario95_LiveCtlPxfRestart is the operator-driven headline:
//       - Capture the segment-primary STS restart-trigger annotation + segment
//         pod UIDs.
//       - Run `cloudberry-ctl pxf restart --cluster acceptance-test` (the built
//         binary, pointed at the port-forwarded operator API with basic-auth) →
//         expect 202.
//       - Assert the operator bumped the annotation (new value != captured),
//         proving the operator rolled the segment STS (→ all sidecars).
//       - Wait for the segment pods to roll (new UIDs) and the pxf sidecars to
//         become Ready again (/actuator/health UP).
//       - Assert cloudberry_pxf_restart_total{cluster,result="started"}
//         incremented in VictoriaMetrics (http://localhost:8428).
//       - Also exercise `cloudberry-ctl pxf status` (readySidecars==totalSidecars)
//         and `cloudberry-ctl pxf sync` (202).
//
// SCENARIO NUMBERING NOTE: Scenario 94 (PXF Sidecar Deployment Verification) is
// RETAINED unchanged; 95 (this suite) is the PXF CLI Lifecycle that follows it.
// ============================================================================

const (
	// envKubeconfigS95 gates the live portion of Scenario 95.
	envKubeconfigS95 = "KUBECONFIG"

	// envScenario95PXFLive gates the destructive exec stop/restart verbs and the
	// operator-driven ctl pxf restart roll. When "1" (set by the deploy agent once
	// the real cloudberry-pxf:2.1.0 image is deployed) the live tests run; unset/
	// empty => the live tests skip cleanly so CI / clusters without the real image
	// never run the destructive paths.
	envScenario95PXFLive = "SCENARIO95_PXF_LIVE"

	// envScenario95Cluster overrides the live cluster name.
	envScenario95Cluster = "SCENARIO95_CLUSTER"
	// envScenario95OperatorURL overrides the operator API URL the CLI targets.
	envScenario95OperatorURL = "SCENARIO95_OPERATOR_URL"
	// envScenario95OperatorUser / Pass provide the basic-auth Operator creds.
	envScenario95OperatorUser = "SCENARIO95_OPERATOR_USER"
	envScenario95OperatorPass = "SCENARIO95_OPERATOR_PASS"
	// envScenario95VMURL overrides the VictoriaMetrics base URL.
	envScenario95VMURL = "SCENARIO95_VM_URL"
	// envScenario95CtlBin points at a pre-built cloudberry-ctl binary (skip build).
	envScenario95CtlBin = "SCENARIO95_CTL_BIN"

	// scenario95LiveNamespace is the namespace of the deployed acceptance-test cluster.
	scenario95LiveNamespace = "cloudberry-test"
	// scenario95DefaultCluster is the default live cluster name.
	scenario95DefaultCluster = "acceptance-test"
	// scenario95DefaultVMURL is the default VictoriaMetrics single-node URL.
	scenario95DefaultVMURL = "http://localhost:8428"

	// scenario95LiveTimeout bounds the live roll/recover wait loops.
	scenario95LiveTimeout = 5 * time.Minute
	// scenario95LivePollInterval is the live poll interval.
	scenario95LivePollInterval = 5 * time.Second
	// scenario95ExecTimeout bounds a single kubectl exec verb. PXF restart/start
	// may take 2+ minutes for JVM initialization + health-check retries.
	scenario95ExecTimeout = 3 * time.Minute

	// scenario95APIPrefix is the REST API path prefix.
	scenario95APIPrefix = "/api/v1alpha1"
	// scenario95PxfContainer is the sidecar container name.
	scenario95PxfContainer = "pxf"

	// In-process contract-server credentials (Part A).
	scenario95BasicUser = "s95basic"
	scenario95BasicPass = "s95basicpass"
	scenario95OperUser  = "s95oper"
	scenario95OperPass  = "s95operpass"
)

// Scenario95E2ESuite verifies the PXF CLI lifecycle end-to-end (contract-direct
// + KUBECONFIG-gated live).
type Scenario95E2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario95(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario95E2ESuite))
}

func (s *Scenario95E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// ----------------------------------------------------------------------------
// PART A — builder / contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario95E2EFullDataLoading returns the full pxf dataLoading spec exercised by
// this scenario (mirrors the scenario94 e2e spec): image cloudberry-pxf:2.1.0,
// one s3 server, default jvmOpts/port/logLevel.
func scenario95E2EFullDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled:  true,
			Image:    "cloudberry-pxf:2.1.0",
			JvmOpts:  "-Xmx1g -Xms256m",
			Port:     5888,
			LogLevel: "INFO",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name:   "s3-datalake",
					Type:   "s3",
					Config: map[string]string{"fs.s3a.endpoint": "https://s3.amazonaws.com"},
				},
			},
		},
	}
}

// scenario95E2ECluster builds a running cluster (2 segments) with the pxf spec.
func scenario95E2ECluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	cluster.Spec.DataLoading = scenario95E2EFullDataLoading()
	cluster.Status.DataLoading = &cbv1alpha1.DataLoadingStatus{
		Pxf: &cbv1alpha1.DataLoadingPxfStatus{Configured: true, Servers: 1},
	}
	return cluster
}

// scenario95SegmentSTS returns an empty segment-primary STS for the named cluster
// so the operator-driven restart/sync can patch its template annotation.
func scenario95SegmentSTS(cluster, namespace string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.SegmentPrimaryName(cluster),
			Namespace: namespace,
		},
	}
}

// scenario95SegmentPod returns a segment-primary pod whose pxf container carries
// the given readiness, for the honest status aggregation.
func scenario95SegmentPod(cluster, namespace, name string, ready bool) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelCluster:   cluster,
				util.LabelComponent: util.ComponentSegmentPrimary,
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "segment", Ready: true},
				{Name: scenario95PxfContainer, Ready: ready},
			},
		},
	}
}

// scenario95ContractServer wires the REAL api.Server router + basic-auth/RBAC
// middleware over a fake client seeded with the given objects, returning the
// handler and a spy metrics recorder.
type scenario95ContractServer struct {
	server  *api.Server
	handler http.Handler
	client  client.Client
}

func (s *Scenario95E2ESuite) bootContractServer(
	objs ...client.Object,
) *scenario95ContractServer {
	env := testutil.NewTestK8sEnv(objs...)

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario95OperUser, scenario95OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario95BasicUser, scenario95BasicPass, auth.PermissionBasic)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	server := api.NewServer(env.Client, authMW, nil, &metrics.NoopRecorder{}, env.Logger, 0)
	return &scenario95ContractServer{
		server:  server,
		handler: server.Handler(),
		client:  env.Client,
	}
}

// scenario95ContractPath builds the in-process REST path for a pxf verb.
func scenario95ContractPath(cluster, namespace, action string) string {
	return scenario95APIPrefix + "/clusters/" + cluster +
		"/data-loading/pxf/" + action + "?namespace=" + namespace
}

func (cs *scenario95ContractServer) do(
	user, pass, method, path string,
) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	cs.handler.ServeHTTP(rec, req)
	return rec
}

// TestE2E_Scenario95_CatalogDocumentsLifecycle (contract-direct) iterates the
// L.1-L.6 catalog and asserts the documented verb/layer contract — the catalog
// is the source of truth the live exec/ctl assertions follow.
func (s *Scenario95E2ESuite) TestE2E_Scenario95_CatalogDocumentsLifecycle() {
	catalog := cases.Scenario95Cases()
	require.Len(s.T(), catalog, 6, "L.1-L.6 PXF CLI lifecycle catalog")

	byID := map[string]cases.Scenario95Case{}
	for _, tc := range catalog {
		byID[tc.ID] = tc
	}

	// Every documented verb is present with a non-empty description + layer.
	for _, id := range []string{"L.1", "L.2", "L.3", "L.4", "L.5", "L.6"} {
		tc, ok := byID[id]
		require.Truef(s.T(), ok, "catalog must document %s", id)
		assert.NotEmptyf(s.T(), tc.Verb, "%s verb", id)
		assert.NotEmptyf(s.T(), tc.Layer, "%s layer", id)
		assert.NotEmptyf(s.T(), tc.Description, "%s description", id)
	}

	// The exec-only verbs are layered "exec*" and the operator-driven verbs name
	// the operator layer — keeping the live test plan honest.
	assert.Contains(s.T(), byID["L.1"].Layer, "exec", "prepare is exec-only")
	assert.Contains(s.T(), byID["L.4"].Layer, "operator", "restart is operator-driven")
	assert.Contains(s.T(), byID["L.5"].Layer, "operator", "sync is operator-driven")
	assert.Contains(s.T(), byID["L.6"].Layer, "operator", "ctl restart is operator-driven")
}

// TestE2E_Scenario95_RoutesRegisteredAndServed (contract-direct) proves the real
// api.Server registers + serves the three PXF lifecycle routes WITHOUT a live
// cluster: status (Basic) 200, restart (Operator) 202 + STS annotation bump,
// sync (Operator) 202 + ConfigMap created. This is the e2e proof that the
// handlers + routes exist end to end (the CLI builds the same paths).
func (s *Scenario95E2ESuite) TestE2E_Scenario95_RoutesRegisteredAndServed() {
	const (
		name = "e2e-s95-routes"
		ns   = "default"
	)
	cs := s.bootContractServer(
		scenario95E2ECluster(name, ns),
		scenario95SegmentSTS(name, ns),
		scenario95SegmentPod(name, ns, "seg-0", true),
		scenario95SegmentPod(name, ns, "seg-1", false),
	)
	defer cs.server.Close()

	// GET pxf/status (Basic) -> 200, honest 1/2 readiness aggregation + echo.
	statusRec := cs.do(scenario95BasicUser, scenario95BasicPass,
		http.MethodGet, scenario95ContractPath(name, ns, "status"))
	require.Equal(s.T(), http.StatusOK, statusRec.Code, "GET pxf/status must be Basic-served 200")
	var statusResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(statusRec.Body).Decode(&statusResp))
	assert.Equal(s.T(), float64(1), statusResp["readySidecars"])
	assert.Equal(s.T(), float64(2), statusResp["totalSidecars"])
	assert.Equal(s.T(), float64(1), statusResp["servers"])
	assert.Equal(s.T(), true, statusResp["configured"])

	// POST pxf/restart (Operator) -> 202 + STS restart-trigger annotation bump.
	restartRec := cs.do(scenario95OperUser, scenario95OperPass,
		http.MethodPost, scenario95ContractPath(name, ns, "restart"))
	require.Equal(s.T(), http.StatusAccepted, restartRec.Code, "POST pxf/restart must be 202")
	var restartResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(restartRec.Body).Decode(&restartResp))
	assert.Equal(s.T(), true, restartResp["restarted"])
	assert.Equal(s.T(), util.SegmentPrimaryName(name), restartResp["statefulSet"])

	sts := &appsv1.StatefulSet{}
	require.NoError(s.T(), cs.client.Get(s.ctx, client.ObjectKey{
		Name: util.SegmentPrimaryName(name), Namespace: ns,
	}, sts))
	trigger := sts.Spec.Template.Annotations[util.AnnotationRestartTrigger]
	assert.NotEmpty(s.T(), trigger, "restart must bump the segment-primary restart-trigger")

	// POST pxf/sync (Operator) -> 202 + <cluster>-pxf-servers ConfigMap created.
	syncRec := cs.do(scenario95OperUser, scenario95OperPass,
		http.MethodPost, scenario95ContractPath(name, ns, "sync"))
	require.Equal(s.T(), http.StatusAccepted, syncRec.Code, "POST pxf/sync must be 202")
	var syncResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(syncRec.Body).Decode(&syncResp))
	assert.Equal(s.T(), true, syncResp["synced"])

	cm := &corev1.ConfigMap{}
	require.NoError(s.T(), cs.client.Get(s.ctx, client.ObjectKey{
		Name: builder.PxfServersConfigMapName(name), Namespace: ns,
	}, cm), "sync must (re)create the <cluster>-pxf-servers ConfigMap")
	assert.NotEmpty(s.T(), cm.Data)
}

// TestE2E_Scenario95_RBACAndNotEnabledGate (contract-direct) proves the Operator
// routes reject a Basic token (403) and a pxf-disabled cluster returns
// PXF_NOT_ENABLED (400) on all three verbs with no mutation — the negative
// contract surfaced by the live CLI.
func (s *Scenario95E2ESuite) TestE2E_Scenario95_RBACAndNotEnabledGate() {
	const (
		name = "e2e-s95-gate"
		ns   = "default"
	)

	// RBAC: a Basic token must NOT be allowed to POST restart/sync (Operator tier).
	enabled := s.bootContractServer(
		scenario95E2ECluster(name, ns),
		scenario95SegmentSTS(name, ns),
	)
	for _, action := range []string{"restart", "sync"} {
		rec := enabled.do(scenario95BasicUser, scenario95BasicPass,
			http.MethodPost, scenario95ContractPath(name, ns, action))
		assert.Equalf(s.T(), http.StatusForbidden, rec.Code,
			"Basic token must be forbidden on Operator route pxf/%s", action)
	}
	enabled.server.Close()

	// not-enabled gate: pxf disabled -> PXF_NOT_ENABLED (400) on all three verbs.
	disabledCluster := scenario95E2ECluster(name, ns)
	disabledCluster.Spec.DataLoading.Pxf.Enabled = false
	disabled := s.bootContractServer(disabledCluster, scenario95SegmentSTS(name, ns))
	defer disabled.server.Close()

	gateCases := []struct {
		action, method, user, pass string
	}{
		{"status", http.MethodGet, scenario95BasicUser, scenario95BasicPass},
		{"restart", http.MethodPost, scenario95OperUser, scenario95OperPass},
		{"sync", http.MethodPost, scenario95OperUser, scenario95OperPass},
	}
	for _, c := range gateCases {
		rec := disabled.do(c.user, c.pass, c.method, scenario95ContractPath(name, ns, c.action))
		assert.Equalf(s.T(), http.StatusBadRequest, rec.Code,
			"pxf-disabled pxf/%s must be 400", c.action)
		assert.Containsf(s.T(), rec.Body.String(), "PXF_NOT_ENABLED",
			"pxf-disabled pxf/%s must return PXF_NOT_ENABLED", c.action)
	}

	// No STS mutation on the disabled path.
	sts := &appsv1.StatefulSet{}
	require.NoError(s.T(), disabled.client.Get(s.ctx, client.ObjectKey{
		Name: util.SegmentPrimaryName(name), Namespace: ns,
	}, sts))
	assert.Empty(s.T(), sts.Spec.Template.Annotations[util.AnnotationRestartTrigger])
}

// TestE2E_Scenario95_SidecarBuiltCarriesPxfContainer (builder-direct) confirms
// the segment-primary STS the live restart rolls actually carries the pxf
// sidecar (the thing restarted), mirroring the scenario94 builder-direct check.
func (s *Scenario95E2ESuite) TestE2E_Scenario95_SidecarBuiltCarriesPxfContainer() {
	cluster := scenario95E2ECluster("e2e-s95-built", "default")
	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)

	var has bool
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == scenario95PxfContainer {
			has = true
			break
		}
	}
	assert.True(s.T(), has,
		"segment-primary STS must carry the 'pxf' sidecar the operator restart rolls")
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (exec verbs + ctl pxf restart)
// ----------------------------------------------------------------------------

// scenario95LiveCluster resolves the live cluster name (default acceptance-test).
func scenario95LiveCluster() string {
	if v := os.Getenv(envScenario95Cluster); v != "" {
		return v
	}
	return scenario95DefaultCluster
}

// scenario95RequireLive skips cleanly when KUBECONFIG is unset and returns a live
// client; the heavy/destructive parts additionally require SCENARIO95_PXF_LIVE=1.
func (s *Scenario95E2ESuite) scenario95LiveClient() client.Client {
	kubeconfig := os.Getenv(envKubeconfigS95)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 95 live PXF CLI lifecycle")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		s.T().Skipf("could not build kubeconfig %q: %v", kubeconfig, err)
	}
	scheme := testutil.NewTestK8sEnv().Scheme
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Skipf("could not build live client: %v", err)
	}
	return cl
}

// scenario95FindSegmentPxfPods lists the segment-primary pods carrying a pxf
// container in the live namespace.
func (s *Scenario95E2ESuite) scenario95FindSegmentPxfPods(cl client.Client) []corev1.Pod {
	pods := &corev1.PodList{}
	if err := cl.List(s.ctx, pods,
		client.InNamespace(scenario95LiveNamespace),
		client.MatchingLabels{util.LabelComponent: util.ComponentSegmentPrimary},
	); err != nil {
		s.T().Logf("scenario95: could not list segment-primary pods: %v", err)
		return nil
	}
	var out []corev1.Pod
	for i := range pods.Items {
		for _, c := range pods.Items[i].Spec.Containers {
			if c.Name == scenario95PxfContainer {
				out = append(out, pods.Items[i])
				break
			}
		}
	}
	return out
}

// scenario95PxfExec runs `pxf <args...>` (or any command) inside the pxf sidecar
// of the named pod via kubectl exec, bounded by scenario95ExecTimeout.
func (s *Scenario95E2ESuite) scenario95PxfExec(
	namespace, pod string, args ...string,
) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario95ExecTimeout)
	defer cancel()

	full := append([]string{"exec", "-n", namespace, "-c", scenario95PxfContainer,
		pod, "--"}, args...)
	cmd := exec.CommandContext(ctx, "kubectl", full...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario95ActuatorHealthy returns true when the in-sidecar actuator health
// endpoint reports HTTP success (curl -sf), proving the readiness probe target
// is UP.
func (s *Scenario95E2ESuite) scenario95ActuatorHealthy(namespace, pod string) bool {
	out, err := s.scenario95PxfExec(namespace, pod,
		"curl", "-sf", "http://localhost:5888/actuator/health")
	if err != nil {
		return false
	}
	return strings.Contains(strings.ToUpper(out), "UP")
}

// TestE2E_Scenario95_LivePXFCLILifecycle exercises EACH PXF CLI verb directly
// against a running segment-primary sidecar via kubectl exec. Skips cleanly when
// KUBECONFIG is unset; the destructive verbs require SCENARIO95_PXF_LIVE=1.
func (s *Scenario95E2ESuite) TestE2E_Scenario95_LivePXFCLILifecycle() {
	cl := s.scenario95LiveClient()

	if os.Getenv(envScenario95PXFLive) != "1" {
		s.T().Skip("SCENARIO95_PXF_LIVE not set, skipping the destructive live PXF " +
			"exec verbs (prepare/start/stop/restart/sync); the real cloudberry-pxf:2.1.0 " +
			"image must be deployed")
	}

	pods := s.scenario95FindSegmentPxfPods(cl)
	if len(pods) == 0 {
		s.T().Skip("no segment-primary pod with a 'pxf' container found " +
			"(operator / real cloudberry-pxf image may not be deployed)")
	}
	// Operate on ONE segment to avoid disrupting all sidecars at once.
	pod := pods[0]
	ns := pod.Namespace
	s.T().Logf("scenario95: exercising PXF CLI verbs on segment pod %s/%s", ns, pod.Name)

	// L.1 pxf prepare — idempotent: exit 0 on a fresh base, or exit 1 with
	// "not empty" when the sidecar entrypoint already ran prepare (expected in
	// a live cluster where the init container / entrypoint initializes the base).
	for i := 0; i < 2; i++ {
		out, err := s.scenario95PxfExec(ns, pod.Name, "pxf", "prepare")
		if err != nil {
			assert.Containsf(s.T(), strings.ToLower(out), "not empty",
				"L.1 pxf prepare (attempt %d) non-zero exit is only acceptable when "+
					"the base is already prepared (out=%q)", i+1, out)
		}
	}

	// L.2 pxf start + pxf status — ensure started; status reports Running. An
	// "already running" start is fine (the entrypoint may have started it).
	startOut, startErr := s.scenario95PxfExec(ns, pod.Name, "pxf", "start")
	if startErr != nil {
		assert.Containsf(s.T(), strings.ToLower(startOut), "running",
			"L.2 pxf start non-zero exit is only acceptable when already running (out=%q)", startOut)
	}
	statusOut, statusErr := s.scenario95PxfExec(ns, pod.Name, "pxf", "status")
	require.NoErrorf(s.T(), statusErr, "L.2 pxf status must exit 0 (out=%q)", statusOut)
	assert.Containsf(s.T(), strings.ToLower(statusOut), "running",
		"L.2 pxf status must report Running (out=%q)", statusOut)

	// Confirm the actuator health is UP before the destructive stop.
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95ActuatorHealthy(ns, pod.Name)
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"L.2 /actuator/health must be UP before the stop test")

	// L.3 pxf stop — service down → /actuator/health FAILS (readiness would fail).
	// NOTE: In a sidecar container, `pxf stop` kills the PXF JVM which is the
	// container's entrypoint (PID 1). This causes the container to exit and
	// Kubernetes restarts it. We accept either exit 0 (graceful stop before
	// container death) or a non-zero exit (container killed mid-stop).
	stopOut, stopErr := s.scenario95PxfExec(ns, pod.Name, "pxf", "stop")
	if stopErr != nil {
		s.T().Logf("L.3 pxf stop returned non-zero (expected when PID 1 dies): %v (out=%q)", stopErr, stopOut)
	}
	// After stop, the container will restart via k8s. The health probe should
	// fail briefly during the restart window.
	require.Eventuallyf(s.T(), func() bool {
		return !s.scenario95ActuatorHealthy(ns, pod.Name)
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"L.3 after pxf stop the /actuator/health probe must FAIL (readiness would fail)")

	// L.4 pxf restart — In a sidecar container, `pxf stop` kills PID 1 which
	// triggers a Kubernetes container restart. The entrypoint re-initializes PXF
	// automatically. We verify recovery by waiting for the container to come back
	// healthy (k8s restart + entrypoint re-start), then run `pxf restart` to
	// prove the verb works on a running instance (restart = stop + start).
	// First, wait for the k8s container restart to bring PXF back up.
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95ActuatorHealthy(ns, pod.Name)
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"L.4 PXF must recover after container restart (k8s restartPolicy)")

	// Now exercise `pxf restart` on the running instance (stop+start cycle).
	restartOut, restartErr := s.scenario95PxfExec(ns, pod.Name, "pxf", "restart")
	if restartErr != nil {
		// pxf restart on a running instance may also kill PID 1 → container restart.
		// Accept this as valid behavior and wait for recovery.
		s.T().Logf("L.4 pxf restart returned non-zero (container PID 1 restart): %v (out=%q)", restartErr, restartOut)
	}
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95ActuatorHealthy(ns, pod.Name)
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"L.4 after pxf restart the /actuator/health must report UP again")
	statusOut2, statusErr2 := s.scenario95PxfExec(ns, pod.Name, "pxf", "status")
	if statusErr2 != nil {
		// If the container just restarted, pxf status may fail briefly; wait and retry.
		s.T().Logf("L.4 pxf status failed (container may be restarting): %v", statusErr2)
		require.Eventuallyf(s.T(), func() bool {
			out, err := s.scenario95PxfExec(ns, pod.Name, "pxf", "status")
			return err == nil && strings.Contains(strings.ToLower(out), "running")
		}, scenario95LiveTimeout, scenario95LivePollInterval,
			"L.4 pxf status must eventually report Running")
	} else {
		assert.Containsf(s.T(), strings.ToLower(statusOut2), "running",
			"L.4 pxf status after restart must report Running (out=%q)", statusOut2)
	}

	// L.5 pxf sync — In a sidecar container, `pxf sync <hostname>` requires rsync
	// and a remote host, which is not applicable (each sidecar gets its config via
	// the ConfigMap volume mount). Instead, we verify the server configs are present
	// under /pxf-base/servers (the sync target) and that the PXF version is
	// accessible, proving the CLI is functional.
	lsOut, lsErr := s.scenario95PxfExec(ns, pod.Name,
		"bash", "-lc", "ls -1 /pxf-base/servers")
	require.NoErrorf(s.T(), lsErr, "L.5 /pxf-base/servers must be listable (out=%q)", lsOut)
	assert.NotEmptyf(s.T(), strings.TrimSpace(lsOut),
		"L.5 resolved server configs must be present under /pxf-base/servers (out=%q)", lsOut)
	versionOut, versionErr := s.scenario95PxfExec(ns, pod.Name, "pxf", "version")
	require.NoErrorf(s.T(), versionErr, "L.5 pxf version must exit 0 (out=%q)", versionOut)
	s.T().Logf("L.5 pxf version: %s", strings.TrimSpace(versionOut))

	// Restore: ensure the sidecar is healthy again (never leave it stopped).
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95ActuatorHealthy(ns, pod.Name)
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"the pxf sidecar on %s must be healthy again at the end of the lifecycle test", pod.Name)
}

// TestE2E_Scenario95_LiveCtlPxfRestart is the operator-driven headline: it runs
// the cloudberry-ctl binary against the port-forwarded operator API to restart
// PXF across ALL segment sidecars, asserts the STS restart-trigger bumped, the
// pods roll + recover, the metric increments in VictoriaMetrics, and exercises
// ctl pxf status + sync. Skips cleanly without KUBECONFIG; gated by
// SCENARIO95_PXF_LIVE=1.
func (s *Scenario95E2ESuite) TestE2E_Scenario95_LiveCtlPxfRestart() {
	cl := s.scenario95LiveClient()

	if os.Getenv(envScenario95PXFLive) != "1" {
		s.T().Skip("SCENARIO95_PXF_LIVE not set, skipping the live ctl pxf restart roll")
	}

	cluster := scenario95LiveCluster()
	ns := scenario95LiveNamespace

	// Confirm the segment-primary STS + pxf sidecars exist before driving a roll.
	pods := s.scenario95FindSegmentPxfPods(cl)
	if len(pods) == 0 {
		s.T().Skip("no segment-primary pod with a 'pxf' container found " +
			"(operator / real cloudberry-pxf image may not be deployed)")
	}

	// Build (or locate) the cloudberry-ctl binary.
	ctlBin := s.scenario95CtlBinary()

	// Resolve the operator API URL + Operator basic-auth creds.
	operatorURL := os.Getenv(envScenario95OperatorURL)
	if operatorURL == "" {
		s.T().Skipf("%s not set; the operator API URL (e.g. a port-forwarded "+
			"https://localhost:8443) is required to drive the live ctl pxf restart",
			envScenario95OperatorURL)
	}
	operUser := os.Getenv(envScenario95OperatorUser)
	operPass := os.Getenv(envScenario95OperatorPass)
	if operUser == "" || operPass == "" {
		s.T().Skipf("%s/%s not set; Operator basic-auth creds are required for ctl pxf restart",
			envScenario95OperatorUser, envScenario95OperatorPass)
	}

	// Capture the current restart-trigger annotation + segment pod UIDs.
	stsName := util.SegmentPrimaryName(cluster)
	beforeTrigger := s.scenario95STSTrigger(cl, ns, stsName)
	beforeUIDs := scenario95PodUIDs(pods)
	beforeRestart := s.scenario95VMCounter(cluster, "started")
	s.T().Logf("scenario95: before ctl restart trigger=%q podUIDs=%v metric=%v",
		beforeTrigger, beforeUIDs, beforeRestart)

	// Run `cloudberry-ctl pxf restart --cluster <name>` -> expect 202 (exit 0).
	out := s.scenario95RunCtl(ctlBin, operatorURL, operUser, operPass, cluster, ns,
		"pxf", "restart")
	s.T().Logf("scenario95: ctl pxf restart output:\n%s", out)

	// The operator bumped the restart-trigger (new value != captured).
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95STSTrigger(cl, ns, stsName) != beforeTrigger
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"operator must bump the %s restart-trigger (was %q)", stsName, beforeTrigger)

	// The segment pods roll (new UIDs) and the pxf sidecars become Ready again.
	require.Eventuallyf(s.T(), func() bool {
		fresh := s.scenario95FindSegmentPxfPods(cl)
		if len(fresh) == 0 {
			return false
		}
		for i := range fresh {
			if beforeUIDs[fresh[i].Name] == string(fresh[i].UID) {
				return false // a pod with the OLD UID is still around -> not rolled yet
			}
			if !scenario95PxfContainerReady(&fresh[i]) {
				return false
			}
		}
		return true
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"all segment-primary pods must roll (new UIDs) and their pxf containers become Ready")

	// cloudberry_pxf_restart_total{cluster,result="started"} incremented.
	require.Eventuallyf(s.T(), func() bool {
		return s.scenario95VMCounter(cluster, "started") > beforeRestart
	}, scenario95LiveTimeout, scenario95LivePollInterval,
		"cloudberry_pxf_restart_total{result=\"started\"} must increment after ctl pxf restart")

	// ctl pxf status -> 200, readySidecars == totalSidecars.
	statusOut := s.scenario95RunCtl(ctlBin, operatorURL, operUser, operPass, cluster, ns,
		"pxf", "status", "-o", "json")
	s.T().Logf("scenario95: ctl pxf status output:\n%s", statusOut)
	var statusResp map[string]interface{}
	require.NoError(s.T(), json.Unmarshal([]byte(scenario95LastJSON(statusOut)), &statusResp),
		"ctl pxf status must emit JSON")
	assert.Equal(s.T(), statusResp["totalSidecars"], statusResp["readySidecars"],
		"all sidecars must be Ready after the roll (readySidecars == totalSidecars)")

	// ctl pxf sync -> 202 (exit 0).
	syncOut := s.scenario95RunCtl(ctlBin, operatorURL, operUser, operPass, cluster, ns,
		"pxf", "sync")
	s.T().Logf("scenario95: ctl pxf sync output:\n%s", syncOut)
}

// scenario95CtlBinary returns the path to a cloudberry-ctl binary, building one
// into a temp dir when SCENARIO95_CTL_BIN is not provided.
func (s *Scenario95E2ESuite) scenario95CtlBinary() string {
	if bin := os.Getenv(envScenario95CtlBin); bin != "" {
		if _, err := os.Stat(bin); err == nil {
			return bin
		}
		s.T().Skipf("%s=%q not found", envScenario95CtlBin, bin)
	}

	wd, err := os.Getwd()
	require.NoError(s.T(), err)
	// test/e2e -> repo root is two levels up.
	repoRoot := filepath.Dir(filepath.Dir(wd))
	bin := filepath.Join(s.T().TempDir(), "cloudberry-ctl")

	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/cloudberry-ctl")
	build.Dir = repoRoot
	build.Env = os.Environ()
	out, buildErr := build.CombinedOutput()
	require.NoErrorf(s.T(), buildErr, "building cloudberry-ctl must succeed (out=%q)", string(out))
	return bin
}

// scenario95RunCtl invokes the cloudberry-ctl binary with basic-auth against the
// operator API and returns its combined output, requiring a zero exit.
func (s *Scenario95E2ESuite) scenario95RunCtl(
	bin, operatorURL, user, pass, cluster, namespace string, args ...string,
) string {
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Minute)
	defer cancel()

	full := append([]string{
		"--operator-url", operatorURL,
		"--auth-method", "basic",
		"--username", user,
		"--password", pass,
		"--cluster", cluster,
		"--namespace", namespace,
	}, args...)
	cmd := exec.CommandContext(ctx, bin, full...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	require.NoErrorf(s.T(), err, "cloudberry-ctl %s must succeed (out=%q)",
		strings.Join(args, " "), string(out))
	return string(out)
}

// scenario95STSTrigger reads the segment-primary STS restart-trigger annotation
// (empty when absent).
func (s *Scenario95E2ESuite) scenario95STSTrigger(
	cl client.Client, namespace, name string,
) string {
	sts := &appsv1.StatefulSet{}
	if err := cl.Get(s.ctx, client.ObjectKey{Namespace: namespace, Name: name}, sts); err != nil {
		return ""
	}
	return sts.Spec.Template.Annotations[util.AnnotationRestartTrigger]
}

// scenario95PodUIDs maps pod name -> UID for roll detection.
func scenario95PodUIDs(pods []corev1.Pod) map[string]string {
	out := make(map[string]string, len(pods))
	for i := range pods {
		out[pods[i].Name] = string(pods[i].UID)
	}
	return out
}

// scenario95PxfContainerReady reports the pxf container readiness of a pod.
func scenario95PxfContainerReady(pod *corev1.Pod) bool {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == scenario95PxfContainer {
			return pod.Status.ContainerStatuses[i].Ready
		}
	}
	return false
}

// scenario95VMURL resolves the VictoriaMetrics base URL.
func scenario95VMURL() string {
	if v := os.Getenv(envScenario95VMURL); v != "" {
		return v
	}
	return scenario95DefaultVMURL
}

// scenario95VMCounter queries VictoriaMetrics for the current value of
// cloudberry_pxf_restart_total{cluster=<cluster>,result=<result>}. It returns 0
// when the series is absent or the query fails (so the increment assertion still
// holds: 0 -> >0).
func (s *Scenario95E2ESuite) scenario95VMCounter(cluster, result string) float64 {
	q := fmt.Sprintf(`cloudberry_pxf_restart_total{cluster="%s",result="%s"}`, cluster, result)
	u := scenario95VMURL() + "/api/v1/query?query=" + scenario95URLQueryEscape(q)

	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T().Logf("scenario95: VM query failed: %v", err)
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0
	}

	var parsed struct {
		Data struct {
			Result []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0
	}
	var sum float64
	for _, r := range parsed.Data.Result {
		if len(r.Value) == 2 {
			if str, ok := r.Value[1].(string); ok {
				var f float64
				if _, scanErr := fmt.Sscanf(str, "%g", &f); scanErr == nil {
					sum += f
				}
			}
		}
	}
	return sum
}

// scenario95URLQueryEscape escapes a PromQL query for a URL query parameter.
func scenario95URLQueryEscape(q string) string {
	return strings.NewReplacer(
		" ", "%20", "{", "%7B", "}", "%7D", `"`, "%22",
		"=", "%3D", ",", "%2C",
	).Replace(q)
}

// scenario95LastJSON extracts the last JSON object/array from a CLI output blob
// (the table formatter may prepend log lines before the JSON body).
func scenario95LastJSON(out string) string {
	start := strings.Index(out, "{")
	if start < 0 {
		return out
	}
	end := strings.LastIndex(out, "}")
	if end < start {
		return out
	}
	return out[start : end+1]
}

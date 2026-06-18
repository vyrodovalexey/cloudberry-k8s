//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 94: PXF Sidecar Deployment Verification (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 94 cases at the e2e layer and adds
// a KUBECONFIG-gated live check against a deployed cluster:
//
//   - Builder-direct (infra-free, always runs): build the segment-primary PXF
//     sidecar from a full pxf spec and assert the FULL deployment contract —
//     container name "pxf", all seven env vars, port 5888 named "pxf" (TCP),
//     liveness+readiness HTTPGet /actuator/health:5888 with the documented
//     delays/periods, command/args ABSENCE (entrypoint-owned), the three volume
//     mounts, and converted resources. Plus a CatalogHonest cross-check against
//     cases.Scenario94Cases().
//
//   - KUBECONFIG-gated live (TestE2E_Scenario94_LivePXFSidecarOnSegmentPod):
//     against the deployed cluster, find a segment-primary pod that carries a
//     "pxf" container and assert the SAME pod-spec-shape contract on the live
//     pod (env, port, probes, mounts, resources, command-absence). When
//     SCENARIO94_PXF_LIVE=1 (set once the real apache/cloudberry-pxf 2.1.0 image
//     is deployed), it ALSO asserts the pxf container is Ready and that
//     `kubectl exec <segpod> -c pxf -- curl -sf localhost:5888/actuator/health`
//     returns "UP". Skipped cleanly when KUBECONFIG is unset; the Ready/curl
//     assertion is skipped (shape-only) when SCENARIO94_PXF_LIVE is unset.
//
// PROBE-PATH + COMMAND-OWNERSHIP HONESTY: the probe path asserted is
// /actuator/health — the real Spring Boot actuator endpoint on the
// apache/cloudberry-pxf 2.1.0 image (returns {"status":"UP"}), NOT the legacy
// /pxf/v15/Status (a DB-client endpoint that returns 404). The container sets NO
// Command/Args: the "pxf prepare → pxf start → tail service log" lifecycle is
// owned by the image ENTRYPOINT (hack/docker-entrypoint-pxf.sh).
// ============================================================================

// envKubeconfigS93 gates the live segment-pod test.
const envKubeconfigS93 = "KUBECONFIG"

// envScenario94PXFLive gates the STRICT Ready + curl /actuator/health assertion.
// When "1" (set by the deploy agent once the real apache/cloudberry-pxf 2.1.0
// image is deployed), the live test also asserts the pxf container is Ready and
// the actuator health endpoint returns UP. Unset/empty => only the live
// pod-spec-shape assertions run, so CI / clusters without the real image skip
// the runtime assertions cleanly.
const envScenario94PXFLive = "SCENARIO94_PXF_LIVE"

// scenario94LiveNamespace is the namespace used for the live segment-pod test.
const scenario94LiveNamespace = "cloudberry-test"

// scenario94LiveTimeout bounds the live wait loops.
const scenario94LiveTimeout = 2 * time.Minute

// scenario94LivePollInterval is the live poll interval.
const scenario94LivePollInterval = 5 * time.Second

// scenario94ExecTimeout bounds the kubectl exec curl probe.
const scenario94ExecTimeout = 30 * time.Second

// Scenario94E2ESuite verifies the PXF sidecar deployment shape end-to-end.
type Scenario94E2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario94(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario94E2ESuite))
}

func (s *Scenario94E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario94E2EFullDataLoading returns the dataLoading spec exercised by this
// scenario: the full pxf block with image cloudberry-pxf:2.1.0, jvmOpts default
// "-Xmx1g -Xms256m", port 5888, logLevel INFO, requests/limits resources,
// default (nil) extensions (=> PXF_EXTENSION_*=true), and one s3 server. The
// values mirror cases.Scenario94Cases() exactly.
func scenario94E2EFullDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled:  true,
			Image:    "cloudberry-pxf:2.1.0",
			JvmOpts:  "-Xmx1g -Xms256m",
			Port:     5888,
			LogLevel: "INFO",
			Resources: &cbv1alpha1.ResourceRequirements{
				Requests: &cbv1alpha1.ResourceList{CPU: "250m", Memory: "256Mi"},
				Limits:   &cbv1alpha1.ResourceList{CPU: "1", Memory: "1Gi"},
			},
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint": "https://s3.amazonaws.com",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "s3-datalake-creds", Key: "access_key"},
					},
				},
			},
		},
	}
}

// scenario94E2ECluster builds a valid cluster in the given namespace with the
// pxf dataLoading spec attached.
func scenario94E2ECluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).Build()
	cluster.Spec.DataLoading = scenario94E2EFullDataLoading()
	return cluster
}

// scenario94E2EEnvValue returns the named env value of a container.
func scenario94E2EEnvValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// scenario94E2EPxfContainer returns the "pxf" container from a list.
func scenario94E2EPxfContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == "pxf" {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// scenario94E2EAssertContract asserts the FULL sidecar deployment contract on a
// "pxf" container (shared by the builder-direct and live pod-spec checks).
func (s *Scenario94E2ESuite) scenario94E2EAssertContract(c corev1.Container) {
	assert.Equal(s.T(), "pxf", c.Name)

	assertEnv := func(name, want string) {
		got, ok := scenario94E2EEnvValue(c, name)
		require.Truef(s.T(), ok, "env %s present", name)
		assert.Equalf(s.T(), want, got, "env %s value", name)
	}
	assertEnv("PXF_HOME", "/usr/local/cloudberry-pxf")
	assertEnv("PXF_BASE", "/pxf-base")
	assertEnv("PXF_PORT", "5888")
	assertEnv("PXF_LOG_LEVEL", "INFO")

	// Container port: 5888 named "pxf" TCP.
	require.Len(s.T(), c.Ports, 1)
	assert.Equal(s.T(), "pxf", c.Ports[0].Name)
	assert.Equal(s.T(), int32(5888), c.Ports[0].ContainerPort)
	assert.Equal(s.T(), corev1.ProtocolTCP, c.Ports[0].Protocol)

	// Liveness probe: /actuator/health:5888 delay 60 period 20 (NOT
	// /pxf/v15/Status — that 404s on the real apache/cloudberry-pxf 2.1.0 image).
	require.NotNil(s.T(), c.LivenessProbe)
	require.NotNil(s.T(), c.LivenessProbe.HTTPGet)
	assert.Equal(s.T(), "/actuator/health", c.LivenessProbe.HTTPGet.Path)
	assert.Equal(s.T(), int32(5888), c.LivenessProbe.HTTPGet.Port.IntVal)
	assert.Equal(s.T(), int32(60), c.LivenessProbe.InitialDelaySeconds)
	assert.Equal(s.T(), int32(20), c.LivenessProbe.PeriodSeconds)

	// Readiness probe: /actuator/health:5888 delay 30 period 10.
	require.NotNil(s.T(), c.ReadinessProbe)
	require.NotNil(s.T(), c.ReadinessProbe.HTTPGet)
	assert.Equal(s.T(), "/actuator/health", c.ReadinessProbe.HTTPGet.Path)
	assert.Equal(s.T(), int32(5888), c.ReadinessProbe.HTTPGet.Port.IntVal)
	assert.Equal(s.T(), int32(30), c.ReadinessProbe.InitialDelaySeconds)
	assert.Equal(s.T(), int32(10), c.ReadinessProbe.PeriodSeconds)

	// Command/Args ABSENCE — entrypoint-owned lifecycle
	// (hack/docker-entrypoint-pxf.sh).
	assert.Nil(s.T(), c.Command, "sidecar Command must be nil (entrypoint-owned lifecycle)")
	assert.Nil(s.T(), c.Args, "sidecar Args must be nil (entrypoint-owned lifecycle)")

	// Volume mounts at the three paths.
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-base", mounts["pxf-base"])
	assert.Equal(s.T(), "/pxf-base/servers", mounts["pxf-servers"])
	assert.Equal(s.T(), "/pxf/lib/custom", mounts["pxf-lib"])

	// Resources set (requests + limits present).
	require.NotNil(s.T(), c.Resources.Requests)
	require.NotNil(s.T(), c.Resources.Limits)
}

// TestE2E_Scenario94_SidecarBuilt (builder-direct, infra-free) builds the
// segment-primary PXF sidecar from the full spec and asserts the full contract.
func (s *Scenario94E2ESuite) TestE2E_Scenario94_SidecarBuilt() {
	cluster := scenario94E2ECluster("e2e-s93", "default")

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	c, present := scenario94E2EPxfContainer(sts.Spec.Template.Spec.Containers)
	require.True(s.T(), present, "segment-primary pod template carries the 'pxf' sidecar")

	s.scenario94E2EAssertContract(c)

	// Exact resource values + the jvmOpts/extension env (builder-direct only).
	assert.Equal(s.T(), "250m", c.Resources.Requests.Cpu().String())
	assert.Equal(s.T(), "256Mi", c.Resources.Requests.Memory().String())
	assert.Equal(s.T(), "1", c.Resources.Limits.Cpu().String())
	assert.Equal(s.T(), "1Gi", c.Resources.Limits.Memory().String())

	jvm, ok := scenario94E2EEnvValue(c, "PXF_JVM_OPTS")
	require.True(s.T(), ok)
	assert.Equal(s.T(), "-Xmx1g -Xms256m", jvm)
	extPxf, ok := scenario94E2EEnvValue(c, "PXF_EXTENSION_PXF")
	require.True(s.T(), ok)
	assert.Equal(s.T(), "true", extPxf)
	extFdw, ok := scenario94E2EEnvValue(c, "PXF_EXTENSION_PXF_FDW")
	require.True(s.T(), ok)
	assert.Equal(s.T(), "true", extFdw)

	// Coordinator never carries the sidecar.
	coord, err := s.builder.BuildCoordinatorStatefulSet(cluster)
	require.NoError(s.T(), err)
	_, coordHas := scenario94E2EPxfContainer(coord.Spec.Template.Spec.Containers)
	assert.False(s.T(), coordHas, "coordinator StatefulSet must NOT carry the pxf sidecar")
}

// TestE2E_Scenario94_NegativeNoSidecar (builder-direct) asserts pxf-disabled and
// dataLoading-disabled clusters carry no "pxf" container (blast-radius safety).
func (s *Scenario94E2ESuite) TestE2E_Scenario94_NegativeNoSidecar() {
	pxfOff := scenario94E2ECluster("e2e-s94-pxf-off", "default")
	pxfOff.Spec.DataLoading.Pxf.Enabled = false
	stsPxfOff, err := s.builder.BuildSegmentPrimaryStatefulSet(pxfOff)
	require.NoError(s.T(), err)
	_, has := scenario94E2EPxfContainer(stsPxfOff.Spec.Template.Spec.Containers)
	assert.False(s.T(), has, "pxf-disabled segment pod must NOT carry the 'pxf' sidecar")

	dlOff := scenario94E2ECluster("e2e-s94-dl-off", "default")
	dlOff.Spec.DataLoading.Enabled = false
	stsDLOff, err := s.builder.BuildSegmentPrimaryStatefulSet(dlOff)
	require.NoError(s.T(), err)
	_, hasDL := scenario94E2EPxfContainer(stsDLOff.Spec.Template.Spec.Containers)
	assert.False(s.T(), hasDL, "dataLoading-disabled segment pod must NOT carry the 'pxf' sidecar")
}

// TestE2E_Scenario94_CatalogHonest (builder-direct) cross-checks the catalog
// against the live built sidecar container.
func (s *Scenario94E2ESuite) TestE2E_Scenario94_CatalogHonest() {
	catalog := cases.Scenario94Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario94E2ECluster("e2e-s94-cat", "default")
	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1)
	live := scenario94E2ELiveValues(containers[0])

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.FieldPath, func() {
			got, ok := live[tc.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", tc.FieldPath)
			assert.Equalf(s.T(), tc.ExpectedValue, got,
				"%s (%s) catalog value must match the live built container",
				tc.ID, tc.FieldPath)
		})
	}
}

// scenario94E2ELiveValues resolves each Scenario94 catalog FieldPath against the
// LIVE built sidecar container.
func scenario94E2ELiveValues(c corev1.Container) map[string]string {
	env := func(name string) string {
		v, _ := scenario94E2EEnvValue(c, name)
		return v
	}
	out := map[string]string{
		"sidecar.name":                      c.Name,
		"sidecar.env.PXF_HOME":              env("PXF_HOME"),
		"sidecar.env.PXF_BASE":              env("PXF_BASE"),
		"sidecar.env.PXF_JVM_OPTS":          env("PXF_JVM_OPTS"),
		"sidecar.env.PXF_PORT":              env("PXF_PORT"),
		"sidecar.env.PXF_LOG_LEVEL":         env("PXF_LOG_LEVEL"),
		"sidecar.env.PXF_EXTENSION_PXF":     env("PXF_EXTENSION_PXF"),
		"sidecar.env.PXF_EXTENSION_PXF_FDW": env("PXF_EXTENSION_PXF_FDW"),
		"sidecar.command":                   scenario94E2ENilOrJoin(c.Command),
		"sidecar.args":                      scenario94E2ENilOrJoin(c.Args),
		"sidecar.resources.requests.cpu":    c.Resources.Requests.Cpu().String(),
		"sidecar.resources.requests.memory": c.Resources.Requests.Memory().String(),
		"sidecar.resources.limits.cpu":      c.Resources.Limits.Cpu().String(),
		"sidecar.resources.limits.memory":   c.Resources.Limits.Memory().String(),
	}
	if len(c.Ports) > 0 {
		out["sidecar.port.name"] = c.Ports[0].Name
		out["sidecar.port.containerPort"] = strconv.Itoa(int(c.Ports[0].ContainerPort))
		out["sidecar.port.protocol"] = string(c.Ports[0].Protocol)
	}
	if c.LivenessProbe != nil && c.LivenessProbe.HTTPGet != nil {
		out["sidecar.liveness.path"] = c.LivenessProbe.HTTPGet.Path
		out["sidecar.liveness.port"] = strconv.Itoa(int(c.LivenessProbe.HTTPGet.Port.IntVal))
		out["sidecar.liveness.initialDelaySeconds"] = strconv.Itoa(int(c.LivenessProbe.InitialDelaySeconds))
		out["sidecar.liveness.periodSeconds"] = strconv.Itoa(int(c.LivenessProbe.PeriodSeconds))
	}
	if c.ReadinessProbe != nil && c.ReadinessProbe.HTTPGet != nil {
		out["sidecar.readiness.path"] = c.ReadinessProbe.HTTPGet.Path
		out["sidecar.readiness.port"] = strconv.Itoa(int(c.ReadinessProbe.HTTPGet.Port.IntVal))
		out["sidecar.readiness.initialDelaySeconds"] = strconv.Itoa(int(c.ReadinessProbe.InitialDelaySeconds))
		out["sidecar.readiness.periodSeconds"] = strconv.Itoa(int(c.ReadinessProbe.PeriodSeconds))
	}
	for _, m := range c.VolumeMounts {
		out["sidecar.volumeMount."+m.Name] = m.MountPath
	}
	return out
}

// scenario94E2ENilOrJoin renders a nil/empty slice as the "<nil>" sentinel and a
// non-empty slice as its space-joined elements (matching the catalog).
func scenario94E2ENilOrJoin(s []string) string {
	if len(s) == 0 {
		return "<nil>"
	}
	return strings.Join(s, " ")
}

// TestE2E_Scenario94_LivePXFSidecarOnSegmentPod is the KUBECONFIG-gated live
// test. It finds a segment-primary pod carrying a "pxf" container and asserts the
// SAME deployment contract on the live pod spec (env, port, probes, mounts,
// resources, command-absence). When SCENARIO94_PXF_LIVE=1 it ALSO asserts the
// pxf container is Ready and that the actuator health endpoint returns UP.
// Skipped cleanly when KUBECONFIG is unset, or when no segment pod with a "pxf"
// container exists yet (the operator/real image may not be deployed).
func (s *Scenario94E2ESuite) TestE2E_Scenario94_LivePXFSidecarOnSegmentPod() {
	kubeconfig := os.Getenv(envKubeconfigS93)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live PXF sidecar segment-pod test")
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

	// Find a segment-primary pod that carries a "pxf" container.
	pod, c, found := s.scenario94FindSegmentPxfPod(cl)
	if !found {
		s.T().Skip("no segment-primary pod with a 'pxf' container found " +
			"(operator / real apache/cloudberry-pxf image may not be deployed)")
	}
	s.T().Logf("scenario94: found segment pod %s/%s with a 'pxf' container",
		pod.Namespace, pod.Name)

	// Pod-spec-shape assertions run whenever such a pod exists.
	s.scenario94E2EAssertContract(c)

	// The strict Ready + actuator-curl assertion is gated behind
	// SCENARIO94_PXF_LIVE=1 (the real image is deployed). Without it, we have
	// proven the live pod spec carries the correct sidecar shape and stop here.
	if os.Getenv(envScenario94PXFLive) != "1" {
		s.T().Logf("scenario94: live pxf sidecar pod-spec shape verified on %s; "+
			"SCENARIO94_PXF_LIVE not set, skipping the Ready + /actuator/health curl assertion",
			pod.Name)
		return
	}

	// Wait for the pxf container to become Ready, then curl the actuator health
	// endpoint from inside the container and assert it reports UP.
	require.Eventuallyf(s.T(), func() bool {
		fresh := &corev1.Pod{}
		if getErr := cl.Get(s.ctx, client.ObjectKey{
			Namespace: pod.Namespace, Name: pod.Name,
		}, fresh); getErr != nil {
			return false
		}
		for _, cs := range fresh.Status.ContainerStatuses {
			if cs.Name == "pxf" {
				return cs.Ready
			}
		}
		return false
	}, scenario94LiveTimeout, scenario94LivePollInterval,
		"pxf container on %s must become Ready", pod.Name)

	out, execErr := s.scenario94PxfExec(pod.Namespace, pod.Name,
		"curl -sf localhost:5888/actuator/health")
	require.NoErrorf(s.T(), execErr,
		"curl localhost:5888/actuator/health in pxf container must succeed (out=%q)", out)
	assert.Containsf(s.T(), strings.ToUpper(out), "UP",
		"actuator health must report status UP (got %q)", out)
}

// scenario94FindSegmentPxfPod lists pods in the live namespace and returns the
// first segment-primary pod that carries a "pxf" container, the container, and
// whether one was found.
func (s *Scenario94E2ESuite) scenario94FindSegmentPxfPod(
	cl client.Client,
) (corev1.Pod, corev1.Container, bool) {
	pods := &corev1.PodList{}
	if err := cl.List(s.ctx, pods,
		client.InNamespace(scenario94LiveNamespace),
		client.MatchingLabels{util.LabelComponent: util.ComponentSegmentPrimary},
	); err != nil {
		s.T().Logf("scenario94: could not list segment-primary pods: %v", err)
		return corev1.Pod{}, corev1.Container{}, false
	}
	for i := range pods.Items {
		pod := pods.Items[i]
		if c, ok := scenario94E2EPxfContainer(pod.Spec.Containers); ok {
			return pod, c, true
		}
	}
	return corev1.Pod{}, corev1.Container{}, false
}

// scenario94PxfExec runs a bash command inside the pxf container of the named
// pod via kubectl exec, bounded by scenario94ExecTimeout. The explicit
// -c pxf targets the sidecar (avoiding the "Defaulted container" noise).
func (s *Scenario94E2ESuite) scenario94PxfExec(
	namespace, pod, bashCmd string,
) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario94ExecTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", namespace,
		"-c", "pxf", pod, "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

//go:build functional

package functional

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 94: PXF Sidecar Deployment Verification (functional)
// ============================================================================
//
// The PXF sidecar builder (internal/builder/pxf_builder.go,
// BuildPXFSidecarContainers) is implemented and live-verified. This functional
// suite drives the BUILDER over a full pxf dataLoading spec and asserts the
// EXACT shape of the injected "pxf" sidecar container on the segment-primary pod
// template — the deployment CONTRACT the operator must satisfy:
//
//   - SidecarContract: the segment-primary pod template carries a container
//     named "pxf" with all seven env vars at their exact values (PXF_HOME,
//     PXF_BASE, PXF_JVM_OPTS == pxf.jvmOpts, PXF_PORT == "5888", PXF_LOG_LEVEL
//     == pxf.logLevel, PXF_EXTENSION_PXF, PXF_EXTENSION_PXF_FDW), a container
//     port 5888 named "pxf" (TCP), liveness+readiness HTTPGet probes on
//     /actuator/health:5888 with the documented delays/periods, the three
//     volume mounts, and the converted resources requests/limits.
//   - CommandAbsence: the sidecar sets NO Command and NO Args — the
//     "pxf prepare → pxf start → tail service log" lifecycle is owned by the
//     IMAGE ENTRYPOINT (hack/docker-entrypoint-pxf.sh).
//   - ProbePathHonest: the liveness/readiness probe path is /actuator/health
//     (the real Spring Boot actuator endpoint on apache/cloudberry-pxf 2.1.0,
//     returns {"status":"UP"}), NOT the legacy /pxf/v15/Status (a DB-client
//     endpoint that returns 404 on the real image).
//   - LogLevelPropagation: re-patching pxf.logLevel (e.g. WARN) and rebuilding
//     re-derives PXF_LOG_LEVEL.
//   - NegativeNoSidecar (blast-radius safety): pxf-disabled and
//     dataLoading-disabled clusters carry NO "pxf" container in the segment pod.
//   - CatalogHonest: every cases.Scenario94Cases() expectation matches the LIVE
//     built sidecar container.
// ============================================================================

// Scenario94Suite verifies the PXF sidecar deployment shape.
type Scenario94Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario94(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario94Suite))
}

func (s *Scenario94Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario94FullDataLoading returns the dataLoading spec exercised by this
// scenario: the full pxf block with image cloudberry-pxf:2.1.0, jvmOpts at the
// default "-Xmx1g -Xms256m", port 5888, logLevel INFO, requests/limits
// resources, default (nil) extensions (=> PXF_EXTENSION_*=true), and one s3
// server. The values mirror cases.Scenario94Cases() exactly so CatalogHonest
// stays honest.
func scenario94FullDataLoading() *cbv1alpha1.DataLoadingSpec {
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
			// Extensions intentionally nil => PXF_EXTENSION_PXF / _FDW default to
			// "true" (pxfExtensionFlag(nil) == "true"), matching the catalog.
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

// scenario94Cluster builds a valid cluster with the pxf dataLoading spec
// attached, applying the supplied mutator (if any) before returning.
func scenario94Cluster(
	name string,
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	dl := scenario94FullDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario94EnvValue returns the value of the named env var in a container.
func scenario94EnvValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// scenario94PxfContainer returns the "pxf" sidecar container from a list.
func scenario94PxfContainer(containers []corev1.Container) (corev1.Container, bool) {
	for _, c := range containers {
		if c.Name == "pxf" {
			return c, true
		}
	}
	return corev1.Container{}, false
}

// TestFunctional_Scenario94_SidecarContract builds the segment-primary
// StatefulSet from the full spec and asserts the FULL sidecar deployment
// contract: name, env, port, probes, command-absence, mounts and resources.
func (s *Scenario94Suite) TestFunctional_Scenario94_SidecarContract() {
	cluster := scenario94Cluster("s94-contract", nil)

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	c, present := scenario94PxfContainer(sts.Spec.Template.Spec.Containers)
	require.True(s.T(), present, "segment-primary pod template carries the 'pxf' sidecar")
	assert.Equal(s.T(), "pxf", c.Name)
	assert.Equal(s.T(), "cloudberry-pxf:2.1.0", c.Image)

	// Env vars (exact values).
	assertEnv := func(name, want string) {
		got, ok := scenario94EnvValue(c, name)
		require.Truef(s.T(), ok, "env %s present", name)
		assert.Equalf(s.T(), want, got, "env %s value", name)
	}
	assertEnv("PXF_HOME", "/usr/local/cloudberry-pxf")
	assertEnv("PXF_BASE", "/pxf-base")
	assertEnv("PXF_JVM_OPTS", "-Xmx1g -Xms256m")
	assertEnv("PXF_PORT", "5888")
	assertEnv("PXF_LOG_LEVEL", "INFO")
	assertEnv("PXF_EXTENSION_PXF", "true")
	assertEnv("PXF_EXTENSION_PXF_FDW", "true")

	// Container port: 5888 named "pxf" TCP.
	require.Len(s.T(), c.Ports, 1)
	assert.Equal(s.T(), "pxf", c.Ports[0].Name)
	assert.Equal(s.T(), int32(5888), c.Ports[0].ContainerPort)
	assert.Equal(s.T(), corev1.ProtocolTCP, c.Ports[0].Protocol)

	// Liveness probe: /actuator/health:5888 delay 60 period 20.
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

	// Command/Args ABSENCE: the "pxf prepare → pxf start → tail service log"
	// lifecycle is owned by the IMAGE ENTRYPOINT (hack/docker-entrypoint-pxf.sh),
	// so the operator injects NO Command and NO Args. (This is also why the probe
	// path above is /actuator/health, the Spring Boot actuator on the real
	// apache/cloudberry-pxf 2.1.0 image — NOT the legacy /pxf/v15/Status, which is
	// a DB-client endpoint that returns 404.)
	assert.Nil(s.T(), c.Command, "sidecar Command must be nil (entrypoint-owned lifecycle)")
	assert.Nil(s.T(), c.Args, "sidecar Args must be nil (entrypoint-owned lifecycle)")

	// Volume mounts.
	mounts := map[string]string{}
	for _, m := range c.VolumeMounts {
		mounts[m.Name] = m.MountPath
	}
	assert.Equal(s.T(), "/pxf-base", mounts["pxf-base"])
	assert.Equal(s.T(), "/pxf-base/servers", mounts["pxf-servers"])
	assert.Equal(s.T(), "/pxf/lib/custom", mounts["pxf-lib"])

	// Resources (requests + limits) converted onto the container.
	require.NotNil(s.T(), c.Resources.Requests)
	require.NotNil(s.T(), c.Resources.Limits)
	assert.Equal(s.T(), "250m", c.Resources.Requests.Cpu().String())
	assert.Equal(s.T(), "256Mi", c.Resources.Requests.Memory().String())
	assert.Equal(s.T(), "1", c.Resources.Limits.Cpu().String())
	assert.Equal(s.T(), "1Gi", c.Resources.Limits.Memory().String())
}

// TestFunctional_Scenario94_LogLevelPropagation re-patches pxf.logLevel to WARN,
// rebuilds the sidecar, and asserts PXF_LOG_LEVEL == WARN (rebuild-from-spec
// propagation).
func (s *Scenario94Suite) TestFunctional_Scenario94_LogLevelPropagation() {
	cluster := scenario94Cluster("s94-loglevel", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.LogLevel = "WARN"
	})
	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1)
	got, ok := scenario94EnvValue(containers[0], "PXF_LOG_LEVEL")
	require.True(s.T(), ok, "PXF_LOG_LEVEL present")
	assert.Equal(s.T(), "WARN", got,
		"pxf.logLevel=WARN must propagate to PXF_LOG_LEVEL on rebuild")
}

// TestFunctional_Scenario94_NegativeNoSidecar is the blast-radius safety check:
// a pxf-disabled cluster and a dataLoading-disabled cluster must carry NO "pxf"
// container in the segment-primary pod template.
func (s *Scenario94Suite) TestFunctional_Scenario94_NegativeNoSidecar() {
	pxfDisabled := scenario94Cluster("s94-pxf-off", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Enabled = false
	})
	stsPxfOff, err := s.builder.BuildSegmentPrimaryStatefulSet(pxfDisabled)
	require.NoError(s.T(), err)
	_, has := scenario94PxfContainer(stsPxfOff.Spec.Template.Spec.Containers)
	assert.False(s.T(), has, "pxf-disabled segment pod must NOT carry the 'pxf' sidecar")

	dlDisabled := scenario94Cluster("s94-dl-off", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false
	})
	stsDLOff, err := s.builder.BuildSegmentPrimaryStatefulSet(dlDisabled)
	require.NoError(s.T(), err)
	_, hasDL := scenario94PxfContainer(stsDLOff.Spec.Template.Spec.Containers)
	assert.False(s.T(), hasDL, "dataLoading-disabled segment pod must NOT carry the 'pxf' sidecar")

	// Coordinator never carries the sidecar either.
	enabled := scenario94Cluster("s94-coord", nil)
	coord, err := s.builder.BuildCoordinatorStatefulSet(enabled)
	require.NoError(s.T(), err)
	_, coordHas := scenario94PxfContainer(coord.Spec.Template.Spec.Containers)
	assert.False(s.T(), coordHas, "coordinator pod must NOT carry the 'pxf' sidecar")
}

// TestFunctional_Scenario94_CatalogHonest iterates cases.Scenario94Cases() and
// asserts each expectation matches the LIVE built sidecar container.
func (s *Scenario94Suite) TestFunctional_Scenario94_CatalogHonest() {
	catalog := cases.Scenario94Cases()
	require.NotEmpty(s.T(), catalog, "Scenario 94 catalog must be non-empty")

	cluster := scenario94Cluster("s94-catalog", nil)
	containers := s.builder.BuildPXFSidecarContainers(cluster)
	require.Len(s.T(), containers, 1)
	live := scenario94LiveValues(containers[0])

	seen := make(map[string]bool, len(catalog))
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.FieldPath, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true
			got, ok := live[tc.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", tc.FieldPath)
			assert.Equalf(s.T(), tc.ExpectedValue, got,
				"%s (%s) catalog value must match the live built container",
				tc.ID, tc.FieldPath)
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario94LiveValues resolves each Scenario94 catalog FieldPath against the
// LIVE built sidecar container. Command/Args absence resolves to the "<nil>"
// sentinel the catalog uses.
func scenario94LiveValues(c corev1.Container) map[string]string {
	env := func(name string) string {
		v, _ := scenario94EnvValue(c, name)
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
		"sidecar.command":                   nilOrJoin(c.Command),
		"sidecar.args":                      nilOrJoin(c.Args),
		"sidecar.resources.requests.cpu":    c.Resources.Requests.Cpu().String(),
		"sidecar.resources.requests.memory": c.Resources.Requests.Memory().String(),
		"sidecar.resources.limits.cpu":      c.Resources.Limits.Cpu().String(),
		"sidecar.resources.limits.memory":   c.Resources.Limits.Memory().String(),
	}
	if len(c.Ports) > 0 {
		out["sidecar.port.name"] = c.Ports[0].Name
		out["sidecar.port.containerPort"] = intToStr(int(c.Ports[0].ContainerPort))
		out["sidecar.port.protocol"] = string(c.Ports[0].Protocol)
	}
	if c.LivenessProbe != nil && c.LivenessProbe.HTTPGet != nil {
		out["sidecar.liveness.path"] = c.LivenessProbe.HTTPGet.Path
		out["sidecar.liveness.port"] = intToStr(int(c.LivenessProbe.HTTPGet.Port.IntVal))
		out["sidecar.liveness.initialDelaySeconds"] = intToStr(int(c.LivenessProbe.InitialDelaySeconds))
		out["sidecar.liveness.periodSeconds"] = intToStr(int(c.LivenessProbe.PeriodSeconds))
	}
	if c.ReadinessProbe != nil && c.ReadinessProbe.HTTPGet != nil {
		out["sidecar.readiness.path"] = c.ReadinessProbe.HTTPGet.Path
		out["sidecar.readiness.port"] = intToStr(int(c.ReadinessProbe.HTTPGet.Port.IntVal))
		out["sidecar.readiness.initialDelaySeconds"] = intToStr(int(c.ReadinessProbe.InitialDelaySeconds))
		out["sidecar.readiness.periodSeconds"] = intToStr(int(c.ReadinessProbe.PeriodSeconds))
	}
	for _, m := range c.VolumeMounts {
		out["sidecar.volumeMount."+m.Name] = m.MountPath
	}
	return out
}

// nilOrJoin renders a nil/empty string slice as the "<nil>" sentinel the
// Scenario 94 catalog uses for the command/args-absence rows, and a non-empty
// slice as its space-joined elements.
func nilOrJoin(s []string) string {
	if len(s) == 0 {
		return "<nil>"
	}
	out := s[0]
	for _, e := range s[1:] {
		out += " " + e
	}
	return out
}

// intToStr renders an int as its base-10 string.
func intToStr(i int) string {
	return strconv.Itoa(i)
}

//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 104: Pre-Load Health Checks (HC.1-HC.5) — functional
// ============================================================================
//
// This suite mirrors the Scenario 101 functional SHAPE, driving the BUILDER
// (BuildDataLoadJob → the dataload-healthcheck init container + the 5-check
// script + the shared scratch emptyDir) WITHOUT a live cluster, asserting the
// shipped production contract:
//
//   - INIT (104-INIT-B): a pxf job's dataload Job pod carries the
//     dataload-healthcheck init container FIRST, named, image=dataLoaderImage,
//     running /bin/bash -c the 5-check script.
//
//   - The 5-check script substrings + per-job-type gating: HC.1 DB-proxy
//     (pg_extension / pxf_version()), HC.2 to_regclass, HC.3 curl
//     AWS_S3_ENDPOINT, HC.5 df — on the pxf job's init; HC.4 (gpfdist-svc) is
//     gpload-only. A pxf job has HC.1+HC.3, NO HC.4.
//
//   - HC.5 scratch (104-HC5-B-VOL): the dataload-scratch emptyDir mounted at
//     /dataload-scratch on BOTH the init AND the main dataload container.
//
//   - KNOB (104-KNOB-B): healthChecks.enabled=false → no init container, no
//     scratch volume, main container byte-identical (no scratch mount).
//
//   - CatalogHonest: resolve each cases.Scenario104Cases() builder row against
//     the REAL built artifact (live/reconcile rows are logged + skipped).
//
// HC.1 HONESTY: HC.1 is a DB-PROXY probe (the load pod cannot reach the
// segment's localhost-only sidecar). The builder proves the SCRIPT substrings;
// the live "stop PXF → job fails" is the behavioral proof (e2e Part B).
//
// PRODUCTION REALITY (honest): BOTH data-load paths wire the init container when
// health checks are enabled. The pxf data-load path (BuildDataLoadJob → the
// native DDL pod spec) carries the init with HC.1/HC.2/HC.3/HC.5 (NOT HC.4); the
// gpload path (BuildGploadJob → its own pod spec) now ALSO prepends the
// dataload-healthcheck init container + the dataload-scratch volume + a
// /dataload-scratch mount, so the gpload Job's init carries HC.2/HC.4/HC.5 (the
// HC.4 gpfdist-svc reachability probe is the gpload-specific check), NOT HC.1 or
// HC.3. The functional suite asserts BOTH directions DIRECTLY: the pxf job's init
// never carries the HC.4 gpfdist-svc line (HC.4 is gpload-only) AND the gpload
// job's init DOES carry the HC.4 gpfdist-svc probe (the gating direction now
// provable against the real artifact).
// ============================================================================

// Scenario104Suite exercises the pre-load health-check builder contract at the
// builder layer.
type Scenario104Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario104(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario104Suite))
}

func (s *Scenario104Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario104PxfJob returns the s3 pxf load job (HC.1/HC.2/HC.3/HC.5) per the
// sample CR §5: s3-datalake server, s3:text profile, the MinIO CSV resource,
// target public.events.
func scenario104PxfJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario104PxfJobName,
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario104Server,
			Profile:     cases.Scenario104Profile,
			Resource:    cases.Scenario104Resource,
			TargetTable: cases.Scenario104TargetTable,
		},
	}
}

// scenario104GploadJob returns the gpload load job (HC.4) per the sample CR §5.
func scenario104GploadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario104GploadJobName,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
			FilePaths:   []string{"/incoming/*.csv"},
			Format:      "csv",
			TargetTable: cases.Scenario104TargetTable,
		},
	}
}

// scenario104Cluster builds a running cluster whose data-loading spec carries
// pxf.enabled + gpfdist.enabled (so the HC.1/HC.4 gates are open), an s3 backup
// destination (so the HC.3 S3 creds env is wired) and the supplied jobs. The
// healthChecks block is left nil (default ON).
func scenario104Cluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, cases.Scenario104Namespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: true},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs:    jobs,
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-data",
				Endpoint: "http://minio:9000",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// scenario104InitContainer returns the dataload-healthcheck init container of a
// built dataload Job (or nil when absent). It is the FIRST init container.
func scenario104InitContainer(job *batchv1.Job) *corev1.Container {
	inits := job.Spec.Template.Spec.InitContainers
	if len(inits) == 0 {
		return nil
	}
	return &inits[0]
}

// scenario104MainContainer returns the main dataload/gpload container of a built
// dataload Job (the single workload container).
func scenario104MainContainer(job *batchv1.Job) *corev1.Container {
	c := job.Spec.Template.Spec.Containers
	if len(c) == 0 {
		return nil
	}
	return &c[0]
}

// scenario104MountPath reports whether the container mounts the given path.
func scenario104MountPath(c *corev1.Container, path string) bool {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].MountPath == path {
			return true
		}
	}
	return false
}

// scenario104FindVolume returns the named volume (or nil) from a pod spec.
func scenario104FindVolume(vols []corev1.Volume, name string) *corev1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// 104-INIT-B — init container shape + the 5-check script substrings
// ----------------------------------------------------------------------------

// TestFunctional_Scenario104_InitContainerShape (104-INIT-B) asserts a pxf job's
// dataload Job pod carries the dataload-healthcheck init container FIRST, named,
// image=dataLoaderImage, /bin/bash -c the 5-check script.
func (s *Scenario104Suite) TestFunctional_Scenario104_InitContainerShape() {
	cluster := scenario104Cluster("s104-init", scenario104PxfJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)

	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init, "pxf dataload Job must carry the health-check init container (default on)")
	assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
	assert.Equal(s.T(), []string{"/bin/bash", "-c"}, init.Command)
	require.Len(s.T(), init.Args, 1)
	assert.Contains(s.T(), init.Args[0], "dataload-healthcheck: starting pre-load health checks")
	assert.Contains(s.T(), init.Args[0], "set -euo pipefail")
	assert.Contains(s.T(), init.Args[0], "all checks passed")
}

// TestFunctional_Scenario104_PxfScriptChecks asserts a pxf s3 job's init script
// carries HC.1 (DB-proxy), HC.2 (to_regclass), HC.3 (curl AWS_S3_ENDPOINT) and
// HC.5 (df) but NOT HC.4 (gpfdist-svc) — the per-job-type gating direction the
// pxf path proves honestly.
func (s *Scenario104Suite) TestFunctional_Scenario104_PxfScriptChecks() {
	cluster := scenario104Cluster("s104-pxf", scenario104PxfJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)
	script := init.Args[0]

	// HC.1 DB-proxy PXF-readiness (NOT a direct sidecar curl).
	assert.Contains(s.T(), script, "pg_extension")
	assert.Contains(s.T(), script, "extname='pxf'")
	assert.Contains(s.T(), script, "pxf_version()")
	assert.Contains(s.T(), script, "HC.1 FAIL")
	assert.NotContains(s.T(), script, "actuator/health",
		"HC.1 is a DB proxy, NOT a direct sidecar curl")

	// HC.2 target-table-exists.
	assert.Contains(s.T(), script, "to_regclass('${tbl}')")
	assert.Contains(s.T(), script, "tbl='"+cases.Scenario104TargetTable+"'")
	assert.Contains(s.T(), script, "HC.2 FAIL")

	// HC.3 external connectivity (s3).
	assert.Contains(s.T(), script, `curl -fsS -m 10 --head "${AWS_S3_ENDPOINT}"`)
	assert.Contains(s.T(), script, "HC.3 FAIL")

	// HC.5 disk-space.
	assert.Contains(s.T(), script, "df -Pk "+cases.Scenario104ScratchMount)
	assert.Contains(s.T(), script, "HC.5 FAIL")

	// HC.4 gpfdist-svc is gpload-ONLY: a pxf job's script must NOT carry it.
	assert.NotContains(s.T(), script, "gpfdist-svc",
		"HC.4 is gpload-only; a pxf job must not curl the gpfdist Service")
	assert.NotContains(s.T(), script, "HC.4 FAIL")
}

// TestFunctional_Scenario104_HC3Env asserts the pxf init container carries the
// HC.3 S3 creds env via SecretKeyRef (NO plaintext) + AWS_S3_ENDPOINT.
func (s *Scenario104Suite) TestFunctional_Scenario104_HC3Env() {
	cluster := scenario104Cluster("s104-hc3", scenario104PxfJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)

	env := map[string]corev1.EnvVar{}
	for _, e := range init.Env {
		env[e.Name] = e
	}
	require.Contains(s.T(), env, "AWS_ACCESS_KEY_ID")
	assert.Empty(s.T(), env["AWS_ACCESS_KEY_ID"].Value, "creds must never be plaintext")
	require.NotNil(s.T(), env["AWS_ACCESS_KEY_ID"].ValueFrom)
	require.NotNil(s.T(), env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef)
	assert.Equal(s.T(), "backup-s3-credentials",
		env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef.Name)
	require.Contains(s.T(), env, "AWS_S3_ENDPOINT")
	assert.Equal(s.T(), "http://minio:9000", env["AWS_S3_ENDPOINT"].Value)
}

// TestFunctional_Scenario104_ScratchVolumeAndMounts (104-HC5-B-VOL) asserts the
// dataload-scratch emptyDir is in the pod's Volumes and mounted at
// /dataload-scratch on BOTH the init AND the main dataload container.
func (s *Scenario104Suite) TestFunctional_Scenario104_ScratchVolumeAndMounts() {
	cluster := scenario104Cluster("s104-vol", scenario104PxfJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	podSpec := out.Spec.Template.Spec

	vol := scenario104FindVolume(podSpec.Volumes, cases.Scenario104ScratchVolume)
	require.NotNil(s.T(), vol, "scratch volume must be present")
	require.NotNil(s.T(), vol.EmptyDir, "scratch volume must be an emptyDir")

	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)
	assert.True(s.T(), scenario104MountPath(init, cases.Scenario104ScratchMount),
		"init container must mount /dataload-scratch")

	main := scenario104MainContainer(out)
	require.NotNil(s.T(), main)
	assert.True(s.T(), scenario104MountPath(main, cases.Scenario104ScratchMount),
		"main dataload container must mount /dataload-scratch")
}

// ----------------------------------------------------------------------------
// Per-job-type gating: the gpload Job (HC.4 path)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario104_GploadJobGating asserts the per-job-type routing
// DIRECTLY: the gpload data-load path (BuildGploadJob) now PREPENDS the
// dataload-healthcheck init container, whose script carries HC.2 (to_regclass),
// HC.4 (gpfdist-svc reachability — the gpload-specific check) and HC.5 (df) but
// NOT HC.1 (pxf-only) or HC.3 (object-store-only). Conversely the pxf job (which
// also carries the init) never carries the HC.4 gpfdist-svc probe (HC.4 is
// gpload-only). Both gating directions are now provable against real artifacts.
func (s *Scenario104Suite) TestFunctional_Scenario104_GploadJobGating() {
	cluster := scenario104Cluster("s104-gpload", scenario104GploadJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104GploadJob())
	require.NotNil(s.T(), out)

	gpInit := scenario104InitContainer(out)
	require.NotNil(s.T(), gpInit,
		"the gpload data-load path now carries the health-check init container (default on)")
	assert.Equal(s.T(), cases.Scenario104InitName, gpInit.Name)
	require.Len(s.T(), gpInit.Args, 1)
	gpScript := gpInit.Args[0]

	// HC.4 gpfdist-svc reachability — the gpload-specific check.
	assert.Contains(s.T(), gpScript, "gpfdist-svc",
		"a gpload job's init must curl the gpfdist Service (HC.4 is gpload-only)")
	assert.Contains(s.T(), gpScript, "HC.4 FAIL")
	// HC.2 target-table + HC.5 disk run on the gpload init too.
	assert.Contains(s.T(), gpScript, "to_regclass('${tbl}')")
	assert.Contains(s.T(), gpScript, "HC.2 FAIL")
	assert.Contains(s.T(), gpScript, "df -Pk "+cases.Scenario104ScratchMount)
	assert.Contains(s.T(), gpScript, "HC.5 FAIL")
	// HC.1 (pxf-only) + HC.3 (object-store-only) are gated OFF for a gpload job.
	assert.NotContains(s.T(), gpScript, "HC.1 FAIL")
	assert.NotContains(s.T(), gpScript, "pxf_version()")
	assert.NotContains(s.T(), gpScript, "HC.3 FAIL")
	assert.NotContains(s.T(), gpScript, `--head "${AWS_S3_ENDPOINT}"`)

	// The pxf job (which DOES carry the init) must NOT carry HC.4 — proving the
	// HC.4 gating is gpload-only and HC.1/HC.3 are pxf-only.
	pxfOut := s.builder.BuildDataLoadJob(
		scenario104Cluster("s104-gpload-pxf", scenario104PxfJob()), scenario104PxfJob())
	require.NotNil(s.T(), pxfOut)
	pxfInit := scenario104InitContainer(pxfOut)
	require.NotNil(s.T(), pxfInit)
	assert.NotContains(s.T(), pxfInit.Args[0], "gpfdist-svc",
		"a pxf job's init must not curl the gpfdist Service (HC.4 is gpload-only)")
}

// ----------------------------------------------------------------------------
// 104-KNOB-B — enabled:false opt-out + nil block default-on
// ----------------------------------------------------------------------------

// TestFunctional_Scenario104_KnobDisabled (104-KNOB-B) asserts
// healthChecks.enabled=false removes the init container, the scratch volume AND
// the main-container scratch mount (byte-identical to a pre-Scenario-104 pod).
func (s *Scenario104Suite) TestFunctional_Scenario104_KnobDisabled() {
	cluster := scenario104Cluster("s104-off", scenario104PxfJob())
	disabled := false
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &disabled}

	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	podSpec := out.Spec.Template.Spec

	assert.Empty(s.T(), podSpec.InitContainers, "no init container when disabled")
	assert.Nil(s.T(), scenario104FindVolume(podSpec.Volumes, cases.Scenario104ScratchVolume),
		"no scratch volume when disabled")
	main := scenario104MainContainer(out)
	require.NotNil(s.T(), main)
	assert.False(s.T(), scenario104MountPath(main, cases.Scenario104ScratchMount),
		"main container must not mount scratch when disabled")
}

// TestFunctional_Scenario104_GploadKnobDisabled asserts a gpload job with
// healthChecks.enabled=false removes the init container AND the dataload-scratch
// volume from the gpload Job (byte-identical to the pre-Scenario-104 gpload pod).
func (s *Scenario104Suite) TestFunctional_Scenario104_GploadKnobDisabled() {
	cluster := scenario104Cluster("s104-gpload-off", scenario104GploadJob())
	disabled := false
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &disabled}

	out := s.builder.BuildDataLoadJob(cluster, scenario104GploadJob())
	require.NotNil(s.T(), out)
	podSpec := out.Spec.Template.Spec

	assert.Empty(s.T(), podSpec.InitContainers, "no init container on the gpload Job when disabled")
	assert.Nil(s.T(), scenario104FindVolume(podSpec.Volumes, cases.Scenario104ScratchVolume),
		"no scratch volume on the gpload Job when disabled")
	main := scenario104MainContainer(out)
	require.NotNil(s.T(), main)
	assert.False(s.T(), scenario104MountPath(main, cases.Scenario104ScratchMount),
		"gpload container must not mount scratch when disabled")
}

// TestFunctional_Scenario104_GploadScratchVolumeAndMounts asserts a gpload job
// (health checks on) carries the dataload-scratch emptyDir mounted at
// /dataload-scratch on BOTH the init container AND the gpload container (HC.5),
// mirroring the pxf path.
func (s *Scenario104Suite) TestFunctional_Scenario104_GploadScratchVolumeAndMounts() {
	cluster := scenario104Cluster("s104-gpload-vol", scenario104GploadJob())
	out := s.builder.BuildDataLoadJob(cluster, scenario104GploadJob())
	require.NotNil(s.T(), out)
	podSpec := out.Spec.Template.Spec

	vol := scenario104FindVolume(podSpec.Volumes, cases.Scenario104ScratchVolume)
	require.NotNil(s.T(), vol, "gpload scratch volume must be present")
	require.NotNil(s.T(), vol.EmptyDir, "gpload scratch volume must be an emptyDir")

	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)
	assert.True(s.T(), scenario104MountPath(init, cases.Scenario104ScratchMount),
		"gpload init container must mount /dataload-scratch")

	main := scenario104MainContainer(out)
	require.NotNil(s.T(), main)
	assert.True(s.T(), scenario104MountPath(main, cases.Scenario104ScratchMount),
		"gpload container must mount /dataload-scratch")
}

// TestFunctional_Scenario104_KnobDefaultsOn (104-KNOB-B-default) asserts a nil
// healthChecks block leaves the checks ON (init container present).
func (s *Scenario104Suite) TestFunctional_Scenario104_KnobDefaultsOn() {
	cluster := scenario104Cluster("s104-default", scenario104PxfJob())
	require.Nil(s.T(), cluster.Spec.DataLoading.HealthChecks)

	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init, "nil healthChecks block defaults the checks ON")
	assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
}

// TestFunctional_Scenario104_CustomDiskMinFreeMB asserts a custom diskMinFreeMB
// is reflected in the HC.5 threshold of the pxf init script.
func (s *Scenario104Suite) TestFunctional_Scenario104_CustomDiskMinFreeMB() {
	cluster := scenario104Cluster("s104-disk", scenario104PxfJob())
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{DiskMinFreeMB: 512}

	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)
	assert.Contains(s.T(), init.Args[0], "512 * 1024")
	assert.NotContains(s.T(), init.Args[0], "64 * 1024")
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario104Cases() builder row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario104_CatalogHonest iterates cases.Scenario104Cases() and
// resolves EVERY builder row against the REAL built artifact: the init container
// shape/script substrings, the scratch volume + mounts, and the knob. Live and
// reconcile rows are logged + skipped (they require a running cluster / envtest).
// This keeps the catalog honest against the implementation. NO new operator
// metric is asserted (job_status=3 + errors_total + the Event + kube-state-metrics
// are the honest signals).
func (s *Scenario104Suite) TestFunctional_Scenario104_CatalogHonest() {
	catalog := cases.Scenario104Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := scenario104Cluster("test-cluster", scenario104PxfJob())
	pxfOut := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), pxfOut)
	pxfInit := scenario104InitContainer(pxfOut)
	require.NotNil(s.T(), pxfInit)
	pxfScript := pxfInit.Args[0]

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario104LayerLive:
				s.T().Logf("scenario104 %s (%s): [LIVE-ONLY] %s — resolved at e2e Part B",
					tc.ID, tc.Req, tc.Expected)
				return
			case cases.Scenario104LayerReconcile:
				s.T().Logf("scenario104 %s (%s): [reconcile] %s — resolved at controller/envtest",
					tc.ID, tc.Req, tc.Expected)
				return
			case cases.Scenario104LayerBuilder:
				s.scenario104ResolveBuilderRow(tc, pxfOut, pxfScript)
			default:
				s.T().Logf("scenario104 %s: layer %q resolved elsewhere", tc.ID, tc.Layer)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario104GploadInitScript builds a gpload dataload Job and returns its
// dataload-healthcheck init container script (HC.2/HC.4/HC.5, no HC.1/HC.3).
func (s *Scenario104Suite) scenario104GploadInitScript(name string) string {
	gp := s.builder.BuildDataLoadJob(
		scenario104Cluster(name, scenario104GploadJob()), scenario104GploadJob())
	require.NotNil(s.T(), gp)
	init := scenario104InitContainer(gp)
	require.NotNil(s.T(), init, "the gpload Job must carry the health-check init container")
	require.Len(s.T(), init.Args, 1)
	return init.Args[0]
}

// scenario104ResolveBuilderRow resolves a builder catalog row against the
// already-built pxf dataload Job + its init script.
func (s *Scenario104Suite) scenario104ResolveBuilderRow(
	tc cases.Scenario104Case, pxfOut *batchv1.Job, pxfScript string,
) {
	switch tc.ID {
	case "104-HC1-B-gate":
		// A gpload job has HC.1 (pxf-only) GATED OFF: the gpload Job's init script
		// carries NO HC.1 lines (the gating direction proven against the gpload
		// init artifact, which now exists).
		gpScript := s.scenario104GploadInitScript("s104-cat-hc1gate")
		assert.NotContains(s.T(), gpScript, "HC.1 FAIL")
		assert.NotContains(s.T(), gpScript, "pxf_version()")
		return
	case "104-HC3-B-skip":
		// HC.3 SKIP is asserted by the pxf s3 job NOT being a non-object-store
		// job; the pxf job under test IS s3 so HC.3 IS present. The skip path is
		// covered by the builder unit suite (jdbc job). Log + pass.
		s.T().Logf("scenario104 %s: HC.3 skip path covered by the builder unit suite (jdbc job)", tc.ID)
		return
	case "104-HC4-B":
		// HC.4 is gpload-only: the gpload Job's init script DIRECTLY carries the
		// gpfdist-svc reachability probe (the contract this row now pins).
		gpScript := s.scenario104GploadInitScript("s104-cat-hc4")
		assert.Contains(s.T(), gpScript, "gpfdist-svc")
		assert.Contains(s.T(), gpScript, "HC.4 FAIL")
		return
	case "104-HC4-B-gate":
		// HC.4 gating: the pxf job's init must NOT carry the gpfdist-svc probe.
		assert.NotContains(s.T(), pxfScript, "gpfdist-svc")
		assert.NotContains(s.T(), pxfScript, "HC.4 FAIL")
		return
	case "104-KNOB-B":
		disabled := false
		cluster := scenario104Cluster("s104-cat-knob", scenario104PxfJob())
		cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &disabled}
		off := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
		require.NotNil(s.T(), off)
		assert.Empty(s.T(), off.Spec.Template.Spec.InitContainers)
		return
	case "104-KNOB-B-default":
		assert.NotNil(s.T(), scenario104InitContainer(pxfOut))
		return
	}

	switch tc.Artifact {
	case cases.Scenario104ArtifactInitScript:
		if tc.Contains != "" {
			assert.Containsf(s.T(), pxfScript, tc.Contains,
				"%s init script must carry %q", tc.ID, tc.Contains)
		}
	case cases.Scenario104ArtifactInitContainer:
		init := scenario104InitContainer(pxfOut)
		require.NotNil(s.T(), init)
		assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
		assert.Equal(s.T(), []string{"/bin/bash", "-c"}, init.Command)
	case cases.Scenario104ArtifactVolume:
		vol := scenario104FindVolume(pxfOut.Spec.Template.Spec.Volumes, cases.Scenario104ScratchVolume)
		require.NotNil(s.T(), vol)
		require.NotNil(s.T(), vol.EmptyDir)
		init := scenario104InitContainer(pxfOut)
		main := scenario104MainContainer(pxfOut)
		require.NotNil(s.T(), init)
		require.NotNil(s.T(), main)
		assert.True(s.T(), scenario104MountPath(init, cases.Scenario104ScratchMount))
		assert.True(s.T(), scenario104MountPath(main, cases.Scenario104ScratchMount))
	default:
		s.T().Logf("scenario104 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

// TestFunctional_Scenario104_InitImageIsDataLoader asserts the init container
// image equals the data-loader image (the cluster image carrying psql/curl/df).
func (s *Scenario104Suite) TestFunctional_Scenario104_InitImageIsDataLoader() {
	cluster := scenario104Cluster("s104-img", scenario104PxfJob())
	cluster.Spec.Image = "cloudberry-official-pxf:2.1.0"
	out := s.builder.BuildDataLoadJob(cluster, scenario104PxfJob())
	require.NotNil(s.T(), out)
	init := scenario104InitContainer(out)
	require.NotNil(s.T(), init)
	main := scenario104MainContainer(out)
	require.NotNil(s.T(), main)
	assert.Equal(s.T(), main.Image, init.Image,
		"the init container shares the data-loader image with the main container")
	assert.Equal(s.T(), "cloudberry-official-pxf:2.1.0", init.Image)

	// Sanity: the Job name is deterministic.
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, cases.Scenario104PxfJobName), out.Name)
}

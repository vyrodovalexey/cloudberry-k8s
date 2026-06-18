package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// Scenario 104 — pre-load health-check init container builder tests (W5).
//
// Catalog IDs covered here (the -B / builder-provable cases):
//   104-HC1-B / 104-HC1-B-gate, 104-HC2-B, 104-HC3-B / 104-HC3-B-skip,
//   104-HC4-B / 104-HC4-B-gate, 104-HC5-B, 104-INIT-B,
//   104-KNOB-B / 104-KNOB-B-default, plus byte-stability + diskMinFreeMB custom.
//
// These tests ADD to the existing builder suite (no existing test changes); they
// exercise buildDataLoadHealthCheckScript, buildDataLoadHealthCheckInitContainer,
// buildDataLoadPodSpec (the init/volume/mount wiring) and the
// healthChecksEnabled/healthCheckDiskMinFreeMB helpers.
// ---------------------------------------------------------------------------

// hcCluster returns a data-loading cluster carrying the given jobs with PXF and
// gpfdist enabled (so the HC.1/HC.4 gates are open) plus an S3 backup
// destination so the HC.3 S3 creds env (reused from the connector-init pattern)
// is wired. The healthChecks block is left nil (default ON).
func hcCluster(jobs []cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	c := dataLoadJobCluster(jobs, nil)
	c.Spec.DataLoading.Pxf = &cbv1alpha1.PxfSpec{Enabled: true}
	c.Spec.DataLoading.Gpfdist = &cbv1alpha1.GpfdistSpec{Enabled: true}
	c.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   "cloudberry-data",
				Endpoint: "https://minio.example.com",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return c
}

// jdbcPxfJob is a non-object-store pxf job (jdbc) for the HC.3 skip case.
func jdbcPxfJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "jdbc-loader",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "pg-source",
			Profile:     "jdbc",
			Resource:    "public.source_events",
			TargetTable: "public.events",
		},
	}
}

// hasMountPath reports whether the container mounts the given path. (The package
// already has hasMount(mounts, name, path) and findVolume/containerByName, which
// the assertions below reuse; this helper matches by mount path only.)
func hasMountPath(c *corev1.Container, path string) bool {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].MountPath == path {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// HC.1 — PXF readiness DB proxy.
// ---------------------------------------------------------------------------

// TestHealthCheckScript_HC1_PXFReadiness (104-HC1-B) asserts a pxf job (pxf
// enabled) yields the DB-proxy PXF-readiness probe substrings.
func TestHealthCheckScript_HC1_PXFReadiness(t *testing.T) {
	job := pxfTestJob() // s3:parquet pxf job
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)

	assert.Contains(t, script, "pg_extension")
	assert.Contains(t, script, "extname='pxf'")
	assert.Contains(t, script, "pxf_version()")
	assert.Contains(t, script, "HC.1 FAIL")
	// The honest DB-proxy uses psql against the coordinator (no direct sidecar curl).
	assert.Contains(t, script, "SELECT 1")
	assert.NotContains(t, script, "actuator/health",
		"HC.1 is a DB proxy, NOT a direct sidecar curl")
}

// TestHealthCheckScript_GpEnvPreamble asserts the health-check script sources the
// Cloudberry env preamble (so psql/curl are on PATH in the data-loader image)
// BEFORE the first psql-based probe — otherwise HC.1/HC.2 fail with
// "psql: command not found" before the real check runs.
func TestHealthCheckScript_GpEnvPreamble(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)

	assert.Contains(t, script, gpEnvPreamble,
		"the health-check script must source the gp env preamble so psql is on PATH")
	// The preamble must precede the first psql probe.
	assert.Less(t, strings.Index(script, gpEnvPreamble), strings.Index(script, "psql"),
		"the gp env preamble must come before the first psql probe")
}

// TestHealthCheckScript_HC1_GateGpload (104-HC1-B-gate) asserts a gpload job has
// NO HC.1 PXF lines (HC.1 is pxf-only).
func TestHealthCheckScript_HC1_GateGpload(t *testing.T) {
	job := gploadTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)

	assert.NotContains(t, script, "pxf_version()")
	assert.NotContains(t, script, "pg_extension")
	assert.NotContains(t, script, "HC.1 FAIL")
}

// TestHealthCheckScript_HC1_GatePxfDisabled (104-HC1-B-gate) asserts a pxf job
// with PXF disabled at the cluster level emits no HC.1 block.
func TestHealthCheckScript_HC1_GatePxfDisabled(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.Pxf.Enabled = false

	script := buildDataLoadHealthCheckScript(cluster, job)

	assert.NotContains(t, script, "pxf_version()")
	assert.NotContains(t, script, "HC.1 FAIL")
}

// ---------------------------------------------------------------------------
// HC.2 — target-table exists (ALL jobs).
// ---------------------------------------------------------------------------

// TestHealthCheckScript_HC2_TargetTable (104-HC2-B) asserts EVERY job type emits
// the to_regclass target-table-exists probe.
func TestHealthCheckScript_HC2_TargetTable(t *testing.T) {
	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
	}{
		{"pxf", pxfTestJob()},
		{"gpload", gploadTestJob()},
		{"jdbc-pxf", jdbcPxfJob()},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cluster := hcCluster([]cbv1alpha1.DataLoadingJob{tc.job})
			script := buildDataLoadHealthCheckScript(cluster, tc.job)

			want := dataLoadTargetTable(tc.job)
			require.NotEmpty(t, want)
			// The target is emitted as an injection-safe single-quoted shell var
			// (tbl=...) then fed into to_regclass('${tbl}').
			assert.Contains(t, script, "tbl='"+want+"'")
			assert.Contains(t, script, "to_regclass('${tbl}')")
			assert.Contains(t, script, "HC.2 FAIL")
			assert.Contains(t, script, "does not exist")
		})
	}
}

// TestHealthCheckScript_HC2_NoTargetTableSkipped asserts the HC.2 probe is
// SKIPPED when no target table is resolvable (e.g. a mis-shaped job with no
// Pxf/Gpload spec) — the load builder catches such jobs earlier. (Edge path.)
func TestHealthCheckScript_HC2_NoTargetTableSkipped(t *testing.T) {
	job := cbv1alpha1.DataLoadingJob{Name: "bad", Type: "pxf", Enabled: true} // nil PxfJob
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.Pxf.Enabled = false // also gate HC.1 off for clarity

	script := buildDataLoadHealthCheckScript(cluster, job)
	require.Empty(t, dataLoadTargetTable(job))
	assert.NotContains(t, script, "HC.2 FAIL")
	assert.NotContains(t, script, "to_regclass")
	// HC.5 still runs for all jobs.
	assert.Contains(t, script, "HC.5 FAIL")
}

// TestHealthCheckScript_HC2_TargetTablePxfExact pins the exact public.events
// target-table substring for the canonical pxf job (104-HC2-B catalog artifact).
func TestHealthCheckScript_HC2_TargetTablePxfExact(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.Contains(t, script, "tbl='public.events'")
	assert.Contains(t, script, "to_regclass('${tbl}')")
}

// ---------------------------------------------------------------------------
// HC.3 — external source connectivity (s3-family only).
// ---------------------------------------------------------------------------

// TestHealthCheckScript_HC3_S3Connectivity (104-HC3-B) asserts a pxf s3 job emits
// the curl-head reachability probe against AWS_S3_ENDPOINT, and that the init
// container carries AWS_* via SecretKeyRef (NO plaintext) + AWS_S3_ENDPOINT.
func TestHealthCheckScript_HC3_S3Connectivity(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob() // s3:parquet
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	// Script substrings.
	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.Contains(t, script, `curl -fsS -m 10 --head "${AWS_S3_ENDPOINT}"`)
	assert.Contains(t, script, "HC.3 FAIL")

	// Init-container env: AWS_ACCESS_KEY_ID via SecretKeyRef, NO plaintext value,
	// plus AWS_S3_ENDPOINT carrying the MinIO endpoint.
	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)
	require.NotEmpty(t, podSpec.InitContainers)
	init := podSpec.InitContainers[0]
	env := map[string]corev1.EnvVar{}
	for _, e := range init.Env {
		env[e.Name] = e
	}
	require.Contains(t, env, "AWS_ACCESS_KEY_ID")
	assert.Empty(t, env["AWS_ACCESS_KEY_ID"].Value, "creds must never be plaintext")
	require.NotNil(t, env["AWS_ACCESS_KEY_ID"].ValueFrom)
	require.NotNil(t, env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef)
	assert.Equal(t, "backup-s3-credentials",
		env["AWS_ACCESS_KEY_ID"].ValueFrom.SecretKeyRef.Name)
	require.Contains(t, env, "AWS_S3_ENDPOINT")
	assert.Equal(t, "https://minio.example.com", env["AWS_S3_ENDPOINT"].Value)
}

// TestHealthCheckScript_HC3_SkipNonObjectStore (104-HC3-B-skip) asserts a jdbc
// pxf job is NOT auto-probed for HC.3 connectivity (no curl --head endpoint
// line; no HC.3 SKIP/FAIL block emitted).
func TestHealthCheckScript_HC3_SkipNonObjectStore(t *testing.T) {
	job := jdbcPxfJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.NotContains(t, script, `--head "${AWS_S3_ENDPOINT}"`)
	assert.NotContains(t, script, "HC.3 FAIL")
	assert.NotContains(t, script, "HC.3 SKIP")
}

// ---------------------------------------------------------------------------
// HC.4 — gpfdist reachability (gpload only).
// ---------------------------------------------------------------------------

// TestHealthCheckScript_HC4_GpfdistReachability (104-HC4-B) asserts a gpload job
// (gpfdist enabled) curls the gpfdist Service.
func TestHealthCheckScript_HC4_GpfdistReachability(t *testing.T) {
	job := gploadTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.Contains(t, script, "http://test-cluster-gpfdist-svc:8080/")
	assert.Contains(t, script, "curl -fsS")
	assert.Contains(t, script, "HC.4 FAIL")
}

// TestHealthCheckScript_HC4_GatePxf (104-HC4-B-gate) asserts a pxf job has no
// gpfdist-svc HC.4 line (HC.4 is gpload-only).
func TestHealthCheckScript_HC4_GatePxf(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.NotContains(t, script, "gpfdist-svc")
	assert.NotContains(t, script, "HC.4 FAIL")
}

// TestHealthCheckScript_HC4_GateGpfdistDisabled (104-HC4-B-gate) asserts a gpload
// job with gpfdist disabled at the cluster level emits no HC.4 block.
func TestHealthCheckScript_HC4_GateGpfdistDisabled(t *testing.T) {
	job := gploadTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.Gpfdist.Enabled = false

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.NotContains(t, script, "gpfdist-svc")
	assert.NotContains(t, script, "HC.4 FAIL")
}

// ---------------------------------------------------------------------------
// HC.5 — disk space + scratch volume/mounts (ALL jobs).
// ---------------------------------------------------------------------------

// TestHealthCheckScript_HC5_DiskSpace (104-HC5-B) asserts the df probe + the
// default 64MB threshold (rendered as `64 * 1024`).
func TestHealthCheckScript_HC5_DiskSpace(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.Contains(t, script, "df -Pk /dataload-scratch")
	assert.Contains(t, script, "64 * 1024")
	assert.Contains(t, script, "HC.5 FAIL")
}

// TestHealthCheckPodSpec_HC5_ScratchVolumeAndMounts (104-HC5-B) asserts the
// scratch emptyDir is in podSpec.Volumes and is mounted at /dataload-scratch on
// BOTH the init container AND the main dataload container.
func TestHealthCheckPodSpec_HC5_ScratchVolumeAndMounts(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)

	// Scratch emptyDir volume present.
	vol := findVolume(podSpec.Volumes, dataLoadScratchVolumeName)
	require.NotNil(t, vol, "scratch volume must be present")
	require.NotNil(t, vol.EmptyDir, "scratch volume must be an emptyDir")

	// Mount on the init container.
	require.NotEmpty(t, podSpec.InitContainers)
	init := podSpec.InitContainers[0]
	assert.True(t, hasMountPath(&init, dataLoadScratchMountPath),
		"init container must mount /dataload-scratch")

	// Mount on the main dataload container.
	main := containerByName(podSpec.Containers, dataLoadContainerName)
	require.NotNil(t, main)
	assert.True(t, hasMountPath(main, dataLoadScratchMountPath),
		"main dataload container must mount /dataload-scratch")
}

// TestHealthCheckScratchVolume_SizeLimit asserts the scratch emptyDir honors the
// configured scratchSizeLimit (HC.5 deterministic-fill knob).
func TestHealthCheckScratchVolume_SizeLimit(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{
		ScratchSizeLimit: "256Mi",
	}

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)
	vol := findVolume(podSpec.Volumes, dataLoadScratchVolumeName)
	require.NotNil(t, vol)
	require.NotNil(t, vol.EmptyDir)
	require.NotNil(t, vol.EmptyDir.SizeLimit, "size limit must be applied")
	assert.Equal(t, "256Mi", vol.EmptyDir.SizeLimit.String())
}

// TestHealthCheckScratchVolume_InvalidSizeLimitIgnored asserts an unparseable
// sizeLimit is ignored (volume stays unbounded; defensive path).
func TestHealthCheckScratchVolume_InvalidSizeLimitIgnored(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{
		ScratchSizeLimit: "not-a-quantity",
	}

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)
	vol := findVolume(podSpec.Volumes, dataLoadScratchVolumeName)
	require.NotNil(t, vol)
	require.NotNil(t, vol.EmptyDir)
	assert.Nil(t, vol.EmptyDir.SizeLimit, "unparseable size limit must be ignored")
}

// TestHealthCheckScript_HC5_CustomDiskMinFreeMB asserts a custom diskMinFreeMB
// value is reflected in the HC.5 threshold.
func TestHealthCheckScript_HC5_CustomDiskMinFreeMB(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{
		DiskMinFreeMB: 512,
	}

	script := buildDataLoadHealthCheckScript(cluster, job)
	assert.Contains(t, script, "512 * 1024")
	assert.NotContains(t, script, "64 * 1024")
}

// ---------------------------------------------------------------------------
// 104-INIT-B — init container shape (FIRST, named, image, command).
// ---------------------------------------------------------------------------

// TestHealthCheckInitContainer_Shape (104-INIT-B) asserts the init container is
// FIRST, named dataload-healthcheck, uses the data-loader image, and runs
// /bin/bash -c.
func TestHealthCheckInitContainer_Shape(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	podSpec := b.buildDataLoadPodSpec(cluster, "load-script", job)

	require.NotEmpty(t, podSpec.InitContainers, "init container must be present (default on)")
	init := podSpec.InitContainers[0]
	assert.Equal(t, "dataload-healthcheck", init.Name)
	assert.Equal(t, dataLoaderImage(cluster), init.Image)
	assert.Equal(t, []string{"/bin/bash", "-c"}, init.Command)
	require.Len(t, init.Args, 1)
	// The init runs the health-check script (the 5-check framing).
	assert.Contains(t, init.Args[0], "dataload-healthcheck: starting pre-load health checks")
	assert.Equal(t, corev1.TerminationMessageFallbackToLogsOnError, init.TerminationMessagePolicy)
}

// TestHealthCheckInitContainer_IsFirst (104-INIT-B) asserts the health-check init
// container is the FIRST init container even when other init containers exist on
// the rendered pod (it is PREPENDED).
func TestHealthCheckInitContainer_IsFirst(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	inits := out.Spec.Template.Spec.InitContainers
	require.NotEmpty(t, inits)
	assert.Equal(t, "dataload-healthcheck", inits[0].Name,
		"the health-check init container must run FIRST")
}

// ---------------------------------------------------------------------------
// 104-KNOB-B — enabled:false opt-out + nil block default-on.
// ---------------------------------------------------------------------------

// TestHealthCheckKnob_DisabledOmitsEverything (104-KNOB-B) asserts
// healthChecks.enabled=false removes the init container, the scratch volume AND
// the main-container scratch mount (byte-identical to a pre-Scenario-104 pod).
func TestHealthCheckKnob_DisabledOmitsEverything(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	disabled := false
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{
		Enabled: &disabled,
	}

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)

	assert.Empty(t, podSpec.InitContainers, "no init container when disabled")
	assert.Nil(t, findVolume(podSpec.Volumes, dataLoadScratchVolumeName),
		"no scratch volume when disabled")
	main := containerByName(podSpec.Containers, dataLoadContainerName)
	require.NotNil(t, main)
	assert.False(t, hasMountPath(main, dataLoadScratchMountPath),
		"main container must not mount scratch when disabled")
}

// TestHealthCheckKnob_NilBlockDefaultsOn (104-KNOB-B-default) asserts a nil
// healthChecks block leaves the checks ON (init container present).
func TestHealthCheckKnob_NilBlockDefaultsOn(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	require.Nil(t, cluster.Spec.DataLoading.HealthChecks)

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)
	require.NotEmpty(t, podSpec.InitContainers)
	assert.Equal(t, "dataload-healthcheck", podSpec.InitContainers[0].Name)
}

// TestHealthCheckKnob_NilEnabledPointerDefaultsOn asserts a healthChecks block
// with a nil Enabled pointer still defaults ON.
func TestHealthCheckKnob_NilEnabledPointerDefaultsOn(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})
	cluster.Spec.DataLoading.HealthChecks = &cbv1alpha1.DataLoadHealthChecksSpec{
		DiskMinFreeMB: 64, // Enabled left nil
	}

	podSpec := b.buildDataLoadPodSpec(cluster, "script", job)
	require.NotEmpty(t, podSpec.InitContainers)
}

// ---------------------------------------------------------------------------
// Helper unit coverage + determinism.
// ---------------------------------------------------------------------------

// TestHealthChecksEnabled covers the gate helper across nil/nil-ptr/true/false.
func TestHealthChecksEnabled(t *testing.T) {
	on := true
	off := false
	tests := []struct {
		name string
		hc   *cbv1alpha1.DataLoadHealthChecksSpec
		dl   bool // include DataLoading block
		want bool
	}{
		{"nil dataloading", nil, false, true},
		{"nil block", nil, true, true},
		{"nil enabled pointer", &cbv1alpha1.DataLoadHealthChecksSpec{}, true, true},
		{"explicit true", &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &on}, true, true},
		{"explicit false", &cbv1alpha1.DataLoadHealthChecksSpec{Enabled: &off}, true, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCluster()
			if tc.dl {
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{HealthChecks: tc.hc}
			}
			assert.Equal(t, tc.want, healthChecksEnabled(c))
		})
	}
}

// TestHealthCheckDiskMinFreeMB covers the threshold resolver (default + custom +
// non-positive clamp + nil block).
func TestHealthCheckDiskMinFreeMB(t *testing.T) {
	tests := []struct {
		name string
		hc   *cbv1alpha1.DataLoadHealthChecksSpec
		dl   bool
		want int32
	}{
		{"nil dataloading default", nil, false, 64},
		{"nil block default", nil, true, 64},
		{"zero clamps to default", &cbv1alpha1.DataLoadHealthChecksSpec{DiskMinFreeMB: 0}, true, 64},
		{"negative clamps to default", &cbv1alpha1.DataLoadHealthChecksSpec{DiskMinFreeMB: -5}, true, 64},
		{"custom value", &cbv1alpha1.DataLoadHealthChecksSpec{DiskMinFreeMB: 128}, true, 128},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCluster()
			if tc.dl {
				c.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{HealthChecks: tc.hc}
			}
			assert.Equal(t, tc.want, healthCheckDiskMinFreeMB(c))
		})
	}
}

// TestHealthCheckScript_ByteStable asserts the script is deterministic: the same
// job + cluster always yields the byte-identical script.
func TestHealthCheckScript_ByteStable(t *testing.T) {
	job := pxfTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	first := buildDataLoadHealthCheckScript(cluster, job)
	second := buildDataLoadHealthCheckScript(cluster, job)
	assert.Equal(t, first, second, "health-check script must be byte-stable")

	// Framing is present (set -euo pipefail + start/end echoes).
	assert.Contains(t, first, "set -euo pipefail")
	assert.Contains(t, first, "all checks passed")
}

// TestHealthCheckScript_GploadFullGating asserts a gpload job's script gates HC.1
// (off) and HC.3 (off) while emitting HC.2, HC.4 and HC.5.
func TestHealthCheckScript_GploadFullGating(t *testing.T) {
	job := gploadTestJob()
	cluster := hcCluster([]cbv1alpha1.DataLoadingJob{job})

	script := buildDataLoadHealthCheckScript(cluster, job)
	// HC.1 / HC.3 off.
	assert.NotContains(t, script, "HC.1 FAIL")
	assert.NotContains(t, script, "HC.3 FAIL")
	// HC.2 / HC.4 / HC.5 on.
	assert.Contains(t, script, "HC.2 FAIL")
	assert.Contains(t, script, "HC.4 FAIL")
	assert.Contains(t, script, "HC.5 FAIL")
}

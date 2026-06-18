//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 101: gpfdist Deployment + Job 4 (gpload-csv) — functional
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario101Cases — this is Scenario 101, the gpfdist
// + gpload-csv verification scenario. It mirrors the Scenario 99 functional SHAPE
// exactly, driving the BUILDER (gpfdist Deployment/Service/PVC + the gpload
// control file / ConfigMap / Job-CronJob) and the VALIDATOR (webhook W.18-W.22)
// WITHOUT a live cluster, asserting the shipped production contract:
//
//   - gpfdist builders (GP.2-GP.5): BuildGpfdistDeployment/Service/PVC shapes —
//     Deployment name/replicas/image/command+args/port/mount; Service
//     name/selector(avsoft.io/component=gpfdist == pod labels)/port; PVC
//     name/mount. ownerRefs on every object.
//
//   - gpload control file (GL.1-GL.7): the byte-exact golden control file for the
//     spec gpload-csv example + per-line substrings, the ConfigMap shape, and the
//     gpload Job/CronJob (J.25 schedule->CronJob; args gpload -f
//     /etc/gpload/<job>.yml; CM mount /etc/gpload).
//
//   - inputSource local (J.27), mode update/merge (J.36/J.37 + matchColumns).
//
//   - Webhook W.18-W.22 via the validate path (accept/reject) + the W.16 file://
//     regression.
//
//   - CatalogHonest: resolve each cases.Scenario101Cases() builder/webhook row
//     against the REAL built artifact (live rows are logged + skipped here).
//
// METRIC HONESTY: cloudberry_gpfdist_* stay PLANNED and are NEVER asserted here.
// gpload reuses cloudberry_data_loading_*; the live "data loads" signal
// (count(*) FROM public.raw_data > 0) is at e2e Part B.
// ============================================================================

// Scenario101Suite exercises the gpfdist + gpload-csv builder + validator
// contract at the builder + webhook layer.
type Scenario101Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario101(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario101Suite))
}

func (s *Scenario101Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario101GpfdistCluster builds a running cluster whose data-loading spec
// carries gpfdist.enabled:true with the supplied sub-spec (nil for defaults).
func scenario101GpfdistCluster(
	name string, gp *cbv1alpha1.GpfdistSpec,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: gp,
	}
	return cluster
}

// scenario101GploadCluster builds a running cluster carrying gpfdist.enabled +
// the supplied gpload jobs (the load reads from the cluster gpfdist Service).
func scenario101GploadCluster(
	name string, jobs ...cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs:    jobs,
	}
	return cluster
}

// scenario101GploadCSVJob returns the spec's gpload-csv job EXACTLY per the
// breakdown §1/§11 example: gpfdist source, /incoming/*.csv glob, csv/",",
// header true, UTF-8, error-limit 50 + log-errors, target public.raw_data,
// mode insert, preload truncate true, postAction ANALYZE. This is the GOLDEN
// fixture the control-file golden test resolves against.
func scenario101GploadCSVJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    cases.Scenario101JobName,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist"},
			FilePaths:   []string{cases.Scenario101FileGlob},
			Format:      "csv",
			Delimiter:   ",",
			Header:      util.Ptr(true),
			Encoding:    "UTF-8",
			ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit: 50,
				LogErrors:          util.Ptr(true),
			},
			TargetTable: cases.Scenario101TargetTable,
			Mode:        "insert",
			Preload:     &cbv1alpha1.GploadPreloadSpec{Truncate: util.Ptr(true)},
			PostActions: []string{"ANALYZE public.raw_data"},
		},
	}
}

// ----------------------------------------------------------------------------
// GP.2-GP.5 — gpfdist Deployment / Service / PVC builders
// ----------------------------------------------------------------------------

// TestFunctional_Scenario101_GpfdistDeployment asserts the GP.2/GP.3/GP.4
// Deployment contract: name <cluster>-gpfdist, default replicas 1, the default
// image, the EXACT command+args, the named port 8080, the /data volumeMount +
// the <cluster>-gpfdist-data-pvc claim, and a cluster ownerRef.
func (s *Scenario101Suite) TestFunctional_Scenario101_GpfdistDeployment() {
	cluster := scenario101GpfdistCluster("s101-gp-dep",
		&cbv1alpha1.GpfdistSpec{Enabled: true})

	dep := s.builder.BuildGpfdistDeployment(cluster)
	require.NotNil(s.T(), dep)

	// GP.2 name + ownerRef.
	assert.Equal(s.T(), cluster.Name+"-gpfdist", dep.Name)
	assert.Equal(s.T(), builder.GpfdistServiceName(cluster.Name), dep.Name)
	scenario101AssertOwned(s.T(), dep.OwnerReferences, cluster)

	// GP.2 replicas default 1, image default.
	require.NotNil(s.T(), dep.Spec.Replicas)
	assert.Equal(s.T(), int32(1), *dep.Spec.Replicas)
	require.Len(s.T(), dep.Spec.Template.Spec.Containers, 1)
	c := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), cases.Scenario101GpfdistImage, c.Image)

	// GP.2 command + args.
	assert.Equal(s.T(), "gpfdist", c.Name)
	assert.Equal(s.T(), []string{"gpfdist"}, c.Command)
	assert.Equal(s.T(), []string{
		"-d", "/data",
		"-p", "8080",
		"-l", "/var/log/gpfdist.log",
	}, c.Args)

	// GP.3 named port 8080.
	require.Len(s.T(), c.Ports, 1)
	assert.Equal(s.T(), "gpfdist", c.Ports[0].Name)
	assert.Equal(s.T(), int32(cases.Scenario101GpfdistPort), c.Ports[0].ContainerPort)

	// GP.4 /data mount + claim.
	require.Len(s.T(), c.VolumeMounts, 1)
	assert.Equal(s.T(), "data", c.VolumeMounts[0].Name)
	assert.Equal(s.T(), "/data", c.VolumeMounts[0].MountPath)
	require.Len(s.T(), dep.Spec.Template.Spec.Volumes, 1)
	vol := dep.Spec.Template.Spec.Volumes[0]
	require.NotNil(s.T(), vol.PersistentVolumeClaim)
	assert.Equal(s.T(), util.GpfdistDataPVCName(cluster.Name),
		vol.PersistentVolumeClaim.ClaimName)
}

// TestFunctional_Scenario101_GpfdistReplicasAndImage asserts GP.2 honors a
// custom replicas + image (J/C.20).
func (s *Scenario101Suite) TestFunctional_Scenario101_GpfdistReplicasAndImage() {
	cluster := scenario101GpfdistCluster("s101-gp-ri", &cbv1alpha1.GpfdistSpec{
		Enabled:  true,
		Replicas: util.Ptr(int32(3)),
		Image:    "my-registry/gpfdist:9.9",
	})
	dep := s.builder.BuildGpfdistDeployment(cluster)
	require.NotNil(s.T(), dep)
	require.NotNil(s.T(), dep.Spec.Replicas)
	assert.Equal(s.T(), int32(3), *dep.Spec.Replicas)
	assert.Equal(s.T(), "my-registry/gpfdist:9.9",
		dep.Spec.Template.Spec.Containers[0].Image)
}

// TestFunctional_Scenario101_GpfdistService asserts the GP.5 Service contract:
// name <cluster>-gpfdist-svc, selector avsoft.io/component=gpfdist EQUAL to the
// Deployment pod labels, port/targetPort 8080, and a cluster ownerRef.
func (s *Scenario101Suite) TestFunctional_Scenario101_GpfdistService() {
	cluster := scenario101GpfdistCluster("s101-gp-svc",
		&cbv1alpha1.GpfdistSpec{Enabled: true})

	svc := s.builder.BuildGpfdistService(cluster)
	dep := s.builder.BuildGpfdistDeployment(cluster)
	require.NotNil(s.T(), svc)
	require.NotNil(s.T(), dep)

	// GP.5 name + ownerRef.
	assert.Equal(s.T(), cluster.Name+"-gpfdist-svc", svc.Name)
	assert.Equal(s.T(), util.GpfdistServiceName2(cluster.Name), svc.Name)
	scenario101AssertOwned(s.T(), svc.OwnerReferences, cluster)

	// GP.5 selector == pod labels, with the gpfdist component label.
	assert.Equal(s.T(), util.ComponentGpfdist, svc.Spec.Selector[util.LabelComponent])
	assert.Equal(s.T(), dep.Spec.Template.ObjectMeta.Labels, svc.Spec.Selector,
		"GP.5: Service selector must EQUAL the Deployment pod labels")

	// GP.5 port 8080 targetPort 8080.
	require.Len(s.T(), svc.Spec.Ports, 1)
	assert.Equal(s.T(), int32(cases.Scenario101GpfdistPort), svc.Spec.Ports[0].Port)
	assert.Equal(s.T(), cases.Scenario101GpfdistPort,
		svc.Spec.Ports[0].TargetPort.IntValue())
}

// TestFunctional_Scenario101_GpfdistPVC asserts the GP.4 PVC contract: name
// <cluster>-gpfdist-data-pvc, gpfdist component label, ownerRef.
func (s *Scenario101Suite) TestFunctional_Scenario101_GpfdistPVC() {
	cluster := scenario101GpfdistCluster("s101-gp-pvc",
		&cbv1alpha1.GpfdistSpec{Enabled: true})

	pvc := s.builder.BuildGpfdistPVC(cluster)
	require.NotNil(s.T(), pvc)
	assert.Equal(s.T(), cluster.Name+"-gpfdist-data-pvc", pvc.Name)
	assert.Equal(s.T(), util.GpfdistDataPVCName(cluster.Name), pvc.Name)
	assert.Equal(s.T(), util.ComponentGpfdist, pvc.Labels[util.LabelComponent])
	scenario101AssertOwned(s.T(), pvc.OwnerReferences, cluster)
}

// scenario101AssertOwned asserts a single controller ownerRef pointing at the
// cluster (GP.2-5 ownerRef contract).
func scenario101AssertOwned(
	t require.TestingT, refs []metav1.OwnerReference, cluster *cbv1alpha1.CloudberryCluster,
) {
	require.Len(t, refs, 1)
	assert.Equal(t, "CloudberryCluster", refs[0].Kind)
	assert.Equal(t, cluster.Name, refs[0].Name)
	require.NotNil(t, refs[0].Controller)
	assert.True(t, *refs[0].Controller)
}

// ----------------------------------------------------------------------------
// GL.1-GL.7 — gpload control file (golden + per-line)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario101_ControlFileGolden asserts the FULL byte-exact
// control file for the spec gpload-csv fixture (GL.1-GL.7). HOST is
// <cluster>-coord-hl and the gpfdist SOURCE emits LOCAL_HOSTNAME
// <cluster>-gpfdist-svc + PORT 8080 plus the LOCAL FILE path (NO gpfdist:// URL).
func (s *Scenario101Suite) TestFunctional_Scenario101_ControlFileGolden() {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").Build()
	job := scenario101GploadCSVJob()

	got, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)

	want := "VERSION: 1.0.0.1\n" +
		"DATABASE: postgres\n" +
		"USER: gpadmin\n" +
		"HOST: test-cluster-coord-hl\n" +
		"PORT: 5432\n" +
		"GPLOAD:\n" +
		"  INPUT:\n" +
		"    - SOURCE:\n" +
		"        LOCAL_HOSTNAME:\n" +
		"          - test-cluster-gpfdist-svc\n" +
		"        PORT: 8080\n" +
		"        FILE:\n" +
		"          - /incoming/*.csv\n" +
		"    - FORMAT: csv\n" +
		"    - DELIMITER: ','\n" +
		"    - HEADER: true\n" +
		"    - ENCODING: UTF-8\n" +
		"    - ERROR_LIMIT: 50\n" +
		"    - LOG_ERRORS: true\n" +
		"  OUTPUT:\n" +
		"    - TABLE: public.raw_data\n" +
		"    - MODE: INSERT\n" +
		"  PRELOAD:\n" +
		"    - TRUNCATE: true\n" +
		"  SQL:\n" +
		"    - AFTER: \"ANALYZE public.raw_data\"\n"
	assert.Equal(s.T(), want, got)

	// Byte-stable: same input rendered twice yields identical output.
	second, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), got, second)
}

// scenario101ControlFile renders the control file for a gpload spec against the
// test cluster, failing the test on error.
func (s *Scenario101Suite) scenario101ControlFile(gp *cbv1alpha1.GploadJobSpec) string {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").Build()
	job := cbv1alpha1.DataLoadingJob{
		Name:      cases.Scenario101JobName,
		Type:      "gpload",
		Enabled:   true,
		GploadJob: gp,
	}
	got, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	return got
}

// TestFunctional_Scenario101_ControlFilePerLine asserts the GL.1-GL.7 per-line
// substrings against the gpload-csv fixture control file.
func (s *Scenario101Suite) TestFunctional_Scenario101_ControlFilePerLine() {
	got := s.scenario101ControlFile(scenario101GploadCSVJob().GploadJob)

	lines := []string{
		cases.Scenario101GLVersion,    // GL.1
		cases.Scenario101GLDatabase,   // GL.1
		cases.Scenario101GLUser,       // GL.1
		"HOST: test-cluster-coord-hl", // GL.1
		"PORT: 5432",                  // GL.1
		"        LOCAL_HOSTNAME:\n          - test-cluster-gpfdist-svc\n", // GL.2
		"        PORT: 8080\n",                         // GL.2
		"        FILE:\n          - /incoming/*.csv\n", // GL.2
		cases.Scenario101GLFormat,                      // GL.3
		cases.Scenario101GLDelimiter,                   // GL.3
		cases.Scenario101GLHeader,                      // GL.3
		cases.Scenario101GLEncoding,                    // GL.3
		cases.Scenario101GLErrLimit,                    // GL.4
		cases.Scenario101GLLogErrors,                   // GL.4
		cases.Scenario101GLTable,                       // GL.5
		cases.Scenario101GLModeIns,                     // GL.5
		cases.Scenario101GLTruncate,                    // GL.6
		cases.Scenario101GLAfter,                       // GL.7
	}
	for _, want := range lines {
		assert.Containsf(s.T(), got, want, "control file must carry %q", want)
	}
}

// TestFunctional_Scenario101_InputSourceVariants asserts the GL.2 / J.27-J.29
// SOURCE-block composition: gpfdist default svc/port -> LOCAL_HOSTNAME/PORT +
// local FILE path, a custom host (LOCAL_HOSTNAME override), a custom port (PORT
// override) and a local verbatim path (no LOCAL_HOSTNAME/PORT, no gpfdist://).
func (s *Scenario101Suite) TestFunctional_Scenario101_InputSourceVariants() {
	base := func() *cbv1alpha1.GploadJobSpec {
		return &cbv1alpha1.GploadJobSpec{
			TargetTable: cases.Scenario101TargetTable,
			FilePaths:   []string{cases.Scenario101FileGlob},
		}
	}

	s.Run("J.26 gpfdist default svc + port 8080", func() {
		got := s.scenario101ControlFile(base())
		assert.Contains(s.T(), got,
			"        LOCAL_HOSTNAME:\n          - test-cluster-gpfdist-svc\n")
		assert.Contains(s.T(), got, "        PORT: 8080\n")
		assert.Contains(s.T(), got, "        FILE:\n          - /incoming/*.csv\n")
		assert.NotContains(s.T(), got, "gpfdist://")
	})

	s.Run("J.28 custom host", func() {
		gp := base()
		gp.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist", Host: "files.internal"}
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got,
			"        LOCAL_HOSTNAME:\n          - files.internal\n")
		assert.Contains(s.T(), got, "        FILE:\n          - /incoming/*.csv\n")
		assert.NotContains(s.T(), got, "gpfdist://")
	})

	s.Run("J.29 custom port", func() {
		gp := base()
		gp.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "gpfdist", Port: 9999}
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got, "        PORT: 9999\n")
		assert.Contains(s.T(), got, "        FILE:\n          - /incoming/*.csv\n")
		assert.NotContains(s.T(), got, "gpfdist://")
	})

	s.Run("J.27 local verbatim path (no LOCAL_HOSTNAME/PORT, no gpfdist:// prefix)", func() {
		gp := base()
		gp.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "local"}
		gp.FilePaths = []string{"/data/incoming/*.csv"}
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got, "        FILE:\n          - /data/incoming/*.csv\n")
		assert.NotContains(s.T(), got, "gpfdist://")
		assert.NotContains(s.T(), got, "LOCAL_HOSTNAME")
	})
}

// TestFunctional_Scenario101_ModeVariants asserts the GL.5 / J.36-J.37 MODE
// variants: update/merge emit MODE UPDATE/MERGE + MATCH_COLUMNS; insert emits
// neither MATCH_COLUMNS nor UPDATE_COLUMNS.
func (s *Scenario101Suite) TestFunctional_Scenario101_ModeVariants() {
	base := func() *cbv1alpha1.GploadJobSpec {
		return &cbv1alpha1.GploadJobSpec{
			TargetTable: cases.Scenario101TargetTable,
			FilePaths:   []string{cases.Scenario101FileGlob},
		}
	}

	s.Run("J.35 insert -> MODE INSERT, no match cols", func() {
		gp := base()
		gp.Mode = "insert"
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got, "    - MODE: INSERT\n")
		assert.NotContains(s.T(), got, "MATCH_COLUMNS")
	})

	s.Run("J.36 update -> MODE UPDATE + MATCH_COLUMNS", func() {
		gp := base()
		gp.Mode = "update"
		gp.MatchColumns = []string{"id"}
		gp.UpdateColumns = []string{"payload"}
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got, "    - MODE: UPDATE\n")
		assert.Contains(s.T(), got, "    - MATCH_COLUMNS: [ id ]\n")
		assert.Contains(s.T(), got, "    - UPDATE_COLUMNS: [ payload ]\n")
	})

	s.Run("J.37 merge -> MODE MERGE + MATCH_COLUMNS", func() {
		gp := base()
		gp.Mode = "merge"
		gp.MatchColumns = []string{"id", "tenant"}
		got := s.scenario101ControlFile(gp)
		assert.Contains(s.T(), got, "    - MODE: MERGE\n")
		assert.Contains(s.T(), got, "    - MATCH_COLUMNS: [ id, tenant ]\n")
	})
}

// ----------------------------------------------------------------------------
// J.25 — ConfigMap + Job / CronJob
// ----------------------------------------------------------------------------

// TestFunctional_Scenario101_ControlFileConfigMap asserts the per-job ConfigMap
// (J.25): name <cluster>-gpload-<job>, data key <job>.yml == the control file,
// cluster ownerRef.
func (s *Scenario101Suite) TestFunctional_Scenario101_ControlFileConfigMap() {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").Build()
	job := scenario101GploadCSVJob()

	cm := s.builder.BuildGploadControlFileConfigMap(cluster, job)
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), util.GploadControlFileConfigMapName(cluster.Name, job.Name), cm.Name)
	scenario101AssertOwned(s.T(), cm.OwnerReferences, cluster)

	want, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	key := job.Name + ".yml"
	require.Contains(s.T(), cm.Data, key)
	assert.Equal(s.T(), want, cm.Data[key])
}

// TestFunctional_Scenario101_JobAndCronJob asserts J.25: a gpload job WITHOUT a
// schedule -> a one-off Job that runs gpload -f /etc/gpload/<job>.yml and mounts
// the control-file ConfigMap at /etc/gpload; WITH a schedule -> a CronJob with
// spec.schedule == "*/30 * * * *".
func (s *Scenario101Suite) TestFunctional_Scenario101_JobAndCronJob() {
	cluster := testutil.NewClusterBuilder("test-cluster", "default").Build()

	s.Run("J.25 one-off Job: gpload -f + CM mount", func() {
		job := scenario101GploadCSVJob()
		out := s.builder.BuildGploadJob(cluster, job)
		require.NotNil(s.T(), out)
		assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), out.Name)

		require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
		c := out.Spec.Template.Spec.Containers[0]
		require.Len(s.T(), c.Args, 1)
		assert.Contains(s.T(), c.Args[0],
			"gpload -f /etc/gpload/"+cases.Scenario101JobName+".yml")

		// CM mounted at /etc/gpload.
		found := false
		for _, m := range c.VolumeMounts {
			if m.MountPath == "/etc/gpload" {
				found = true
			}
		}
		assert.True(s.T(), found, "gpload Job must mount the control-file CM at /etc/gpload")
	})

	s.Run("J.25 scheduled -> CronJob */30", func() {
		job := scenario101GploadCSVJob()
		job.Schedule = cases.Scenario101Schedule
		cron := s.builder.BuildGploadCronJob(cluster, job)
		require.NotNil(s.T(), cron)
		assert.Equal(s.T(), cases.Scenario101Schedule, cron.Spec.Schedule)
		c := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
		require.Len(s.T(), c.Args, 1)
		assert.Contains(s.T(), c.Args[0],
			"gpload -f /etc/gpload/"+cases.Scenario101JobName+".yml")
	})

	s.Run("no schedule -> nil CronJob", func() {
		job := scenario101GploadCSVJob()
		assert.Nil(s.T(), s.builder.BuildGploadCronJob(cluster, job))
	})
}

// ----------------------------------------------------------------------------
// W.18-W.22 + W.16 — webhook admission
// ----------------------------------------------------------------------------

// scenario101GploadJob returns a minimal valid gpload job (the W.* baseline);
// callers mutate the GploadJob field to produce a negative case.
func scenario101GploadJob(name string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    name,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: cases.Scenario101TargetTable,
			FilePaths:   []string{cases.Scenario101FileGlob},
		},
	}
}

// TestFunctional_Scenario101_WebhookAdmission drives the validate path for the
// gpload webhook rules W.18-W.22 (and the W.16 file:// regression): each negative
// case mutates exactly one field and asserts the DENY; the positive baseline +
// the accept variants are admitted.
func (s *Scenario101Suite) TestFunctional_Scenario101_WebhookAdmission() {
	s.Run("baseline gpload job admitted", func() {
		cluster := scenario101GploadCluster("s101-ok", scenario101GploadJob("ok"))
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.18 bad inputSource.type -> DENY", func() {
		job := scenario101GploadJob("w18")
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "ftp"}
		cluster := scenario101GploadCluster("s101-w18", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "inputSource.type")
	})

	s.Run("W.18 type gpfdist/local accepted", func() {
		for _, typ := range []string{"gpfdist", "local"} {
			job := scenario101GploadJob("w18-ok-" + typ)
			job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: typ}
			cluster := scenario101GploadCluster("s101-w18-ok-"+typ, job)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.NoErrorf(s.T(), err, "inputSource.type=%q must be admitted", typ)
		}
	})

	s.Run("W.19 multi-char delimiter -> DENY", func() {
		job := scenario101GploadJob("w19")
		job.GploadJob.Delimiter = ",,"
		cluster := scenario101GploadCluster("s101-w19", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "delimiter")
	})

	s.Run("W.20 update without matchColumns -> DENY", func() {
		for _, mode := range []string{"update", "merge"} {
			job := scenario101GploadJob("w20-" + mode)
			job.GploadJob.Mode = mode
			job.GploadJob.MatchColumns = nil
			cluster := scenario101GploadCluster("s101-w20-"+mode, job)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "mode=%q without matchColumns must be DENIED", mode)
			assert.Contains(s.T(), err.Error(), "matchColumns")
		}
	})

	s.Run("W.20 update with matchColumns accepted", func() {
		job := scenario101GploadJob("w20-ok")
		job.GploadJob.Mode = "update"
		job.GploadJob.MatchColumns = []string{"id"}
		cluster := scenario101GploadCluster("s101-w20-ok", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.21 unsafe postAction -> DENY", func() {
		job := scenario101GploadJob("w21")
		job.GploadJob.PostActions = []string{"ANALYZE public.raw_data; DROP TABLE x"}
		cluster := scenario101GploadCluster("s101-w21", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "postActions")
	})

	s.Run("W.22 host/port on type=local -> DENY", func() {
		job := scenario101GploadJob("w22")
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{
			Type: "local", Host: "files.internal", Port: 8080,
		}
		cluster := scenario101GploadCluster("s101-w22", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "host/port")
	})

	s.Run("W.16 file:// in filePaths -> DENY (regression)", func() {
		job := scenario101GploadJob("w16")
		job.GploadJob.FilePaths = []string{"file:///data/incoming/a.csv"}
		cluster := scenario101GploadCluster("s101-w16", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "file://")
	})
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario101Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario101_CatalogHonest iterates cases.Scenario101Cases() and
// resolves EVERY builder/webhook row against the REAL built artifact: gpfdist
// Deployment/Service/PVC names/ports/selector/ownerRef, the gpload control-file
// GL.1-7 substrings, the ConfigMap + Job pod args, and the W.18-W.22 DENY paths.
// Live rows are logged + skipped (they require a running cluster). This keeps the
// catalog honest against the implementation. gpfdist_* metrics are NEVER asserted.
func (s *Scenario101Suite) TestFunctional_Scenario101_CatalogHonest() {
	catalog := cases.Scenario101Cases()
	require.NotEmpty(s.T(), catalog)

	cluster := testutil.NewClusterBuilder("test-cluster", "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
	}
	job := scenario101GploadCSVJob()
	controlFile, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	dep := s.builder.BuildGpfdistDeployment(cluster)
	svc := s.builder.BuildGpfdistService(cluster)
	pvc := s.builder.BuildGpfdistPVC(cluster)
	gploadJob := s.builder.BuildGploadJob(cluster, job)
	require.NotNil(s.T(), dep)
	require.NotNil(s.T(), svc)
	require.NotNil(s.T(), pvc)
	require.NotNil(s.T(), gploadJob)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario101LayerLive:
				s.T().Logf("scenario101 %s (%s): [LIVE-ONLY] %s — resolved at e2e Part B",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario101LayerWebhook:
				// Resolve the DENY against the validate path.
				s.scenario101ResolveWebhookRow(tc)

			case cases.Scenario101LayerBuilder:
				s.scenario101ResolveBuilderRow(tc, controlFile, dep, svc, pvc, gploadJob, job)

			default:
				s.T().Logf("scenario101 %s: layer %q resolved at envtest", tc.ID, tc.Layer)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario101ResolveWebhookRow resolves a webhook catalog row by exercising the
// matching DENY path through the validate webhook.
func (s *Scenario101Suite) scenario101ResolveWebhookRow(tc cases.Scenario101Case) {
	job := scenario101GploadJob("cat-" + strings.ToLower(strings.ReplaceAll(tc.ID, "-", "")))
	switch tc.Req {
	case "W.18":
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{Type: "ftp"}
	case "W.19":
		job.GploadJob.Delimiter = ",,"
	case "W.20":
		job.GploadJob.Mode = "update"
		job.GploadJob.MatchColumns = nil
	case "W.21":
		job.GploadJob.PostActions = []string{"DROP TABLE x; --"}
	case "W.22":
		job.GploadJob.InputSource = &cbv1alpha1.GploadInputSourceSpec{
			Type: "local", Host: "h", Port: 8080,
		}
	case "W.16":
		job.GploadJob.FilePaths = []string{"file:///a.csv"}
	default:
		s.T().Fatalf("scenario101 %s: unknown webhook req %q", tc.ID, tc.Req)
	}
	cluster := scenario101GploadCluster("s101-cat-"+
		strings.ToLower(strings.ReplaceAll(tc.ID, "-", "")), job)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)
}

// scenario101ResolveBuilderRow resolves a builder catalog row against the
// already-built artifacts (control file / Deployment / Service / PVC / Job).
func (s *Scenario101Suite) scenario101ResolveBuilderRow(
	tc cases.Scenario101Case,
	controlFile string,
	dep *appsv1.Deployment,
	svc *corev1.Service,
	pvc *corev1.PersistentVolumeClaim,
	gploadJob *batchv1.Job,
	job cbv1alpha1.DataLoadingJob,
) {
	switch tc.Artifact {
	case cases.Scenario101ArtifactCronJob:
		// J.25 schedule -> CronJob with spec.schedule == "*/30 * * * *".
		scheduled := job
		scheduled.Schedule = cases.Scenario101Schedule
		cron := s.builder.BuildGploadCronJob(
			testutil.NewClusterBuilder("test-cluster", "default").Build(), scheduled)
		require.NotNil(s.T(), cron)
		assert.Equal(s.T(), cases.Scenario101Schedule, cron.Spec.Schedule)

	case cases.Scenario101ArtifactControlFile:
		if tc.Contains != "" {
			assert.Containsf(s.T(), controlFile, tc.Contains,
				"%s built control file must carry %q", tc.ID, tc.Contains)
		}
		// MODE update/merge rows are proven by the dedicated mode test; here we
		// re-resolve them against a freshly-built control file.
		if tc.Req == "J.36" || tc.Req == "J.37" {
			mode := "update"
			if tc.Req == "J.37" {
				mode = "merge"
			}
			assert.Contains(s.T(), s.scenario101ControlFile(&cbv1alpha1.GploadJobSpec{
				TargetTable:  cases.Scenario101TargetTable,
				FilePaths:    []string{cases.Scenario101FileGlob},
				Mode:         mode,
				MatchColumns: []string{"id"},
			}), "MATCH_COLUMNS")
		}
		if tc.Req == "J.27" {
			assert.NotContains(s.T(), s.scenario101ControlFile(&cbv1alpha1.GploadJobSpec{
				TargetTable: cases.Scenario101TargetTable,
				InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
				FilePaths:   []string{"/data/incoming/*.csv"},
			}), "gpfdist://")
		}

	case cases.Scenario101ArtifactDeployment:
		assert.Equal(s.T(), builder.GpfdistServiceName("test-cluster"), dep.Name)
		assert.Equal(s.T(), util.ComponentGpfdist,
			dep.Spec.Template.ObjectMeta.Labels[util.LabelComponent])

	case cases.Scenario101ArtifactService:
		assert.Equal(s.T(), util.GpfdistServiceName2("test-cluster"), svc.Name)
		assert.Equal(s.T(), util.ComponentGpfdist, svc.Spec.Selector[util.LabelComponent])

	case cases.Scenario101ArtifactPVC:
		assert.Equal(s.T(), util.GpfdistDataPVCName("test-cluster"), pvc.Name)

	case cases.Scenario101ArtifactConfigMap:
		cm := s.builder.BuildGploadControlFileConfigMap(
			testutil.NewClusterBuilder("test-cluster", "default").Build(), job)
		require.NotNil(s.T(), cm)
		assert.Contains(s.T(), cm.Data, job.Name+".yml")

	case cases.Scenario101ArtifactJob:
		require.Len(s.T(), gploadJob.Spec.Template.Spec.Containers, 1)
		require.Len(s.T(), gploadJob.Spec.Template.Spec.Containers[0].Args, 1)
		assert.Contains(s.T(), gploadJob.Spec.Template.Spec.Containers[0].Args[0],
			"gpload -f /etc/gpload/"+cases.Scenario101JobName+".yml")

	default:
		s.T().Logf("scenario101 %s: artifact %q resolved elsewhere", tc.ID, tc.Artifact)
	}
}

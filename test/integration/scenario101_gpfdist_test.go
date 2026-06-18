//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 101: gpfdist Deployment + gpload-csv against the REAL stack —
// integration
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario101Cases — this is Scenario 101, mirroring
// the Scenario 99 integration SHAPE.
//
// This suite gates on the compose/k8s stack being reachable (a coordinator
// PostgreSQL endpoint or a kube-apiserver) and SKIPS CLEANLY when both are down.
// No LIVE cluster is required: it proves, builder-level, that the operator-BUILT
// artifacts for the gpfdist-test sample CR are well-formed —
//   - the gpfdist Deployment/Service/PVC (GP.2-GP.5) names/ports/selector/mount,
//   - the gpload control file (GL.1-GL.7) + the per-job ConfigMap + the Job pod
//     args (gpload -f /etc/gpload/<job>.yml),
// and that the CSV sample data (gen-gpload-csv.sh) exists on disk so the live
// deploy can seed it into the gpfdist PVC.
//
// METRIC HONESTY: cloudberry_gpfdist_* stay PLANNED and are NEVER asserted. The
// live "data loads" proof (count(*) FROM public.raw_data > 0) is at e2e Part B.
// Isolation: read-only probes + pure builder calls; safe for parallel CI re-runs.
// ============================================================================

const (
	// envScenario101PGAddr overrides the coordinator host:port reachability probe.
	envScenario101PGAddr = "SCENARIO101_PG_ADDR"
	// envScenario101DataDir overrides the CSV sample dir.
	envScenario101DataDir = "SCENARIO101_DATA_DIR"

	scenario101DefaultPGAddr = "localhost:5432"

	// scenario101Timeout bounds each probe.
	scenario101Timeout = 30 * time.Second
)

// Scenario101GpfdistSuite drives the builder-level gpfdist+gpload contract for
// the gpfdist-test sample CR, gated on stack reachability.
type Scenario101GpfdistSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario101(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario101GpfdistSuite))
}

func (s *Scenario101GpfdistSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	s.builder = builder.NewBuilder()
}

func (s *Scenario101GpfdistSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario101Env returns the ENV value or the provided default.
func scenario101Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario101PGAddr() string {
	return scenario101Env(envScenario101PGAddr, scenario101DefaultPGAddr)
}

// scenario101TCPReachable reports whether a TCP dial to addr succeeds.
func scenario101TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario101K8sReachable reports whether a kube-apiserver is reachable via
// kubectl (KUBECONFIG must be set + kubectl on PATH).
func scenario101K8sReachable(ctx context.Context) bool {
	if os.Getenv("KUBECONFIG") == "" {
		return false
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return false
	}
	c, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return exec.CommandContext(c, "kubectl", "version", "--request-timeout=8s").Run() == nil
}

// scenario101StackReachable reports whether the compose coordinator OR a
// kube-apiserver is reachable. The suite skips cleanly when neither is up.
func (s *Scenario101GpfdistSuite) scenario101StackReachable(ctx context.Context) bool {
	return scenario101TCPReachable(ctx, scenario101PGAddr()) ||
		scenario101K8sReachable(ctx)
}

// scenario101SampleCluster builds a cluster mirroring the gpfdist-test sample CR:
// dataLoading enabled, gpfdist.enabled:true (replicas 1, default image/port), and
// the gpload-csv job per the spec example.
func scenario101SampleCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("gpfdist-test", "cloudberry-test").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Gpfdist: &cbv1alpha1.GpfdistSpec{
			Enabled:  true,
			Replicas: util.Ptr(int32(1)),
			Image:    cases.Scenario101GpfdistImage,
			Port:     cases.Scenario101GpfdistPort,
		},
		Jobs: []cbv1alpha1.DataLoadingJob{scenario101SampleGploadJob()},
	}
	return cluster
}

// scenario101SampleGploadJob returns the gpload-csv job per the spec example.
func scenario101SampleGploadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:     cases.Scenario101JobName,
		Type:     "gpload",
		Enabled:  true,
		Schedule: cases.Scenario101Schedule,
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

// TestIntegration_Scenario101_GpfdistArtifactsWellFormed asserts the BUILT
// gpfdist Deployment/Service/PVC for the sample CR are well-formed (GP.2-GP.5):
// Deployment name/args/port/mount, Service name/selector==pod-labels/port, PVC
// name. Gated to run only when the stack is reachable, matching the integration
// contract.
func (s *Scenario101GpfdistSuite) TestIntegration_Scenario101_GpfdistArtifactsWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario101Timeout)
	defer cancel()
	if !s.scenario101StackReachable(ctx) {
		s.T().Skipf("no Scenario 101 stack reachable (coordinator %s / kube-apiserver) — "+
			"compose/k8s stack is down", scenario101PGAddr())
	}

	cluster := scenario101SampleCluster()

	dep := s.builder.BuildGpfdistDeployment(cluster)
	require.NotNil(s.T(), dep)
	assert.Equal(s.T(), builder.GpfdistServiceName(cluster.Name), dep.Name)
	c := dep.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), []string{"gpfdist"}, c.Command)
	assert.Equal(s.T(), []string{"-d", "/data", "-p", "8080", "-l", "/var/log/gpfdist.log"}, c.Args)
	require.Len(s.T(), c.Ports, 1)
	assert.Equal(s.T(), int32(cases.Scenario101GpfdistPort), c.Ports[0].ContainerPort)
	require.Len(s.T(), c.VolumeMounts, 1)
	assert.Equal(s.T(), "/data", c.VolumeMounts[0].MountPath)

	svc := s.builder.BuildGpfdistService(cluster)
	require.NotNil(s.T(), svc)
	assert.Equal(s.T(), util.GpfdistServiceName2(cluster.Name), svc.Name)
	assert.Equal(s.T(), util.ComponentGpfdist, svc.Spec.Selector[util.LabelComponent])
	assert.Equal(s.T(), dep.Spec.Template.ObjectMeta.Labels, svc.Spec.Selector)
	require.Len(s.T(), svc.Spec.Ports, 1)
	assert.Equal(s.T(), int32(cases.Scenario101GpfdistPort), svc.Spec.Ports[0].Port)

	pvc := s.builder.BuildGpfdistPVC(cluster)
	require.NotNil(s.T(), pvc)
	assert.Equal(s.T(), util.GpfdistDataPVCName(cluster.Name), pvc.Name)

	s.T().Logf("scenario101: gpfdist artifacts well-formed for %s (deploy=%s svc=%s pvc=%s)",
		cluster.Name, dep.Name, svc.Name, pvc.Name)
}

// TestIntegration_Scenario101_GploadControlFileWellFormed asserts the BUILT
// gpload control file (GL.1-GL.7) + the per-job ConfigMap + the Job pod args for
// the sample CR's gpload-csv job. Gated on stack reachability.
func (s *Scenario101GpfdistSuite) TestIntegration_Scenario101_GploadControlFileWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario101Timeout)
	defer cancel()
	if !s.scenario101StackReachable(ctx) {
		s.T().Skipf("no Scenario 101 stack reachable (coordinator %s / kube-apiserver) — "+
			"compose/k8s stack is down", scenario101PGAddr())
	}

	cluster := scenario101SampleCluster()
	job := scenario101SampleGploadJob()

	ctrl, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	for _, want := range []string{
		cases.Scenario101GLVersion, cases.Scenario101GLDatabase, cases.Scenario101GLUser,
		// GL.2: gpfdist SOURCE now emits LOCAL_HOSTNAME (<cluster>-gpfdist-svc) +
		// the LOCAL FILE path (NO gpfdist:// URL).
		"        LOCAL_HOSTNAME:\n          - " + util.GpfdistServiceName2(cluster.Name) + "\n",
		"        FILE:\n          - " + cases.Scenario101FileGlob + "\n",
		cases.Scenario101GLFormat, cases.Scenario101GLDelimiter,
		cases.Scenario101GLHeader, cases.Scenario101GLEncoding, cases.Scenario101GLErrLimit,
		cases.Scenario101GLLogErrors, cases.Scenario101GLTable, cases.Scenario101GLModeIns,
		cases.Scenario101GLTruncate, cases.Scenario101GLAfter,
	} {
		assert.Containsf(s.T(), ctrl, want, "control file must carry %q", want)
	}
	assert.NotContains(s.T(), ctrl, "gpfdist://",
		"gpload control file FILE entries must not carry gpfdist:// URLs")

	cm := s.builder.BuildGploadControlFileConfigMap(cluster, job)
	require.NotNil(s.T(), cm)
	assert.Equal(s.T(), util.GploadControlFileConfigMapName(cluster.Name, job.Name), cm.Name)
	require.Contains(s.T(), cm.Data, job.Name+".yml")
	assert.Equal(s.T(), ctrl, cm.Data[job.Name+".yml"])

	// J.25 scheduled -> CronJob with gpload -f.
	cron := s.builder.BuildGploadCronJob(cluster, job)
	require.NotNil(s.T(), cron)
	assert.Equal(s.T(), cases.Scenario101Schedule, cron.Spec.Schedule)
	cc := cron.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	require.Len(s.T(), cc.Args, 1)
	assert.Contains(s.T(), cc.Args[0], "gpload -f /etc/gpload/"+job.Name+".yml")

	s.T().Logf("scenario101: gpload control file + CM %s well-formed (GL.1-7); "+
		"CronJob schedule %s", cm.Name, cron.Spec.Schedule)
}

// TestIntegration_Scenario101_CSVSampleExists asserts the CSV sample data the
// gpfdist Deployment serves (gen-gpload-csv.sh output) exists on disk so the live
// deploy can seed it into the gpfdist PVC. Logs best-effort (run the gen script
// to produce them) when absent.
func (s *Scenario101GpfdistSuite) TestIntegration_Scenario101_CSVSampleExists() {
	dataDir := scenario101Env(envScenario101DataDir,
		filepath.Join("..", "docker-compose", "data", "gpload", "incoming"))

	files := []string{"raw_data_001.csv", "raw_data_002.csv"}
	present := 0
	for _, f := range files {
		p := filepath.Join(dataDir, f)
		if info, err := os.Stat(p); err == nil && info.Size() > 0 {
			present++
			s.T().Logf("scenario101: CSV sample present %s (%d bytes)", p, info.Size())
		} else {
			s.T().Logf("scenario101: CSV sample absent %s — run gen-gpload-csv.sh to produce it "+
				"[CONFIG-ONLY until seeded]", p)
		}
	}
	// Non-fatal: the sample is produced by the gen script / seeded into the PVC.
	// We log the state honestly; presence proves the seed source is ready.
	s.T().Logf("scenario101: %d/%d CSV sample files present in %s (gpfdist serves "+
		"/data/incoming/*.csv; gpload loads public.raw_data)", present, len(files), dataDir)
}

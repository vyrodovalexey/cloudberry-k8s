//go:build integration

package integration

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 104: Pre-Load Health Checks against the REAL stack — integration
// ============================================================================
//
// Mirrors the Scenario 101 integration SHAPE. This suite gates on the compose/k8s
// stack being reachable (a coordinator PostgreSQL endpoint, a MinIO endpoint, OR
// a kube-apiserver) and SKIPS CLEANLY when all are down. No LIVE cluster is
// required: it proves, builder-level, that the operator-BUILT artifacts for the
// healthcheck-test sample CR's jobs are well-formed —
//   - the s3-load (pxf) dataload Job carries the dataload-healthcheck init
//     container FIRST with the 5-check script (HC.1 DB-proxy / HC.2 to_regclass /
//     HC.3 curl AWS_S3_ENDPOINT / HC.5 df) and the shared scratch emptyDir
//     mounted at /dataload-scratch on BOTH the init AND the main container, AND
//   - the per-job-type gating: the pxf job's init does NOT carry HC.4
//     (gpfdist-svc), while the gpload data-load path now ALSO carries the
//     dataload-healthcheck init container whose script DOES carry the HC.4
//     gpfdist-svc reachability probe (plus HC.2/HC.5), NOT HC.1/HC.3.
//
// METRIC HONESTY: NO new operator metric. HC failures are observed via the REAL
// cloudberry_data_loading_job_status=3 + errors_total + the
// DataLoadingHealthCheckFailed Event + kube-state-metrics. The live fail+restore
// of each HC is at e2e Part B. Isolation: read-only probes + pure builder calls;
// safe for parallel CI re-runs.
// ============================================================================

const (
	// envScenario104PGAddr overrides the coordinator host:port reachability probe.
	envScenario104PGAddr = "SCENARIO104_PG_ADDR"
	// envScenario104MinioAddr overrides the MinIO host:port reachability probe.
	envScenario104MinioAddr = "SCENARIO104_MINIO_ADDR"

	scenario104DefaultPGAddr    = "localhost:5432"
	scenario104DefaultMinioAddr = "localhost:9000"

	// scenario104Timeout bounds each probe.
	scenario104Timeout = 30 * time.Second
)

// Scenario104HealthCheckSuite drives the builder-level health-check contract for
// the healthcheck-test sample CR, gated on stack reachability.
type Scenario104HealthCheckSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario104(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario104HealthCheckSuite))
}

func (s *Scenario104HealthCheckSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	s.builder = builder.NewBuilder()
}

func (s *Scenario104HealthCheckSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario104Env returns the ENV value or the provided default.
func scenario104Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario104PGAddr() string {
	return scenario104Env(envScenario104PGAddr, scenario104DefaultPGAddr)
}
func scenario104MinioAddr() string {
	return scenario104Env(envScenario104MinioAddr, scenario104DefaultMinioAddr)
}

// scenario104TCPReachable reports whether a TCP dial to addr succeeds.
func scenario104TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario104K8sReachable reports whether a kube-apiserver is reachable via
// kubectl (KUBECONFIG must be set + kubectl on PATH).
func scenario104K8sReachable(ctx context.Context) bool {
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

// scenario104StackReachable reports whether the compose coordinator, the MinIO
// endpoint OR a kube-apiserver is reachable. The suite skips cleanly when all
// are down.
func (s *Scenario104HealthCheckSuite) scenario104StackReachable(ctx context.Context) bool {
	return scenario104TCPReachable(ctx, scenario104PGAddr()) ||
		scenario104TCPReachable(ctx, scenario104MinioAddr()) ||
		scenario104K8sReachable(ctx)
}

// scenario104SampleCluster builds a cluster mirroring the healthcheck-test sample
// CR: dataLoading enabled, pxf.enabled + gpfdist.enabled, the healthChecks block
// (default values), an s3 backup destination (HC.3 creds env), and the s3-load
// pxf job + gpload-csv gpload job.
func scenario104SampleCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(cases.Scenario104ClusterName, cases.Scenario104Namespace).Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf:     &cbv1alpha1.PxfSpec{Enabled: true, Image: "cloudberry-pxf:2.1.0"},
		Gpfdist: &cbv1alpha1.GpfdistSpec{
			Enabled: true,
			Image:   "cloudberry-gpfdist:2.1.0",
			Port:    cases.Scenario104GpfdistPort,
		},
		HealthChecks: &cbv1alpha1.DataLoadHealthChecksSpec{
			DiskMinFreeMB:    cases.Scenario104DiskMinFreeMB,
			ScratchSizeLimit: "64Mi",
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			scenario104SamplePxfJob(),
			scenario104SampleGploadJob(),
		},
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

// scenario104SamplePxfJob returns the s3-load pxf job per the sample CR §5.
func scenario104SamplePxfJob() cbv1alpha1.DataLoadingJob {
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

// scenario104SampleGploadJob returns the gpload-csv gpload job per the sample CR §5.
func scenario104SampleGploadJob() cbv1alpha1.DataLoadingJob {
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

// scenario104InitOf returns the FIRST init container of a built Job (or nil).
func scenario104InitOf(job *batchv1.Job) *corev1.Container {
	inits := job.Spec.Template.Spec.InitContainers
	if len(inits) == 0 {
		return nil
	}
	return &inits[0]
}

// scenario104MainOf returns the main workload container of a built Job (or nil).
func scenario104MainOf(job *batchv1.Job) *corev1.Container {
	c := job.Spec.Template.Spec.Containers
	if len(c) == 0 {
		return nil
	}
	return &c[0]
}

// scenario104Mounts reports whether the container mounts the given path.
func scenario104Mounts(c *corev1.Container, path string) bool {
	for i := range c.VolumeMounts {
		if c.VolumeMounts[i].MountPath == path {
			return true
		}
	}
	return false
}

// scenario104HasVolume reports whether the pod spec carries the named volume.
func scenario104HasVolume(job *batchv1.Job, name string) bool {
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == name {
			return true
		}
	}
	return false
}

// TestIntegration_Scenario104_PxfInitWellFormed asserts the BUILT s3-load pxf
// dataload Job carries the dataload-healthcheck init container FIRST with the
// 5-check script substrings (HC.1/HC.2/HC.3/HC.5, no HC.4) and the scratch
// volume mounted at /dataload-scratch on BOTH containers. Gated on stack
// reachability.
func (s *Scenario104HealthCheckSuite) TestIntegration_Scenario104_PxfInitWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario104Timeout)
	defer cancel()
	if !s.scenario104StackReachable(ctx) {
		s.T().Skipf("no Scenario 104 stack reachable (coordinator %s / MinIO %s / kube-apiserver) "+
			"— compose/k8s stack is down", scenario104PGAddr(), scenario104MinioAddr())
	}

	cluster := scenario104SampleCluster()
	out := s.builder.BuildDataLoadJob(cluster, scenario104SamplePxfJob())
	require.NotNil(s.T(), out)

	init := scenario104InitOf(out)
	require.NotNil(s.T(), init, "s3-load pxf Job must carry the health-check init container")
	assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
	assert.Equal(s.T(), []string{"/bin/bash", "-c"}, init.Command)
	require.Len(s.T(), init.Args, 1)
	script := init.Args[0]

	for _, want := range []string{
		"pg_extension", "pxf_version()", "HC.1 FAIL", // HC.1 DB-proxy
		"to_regclass('${tbl}')", "HC.2 FAIL", // HC.2 target table
		`curl -fsS -m 10 --head "${AWS_S3_ENDPOINT}"`, "HC.3 FAIL", // HC.3 s3
		"df -Pk " + cases.Scenario104ScratchMount, "HC.5 FAIL", // HC.5 disk
	} {
		assert.Containsf(s.T(), script, want, "pxf init script must carry %q", want)
	}
	assert.NotContains(s.T(), script, "gpfdist-svc",
		"HC.4 is gpload-only; the pxf init must not carry the gpfdist-svc probe")
	assert.NotContains(s.T(), script, "actuator/health",
		"HC.1 is a DB proxy, NOT a direct sidecar curl")

	// Scratch volume + mounts on BOTH containers.
	assert.True(s.T(), scenario104HasVolume(out, cases.Scenario104ScratchVolume),
		"scratch emptyDir must be present")
	main := scenario104MainOf(out)
	require.NotNil(s.T(), main)
	assert.True(s.T(), scenario104Mounts(init, cases.Scenario104ScratchMount),
		"init must mount /dataload-scratch")
	assert.True(s.T(), scenario104Mounts(main, cases.Scenario104ScratchMount),
		"main must mount /dataload-scratch")

	s.T().Logf("scenario104: s3-load pxf init well-formed (init=%s; HC.1/2/3/5 present, no HC.4; "+
		"scratch %s mounted on both containers)", init.Name, cases.Scenario104ScratchVolume)
}

// TestIntegration_Scenario104_GploadGating asserts the per-job-type routing for
// the gpload-csv job DIRECTLY: the gpload data-load path now ALSO carries the
// dataload-healthcheck init container, whose script carries HC.2/HC.4/HC.5 (the
// HC.4 gpfdist-svc reachability probe is the gpload-specific check) but NOT HC.1
// (pxf-only) or HC.3 (object-store-only). Gated on stack reachability.
func (s *Scenario104HealthCheckSuite) TestIntegration_Scenario104_GploadGating() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario104Timeout)
	defer cancel()
	if !s.scenario104StackReachable(ctx) {
		s.T().Skipf("no Scenario 104 stack reachable (coordinator %s / MinIO %s / kube-apiserver) "+
			"— compose/k8s stack is down", scenario104PGAddr(), scenario104MinioAddr())
	}

	cluster := scenario104SampleCluster()
	out := s.builder.BuildDataLoadJob(cluster, scenario104SampleGploadJob())
	require.NotNil(s.T(), out)

	init := scenario104InitOf(out)
	require.NotNil(s.T(), init, "the gpload data-load Job must carry the health-check init container")
	assert.Equal(s.T(), cases.Scenario104InitName, init.Name)
	require.Len(s.T(), init.Args, 1)
	script := init.Args[0]

	// HC.4 gpfdist-svc reachability — the gpload-specific check.
	assert.Contains(s.T(), script, "gpfdist-svc",
		"the gpload init must curl the gpfdist Service (HC.4 is gpload-only)")
	assert.Contains(s.T(), script, "HC.4 FAIL")
	// HC.2 + HC.5 run on the gpload init too.
	assert.Contains(s.T(), script, "to_regclass('${tbl}')")
	assert.Contains(s.T(), script, "HC.2 FAIL")
	assert.Contains(s.T(), script, "df -Pk "+cases.Scenario104ScratchMount)
	assert.Contains(s.T(), script, "HC.5 FAIL")
	// HC.1 (pxf-only) + HC.3 (object-store-only) are gated OFF for a gpload job.
	assert.NotContains(s.T(), script, "HC.1 FAIL")
	assert.NotContains(s.T(), script, "pxf_version()")
	assert.NotContains(s.T(), script, "HC.3 FAIL")

	// Scratch volume + mounts on BOTH containers (HC.5).
	assert.True(s.T(), scenario104HasVolume(out, cases.Scenario104ScratchVolume),
		"gpload scratch emptyDir must be present")
	main := scenario104MainOf(out)
	require.NotNil(s.T(), main)
	assert.True(s.T(), scenario104Mounts(init, cases.Scenario104ScratchMount),
		"gpload init must mount /dataload-scratch")
	assert.True(s.T(), scenario104Mounts(main, cases.Scenario104ScratchMount),
		"gpload container must mount /dataload-scratch")

	s.T().Logf("scenario104: gpload-csv init well-formed (init=%s; HC.2/4/5 present, no HC.1/HC.3; "+
		"scratch %s mounted on both containers)", init.Name, cases.Scenario104ScratchVolume)
}

// TestIntegration_Scenario104_KnobDisabled asserts the sample CR with
// healthChecks.enabled=false omits the init container + scratch volume entirely.
// Gated on stack reachability.
func (s *Scenario104HealthCheckSuite) TestIntegration_Scenario104_KnobDisabled() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario104Timeout)
	defer cancel()
	if !s.scenario104StackReachable(ctx) {
		s.T().Skipf("no Scenario 104 stack reachable — compose/k8s stack is down")
	}

	cluster := scenario104SampleCluster()
	disabled := false
	cluster.Spec.DataLoading.HealthChecks.Enabled = &disabled

	out := s.builder.BuildDataLoadJob(cluster, scenario104SamplePxfJob())
	require.NotNil(s.T(), out)
	assert.Empty(s.T(), out.Spec.Template.Spec.InitContainers,
		"no init container when healthChecks.enabled=false")
	assert.False(s.T(), scenario104HasVolume(out, cases.Scenario104ScratchVolume),
		"no scratch volume when disabled")
}

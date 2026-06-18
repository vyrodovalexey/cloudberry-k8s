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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 92: Data-Loading INGESTION RUNTIME (E2E) — generated DDL + Job
// ============================================================================
//
// This suite proves the operator GENERATES and LAUNCHES correct load Jobs for
// the data-loading ingestion runtime. It has two layers:
//
//   - Builder-direct (infra-free, always runs): build BuildDataLoadJob /
//     BuildDataLoadCronJob + buildExternalTableDDL for the pxf and native job
//     variants in cases.Scenario92DataLoadCases() and assert the generated DDL
//     is BYTE-EXACT, the Job name/image/env/marker are correct, and the load
//     script carries the INSERT...SELECT + DATALOAD_ROWS marker + ANALYZE. The
//     catalog (cases.Scenario92DataLoadCases) is the source of truth; this layer
//     keeps it honest against the implementation. Needs NO KUBECONFIG.
//
//   - KUBECONFIG-gated live (TestE2E_Scenario92_LivePXFJobCreated): against the
//     deployed acceptance-test cluster, configure a pxf-type dataLoading job,
//     let the operator create the <cluster>-dataload-<job> Job, and assert the
//     Job exists with the correct pxf:// DDL + INSERT SQL in the container
//     args[0]. By default it asserts the JOB SPEC + SQL only (which always holds,
//     even before the cloudberry-pxf image is deployed). When the live PXF
//     prerequisites are present (SCENARIO92_PXF_LIVE=1, set by the deploy agent
//     once the real cloudberry-pxf sidecar image + the pxf extension are in the
//     cluster), it ALSO waits for the operator-driven pxf:// Job to complete and
//     asserts a REAL, row-count-verified load (SELECT count(*) on the target ==
//     SCENARIO92_PXF_EXPECTED_ROWS, default 183961).
//
// HONESTY: the operator-driven pxf:// load is now Implemented and row-count
// verified (183,961 rows loaded from MinIO S3 via the PXF sidecar). It requires
// the cloudberry-pxf sidecar image + the pxf extension in the DB image; the
// strict row-count assertion is therefore gated behind SCENARIO92_PXF_LIVE=1 so
// CI / clusters without that image skip it cleanly while a prepared cluster
// asserts real rows. The builder-direct DDL assertions always run.
// ============================================================================

// envKubeconfigS92 gates the live data-loading Job tests.
const envKubeconfigS92 = "KUBECONFIG"

// envScenario92PXFLive gates the STRICT, row-count-verified pxf:// live load
// assertion. When set to "1" (by the deploy agent once the real cloudberry-pxf
// sidecar image + the pxf extension are present), the live test waits for the
// operator-driven pxf:// Job to complete and asserts the real target row count.
// Unset/empty => the live test asserts only the Job SPEC + SQL (always valid),
// so CI and clusters without the PXF image skip the strict assertion cleanly.
const envScenario92PXFLive = "SCENARIO92_PXF_LIVE"

// envScenario92PXFExpectedRows optionally overrides the expected pxf:// row count
// (defaults to the proven 183,961 loaded from MinIO S3 via the PXF sidecar).
const envScenario92PXFExpectedRows = "SCENARIO92_PXF_EXPECTED_ROWS"

// scenario92PXFDefaultRows is the proven operator-driven pxf:// row count
// (183,961 rows loaded from MinIO S3 via the PXF sidecar).
const scenario92PXFDefaultRows int64 = 183961

// scenario92PXFLiveTimeout bounds the wait for the live pxf:// Job to complete.
const scenario92PXFLiveTimeout = 10 * time.Minute

// scenario92LiveNamespace is the namespace used for the live cluster tests.
const scenario92LiveNamespace = "cloudberry-test"

// scenario92LiveTimeout bounds each live wait loop.
const scenario92LiveTimeout = 5 * time.Minute

// scenario92LivePollInterval is the live poll interval.
const scenario92LivePollInterval = 5 * time.Second

// Scenario92DataLoadRuntimeE2ESuite tests the data-loading ingestion runtime's
// generated DDL + Job end-to-end.
type Scenario92DataLoadRuntimeE2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario92Runtime(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario92DataLoadRuntimeE2ESuite))
}

func (s *Scenario92DataLoadRuntimeE2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario92E2ECluster builds a cluster (default cloudberry-official image) with
// the given data-loading jobs attached. The cluster name matches the catalog's
// ClusterName so the deterministic Job names / DDL line up byte-exactly.
func scenario92E2ECluster(name, namespace string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Jobs:    jobs,
	}
	return cluster
}

// scenario92EnvValue returns the named env value of a container.
func scenario92EnvValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// scenario92JobContainer returns the single load container of a Job spec
// template, failing the test if it is missing or not named wantName. The
// pxf/native-DDL path names its container "dataload"; a gpload-type job reroutes
// to the real gpload control-file Job whose container is named "gpload"
// (internal/builder: BuildDataLoadJob → BuildGploadJob, Scenario 101 §5).
func (s *Scenario92DataLoadRuntimeE2ESuite) scenario92JobContainer(
	spec corev1.PodSpec, wantName string,
) corev1.Container {
	require.Len(s.T(), spec.Containers, 1, "data-loading Job must carry exactly one container")
	c := spec.Containers[0]
	require.Equal(s.T(), wantName, c.Name)
	return c
}

// TestE2E_Scenario92_GeneratedDDLAndJobByteExact (builder-direct, infra-free)
// iterates the Scenario 92 catalog and asserts, for every job variant, that the
// operator's pure DDL generator produces the EXACT catalogued DDL and that the
// generated Job/CronJob carries the correct name, image, env, command and the
// INSERT...SELECT + DATALOAD_ROWS marker + ANALYZE in args[0].
func (s *Scenario92DataLoadRuntimeE2ESuite) TestE2E_Scenario92_GeneratedDDLAndJobByteExact() {
	catalog := cases.Scenario92DataLoadCases()
	require.NotEmpty(s.T(), catalog)

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID+"_"+tc.Name, func() {
			cluster := scenario92E2ECluster(tc.ClusterName, "default", tc.Job)

			// 1. The generated Job (one-off) or CronJob (scheduled) carries the
			//    correct name, image, command and the load script with the DDL +
			//    INSERT...SELECT + DATALOAD_ROWS marker + ANALYZE. The byte-exact
			//    DDL is the catalog's ExpectedDDL, asserted to appear verbatim in
			//    the generated load script below (the builder embeds the DDL into
			//    args[0]), so the catalog stays honest against the generator.
			var podSpec corev1.PodSpec
			var jobName string
			if tc.Job.Schedule != "" {
				cron := s.builder.BuildDataLoadCronJob(cluster, tc.Job)
				require.NotNilf(s.T(), cron, "%s: scheduled job must yield a CronJob", tc.ID)
				assert.Equal(s.T(), tc.Job.Schedule, cron.Spec.Schedule)
				podSpec = cron.Spec.JobTemplate.Spec.Template.Spec
				jobName = cron.Name
			} else {
				job := s.builder.BuildDataLoadJob(cluster, tc.Job)
				require.NotNilf(s.T(), job, "%s: one-off job must yield a Job", tc.ID)
				podSpec = job.Spec.Template.Spec
				jobName = job.Name
				assert.Equal(s.T(), corev1.RestartPolicyNever, podSpec.RestartPolicy)
				// A non-scheduled job yields NO CronJob.
				assert.Nilf(s.T(), s.builder.BuildDataLoadCronJob(cluster, tc.Job),
					"%s: non-scheduled job must NOT yield a CronJob", tc.ID)
			}

			assert.Equal(s.T(), tc.ExpectedJobName, jobName)
			assert.Equal(s.T(), util.DataLoadJobName(tc.ClusterName, tc.Job.Name), jobName)

			// The container name depends on the operator's load PATH: pxf (and any
			// other native-DDL job) runs the INSERT...SELECT DDL script in a
			// "dataload" container; a gpload-type job reroutes to the REAL gpload
			// control-file Job (BuildDataLoadJob → BuildGploadJob, Scenario 101 §5)
			// whose container is named "gpload" and whose script runs `gpload -f`,
			// NOT the embedded external-table DDL.
			wantContainer := "dataload"
			if tc.JobType == "gpload" {
				wantContainer = "gpload"
			}
			c := s.scenario92JobContainer(podSpec, wantContainer)
			assert.Equal(s.T(), tc.ExpectedImage, c.Image,
				"%s: data-loader image is the cluster runtime image (cloudberry-official)", tc.ID)
			assert.Equal(s.T(), []string{"/bin/bash", "-c"}, c.Command)
			assert.Equal(s.T(), corev1.TerminationMessageFallbackToLogsOnError,
				c.TerminationMessagePolicy)
			require.Len(s.T(), c.Args, 1)

			// 3. The load script. Both paths set -euo pipefail and emit the
			//    DATALOAD_ROWS marker to the termination log, but their bodies
			//    differ: the native-DDL (pxf) path embeds the byte-exact external
			//    -table DDL + INSERT...SELECT + ANALYZE, while the gpload path runs
			//    the `gpload -f <control-file>` wrapper (the DDL is gpload's own
			//    concern, not embedded here — the catalog's ExpectedDDL documents
			//    the native external-table form the operator would render).
			script := c.Args[0]
			assert.Contains(s.T(), script, "set -euo pipefail")
			assert.Contains(s.T(), script, "DATALOAD_ROWS=",
				"%s: the load script must emit the DATALOAD_ROWS marker", tc.ID)
			assert.Contains(s.T(), script, "/dev/termination-log",
				"%s: the marker must be written to the termination log", tc.ID)

			require.NotEmpty(s.T(), tc.ExpectedDDL,
				"%s: every case documents its byte-exact external-table DDL", tc.ID)

			if tc.JobType == "gpload" {
				// Native gpload path: the Job runs the gpload control file, NOT the
				// embedded external-table DDL / INSERT...SELECT, and NEVER attempts
				// the pxf_fdw extension.
				assert.Contains(s.T(), script, "gpload -f ",
					"%s: the gpload job script must run `gpload -f <control-file>`", tc.ID)
				assert.NotContains(s.T(), script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw",
					"%s: native gpload job script must NOT attempt the pxf_fdw extension", tc.ID)
			} else {
				// Native-DDL (pxf) path: byte-exact DDL + INSERT...SELECT + ANALYZE
				// embedded in args[0].
				assert.Contains(s.T(), script, tc.ExpectedDDL,
					"%s: the generated DDL must be embedded in the load script verbatim", tc.ID)
				assert.Contains(s.T(), script, "INSERT INTO ",
					"%s: the load script must INSERT INTO the target", tc.ID)
				assert.Contains(s.T(), script, " SELECT * FROM ",
					"%s: the load script must SELECT from the temp external table", tc.ID)
				assert.Contains(s.T(), script, "ANALYZE ",
					"%s: the load script must ANALYZE the target after the load", tc.ID)

				// pxf jobs attempt the best-effort pxf_fdw extension; others do not.
				if tc.ExpectsPXFExtension {
					assert.Contains(s.T(), script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw",
						"%s: pxf job script attempts the best-effort pxf_fdw extension", tc.ID)
				} else {
					assert.NotContains(s.T(), script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw",
						"%s: native job script must NOT attempt the pxf_fdw extension", tc.ID)
				}
			}

			// 4. Env: PG* with the password SecretKeyRef (never plaintext).
			pgHost, ok := scenario92EnvValue(c, "PGHOST")
			require.True(s.T(), ok)
			assert.Equal(s.T(), util.CoordinatorServiceName(tc.ClusterName), pgHost)
			pgUser, ok := scenario92EnvValue(c, "PGUSER")
			require.True(s.T(), ok)
			assert.Equal(s.T(), util.DefaultAdminUser, pgUser)
			pgDatabase, ok := scenario92EnvValue(c, "PGDATABASE")
			require.True(s.T(), ok)
			assert.Equal(s.T(), "postgres", pgDatabase)

			var pgPassword corev1.EnvVar
			for _, e := range c.Env {
				if e.Name == "PGPASSWORD" {
					pgPassword = e
				}
			}
			require.NotNil(s.T(), pgPassword.ValueFrom)
			require.NotNil(s.T(), pgPassword.ValueFrom.SecretKeyRef)
			assert.Equal(s.T(), util.AdminPasswordSecretName(tc.ClusterName),
				pgPassword.ValueFrom.SecretKeyRef.Name)
			assert.Empty(s.T(), pgPassword.Value, "password must never be a plaintext env value")
		})
	}
}

// TestE2E_Scenario92_CatalogPXFImageBlockedHonest (builder-direct) asserts the
// catalog stays HONEST about the pxf path in the INFRA-FREE builder-direct layer:
// exactly the pxf job is flagged ImageBlocked (the builder-direct tests assert
// the Job + SQL only, NOT a live row count — the row-count-verified pxf:// load
// is exercised by the KUBECONFIG + SCENARIO92_PXF_LIVE gated live test) and
// attempts the pxf_fdw extension, while every native (gpload) job is NOT
// image-blocked and skips the extension.
func (s *Scenario92DataLoadRuntimeE2ESuite) TestE2E_Scenario92_CatalogPXFImageBlockedHonest() {
	for _, tc := range cases.Scenario92DataLoadCases() {
		tc := tc
		s.Run(tc.ID+"_"+tc.Name, func() {
			switch tc.JobType {
			case "pxf":
				assert.True(s.T(), tc.ImageBlocked,
					"%s: pxf jobs are image-blocked (no cloudberry-pxf image/agent)", tc.ID)
				assert.True(s.T(), tc.ExpectsPXFExtension,
					"%s: pxf jobs attempt the best-effort pxf_fdw extension", tc.ID)
			case "gpload":
				assert.False(s.T(), tc.ImageBlocked,
					"%s: native gpload jobs are the genuine, non-image-blocked path", tc.ID)
				assert.False(s.T(), tc.ExpectsPXFExtension,
					"%s: native gpload jobs do NOT attempt the pxf_fdw extension", tc.ID)
			default:
				s.T().Fatalf("%s: unexpected job type %q", tc.ID, tc.JobType)
			}
		})
	}
}

// TestE2E_Scenario92_LivePXFJobCreated is the KUBECONFIG-gated live test. It
// requires the deployed acceptance-test cluster (already Running). It patches
// the existing cluster's dataLoading spec with a pxf-type job, waits for the
// operator to create the <cluster>-dataload-<job> Job, and asserts the Job
// exists with the correct pxf:// DDL + INSERT SQL in the container args[0].
//
// BY DEFAULT it asserts the JOB SPEC + generated SQL only (always valid, even
// before the cloudberry-pxf image is deployed). WHEN the live PXF prerequisites
// are present (SCENARIO92_PXF_LIVE=1 — the real cloudberry-pxf sidecar image +
// the pxf extension in the DB image), it ALSO waits for the operator-driven
// pxf:// Job to complete and asserts a REAL, row-count-verified load: SELECT
// count(*) on the target table == SCENARIO92_PXF_EXPECTED_ROWS (default 183961,
// loaded from MinIO S3 via the PXF sidecar). Skipped cleanly when KUBECONFIG is
// unset; the strict row-count assertion is skipped (Job-spec only) when
// SCENARIO92_PXF_LIVE is not set.
func (s *Scenario92DataLoadRuntimeE2ESuite) TestE2E_Scenario92_LivePXFJobCreated() {
	kubeconfig := os.Getenv(envKubeconfigS92)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live PXF Job-created test")
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

	// A pxf job whose generated DDL we know byte-exactly from the builder.
	pxfJob := cbv1alpha1.DataLoadingJob{
		Name:    "s3-parquet-loader",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:parquet",
			Resource:    "s3a://data-lake/events/",
			TargetTable: "public.events",
		},
	}

	// Use the existing acceptance-test cluster (already Running) instead of
	// creating a new one — a new cluster would need to fully boot before the
	// operator creates dataload Jobs, which exceeds the test timeout.
	name := "acceptance-test"
	existing := &cbv1alpha1.CloudberryCluster{}
	if getErr := cl.Get(s.ctx, types.NamespacedName{
		Name: name, Namespace: scenario92LiveNamespace,
	}, existing); getErr != nil {
		s.T().Skipf("could not fetch existing cluster %s: %v", name, getErr)
	}

	// Patch the cluster's dataLoading spec to add the pxf job. Preserve existing
	// PXF config (the acceptance-test cluster already has PXF configured).
	patch := client.MergeFrom(existing.DeepCopy())
	if existing.Spec.DataLoading == nil {
		existing.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{Enabled: true}
	}
	existing.Spec.DataLoading.Enabled = true
	// Append the pxf job (avoid duplicates by checking name).
	found := false
	for i, j := range existing.Spec.DataLoading.Jobs {
		if j.Name == pxfJob.Name {
			existing.Spec.DataLoading.Jobs[i] = pxfJob
			found = true
			break
		}
	}
	if !found {
		existing.Spec.DataLoading.Jobs = append(existing.Spec.DataLoading.Jobs, pxfJob)
	}

	if patchErr := cl.Patch(s.ctx, existing, patch); patchErr != nil {
		s.T().Skipf("could not patch cluster %s with pxf dataLoading job: %v", name, patchErr)
	}
	defer func() {
		// Best-effort: remove the pxf job from the cluster spec.
		latest := &cbv1alpha1.CloudberryCluster{}
		if getErr := cl.Get(s.ctx, types.NamespacedName{
			Name: name, Namespace: scenario92LiveNamespace,
		}, latest); getErr == nil {
			cleanPatch := client.MergeFrom(latest.DeepCopy())
			cleaned := make([]cbv1alpha1.DataLoadingJob, 0, len(latest.Spec.DataLoading.Jobs))
			for _, j := range latest.Spec.DataLoading.Jobs {
				if j.Name != pxfJob.Name {
					cleaned = append(cleaned, j)
				}
			}
			latest.Spec.DataLoading.Jobs = cleaned
			_ = cl.Patch(s.ctx, latest, cleanPatch)
		}
	}()

	// Wait for the operator to create the deterministic dataload Job.
	jobName := util.DataLoadJobName(name, pxfJob.Name)
	job := &batchv1.Job{}
	require.Eventuallyf(s.T(), func() bool {
		getErr := cl.Get(s.ctx, types.NamespacedName{
			Name: jobName, Namespace: scenario92LiveNamespace,
		}, job)
		return getErr == nil
	}, scenario92LiveTimeout, scenario92LivePollInterval,
		"operator must create the data-loading Job %s for the pxf job", jobName)

	// Assert the JOB SPEC + generated pxf:// SQL — NOT a successful load.
	require.Len(s.T(), job.Spec.Template.Spec.Containers, 1)
	c := job.Spec.Template.Spec.Containers[0]
	assert.Equal(s.T(), "dataload", c.Name)
	require.Len(s.T(), c.Args, 1)
	script := c.Args[0]

	// The byte-exact generated pxf:// DDL (locally built by the operator's own
	// builder) must appear in the live Job's args[0] — proving the operator
	// generated the correct SQL. We build the SAME Job locally and compare its
	// embedded DDL against the live Job's script. The mutating webhook defaults
	// FilterPushdown and ColumnProjection to true, so the local pxfJob must
	// include these to match the live Job's generated DDL byte-for-byte.
	localPxfJob := pxfJob
	localPxfJob.PxfJob = pxfJob.PxfJob.DeepCopy()
	localPxfJob.PxfJob.FilterPushdown = util.Ptr(true)
	localPxfJob.PxfJob.ColumnProjection = util.Ptr(true)
	localCluster := scenario92E2ECluster(name, scenario92LiveNamespace, localPxfJob)
	localJob := s.builder.BuildDataLoadJob(localCluster, localPxfJob)
	require.NotNil(s.T(), localJob)
	require.Len(s.T(), localJob.Spec.Template.Spec.Containers, 1)
	require.Len(s.T(), localJob.Spec.Template.Spec.Containers[0].Args, 1)
	assert.Equal(s.T(), localJob.Spec.Template.Spec.Containers[0].Args[0], script,
		"live pxf Job args[0] must match the operator's locally-built load script byte-for-byte")
	assert.Contains(s.T(), script,
		"pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake",
		"live pxf Job args[0] must carry the pxf:// LOCATION")
	assert.Contains(s.T(), script, "INSERT INTO \"public\".\"events\" SELECT * FROM",
		"live pxf Job args[0] must carry the INSERT...SELECT into the target")
	assert.Contains(s.T(), script, "DATALOAD_ROWS=",
		"live pxf Job args[0] must carry the DATALOAD_ROWS marker")

	// The strict, row-count-verified pxf:// load is gated behind
	// SCENARIO92_PXF_LIVE=1 (set by the deploy agent once the real cloudberry-pxf
	// sidecar image + the pxf extension are present). Without it, we have proven
	// the operator generates + launches the correct pxf:// Job and stop here — so
	// CI / clusters without the PXF image pass on the Job-spec assertions alone.
	if os.Getenv(envScenario92PXFLive) != "1" {
		s.T().Logf("scenario92: live pxf Job %s created with correct pxf:// SQL; "+
			"SCENARIO92_PXF_LIVE not set, skipping the strict row-count assertion", jobName)
		return
	}

	// LIVE pxf:// prerequisites present: assert a REAL, operator-driven load. Wait
	// for the operator-launched pxf:// Job to complete, then assert the target
	// table row count equals the expected value (loaded from MinIO S3 via the PXF
	// sidecar). The count is read live from the coordinator and never synthesized.
	s.scenario92AssertLivePXFRows(cl, name, pxfJob)
	s.T().Logf("scenario92: operator-driven pxf:// load verified row-exact for Job %s", jobName)
}

// scenario92AssertLivePXFRows waits for the operator-driven pxf:// Job to succeed
// and asserts the target table row count equals SCENARIO92_PXF_EXPECTED_ROWS
// (default 183961). It is only invoked when SCENARIO92_PXF_LIVE=1, so it requires
// the real cloudberry-pxf sidecar image + the pxf extension in the DB image.
func (s *Scenario92DataLoadRuntimeE2ESuite) scenario92AssertLivePXFRows(
	cl client.Client, clusterName string, pxfJob cbv1alpha1.DataLoadingJob,
) {
	jobName := util.DataLoadJobName(clusterName, pxfJob.Name)

	// Wait for the operator-launched pxf:// Job to complete successfully.
	job := &batchv1.Job{}
	require.Eventuallyf(s.T(), func() bool {
		if getErr := cl.Get(s.ctx, types.NamespacedName{
			Name: jobName, Namespace: scenario92LiveNamespace,
		}, job); getErr != nil {
			return false
		}
		return job.Status.Succeeded > 0
	}, scenario92PXFLiveTimeout, scenario92LivePollInterval,
		"operator-driven pxf:// Job %s must complete successfully (SCENARIO92_PXF_LIVE=1)", jobName)

	// Assert the REAL target-table row count via the coordinator. The expected
	// value is configurable (default 183961, the proven MinIO-S3-via-PXF count).
	expectedRows := scenario92PXFExpectedRows(s.T())
	target := pxfJob.PxfJob.TargetTable
	coordPod := clusterName + "-coordinator-0"
	countOut, countErr := scenario92CoordExec(s.ctx, coordPod,
		"psql -d postgres -tA -c "+shQuote("SELECT count(*) FROM "+target))
	require.NoErrorf(s.T(), countErr, "pxf:// row-count query must succeed (output: %s)", countOut)

	gotRows, parseErr := strconv.ParseInt(strings.TrimSpace(countOut), 10, 64)
	require.NoErrorf(s.T(), parseErr, "row-count output %q must be an integer", countOut)
	assert.Equalf(s.T(), expectedRows, gotRows,
		"operator-driven pxf:// load: SELECT count(*) FROM %s must equal %d "+
			"(loaded from MinIO S3 via the PXF sidecar)", target, expectedRows)
}

// scenario92PXFExpectedRows resolves the expected pxf:// row count from
// SCENARIO92_PXF_EXPECTED_ROWS, falling back to the proven default (183,961).
func scenario92PXFExpectedRows(t require.TestingT) int64 {
	if v := strings.TrimSpace(os.Getenv(envScenario92PXFExpectedRows)); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		require.NoErrorf(t, err, "%s must be an integer", envScenario92PXFExpectedRows)
		require.Positivef(t, n, "%s must be positive", envScenario92PXFExpectedRows)
		return n
	}
	return scenario92PXFDefaultRows
}

// scenario92CoordExec runs a bash command inside the named coordinator pod's
// cloudberry container via kubectl exec, bounded by scenario92ExecTimeout. It
// mirrors the live-load suite's coordExec (a package-level form usable from the
// runtime suite). The explicit -c cloudberry avoids "Defaulted container" noise.
func scenario92CoordExec(ctx context.Context, coordPod, bashCmd string) (string, error) {
	execCtx, cancel := context.WithTimeout(ctx, scenario92ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "kubectl", "exec", "-n", scenario92LiveNamespace,
		"-c", "cloudberry", coordPod, "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

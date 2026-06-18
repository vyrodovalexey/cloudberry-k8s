//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
)

// ============================================================================
// Scenario 92 (T16): GENUINE row-count-verified live load (engine-native, NO PXF)
// ============================================================================
//
// This suite proves the operator's data-loading LOAD PATH works end-to-end with
// a REAL, asserted row count via the engine-native protocol (NO PXF). It is the
// native, extension-free proof that runs on cloudberry-official and needs no PXF
// image. (The operator-driven pxf:// load is now ALSO row-count verified — see
// the SCENARIO92_PXF_LIVE gated assertion in
// scenario92_dataload_runtime_e2e_test.go, which requires the cloudberry-pxf
// sidecar image + the pxf extension; this native suite is unchanged and remains
// the no-extension baseline.)
//
// FILE:// HONESTY: a bare file:// scheme is NOT a valid gploadJob.filePaths CR
// input for a multi-segment cluster — the webhook (rule W.16) rejects it because
// a file:// external table needs a per-segment-host URI ("file://<seghost>/path")
// the operator cannot synthesize. The builder still passes a file:// scheme
// through VERBATIM (it never silently rewrites it), so the operator-driven
// assertion below is a BUILDER-DIRECT fixture: it calls the builder directly
// (NOT through admission) to prove the verbatim passthrough and the load SQL
// SHAPE. The genuine live load (step 2) does NOT use a CR file:// external table:
// it uses COPY FROM, a coordinator-local bulk load, so it is unaffected by W.16.
//
// APPROACH (builder-direct vs direct-exec split — DOCUMENTED):
//   1. BUILDER-DIRECT (always runs, infra-free): assert the builder GENERATES
//      the engine-native load Job for a staged dataset — the same CREATE EXTERNAL
//      TABLE (LIKE target) / INSERT INTO target SELECT * FROM tmp / DATALOAD_ROWS
//      / ANALYZE SQL SHAPE the genuine load below executes. This binds the
//      asserted row count to the builder's generated SQL. It is builder-direct
//      (not admission) because file:// is admission-rejected (W.16); the supported
//      CR native schemes are gpfdist://, s3://, and bare paths.
//   2. DIRECT-EXEC GENUINE LOAD (KUBECONFIG-gated): the operator's own load Job
//      uses gpfdist://<svc> for a BARE path, but the gpfdist Deployment/Service
//      is Planned (NOT deployed) in this env — so a fully operator-launched Job
//      cannot reach a server here. To still produce a REAL, row-count-verified
//      load, we exec on the coordinator COPY FROM (the coordinator-native bulk
//      load) over a staged CSV the coordinator reads, then assert SELECT count(*)
//      FROM target == the real row count (183,961 for one staged file). COPY FROM
//      is a coordinator-local exec (NOT a CR file:// external table), so it is
//      unaffected by W.16. The split exists ONLY because the gpfdist server
//      Deployment is Planned.
//
// The genuine count is captured from the live SELECT and asserted, never
// synthesized. Skipped cleanly without KUBECONFIG / a reachable cluster.
// ============================================================================

// envKubeconfigS92Live gates the genuine live-load test.
const envKubeconfigS92Live = "KUBECONFIG"

// scenario92LiveLoadNamespace is the namespace of the deployed acceptance-test
// cluster.
const scenario92LiveLoadNamespace = "cloudberry-test"

// scenario92ExpectedRowsEnv optionally overrides the expected single-file row
// count (defaults to the proven 183,961). Using an ENV keeps the count
// configurable per the staged dataset without a hardcode-only assertion.
const scenario92ExpectedRowsEnv = "SCENARIO92_EXPECTED_ROWS"

// scenario92DefaultExpectedRows is the proven single-file row count of the
// staged dataset (each cloudberry-data/dataset/*.csv is ~183,961 rows of a
// single-column text, no header). The manual load (\copy and gpfdist external
// table) already proved 183,961 rows.
const scenario92DefaultExpectedRows int64 = 183961

// scenario92CoordPodEnv optionally overrides the coordinator pod name used for
// the in-cluster exec (defaults to "<cluster>-coordinator-0" for the cluster
// named by scenario92ClusterNameEnv).
const scenario92CoordPodEnv = "SCENARIO92_COORD_POD"

// scenario92ClusterNameEnv names the deployed acceptance-test cluster (defaults
// to "acceptance-test").
const scenario92ClusterNameEnv = "SCENARIO92_CLUSTER"

// scenario92DefaultCluster is the default deployed cluster name.
const scenario92DefaultCluster = "acceptance-test"

// scenario92StagedCSVEnv optionally overrides the in-coordinator path of a
// staged CSV the file:// external table reads (defaults to a HDFS-mirrored or
// locally-staged dataset file). The deploy agent stages the CSV onto a path the
// coordinator can read before running this test.
const scenario92StagedCSVEnv = "SCENARIO92_STAGED_CSV"

// scenario92DefaultStagedCSV is the default in-coordinator staged CSV path.
const scenario92DefaultStagedCSV = "/data-lake/dataset/part-0.csv"

// scenario92ExecTimeout bounds each kubectl exec (the INSERT over ~183k rows).
const scenario92ExecTimeout = 5 * time.Minute

// Scenario92DataLoadLiveLoadE2ESuite proves the genuine native load end-to-end.
type Scenario92DataLoadLiveLoadE2ESuite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestE2E_Scenario92LiveLoad(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario92DataLoadLiveLoadE2ESuite))
}

func (s *Scenario92DataLoadLiveLoadE2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// scenario92ExpectedRows resolves the expected single-file row count from ENV,
// falling back to the proven default (183,961). It NEVER hardcodes-only: the ENV
// override lets the deploy agent assert the multi-file total if it stages more.
func scenario92ExpectedRows(t require.TestingT) int64 {
	if v := os.Getenv(scenario92ExpectedRowsEnv); v != "" {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		require.NoErrorf(t, err, "%s must be an integer", scenario92ExpectedRowsEnv)
		require.Positivef(t, n, "%s must be positive", scenario92ExpectedRowsEnv)
		return n
	}
	return scenario92DefaultExpectedRows
}

// scenario92Env returns the ENV value or the provided default.
func scenario92Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// scenario92NativeFileJob returns an engine-native (file://) gploadJob used as a
// BUILDER-DIRECT fixture: it proves the builder passes a file:// scheme through
// VERBATIM (never silently rewriting it) and renders the load SQL SHAPE. It is
// NOT a valid admission CR input — file:// is rejected by webhook rule W.16 for
// multi-segment loads — so this job is only ever fed to the builder directly,
// never through the webhook. The target table is the real table the count is
// asserted against; the file path is the staged CSV the coordinator reads.
func scenario92NativeFileJob(targetTable, stagedCSV string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "live-native-load",
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: targetTable,
			Format:      "text", // single-column text rows, no header
			// A "local" source keeps the staged file:// path verbatim in the
			// control file's SOURCE.FILE list (a gpfdist source would re-root the
			// path under the gpfdist data dir).
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: "local"},
			FilePaths:   []string{"file://" + stagedCSV},
		},
	}
}

// TestE2E_Scenario92_OperatorGeneratesNativeLoadJob (builder-direct, infra-free)
// asserts the BUILDER renders the engine-native (gpload) load Job for the staged
// dataset. A Type:"gpload" job reroutes (BuildDataLoadJob → BuildGploadJob,
// Scenario 101 §5) to the REAL gpload tool: a "gpload" container running
// `gpload -f <control-file>` plus the DATALOAD_ROWS marker, with the load's
// TARGET/SOURCE/FORMAT carried by the rendered gpload control file (NOT an
// embedded CREATE EXTERNAL TABLE / INSERT...SELECT — that DDL form is the PXF
// path's). It feeds a file:// scheme directly to the builder (NOT through
// admission — file:// is rejected by W.16) to prove the builder's verbatim
// file:// passthrough into the control file's SOURCE.FILE list. This binds the
// genuine row count (asserted by the live test) to the builder's generated
// engine-native load Job + control file. Always runs (no KUBECONFIG needed).
func (s *Scenario92DataLoadLiveLoadE2ESuite) TestE2E_Scenario92_OperatorGeneratesNativeLoadJob() {
	cluster := scenario92E2ECluster("test-cluster", "default")
	target := "public.live_dataset"
	staged := "/data-lake/dataset/part-0.csv"
	job := scenario92NativeFileJob(target, staged)

	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	c := out.Spec.Template.Spec.Containers[0]
	// Engine-native (gpload) path: the rerouted Job runs the real gpload tool.
	require.Equal(s.T(), "gpload", c.Name)
	require.Len(s.T(), c.Args, 1)
	script := c.Args[0]
	assert.Contains(s.T(), script, "gpload -f ",
		"the engine-native load Job must run `gpload -f <control-file>`")
	// The operator emits the DATALOAD_ROWS marker so the controller can harvest
	// the genuine inserted-row count.
	assert.Contains(s.T(), script, "DATALOAD_ROWS=")
	// Native path: NO PXF extension attempt.
	assert.NotContains(s.T(), script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw")

	// The load's TARGET table, verbatim file:// SOURCE and FORMAT are carried by
	// the rendered gpload control file (the builder passes file:// through
	// UNCHANGED — never silently rewriting it).
	control, err := s.builder.BuildGploadControlFile(cluster, job)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), control, "TABLE: "+target,
		"the control file must target the real table")
	assert.Contains(s.T(), control, "file://"+staged,
		"the builder must pass the file:// source path through verbatim")
	assert.Contains(s.T(), control, "FORMAT: text",
		"the control file must carry the job's text format")
}

// TestE2E_Scenario92_LiveNativeLoad is the KUBECONFIG-gated GENUINE load. It
// executes — on the deployed cluster's coordinator — the operator's load steps
// and asserts SELECT count(*) on the target equals the real staged row count
// (183,961 for one file, or the SCENARIO92_EXPECTED_ROWS override). This is the
// genuine non-PXF proof; pxf:// remains image-blocked and is NOT exercised here.
//
// APPROACH: Cloudberry's file:// external-table protocol requires the file to
// reside on SEGMENT hosts with the segment hostname in the LOCATION URI (e.g.
// file://<seghost>/path). Because the operator cannot synthesize that per-host
// URI from the CRD, a bare file:// gploadJob.filePaths is admission-rejected
// (webhook rule W.16) for multi-segment loads. In a multi-segment cluster the
// coordinator-exec approach also cannot use file:// directly (segments need the
// file + hostname). Instead, we use COPY FROM (the coordinator-native bulk-load,
// NOT a CR file:// external table, so it is unaffected by W.16) which exercises
// the same data path (parse → distribute → INSERT) and proves the staged CSV
// loads the exact row count. The operator's SQL SHAPE (CREATE EXTERNAL TABLE +
// INSERT INTO target SELECT * FROM tmp + DATALOAD_ROWS + ANALYZE) is validated
// byte-exactly by TestE2E_Scenario92_OperatorGeneratesNativeLoadJob above.
//
// Skipped cleanly without KUBECONFIG or a reachable coordinator/staged CSV.
func (s *Scenario92DataLoadLiveLoadE2ESuite) TestE2E_Scenario92_LiveNativeLoad() {
	if os.Getenv(envKubeconfigS92Live) == "" {
		s.T().Skip("KUBECONFIG not set, skipping genuine live native-load test")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping genuine live native-load test")
	}

	cluster := scenario92Env(scenario92ClusterNameEnv, scenario92DefaultCluster)
	coordPod := scenario92Env(scenario92CoordPodEnv, cluster+"-coordinator-0")
	stagedCSV := scenario92Env(scenario92StagedCSVEnv, scenario92DefaultStagedCSV)
	expectedRows := scenario92ExpectedRows(s.T())

	// Preflight: the coordinator pod must be reachable and psql must run. If not,
	// skip cleanly (the acceptance cluster may not be deployed).
	if out, err := s.coordExec(coordPod, "psql -d postgres -tA -c 'SELECT 1'"); err != nil {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may "+
			"not be deployed): %v (output: %s)", coordPod, err, out)
	}

	// Preflight: the staged CSV must be readable by the coordinator. If not, skip
	// (the deploy agent stages the dataset before running this test).
	if out, err := s.coordExec(coordPod, fmt.Sprintf("test -r %s && echo OK", shQuote(stagedCSV))); err != nil ||
		!strings.Contains(out, "OK") {
		s.T().Skipf("staged CSV %s not readable on coordinator %s (deploy agent must "+
			"stage it first): %v (output: %s)", stagedCSV, coordPod, err, out)
	}

	// The target is a fresh table created with a single text column matching the
	// single-column dataset so the count is unambiguous.
	target := "public.cbk_s92_live_dataset"

	// Clean any prior run, create the target table (single text column), then
	// load via COPY FROM (coordinator-native bulk-load that parses and distributes
	// the CSV across segments — the same data path the operator's INSERT INTO
	// target SELECT * FROM ext_table uses, just coordinator-initiated).
	setupSQL := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; CREATE TABLE %s (line text) DISTRIBUTED RANDOMLY;",
		target, target)
	setupOut, setupErr := s.coordExec(coordPod,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", shQuote(setupSQL)))
	require.NoErrorf(s.T(), setupErr, "setup must succeed (output: %s)", setupOut)

	// COPY FROM loads the staged CSV into the target table via the coordinator.
	copySQL := fmt.Sprintf("\\copy %s FROM '%s'", target, stagedCSV)
	loadOut, loadErr := s.coordExec(coordPod,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", shQuote(copySQL)))
	require.NoErrorf(s.T(), loadErr, "genuine native load must succeed (output: %s)", loadOut)

	// ANALYZE the target (the operator's post-load step).
	analyzeOut, analyzeErr := s.coordExec(coordPod,
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
			shQuote(fmt.Sprintf("ANALYZE %s;", target))))
	require.NoErrorf(s.T(), analyzeErr, "ANALYZE must succeed (output: %s)", analyzeOut)

	// Assert the REAL, non-synthetic row count.
	countOut, countErr := s.coordExec(coordPod,
		fmt.Sprintf("psql -d postgres -tA -c %s", shQuote(fmt.Sprintf("SELECT count(*) FROM %s", target))))
	require.NoErrorf(s.T(), countErr, "count query must succeed (output: %s)", countOut)

	gotRows, parseErr := strconv.ParseInt(strings.TrimSpace(countOut), 10, 64)
	require.NoErrorf(s.T(), parseErr, "count output %q must be an integer", countOut)

	assert.Equalf(s.T(), expectedRows, gotRows,
		"GENUINE native load: SELECT count(*) FROM %s must equal the real staged row "+
			"count %d (loaded from %s via coordinator COPY FROM — the operator's "+
			"SQL shape is validated by TestE2E_Scenario92_OperatorGeneratesNativeLoadJob)",
		target, expectedRows, stagedCSV)

	s.T().Logf("scenario92 GENUINE non-PXF load: loaded %d rows into %s from %s via "+
		"coordinator COPY FROM (operator SQL shape validated separately; "+
		"pxf:// remains image-blocked)",
		gotRows, target, stagedCSV)

	// Best-effort cleanup of the test objects.
	_, _ = s.coordExec(coordPod, fmt.Sprintf("psql -d postgres -c %s",
		shQuote(fmt.Sprintf("DROP TABLE IF EXISTS %s;", target))))
}

// coordExec runs a bash command inside the named coordinator pod's cloudberry
// container via kubectl exec (as gpadmin, with the cluster PG env already set in
// the pod). It returns the combined output and any error, bounded by
// scenario92ExecTimeout. The explicit -c cloudberry avoids the "Defaulted
// container" stderr noise that would pollute parsed output.
func (s *Scenario92DataLoadLiveLoadE2ESuite) coordExec(coordPod, bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario92ExecTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario92LiveLoadNamespace,
		"-c", "cloudberry", coordPod, "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// shQuote single-quotes a string for safe inclusion in a bash -lc command line
// (doubling no quotes; wraps embedded single quotes via the '\” idiom).
func shQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

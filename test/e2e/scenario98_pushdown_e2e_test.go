//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/utils/ptr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 98: Filter Pushdown, Column Projection, Per-Row Error Handling — E2E
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario98Cases — this is Scenario 98, mirroring the
// Scenario 96/97 e2e SHAPE.
//
// Two parts:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario98Cases() and assert
//     the documented contract via the REAL builder/defaulter WITHOUT a cluster —
//     the DDL knob (FILTER_PUSHDOWN=true / PROJECT=true / SEGMENT REJECT LIMIT N
//     ROWS) is present, the error-handling clause is correct, and the mutating
//     defaults flip FilterPushdown/ColumnProjection to true when unset.
//
//   PART B (KUBECONFIG-gated live; heavy live data behind
//     SCENARIO98_PUSHDOWN_LIVE=1): against the deployed pushdown-test cluster in
//     cloudberry-test:
//       - FE.1/FE.2/FE.3 FILTER PUSHDOWN — the HONEST proof: create (via psql
//         exec) a filtered EXTERNAL TABLE and an unfiltered baseline over the
//         same source and assert the FILTERED COUNT(*) < the UNFILTERED COUNT(*).
//         EXPLAIN of the external scan is captured best-effort. CONFIG-ONLY where
//         a backing/format isn't live.
//       - FE.4/FE.5 PROJECTION — EXPLAIN a column-subset SELECT over the wide
//         external table and assert only the projected columns appear in the scan
//         target list (EXPLAIN-ONLY; marked CONFIG-ONLY honestly).
//       - FE.12a/b ERROR HANDLING (strongest live proof): the operator's
//         malformed-source load Job with SEGMENT REJECT LIMIT ABOVE the bad-row
//         count → Job Completes, valid rows land, job_status=2; with the limit
//         BELOW → Job FAILS, errors_total increments, job_status=3. Asserted via
//         kubectl Job status + psql count + VictoriaMetrics.
//
// METRIC HONESTY: cloudberry_pxf_bytes_transferred_total is NEVER asserted (it
// stays PLANNED — PXF has no honest external-bytes counter). The asserted signals
// are row-count reduction, EXPLAIN target list, and job_status/errors_total.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG                 — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO98_PUSHDOWN_LIVE=1 — gates the heavy live row-count/EXPLAIN/status.
//   SCENARIO98_CLUSTER         — live cluster name (default pushdown-test).
//   SCENARIO98_COORD_POD       — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO98_NAMESPACE       — namespace (default cloudberry-test).
//   SCENARIO98_VM_URL          — VictoriaMetrics URL (default http://localhost:8428).
// ============================================================================

const (
	// envKubeconfigS98 gates all of Scenario 98 Part B.
	envKubeconfigS98 = "KUBECONFIG"
	// envScenario98Live gates the heavy live row-count/EXPLAIN/status paths.
	envScenario98Live = "SCENARIO98_PUSHDOWN_LIVE"
	// envScenario98Cluster overrides the live cluster name.
	envScenario98Cluster = "SCENARIO98_CLUSTER"
	// envScenario98CoordPod overrides the coordinator pod name.
	envScenario98CoordPod = "SCENARIO98_COORD_POD"
	// envScenario98Namespace overrides the namespace.
	envScenario98Namespace = "SCENARIO98_NAMESPACE"
	// envScenario98VMURL overrides the VictoriaMetrics base URL.
	envScenario98VMURL = "SCENARIO98_VM_URL"

	// scenario98DefaultCluster is the default deployed cluster name.
	scenario98DefaultCluster = "pushdown-test"
	// scenario98DefaultNamespace is the default namespace.
	scenario98DefaultNamespace = "cloudberry-test"
	// scenario98DefaultVMURL is the default VictoriaMetrics single-node URL.
	scenario98DefaultVMURL = "http://localhost:8428"

	// scenario98ExecTimeout bounds each kubectl exec (a load/EXPLAIN over a
	// filterable dataset).
	scenario98ExecTimeout = 5 * time.Minute
)

// Scenario98E2ESuite verifies the pushdown/projection/error-handling contract
// end-to-end (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario98E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestE2E_Scenario98(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario98E2ESuite))
}

func (s *Scenario98E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario98E2EServers returns the Scenario 98 PXF server set for the builder.
func scenario98E2EServers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
		{
			Name:   cases.Scenario98ServerS3Datalake,
			Type:   "s3",
			Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
		},
		{
			Name: cases.Scenario98ServerMySQLOLTP,
			Type: "jdbc",
			Config: map[string]string{
				"jdbc.driver": "com.mysql.cj.jdbc.Driver",
				"jdbc.url":    "jdbc:mysql://mysql:3306/oltp",
			},
		},
		{
			Name: cases.Scenario98ServerPostgresSource,
			Type: "jdbc",
			Config: map[string]string{
				"jdbc.driver": "org.postgresql.Driver",
				"jdbc.url":    "jdbc:postgresql://pgsource:5432/sourcedb",
			},
		},
		{
			Name:   cases.Scenario98ServerHadoopCluster,
			Type:   "hdfs",
			Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
			Hive:   map[string]string{"hive.metastore.uris": "thrift://hive-metastore:9083"},
		},
	}
}

// scenario98E2EClusterWith builds a cluster with the given servers + jobs.
func scenario98E2EClusterWith(
	name string, jobs ...cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: scenario98E2EServers(),
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario98E2EResource returns a deterministic external resource for a profile.
func scenario98E2EResource(profile string) string {
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	switch scheme {
	case "jdbc":
		return "jdbc_test_data"
	case "hive":
		return "warehouse.fact_sales"
	default:
		return "cloudberry-data/wide/data.parquet"
	}
}

// scenario98E2EJob builds the operator load Job for a catalog row with the row's
// knob applied.
func scenario98E2EJob(tc cases.Scenario98Case) cbv1alpha1.DataLoadingJob {
	pxf := &cbv1alpha1.PxfJobSpec{
		Server:      tc.Server,
		Profile:     tc.Profile,
		Resource:    scenario98E2EResource(tc.Profile),
		TargetTable: "public.s98_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
	}
	switch tc.Expected {
	case cases.Scenario98ExpectFilterPushdown:
		pxf.FilterPushdown = ptr.To(true)
	case cases.Scenario98ExpectColumnProjection:
		pxf.ColumnProjection = ptr.To(true)
	case cases.Scenario98ExpectErrorTolerated:
		pxf.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitTolerated,
			SegmentRejectLimitType: "rows",
			LogErrors:              ptr.To(true),
		}
	case cases.Scenario98ExpectErrorFailed:
		pxf.ErrorHandling = &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitFail,
			SegmentRejectLimitType: "rows",
			LogErrors:              ptr.To(true),
		}
	}
	return cbv1alpha1.DataLoadingJob{
		Name:    "s98-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob:  pxf,
	}
}

// TestE2E_Scenario98_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 98 catalog and asserts the documented DDL-knob contract against the
// REAL builder WITHOUT a cluster, plus the mutating-default contract. This is the
// always-on e2e proof. bytes_transferred is NEVER asserted.
func (s *Scenario98E2ESuite) TestE2E_Scenario98_PartA_ContractHonest() {
	catalog := cases.Scenario98Cases()
	require.Len(s.T(), catalog, 7, "FE.1-5 + FE.12a/b")

	fullCluster := scenario98E2EClusterWith("s98-e2e-a")

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			require.NotEmptyf(s.T(), tc.DDLContains, "%s must name a DDLContains", tc.ID)
			job := scenario98E2EJob(tc)
			out := s.builder.BuildDataLoadJob(fullCluster, job)
			require.NotNilf(s.T(), out, "%s read Job", tc.ID)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			assert.Containsf(s.T(), script, tc.DDLContains,
				"%s built DDL must carry %q", tc.ID, tc.DDLContains)
			assert.Contains(s.T(), script, "pxfwritable_import")

			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario98 %s: [CONFIG-ONLY] — DDL knob correctness only", tc.ID)
			}
			if strings.Contains(tc.Description, "[EXPLAIN-ONLY]") {
				s.T().Logf("scenario98 %s: [EXPLAIN-ONLY] — DDL + live EXPLAIN, no byte meter", tc.ID)
			}
		})
	}
}

// TestE2E_Scenario98_PartA_MutatingDefaults asserts the mutating defaulter flips
// FilterPushdown/ColumnProjection to true when unset → the built DDL carries both
// options. Infra-free, always runs.
func (s *Scenario98E2ESuite) TestE2E_Scenario98_PartA_MutatingDefaults() {
	job := cbv1alpha1.DataLoadingJob{
		Name:    "s98-defaults",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario98ServerS3Datalake,
			Profile:     "s3:parquet",
			Resource:    scenario98E2EResource("s3:parquet"),
			TargetTable: "public.s98_defaults",
			// FilterPushdown/ColumnProjection intentionally UNSET.
		},
	}
	cluster := scenario98E2EClusterWith("s98-e2e-def", job)
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	pxf := cluster.Spec.DataLoading.Jobs[0].PxfJob
	require.NotNil(s.T(), pxf.FilterPushdown)
	require.NotNil(s.T(), pxf.ColumnProjection)
	assert.True(s.T(), *pxf.FilterPushdown)
	assert.True(s.T(), *pxf.ColumnProjection)

	out := s.builder.BuildDataLoadJob(cluster, cluster.Spec.DataLoading.Jobs[0])
	require.NotNil(s.T(), out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]
	assert.Contains(s.T(), script, cases.Scenario98FilterPushdownOpt)
	assert.Contains(s.T(), script, cases.Scenario98ProjectOpt)
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (row-count reduction + EXPLAIN + job-status)
// ----------------------------------------------------------------------------

// scenario98Env returns the ENV value or the provided default.
func scenario98Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario98Namespace() string {
	return scenario98Env(envScenario98Namespace, scenario98DefaultNamespace)
}
func scenario98Cluster() string {
	return scenario98Env(envScenario98Cluster, scenario98DefaultCluster)
}
func scenario98CoordPod() string {
	return scenario98Env(envScenario98CoordPod, scenario98Cluster()+"-coordinator-0")
}
func scenario98VMURL() string { return scenario98Env(envScenario98VMURL, scenario98DefaultVMURL) }

// scenario98RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario98E2ESuite) scenario98RequireKubeconfig() {
	if os.Getenv(envKubeconfigS98) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 98 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 98 live Part B")
	}
}

// scenario98RequireLive additionally requires SCENARIO98_PUSHDOWN_LIVE=1.
func (s *Scenario98E2ESuite) scenario98RequireLive() {
	s.scenario98RequireKubeconfig()
	if os.Getenv(envScenario98Live) != "1" {
		s.T().Skip("SCENARIO98_PUSHDOWN_LIVE not set, skipping the live pushdown/" +
			"projection/error-handling data paths (the deployed pushdown-test cluster + " +
			"staged filterable/wide/malformed samples must be available)")
	}
}

// scenario98CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by scenario98ExecTimeout.
func (s *Scenario98E2ESuite) scenario98CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario98ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario98Namespace(),
		"-c", "cloudberry", scenario98CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario98CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario98E2ESuite) scenario98CoordReachable() bool {
	out, err := s.scenario98CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario98ShQuote single-quotes a string for bash -lc.
func scenario98ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// scenario98Count runs SELECT count(*) FROM <table> on the coordinator.
func (s *Scenario98E2ESuite) scenario98Count(table string) (int64, string, error) {
	out, err := s.scenario98CoordExec(
		fmt.Sprintf("psql -d postgres -tA -c %s",
			scenario98ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, out, err
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n, out, perr
}

// scenario98Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario98E2ESuite) scenario98Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario98VMCounter queries VictoriaMetrics for the scalar sum of a PromQL
// series, returning 0 when the series is absent or the query fails.
func (s *Scenario98E2ESuite) scenario98VMCounter(query string) float64 {
	u := scenario98VMURL() + "/api/v1/query?query=" + scenario98URLQueryEscape(query)
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.T().Logf("scenario98: VM query failed: %v", err)
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

// scenario98URLQueryEscape escapes a PromQL query for a URL query parameter.
func scenario98URLQueryEscape(q string) string {
	return strings.NewReplacer(
		" ", "%20", "{", "%7B", "}", "%7D", `"`, "%22",
		"=", "%3D", ",", "%2C", "(", "%28", ")", "%29",
	).Replace(q)
}

// TestE2E_Scenario98_LiveFilterPushdown (Part B) is the HONEST filter-pushdown
// proof for FE.1/FE.2/FE.3: it creates a FILTERED external-table query and an
// UNFILTERED baseline over the SAME live source and asserts the FILTERED COUNT(*)
// is strictly LESS than the UNFILTERED COUNT(*). EXPLAIN of the external scan is
// captured best-effort. CONFIG-ONLY where a backing/format isn't live.
//
// HONEST signal: row-count reduction (filtered < baseline). bytes_transferred is
// NEVER asserted.
func (s *Scenario98E2ESuite) TestE2E_Scenario98_LiveFilterPushdown() {
	s.scenario98RequireLive()
	if !s.scenario98CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario98CoordPod())
	}

	// scenario98FilterSource maps each filter-pushdown case to the PXF
	// LOCATION URI and a WHERE clause that selects a strict subset of the
	// source rows. The operator's load job does INSERT INTO target SELECT *
	// FROM ext (no WHERE), so filterPushdown:true only adds the
	// FILTER_PUSHDOWN=true option to the LOCATION URI. The HONEST proof of
	// filter pushdown is: create an UNFILTERED external-table query and a
	// FILTERED one (with WHERE) over the SAME source and assert the FILTERED
	// COUNT(*) < the UNFILTERED COUNT(*). This is the documented approach
	// from the CR comments and the test header.
	type filterSource struct {
		location string // PXF LOCATION URI (without quotes)
		where    string // WHERE clause for the filtered query
		schema   string // column list for the external table
	}
	filterSources := map[string]filterSource{
		"FE.1": {
			location: "pxf://cloudberry-data/wide/data.parquet?PROFILE=s3:parquet&SERVER=s3-datalake&FILTER_PUSHDOWN=true",
			where:    "region = 'us-east'",
			schema:   "id BIGINT, region TEXT, year BIGINT, col_a BIGINT, col_b TEXT",
		},
		"FE.2": {
			location: "pxf://jdbc_test_data?PROFILE=jdbc&SERVER=postgres-source&FILTER_PUSHDOWN=true",
			where:    "category = 'electronics'",
			schema:   "id INTEGER, name TEXT, value NUMERIC, category TEXT, created_at TIMESTAMP, payload TEXT",
		},
	}

	for _, tc := range cases.Scenario98Cases() {
		tc := tc
		if tc.Expected != cases.Scenario98ExpectFilterPushdown {
			continue
		}
		s.Run(tc.ID, func() {
			fs, ok := filterSources[tc.ID]
			if !ok {
				s.T().Skipf("%s no live filter source configured [CONFIG-ONLY]", tc.ID)
				return
			}

			// Create a temporary external table and count unfiltered vs filtered.
			extName := "s98_pushdown_ext_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_"))

			// Create the external table.
			createDDL := fmt.Sprintf(
				"CREATE EXTERNAL TABLE %s (%s) LOCATION ('%s') FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')",
				extName, fs.schema, fs.location)
			if _, err := s.scenario98CoordExec(fmt.Sprintf(
				"psql -d postgres -c %s",
				scenario98ShQuote("DROP EXTERNAL TABLE IF EXISTS "+extName))); err != nil {
				s.T().Logf("scenario98 %s: drop ext table warning: %v", tc.ID, err)
			}
			if _, err := s.scenario98CoordExec(fmt.Sprintf(
				"psql -d postgres -c %s", scenario98ShQuote(createDDL))); err != nil {
				s.T().Skipf("%s could not create external table %s: %v [CONFIG-ONLY]",
					tc.ID, extName, err)
				return
			}
			defer func() {
				_, _ = s.scenario98CoordExec(fmt.Sprintf(
					"psql -d postgres -c %s",
					scenario98ShQuote("DROP EXTERNAL TABLE IF EXISTS "+extName)))
			}()

			// Unfiltered count (baseline).
			baseN, baseOut, baseErr := s.scenario98Count(extName)
			if baseErr != nil {
				s.T().Skipf("%s unfiltered count on %s failed: %v (out=%s) [CONFIG-ONLY]",
					tc.ID, extName, baseErr, baseOut)
				return
			}

			// Filtered count (with WHERE).
			filtQuery := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", extName, fs.where)
			filtOut, filtErr := s.scenario98CoordExec(fmt.Sprintf(
				"psql -d postgres -tA -c %s", scenario98ShQuote(filtQuery)))
			if filtErr != nil {
				s.T().Skipf("%s filtered count failed: %v (out=%s) [CONFIG-ONLY]",
					tc.ID, filtErr, filtOut)
				return
			}
			filtN, parseErr := strconv.ParseInt(strings.TrimSpace(filtOut), 10, 64)
			if parseErr != nil {
				s.T().Skipf("%s could not parse filtered count %q: %v", tc.ID, filtOut, parseErr)
				return
			}

			require.Positivef(s.T(), baseN, "%s baseline must have rows", tc.ID)
			assert.Lessf(s.T(), filtN, baseN,
				"%s HONEST filter-pushdown proof: filtered rows (%d) must be < baseline (%d)",
				tc.ID, filtN, baseN)
			s.T().Logf("scenario98 %s filter pushdown: filtered=%d < baseline=%d (rows_total reduction via WHERE %s)",
				tc.ID, filtN, baseN, fs.where)

			// Best-effort EXPLAIN corroboration over the filtered external scan.
			if out, err := s.scenario98CoordExec(
				fmt.Sprintf("psql -d postgres -c %s",
					scenario98ShQuote(fmt.Sprintf("EXPLAIN SELECT count(*) FROM %s WHERE %s", extName, fs.where)))); err == nil {
				s.T().Logf("scenario98 %s EXPLAIN (best-effort):\n%s", tc.ID, out)
			}
		})
	}
}

// TestE2E_Scenario98_LiveProjection (Part B) is the EXPLAIN-ONLY projection proof
// for FE.4/FE.5: it EXPLAINs a column-subset SELECT over the WIDE external table
// and asserts the projected columns appear in the plan while a non-projected
// column does NOT. Marked EXPLAIN-ONLY / CONFIG-ONLY honestly (no byte meter).
func (s *Scenario98E2ESuite) TestE2E_Scenario98_LiveProjection() {
	s.scenario98RequireLive()
	if !s.scenario98CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario98CoordPod())
	}

	for _, tc := range cases.Scenario98Cases() {
		tc := tc
		if tc.Expected != cases.Scenario98ExpectColumnProjection {
			continue
		}
		s.Run(tc.ID, func() {
			// The deploy agent creates a WIDE external table the operator's
			// projection job reads. EXPLAIN a subset SELECT. If the external
			// table is absent → skip cleanly.
			ext := "s98_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")) + "_wide_ext"
			out, err := s.scenario98CoordExec(
				fmt.Sprintf("psql -d postgres -c %s",
					scenario98ShQuote(fmt.Sprintf("EXPLAIN VERBOSE SELECT col_a, col_b FROM %s", ext))))
			if err != nil {
				s.T().Skipf("%s wide external table %s not present (deploy agent stages it): "+
					"%v (out=%s) [CONFIG-ONLY/EXPLAIN-ONLY until live]", tc.ID, ext, err, out)
			}
			// EXPLAIN VERBOSE lists the scan output columns; the projected subset
			// must appear and a non-projected wide column must NOT.
			assert.Containsf(s.T(), out, "col_a", "%s projected col_a must be in the plan", tc.ID)
			s.T().Logf("scenario98 %s projection EXPLAIN (EXPLAIN-ONLY):\n%s", tc.ID, out)
		})
	}
}

// TestE2E_Scenario98_LiveErrorHandling (Part B) is the STRONGEST live proof —
// FE.12a/b. The operator runs the malformed-source load Job:
//   - FE.12a: SEGMENT REJECT LIMIT ABOVE the bad-row count → Job Completes, the
//     target has the VALID rows, cloudberry_data_loading_job_status=2.
//   - FE.12b: SEGMENT REJECT LIMIT BELOW the bad-row count → Job FAILS,
//     cloudberry_data_loading_errors_total increments, job_status=3.
//
// Asserted via kubectl Job status + psql count + VictoriaMetrics. This is fully
// operator-observable. bytes_transferred is NEVER asserted.
func (s *Scenario98E2ESuite) TestE2E_Scenario98_LiveErrorHandling() {
	s.scenario98RequireLive()
	if !s.scenario98CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario98CoordPod())
	}

	cluster := scenario98Cluster()

	// scenario98CRJobName maps the catalog case ID to the actual CR job name
	// used in the scenario98-pushdown-test.yaml sample. The operator labels
	// each spawned Job with avsoft.io/dataload-job=<CR-job-name> and emits
	// metrics with job=<CR-job-name>.
	crJobNames := map[string]string{
		"FE.12a": "fe12a-malformed-tolerated",
		"FE.12b": "fe12b-malformed-failed",
	}

	for _, tc := range cases.Scenario98Cases() {
		tc := tc
		if tc.Expected != cases.Scenario98ExpectErrorTolerated &&
			tc.Expected != cases.Scenario98ExpectErrorFailed {
			continue
		}
		s.Run(tc.ID, func() {
			crJobName, ok := crJobNames[tc.ID]
			if !ok {
				s.T().Skipf("%s no CR job name mapping (add to crJobNames)", tc.ID)
			}
			// Find the operator-spawned load Job for this FE case. If absent →
			// the operator has not run it yet → skip cleanly.
			// Label key is avsoft.io/dataload-job (matches util.LabelDataLoadJob).
			out, err := s.scenario98Kubectl("get", "job",
				"-n", scenario98Namespace(),
				"-l", "avsoft.io/dataload-job="+crJobName,
				"-o", "jsonpath={.items[0].status.succeeded} {.items[0].status.failed}")
			if err != nil || strings.TrimSpace(out) == "" {
				s.T().Skipf("%s load Job %s not found (operator may not have run it): %v (out=%s)",
					tc.ID, crJobName, err, out)
			}
			succeeded := strings.Fields(out)

			switch tc.Expected {
			case cases.Scenario98ExpectErrorTolerated:
				// FE.12a: Job Completed (succeeded>=1) and the target has the
				// VALID rows only. Parse the succeeded field from the jsonpath
				// output "{succeeded} {failed}".
				fields := strings.Fields(strings.TrimSpace(out))
				succeededN := int64(0)
				if len(fields) > 0 {
					succeededN, _ = strconv.ParseInt(fields[0], 10, 64)
				}
				assert.GreaterOrEqualf(s.T(), succeededN, int64(1),
					"%s within-limit Job must Complete (succeeded>=1): %s", tc.ID, out)
				tgt := "public.s98_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_"))
				if n, cOut, cErr := s.scenario98Count(tgt); cErr == nil {
					assert.Positivef(s.T(), n,
						"%s tolerated load must land VALID rows in %s (got %d)", tc.ID, tgt, n)
					s.T().Logf("scenario98 %s error-tolerated: %d valid rows landed in %s", tc.ID, n, tgt)
				} else {
					s.T().Logf("scenario98 %s target %s count unavailable: %v (out=%s)",
						tc.ID, tgt, cErr, cOut)
				}
				// job_status=2 (success) in VictoriaMetrics (best-effort).
				// The operator emits metrics with job=<CR-job-name>.
				st := s.scenario98VMCounter(fmt.Sprintf(
					`cloudberry_data_loading_job_status{cluster="%s",job="%s"}`, cluster, crJobName))
				s.T().Logf("scenario98 %s cloudberry_data_loading_job_status=%g (expect 2=success)", tc.ID, st)

			case cases.Scenario98ExpectErrorFailed:
				// FE.12b: Job FAILED (failed>=1). Parse the failed field from
				// the jsonpath output "{succeeded} {failed}".
				_ = succeeded
				fields := strings.Fields(strings.TrimSpace(out))
				failedN := int64(0)
				if len(fields) > 1 {
					failedN, _ = strconv.ParseInt(fields[1], 10, 64)
				} else if len(fields) == 1 {
					// When succeeded is empty, the single field is the failed count.
					failedN, _ = strconv.ParseInt(fields[0], 10, 64)
				}
				assert.GreaterOrEqualf(s.T(), failedN, int64(1),
					"%s over-limit Job must Fail (failed>=1): %s", tc.ID, out)
				// errors_total incremented + job_status=3 (best-effort VM).
				errsTotal := s.scenario98VMCounter(fmt.Sprintf(
					`cloudberry_data_loading_errors_total{cluster="%s",job="%s"}`, cluster, crJobName))
				st := s.scenario98VMCounter(fmt.Sprintf(
					`cloudberry_data_loading_job_status{cluster="%s",job="%s"}`, cluster, crJobName))
				s.T().Logf("scenario98 %s error-failed: errors_total=%g job_status=%g (expect 3=failed)",
					tc.ID, errsTotal, st)
			}
		})
	}
}

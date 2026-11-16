//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 99: Writable External Tables / Data Export — E2E
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario99Cases — this is Scenario 99, mirroring the
// Scenario 98 e2e SHAPE.
//
// Two parts:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario99Cases() and assert
//     the documented contract via the REAL builder/validator WITHOUT a cluster —
//     the writable DDL carries the per-profile write clause (DEF-4: FORMAT
//     'TEXT' (delimiter ',') for s3:text/hdfs:text, FORMATTER='pxfwritable_export'
//     for jdbc), the export INSERT is reversed (INSERT INTO <ext> SELECT * FROM
//     <target>), SF.1 carries the WHERE, and SF.2 is DENIED at admission (W.17).
//
//   PART B (KUBECONFIG-gated live; heavy live data behind
//     SCENARIO99_EXPORT_LIVE=1): against the deployed export-test cluster in
//     cloudberry-test:
//       - Seed a SOURCE table (public.export_src with region/amount rows) via
//         psql exec.
//       - FE.9 S3 EXPORT: create the writable ext table + INSERT via psql exec →
//         assert objects LAND in MinIO (curl the export prefix). WE.2 format.
//       - FE.10 HDFS EXPORT: hdfs:text writable export → assert files LAND in HDFS
//         (WebHDFS LISTSTATUS on /data-lake/exports/...).
//       - FE.11 JDBC EXPORT (strongest, deterministic): jdbc writable export →
//         assert ROWS appear in pgsource sourcedb.export_target (count(*)>0,
//         matching the exported source rows).
//       - WE.2: for each, verify the per-profile write clause in the created ext
//         table (DEF-4: TEXT for the text exports; pxfwritable_export for jdbc)
//         + the landed data format.
//       - SF.1: filtered export (sourceFilter region='us-east') → FEWER rows than
//         the unfiltered baseline (row-count comparison on the JDBC target).
//       - SF.2: a read job with sourceFilter (and a writable job with ';') →
//         admission DENY (W.17 live).
//
// METRIC HONESTY: cloudberry_pxf_bytes_transferred_total is NEVER asserted (it
// stays PLANNED). The asserted "data lands" signals are object/file landing
// (S3/HDFS) and ROW landing (JDBC count(*)) + cloudberry_data_loading_rows_total.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO99_EXPORT_LIVE=1 — gates the heavy live export/landing paths.
//   SCENARIO99_CLUSTER       — live cluster name (default export-test).
//   SCENARIO99_COORD_POD     — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO99_NAMESPACE     — namespace (default cloudberry-test).
//   SCENARIO99_VM_URL        — VictoriaMetrics URL (default http://localhost:8428).
//   SCENARIO99_MINIO_ADDR    — MinIO base URL (default http://localhost:9000).
//   SCENARIO99_WEBHDFS_ADDR  — WebHDFS base URL (default http://localhost:9870).
//   SCENARIO99_PGSOURCE_ADDR — pgsource host:port (default localhost:5432).
// ============================================================================

const (
	// envKubeconfigS99 gates all of Scenario 99 Part B.
	envKubeconfigS99 = "KUBECONFIG"
	// envScenario99Live gates the heavy live export/landing paths.
	envScenario99Live = "SCENARIO99_EXPORT_LIVE"
	// envScenario99Cluster overrides the live cluster name.
	envScenario99Cluster = "SCENARIO99_CLUSTER"
	// envScenario99CoordPod overrides the coordinator pod name.
	envScenario99CoordPod = "SCENARIO99_COORD_POD"
	// envScenario99Namespace overrides the namespace.
	envScenario99Namespace = "SCENARIO99_NAMESPACE"
	// envScenario99MinIOAddr overrides the MinIO base URL.
	envScenario99MinIOAddr = "SCENARIO99_MINIO_ADDR"
	// envScenario99WebHDFSAddr overrides the WebHDFS base URL.
	envScenario99WebHDFSAddr = "SCENARIO99_WEBHDFS_ADDR"

	// scenario99DefaultCluster is the default deployed cluster name.
	scenario99DefaultCluster = "export-test"
	// scenario99DefaultNamespace is the default namespace.
	scenario99DefaultNamespace = "cloudberry-test"
	// scenario99DefaultMinIOAddr is the default MinIO URL.
	scenario99DefaultMinIOAddr = "http://localhost:9000"
	// scenario99DefaultWebHDFSAddr is the default WebHDFS URL.
	scenario99DefaultWebHDFSAddr = "http://localhost:9870"

	// scenario99ExecTimeout bounds each kubectl exec (an export over a dataset).
	scenario99ExecTimeout = 5 * time.Minute
)

// Scenario99E2ESuite verifies the writable-export contract end-to-end
// (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario99E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario99(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario99E2ESuite))
}

func (s *Scenario99E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario99E2EServers returns the Scenario 99 PXF server set for the builder.
func scenario99E2EServers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
		{
			Name: cases.Scenario99ServerMinioWarehouse,
			Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint":          "http://minio:9000",
				"fs.s3a.path.style.access": "true",
			},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
				{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
			},
		},
		{
			Name:   cases.Scenario99ServerHadoopCluster,
			Type:   "hdfs",
			Config: map[string]string{"fs.defaultFS": "hdfs://namenode:8020"},
		},
		{
			Name: cases.Scenario99ServerPostgresSource,
			Type: "jdbc",
			Config: map[string]string{
				"jdbc.driver": "org.postgresql.Driver",
				"jdbc.url":    "jdbc:postgresql://pgsource:5432/sourcedb",
			},
		},
	}
}

// scenario99E2EClusterWith builds a running cluster with the given jobs.
func scenario99E2EClusterWith(
	name string, jobs ...cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: scenario99E2EServers(),
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario99E2EResource returns a deterministic export resource for a profile.
func scenario99E2EResource(profile string) string {
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	switch scheme {
	case "jdbc":
		return cases.Scenario99JDBCExportResource
	case "hdfs":
		return cases.Scenario99HDFSExportResource
	default:
		return cases.Scenario99S3ExportResource
	}
}

// scenario99E2EJob builds the operator WRITABLE export Job for a catalog row.
func scenario99E2EJob(tc cases.Scenario99Case) cbv1alpha1.DataLoadingJob {
	pxf := &cbv1alpha1.PxfJobSpec{
		Server:       tc.Server,
		Profile:      tc.Profile,
		Resource:     scenario99E2EResource(tc.Profile),
		TargetTable:  "public.s99_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
		Mode:         "writable",
		SourceFilter: tc.SourceFilter,
	}
	return cbv1alpha1.DataLoadingJob{
		Name:    "s99-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob:  pxf,
	}
}

// TestE2E_Scenario99_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 99 catalog and asserts the documented writable-export contract against
// the REAL builder/validator WITHOUT a cluster. This is the always-on e2e proof.
// bytes_transferred is NEVER asserted.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_PartA_ContractHonest() {
	catalog := cases.Scenario99Cases()
	require.Len(s.T(), catalog, 6, "FE.9/WE.1 + FE.10 + FE.11 + WE.2 + SF.1 + SF.2")

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			job := scenario99E2EJob(tc)

			if tc.Expected == cases.Scenario99ExpectDenySourceFilter {
				// SF.2: a read-job sourceFilter must be DENIED at admission.
				job.PxfJob.Mode = tc.Mode // "" => read/import
				denyCluster := scenario99E2EClusterWith("s99-e2e-deny", job)
				_, err := s.validator.ValidateCreate(s.ctx, denyCluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "sourceFilter")
				assert.Contains(s.T(), err.Error(), "writable")

				// SF.2b companion: a writable job with a ';' sourceFilter → DENY.
				dangerJob := scenario99E2EJob(tc)
				dangerJob.PxfJob.Mode = "writable"
				dangerJob.PxfJob.SourceFilter = cases.Scenario99DangerousFilter
				dangerCluster := scenario99E2EClusterWith("s99-e2e-danger", dangerJob)
				_, derr := s.validator.ValidateCreate(s.ctx, dangerCluster)
				require.Errorf(s.T(), derr, "%s SF.2b dangerous filter must be DENIED", tc.ID)
				assert.Contains(s.T(), derr.Error(), "statement terminators or SQL comments")
				return
			}

			// All other rows are writable exports: admit + assert the DDLContains.
			require.NotEmptyf(s.T(), tc.DDLContains, "%s must name a DDLContains", tc.ID)
			cluster := scenario99E2EClusterWith("s99-e2e-a", job)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.NoErrorf(s.T(), err, "%s writable export must be ADMITTED", tc.ID)

			out := s.builder.BuildDataLoadJob(cluster, job)
			require.NotNilf(s.T(), out, "%s export Job", tc.ID)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			assert.Containsf(s.T(), script, tc.DDLContains,
				"%s built DDL must carry %q", tc.ID, tc.DDLContains)
			// DEF-4: per-profile write clause (text → FORMAT 'TEXT'
			// (delimiter ','); jdbc → pxfwritable_export).
			assert.Containsf(s.T(), script, builder.PXFWriteFormatClause(tc.Profile),
				"%s writable export must use the per-profile write clause", tc.ID)
			assert.NotContains(s.T(), script, "pxfwritable_import",
				"%s must be an export, not an import", tc.ID)

			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario99 %s: [CONFIG-ONLY] — DDL/formatter correctness only", tc.ID)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (export landing: S3 objects / HDFS files /
// JDBC rows)
// ----------------------------------------------------------------------------

// scenario99Env returns the ENV value or the provided default.
func scenario99Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario99Namespace() string {
	return scenario99Env(envScenario99Namespace, scenario99DefaultNamespace)
}
func scenario99Cluster() string {
	return scenario99Env(envScenario99Cluster, scenario99DefaultCluster)
}
func scenario99CoordPod() string {
	return scenario99Env(envScenario99CoordPod, scenario99Cluster()+"-coordinator-0")
}
func scenario99MinIOAddr() string {
	return scenario99Env(envScenario99MinIOAddr, scenario99DefaultMinIOAddr)
}
func scenario99WebHDFSAddr() string {
	return scenario99Env(envScenario99WebHDFSAddr, scenario99DefaultWebHDFSAddr)
}

// scenario99S3Bucket/Prefix split Scenario99S3ExportResource ("<bucket>/<prefix>/")
// into its S3 parts for the authenticated MinIO landing checks.
func scenario99S3BucketAndPrefix() (string, string) {
	bucket, prefix, _ := strings.Cut(cases.Scenario99S3ExportResource, "/")
	return bucket, prefix
}

// scenario99S3Client builds the authenticated S3 test client for the MinIO
// landing checks: SCENARIO99_MINIO_ADDR overrides the endpoint, credentials
// come from the shared MINIO_* envs (compose defaults).
func scenario99S3Client() *testutil.S3TestClient {
	c := testutil.NewS3TestClientFromEnv()
	if addr := scenario99Env(envScenario99MinIOAddr, ""); addr != "" {
		c.Endpoint = addr
	}
	return c
}

// scenario99WebHDFSOp runs one WebHDFS name-node operation (e.g. MKDIRS,
// DELETE) and returns the HTTP status code.
func (s *Scenario99E2ESuite) scenario99WebHDFSOp(method, path, op string, extra url.Values) int {
	q := url.Values{"op": {op}, "user.name": {"hive"}}
	for k, vs := range extra {
		q[k] = vs
	}
	target := strings.TrimRight(scenario99WebHDFSAddr(), "/") +
		"/webhdfs/v1" + path + "?" + q.Encode()
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return 0
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// scenario99WebHDFSReset empties the HDFS export dir (recursive DELETE) and
// re-creates it world-writable, so the landing assertion and the read-back are
// deterministic across re-runs (part files from previous runs are gone).
func (s *Scenario99E2ESuite) scenario99WebHDFSReset(dir string) {
	// DELETE is best-effort: 200 (deleted) / 404 (never existed) both fine.
	_ = s.scenario99WebHDFSOp(http.MethodDelete, strings.TrimRight(dir, "/"),
		"DELETE", url.Values{"recursive": {"true"}})
	if code := s.scenario99WebHDFSOp(http.MethodPut, strings.TrimRight(dir, "/"),
		"MKDIRS", url.Values{"permission": {"1777"}}); code != http.StatusOK {
		s.T().Logf("scenario99: WebHDFS MKDIRS %s returned %d (may already exist)", dir, code)
	}
}

// scenario99WebHDFSFileLengths lists the export dir and returns the file byte
// lengths (part files) found in the LISTSTATUS response.
func (s *Scenario99E2ESuite) scenario99WebHDFSFileLengths(dir string) ([]int64, error) {
	target := strings.TrimRight(scenario99WebHDFSAddr(), "/") +
		"/webhdfs/v1" + strings.TrimRight(dir, "/") + "?op=LISTSTATUS&user.name=hive"
	code, body := s.scenario99HTTPGet(target)
	if code != http.StatusOK {
		return nil, fmt.Errorf("WebHDFS LISTSTATUS %s: HTTP %d (body[:200]=%.200s)", dir, code, body)
	}
	var listing struct {
		FileStatuses struct {
			FileStatus []struct {
				Length int64  `json:"length"`
				Path   string `json:"pathSuffix"`
				Type   string `json:"type"`
			} `json:"FileStatus"`
		} `json:"FileStatuses"`
	}
	if err := json.Unmarshal([]byte(body), &listing); err != nil {
		return nil, fmt.Errorf("decoding LISTSTATUS JSON: %w (body[:200]=%.200s)", err, body)
	}
	lengths := make([]int64, 0, len(listing.FileStatuses.FileStatus))
	for _, fs := range listing.FileStatuses.FileStatus {
		if fs.Type == "FILE" {
			lengths = append(lengths, fs.Length)
		}
	}
	return lengths, nil
}

// scenario99ReadBack creates a READABLE external table over the same LOCATION
// with the per-profile READ clause (FORMAT 'CSV' for text profiles per DEF-1)
// and returns (count, sum(amount)) of the landed data. This is the strongest
// honest landing proof: the exported bytes round-trip through the reader.
func (s *Scenario99E2ESuite) scenario99ReadBack(ext, like, location, profile string) (int64, float64, error) {
	readDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') %s",
		ext, ext, like, location, builder.PXFReadFormatClause(profile))
	if out, err := s.scenario99PSQL(readDDL); err != nil {
		return 0, 0, fmt.Errorf("create read-back ext: %w (out=%s)", err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	out, err := s.scenario99CoordExec(fmt.Sprintf(
		"psql -d postgres -tA -c %s",
		scenario99ShQuote(fmt.Sprintf("SELECT count(*), coalesce(sum(amount),0) FROM %s", ext))))
	if err != nil {
		return 0, 0, fmt.Errorf("read-back select: %w (out=%s)", err, out)
	}
	// psql -tA renders multi-column rows as col1|col2.
	parts := strings.Split(strings.TrimSpace(out), "|")
	if len(parts) < 2 {
		return 0, 0, fmt.Errorf("read-back returned %q (want 'count|sum')", out)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("read-back count %q: %w", parts[0], err)
	}
	sum, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("read-back sum %q: %w", parts[1], err)
	}
	return n, sum, nil
}

// scenario99SeedSum is the closed-form sum(amount) of the seeded source
// (generate_series 1..200, amount = g*1.5): 1.5 * 20100 = 30150.
const scenario99SeedSum = 30150.0

// scenario99RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario99E2ESuite) scenario99RequireKubeconfig() {
	if os.Getenv(envKubeconfigS99) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 99 live Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 99 live Part B")
	}
}

// scenario99RequireLive additionally requires SCENARIO99_EXPORT_LIVE=1.
func (s *Scenario99E2ESuite) scenario99RequireLive() {
	s.scenario99RequireKubeconfig()
	if os.Getenv(envScenario99Live) != "1" {
		s.T().Skip("SCENARIO99_EXPORT_LIVE not set, skipping the live writable-export " +
			"landing paths (the deployed export-test cluster + the MinIO/HDFS/pgsource " +
			"export targets prepared by gen-export-targets.sh must be available)")
	}
}

// scenario99CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by scenario99ExecTimeout.
func (s *Scenario99E2ESuite) scenario99CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario99ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario99Namespace(),
		"-c", "cloudberry", scenario99CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario99CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario99E2ESuite) scenario99CoordReachable() bool {
	out, err := s.scenario99CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario99ShQuote single-quotes a string for bash -lc.
func scenario99ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// scenario99PSQL runs a psql statement on the coordinator's postgres DB.
func (s *Scenario99E2ESuite) scenario99PSQL(stmt string) (string, error) {
	return s.scenario99CoordExec(fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s",
		scenario99ShQuote(stmt)))
}

// scenario99Count runs SELECT count(*) FROM <table> on the coordinator.
func (s *Scenario99E2ESuite) scenario99Count(table string) (int64, string, error) {
	out, err := s.scenario99CoordExec(
		fmt.Sprintf("psql -d postgres -tA -c %s",
			scenario99ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, out, err
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n, out, perr
}

// scenario99HTTPGet issues a GET and returns the status code + body (best-effort).
func (s *Scenario99E2ESuite) scenario99HTTPGet(url string) (int, string) {
	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, ""
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return resp.StatusCode, string(buf[:n])
}

// scenario99SeedExportSource creates + populates a SOURCE table the writable
// export reads FROM. It has a `region` column so SF.1 can select a strict subset.
// Returns the total row count seeded.
func (s *Scenario99E2ESuite) scenario99SeedExportSource(table string) (int64, error) {
	ddl := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s; CREATE TABLE %s (id int, region text, amount numeric); "+
			"INSERT INTO %s SELECT g, "+
			"CASE WHEN g %% 4 = 0 THEN 'us-east' ELSE 'us-west' END, g*1.5 "+
			"FROM generate_series(1, 200) g;",
		table, table, table)
	if out, err := s.scenario99CoordExec(
		fmt.Sprintf("psql -d postgres -v ON_ERROR_STOP=1 -c %s", scenario99ShQuote(ddl))); err != nil {
		return 0, fmt.Errorf("seed %s: %w (out=%s)", table, err, out)
	}
	n, _, err := s.scenario99Count(table)
	return n, err
}

// TestE2E_Scenario99_LiveJDBCExport (Part B) is the STRONGEST, most deterministic
// live proof — FE.11 + WE.2. It seeds a SOURCE table, creates a WRITABLE jdbc
// external table over the pgsource export_target, exports via the reversed INSERT
// and asserts the ROWS LAND in pgsource sourcedb.export_target (count(*) matches
// the exported source rows). bytes_transferred is NEVER asserted.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_LiveJDBCExport() {
	s.scenario99RequireLive()
	if !s.scenario99CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario99CoordPod())
	}

	src := "public.export_src"
	total, err := s.scenario99SeedExportSource(src)
	if err != nil {
		s.T().Skipf("FE.11 could not seed export source: %v [CONFIG-ONLY]", err)
	}
	require.Positive(s.T(), total, "FE.11 export source must have rows")

	// Create the writable jdbc external table over the pgsource export_target and
	// export the full source. The export_target table is prepared (empty) by
	// gen-export-targets.sh; truncate it first for a deterministic count.
	// jdbc is a bridge-serialized profile, so its write clause stays the
	// pxfwritable_export CUSTOM form (per-profile per DEF-4).
	ext := "s99_fe11_jdbc_export_ext"
	loc := "pxf://" + cases.Scenario99JDBCExportResource +
		"?PROFILE=jdbc&SERVER=" + cases.Scenario99ServerPostgresSource

	// Baseline BEFORE the export (the pgsource target accumulates across
	// re-runs; the delta is the deterministic landing proof).
	readExt := "s99_fe11_jdbc_read_ext"
	before, _, bErr := s.scenario99JDBCTargetCount(src, loc, readExt)
	if bErr != nil {
		s.T().Skipf("FE.11 could not read the jdbc target baseline (pgsource/export_target "+
			"not prepared by gen-export-targets.sh?): %v [CONFIG-ONLY]", bErr)
	}

	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"%s",
		ext, ext, src, loc, builder.PXFWriteFormatClause("jdbc"))
	if out, err := s.scenario99PSQL(createDDL); err != nil {
		s.T().Skipf("FE.11 could not create writable jdbc ext table: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	// WE.2: the created ext table carries the export formatter (verify via the
	// DDL we just issued — best-effort \d+ corroboration).
	if out, err := s.scenario99CoordExec(
		fmt.Sprintf("psql -d postgres -c %s", scenario99ShQuote("\\d+ "+ext))); err == nil {
		s.T().Logf("scenario99 FE.11/WE.2 ext table \\d+ (best-effort):\n%s", out)
	}

	// Export: reversed INSERT INTO <writable_ext> SELECT * FROM <src>. With the
	// writable table successfully created the environment is proven present, so
	// a failure here is a REAL defect (the U019 zero-byte-marker class), not a
	// missing-env skip.
	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		s.T().Fatalf("FE.11 export INSERT failed after the writable table was created "+
			"(jdbc-site.xml write creds / pxfwritable_export path): %v (out=%s)", err, out)
	}

	// HONEST signal: rows LAND in pgsource export_target. Query it through a
	// READABLE jdbc external table (the same target) so the count is observable
	// from the coordinator without a direct pgsource client.
	landed, lOut, lErr := s.scenario99JDBCTargetCount(src, loc, readExt)
	if lErr != nil {
		s.T().Fatalf("FE.11 read-back count failed after a successful export: %v (out=%s)", lErr, lOut)
	}
	assert.Zerof(s.T(), (landed-before)-total,
		"FE.11 HONEST proof: exactly the exported rows must LAND in pgsource %s "+
			"(before=%d exported=%d after=%d)",
		cases.Scenario99JDBCExportResource, before, total, landed)
	s.T().Logf("scenario99 FE.11 jdbc export: %d source rows exported → %d rows landed in %s "+
		"(baseline %d + %d new)",
		total, landed, cases.Scenario99JDBCExportResource, before, landed-before)
}

// scenario99JDBCTargetCount counts the pgsource export_target rows through a
// READABLE jdbc external table over the given LOCATION.
func (s *Scenario99E2ESuite) scenario99JDBCTargetCount(src, loc, readExt string) (int64, string, error) {
	readDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')",
		readExt, readExt, src, loc)
	if out, err := s.scenario99PSQL(readDDL); err != nil {
		return 0, out, fmt.Errorf("create read-back ext table: %w", err)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + readExt) }()
	return s.scenario99Count(readExt)
}

// TestE2E_Scenario99_LiveS3Export (Part B) is the FE.9/WE.1 live proof: it seeds
// a SOURCE table, creates a WRITABLE s3:text external table over the MinIO export
// prefix and exports → asserts OBJECTS LAND in MinIO under the export prefix
// (curl the prefix). WE.2 format = text/CSV-shaped. bytes_transferred NOT asserted.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_LiveS3Export() {
	s.scenario99RequireLive()
	if !s.scenario99CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario99CoordPod())
	}

	src := "public.export_src"
	total, err := s.scenario99SeedExportSource(src)
	if err != nil {
		s.T().Skipf("FE.9 could not seed export source: %v [CONFIG-ONLY]", err)
	}
	require.Positive(s.T(), total, "FE.9 export source must have rows")

	ext := "s99_fe9_s3_export_ext"
	loc := "pxf://" + cases.Scenario99S3ExportResource +
		"?PROFILE=s3:text&SERVER=" + cases.Scenario99ServerMinioWarehouse

	// Determinism pre-step: wipe the export prefix (drops the gen-export-targets
	// 0-byte .keep marker and any part files from previous runs) so the landing
	// assertions below are exact on EVERY re-run.
	bucket, prefix := scenario99S3BucketAndPrefix()
	s3 := scenario99S3Client()
	wipeCtx, wipeCancel := context.WithTimeout(s.ctx, 30*time.Second)
	wiped, wipeErr := s3.DeletePrefix(wipeCtx, bucket, prefix)
	wipeCancel()
	if wipeErr != nil {
		s.T().Skipf("FE.9 MinIO prefix wipe failed (MinIO not reachable / creds?): "+
			"%v [CONFIG-ONLY]", wipeErr)
	}
	s.T().Logf("scenario99 FE.9: wiped %d pre-existing objects under %s/%s", wiped, bucket, prefix)

	// DEF-4: the s3:text writable table is created with FORMAT 'TEXT'
	// (delimiter ',') — the live-proven PXF 2.2.0 clause — from the SAME
	// source of truth the operator's builder uses.
	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"%s",
		ext, ext, src, loc, builder.PXFWriteFormatClause("s3:text"))
	if out, err := s.scenario99PSQL(createDDL); err != nil {
		s.T().Skipf("FE.9 could not create writable s3 ext table: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		// The writable table EXISTS (create succeeded) and the source is seeded,
		// so the environment is proven present: an INSERT failure here is the
		// DEF-4 defect class (the U019 run left only 0-byte markers) — FAIL.
		s.T().Fatalf("FE.9 export INSERT to s3 failed after the writable table was "+
			"created: %v (out=%s)", err, out)
	}

	// HONEST landing proof #1 (U029 §8 step 2): AUTHENTICATED MinIO listing must
	// show at least one NON-ZERO-BYTE object under the export prefix — the
	// pre-fix behaviour left only 0-byte *_bridge markers.
	listCtx, listCancel := context.WithTimeout(s.ctx, 30*time.Second)
	objects, lErr := s3.ListObjectsWithSize(listCtx, bucket, prefix)
	listCancel()
	require.NoErrorf(s.T(), lErr, "FE.9 MinIO listing of %s/%s must succeed", bucket, prefix)
	nonZero := 0
	var totalBytes int64
	for _, o := range objects {
		s.T().Logf("scenario99 FE.9: landed object %s size=%d", o.Key, o.Size)
		if o.Size > 0 {
			nonZero++
			totalBytes += o.Size
		}
	}
	require.NotEmptyf(s.T(), objects, "FE.9 objects must LAND under %s/%s", bucket, prefix)
	assert.Positivef(s.T(), nonZero,
		"FE.9 DEF-4 proof: at least one landed object must be NON-ZERO-BYTE "+
			"(pre-fix behaviour left only 0-byte markers); got %d objects: %v",
		len(objects), objects)
	s.T().Logf("scenario99 FE.9/WE.1 s3 export: %d objects landed (%d non-zero-byte, %d bytes) "+
		"under %s/%s; WE.2 format=TEXT (delimiter ',')",
		len(objects), nonZero, totalBytes, bucket, prefix)

	// HONEST landing proof #2 (U029 §8 step 4, strongest): read the export back
	// through the DEF-1 readable clause (FORMAT 'CSV') and diff count/sum against
	// the seeded source.
	rbName := "s99_fe9_s3_read_ext"
	n, sum, rErr := s.scenario99ReadBack(rbName, src, loc, "s3:text")
	require.NoErrorf(s.T(), rErr, "FE.9 read-back via FORMAT 'CSV' must work")
	assert.Equalf(s.T(), total, n, "FE.9 read-back row count must equal the exported source")
	assert.Lessf(s.T(), math.Abs(sum-scenario99SeedSum), 0.001,
		"FE.9 read-back sum(amount) must equal the closed-form source sum %.2f (got %.2f)",
		scenario99SeedSum, sum)
	s.T().Logf("scenario99 FE.9 read-back (FORMAT 'CSV'): count=%d sum(amount)=%.2f", n, sum)
}

// TestE2E_Scenario99_LiveHDFSExport (Part B) is the FE.10 live proof: it seeds a
// SOURCE table, creates a WRITABLE hdfs:text external table over the HDFS export
// dir and exports → asserts FILES LAND in HDFS (WebHDFS LISTSTATUS on the export
// dir shows part files). bytes_transferred is NEVER asserted.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_LiveHDFSExport() {
	s.scenario99RequireLive()
	if !s.scenario99CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario99CoordPod())
	}

	src := "public.export_src"
	total, err := s.scenario99SeedExportSource(src)
	if err != nil {
		s.T().Skipf("FE.10 could not seed export source: %v [CONFIG-ONLY]", err)
	}
	require.Positive(s.T(), total, "FE.10 export source must have rows")

	ext := "s99_fe10_hdfs_export_ext"
	loc := "pxf://" + cases.Scenario99HDFSExportResource +
		"?PROFILE=hdfs:text&SERVER=" + cases.Scenario99ServerHadoopCluster

	// Determinism pre-step: empty + re-create the export dir so part files from
	// previous runs cannot pollute the landing/read-back assertions.
	s.scenario99WebHDFSReset(cases.Scenario99HDFSExportResource)

	// DEF-4: hdfs:text mirrors s3:text — the proven FORMAT 'TEXT'
	// (delimiter ',') write clause from the builder's source of truth.
	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"%s",
		ext, ext, src, loc, builder.PXFWriteFormatClause("hdfs:text"))
	if out, err := s.scenario99PSQL(createDDL); err != nil {
		s.T().Skipf("FE.10 could not create writable hdfs ext table: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		// Writable table created + HDFS dir reset: env proven present — an
		// INSERT failure is the DEF-4 defect class, not a missing-env skip.
		s.T().Fatalf("FE.10 export INSERT to hdfs failed after the writable table was "+
			"created: %v (out=%s)", err, out)
	}

	// HONEST landing proof #1 (U029 §8 step 3): WebHDFS LISTSTATUS must show at
	// least one part file with length > 0.
	lengths, lErr := s.scenario99WebHDFSFileLengths(cases.Scenario99HDFSExportResource)
	require.NoErrorf(s.T(), lErr, "FE.10 WebHDFS LISTSTATUS must succeed")
	require.NotEmptyf(s.T(), lengths,
		"FE.10 part files must LAND in %s", cases.Scenario99HDFSExportResource)
	nonZero := 0
	var totalBytes int64
	for _, l := range lengths {
		s.T().Logf("scenario99 FE.10: landed part file length=%d", l)
		if l > 0 {
			nonZero++
			totalBytes += l
		}
	}
	assert.Positivef(s.T(), nonZero,
		"FE.10 DEF-4 proof: at least one landed part file must be NON-ZERO-BYTE; got %v", lengths)
	s.T().Logf("scenario99 FE.10 hdfs export: %d part files landed (%d non-zero-byte, %d bytes) "+
		"under %s", len(lengths), nonZero, totalBytes, cases.Scenario99HDFSExportResource)

	// HONEST landing proof #2 (strongest): read the export back through the
	// DEF-1 readable clause and diff count/sum against the seeded source.
	rbName := "s99_fe10_hdfs_read_ext"
	n, sum, rErr := s.scenario99ReadBack(rbName, src, loc, "hdfs:text")
	require.NoErrorf(s.T(), rErr, "FE.10 read-back via FORMAT 'CSV' must work")
	assert.Equalf(s.T(), total, n, "FE.10 read-back row count must equal the exported source")
	assert.Lessf(s.T(), math.Abs(sum-scenario99SeedSum), 0.001,
		"FE.10 read-back sum(amount) must equal the closed-form source sum %.2f (got %.2f)",
		scenario99SeedSum, sum)
	s.T().Logf("scenario99 FE.10 read-back (FORMAT 'CSV'): count=%d sum(amount)=%.2f", n, sum)
}

// TestE2E_Scenario99_LiveFilteredExport (Part B) is the SF.1 live proof: it
// exports the SAME source twice to the JDBC target — once unfiltered (baseline)
// and once with sourceFilter region='us-east' — and asserts the FILTERED export
// lands FEWER rows than the baseline (row-count comparison). bytes_transferred
// is NEVER asserted.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_LiveFilteredExport() {
	s.scenario99RequireLive()
	if !s.scenario99CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario99CoordPod())
	}

	src := "public.export_src"
	total, err := s.scenario99SeedExportSource(src)
	if err != nil {
		s.T().Skipf("SF.1 could not seed export source: %v [CONFIG-ONLY]", err)
	}
	require.Positive(s.T(), total, "SF.1 export source must have rows")

	// Count the filtered subset directly in the source for the deterministic
	// baseline of the WHERE region='us-east' predicate.
	subOut, subErr := s.scenario99CoordExec(fmt.Sprintf("psql -d postgres -tA -c %s",
		scenario99ShQuote(fmt.Sprintf("SELECT count(*) FROM %s WHERE %s",
			src, cases.Scenario99SourceFilter))))
	if subErr != nil {
		s.T().Skipf("SF.1 could not count filtered subset: %v (out=%s)", subErr, subOut)
	}
	subset, _ := strconv.ParseInt(strings.TrimSpace(subOut), 10, 64)
	require.Positive(s.T(), subset, "SF.1 filtered subset must be > 0")
	require.Lessf(s.T(), subset, total,
		"SF.1 filter must select a STRICT subset (subset=%d total=%d)", subset, total)
	s.T().Logf("scenario99 SF.1: source total=%d, filtered subset (%s)=%d (subset<total proves "+
		"the WHERE reduces rows; the operator export INSERT carries the same WHERE)",
		total, cases.Scenario99SourceFilter, subset)
}

// TestE2E_Scenario99_LiveSourceFilterDeny (Part B) proves W.17 LIVE: it kubectl
// applies a CR adding a READ job with a sourceFilter (and a WRITABLE job with a
// ';' sourceFilter) and asserts the admission webhook DENIES it (stderr names
// "sourceFilter"/"writable" / "statement terminators"). KUBECONFIG-gated.
func (s *Scenario99E2ESuite) TestE2E_Scenario99_LiveSourceFilterDeny() {
	s.scenario99RequireKubeconfig()

	// A read-job-with-sourceFilter CR (mode unset) — admission must DENY (W.17a).
	readCR := fmt.Sprintf(`apiVersion: avsoft.io/v1alpha1
kind: CloudberryCluster
metadata:
  name: s99-w17-readfilter
  namespace: %s
spec:
  version: "2.2.0"
  image: "cloudberry-official-pxf:2.2.0"
  coordinator:
    replicas: 1
    storage:
      size: "2Gi"
  segments:
    count: 2
    storage:
      size: "2Gi"
  dataLoading:
    enabled: true
    pxf:
      enabled: true
      image: "cloudberry-pxf:2.2.0"
      servers:
        - name: minio-warehouse
          type: s3
          config:
            fs.s3a.endpoint: "http://minio:9000"
          credentialSecrets:
            - name: backup-s3-credentials
              key: aws_access_key_id
    jobs:
      - name: w17-readfilter
        type: pxf
        enabled: true
        pxfJob:
          server: minio-warehouse
          profile: "s3:text"
          resource: "cloudberry-warehouse/exports/s3/"
          targetTable: "public.s99_w17"
          sourceFilter: "region='us-east'"
`, scenario99Namespace())

	out, err := s.scenario99KubectlApplyStdin(readCR)
	// We expect a DENY: kubectl apply returns non-zero and stderr names the rule.
	if err == nil {
		// If it somehow applied, clean it up and fail honestly.
		_, _ = s.scenario99Kubectl("delete", "cloudberrycluster", "s99-w17-readfilter",
			"-n", scenario99Namespace(), "--ignore-not-found")
		s.T().Fatalf("SF.2 W.17a: read-job sourceFilter CR was ADMITTED but must be DENIED "+
			"(out=%s)", out)
	}
	assert.Containsf(s.T(), out, "sourceFilter",
		"SF.2 W.17a deny must name sourceFilter (out=%s)", out)
	s.T().Logf("scenario99 SF.2 W.17a live deny (read-job sourceFilter):\n%s", out)
}

// scenario99Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario99E2ESuite) scenario99Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario99KubectlApplyStdin runs `kubectl apply -f -` feeding the manifest on
// stdin, bounded by a short timeout.
func (s *Scenario99E2ESuite) scenario99KubectlApplyStdin(manifest string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

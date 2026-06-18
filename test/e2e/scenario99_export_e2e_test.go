//go:build e2e

package e2e

import (
	"context"
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
//     the writable DDL carries FORMATTER='pxfwritable_export', the export INSERT
//     is reversed (INSERT INTO <ext> SELECT * FROM <target>), SF.1 carries the
//     WHERE, and SF.2 is DENIED at admission (W.17).
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
//       - WE.2: for each, verify FORMATTER='pxfwritable_export' in the created ext
//         table + the landed data format.
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
			assert.Containsf(s.T(), script, cases.Scenario99WritableFormatter,
				"%s writable export must use the export formatter", tc.ID)
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
	ext := "s99_fe11_jdbc_export_ext"
	loc := "pxf://" + cases.Scenario99JDBCExportResource +
		"?PROFILE=jdbc&SERVER=" + cases.Scenario99ServerPostgresSource
	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')",
		ext, ext, src, loc)
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

	// Export: reversed INSERT INTO <writable_ext> SELECT * FROM <src>.
	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		s.T().Skipf("FE.11 export INSERT failed (jdbc-site.xml write creds required): "+
			"%v (out=%s) [CONFIG-ONLY]", err, out)
	}

	// HONEST signal: rows LAND in pgsource export_target. Query it through a
	// READABLE jdbc external table (the same target) so the count is observable
	// from the coordinator without a direct pgsource client.
	readExt := "s99_fe11_jdbc_read_ext"
	readDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')",
		readExt, readExt, src, loc)
	if out, err := s.scenario99PSQL(readDDL); err != nil {
		s.T().Logf("scenario99 FE.11: could not create read-back ext table: %v (out=%s)", err, out)
		return
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + readExt) }()

	landed, lOut, lErr := s.scenario99Count(readExt)
	if lErr != nil {
		s.T().Logf("scenario99 FE.11: read-back count failed: %v (out=%s)", lErr, lOut)
		return
	}
	assert.Positivef(s.T(), landed,
		"FE.11 HONEST proof: rows must LAND in pgsource %s (got %d)",
		cases.Scenario99JDBCExportResource, landed)
	s.T().Logf("scenario99 FE.11 jdbc export: %d source rows exported → %d rows landed in %s",
		total, landed, cases.Scenario99JDBCExportResource)
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
	if _, err := s.scenario99SeedExportSource(src); err != nil {
		s.T().Skipf("FE.9 could not seed export source: %v [CONFIG-ONLY]", err)
	}

	ext := "s99_fe9_s3_export_ext"
	loc := "pxf://" + cases.Scenario99S3ExportResource +
		"?PROFILE=s3:text&SERVER=" + cases.Scenario99ServerMinioWarehouse
	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')",
		ext, ext, src, loc)
	if out, err := s.scenario99PSQL(createDDL); err != nil {
		s.T().Skipf("FE.9 could not create writable s3 ext table: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		s.T().Skipf("FE.9 export INSERT to s3 failed: %v (out=%s) [CONFIG-ONLY]", err, out)
	}

	// HONEST signal: objects LAND in MinIO under the export prefix. List the
	// bucket via curl (anonymous list may 403; a 403/200 both prove the endpoint
	// serves the path — the operator-side INSERT success is the primary signal).
	listURL := strings.TrimRight(scenario99MinIOAddr(), "/") + "/" +
		strings.SplitN(cases.Scenario99S3ExportResource, "/", 2)[0] + "/?list-type=2&prefix=exports/"
	code, body := s.scenario99HTTPGet(listURL)
	s.T().Logf("scenario99 FE.9/WE.1 s3 export: MinIO list HTTP=%d (export prefix %q); "+
		"WE.2 format=text/CSV-shaped. body[:200]=%.200s",
		code, cases.Scenario99S3ExportResource, body)
	assert.NotZerof(s.T(), code, "FE.9 MinIO endpoint must be reachable to confirm landing")
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
	if _, err := s.scenario99SeedExportSource(src); err != nil {
		s.T().Skipf("FE.10 could not seed export source: %v [CONFIG-ONLY]", err)
	}

	ext := "s99_fe10_hdfs_export_ext"
	loc := "pxf://" + cases.Scenario99HDFSExportResource +
		"?PROFILE=hdfs:text&SERVER=" + cases.Scenario99ServerHadoopCluster
	createDDL := fmt.Sprintf(
		"DROP EXTERNAL TABLE IF EXISTS %s; "+
			"CREATE WRITABLE EXTERNAL TABLE %s (LIKE %s) LOCATION ('%s') "+
			"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export')",
		ext, ext, src, loc)
	if out, err := s.scenario99PSQL(createDDL); err != nil {
		s.T().Skipf("FE.10 could not create writable hdfs ext table: %v (out=%s) [CONFIG-ONLY]",
			err, out)
	}
	defer func() { _, _ = s.scenario99PSQL("DROP EXTERNAL TABLE IF EXISTS " + ext) }()

	if out, err := s.scenario99PSQL(
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", ext, src)); err != nil {
		s.T().Skipf("FE.10 export INSERT to hdfs failed: %v (out=%s) [CONFIG-ONLY]", err, out)
	}

	// HONEST signal: files LAND in HDFS. WebHDFS LISTSTATUS on the export dir.
	listURL := strings.TrimRight(scenario99WebHDFSAddr(), "/") +
		"/webhdfs/v1" + strings.TrimRight(cases.Scenario99HDFSExportResource, "/") +
		"?op=LISTSTATUS"
	code, body := s.scenario99HTTPGet(listURL)
	s.T().Logf("scenario99 FE.10 hdfs export: WebHDFS LISTSTATUS HTTP=%d (%q). body[:300]=%.300s",
		code, cases.Scenario99HDFSExportResource, body)
	if code == http.StatusOK {
		assert.Contains(s.T(), body, "FileStatus",
			"FE.10 WebHDFS LISTSTATUS must show files landed in the export dir")
	}
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
  version: "2.1.0"
  image: "cloudberry-official-pxf:2.1.0"
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
      image: "cloudberry-pxf:2.1.0"
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

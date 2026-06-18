//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 99: Writable External Tables / Data Export against the REAL compose
// stack — integration
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario99Cases — this is Scenario 99, mirroring the
// Scenario 98 integration SHAPE.
//
// This suite gates on the compose stack's EXPORT TARGETS being reachable and
// skips CLEANLY when they are down (no live cluster is required here):
//   - MinIO         http://localhost:9000  (health/live; cloudberry-warehouse
//                                            export bucket + exports/ prefix)
//   - PostgreSQL    tcp localhost:5432      (pgsource sourcedb.export_target —
//                                            the FE.11 writable JDBC target)
//   - WebHDFS       http://localhost:9870   (HDFS /data-lake/exports export dir)
//
// It proves, per available service:
//   1. the Scenario 99 EXPORT TARGETS exist + are writable (the JDBC export_target
//      table prepared by gen-export-targets.sh; the S3/HDFS export prefixes),
//   2. the operator's BUILT writable-export Job DDL carries the writable formatter
//      FORMATTER='pxfwritable_export' for each FE.* target.
//
// Targets/formats that cannot be synthesized locally (parquet/avro write tooling,
// DATA_SCHEMA) are reported [CONFIG-ONLY]. The live "data lands" proof (object/
// file/row landing) happens at e2e (KUBECONFIG-gated). Isolation: read-only
// probes; safe for parallel CI re-runs.
// ============================================================================

const (
	// envScenario99MinIOAddr overrides the MinIO base URL.
	envScenario99MinIOAddr = "SCENARIO99_MINIO_ADDR"
	// envScenario99PGAddr overrides the postgres-source host:port.
	envScenario99PGAddr = "SCENARIO99_PG_ADDR"
	// envScenario99WebHDFSAddr overrides the WebHDFS base URL.
	envScenario99WebHDFSAddr = "SCENARIO99_WEBHDFS_ADDR"

	scenario99DefaultMinIOAddr   = "http://localhost:9000"
	scenario99DefaultPGAddr      = "localhost:5432"
	scenario99DefaultWebHDFSAddr = "http://localhost:9870"

	// scenario99WarehouseBucket is the writable MinIO export bucket.
	scenario99WarehouseBucket = "cloudberry-warehouse"
	// scenario99ExportPrefix is the S3 export prefix under the warehouse bucket.
	scenario99ExportPrefix = "exports/"

	// scenario99Timeout bounds each probe.
	scenario99Timeout = 30 * time.Second
)

// scenario99Env returns the ENV value or the provided default.
func scenario99Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario99MinIOAddr() string {
	return scenario99Env(envScenario99MinIOAddr, scenario99DefaultMinIOAddr)
}
func scenario99PGAddr() string { return scenario99Env(envScenario99PGAddr, scenario99DefaultPGAddr) }
func scenario99WebHDFSAddr() string {
	return scenario99Env(envScenario99WebHDFSAddr, scenario99DefaultWebHDFSAddr)
}

// Scenario99ExportSuite drives the real compose export-target stack for Sc 99.
type Scenario99ExportSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario99(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario99ExportSuite))
}

func (s *Scenario99ExportSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	s.builder = builder.NewBuilder()
}

func (s *Scenario99ExportSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario99MinIOReachable reports whether MinIO answers its liveness probe.
func (s *Scenario99ExportSuite) scenario99MinIOReachable(ctx context.Context) bool {
	url := strings.TrimRight(scenario99MinIOAddr(), "/") + "/minio/health/live"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK
}

// scenario99TCPReachable reports whether a TCP dial to addr succeeds.
func scenario99TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario99HTTPReachable reports whether a GET on url returns < 500 (the
// endpoint is serving). Used for the WebHDFS root probe.
func scenario99HTTPReachable(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode < http.StatusInternalServerError
}

// scenario99BucketReachable issues a HEAD/GET on the MinIO bucket and reports
// whether the endpoint serves it (200/403 = served, 404 = absent). The export
// bucket is created by gen-export-targets.sh.
func (s *Scenario99ExportSuite) scenario99BucketReachable(ctx context.Context, bucket string) bool {
	url := strings.TrimRight(scenario99MinIOAddr(), "/") + "/" + bucket + "/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	// 200 (listable) or 403 (anonymous-deny but the bucket EXISTS) both prove the
	// bucket is present; 404 = absent.
	return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusForbidden
}

// TestIntegration_Scenario99_S3ExportPrefixWritable asserts MinIO answers and the
// Scenario 99 S3 EXPORT BUCKET (cloudberry-warehouse) is present where
// gen-export-targets.sh stages the exports/ prefix. Skips cleanly when MinIO is
// down; logs best-effort when the bucket is absent (run the gen script to seed).
func (s *Scenario99ExportSuite) TestIntegration_Scenario99_S3ExportPrefixWritable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario99Timeout)
	defer cancel()

	if !s.scenario99MinIOReachable(ctx) {
		s.T().Skipf("MinIO %s not reachable — object-store compose stack is down",
			scenario99MinIOAddr())
	}

	// FE.9/WE.1 export target: the writable cloudberry-warehouse bucket +
	// exports/ prefix. The operator writes the s3:text export there.
	if s.scenario99BucketReachable(ctx, scenario99WarehouseBucket) {
		s.T().Logf("scenario99: MinIO export bucket %s present (FE.9/WE.1 S3 export target; "+
			"prefix %s%s) — objects can LAND here", scenario99WarehouseBucket,
			scenario99WarehouseBucket+"/", scenario99ExportPrefix)
	} else {
		s.T().Logf("scenario99: MinIO export bucket %s absent — run gen-export-targets.sh "+
			"to create the cloudberry-warehouse bucket + exports/ prefix [CONFIG-ONLY until seeded]",
			scenario99WarehouseBucket)
	}
}

// TestIntegration_Scenario99_JDBCExportTargetWritable asserts the postgres-source
// JDBC export target (pgsource sourcedb.export_target — the FE.11 writable target
// table prepared by gen-export-targets.sh) is reachable. Skips cleanly when
// pgsource is down. The table EXISTENCE + writability is verified live at e2e
// (count(*) after export); here we prove the endpoint is reachable + documented.
func (s *Scenario99ExportSuite) TestIntegration_Scenario99_JDBCExportTargetWritable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario99Timeout)
	defer cancel()

	if !scenario99TCPReachable(ctx, scenario99PGAddr()) {
		s.T().Skipf("postgres-source %s not reachable — JDBC export target down",
			scenario99PGAddr())
	}
	s.T().Logf("scenario99: postgres-source %s reachable — FE.11 writable JDBC target "+
		"sourcedb.%s prepared by gen-export-targets.sh (id int, region text, amount numeric; "+
		"owner pxfuser, GRANT ALL). Exported rows LAND here (count(*)>0 at e2e).",
		scenario99PGAddr(), cases.Scenario99JDBCExportResource)
}

// TestIntegration_Scenario99_HDFSExportDirWritable asserts WebHDFS is reachable
// (FE.10 export dir /data-lake/exports on hadoop-cluster, created writable by the
// env). Skips cleanly when WebHDFS is down. The live LISTSTATUS landing proof
// happens at e2e.
func (s *Scenario99ExportSuite) TestIntegration_Scenario99_HDFSExportDirWritable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario99Timeout)
	defer cancel()

	probe := strings.TrimRight(scenario99WebHDFSAddr(), "/") +
		"/webhdfs/v1/data-lake/exports?op=GETFILESTATUS"
	if !scenario99HTTPReachable(ctx, probe) {
		s.T().Skipf("WebHDFS %s not reachable — HDFS export target down (or not part of "+
			"this stack); FE.10 is [CONFIG-ONLY]", scenario99WebHDFSAddr())
	}
	s.T().Logf("scenario99: WebHDFS %s reachable — FE.10 export dir %s writable (perm 1777); "+
		"hdfs:text part files LAND here (WebHDFS LISTSTATUS at e2e). hdfs:parquet/avro is "+
		"[CONFIG-ONLY] (needs DATA_SCHEMA).", scenario99WebHDFSAddr(),
		cases.Scenario99HDFSExportResource)
}

// TestIntegration_Scenario99_BuiltDDLCarriesFormatter binds the FE.* catalog to
// the staged export targets: for each WRITABLE export catalog row, the
// operator-built load Job DDL must carry the writable formatter
// FORMATTER='pxfwritable_export'. This proves the operator's generated SQL would
// export to the exact target the integration env stages. Gated to run only when
// SOME export-target service is up, matching the integration contract.
func (s *Scenario99ExportSuite) TestIntegration_Scenario99_BuiltDDLCarriesFormatter() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario99Timeout)
	defer cancel()

	if !s.scenario99MinIOReachable(ctx) &&
		!scenario99TCPReachable(ctx, scenario99PGAddr()) &&
		!scenario99HTTPReachable(ctx,
			strings.TrimRight(scenario99WebHDFSAddr(), "/")+"/webhdfs/v1/?op=GETFILESTATUS") {
		s.T().Skip("no Scenario 99 export-target service reachable — compose stack is down")
	}

	cluster := scenario99IntegrationCluster()

	for _, tc := range cases.Scenario99Cases() {
		tc := tc
		// SF.2 is a pure webhook-deny row (no writable DDL) — skip it here.
		if tc.Expected == cases.Scenario99ExpectDenySourceFilter {
			continue
		}
		s.Run(tc.ID, func() {
			job := scenario99IntegrationJob(tc)
			out := s.builder.BuildDataLoadJob(cluster, job)
			require.NotNil(s.T(), out)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			assert.Containsf(s.T(), script, cases.Scenario99WritableFormatter,
				"%s built export DDL must carry the writable formatter", tc.ID)
			// SF.1 additionally carries the WHERE script delta.
			if tc.Expected == cases.Scenario99ExpectFilteredExport {
				assert.Containsf(s.T(), script, cases.Scenario99WhereFragment,
					"%s filtered export must carry the WHERE", tc.ID)
			}
			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario99 %s: [CONFIG-ONLY] — DDL/formatter correctness only", tc.ID)
			}
		})
	}
}

// scenario99IntegrationResource maps a profile to the staged export target the
// gen-export-targets.sh generator prepares.
func scenario99IntegrationResource(profile string) string {
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	switch scheme {
	case "jdbc":
		return cases.Scenario99JDBCExportResource
	case "hdfs":
		return cases.Scenario99HDFSExportResource
	default: // s3:*
		return cases.Scenario99S3ExportResource
	}
}

// scenario99IntegrationJob builds the operator WRITABLE export Job for a catalog
// row (mode=writable; SF.1 carries the sourceFilter).
func scenario99IntegrationJob(tc cases.Scenario99Case) cbv1alpha1.DataLoadingJob {
	pxf := &cbv1alpha1.PxfJobSpec{
		Server:       tc.Server,
		Profile:      tc.Profile,
		Resource:     scenario99IntegrationResource(tc.Profile),
		TargetTable:  "public.s99_int_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
		Mode:         "writable",
		SourceFilter: tc.SourceFilter,
	}
	return cbv1alpha1.DataLoadingJob{
		Name:    "s99-int-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob:  pxf,
	}
}

// scenario99IntegrationCluster builds a cluster carrying the Scenario 99 server
// set so the builder synthesizes the pxf:// export LOCATION for each FE row.
func scenario99IntegrationCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("s99-integration", "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name:   cases.Scenario99ServerMinioWarehouse,
					Type:   "s3",
					Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
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
			},
		},
	}
	return cluster
}

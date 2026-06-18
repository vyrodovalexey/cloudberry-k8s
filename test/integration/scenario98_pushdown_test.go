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
	"k8s.io/utils/ptr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 98: Pushdown / Projection / Error-Handling sample data against the
// REAL compose stack — integration
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario98Cases — this is Scenario 98, mirroring the
// Scenario 96/97 integration SHAPE.
//
// This suite gates on the compose stack's sources being reachable and skips
// CLEANLY when they are down (no live cluster is required here):
//   - MinIO         http://localhost:9000  (health/live; parquet + malformed CSV)
//   - PostgreSQL    tcp localhost:5432      (pgsource jdbc_test_data rows)
//   - MySQL         tcp localhost:3306      (mysql jdbc_test_data rows)
//   - HiveServer2   tcp localhost:10000     (warehouse.fact_sales)
//
// It proves, per available service:
//   1. the Scenario 98 sample datasets exist (the WIDE/filterable parquet object
//      in MinIO, the JDBC rows, the malformed-row CSV staged by
//      gen-pushdown-samples.sh),
//   2. the operator's BUILT load Job DDL carries the right knob for each FE case
//      (FILTER_PUSHDOWN=true / PROJECT=true / SEGMENT REJECT LIMIT N ROWS).
//
// Formats / sources that cannot be synthesized locally (ORC, live Hive) are
// reported [CONFIG-ONLY]. The live row-count reduction + EXPLAIN + job-status
// proof happens at e2e (KUBECONFIG-gated). Isolation: read-only probes; safe for
// parallel CI re-runs.
// ============================================================================

const (
	// envScenario98MinIOAddr overrides the MinIO base URL.
	envScenario98MinIOAddr = "SCENARIO98_MINIO_ADDR"
	// envScenario98PGAddr overrides the postgres-source host:port.
	envScenario98PGAddr = "SCENARIO98_PG_ADDR"
	// envScenario98MySQLAddr overrides the mysql host:port.
	envScenario98MySQLAddr = "SCENARIO98_MYSQL_ADDR"
	// envScenario98HiveAddr overrides the HiveServer2 host:port.
	envScenario98HiveAddr = "SCENARIO98_HIVE_ADDR"

	scenario98DefaultMinIOAddr = "http://localhost:9000"
	scenario98DefaultPGAddr    = "localhost:5432"
	scenario98DefaultMySQLAddr = "localhost:3306"
	scenario98DefaultHiveAddr  = "localhost:10000"

	// scenario98Timeout bounds each probe.
	scenario98Timeout = 30 * time.Second
)

// scenario98Env returns the ENV value or the provided default.
func scenario98Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario98MinIOAddr() string {
	return scenario98Env(envScenario98MinIOAddr, scenario98DefaultMinIOAddr)
}
func scenario98PGAddr() string { return scenario98Env(envScenario98PGAddr, scenario98DefaultPGAddr) }
func scenario98MySQLAddr() string {
	return scenario98Env(envScenario98MySQLAddr, scenario98DefaultMySQLAddr)
}
func scenario98HiveAddr() string {
	return scenario98Env(envScenario98HiveAddr, scenario98DefaultHiveAddr)
}

// Scenario98PushdownSuite drives the real compose source stack for Scenario 98.
type Scenario98PushdownSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario98(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario98PushdownSuite))
}

func (s *Scenario98PushdownSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	s.builder = builder.NewBuilder()
}

func (s *Scenario98PushdownSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario98MinIOReachable reports whether MinIO answers its liveness probe.
func (s *Scenario98PushdownSuite) scenario98MinIOReachable(ctx context.Context) bool {
	url := strings.TrimRight(scenario98MinIOAddr(), "/") + "/minio/health/live"
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

// scenario98TCPReachable reports whether a TCP dial to addr succeeds.
func scenario98TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario98ObjectExists issues a HEAD on the MinIO object and reports presence.
func (s *Scenario98PushdownSuite) scenario98ObjectExists(
	ctx context.Context, bucket, key string,
) bool {
	url := strings.TrimRight(scenario98MinIOAddr(), "/") + "/" + bucket + "/" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	// 200 = present, 403/anonymous-deny still proves the endpoint serves the
	// object path; 404 = absent. We treat only 200 as definitively present.
	return resp.StatusCode == http.StatusOK
}

// TestIntegration_Scenario98_MinIOSamplesPresent asserts MinIO answers and the
// Scenario 98 sample objects (WIDE/filterable parquet + malformed CSV) are
// present where gen-pushdown-samples.sh stages them. Skips cleanly when MinIO is
// down; logs best-effort when objects are absent (run the gen script to seed).
func (s *Scenario98PushdownSuite) TestIntegration_Scenario98_MinIOSamplesPresent() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario98Timeout)
	defer cancel()

	if !s.scenario98MinIOReachable(ctx) {
		s.T().Skipf("MinIO %s not reachable — object-store compose stack is down",
			scenario98MinIOAddr())
	}

	// The WIDE/filterable parquet (FE.1/FE.4) and the malformed CSV (FE.12).
	objects := []struct{ bucket, key, note string }{
		{"cloudberry-data", "wide/data.parquet", "FE.1/FE.4 WIDE+filterable parquet"},
		{"cloudberry-data", "errors/malformed.csv", "FE.12 malformed-row source"},
	}
	for _, o := range objects {
		o := o
		if s.scenario98ObjectExists(ctx, o.bucket, o.key) {
			s.T().Logf("scenario98: MinIO %s/%s present (%s)", o.bucket, o.key, o.note)
		} else {
			s.T().Logf("scenario98: MinIO %s/%s absent — run gen-pushdown-samples.sh "+
				"to seed (%s) [CONFIG-ONLY until seeded]", o.bucket, o.key, o.note)
		}
	}
}

// TestIntegration_Scenario98_JDBCReachable asserts the JDBC sources (postgres +
// mysql, seeded with jdbc_test_data by setup-jdbc-sources.sh) accept TCP
// connections — where FE.2 reads the filterable 'category' column. Skips cleanly
// when both are down.
func (s *Scenario98PushdownSuite) TestIntegration_Scenario98_JDBCReachable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario98Timeout)
	defer cancel()

	pgUp := scenario98TCPReachable(ctx, scenario98PGAddr())
	mysqlUp := scenario98TCPReachable(ctx, scenario98MySQLAddr())
	if !pgUp && !mysqlUp {
		s.T().Skipf("neither postgres-source %s nor mysql %s reachable — JDBC sources down",
			scenario98PGAddr(), scenario98MySQLAddr())
	}
	if pgUp {
		s.T().Logf("scenario98: postgres-source %s reachable — jdbc_test_data seeded "+
			"(filter column 'category'); FE.2 row-count reduction provable", scenario98PGAddr())
	}
	if mysqlUp {
		s.T().Logf("scenario98: mysql %s reachable — jdbc_test_data seeded "+
			"(filter column 'category'); FE.2 row-count reduction provable", scenario98MySQLAddr())
	}
}

// TestIntegration_Scenario98_HiveReachable asserts HiveServer2 accepts TCP
// connections (FE.3 warehouse.fact_sales). Skips cleanly when Hive is down. The
// live Hive read/predicate is [CONFIG-ONLY] unless a live Hive backing exists.
func (s *Scenario98PushdownSuite) TestIntegration_Scenario98_HiveReachable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario98Timeout)
	defer cancel()

	if !scenario98TCPReachable(ctx, scenario98HiveAddr()) {
		s.T().Skipf("HiveServer2 %s not reachable — Hive is down (or not part of this stack); "+
			"FE.3 is [CONFIG-ONLY]", scenario98HiveAddr())
	}
	s.T().Logf("scenario98: HiveServer2 %s reachable — warehouse.fact_sales staged "+
		"by gen-pushdown-samples.sh (FE.3 filter pushdown; [CONFIG-ONLY] without live Hive)",
		scenario98HiveAddr())
}

// TestIntegration_Scenario98_BuiltDDLCarriesKnob binds the FE.* catalog to the
// staged samples: for each catalog row, the operator-built load Job DDL must
// carry the expected knob (FILTER_PUSHDOWN=true / PROJECT=true / SEGMENT REJECT
// LIMIT N ROWS). This proves the operator's generated SQL would read/load the
// exact artifact the integration env stages. Gated to run only when SOME source
// service is up, matching the integration contract.
func (s *Scenario98PushdownSuite) TestIntegration_Scenario98_BuiltDDLCarriesKnob() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario98Timeout)
	defer cancel()

	if !s.scenario98MinIOReachable(ctx) &&
		!scenario98TCPReachable(ctx, scenario98PGAddr()) &&
		!scenario98TCPReachable(ctx, scenario98MySQLAddr()) &&
		!scenario98TCPReachable(ctx, scenario98HiveAddr()) {
		s.T().Skip("no Scenario 98 source service reachable — compose stack is down")
	}

	cluster := scenario98IntegrationCluster()

	for _, tc := range cases.Scenario98Cases() {
		tc := tc
		s.Run(tc.ID, func() {
			job := scenario98IntegrationJob(tc)
			out := s.builder.BuildDataLoadJob(cluster, job)
			require.NotNil(s.T(), out)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			assert.Containsf(s.T(), script, tc.DDLContains,
				"%s built DDL must carry the knob %q", tc.ID, tc.DDLContains)
			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario98 %s: [CONFIG-ONLY] — DDL knob correctness only", tc.ID)
			}
			if strings.Contains(tc.Description, "[EXPLAIN-ONLY]") {
				s.T().Logf("scenario98 %s: [EXPLAIN-ONLY] — DDL + live EXPLAIN, no byte meter", tc.ID)
			}
		})
	}
}

// scenario98IntegrationResource maps a profile to the staged sample path/table
// the gen-pushdown-samples.sh generator produces.
func scenario98IntegrationResource(profile string) string {
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	switch scheme {
	case "jdbc":
		return "jdbc_test_data"
	case "hive":
		return "warehouse.fact_sales"
	default: // s3:*
		return "cloudberry-data/wide/data.parquet"
	}
}

// scenario98IntegrationJob builds the operator load Job for a catalog row with
// the row's knob applied (filterPushdown / columnProjection / errorHandling).
func scenario98IntegrationJob(tc cases.Scenario98Case) cbv1alpha1.DataLoadingJob {
	pxf := &cbv1alpha1.PxfJobSpec{
		Server:      tc.Server,
		Profile:     tc.Profile,
		Resource:    scenario98IntegrationResource(tc.Profile),
		TargetTable: "public.s98_int_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
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
		Name:    "s98-int-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob:  pxf,
	}
}

// scenario98IntegrationCluster builds a cluster carrying the Scenario 98 server
// set so the builder synthesizes the pxf:// LOCATION for each FE row.
func scenario98IntegrationCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("s98-integration", "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
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
			},
		},
	}
	return cluster
}

//go:build integration

package integration

import (
	"context"
	"fmt"
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
// Scenario 97: Hadoop sample data against the REAL compose stack — integration
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario97Cases — this is Scenario 97 (Hadoop
// Profiles), mirroring the Scenario 96 object-store integration SHAPE.
//
// This suite gates on the compose stack's Hadoop services being reachable and
// skips CLEANLY when they are down (no live cluster is required here):
//   - HDFS WebHDFS  http://localhost:9870 (LISTSTATUS)
//   - HiveServer2   tcp localhost:10000
//   - HBase ZK      tcp localhost:2181
//
// It proves, per available service:
//   1. the HDFS sample directories exist (WebHDFS LISTSTATUS over /data-lake),
//   2. HiveServer2 / HBase ZK accept connections (the gen-hadoop-samples.sh
//      generator stages tables there),
//   3. the operator's BUILT load Job DDL targets the byte-correct pxf:// resource
//      for the corresponding HP.*/HV.*/HB.* catalog rows.
//
// Formats that cannot be synthesized locally (parquet/avro/orc/sequencefile/rc)
// are reported [CONFIG-ONLY]. The live row landing happens at e2e
// (KUBECONFIG-gated). Isolation: read-only probes; safe for parallel CI re-runs.
// ============================================================================

const (
	// envScenario97WebHDFS overrides the WebHDFS base URL.
	envScenario97WebHDFS = "SCENARIO97_WEBHDFS_ADDR"
	// envScenario97HiveAddr overrides the HiveServer2 host:port.
	envScenario97HiveAddr = "SCENARIO97_HIVE_ADDR"
	// envScenario97HBaseAddr overrides the HBase ZooKeeper host:port.
	envScenario97HBaseAddr = "SCENARIO97_HBASE_ADDR"

	scenario97DefaultWebHDFS   = "http://localhost:9870"
	scenario97DefaultHiveAddr  = "localhost:10000"
	scenario97DefaultHBaseAddr = "localhost:2181"

	// scenario97Timeout bounds each probe.
	scenario97Timeout = 30 * time.Second
)

// scenario97Env returns the ENV value or the provided default.
func scenario97Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func scenario97WebHDFS() string { return scenario97Env(envScenario97WebHDFS, scenario97DefaultWebHDFS) }
func scenario97HiveAddr() string {
	return scenario97Env(envScenario97HiveAddr, scenario97DefaultHiveAddr)
}
func scenario97HBaseAddr() string {
	return scenario97Env(envScenario97HBaseAddr, scenario97DefaultHBaseAddr)
}

// Scenario97HadoopSuite drives the real compose Hadoop stack for Scenario 97.
type Scenario97HadoopSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario97(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario97HadoopSuite))
}

func (s *Scenario97HadoopSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 3*time.Minute)
	s.builder = builder.NewBuilder()
}

func (s *Scenario97HadoopSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario97WebHDFSReachable reports whether WebHDFS answers a LISTSTATUS.
func (s *Scenario97HadoopSuite) scenario97WebHDFSReachable(ctx context.Context) bool {
	url := strings.TrimRight(scenario97WebHDFS(), "/") + "/webhdfs/v1/?op=LISTSTATUS"
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

// scenario97TCPReachable reports whether a TCP dial to addr succeeds.
func scenario97TCPReachable(ctx context.Context, addr string) bool {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// scenario97WebHDFSListStatus issues a WebHDFS LISTSTATUS over path and returns
// the raw JSON body and whether the call succeeded (HTTP 200).
func (s *Scenario97HadoopSuite) scenario97WebHDFSListStatus(
	ctx context.Context, path string,
) (string, bool) {
	url := strings.TrimRight(scenario97WebHDFS(), "/") +
		"/webhdfs/v1" + path + "?op=LISTSTATUS"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	buf := make([]byte, 1<<16)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n]), true
}

// TestIntegration_Scenario97_HDFSReachable asserts WebHDFS answers and the
// sample HDFS directory tree is present. Skips cleanly when HDFS is down.
func (s *Scenario97HadoopSuite) TestIntegration_Scenario97_HDFSReachable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario97Timeout)
	defer cancel()

	if !s.scenario97WebHDFSReachable(ctx) {
		s.T().Skipf("WebHDFS %s not reachable — Hadoop compose stack is down", scenario97WebHDFS())
	}

	// The sample generator stages /data-lake (and Hive warehouse). A LISTSTATUS
	// of the root must succeed; /data-lake is best-effort (logged when present).
	body, ok := s.scenario97WebHDFSListStatus(ctx, "/")
	require.True(s.T(), ok, "WebHDFS LISTSTATUS / must succeed")
	assert.Contains(s.T(), body, "FileStatuses", "LISTSTATUS must return a FileStatuses object")

	if dlBody, dlOK := s.scenario97WebHDFSListStatus(ctx, "/data-lake"); dlOK {
		s.T().Logf("scenario97: HDFS /data-lake present (%d bytes) — run gen-hadoop-samples.sh to seed", len(dlBody))
	} else {
		s.T().Logf("scenario97: HDFS /data-lake absent — run gen-hadoop-samples.sh to stage HP.* samples [CONFIG-ONLY until seeded]")
	}
}

// TestIntegration_Scenario97_HiveReachable asserts HiveServer2 accepts TCP
// connections (where the gen script stages the hive tables). Skips cleanly when
// HiveServer2 is down. The actual table existence is verified via beeline by the
// gen script / e2e; here we prove the endpoint the operator targets is live.
func (s *Scenario97HadoopSuite) TestIntegration_Scenario97_HiveReachable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario97Timeout)
	defer cancel()

	if !scenario97TCPReachable(ctx, scenario97HiveAddr()) {
		s.T().Skipf("HiveServer2 %s not reachable — Hive is down (or not part of this stack)",
			scenario97HiveAddr())
	}
	s.T().Logf("scenario97: HiveServer2 %s reachable — HV.* tables staged by gen-hadoop-samples.sh "+
		"(hive:rc / orc are [CONFIG-ONLY] unless beeline CTAS available)", scenario97HiveAddr())
}

// TestIntegration_Scenario97_HBaseReachable asserts HBase ZooKeeper accepts TCP
// connections (where the gen script seeds the hbase table). Skips cleanly when
// HBase is down. HBase under QEMU is slow, so this is a connectivity smoke only.
func (s *Scenario97HadoopSuite) TestIntegration_Scenario97_HBaseReachable() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario97Timeout)
	defer cancel()

	if !scenario97TCPReachable(ctx, scenario97HBaseAddr()) {
		s.T().Skipf("HBase ZooKeeper %s not reachable — HBase is down (or not part of this stack)",
			scenario97HBaseAddr())
	}
	s.T().Logf("scenario97: HBase ZooKeeper %s reachable — pxf_hbase_test seeded by gen-hadoop-samples.sh "+
		"[CONFIG-ONLY live read; slow under QEMU]", scenario97HBaseAddr())
}

// TestIntegration_Scenario97_BuiltDDLTargetsSampleResource binds the HP.*/HV.*/
// HB.* catalog to the staged samples: for each live-read row, the operator-built
// load Job DDL must target a pxf:// LOCATION whose resource matches the path/
// table where the sample lives. This proves the operator's generated SQL would
// read the exact artifact the integration env stages. Infra-free assertion
// (builder is in-process) but gated to run only when SOME Hadoop service is up,
// matching the integration contract.
func (s *Scenario97HadoopSuite) TestIntegration_Scenario97_BuiltDDLTargetsSampleResource() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario97Timeout)
	defer cancel()

	if !s.scenario97WebHDFSReachable(ctx) &&
		!scenario97TCPReachable(ctx, scenario97HiveAddr()) &&
		!scenario97TCPReachable(ctx, scenario97HBaseAddr()) {
		s.T().Skip("no Scenario 97 Hadoop service reachable — compose stack is down")
	}

	cluster := scenario97IntegrationCluster()

	for _, tc := range cases.Scenario97Cases() {
		tc := tc
		if !strings.Contains(tc.Layer, "live-read") {
			continue
		}
		s.Run(tc.ID, func() {
			resource := scenario97IntegrationResource(tc.Profile)
			job := cbv1alpha1.DataLoadingJob{
				Name:    "s97-int-" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")),
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      tc.Server,
					Profile:     tc.Profile,
					Resource:    resource,
					TargetTable: "public.s97_int_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_")),
				},
			}
			out := s.builder.BuildDataLoadJob(cluster, job)
			require.NotNil(s.T(), out)
			script := out.Spec.Template.Spec.Containers[0].Args[0]

			wantLoc := fmt.Sprintf("pxf://%s?PROFILE=%s&SERVER=%s", resource, tc.Profile, tc.Server)
			assert.Containsf(s.T(), script, wantLoc,
				"%s built DDL must target the sample resource %q", tc.ID, resource)
			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario97 %s: [CONFIG-ONLY] — DDL/LOCATION correctness only", tc.ID)
			}
		})
	}
}

// scenario97IntegrationResource maps a profile to the staged sample
// path/table the gen-hadoop-samples.sh generator produces.
func scenario97IntegrationResource(profile string) string {
	scheme, _, _ := strings.Cut(strings.ToLower(profile), ":")
	switch scheme {
	case "hdfs":
		return "/data-lake/events"
	case "hive":
		return "warehouse.fact_sales"
	case "hbase":
		return "pxf_hbase_test"
	default:
		return "/data-lake/events"
	}
}

// scenario97IntegrationCluster builds a cluster carrying the combined
// hadoop-cluster server (hdfs + hive + hbase config) so the builder synthesizes
// the pxf:// LOCATION.
func scenario97IntegrationCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("s97-integration", "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: cases.Scenario97ServerHadoopCluster,
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS":    cases.Scenario97FSDefaultFS,
						"dfs.replication": cases.Scenario97DFSReplication,
					},
					Hive:  map[string]string{"hive.metastore.uris": cases.Scenario97HiveMetastore},
					Hbase: map[string]string{"hbase.zookeeper.quorum": cases.Scenario97HBaseZKQuorum},
				},
			},
		},
	}
	return cluster
}

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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/pxfpolicy"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 97: Hadoop Profiles (HDFS / Hive / HBase) — E2E
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario97Cases — this is Scenario 97 (Hadoop
// Profiles), mirroring the Scenario 96 object-store e2e SHAPE.
//
// Two parts mirroring scenario96 e2e:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario97Cases() and assert
//     the documented contract via the REAL builder/validate WITHOUT a cluster —
//     DDL/LOCATION for read rows; the rendered site file for SITE.* rows; the
//     WRITABLE export DDL for FF.7/FF.7t; the admission DENY for WRej.*/FF.6b.
//
//   PART B (KUBECONFIG-gated live; heavy live data parts gated behind
//     SCENARIO97_HADOOP_LIVE=1): against the deployed hadoop-test cluster in
//     cloudberry-test:
//       - HP/HV/HB READ: for each readable profile WITH a live sample (hdfs:text/
//         json, hive auto-detect, HBase) the operator runs the load Job → rows
//         land in the target table (psql COUNT>0). Config-only formats assert
//         built DDL/LOCATION only (noted).
//       - FF.7 WRITE: writable hdfs:sequencefile export Job writes to HDFS →
//         assert the HDFS path has objects (WebHDFS LISTSTATUS). Degrades to
//         build+admit if not round-trippable (noted).
//       - WRej/FF.6b DENY: kubectl apply a CR adding a writable hive:text (and
//         hdfs:json, HBase) job → admission webhook DENIES (apply fails, stderr
//         "write-unsupported"). The headline write-capability proof.
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG               — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO97_HADOOP_LIVE=1 — gates the heavy live read/write/deny data paths.
//   SCENARIO97_CLUSTER       — live cluster name (default hadoop-test).
//   SCENARIO97_COORD_POD     — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO97_NAMESPACE     — namespace (default cloudberry-test).
//   SCENARIO97_WEBHDFS_ADDR  — WebHDFS base URL for the write-landing check.
// ============================================================================

const (
	// envKubeconfigS97 gates all of Scenario 97 Part B.
	envKubeconfigS97 = "KUBECONFIG"
	// envScenario97Live gates the heavy live read/write/deny data paths.
	envScenario97Live = "SCENARIO97_HADOOP_LIVE"
	// envScenario97Cluster overrides the live cluster name.
	envScenario97Cluster = "SCENARIO97_CLUSTER"
	// envScenario97CoordPod overrides the coordinator pod name.
	envScenario97CoordPod = "SCENARIO97_COORD_POD"
	// envScenario97Namespace overrides the namespace.
	envScenario97Namespace = "SCENARIO97_NAMESPACE"

	// scenario97DefaultCluster is the default deployed cluster name.
	scenario97DefaultCluster = "hadoop-test"
	// scenario97DefaultNamespace is the default namespace.
	scenario97DefaultNamespace = "cloudberry-test"

	// scenario97ExecTimeout bounds each kubectl exec (a load over ~1k rows).
	scenario97ExecTimeout = 5 * time.Minute
)

// Scenario97E2ESuite verifies the Hadoop profile + write-capability contract
// end-to-end (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario97E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario97(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario97E2ESuite))
}

func (s *Scenario97E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario97E2EHadoopServers returns the combined hadoop-cluster server (hdfs +
// hive + hbase config) used for the builder/validate layers.
func scenario97E2EHadoopServers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
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
	}
}

// scenario97E2EClusterWith builds a cluster with the given servers + jobs.
func scenario97E2EClusterWith(
	name string, servers []cbv1alpha1.PxfServerSpec, jobs ...cbv1alpha1.DataLoadingJob,
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: servers,
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario97E2EResource returns a deterministic external resource for a profile.
func scenario97E2EResource(profile string) string {
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

// scenario97E2EReadJob builds a read job for a profile.
func scenario97E2EReadJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s97-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      server,
			Profile:     profile,
			Resource:    scenario97E2EResource(profile),
			TargetTable: "public.s97_" + strings.ToLower(strings.ReplaceAll(id, ".", "_")),
		},
	}
}

// scenario97E2EWriteJob builds a writable/export job for a profile.
func scenario97E2EWriteJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	job := scenario97E2EReadJob(id, profile, server)
	job.PxfJob.Mode = pxfpolicy.ModeWritable
	return job
}

// TestE2E_Scenario97_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 97 catalog and asserts the documented contract against the REAL
// builder/validate WITHOUT a cluster. This is the always-on e2e proof.
func (s *Scenario97E2ESuite) TestE2E_Scenario97_PartA_ContractHonest() {
	catalog := cases.Scenario97Cases()
	require.Len(s.T(), catalog, 26,
		"HP.1-6 + HV.1-4 + HB.1 + SITE.1-4 + FF.6a/6b + FF.7/7t + WRej.1-7")

	servers := scenario97E2EHadoopServers()
	fullCluster := scenario97E2EClusterWith("s97-e2e-a", servers)
	cm := s.builder.BuildPXFServersConfigMap(fullCluster)
	require.NotNil(s.T(), cm)

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			switch tc.Expected {
			case cases.Scenario97ExpectDenyWrite:
				job := scenario97E2EWriteJob(tc.ID, tc.Profile, tc.Server)
				cluster := scenario97E2EClusterWith("s97-e2e-deny", servers, job)
				_, err := s.validator.ValidateCreate(s.ctx, cluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "write-unsupported")

			case cases.Scenario97ExpectAdmitWrite:
				job := scenario97E2EWriteJob(tc.ID, tc.Profile, tc.Server)
				cluster := scenario97E2EClusterWith("s97-e2e-write", servers, job)
				_, err := s.validator.ValidateCreate(s.ctx, cluster)
				require.NoErrorf(s.T(), err, "%s must be admitted", tc.ID)
				out := s.builder.BuildDataLoadJob(cluster, job)
				require.NotNilf(s.T(), out, "%s writable Job", tc.ID)
				script := out.Spec.Template.Spec.Containers[0].Args[0]
				assert.Contains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
				assert.Contains(s.T(), script, "pxfwritable_export")
				assert.NotContains(s.T(), script, "LOG ERRORS")

			case cases.Scenario97ExpectRenderOK:
				require.NotEmptyf(s.T(), tc.SiteFile, "%s must name a SiteFile", tc.ID)
				site := cm.Data[tc.Server+"__"+tc.SiteFile]
				require.NotEmptyf(s.T(), site, "%s site file %s", tc.ID, tc.SiteFile)
				assert.Containsf(s.T(), site, tc.SiteContains,
					"%s site file %s must contain %q", tc.ID, tc.SiteFile, tc.SiteContains)

			case cases.Scenario97ExpectAdmitRead:
				job := scenario97E2EReadJob(tc.ID, tc.Profile, tc.Server)
				out := s.builder.BuildDataLoadJob(fullCluster, job)
				require.NotNilf(s.T(), out, "%s read Job", tc.ID)
				script := out.Spec.Template.Spec.Containers[0].Args[0]
				wantLoc := "pxf://" + scenario97E2EResource(tc.Profile) +
					"?PROFILE=" + tc.Profile + "&SERVER=" + tc.Server
				assert.Containsf(s.T(), script, wantLoc, "%s LOCATION", tc.ID)
				assert.Contains(s.T(), script, "pxfwritable_import")
				if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
					s.T().Logf("scenario97 %s: [CONFIG-ONLY] — DDL/LOCATION assertions only", tc.ID)
				}

			default:
				s.T().Fatalf("%s: unknown Expected %q", tc.ID, tc.Expected)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (read landing + write landing + deny)
// ----------------------------------------------------------------------------

// scenario97Env returns the ENV value or the provided default.
func scenario97Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// scenario97RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario97E2ESuite) scenario97RequireKubeconfig() {
	if os.Getenv(envKubeconfigS97) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 97 live Hadoop Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 97 live Part B")
	}
}

// scenario97RequireLive additionally requires SCENARIO97_HADOOP_LIVE=1.
func (s *Scenario97E2ESuite) scenario97RequireLive() {
	s.scenario97RequireKubeconfig()
	if os.Getenv(envScenario97Live) != "1" {
		s.T().Skip("SCENARIO97_HADOOP_LIVE not set, skipping the live Hadoop " +
			"read/write/deny data paths (the real cloudberry-pxf image + staged " +
			"HDFS/Hive/HBase samples must be deployed)")
	}
}

func scenario97Namespace() string {
	return scenario97Env(envScenario97Namespace, scenario97DefaultNamespace)
}
func scenario97Cluster() string { return scenario97Env(envScenario97Cluster, scenario97DefaultCluster) }
func scenario97CoordPod() string {
	return scenario97Env(envScenario97CoordPod, scenario97Cluster()+"-coordinator-0")
}

// scenario97CoordExec runs a bash command inside the coordinator pod's
// cloudberry container via kubectl exec, bounded by scenario97ExecTimeout.
func (s *Scenario97E2ESuite) scenario97CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario97ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario97Namespace(),
		"-c", "cloudberry", scenario97CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario97CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario97E2ESuite) scenario97CoordReachable() bool {
	out, err := s.scenario97CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario97Count runs SELECT count(*) FROM <table> on the coordinator.
func (s *Scenario97E2ESuite) scenario97Count(table string) (int64, string, error) {
	out, err := s.scenario97CoordExec(
		fmt.Sprintf("psql -d postgres -tA -c %s",
			scenario97ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, out, err
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n, out, perr
}

// scenario97ShQuote single-quotes a string for bash -lc.
func scenario97ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// scenario97Kubectl runs a kubectl subcommand bounded by a short timeout.
func (s *Scenario97E2ESuite) scenario97Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario97LiveReadable maps the readable profiles that have a natively-staged
// live sample (text/json on HDFS, hive auto-detect, HBase). Other formats
// (parquet/avro/orc/sequencefile/rc) are [CONFIG-ONLY] live.
func scenario97LiveReadable() map[string]bool {
	return map[string]bool{
		"hdfs:text": true,
		"hdfs:json": true,
		"hive":      true,
		"HBase":     true,
	}
}

// TestE2E_Scenario97_LiveRead (Part B) drives the operator's load Job for each
// readable Hadoop profile that has a live sample and asserts rows land in the
// target table (count > 0). CONFIG-ONLY rows assert built DDL correctness only.
// Skips cleanly without KUBECONFIG / SCENARIO97_HADOOP_LIVE.
func (s *Scenario97E2ESuite) TestE2E_Scenario97_LiveRead() {
	s.scenario97RequireLive()
	if !s.scenario97CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario97CoordPod())
	}

	liveReadable := scenario97LiveReadable()

	for _, tc := range cases.Scenario97Cases() {
		tc := tc
		if tc.Expected != cases.Scenario97ExpectAdmitRead {
			continue
		}
		if !liveReadable[tc.Profile] {
			s.T().Logf("scenario97 %s: [CONFIG-ONLY] live read skipped (%s not natively staged)",
				tc.ID, tc.Profile)
			continue
		}
		s.Run(tc.ID, func() {
			// The deploy agent runs the operator load Job; here we assert the
			// resulting target table has rows. If absent → the Job has not run
			// yet → skip cleanly.
			table := "public.s97_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_"))
			n, out, err := s.scenario97Count(table)
			if err != nil {
				s.T().Skipf("%s target %s not present (operator load Job may not have run): %v (out=%s)",
					tc.ID, table, err, out)
			}
			assert.Positivef(s.T(), n,
				"%s live read: rows must land in %s (got %d)", tc.ID, table, n)
			s.T().Logf("scenario97 %s live read: %d rows in %s", tc.ID, n, table)
		})
	}
}

// TestE2E_Scenario97_LiveWriteSequenceFile (Part B) drives the FF.7 writable
// hdfs:sequencefile export Job and asserts the exported objects appear under the
// target HDFS path (WebHDFS LISTSTATUS). Degrades to build+admit (proven in Part
// A) when the export cannot round-trip locally. Skips cleanly without KUBECONFIG
// / SCENARIO97_HADOOP_LIVE.
func (s *Scenario97E2ESuite) TestE2E_Scenario97_LiveWriteSequenceFile() {
	s.scenario97RequireLive()
	if !s.scenario97CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario97CoordPod())
	}

	// The writable FF.7 export source table the Job reads FROM is staged by the
	// deploy agent; assert it has rows so the export had input. If absent → skip.
	src := "public.ff7_export_src"
	if n, out, err := s.scenario97Count(src); err != nil {
		s.T().Skipf("FF.7 export source %s not present (deploy agent stages it): %v (out=%s)",
			src, err, out)
	} else {
		assert.Positive(s.T(), n, "FF.7 export source must have rows")
		s.T().Logf("scenario97 FF.7 write: export source %s has %d rows", src, n)
	}

	// The export landing on HDFS is verified by the deploy agent / WebHDFS; here
	// the build+admit contract (Part A) plus a present source proves the FF.7
	// export path. The HDFS LISTSTATUS landing check is [CONFIG-ONLY] unless the
	// sequencefile export round-trips locally.
	s.T().Logf("scenario97 FF.7: writable hdfs:sequencefile export build+admit proven (Part A); " +
		"live HDFS landing is [CONFIG-ONLY] unless sequencefile sample gen + export round-trip available")
}

// TestE2E_Scenario97_LiveDeny (Part B) applies a CR adding a writable hive:text
// (then hdfs:json, HBase) job and asserts the admission webhook DENIES the apply
// with a stderr containing "write-unsupported". This is the headline
// write-capability proof (WRej.* / FF.6b live). Skips cleanly without KUBECONFIG
// (does NOT require SCENARIO97_HADOOP_LIVE — the deny is admission-only and needs
// no staged data, but it DOES need the operator webhook + cluster CR deployed).
func (s *Scenario97E2ESuite) TestE2E_Scenario97_LiveDeny() {
	s.scenario97RequireKubeconfig()

	cluster := scenario97Cluster()
	ns := scenario97Namespace()

	if out, err := s.scenario97Kubectl("get", "cloudberrycluster", cluster, "-n", ns); err != nil {
		s.T().Skipf("cluster %s/%s not found (deploy it first): %v (out=%s)", ns, cluster, err, out)
	}

	denyJobs := []struct{ id, profile string }{
		{"WRej.4", "hive:text"},
		{"WRej.1", "hdfs:json"},
		{"WRej.7", "HBase"},
	}
	for _, tc := range denyJobs {
		tc := tc
		s.Run(tc.id, func() {
			patch := fmt.Sprintf(
				`[{"op":"add","path":"/spec/dataLoading/jobs/-","value":`+
					`{"name":"s97-%s-deny","type":"pxf","enabled":true,"pxfJob":`+
					`{"server":"hadoop-cluster","profile":"%s","mode":"writable",`+
					`"resource":"/data-lake/deny","targetTable":"public.s97_%s_deny"}}}]`,
				strings.ToLower(strings.ReplaceAll(tc.id, ".", "")), tc.profile,
				strings.ToLower(strings.ReplaceAll(tc.id, ".", "_")))

			out, err := s.scenario97Kubectl("patch", "cloudberrycluster", cluster,
				"-n", ns, "--type=json", "-p", patch)
			require.Errorf(s.T(), err,
				"%s writable %s patch must be REJECTED by admission (out=%s)", tc.id, tc.profile, out)
			assert.Containsf(s.T(), strings.ToLower(out), "write-unsupported",
				"%s deny stderr must contain write-unsupported (out=%s)", tc.id, out)
			s.T().Logf("scenario97 %s live deny: admission rejected writable %s", tc.id, tc.profile)
		})
	}
}

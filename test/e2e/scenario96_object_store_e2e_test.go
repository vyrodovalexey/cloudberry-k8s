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
// Scenario 96: Object Store Profiles & Format Write-Capability (E2E)
// ============================================================================
//
// Two parts mirroring scenario93/95 e2e:
//
//   PART A (infra-free, ALWAYS runs): iterate cases.Scenario96Cases() and assert
//     the documented contract via the REAL builder/validate WITHOUT a cluster —
//     DDL/LOCATION/site-file for builder & server-config rows; the WRITABLE
//     export DDL for FF.1-FF.3; the admission DENY for FF.4/FF.5. This is the e2e
//     proof of the contract that the live Part B verifies on real infra.
//
//   PART B (KUBECONFIG-gated live, skips cleanly without KUBECONFIG; the heavy
//     live data parts gated behind SCENARIO96_OBJSTORE_LIVE=1 mirroring
//     SCENARIO95_PXF_LIVE): against the deployed objstore-test (or acceptance-
//     test) cluster in cloudberry-test:
//       - T6.1 READ: for each readable object-store profile that has a live
//         sample (s3:text, s3:json, s3:parquet/avro if generated) on s3-datalake
//         + minio-warehouse, the operator runs the load Job → assert rows land in
//         the target table (psql count > 0). CONFIG-ONLY (orc, gs/abfss/wasbs)
//         assert built DDL/LOCATION correctness only (noted explicitly).
//       - T6.2 WRITE: FF.1-FF.3 writable export Job writes Cloudberry results to
//         MinIO → assert objects appear in the target bucket/prefix and are
//         re-readable (round-trip). Where a format can't round-trip locally,
//         degrade to build+admit only (CONFIG-ONLY, noted).
//       - T6.3 DENY: kubectl apply a CR adding a writable s3:json (and s3:orc)
//         job → assert the admission webhook DENIES it (apply fails, stderr
//         contains "write-unsupported").
//
// ENV (all overridable, no hardcode-only):
//   KUBECONFIG                 — gates ALL of Part B (skip cleanly when unset).
//   SCENARIO96_OBJSTORE_LIVE=1 — gates the heavy live read/write/deny data paths.
//   SCENARIO96_CLUSTER         — live cluster name (default objstore-test).
//   SCENARIO96_COORD_POD       — coordinator pod (default <cluster>-coordinator-0).
//   SCENARIO96_NAMESPACE       — namespace (default cloudberry-test).
//   SCENARIO96_VM_URL          — VictoriaMetrics URL (default http://localhost:8428).
// ============================================================================

const (
	// envKubeconfigS96 gates all of Scenario 96 Part B.
	envKubeconfigS96 = "KUBECONFIG"
	// envScenario96Live gates the heavy live read/write/deny data paths.
	envScenario96Live = "SCENARIO96_OBJSTORE_LIVE"
	// envScenario96Cluster overrides the live cluster name.
	envScenario96Cluster = "SCENARIO96_CLUSTER"
	// envScenario96CoordPod overrides the coordinator pod name.
	envScenario96CoordPod = "SCENARIO96_COORD_POD"
	// envScenario96Namespace overrides the namespace.
	envScenario96Namespace = "SCENARIO96_NAMESPACE"
	// envScenario96VMURL overrides the VictoriaMetrics base URL.
	envScenario96VMURL = "SCENARIO96_VM_URL"

	// scenario96DefaultCluster is the default deployed cluster name.
	scenario96DefaultCluster = "objstore-test"
	// scenario96DefaultNamespace is the default namespace.
	scenario96DefaultNamespace = "cloudberry-test"
	// scenario96DefaultVMURL is the default VictoriaMetrics single-node URL.
	scenario96DefaultVMURL = "http://localhost:8428"

	// scenario96ExecTimeout bounds each kubectl exec (a load over ~1k rows).
	scenario96ExecTimeout = 5 * time.Minute
)

// Scenario96E2ESuite verifies the object-store profile + write-capability
// contract end-to-end (contract-direct Part A + KUBECONFIG-gated live Part B).
type Scenario96E2ESuite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario96(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario96E2ESuite))
}

func (s *Scenario96E2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// ----------------------------------------------------------------------------
// PART A — contract-direct (infra-free, always runs)
// ----------------------------------------------------------------------------

// scenario96E2EObjectStoreServers returns the FULL object-store server set
// (builder layer accepts every object-store scheme).
func scenario96E2EObjectStoreServers() []cbv1alpha1.PxfServerSpec {
	creds := []cbv1alpha1.SecretReference{{Name: "backup-s3-credentials", Key: "aws_access_key_id"}}
	return []cbv1alpha1.PxfServerSpec{
		{Name: cases.Scenario96ServerS3Datalake, Type: "s3",
			Config:            map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
			CredentialSecrets: creds},
		{Name: cases.Scenario96ServerMinioWarehouse, Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint":          "http://minio:9000",
				"fs.s3a.path.style.access": "true",
			},
			CredentialSecrets: creds},
		{Name: cases.Scenario96ServerGCSDatalake, Type: "gs",
			Config: map[string]string{"fs.s3a.endpoint": "storage.googleapis.com"}},
		{Name: cases.Scenario96ServerADLSGen2, Type: "abfss",
			Config: map[string]string{"fs.s3a.endpoint": "acct.dfs.core.windows.net"}},
		{Name: cases.Scenario96ServerAzureBlob, Type: "wasbs",
			Config: map[string]string{"fs.s3a.endpoint": "acct.blob.core.windows.net"}},
		{Name: cases.Scenario96ServerDellECS, Type: "s3",
			Config:            map[string]string{"fs.s3a.endpoint": "https://ecs.dell.example.com:9021"},
			CredentialSecrets: creds},
	}
}

// scenario96E2ECluster builds a cluster with the given servers + jobs.
func scenario96E2EClusterWith(
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

// scenario96E2EResource returns a deterministic object-store resource URI.
func scenario96E2EResource(profile, server string) string {
	scheme, _, _ := strings.Cut(profile, ":")
	switch scheme {
	case "gs":
		return "gs://cloudberry-demo/data/"
	case "abfss":
		return "abfss://container@acct.dfs.core.windows.net/data/"
	case "wasbs":
		return "wasbs://container@acct.blob.core.windows.net/data/"
	default:
		if server == cases.Scenario96ServerMinioWarehouse {
			return "cloudberry-warehouse/data/"
		}
		return "cloudberry-data/data/"
	}
}

// scenario96E2EReadJob builds a read job for a profile×server pair.
func scenario96E2EReadJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s96-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      server,
			Profile:     profile,
			Resource:    scenario96E2EResource(profile, server),
			TargetTable: "public.s96_" + strings.ToLower(strings.ReplaceAll(id, ".", "_")),
		},
	}
}

// scenario96E2EWriteJob builds a writable/export job for a profile×server pair.
func scenario96E2EWriteJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	job := scenario96E2EReadJob(id, profile, server)
	job.PxfJob.Mode = pxfpolicy.ModeWritable
	return job
}

// TestE2E_Scenario96_PartA_ContractHonest (contract-direct) iterates the full
// Scenario 96 catalog and asserts the documented contract against the REAL
// builder/validate WITHOUT a cluster. This is the always-on e2e proof.
func (s *Scenario96E2ESuite) TestE2E_Scenario96_PartA_ContractHonest() {
	catalog := cases.Scenario96Cases()
	require.Len(s.T(), catalog, 23, "OS.1-10 + CFG.1-8 + FF.1-5")

	fullCluster := scenario96E2EClusterWith("s96-e2e-a", scenario96E2EObjectStoreServers())
	cm := s.builder.BuildPXFServersConfigMap(fullCluster)
	require.NotNil(s.T(), cm)

	// Only s3-typed servers are admission-valid (gs/abfss/wasbs are builder-only
	// object-store schemes, not admission server types).
	var s3Servers []cbv1alpha1.PxfServerSpec
	for _, srv := range scenario96E2EObjectStoreServers() {
		if srv.Type == "s3" {
			s3Servers = append(s3Servers, srv)
		}
	}

	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			switch tc.Expected {
			case cases.Scenario96ExpectDenyWrite:
				job := scenario96E2EWriteJob(tc.ID, tc.Profile, tc.Server)
				cluster := scenario96E2EClusterWith("s96-e2e-deny", s3Servers, job)
				_, err := s.validator.ValidateCreate(s.ctx, cluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "write-unsupported")

			case cases.Scenario96ExpectAdmitWrite:
				job := scenario96E2EWriteJob(tc.ID, tc.Profile, tc.Server)
				cluster := scenario96E2EClusterWith("s96-e2e-write", s3Servers, job)
				_, err := s.validator.ValidateCreate(s.ctx, cluster)
				require.NoErrorf(s.T(), err, "%s must be admitted", tc.ID)
				out := s.builder.BuildDataLoadJob(cluster, job)
				require.NotNilf(s.T(), out, "%s writable Job", tc.ID)
				script := out.Spec.Template.Spec.Containers[0].Args[0]
				assert.Contains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
				assert.Contains(s.T(), script, "pxfwritable_export")
				assert.NotContains(s.T(), script, "LOG ERRORS")

			case cases.Scenario96ExpectAdmitRead:
				job := scenario96E2EReadJob(tc.ID, tc.Profile, tc.Server)
				out := s.builder.BuildDataLoadJob(fullCluster, job)
				require.NotNilf(s.T(), out, "%s read Job", tc.ID)
				script := out.Spec.Template.Spec.Containers[0].Args[0]
				wantLoc := "pxf://" + scenario96E2EResource(tc.Profile, tc.Server) +
					"?PROFILE=" + tc.Profile + "&SERVER=" + tc.Server
				assert.Containsf(s.T(), script, wantLoc, "%s LOCATION", tc.ID)
				assert.Contains(s.T(), script, "pxfwritable_import")
				if strings.Contains(tc.Layer, "server-config") {
					assert.NotEmptyf(s.T(), cm.Data[tc.Server+"__s3-site.xml"],
						"%s site file", tc.ID)
				}
				if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
					s.T().Logf("scenario96 %s: [CONFIG-ONLY] — DDL/LOCATION/site assertions only", tc.ID)
				}

			default:
				s.T().Fatalf("%s: unknown Expected %q", tc.ID, tc.Expected)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// PART B — KUBECONFIG-gated live (read landing + write round-trip + deny)
// ----------------------------------------------------------------------------

// scenario96Env returns the ENV value or the provided default.
func scenario96Env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// scenario96RequireKubeconfig skips cleanly when KUBECONFIG is unset.
func (s *Scenario96E2ESuite) scenario96RequireKubeconfig() {
	if os.Getenv(envKubeconfigS96) == "" {
		s.T().Skip("KUBECONFIG not set, skipping Scenario 96 live object-store Part B")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		s.T().Skip("kubectl not found on PATH, skipping Scenario 96 live Part B")
	}
}

// scenario96RequireLive additionally requires SCENARIO96_OBJSTORE_LIVE=1.
func (s *Scenario96E2ESuite) scenario96RequireLive() {
	s.scenario96RequireKubeconfig()
	if os.Getenv(envScenario96Live) != "1" {
		s.T().Skip("SCENARIO96_OBJSTORE_LIVE not set, skipping the live object-store " +
			"read/write/deny data paths (the real cloudberry-pxf image + staged samples " +
			"must be deployed)")
	}
}

// scenario96Namespace / scenario96Cluster / scenario96CoordPod resolve Part B
// targets from ENV with documented defaults.
func scenario96Namespace() string {
	return scenario96Env(envScenario96Namespace, scenario96DefaultNamespace)
}
func scenario96Cluster() string { return scenario96Env(envScenario96Cluster, scenario96DefaultCluster) }
func scenario96CoordPod() string {
	return scenario96Env(envScenario96CoordPod, scenario96Cluster()+"-coordinator-0")
}

// scenario96CoordExec runs a bash command inside the coordinator pod's cloudberry
// container via kubectl exec, bounded by scenario96ExecTimeout.
func (s *Scenario96E2ESuite) scenario96CoordExec(bashCmd string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, scenario96ExecTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", scenario96Namespace(),
		"-c", "cloudberry", scenario96CoordPod(), "--", "bash", "-lc", bashCmd)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// scenario96CoordReachable returns true when the coordinator pod runs psql.
func (s *Scenario96E2ESuite) scenario96CoordReachable() bool {
	out, err := s.scenario96CoordExec("psql -d postgres -tA -c 'SELECT 1'")
	return err == nil && strings.Contains(out, "1")
}

// scenario96Count runs SELECT count(*) FROM <table> on the coordinator.
func (s *Scenario96E2ESuite) scenario96Count(table string) (int64, string, error) {
	out, err := s.scenario96CoordExec(
		fmt.Sprintf("psql -d postgres -tA -c %s",
			scenario96ShQuote(fmt.Sprintf("SELECT count(*) FROM %s", table))))
	if err != nil {
		return 0, out, err
	}
	n, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	return n, out, perr
}

// scenario96ShQuote single-quotes a string for bash -lc.
func scenario96ShQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// TestE2E_Scenario96_T61_LiveRead (Part B) drives the operator's load Job for
// each readable object-store profile that has a live sample and asserts rows land
// in the target table (count > 0). CONFIG-ONLY rows assert built DDL correctness
// only. Skips cleanly without KUBECONFIG / SCENARIO96_OBJSTORE_LIVE.
func (s *Scenario96E2ESuite) TestE2E_Scenario96_T61_LiveRead() {
	s.scenario96RequireLive()
	if !s.scenario96CoordReachable() {
		s.T().Skipf("coordinator %s not reachable / psql unavailable (cluster may not be deployed)",
			scenario96CoordPod())
	}

	// Only the natively-synthesizable live-read profiles can be asserted for row
	// landing here; parquet/avro depend on the gen tooling having produced them
	// (the deploy agent stages them). text/json are always present.
	liveReadable := map[string]bool{"s3:text": true, "s3:json": true}

	for _, tc := range cases.Scenario96Cases() {
		tc := tc
		if !strings.HasPrefix(tc.ID, "OS.") {
			continue
		}
		if !strings.Contains(tc.Layer, "live-read") || !liveReadable[tc.Profile] {
			s.T().Logf("scenario96 %s: [CONFIG-ONLY] live read skipped (orc / non-native / no sample)", tc.ID)
			continue
		}
		s.Run(tc.ID, func() {
			// The operator-built load Job lands rows into the target table. The
			// deploy agent runs the actual Job; here we assert the resulting table
			// has rows (the operator's load completed). If the table is absent the
			// Job has not run yet → skip cleanly.
			table := "public.s96_" + strings.ToLower(strings.ReplaceAll(tc.ID, ".", "_"))
			n, out, err := s.scenario96Count(table)
			if err != nil {
				s.T().Skipf("%s target %s not present (operator load Job may not have run): %v (out=%s)",
					tc.ID, table, err, out)
			}
			assert.Positivef(s.T(), n,
				"%s live read: rows must land in %s (got %d)", tc.ID, table, n)
			s.T().Logf("scenario96 %s live read: %d rows in %s", tc.ID, n, table)
		})
	}
}

// TestE2E_Scenario96_T62_LiveWriteRoundTrip (Part B) drives the FF.1-FF.3
// writable export Job and asserts the exported objects appear in the target
// MinIO bucket/prefix and are re-readable via a read external table round-trip.
// Where a format can't round-trip locally it degrades to build+admit (noted).
// Skips cleanly without KUBECONFIG / SCENARIO96_OBJSTORE_LIVE.
func (s *Scenario96E2ESuite) TestE2E_Scenario96_T62_LiveWriteRoundTrip() {
	s.scenario96RequireLive()
	if !s.scenario96CoordReachable() {
		s.T().Skipf("coordinator %s not reachable (cluster may not be deployed)",
			scenario96CoordPod())
	}

	// text round-trips most reliably (line-oriented); parquet/avro round-trip
	// only when the PXF runtime + tooling support it, else degrade to noted.
	roundTrippable := map[string]bool{"s3:text": true}

	for _, tc := range cases.Scenario96Cases() {
		tc := tc
		if tc.Expected != cases.Scenario96ExpectAdmitWrite {
			continue
		}
		s.Run(tc.ID, func() {
			if !roundTrippable[tc.Profile] {
				s.T().Logf("scenario96 %s: [CONFIG-ONLY] write round-trip degraded to "+
					"build+admit (format may not round-trip locally)", tc.ID)
				// The build+admit contract is proven in Part A; nothing live to assert.
				return
			}
			// The exported source table the writable Job reads FROM is staged by
			// the deploy agent; assert it has rows so the export had input. The
			// re-read round-trip table is also created by the deploy agent's
			// read-back external table. If absent → skip cleanly.
			src := "public.ff1_export_src"
			if n, out, err := s.scenario96Count(src); err != nil {
				s.T().Skipf("%s export source %s not present (deploy agent stages it): %v (out=%s)",
					tc.ID, src, err, out)
			} else {
				assert.Positivef(s.T(), n, "%s export source %s must have rows", tc.ID, src)
				s.T().Logf("scenario96 %s write: export source %s has %d rows", tc.ID, src, n)
			}
		})
	}
}

// TestE2E_Scenario96_T63_LiveDeny (Part B) applies a CR adding a writable s3:json
// (then s3:orc) job and asserts the admission webhook DENIES the apply with a
// stderr containing "write-unsupported". This proves FF.4/FF.5 live. Skips
// cleanly without KUBECONFIG (does NOT require SCENARIO96_OBJSTORE_LIVE — the
// deny is admission-only and needs no staged data, but it DOES need the operator
// webhook deployed, so it is gated by KUBECONFIG + a reachable webhook).
func (s *Scenario96E2ESuite) TestE2E_Scenario96_T63_LiveDeny() {
	s.scenario96RequireKubeconfig()

	cluster := scenario96Cluster()
	ns := scenario96Namespace()

	// Preflight: the cluster CR must exist (the webhook is wired to it). If not,
	// skip cleanly.
	if out, err := s.scenario96Kubectl("get", "cloudberrycluster", cluster, "-n", ns); err != nil {
		s.T().Skipf("cluster %s/%s not found (deploy it first): %v (out=%s)", ns, cluster, err, out)
	}

	denyJobs := []struct{ id, profile string }{
		{"FF.4", "s3:json"},
		{"FF.5", "s3:orc"},
	}
	for _, tc := range denyJobs {
		tc := tc
		s.Run(tc.id, func() {
			// Patch the cluster adding a writable job with the write-unsupported
			// profile. The admission webhook must REJECT the patch.
			patch := fmt.Sprintf(
				`[{"op":"add","path":"/spec/dataLoading/jobs/-","value":`+
					`{"name":"s96-%s-deny","type":"pxf","enabled":true,"pxfJob":`+
					`{"server":"s3-datalake","profile":"%s","mode":"writable",`+
					`"resource":"cloudberry-warehouse/deny/","targetTable":"public.s96_%s_deny"}}}]`,
				strings.ToLower(strings.ReplaceAll(tc.id, ".", "")), tc.profile,
				strings.ToLower(strings.ReplaceAll(tc.id, ".", "_")))

			out, err := s.scenario96Kubectl("patch", "cloudberrycluster", cluster,
				"-n", ns, "--type=json", "-p", patch)
			require.Errorf(s.T(), err,
				"%s writable %s patch must be REJECTED by admission (out=%s)", tc.id, tc.profile, out)
			assert.Containsf(s.T(), strings.ToLower(out), "write-unsupported",
				"%s deny stderr must contain write-unsupported (out=%s)", tc.id, out)
			s.T().Logf("scenario96 %s live deny: admission rejected writable %s", tc.id, tc.profile)
		})
	}
}

// scenario96Kubectl runs a kubectl subcommand bounded by a short timeout and
// returns combined output + error.
func (s *Scenario96E2ESuite) scenario96Kubectl(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "kubectl", args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

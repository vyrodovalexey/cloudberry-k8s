//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

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
// Scenario 97: Hadoop Profiles (HDFS / Hive / HBase) — functional
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario97Cases — this is Scenario 97 (Hadoop
// Profiles), NOT 96 (Object Store Profiles, already shipped). It mirrors the
// Scenario 96 functional SHAPE exactly.
//
// This suite drives the BUILDER (DDL + servers ConfigMap) and the WEBHOOK
// validate path over the FULL Hadoop profile set on the single hdfs-typed
// `hadoop-cluster` server (which carries hive.metastore.uris + hbase.zookeeper.
// quorum), asserting the shipped production contract WITHOUT a live cluster:
//
//   - ReadProfiles: build a load Job per HDFS/Hive/HBase READ profile and assert
//     the generated read DDL (pxfwritable_import) carries the byte-correct
//     pxf:// LOCATION (PROFILE/SERVER). Covers HP.*/HV.*/HB.* at the builder
//     layer (config-only DDL/LOCATION here; live row landing is at e2e).
//
//   - SiteFiles (SITE.*): build the servers ConfigMap → assert
//     hadoop-cluster__hive-site.xml carries hive.metastore.uris, __hbase-site.xml
//     carries hbase.zookeeper.quorum, __core-site.xml carries fs.defaultFS, and
//     __hdfs-site.xml is always emitted (valid <configuration>).
//
//   - WritableSequenceFile (FF.7 + FF.7t): writable hdfs:sequencefile / hdfs:text
//     → admission ADMITS and the builder emits a WRITABLE export Job
//     (pxfwritable_export, NO LOG ERRORS).
//
//   - WritableDenied (WRej.* + FF.6b): writable hdfs:json, hdfs:orc, hive,
//     hive:text, hive:orc, hive:rc, HBase → admission DENIES with an error
//     containing "write-unsupported", and the builder refuses the writable DDL.
//
//   - CatalogHonest: iterate cases.Scenario97Cases() and resolve EVERY row
//     against the real built artifact (DDL/LOCATION/site-file/deny).
//
// ENV NOTE: parquet/avro/orc/sequencefile/rc samples are not always
// synthesizable locally, so HP.5/HP.6/HV.4/HB.1/FF.6a are [CONFIG-ONLY] (DDL/
// LOCATION/site assertions only). The live read/write landing is at e2e.
// ============================================================================

// Scenario97Suite exercises the Hadoop profile + write-capability contract.
type Scenario97Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario97(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario97Suite))
}

func (s *Scenario97Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario97HadoopServers returns the Hadoop PXF server set the Scenario 97
// sample CR ships: the combined hdfs-typed `hadoop-cluster` server (carrying
// fs.defaultFS + dfs.replication in Config, hive.metastore.uris in Hive,
// hbase.zookeeper.quorum in Hbase) PLUS the dedicated hive-warehouse (hive type)
// and hbase-store (hbase type) servers. The values mirror the sample CR.
func scenario97HadoopServers() []cbv1alpha1.PxfServerSpec {
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
		{
			Name: cases.Scenario97ServerHiveWarehouse,
			Type: "hive",
			Config: map[string]string{
				"fs.defaultFS":        cases.Scenario97FSDefaultFS,
				"hive.metastore.uris": cases.Scenario97HiveMetastore,
			},
		},
		{
			Name: cases.Scenario97ServerHBaseStore,
			Type: "hbase",
			Config: map[string]string{
				"fs.defaultFS":           cases.Scenario97FSDefaultFS,
				"hbase.zookeeper.quorum": cases.Scenario97HBaseZKQuorum,
			},
		},
	}
}

// scenario97Resource returns a deterministic external resource for a profile so
// the LOCATION the builder synthesizes is byte-stable. HDFS profiles read a
// path; Hive/HBase profiles read a metastore-/cluster-resolved table name.
func scenario97Resource(profile string) string {
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

// scenario97TargetTable returns a deterministic per-case target table.
func scenario97TargetTable(id string) string {
	return "public." + strings.ToLower(strings.ReplaceAll(id, ".", "_"))
}

// scenario97ReadJob builds a read/import PXF job for a profile on hadoop-cluster.
func scenario97ReadJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "job-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      server,
			Profile:     profile,
			Resource:    scenario97Resource(profile),
			TargetTable: scenario97TargetTable(id),
		},
	}
}

// scenario97WriteJob builds a writable/export PXF job (Mode=writable).
func scenario97WriteJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	job := scenario97ReadJob(id, profile, server)
	job.PxfJob.Mode = pxfpolicy.ModeWritable
	return job
}

// scenario97Cluster builds a running cluster carrying the full Hadoop server set
// plus the supplied jobs.
func scenario97Cluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: scenario97HadoopServers(),
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario97JobScript builds the load Job for a job and returns its script
// (container args[0]), failing the test if the Job/container is missing.
func (s *Scenario97Suite) scenario97JobScript(
	cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob,
) string {
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNilf(s.T(), out, "BuildDataLoadJob must produce a Job for %q", job.Name)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// ----------------------------------------------------------------------------
// ReadProfiles — HDFS/Hive/HBase reads on hadoop-cluster
// ----------------------------------------------------------------------------

// scenario97ReadProfiles are the HDFS/Hive/HBase READ profiles exercised on
// hadoop-cluster (HP.*, HV.*, HB.*).
var scenario97ReadProfiles = []struct{ id, profile string }{
	{"HP.1", "hdfs:text"},
	{"HP.2", "hdfs:parquet"},
	{"HP.3", "hdfs:avro"},
	{"HP.4", "hdfs:json"},
	{"HP.5", "hdfs:orc"},
	{"HP.6", "hdfs:sequencefile"},
	{"HV.1", "hive"},
	{"HV.2", "hive:text"},
	{"HV.3", "hive:orc"},
	{"HV.4", "hive:rc"},
	{"HB.1", "HBase"},
}

// TestFunctional_Scenario97_ReadProfiles builds a load Job for each HDFS/Hive/
// HBase READ profile on hadoop-cluster and asserts the generated READ DDL
// (pxfwritable_import) carries the byte-correct pxf:// LOCATION (PROFILE/SERVER).
// Covers HP.*/HV.*/HB.* at the builder layer (config-only; live landing at e2e).
func (s *Scenario97Suite) TestFunctional_Scenario97_ReadProfiles() {
	cluster := scenario97Cluster("s97-read")
	server := cases.Scenario97ServerHadoopCluster

	for _, tc := range scenario97ReadProfiles {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario97ReadJob(tc.id, tc.profile, server)
			script := s.scenario97JobScript(cluster, job)

			wantLoc := "pxf://" + scenario97Resource(tc.profile) +
				"?PROFILE=" + tc.profile + "&SERVER=" + server
			assert.Containsf(s.T(), script, wantLoc,
				"read DDL LOCATION must be byte-correct for %s", tc.profile)
			// Read path uses the import formatter and CREATE EXTERNAL TABLE
			// (never WRITABLE).
			assert.Contains(s.T(), script, "pxfwritable_import")
			assert.NotContains(s.T(), script, "pxfwritable_export")
			assert.NotContains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
			assert.Contains(s.T(), script, "CREATE EXTERNAL TABLE")
		})
	}
}

// ----------------------------------------------------------------------------
// SITE.* — rendered site-file assertions
// ----------------------------------------------------------------------------

// TestFunctional_Scenario97_SiteFiles asserts the servers ConfigMap renders the
// hadoop-cluster Hadoop site files with the expected keys/values (SITE.1-4):
// hive-site.xml carries hive.metastore.uris; hbase-site.xml carries
// hbase.zookeeper.quorum; core-site.xml carries fs.defaultFS; hdfs-site.xml is
// always emitted (valid <configuration> with dfs.replication=1).
func (s *Scenario97Suite) TestFunctional_Scenario97_SiteFiles() {
	cluster := scenario97Cluster("s97-sites")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	srv := cases.Scenario97ServerHadoopCluster

	// SITE.1 — hive-site.xml carries the metastore URI.
	hive := cm.Data[srv+"__hive-site.xml"]
	require.NotEmpty(s.T(), hive, "hadoop-cluster__hive-site.xml must be emitted")
	assert.Contains(s.T(), hive, "<name>hive.metastore.uris</name>")
	assert.Contains(s.T(), hive, cases.Scenario97HiveMetastore)

	// SITE.2 — hbase-site.xml carries the ZooKeeper quorum.
	hbase := cm.Data[srv+"__hbase-site.xml"]
	require.NotEmpty(s.T(), hbase, "hadoop-cluster__hbase-site.xml must be emitted")
	assert.Contains(s.T(), hbase, "<name>hbase.zookeeper.quorum</name>")
	assert.Contains(s.T(), hbase, cases.Scenario97HBaseZKQuorum)

	// SITE.3 — core-site.xml carries fs.defaultFS.
	core := cm.Data[srv+"__core-site.xml"]
	require.NotEmpty(s.T(), core, "hadoop-cluster__core-site.xml must be emitted")
	assert.Contains(s.T(), core, "<name>fs.defaultFS</name>")
	assert.Contains(s.T(), core, cases.Scenario97FSDefaultFS)

	// SITE.4 — hdfs-site.xml is ALWAYS emitted (valid configuration), with
	// dfs.replication=1.
	hdfs := cm.Data[srv+"__hdfs-site.xml"]
	require.NotEmpty(s.T(), hdfs, "hadoop-cluster__hdfs-site.xml must ALWAYS be emitted")
	assert.Contains(s.T(), hdfs, "<configuration>")
	assert.Contains(s.T(), hdfs, "<name>dfs.replication</name>")
}

// ----------------------------------------------------------------------------
// FF.7 — writable hdfs:sequencefile (+ companion hdfs:text) SUCCEEDS
// ----------------------------------------------------------------------------

// TestFunctional_Scenario97_WritableSequenceFile asserts FF.7 (hdfs:sequencefile)
// and FF.7t (hdfs:text) BOTH admit at the webhook AND produce a WRITABLE export
// Job whose script carries pxfwritable_export and NO LOG ERRORS.
func (s *Scenario97Suite) TestFunctional_Scenario97_WritableSequenceFile() {
	writable := []struct{ id, profile string }{
		{"FF.7", "hdfs:sequencefile"},
		{"FF.7t", "hdfs:text"},
	}
	for _, tc := range writable {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario97WriteJob(tc.id, tc.profile, cases.Scenario97ServerHadoopCluster)
			cluster := scenario97Cluster(
				"s97-"+strings.ToLower(strings.ReplaceAll(tc.id, ".", "-")), job)

			// Admission ADMITS the writable job.
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.NoErrorf(s.T(), err, "%s writable %s must be admitted", tc.id, tc.profile)

			// The operator builds the WRITABLE export Job.
			script := s.scenario97JobScript(cluster, job)
			assert.Contains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
			assert.Contains(s.T(), script, "pxfwritable_export")
			assert.NotContains(s.T(), script, "pxfwritable_import")
			// Writable tables take no reject limit → no LOG ERRORS.
			assert.NotContains(s.T(), script, "LOG ERRORS")
			assert.Contains(s.T(), script, "PROFILE="+tc.profile)
		})
	}
}

// ----------------------------------------------------------------------------
// WRej.* + FF.6b — writable DENY matrix
// ----------------------------------------------------------------------------

// TestFunctional_Scenario97_WritableDenied asserts the writable DENY matrix
// (WRej.1-7 + FF.6b): writable hdfs:json, hdfs:orc, hive, hive:text, hive:orc,
// hive:rc, HBase are DENIED at admission with an error containing
// "write-unsupported", AND the builder refuses to emit a writable DDL for the
// read-only profile (defense in depth).
func (s *Scenario97Suite) TestFunctional_Scenario97_WritableDenied() {
	denied := []struct{ id, profile string }{
		{"WRej.1", "hdfs:json"},
		{"WRej.2", "hdfs:orc"},
		{"WRej.3", "hive"},
		{"WRej.4", "hive:text"},
		{"WRej.5", "hive:orc"},
		{"WRej.6 (=FF.6b)", "hive:rc"},
		{"WRej.7", "HBase"},
	}
	for _, tc := range denied {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario97WriteJob(tc.id, tc.profile, cases.Scenario97ServerHadoopCluster)
			cluster := scenario97Cluster(
				"s97-"+strings.ToLower(strings.ReplaceAll(
					strings.ReplaceAll(tc.id, " ", ""), "(=ff.6b)", "")), job)

			// Admission DENIES with the write-unsupported message.
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "%s writable %s must be DENIED", tc.id, tc.profile)
			assert.Containsf(s.T(), err.Error(), "write-unsupported",
				"%s deny error must mention write-unsupported", tc.id)

			// Defense in depth: the builder returns a nil Job (it cannot emit a
			// writable DDL for a read-only profile).
			out := s.builder.BuildDataLoadJob(cluster, job)
			assert.Nilf(s.T(), out,
				"%s builder must refuse a writable DDL for read-only %s", tc.id, tc.profile)
		})
	}
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario97Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario97_CatalogHonest iterates cases.Scenario97Cases() and
// resolves EVERY row against the real built artifact: read rows assert the
// byte-correct LOCATION + import formatter; SITE.* rows assert the rendered site
// file key/value; admit-write rows assert the WRITABLE export DDL + admit;
// deny-write rows assert the validate-path deny. This keeps the catalog honest
// against the implementation.
func (s *Scenario97Suite) TestFunctional_Scenario97_CatalogHonest() {
	catalog := cases.Scenario97Cases()
	require.Len(s.T(), catalog, 26, "HP.1-6 + HV.1-4 + HB.1 + SITE.1-4 + FF.6a/6b + FF.7/7t + WRej.1-7")

	cluster := scenario97Cluster("s97-catalog")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Expected {
			case cases.Scenario97ExpectDenyWrite:
				// Webhook rows: the validate path must DENY with the message.
				job := scenario97WriteJob(tc.ID, tc.Profile, tc.Server)
				denyCluster := scenario97Cluster("s97-cat-"+
					strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job)
				_, err := s.validator.ValidateCreate(s.ctx, denyCluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "write-unsupported")
				// Defense in depth: builder refuses the writable DDL.
				assert.Nilf(s.T(), s.builder.BuildDataLoadJob(denyCluster, job),
					"%s builder must refuse writable DDL", tc.ID)

			case cases.Scenario97ExpectAdmitWrite:
				// Writable success rows: WRITABLE export DDL + admit.
				job := scenario97WriteJob(tc.ID, tc.Profile, tc.Server)
				admitCluster := scenario97Cluster("s97-cat-"+
					strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job)
				_, err := s.validator.ValidateCreate(s.ctx, admitCluster)
				require.NoErrorf(s.T(), err, "%s must be admitted", tc.ID)
				script := s.scenario97JobScript(admitCluster, job)
				assert.Contains(s.T(), script, "pxfwritable_export")
				assert.Contains(s.T(), script, "PROFILE="+tc.Profile)
				assert.NotContains(s.T(), script, "LOG ERRORS")

			case cases.Scenario97ExpectRenderOK:
				// SITE.* rows: the rendered site file must carry the documented
				// key/value.
				require.NotEmptyf(s.T(), tc.SiteFile, "%s must name a SiteFile", tc.ID)
				site := cm.Data[tc.Server+"__"+tc.SiteFile]
				require.NotEmptyf(s.T(), site, "%s site file %s must be emitted", tc.ID, tc.SiteFile)
				assert.Containsf(s.T(), site, tc.SiteContains,
					"%s site file %s must contain %q", tc.ID, tc.SiteFile, tc.SiteContains)

			case cases.Scenario97ExpectAdmitRead:
				// Read rows (HP.*/HV.*/HB.*/FF.6a): byte-correct LOCATION + import.
				job := scenario97ReadJob(tc.ID, tc.Profile, tc.Server)
				script := s.scenario97JobScript(cluster, job)
				wantLoc := "pxf://" + scenario97Resource(tc.Profile) +
					"?PROFILE=" + tc.Profile + "&SERVER=" + tc.Server
				assert.Containsf(s.T(), script, wantLoc,
					"%s LOCATION must be byte-correct", tc.ID)
				assert.Contains(s.T(), script, "pxfwritable_import")
				if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
					s.T().Logf("scenario97 %s: [CONFIG-ONLY] — DDL/LOCATION assertions only", tc.ID)
				}

			default:
				s.T().Fatalf("%s: unknown Expected token %q", tc.ID, tc.Expected)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

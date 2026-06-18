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
// Scenario 96: Object Store Profiles & Format Write-Capability (functional)
// ============================================================================
//
// This suite drives the BUILDER (DDL + servers ConfigMap) and the WEBHOOK
// validate path over the FULL object-store profile/server set, asserting the
// shipped production contract WITHOUT a live cluster:
//
//   - T4.1 ReadProfilesAcrossServers: build a DataLoading spec with all 5
//     object-store profiles (text/parquet/avro/json/orc) × 2 MinIO-backed
//     servers (s3-datalake, minio-warehouse). For each readable job the operator
//     builds a load Job whose script carries the pxfwritable_import READ DDL with
//     the byte-correct pxf:// LOCATION (PROFILE/SERVER). The servers ConfigMap
//     carries one <server>__s3-site.xml per object-store server (path-style for
//     minio-warehouse). Covers OS.* (config-only DDL/site assertions here; the
//     live row landing is at e2e) + the MinIO CFG-style path-style site file.
//
//   - T4.2 WritableMatrix: FF.1-FF.3 (s3:text/parquet/avro, Mode=writable) →
//     the operator builds a WRITABLE export Job whose script carries
//     pxfwritable_export and NO LOG ERRORS, AND admission ADMITS. FF.4/FF.5
//     (s3:json/s3:orc, Mode=writable) → admission DENIES with an error
//     containing "write-unsupported"/"writable" (the established scenario89/90
//     validate-direct pattern), AND the builder refuses to emit a writable DDL
//     for the read-only format (defense in depth).
//
//   - CFG.* gs/abfss/wasbs/Dell-ECS: build the servers → assert a valid
//     <server>__s3-site.xml is rendered and the pxf:// LOCATION per profile is
//     byte-correct. These are EXPLICITLY [CONFIG-ONLY] (no local backing store)
//     and are named/commented as such.
//
//   - T4.3 CatalogHonest: iterate cases.Scenario96Cases() and resolve EVERY row
//     against the real built artifact (DDL/LOCATION/site-file for builder/server
//     rows; the validate-path deny for FF.4/FF.5). The catalog must match code.
//
// ENV NOTE: gs/abfss/wasbs/Dell-ECS have no local backing store, so all CFG.*
// rows and the ORC OS rows (OS.5/OS.10) are config-only (DDL/site assertions
// only). The live read/write landing is exercised at e2e (KUBECONFIG-gated).
// ============================================================================

// Scenario96Suite exercises the object-store profile + write-capability contract.
type Scenario96Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario96(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario96Suite))
}

func (s *Scenario96Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario96ObjectStoreServers returns the full object-store PXF server set the
// Scenario 96 sample CR ships: the two MinIO-backed s3 servers (s3-datalake
// AWS-style, minio-warehouse path-style) PLUS the four config-only object-store
// servers (gcs-datalake gs, adls-gen2 abfss, azure-blob wasbs, dell-ecs s3 with
// a custom fs.s3a.endpoint). The values mirror the sample CR.
func scenario96ObjectStoreServers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
		{
			Name: cases.Scenario96ServerS3Datalake,
			Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint": "http://minio:9000",
			},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
			},
		},
		{
			Name: cases.Scenario96ServerMinioWarehouse,
			Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint":          "http://minio:9000",
				"fs.s3a.path.style.access": "true",
			},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
			},
		},
		{
			Name: cases.Scenario96ServerGCSDatalake,
			Type: "gs",
			Config: map[string]string{
				"fs.s3a.endpoint":  "storage.googleapis.com",
				"fs.gs.project.id": "cloudberry-demo",
			},
		},
		{
			Name: cases.Scenario96ServerADLSGen2,
			Type: "abfss",
			Config: map[string]string{
				"fs.s3a.endpoint": "acct.dfs.core.windows.net",
			},
		},
		{
			Name: cases.Scenario96ServerAzureBlob,
			Type: "wasbs",
			Config: map[string]string{
				"fs.s3a.endpoint": "acct.blob.core.windows.net",
			},
		},
		{
			Name: cases.Scenario96ServerDellECS,
			Type: "s3",
			Config: map[string]string{
				// Dell ECS = an s3 server with a CUSTOM endpoint override.
				"fs.s3a.endpoint": "https://ecs.dell.example.com:9021",
			},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "dell-ecs-credentials", Key: "access_key"},
			},
		},
	}
}

// scenario96Resource returns a deterministic object-store resource URI for a
// profile×server pair so the LOCATION the builder synthesizes is byte-stable.
func scenario96Resource(profile, server string) string {
	scheme, _, _ := strings.Cut(profile, ":")
	switch scheme {
	case "gs":
		return "gs://cloudberry-demo/data/"
	case "abfss":
		return "abfss://container@acct.dfs.core.windows.net/data/"
	case "wasbs":
		return "wasbs://container@acct.blob.core.windows.net/data/"
	default: // s3 (incl. minio-warehouse / dell-ecs)
		if server == cases.Scenario96ServerMinioWarehouse {
			return "cloudberry-warehouse/data/"
		}
		return "cloudberry-data/data/"
	}
}

// scenario96TargetTable returns a deterministic per-case target table.
func scenario96TargetTable(id string) string {
	return "public." + strings.ToLower(strings.ReplaceAll(id, ".", "_"))
}

// scenario96ReadJob builds a read/import PXF job for a profile×server pair.
func scenario96ReadJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "job-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      server,
			Profile:     profile,
			Resource:    scenario96Resource(profile, server),
			TargetTable: scenario96TargetTable(id),
		},
	}
}

// scenario96WriteJob builds a writable/export PXF job (Mode=writable).
func scenario96WriteJob(id, profile, server string) cbv1alpha1.DataLoadingJob {
	job := scenario96ReadJob(id, profile, server)
	job.PxfJob.Mode = pxfpolicy.ModeWritable
	return job
}

// scenario96AdmissionServers returns ONLY the admission-valid (s3-typed)
// object-store servers. The webhook server-type allowlist is
// {s3,hdfs,jdbc,hbase,hive}, so gs/abfss/wasbs are NOT valid admission server
// types (they are object-store SCHEMES the BUILDER routes to s3-site.xml, not
// admission server types). Webhook tests therefore use this s3-only set; the
// builder/site-file tests use the full set via scenario96ObjectStoreServers.
func scenario96AdmissionServers() []cbv1alpha1.PxfServerSpec {
	var out []cbv1alpha1.PxfServerSpec
	for _, srv := range scenario96ObjectStoreServers() {
		if srv.Type == "s3" {
			out = append(out, srv)
		}
	}
	return out
}

// scenario96Cluster builds a running cluster carrying the FULL object-store
// server set (incl. gs/abfss/wasbs) plus the supplied jobs. It is used for the
// BUILDER (DDL + site files) layer, which accepts every object-store scheme.
func scenario96Cluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	return scenario96ClusterWith(name, scenario96ObjectStoreServers(), jobs...)
}

// scenario96AdmissionCluster builds a running cluster carrying ONLY the
// admission-valid (s3-typed) servers plus the supplied jobs. It is used for the
// WEBHOOK validate layer (FF.* admit/deny).
func scenario96AdmissionCluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	return scenario96ClusterWith(name, scenario96AdmissionServers(), jobs...)
}

// scenario96ClusterWith builds a running cluster with the given servers + jobs.
func scenario96ClusterWith(
	name string, servers []cbv1alpha1.PxfServerSpec, jobs ...cbv1alpha1.DataLoadingJob,
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
			Servers: servers,
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario96JobScript builds the load Job for a job and returns its script
// (container args[0]), failing the test if the Job/container is missing.
func (s *Scenario96Suite) scenario96JobScript(
	cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob,
) string {
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNilf(s.T(), out, "BuildDataLoadJob must produce a Job for %q", job.Name)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// scenario96Profiles are the five object-store formats exercised per server.
var scenario96Profiles = []string{"s3:text", "s3:parquet", "s3:avro", "s3:json", "s3:orc"}

// ----------------------------------------------------------------------------
// T4.1 — read profiles across the two MinIO-backed servers + site files
// ----------------------------------------------------------------------------

// TestFunctional_Scenario96_ReadProfilesAcrossServers builds a load Job for each
// of the 5 object-store profiles × 2 MinIO-backed servers and asserts the
// generated READ DDL (pxfwritable_import) carries the byte-correct pxf://
// LOCATION (PROFILE/SERVER). Covers OS.1-OS.10 at the builder layer (config-only;
// the live row landing is at e2e).
func (s *Scenario96Suite) TestFunctional_Scenario96_ReadProfilesAcrossServers() {
	servers := []string{cases.Scenario96ServerS3Datalake, cases.Scenario96ServerMinioWarehouse}
	cluster := scenario96Cluster("s96-read")

	for _, server := range servers {
		for i, profile := range scenario96Profiles {
			profile, server := profile, server
			id := "OS-" + server + "-" + profile
			_ = i
			s.Run(id, func() {
				job := scenario96ReadJob(id, profile, server)
				script := s.scenario96JobScript(cluster, job)

				wantLoc := "pxf://" + scenario96Resource(profile, server) +
					"?PROFILE=" + profile + "&SERVER=" + server
				assert.Containsf(s.T(), script, wantLoc,
					"read DDL LOCATION must be byte-correct for %s on %s", profile, server)
				// Read path uses the import formatter and CREATE EXTERNAL TABLE
				// (never WRITABLE).
				assert.Contains(s.T(), script, "pxfwritable_import")
				assert.NotContains(s.T(), script, "pxfwritable_export")
				assert.NotContains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
				assert.Contains(s.T(), script, "CREATE EXTERNAL TABLE")
			})
		}
	}
}

// TestFunctional_Scenario96_ObjectStoreSiteFiles asserts the servers ConfigMap
// renders exactly one <server>__s3-site.xml per object-store server, with the
// expected fs.* keys: s3-datalake (endpoint), minio-warehouse (path-style),
// gcs-datalake/adls-gen2/azure-blob (gs/abfss/wasbs → object-store site file),
// dell-ecs (custom endpoint). Covers OS.6/OS.10 (path-style) + CFG.* site files.
func (s *Scenario96Suite) TestFunctional_Scenario96_ObjectStoreSiteFiles() {
	cluster := scenario96Cluster("s96-sites")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	for _, server := range []string{
		cases.Scenario96ServerS3Datalake,
		cases.Scenario96ServerMinioWarehouse,
		cases.Scenario96ServerGCSDatalake,
		cases.Scenario96ServerADLSGen2,
		cases.Scenario96ServerAzureBlob,
		cases.Scenario96ServerDellECS,
	} {
		key := server + "__s3-site.xml"
		assert.Containsf(s.T(), cm.Data, key,
			"every object-store server must emit %s", key)
	}

	// s3-datalake carries its endpoint.
	assert.Contains(s.T(), cm.Data[cases.Scenario96ServerS3Datalake+"__s3-site.xml"],
		"<name>fs.s3a.endpoint</name>")

	// minio-warehouse renders path-style (the live read backbone).
	minio := cm.Data[cases.Scenario96ServerMinioWarehouse+"__s3-site.xml"]
	assert.Contains(s.T(), minio, "<name>fs.s3a.path.style.access</name>")
	assert.Contains(s.T(), minio, "<value>true</value>")

	// CONFIG-ONLY: dell-ecs carries its CUSTOM endpoint verbatim (no backing store).
	dell := cm.Data[cases.Scenario96ServerDellECS+"__s3-site.xml"]
	assert.Contains(s.T(), dell, "https://ecs.dell.example.com:9021",
		"[CONFIG-ONLY] dell-ecs s3-site.xml must carry the custom endpoint")
}

// ----------------------------------------------------------------------------
// T4.2 — writable matrix FF.1-FF.5
// ----------------------------------------------------------------------------

// TestFunctional_Scenario96_WritableAdmitAndExportDDL asserts FF.1-FF.3
// (s3:text/parquet/avro, Mode=writable) BOTH admit at the webhook AND produce a
// WRITABLE export Job whose script carries pxfwritable_export and NO LOG ERRORS.
func (s *Scenario96Suite) TestFunctional_Scenario96_WritableAdmitAndExportDDL() {
	writable := []struct{ id, profile string }{
		{"FF.1", "s3:text"},
		{"FF.2", "s3:parquet"},
		{"FF.3", "s3:avro"},
	}
	for _, tc := range writable {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario96WriteJob(tc.id, tc.profile, cases.Scenario96ServerS3Datalake)
			cluster := scenario96AdmissionCluster("s96-"+strings.ToLower(strings.ReplaceAll(tc.id, ".", "-")), job)

			// Admission ADMITS the writable job.
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.NoErrorf(s.T(), err, "%s writable %s must be admitted", tc.id, tc.profile)

			// The operator builds the WRITABLE export Job.
			script := s.scenario96JobScript(cluster, job)
			assert.Contains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
			assert.Contains(s.T(), script, "pxfwritable_export")
			assert.NotContains(s.T(), script, "pxfwritable_import")
			// Writable tables take no reject limit → no LOG ERRORS.
			assert.NotContains(s.T(), script, "LOG ERRORS")
			assert.Contains(s.T(), script, "PROFILE="+tc.profile)
		})
	}
}

// TestFunctional_Scenario96_WritableDenied asserts FF.4/FF.5 (s3:json/s3:orc,
// Mode=writable) are DENIED at admission with an error containing
// "write-unsupported"/"writable" (the scenario89-style validate-direct pattern),
// AND that the builder refuses to emit a writable DDL for the read-only format
// (defense in depth).
func (s *Scenario96Suite) TestFunctional_Scenario96_WritableDenied() {
	denied := []struct{ id, profile string }{
		{"FF.4", "s3:json"},
		{"FF.5", "s3:orc"},
	}
	for _, tc := range denied {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario96WriteJob(tc.id, tc.profile, cases.Scenario96ServerS3Datalake)
			cluster := scenario96AdmissionCluster("s96-"+strings.ToLower(strings.ReplaceAll(tc.id, ".", "-")), job)

			// Admission DENIES with the write-unsupported message.
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "%s writable %s must be DENIED", tc.id, tc.profile)
			assert.Containsf(s.T(), err.Error(), "write-unsupported",
				"%s deny error must mention write-unsupported", tc.id)
			assert.Containsf(s.T(), err.Error(), "writable",
				"%s deny error must mention writable", tc.id)

			// Defense in depth: the builder returns a nil Job (it cannot emit a
			// writable DDL for a read-only format).
			out := s.builder.BuildDataLoadJob(cluster, job)
			assert.Nilf(s.T(), out,
				"%s builder must refuse a writable DDL for read-only %s", tc.id, tc.profile)
		})
	}
}

// ----------------------------------------------------------------------------
// CFG.* — gs/abfss/wasbs/Dell-ECS config-only LOCATION + site file
// ----------------------------------------------------------------------------

// TestFunctional_Scenario96_ConfigOnlyServersLocationAndSite builds the
// gs/abfss/wasbs/Dell-ECS read jobs and asserts the byte-correct pxf:// LOCATION
// per profile AND a valid <server>__s3-site.xml. These are EXPLICITLY
// [CONFIG-ONLY] — there is NO local backing store, so only DDL/LOCATION and the
// rendered site file are asserted (no live rows).
func (s *Scenario96Suite) TestFunctional_Scenario96_ConfigOnlyServersLocationAndSite() {
	cluster := scenario96Cluster("s96-cfg")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	cfg := []struct{ id, profile, server string }{
		{"CFG.1", "gs:text", cases.Scenario96ServerGCSDatalake},
		{"CFG.2", "gs:parquet", cases.Scenario96ServerGCSDatalake},
		{"CFG.3", "abfss:text", cases.Scenario96ServerADLSGen2},
		{"CFG.4", "abfss:parquet", cases.Scenario96ServerADLSGen2},
		{"CFG.5", "wasbs:text", cases.Scenario96ServerAzureBlob},
		{"CFG.6", "wasbs:json", cases.Scenario96ServerAzureBlob},
		{"CFG.7", "s3:text", cases.Scenario96ServerDellECS},
		{"CFG.8", "s3:parquet", cases.Scenario96ServerDellECS},
	}
	for _, tc := range cfg {
		tc := tc
		s.Run(tc.id+"_CONFIG_ONLY", func() {
			job := scenario96ReadJob(tc.id, tc.profile, tc.server)
			script := s.scenario96JobScript(cluster, job)

			wantLoc := "pxf://" + scenario96Resource(tc.profile, tc.server) +
				"?PROFILE=" + tc.profile + "&SERVER=" + tc.server
			assert.Containsf(s.T(), script, wantLoc,
				"[CONFIG-ONLY] %s LOCATION must be byte-correct", tc.id)

			// A valid (non-empty, well-formed) site file is rendered.
			site := cm.Data[tc.server+"__s3-site.xml"]
			require.NotEmptyf(s.T(), site, "[CONFIG-ONLY] %s site file must exist", tc.id)
			assert.Contains(s.T(), site, "<configuration>")
		})
	}
}

// ----------------------------------------------------------------------------
// T4.3 — catalog honesty
// ----------------------------------------------------------------------------

// TestFunctional_Scenario96_CatalogHonest iterates cases.Scenario96Cases() and
// resolves EVERY row against the real built artifact: for builder/DDL &
// server-config rows it builds the DDL/ConfigMap and asserts the documented
// LOCATION/format/site-file; for webhook rows (FF.4/FF.5) it asserts the validate
// path denies. This keeps the catalog honest against the implementation.
func (s *Scenario96Suite) TestFunctional_Scenario96_CatalogHonest() {
	catalog := cases.Scenario96Cases()
	require.Len(s.T(), catalog, 23, "OS.1-10 + CFG.1-8 + FF.1-5")

	cluster := scenario96Cluster("s96-catalog")
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Expected {
			case cases.Scenario96ExpectDenyWrite:
				// Webhook rows: the validate path must DENY with the message.
				job := scenario96WriteJob(tc.ID, tc.Profile, tc.Server)
				denyCluster := scenario96AdmissionCluster("s96-cat-"+
					strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job)
				_, err := s.validator.ValidateCreate(s.ctx, denyCluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "write-unsupported")

			case cases.Scenario96ExpectAdmitWrite:
				// Writable success rows: WRITABLE export DDL + admit.
				job := scenario96WriteJob(tc.ID, tc.Profile, tc.Server)
				admitCluster := scenario96AdmissionCluster("s96-cat-"+
					strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job)
				_, err := s.validator.ValidateCreate(s.ctx, admitCluster)
				require.NoErrorf(s.T(), err, "%s must be admitted", tc.ID)
				script := s.scenario96JobScript(admitCluster, job)
				assert.Contains(s.T(), script, "pxfwritable_export")
				assert.Contains(s.T(), script, "PROFILE="+tc.Profile)

			case cases.Scenario96ExpectAdmitRead:
				// Read rows (OS.*/CFG.*): byte-correct LOCATION + site file when
				// the layer covers server-config.
				job := scenario96ReadJob(tc.ID, tc.Profile, tc.Server)
				script := s.scenario96JobScript(cluster, job)
				wantLoc := "pxf://" + scenario96Resource(tc.Profile, tc.Server) +
					"?PROFILE=" + tc.Profile + "&SERVER=" + tc.Server
				assert.Containsf(s.T(), script, wantLoc,
					"%s LOCATION must be byte-correct", tc.ID)
				assert.Contains(s.T(), script, "pxfwritable_import")
				if strings.Contains(tc.Layer, "server-config") {
					site := cm.Data[tc.Server+"__s3-site.xml"]
					assert.NotEmptyf(s.T(), site, "%s site file must exist", tc.ID)
				}

			default:
				s.T().Fatalf("%s: unknown Expected token %q", tc.ID, tc.Expected)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

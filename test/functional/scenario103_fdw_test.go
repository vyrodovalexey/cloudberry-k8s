//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 103: FDW-Based Loading Path (EX.5-EX.8) — functional
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario103Cases — this is Scenario 103, the FDW
// loading-path verification scenario. It mirrors the Scenario 101/102 functional
// SHAPE, driving the BUILDER (the persistent FDW DDL chain + the FDW load script,
// delivered as the dataload Job Args[0] of a loadMethod:fdw job) and the
// VALIDATOR (webhook W.25 / W.17 tweak) WITHOUT a live cluster, asserting the
// shipped production contract:
//
//   - EX.5-EX.7 (FDW DDL): a loadMethod:fdw s3:parquet job → Args[0] carries the
//     CREATE SERVER IF NOT EXISTS "foreign_<server>" FOREIGN DATA WRAPPER
//     s3_pxf_fdw + OPTIONS resource/format, CREATE USER MAPPING ... FOR "gpadmin",
//     CREATE FOREIGN TABLE IF NOT EXISTS "foreign_<job>" (LIKE <target>) chain;
//     a jdbc loadMethod:fdw job → jdbc_pxf_fdw + no format. NO DROP (persistent).
//
//   - EX.8 (FDW load): the script queries the foreign table directly
//     (SELECT count(*)) + INSERTs INTO <target> SELECT * FROM the foreign table
//     (the EQUIVALENT shape) + ANALYZE; with sourceFilter → a WHERE predicate.
//
//   - W.25 / W.17: loadMethod bogus → DENY; fdw+writable → DENY; fdw+continuous →
//     DENY; fdw read → ADMIT; sourceFilter+fdw-read → ADMIT; sourceFilter+plain-
//     ext-read → DENY.
//
//   - EQUIVALENCE (builder-level): the SAME job built once with loadMethod unset
//     (external-table) and once with loadMethod:fdw both produce a valid load Job
//     with the SAME INSERT INTO <target> SELECT * FROM <source> shape (the only
//     diff is the ext-table vs FDW source object).
//
//   - CatalogHonest: resolve each cases.Scenario103Cases() builder/webhook row
//     against the REAL built artifact (live rows are logged + skipped here).
//
// METRIC HONESTY: NO new metric. The FDW load reuses cloudberry_data_loading_*;
// cloudberry_pxf_* / cloudberry_gpfdist_* stay PLANNED and are NEVER asserted.
// The EX.8 EQUIVALENCE is proven by row counts at e2e Part B, not a bytes metric.
// ============================================================================

// Scenario103Suite exercises the FDW loading-path builder + validator contract
// at the builder + webhook layer.
type Scenario103Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario103(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario103Suite))
}

func (s *Scenario103Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario103Cluster builds a running cluster carrying the PXF sidecar + the
// s3-datalake server + the supplied data-loading jobs. The per-case fn mutates
// the spec for the negative webhook variants.
func scenario103Cluster(
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
			Servers: []cbv1alpha1.PxfServerSpec{scenario103S3Server()},
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario103S3Server returns a fully-valid s3 PXF server (endpoint + credential
// secrets) so the W.4 s3-server validation passes and only the W.25 / W.17 rules
// are exercised.
func scenario103S3Server() cbv1alpha1.PxfServerSpec {
	return cbv1alpha1.PxfServerSpec{
		Name: cases.Scenario103Server,
		Type: "s3",
		Config: map[string]string{
			"fs.s3a.endpoint":          "http://minio:9000",
			"fs.s3a.path.style.access": "true",
		},
		CredentialSecrets: []cbv1alpha1.SecretReference{
			{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
			{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
		},
	}
}

// scenario103FDWReadJob returns the canonical Scenario 103 FDW read/import job:
// an s3:parquet PXF job with loadMethod=fdw, server s3-datalake, target
// public.events and the events prefix as the resource. It is the byte-golden
// fixture for the EX.5-EX.8 FDW DDL chain + load script (it mirrors the builder
// package's fdwReadJob so the asserted substrings line up).
func scenario103FDWReadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "fdw-ingest",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario103Server,
			Profile:     "s3:parquet",
			Resource:    "s3a://cloudberry-data/events/",
			TargetTable: "public.events",
			LoadMethod:  "fdw",
		},
	}
}

// scenario103ExtReadJob returns the SAME job as scenario103FDWReadJob but with
// loadMethod unset (the external-table read path), for the builder-level
// EQUIVALENCE proof.
func scenario103ExtReadJob() cbv1alpha1.DataLoadingJob {
	job := scenario103FDWReadJob()
	job.Name = "ext-ingest"
	job.PxfJob.LoadMethod = ""
	return job
}

// scenario103WebhookJob returns a minimal valid s3 PXF read job (the W.25 / W.17
// baseline); callers mutate the PxfJob field to produce a negative case.
func scenario103WebhookJob(name string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    name,
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario103Server,
			Profile:     "s3:parquet",
			Resource:    "s3a://cloudberry-data/events/",
			TargetTable: "public.events",
		},
	}
}

// scenario103Script builds the dataload Job for a job against a Scenario 103
// cluster and returns its single container's Args[0] (the load script). The FDW
// DDL chain (for a loadMethod:fdw job) is delivered IN this script.
func (s *Scenario103Suite) scenario103Script(job cbv1alpha1.DataLoadingJob) string {
	cluster := scenario103Cluster("s103-script", job)
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	require.Len(s.T(), out.Spec.Template.Spec.Containers[0].Args, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// ----------------------------------------------------------------------------
// EX.5-EX.7 — the persistent FDW DDL chain
// ----------------------------------------------------------------------------

// TestFunctional_Scenario103_FDWDDLChain asserts the EX.5-EX.7 FDW DDL chain in
// the loadMethod:fdw job's load script (Args[0]): CREATE SERVER + wrapper +
// OPTIONS, CREATE USER MAPPING FOR "gpadmin", CREATE FOREIGN TABLE (LIKE target),
// all IF NOT EXISTS, and NO DROP (persistent). The pxf_fdw extension install is
// also present.
func (s *Scenario103Suite) TestFunctional_Scenario103_FDWDDLChain() {
	script := s.scenario103Script(scenario103FDWReadJob())

	// EX.5: CREATE SERVER + wrapper + OPTIONS (config '<pxf-server>'). The SERVER
	// carries config (the named PXF server config) — NOT resource/format (the
	// pxf_fdw validator rejects resource at the pg_foreign_server level).
	assert.Contains(s.T(), script, "CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"")
	assert.Contains(s.T(), script, "FOREIGN DATA WRAPPER s3_pxf_fdw")
	assert.Contains(s.T(), script, "OPTIONS (config 's3-datalake')")
	// The CREATE SERVER line must NOT carry a resource OPTION.
	assert.NotContains(s.T(), script,
		"FOREIGN DATA WRAPPER s3_pxf_fdw\n  OPTIONS (resource")
	// EX.6: USER MAPPING for the gpadmin data-loader role.
	assert.Contains(s.T(), script, "CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"")
	assert.Contains(s.T(), script, "SERVER \"foreign_s3_datalake\"")
	// EX.7: FOREIGN TABLE (LIKE target) on the server; resource/format OPTIONS
	// live HERE (the pg_foreign_table level).
	assert.Contains(s.T(), script,
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")")
	assert.Contains(s.T(), script,
		"OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet')")
	// Best-effort pxf_fdw extension install.
	assert.Contains(s.T(), script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw")

	// Persistence: the FDW objects are NEVER dropped.
	assert.NotContains(s.T(), script, "DROP FOREIGN TABLE")
	assert.NotContains(s.T(), script, "DROP SERVER")
	assert.NotContains(s.T(), script, "DROP EXTERNAL TABLE")
	// No external-table DDL leaks onto the fdw path.
	assert.NotContains(s.T(), script, "CREATE EXTERNAL TABLE")
}

// TestFunctional_Scenario103_FDWDDLJDBCNoFormat asserts a BARE jdbc loadMethod:fdw
// profile resolves the jdbc_pxf_fdw wrapper AND omits the `format` OPTION (a JDBC
// FDW takes a resource=table, no format suffix).
func (s *Scenario103Suite) TestFunctional_Scenario103_FDWDDLJDBCNoFormat() {
	job := cbv1alpha1.DataLoadingJob{
		Name:    "jdbc-fdw",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "mysql-oltp",
			Profile:     "jdbc",
			Resource:    "sales.orders",
			TargetTable: "public.orders",
			LoadMethod:  "fdw",
		},
	}
	// Use a jdbc server so the server reference resolves; build the Job directly.
	cluster := scenario103Cluster("s103-jdbc", job)
	cluster.Spec.DataLoading.Pxf.Servers = append(cluster.Spec.DataLoading.Pxf.Servers,
		cbv1alpha1.PxfServerSpec{
			Name: "mysql-oltp",
			Type: "jdbc",
			Config: map[string]string{
				"jdbc.driver": "com.mysql.cj.jdbc.Driver",
				"jdbc.url":    "jdbc:mysql://mysql:3306/oltp",
			},
		})
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(s.T(), script, "FOREIGN DATA WRAPPER jdbc_pxf_fdw")
	assert.Contains(s.T(), script,
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_jdbc_fdw\" (LIKE \"public\".\"orders\")")
	assert.NotContains(s.T(), script, "format ",
		"a bare jdbc FDW must omit the format OPTION")
}

// ----------------------------------------------------------------------------
// EX.8 — direct query + INSERT...SELECT...WHERE + ANALYZE
// ----------------------------------------------------------------------------

// TestFunctional_Scenario103_FDWLoadScript asserts the EX.8 FDW load shape: the
// direct foreign-table count query, the INSERT INTO <target> SELECT * FROM the
// foreign table (the EQUIVALENT shape), the DATALOAD_ROWS marker, and ANALYZE.
func (s *Scenario103Suite) TestFunctional_Scenario103_FDWLoadScript() {
	script := s.scenario103Script(scenario103FDWReadJob())

	// EX.8 direct query.
	assert.Contains(s.T(), script, "SELECT count(*) FROM \"foreign_fdw_ingest\"")
	// EX.8 INSERT (equivalent shape).
	assert.Contains(s.T(), script,
		"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\"")
	// Rowcount marker harvested into /dev/termination-log (reuses the data-loading
	// marker; NO new metric).
	assert.Contains(s.T(), script, "DATALOAD_ROWS=")
	assert.Contains(s.T(), script, "/dev/termination-log")
	// Read path ANALYZE.
	assert.Contains(s.T(), script, "ANALYZE \"public\".\"events\"")
}

// TestFunctional_Scenario103_FDWLoadScriptWhere asserts a fdw read job carrying a
// sourceFilter emits the INSERT with the WHERE predicate via the shared
// single-quote-safe quoted-heredoc path, and still has no DROP.
func (s *Scenario103Suite) TestFunctional_Scenario103_FDWLoadScriptWhere() {
	job := scenario103FDWReadJob()
	job.PxfJob.SourceFilter = "region='us-east'"
	script := s.scenario103Script(job)

	assert.Contains(s.T(), script,
		"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\" "+
			"WHERE region='us-east'")
	// The single-quote-safe quoted heredoc is used for the filtered INSERT.
	assert.Contains(s.T(), script, "<<'_CBK_INSERT_EOF_'")
	assert.NotContains(s.T(), script, "DROP")
}

// ----------------------------------------------------------------------------
// W.25 / W.17 — webhook admission
// ----------------------------------------------------------------------------

// TestFunctional_Scenario103_WebhookAdmission drives the validate path for the
// Scenario 103 webhook rules: loadMethod bogus → DENY; fdw+writable → DENY;
// fdw+continuous → DENY; fdw read → ADMIT; sourceFilter+fdw-read → ADMIT;
// sourceFilter+plain-ext-read → DENY.
func (s *Scenario103Suite) TestFunctional_Scenario103_WebhookAdmission() {
	s.Run("baseline s3 read job admitted", func() {
		cluster := scenario103Cluster("s103-ok", scenario103WebhookJob("ok"))
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.25 loadMethod bogus -> DENY (enum)", func() {
		job := scenario103WebhookJob("w25-enum")
		job.PxfJob.LoadMethod = "bogus"
		cluster := scenario103Cluster("s103-w25-enum", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "must be external-table or fdw")
	})

	s.Run("W.25 loadMethod fdw read -> ADMIT", func() {
		job := scenario103WebhookJob("w25-ok")
		job.PxfJob.LoadMethod = "fdw"
		cluster := scenario103Cluster("s103-w25-ok", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.25 loadMethod fdw + mode writable -> DENY", func() {
		job := scenario103WebhookJob("w25-write")
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.Mode = "writable"
		job.PxfJob.Profile = "s3:text" // writable format so W.10b passes; W.25 rejects
		cluster := scenario103Cluster("s103-w25-write", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "not valid with mode=writable")
	})

	s.Run("W.25 loadMethod fdw + continuous -> DENY", func() {
		job := scenario103WebhookJob("w25-cont")
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.Continuous = util.Ptr(true)
		cluster := scenario103Cluster("s103-w25-cont", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "continuous")
	})

	s.Run("W.17 sourceFilter on fdw read -> ADMIT", func() {
		job := scenario103WebhookJob("w17-fdw")
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.SourceFilter = "region='us-east'"
		cluster := scenario103Cluster("s103-w17-fdw", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})

	s.Run("W.17 sourceFilter on plain ext-table read -> DENY", func() {
		job := scenario103WebhookJob("w17-ext")
		job.PxfJob.SourceFilter = "region='us-east'"
		cluster := scenario103Cluster("s103-w17-ext", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err)
		assert.Contains(s.T(), err.Error(), "fdw read job")
	})

	s.Run("W.17 sourceFilter on writable export still admitted", func() {
		job := scenario103WebhookJob("w17-write")
		job.PxfJob.Mode = "writable"
		job.PxfJob.Profile = "s3:text"
		job.PxfJob.SourceFilter = "region='us-east'"
		cluster := scenario103Cluster("s103-w17-write", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err)
	})
}

// ----------------------------------------------------------------------------
// EQUIVALENCE — same INSERT shape for ext-table vs FDW (builder-level)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario103_Equivalence builds the SAME job once with loadMethod
// unset (external-table) and once with loadMethod:fdw and asserts both produce a
// valid load Job carrying the SAME INSERT INTO <target> SELECT * FROM <source>
// shape — the only difference is the source object (external table vs foreign
// table). This is the builder-level EQUIVALENCE proof (the live count(ext)==
// count(fdw) headline is at e2e Part B).
func (s *Scenario103Suite) TestFunctional_Scenario103_Equivalence() {
	extScript := s.scenario103Script(scenario103ExtReadJob())
	fdwScript := s.scenario103Script(scenario103FDWReadJob())

	// Both produce a valid load Job that INSERTs INTO the SAME target via
	// SELECT * FROM <source>.
	assert.Contains(s.T(), extScript, "INSERT INTO \"public\".\"events\" SELECT * FROM ",
		"the external-table leg INSERTs INTO <target> SELECT * FROM <ext table>")
	assert.Contains(s.T(), fdwScript,
		"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\"",
		"the FDW leg INSERTs INTO <target> SELECT * FROM <foreign table> (equivalent shape)")

	// The ext-table leg sources from a transient external table (DROP present);
	// the FDW leg sources from a persistent foreign table (NO DROP). This is the
	// ONLY material difference in the load mechanism.
	assert.Contains(s.T(), extScript, "CREATE EXTERNAL TABLE")
	assert.Contains(s.T(), extScript, "DROP EXTERNAL TABLE IF EXISTS")
	assert.Contains(s.T(), fdwScript, "CREATE FOREIGN TABLE IF NOT EXISTS")
	assert.NotContains(s.T(), fdwScript, "DROP")

	// Both run ANALYZE <target> on the read path (the target statistics refresh
	// is identical), proving the load semantics are equivalent.
	assert.Contains(s.T(), extScript, "ANALYZE \"public\".\"events\"")
	assert.Contains(s.T(), fdwScript, "ANALYZE \"public\".\"events\"")
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario103Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario103_CatalogHonest iterates cases.Scenario103Cases() and
// resolves EVERY builder/webhook row against the REAL built artifact: the FDW DDL
// chain + load script (in the dataload Job Args[0]) and the W.25 / W.17 DENY/ADMIT
// paths. Reconcile + live rows are logged + skipped (resolved at integration /
// e2e Part B). NO cloudberry_pxf_* / data_loading_* metric is asserted here.
func (s *Scenario103Suite) TestFunctional_Scenario103_CatalogHonest() {
	catalog := cases.Scenario103Cases()
	require.NotEmpty(s.T(), catalog)

	fdwScript := s.scenario103Script(scenario103FDWReadJob())
	whereJob := scenario103FDWReadJob()
	whereJob.PxfJob.SourceFilter = "region='us-east'"
	whereScript := s.scenario103Script(whereJob)

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Layer {
			case cases.Scenario103LayerLive:
				s.T().Logf("scenario103 %s (%s): [LIVE-ONLY] %s — resolved at e2e Part B",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario103LayerReconcile:
				s.T().Logf("scenario103 %s (%s): %s — resolved at integration/e2e",
					tc.ID, tc.Req, tc.Expected)
				return

			case cases.Scenario103LayerWebhook:
				s.scenario103ResolveWebhookRow(tc)

			case cases.Scenario103LayerBuilder:
				s.scenario103ResolveBuilderRow(tc, fdwScript, whereScript)

			default:
				s.T().Logf("scenario103 %s: layer %q resolved elsewhere", tc.ID, tc.Layer)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

// scenario103ResolveWebhookRow resolves a webhook catalog row by exercising the
// matching accept/deny path through the validate webhook.
func (s *Scenario103Suite) scenario103ResolveWebhookRow(tc cases.Scenario103Case) {
	id := strings.ToLower(strings.ReplaceAll(tc.ID, "-", ""))
	switch tc.ID {
	case "SC103-W25-ENUM":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.LoadMethod = "bogus"
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC103-W25-FDW-WRITABLE":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.Mode = "writable"
		job.PxfJob.Profile = "s3:text"
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC103-W25-FDW-CONTINUOUS":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.Continuous = util.Ptr(true)
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	case "SC103-W25-FDW-OK":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.LoadMethod = "fdw"
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.NoErrorf(s.T(), err, "%s (%s) must be ADMITTED", tc.ID, tc.Req)

	case "SC103-W17-FDW-FILTER":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.LoadMethod = "fdw"
		job.PxfJob.SourceFilter = "region='us-east'"
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.NoErrorf(s.T(), err, "%s (%s) must be ADMITTED", tc.ID, tc.Req)

	case "SC103-W17-EXT-READ-DENY":
		job := scenario103WebhookJob("cat-" + id)
		job.PxfJob.SourceFilter = "region='us-east'"
		_, err := s.validator.ValidateCreate(s.ctx, scenario103Cluster("s103-cat-"+id, job))
		require.Errorf(s.T(), err, "%s (%s) must be DENIED", tc.ID, tc.Req)

	default:
		s.T().Fatalf("scenario103 %s: unknown webhook row", tc.ID)
	}
}

// scenario103ResolveBuilderRow resolves a builder catalog row against the
// already-built FDW DDL chain + load script (the filtered variant for the WHERE
// row).
func (s *Scenario103Suite) scenario103ResolveBuilderRow(
	tc cases.Scenario103Case, fdwScript, whereScript string,
) {
	switch tc.ID {
	case "SC103-EX5-OPTS":
		// The catalog Contains pins the CREATE SERVER OPTIONS (config '<pxf-server>').
		// Build the s3:text variant (live CSV sample) and assert the SERVER carries
		// config '<server>' (NOT resource — the validator rejects it there) and the
		// FOREIGN TABLE carries the csv format (s3:text -> csv).
		job := scenario103FDWReadJob()
		job.PxfJob.Profile = "s3:text"
		script := s.scenario103Script(job)
		assert.Contains(s.T(), script, tc.Contains,
			"%s: the FDW CREATE SERVER OPTIONS must carry config '<pxf-server>'", tc.ID)
		assert.NotContains(s.T(), script,
			"FOREIGN DATA WRAPPER s3_pxf_fdw\n  OPTIONS (resource",
			"%s: the CREATE SERVER must NOT carry a resource OPTION", tc.ID)
		assert.Contains(s.T(), script,
			"OPTIONS (resource 's3a://cloudberry-data/events/', format 'csv')",
			"%s: the s3:text FOREIGN TABLE OPTIONS must carry format 'csv'", tc.ID)

	case "SC103-EX5-WRAPPER-JDBC":
		// The catalog Contains pins the jdbc wrapper. Build a jdbc FDW job (with
		// a jdbc server) and assert the jdbc_pxf_fdw wrapper.
		job := cbv1alpha1.DataLoadingJob{
			Name: "jdbc-fdw", Type: "pxf", Enabled: true,
			PxfJob: &cbv1alpha1.PxfJobSpec{
				Server: "mysql-oltp", Profile: "jdbc", Resource: "sales.orders",
				TargetTable: "public.orders", LoadMethod: "fdw",
			},
		}
		cluster := scenario103Cluster("s103-cat-jdbc", job)
		cluster.Spec.DataLoading.Pxf.Servers = append(cluster.Spec.DataLoading.Pxf.Servers,
			cbv1alpha1.PxfServerSpec{
				Name: "mysql-oltp", Type: "jdbc",
				Config: map[string]string{
					"jdbc.driver": "com.mysql.cj.jdbc.Driver",
					"jdbc.url":    "jdbc:mysql://mysql:3306/oltp",
				},
			})
		out := s.builder.BuildDataLoadJob(cluster, job)
		require.NotNil(s.T(), out)
		assert.Contains(s.T(), out.Spec.Template.Spec.Containers[0].Args[0], tc.Contains,
			"%s: the jdbc FDW must use the jdbc_pxf_fdw wrapper", tc.ID)

	case "SC103-EX7-FTABLE-OPTS":
		// The resource/format OPTIONS live ONLY on the FOREIGN TABLE (the
		// pg_foreign_table level) — exactly ONCE. The SERVER carries config instead
		// (the pxf_fdw validator rejects resource at the pg_foreign_server level).
		assert.Equal(s.T(), 1,
			strings.Count(fdwScript, "OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet')"),
			"%s: resource/format OPTIONS must appear ONLY on the FOREIGN TABLE", tc.ID)
		assert.Equal(s.T(), 1,
			strings.Count(fdwScript, "OPTIONS (config 's3-datalake')"),
			"%s: the CREATE SERVER must carry config '<pxf-server>' (not resource)", tc.ID)

	case "SC103-EX8-WHERE":
		assert.Contains(s.T(), whereScript,
			"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\" "+
				"WHERE region='us-east'",
			"%s: the filtered INSERT must carry the WHERE predicate", tc.ID)

	case "SC103-EX7-PERSIST":
		assert.NotContains(s.T(), fdwScript, "DROP FOREIGN TABLE")
		assert.NotContains(s.T(), fdwScript, "DROP SERVER")
		assert.NotContains(s.T(), fdwScript, "DROP EXTERNAL TABLE")

	default:
		if tc.Contains != "" {
			assert.Containsf(s.T(), fdwScript, tc.Contains,
				"%s built FDW artifact must carry %q", tc.ID, tc.Contains)
		}
	}
}

// scenario103AssertLoadJob is a small structural helper kept for parity with the
// scenario101/102 suites; it asserts the built load Job is well-named.
func (s *Scenario103Suite) scenario103AssertLoadJob(job cbv1alpha1.DataLoadingJob) *batchv1.Job {
	cluster := scenario103Cluster("s103-name", job)
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), out.Name)
	return out
}

// TestFunctional_Scenario103_LoadJobName asserts the deterministic dataload Job
// name for the FDW job (<cluster>-dataload-<job>).
func (s *Scenario103Suite) TestFunctional_Scenario103_LoadJobName() {
	s.scenario103AssertLoadJob(scenario103FDWReadJob())
}

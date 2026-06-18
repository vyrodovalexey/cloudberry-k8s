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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 99: Writable External Tables / Data Export (functional)
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario99Cases — this is Scenario 99, the
// WRITABLE-EXPORT verification scenario. It mirrors the Scenario 98 functional
// SHAPE exactly, driving the BUILDER (DDL/script) and the VALIDATOR (webhook)
// WITHOUT a live cluster, asserting the shipped production contract:
//
//   - FE.9 / FE.10 / FE.11 (builder/DDL): a WRITABLE export job (mode:writable)
//     on s3:text, hdfs:text and bare jdbc → the generated DDL (BuildDataLoadJob →
//     Args[0]) carries "CREATE WRITABLE EXTERNAL TABLE" +
//     "FORMATTER='pxfwritable_export'" + the REVERSED INSERT
//     (INSERT INTO <ext> SELECT * FROM <target>). NO LOG ERRORS / SEGMENT REJECT
//     LIMIT (writable tables take no reject limit).
//
//   - WE.2: assert the FORMATTER='pxfwritable_export' clause + the correct format
//     per profile (FORMAT 'CUSTOM' for text/parquet/avro on object-store/hdfs).
//
//   - SF.1: a writable export with SourceFilter="region='us-east'" → the export
//     SCRIPT carries `... WHERE region='us-east'`; without a SourceFilter → no
//     WHERE (byte-stable baseline).
//
//   - SF.2: the validate (webhook) path — sourceFilter on a read/import job →
//     DENY (the message names "sourceFilter" + "writable"); a writable job with a
//     dangerous sourceFilter (";DROP") → DENY (statement terminators); a clean
//     writable + sourceFilter → admit.
//
//   - Writable-reject parity (cross-ref W.10b): hdfs:json / hive:text writable
//     still DENY (a quick guard so the writable matrix stays honest).
//
//   - CatalogHonest: resolve each cases.Scenario99Cases() row against the REAL
//     built artifact (writable rows assert FORMATTER='pxfwritable_export'; SF.1
//     asserts the WHERE; SF.2 asserts the admission DENY).
//
// METRIC HONESTY: cloudberry_pxf_bytes_transferred_total stays PLANNED and is
// NEVER asserted here. This functional layer asserts the DDL/script + admission
// contract only; the live "data lands" signals (object/file/row landing) are at
// e2e Part B.
// ============================================================================

// Scenario99Suite exercises the writable-export DDL/script + W.17 sourceFilter
// contract at the builder + validator layer.
type Scenario99Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario99(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario99Suite))
}

func (s *Scenario99Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario99Servers returns the PXF server set the Scenario 99 sample CR ships:
// the MinIO-backed object-store servers (s3-datalake, minio-warehouse), the
// hadoop-cluster (hdfs), and the two JDBC servers (postgres-source for the
// writable target, mysql-oltp). Values mirror the sample CR.
func scenario99Servers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
		{
			Name:   cases.Scenario99ServerS3Datalake,
			Type:   "s3",
			Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
			CredentialSecrets: []cbv1alpha1.SecretReference{
				{Name: "backup-s3-credentials", Key: "aws_access_key_id"},
				{Name: "backup-s3-credentials", Key: "aws_secret_access_key"},
			},
		},
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
		{
			Name: cases.Scenario99ServerMySQLOLTP,
			Type: "jdbc",
			Config: map[string]string{
				"jdbc.driver": "com.mysql.cj.jdbc.Driver",
				"jdbc.url":    "jdbc:mysql://mysql:3306/oltp",
			},
		},
	}
}

// scenario99Cluster builds a running cluster carrying the Scenario 99 server set
// plus the supplied jobs.
func scenario99Cluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: scenario99Servers(),
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario99Resource returns a deterministic export resource for a profile so the
// synthesized export LOCATION is byte-stable.
func scenario99Resource(profile string) string {
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

// scenario99ExportJob builds a WRITABLE export PXF job with the supplied
// profile/server/sourceFilter. mode is "writable" by default; the caller may
// override it (e.g. for the SF.2 read-job deny).
func scenario99ExportJob(
	id, profile, server, sourceFilter string,
) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s99-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:       server,
			Profile:      profile,
			Resource:     scenario99Resource(profile),
			TargetTable:  "public.s99_" + strings.ToLower(strings.ReplaceAll(id, ".", "_")),
			Mode:         "writable",
			SourceFilter: sourceFilter,
		},
	}
}

// scenario99JobScript builds the load Job for a job and returns its script
// (container args[0]), failing the test if the Job/container is missing.
func (s *Scenario99Suite) scenario99JobScript(
	cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob,
) string {
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNilf(s.T(), out, "BuildDataLoadJob must produce a Job for %q", job.Name)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// ----------------------------------------------------------------------------
// FE.9 / FE.10 / FE.11 — WRITABLE export DDL + reversed INSERT
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_WritableExportDDL builds writable export jobs on
// s3:text (FE.9/WE.1), hdfs:text (FE.10) and bare jdbc (FE.11) → asserts the
// generated DDL carries "CREATE WRITABLE EXTERNAL TABLE" +
// "FORMATTER='pxfwritable_export'" + the REVERSED INSERT
// (INSERT INTO <ext> SELECT * FROM <target>), with NO LOG ERRORS / SEGMENT
// REJECT LIMIT.
func (s *Scenario99Suite) TestFunctional_Scenario99_WritableExportDDL() {
	cluster := scenario99Cluster("s99-export")
	exports := []struct{ id, profile, server string }{
		{"FE.9", "s3:text", cases.Scenario99ServerMinioWarehouse},
		{"FE.10", "hdfs:text", cases.Scenario99ServerHadoopCluster},
		{"FE.11", "jdbc", cases.Scenario99ServerPostgresSource},
	}
	for _, tc := range exports {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario99ExportJob(tc.id, tc.profile, tc.server, "")
			script := s.scenario99JobScript(cluster, job)

			assert.Containsf(s.T(), script, cases.Scenario99WritableDDL,
				"%s must emit CREATE WRITABLE EXTERNAL TABLE", tc.id)
			assert.Containsf(s.T(), script, cases.Scenario99WritableFormatter,
				"%s must emit FORMATTER='pxfwritable_export'", tc.id)
			assert.Containsf(s.T(), script, "PROFILE="+tc.profile,
				"%s LOCATION must carry the profile", tc.id)

			// The export INSERT is REVERSED: the WRITABLE external (tmp) table is
			// the INSERT TARGET and the cluster table is the SOURCE. The builder
			// names the tmp table cbk_dataload_ext_<sanitized-job> and quotes the
			// (schema-qualified) target.
			tmp := scenario99TmpTable(job.Name)
			target := scenario99QuotedTarget("public.s99_" +
				strings.ToLower(strings.ReplaceAll(tc.id, ".", "_")))
			assert.Containsf(s.T(), script,
				"INSERT INTO "+tmp+" SELECT * FROM "+target,
				"%s must emit the reversed export INSERT (INTO <ext> FROM <target>)", tc.id)

			// Writable tables take NO error-handling suffix.
			assert.NotContains(s.T(), script, "LOG ERRORS")
			assert.NotContains(s.T(), script, "SEGMENT REJECT LIMIT")
			// It is an EXPORT, not an import.
			assert.NotContains(s.T(), script, "pxfwritable_import")
		})
	}
}

// scenario99TmpTable mirrors the builder's temp external-table name derivation
// (the writable ext table the export INSERTs INTO): the prefix
// "cbk_dataload_ext_" + a sanitized job name (lowercase, non [a-z0-9_] → '_'),
// double-quoted as a single identifier.
func scenario99TmpTable(jobName string) string {
	return `"cbk_dataload_ext_` + scenario99SanitizeIdent(jobName) + `"`
}

// scenario99SanitizeIdent reduces a string to a safe unquoted-identifier body,
// mirroring the builder's sanitizeSQLIdentBody (lowercase; non [a-z0-9_] → '_').
func scenario99SanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// scenario99QuotedTarget mirrors the builder's quoteSQLIdentifier: each
// dot-separated part is double-quoted independently ("public"."s99_x").
func scenario99QuotedTarget(ident string) string {
	parts := strings.Split(ident, ".")
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(p, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

// ----------------------------------------------------------------------------
// WE.2 — DATA LANDS WITH CORRECT FORMAT (FORMATTER + format per profile)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_FormatterAndFormat asserts the WE.2 "correct format"
// gate: every writable export DDL carries FORMATTER='pxfwritable_export' and the
// FORMAT 'CUSTOM' clause (the writable formatter is a CUSTOM format), per the
// profile under test.
func (s *Scenario99Suite) TestFunctional_Scenario99_FormatterAndFormat() {
	cluster := scenario99Cluster("s99-format")
	formats := []struct{ id, profile, server string }{
		{"WE.2-s3", "s3:text", cases.Scenario99ServerMinioWarehouse},
		{"WE.2-hdfs", "hdfs:text", cases.Scenario99ServerHadoopCluster},
		{"WE.2-jdbc", "jdbc", cases.Scenario99ServerPostgresSource},
	}
	for _, tc := range formats {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario99ExportJob(tc.id, tc.profile, tc.server, "")
			script := s.scenario99JobScript(cluster, job)
			assert.Containsf(s.T(), script, cases.Scenario99WritableFormatter,
				"%s WE.2 gate: DDL must carry FORMATTER='pxfwritable_export'", tc.id)
			assert.Containsf(s.T(), script, "FORMAT 'CUSTOM'",
				"%s writable export uses the CUSTOM format with the export formatter", tc.id)
		})
	}
}

// ----------------------------------------------------------------------------
// SF.1 — SOURCEFILTER → WHERE in the export script
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_SourceFilterWhere asserts the SF.1 contract: a
// writable export with SourceFilter="region='us-east'" → the export SCRIPT emits
// `INSERT INTO <ext> SELECT * FROM <target> WHERE region='us-east'`; without a
// SourceFilter → NO WHERE (the byte-stable baseline). DDL is unchanged either way.
func (s *Scenario99Suite) TestFunctional_Scenario99_SourceFilterWhere() {
	cluster := scenario99Cluster("s99-sf")

	// With SourceFilter → the WHERE fragment is present.
	s.Run("SF.1-with-filter", func() {
		job := scenario99ExportJob("SF.1", "s3:text",
			cases.Scenario99ServerMinioWarehouse, cases.Scenario99SourceFilter)
		script := s.scenario99JobScript(cluster, job)
		assert.Contains(s.T(), script, cases.Scenario99WhereFragment,
			"SF.1 filtered export must carry WHERE region='us-east'")
		// DDL is unchanged — the writable formatter is still present.
		assert.Contains(s.T(), script, cases.Scenario99WritableFormatter)
	})

	// Without SourceFilter → NO WHERE (baseline).
	s.Run("SF.1-baseline-no-filter", func() {
		job := scenario99ExportJob("SF.1b", "s3:text",
			cases.Scenario99ServerMinioWarehouse, "")
		script := s.scenario99JobScript(cluster, job)
		assert.NotContains(s.T(), script, "WHERE region",
			"unfiltered export must NOT carry a WHERE")
		// The reversed INSERT is still emitted (without a WHERE).
		assert.Contains(s.T(), script, "SELECT * FROM")
	})
}

// ----------------------------------------------------------------------------
// SF.2 — WEBHOOK W.17 (mode gate + sanity check)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_SourceFilterAdmission drives the validate (webhook)
// path for W.17: sourceFilter on a read/import job → DENY (names "sourceFilter"
// and "writable"); sourceFilter=";DROP" on a writable job → DENY (statement
// terminators); a clean writable + sourceFilter → admit.
func (s *Scenario99Suite) TestFunctional_Scenario99_SourceFilterAdmission() {
	// SF.2(a): sourceFilter on a READ/import job (mode unset) → DENY.
	s.Run("SF.2a-read-job-deny", func() {
		job := scenario99ExportJob("SF.2a", "s3:text", cases.Scenario99ServerMinioWarehouse,
			cases.Scenario99SourceFilter)
		job.PxfJob.Mode = "" // read/import, NOT writable
		cluster := scenario99Cluster("s99-sf2a", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err, "SF.2 sourceFilter on a read job must be DENIED")
		assert.Contains(s.T(), err.Error(), "sourceFilter")
		assert.Contains(s.T(), err.Error(), "writable")
	})

	// SF.2b: a writable job with a dangerous sourceFilter (";DROP") → DENY.
	s.Run("SF.2b-dangerous-filter-deny", func() {
		job := scenario99ExportJob("SF.2b", "s3:text", cases.Scenario99ServerMinioWarehouse,
			cases.Scenario99DangerousFilter)
		cluster := scenario99Cluster("s99-sf2b", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.Error(s.T(), err, "SF.2b dangerous sourceFilter must be DENIED")
		assert.Contains(s.T(), err.Error(), "statement terminators or SQL comments")
	})

	// A clean writable + sourceFilter → ADMIT.
	s.Run("SF.1-clean-writable-admit", func() {
		job := scenario99ExportJob("SF.1", "s3:text", cases.Scenario99ServerMinioWarehouse,
			cases.Scenario99SourceFilter)
		cluster := scenario99Cluster("s99-sf1-admit", job)
		_, err := s.validator.ValidateCreate(s.ctx, cluster)
		require.NoError(s.T(), err, "a clean writable + sourceFilter must be ADMITTED")
	})
}

// ----------------------------------------------------------------------------
// Writable-reject parity (cross-ref W.10b) — quick guard
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_WritableRejectParity is a quick cross-ref guard that
// the writable matrix (W.10b) still rejects write-unsupported profiles even on
// the Scenario 99 server set: hdfs:json and hive:text writable → DENY, and the
// builder defensively refuses the writable DDL.
func (s *Scenario99Suite) TestFunctional_Scenario99_WritableRejectParity() {
	rejects := []struct{ id, profile, server string }{
		{"WRej-hdfs-json", "hdfs:json", cases.Scenario99ServerHadoopCluster},
		{"WRej-hive-text", "hive:text", cases.Scenario99ServerHadoopCluster},
	}
	for _, tc := range rejects {
		tc := tc
		s.Run(tc.id, func() {
			job := scenario99ExportJob(tc.id, tc.profile, tc.server, "")
			cluster := scenario99Cluster("s99-"+strings.ToLower(strings.ReplaceAll(tc.id, ".", "-")), job)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "%s write-unsupported profile must be DENIED", tc.id)
			assert.Contains(s.T(), err.Error(), "write-unsupported")
			// Defense in depth: builder refuses the writable DDL.
			assert.Nilf(s.T(), s.builder.BuildDataLoadJob(cluster, job),
				"%s builder must refuse the writable DDL", tc.id)
		})
	}
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario99Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario99_CatalogHonest iterates cases.Scenario99Cases() and
// resolves EVERY row against the REAL built artifact: writable export rows assert
// FORMATTER='pxfwritable_export' + the reversed INSERT; SF.1 asserts the WHERE
// script delta; SF.2 asserts the admission DENY. This keeps the catalog honest
// against the implementation. bytes_transferred is NEVER asserted.
func (s *Scenario99Suite) TestFunctional_Scenario99_CatalogHonest() {
	catalog := cases.Scenario99Cases()
	require.Len(s.T(), catalog, 6, "FE.9/WE.1 + FE.10 + FE.11 + WE.2 + SF.1 + SF.2")

	cluster := scenario99Cluster("s99-catalog")

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			switch tc.Expected {
			case cases.Scenario99ExpectDenySourceFilter:
				// SF.2: a read-job sourceFilter must be DENIED at admission.
				job := scenario99ExportJob(tc.ID, tc.Profile, tc.Server, tc.SourceFilter)
				job.PxfJob.Mode = tc.Mode // "" => read/import
				denyCluster := scenario99Cluster("s99-cat-"+
					strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job)
				_, err := s.validator.ValidateCreate(s.ctx, denyCluster)
				require.Errorf(s.T(), err, "%s must be DENIED", tc.ID)
				assert.Contains(s.T(), err.Error(), "sourceFilter")

			default:
				// All other rows are writable exports: resolve the DDLContains
				// substring against the built script, and assert the writable
				// formatter + admission.
				require.NotEmptyf(s.T(), tc.DDLContains, "%s must name a DDLContains", tc.ID)
				job := scenario99ExportJob(tc.ID, tc.Profile, tc.Server, tc.SourceFilter)
				job.PxfJob.Mode = tc.Mode

				_, err := s.validator.ValidateCreate(s.ctx, scenario99Cluster(
					"s99-cat-admit-"+strings.ToLower(strings.ReplaceAll(tc.ID, ".", "-")), job))
				require.NoErrorf(s.T(), err, "%s writable export must be ADMITTED", tc.ID)

				script := s.scenario99JobScript(cluster, job)
				assert.Containsf(s.T(), script, tc.DDLContains,
					"%s built artifact must carry %q", tc.ID, tc.DDLContains)
				// Every writable export row carries the export formatter.
				assert.Containsf(s.T(), script, cases.Scenario99WritableFormatter,
					"%s writable export must use the export formatter", tc.ID)
			}

			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario99 %s: [CONFIG-ONLY] — DDL/admission correctness only", tc.ID)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

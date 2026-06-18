package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// fdwReadJob returns the canonical Scenario 103 FDW read/import job: an s3:parquet
// PXF job with loadMethod=fdw, server s3-datalake, target public.events and the
// MinIO events prefix as the resource. It is the byte-golden fixture for the
// EX.5-EX.7 FDW DDL chain and the EX.8 FDW load script.
func fdwReadJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "fdw-ingest",
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:parquet",
			Resource:    "s3a://cloudberry-data/events/",
			TargetTable: "public.events",
			LoadMethod:  "fdw",
		},
	}
}

// TestBuildFDWDDL_GoldenS3Parquet (SC103-EX5/6/7) is the byte-exact FDW DDL
// golden for the canonical s3:parquet read job. It pins the full, persistent,
// idempotent CREATE SERVER / USER MAPPING / FOREIGN TABLE chain (EX.5-EX.7) so
// the generated DDL stays deterministic: server "foreign_s3_datalake", wrapper
// s3_pxf_fdw (per-scheme map), USER MAPPING FOR "gpadmin", foreign table
// "foreign_fdw_ingest" (LIKE "public"."events"), all IF NOT EXISTS, OPTIONS
// carrying resource + format 'parquet'.
func TestBuildFDWDDL_GoldenS3Parquet(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
	job := fdwReadJob()

	want := "CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"\n" +
		"  FOREIGN DATA WRAPPER s3_pxf_fdw\n" +
		"  OPTIONS (config 's3-datalake');\n" +
		"CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"\n" +
		"  SERVER \"foreign_s3_datalake\";\n" +
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")\n" +
		"  SERVER \"foreign_s3_datalake\"\n" +
		"  OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet');"

	got, err := buildFDWDDL(cluster, job)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// EX.5: CREATE SERVER + wrapper; SERVER carries OPTIONS (config '<pxf-server>'),
	// NOT resource/format (the pxf_fdw validator rejects `resource` at the server
	// level — it can only be defined at the pg_foreign_table level).
	assert.Contains(t, got, "CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"")
	assert.Contains(t, got, "FOREIGN DATA WRAPPER s3_pxf_fdw")
	assert.Contains(t, got, "OPTIONS (config 's3-datalake')")
	// The CREATE SERVER line must NOT carry a resource OPTION.
	serverLine := "  FOREIGN DATA WRAPPER s3_pxf_fdw\n  OPTIONS (config 's3-datalake');"
	assert.Contains(t, got, serverLine)
	assert.NotContains(t, got, "FOREIGN DATA WRAPPER s3_pxf_fdw\n  OPTIONS (resource")
	// EX.6: USER MAPPING for the gpadmin data-loader role.
	assert.Contains(t, got,
		"CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"\n  SERVER \"foreign_s3_datalake\"")
	// EX.7: FOREIGN TABLE (LIKE target) on the server; resource/format OPTIONS
	// live HERE (the pg_foreign_table level), not on the SERVER.
	assert.Contains(t, got,
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")")
	assert.Contains(t, got, "SERVER \"foreign_s3_datalake\"")
	assert.Contains(t, got, "OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet')")
	// Persistence: the DDL never drops the objects.
	assert.NotContains(t, got, "DROP")

	// Determinism: same input => byte-identical output.
	again, err := buildFDWDDL(cluster, job)
	require.NoError(t, err)
	assert.Equal(t, got, again)
}

// TestBuildFDWDDL_JDBCNoFormat (SC103-EX5-WRAPPER-JDBC) asserts a BARE jdbc
// profile resolves the jdbc_pxf_fdw wrapper AND omits the `format` OPTION (a
// JDBC FDW takes a resource=table, no format suffix to emit).
func TestBuildFDWDDL_JDBCNoFormat(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
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

	want := "CREATE SERVER IF NOT EXISTS \"foreign_mysql_oltp\"\n" +
		"  FOREIGN DATA WRAPPER jdbc_pxf_fdw\n" +
		"  OPTIONS (config 'mysql-oltp');\n" +
		"CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"\n" +
		"  SERVER \"foreign_mysql_oltp\";\n" +
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_jdbc_fdw\" (LIKE \"public\".\"orders\")\n" +
		"  SERVER \"foreign_mysql_oltp\"\n" +
		"  OPTIONS (resource 'sales.orders');"

	got, err := buildFDWDDL(cluster, job)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Contains(t, got, "FOREIGN DATA WRAPPER jdbc_pxf_fdw")
	// The SERVER carries OPTIONS (config '<pxf-server>') regardless of profile.
	assert.Contains(t, got, "OPTIONS (config 'mysql-oltp')")
	// The FOREIGN TABLE carries resource but, for bare jdbc, NO format OPTION.
	assert.Contains(t, got, "OPTIONS (resource 'sales.orders')")
	assert.NotContains(t, got, "format ", "bare jdbc must omit the format OPTION")
}

// TestBuildFDWDDL_Errors covers the build-time error branches: nil pxfJob,
// missing targetTable, missing profile.
func TestBuildFDWDDL_Errors(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
	}{
		{"nil pxfJob", cbv1alpha1.DataLoadingJob{Name: "x", Type: "pxf"}},
		{"missing target", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "pxf",
			PxfJob: &cbv1alpha1.PxfJobSpec{Profile: "s3:parquet", LoadMethod: "fdw"},
		}},
		{"missing profile", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "pxf",
			PxfJob: &cbv1alpha1.PxfJobSpec{TargetTable: "t", LoadMethod: "fdw"},
		}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildFDWDDL(cluster, tc.job)
			assert.Error(t, err)
		})
	}
}

// TestFDWWrapperForProfile (SC103-EX5-WRAPPER) is the per-scheme wrapper map
// matrix: each registered object-store/jdbc/hadoop scheme maps to its OWN
// live-verified wrapper, and an unknown scheme falls back to the generic
// pxf_fdw. The schemes are NOT collapsed (gs uses gs_pxf_fdw, not s3_pxf_fdw).
func TestFDWWrapperForProfile(t *testing.T) {
	tests := []struct {
		profile string
		want    string
	}{
		{"s3:parquet", "s3_pxf_fdw"},
		{"s3", "s3_pxf_fdw"},
		{"gs:text", "gs_pxf_fdw"},
		{"abfss:parquet", "abfss_pxf_fdw"},
		{"wasbs:text", "wasbs_pxf_fdw"},
		{"jdbc", "jdbc_pxf_fdw"},
		{"hdfs:text", "hdfs_pxf_fdw"},
		{"hive", "hive_pxf_fdw"},
		{"hive:orc", "hive_pxf_fdw"},
		{"hbase", "hbase_pxf_fdw"},
		// Case-insensitive scheme match.
		{"S3:PARQUET", "s3_pxf_fdw"},
		{"HBase", "hbase_pxf_fdw"},
		// Unknown / custom-connector / empty scheme => generic fallback.
		{"kafka", "pxf_fdw"},
		{"nonsense", "pxf_fdw"},
		{"", "pxf_fdw"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.profile, func(t *testing.T) {
			assert.Equal(t, tc.want, fdwWrapperForProfile(tc.profile))
		})
	}
}

// TestFDWFormatOption covers the FDW `format` OPTION suffix derivation: the part
// after ":" for a profile carrying a format, and "" for a bare profile (so the
// caller omits the OPTION).
func TestFDWFormatOption(t *testing.T) {
	tests := []struct {
		profile string
		want    string
	}{
		{"s3:parquet", "parquet"},
		// s3:text maps to the FDW `csv` format: object-store text data is
		// comma-delimited CSV, whereas the pxf_fdw `text` format is tab-delimited.
		{"s3:text", "csv"},
		{"hdfs:SequenceFile", "SequenceFile"},
		{"jdbc", ""},
		{"hive", ""},
		{"", ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.profile, func(t *testing.T) {
			assert.Equal(t, tc.want, fdwFormatOption(tc.profile))
		})
	}
}

// TestFDWDerivedNames covers the deterministic derived FDW identifiers: the
// "foreign_"-prefixed, sanitized server / foreign-table names.
func TestFDWDerivedNames(t *testing.T) {
	assert.Equal(t, "foreign_s3_datalake", fdwServerName("s3-datalake"))
	assert.Equal(t, "foreign_fdw_ingest", fdwForeignTableName("fdw-ingest"))
	assert.Equal(t, "foreign_mysql_oltp", fdwServerName("mysql-oltp"))
	// Empty inputs fall back to the sanitizer's "job" default (still prefixed).
	assert.Equal(t, "foreign_job", fdwServerName(""))
	assert.Equal(t, "foreign_job", fdwForeignTableName(""))
}

// TestFDWOptionsClause covers the shared OPTIONS clause: resource always
// present (single-quoted literal), format only when non-empty, injection-safe
// single-quote doubling for the resource literal.
func TestFDWOptionsClause(t *testing.T) {
	assert.Equal(t, "OPTIONS (resource 's3a://b/p/', format 'parquet')",
		fdwOptionsClause("s3a://b/p/", "parquet"))
	assert.Equal(t, "OPTIONS (resource 'sales.orders')",
		fdwOptionsClause("sales.orders", ""))
	// Embedded single quote in the resource is doubled (no injection surface).
	assert.Equal(t, "OPTIONS (resource 'a''b')", fdwOptionsClause("a'b", ""))
}

// TestFDWServerOptionsClause covers the CREATE SERVER OPTIONS clause: it carries
// OPTIONS (config '<pxf-server>') — the named PXF server config whose
// credentials/endpoint the FDW read resolves — NOT resource/format (the pxf_fdw
// validator rejects `resource` at the pg_foreign_server level). The pxf server
// name is a single-quoted literal (injection-safe single-quote doubling).
func TestFDWServerOptionsClause(t *testing.T) {
	assert.Equal(t, "OPTIONS (config 's3-datalake')",
		fdwServerOptionsClause("s3-datalake"))
	assert.Equal(t, "OPTIONS (config 'mysql-oltp')",
		fdwServerOptionsClause("mysql-oltp"))
	// Embedded single quote in the server name is doubled (no injection surface).
	assert.Equal(t, "OPTIONS (config 'a''b')", fdwServerOptionsClause("a'b"))
	// The SERVER OPTIONS never carry resource/format.
	assert.NotContains(t, fdwServerOptionsClause("s3-datalake"), "resource")
	assert.NotContains(t, fdwServerOptionsClause("s3-datalake"), "format")
}

// TestIsFDWPxfJob covers the FDW routing predicate: true only for a PXF job with
// loadMethod=fdw (case-insensitive); false for non-fdw PXF, gpload, and nil-pxf.
func TestIsFDWPxfJob(t *testing.T) {
	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
		want bool
	}{
		{"fdw pxf job", fdwReadJob(), true},
		{
			name: "uppercase FDW (case-insensitive)",
			job: func() cbv1alpha1.DataLoadingJob {
				j := fdwReadJob()
				j.PxfJob.LoadMethod = "FDW"
				return j
			}(),
			want: true,
		},
		{"external-table pxf job is not fdw", pxfTestJob(), false},
		{
			name: "explicit external-table is not fdw",
			job: func() cbv1alpha1.DataLoadingJob {
				j := pxfTestJob()
				j.PxfJob.LoadMethod = "external-table"
				return j
			}(),
			want: false,
		},
		{"gpload job is not fdw", gploadTestJob(), false},
		{"pxf with nil PxfJob is not fdw", cbv1alpha1.DataLoadingJob{Name: "x", Type: "pxf"}, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isFDWPxfJob(tc.job))
		})
	}
}

// TestBuildDataLoadJob_FDWScript (SC103-EX7-PERSIST / EX8-DIRECT/INSERT/ANALYZE)
// asserts the FDW load script (delivered as the dataload container Args[0] of a
// loadMethod=fdw job) carries the full EX.5-EX.8 shape: the persistent FDW DDL
// chain, the direct foreign-table count query, the INSERT INTO <target> SELECT *
// FROM <foreign>, the DATALOAD_ROWS marker, ANALYZE <target>, and — crucially —
// NO DROP of the persistent FDW objects. The non-fdw path is asserted unchanged
// elsewhere (TestBuildDataLoadJob_NonFDWScriptUnchanged).
func TestBuildDataLoadJob_FDWScript(t *testing.T) {
	b := NewBuilder()
	job := fdwReadJob()
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	require.Len(t, out.Spec.Template.Spec.Containers, 1)
	require.Len(t, out.Spec.Template.Spec.Containers[0].Args, 1)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// The persistent FDW DDL chain (EX.5-EX.7) is present in args[0].
	assert.Contains(t, script, "CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"")
	assert.Contains(t, script, "FOREIGN DATA WRAPPER s3_pxf_fdw")
	assert.Contains(t, script, "CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"")
	assert.Contains(t, script,
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")")
	// Best-effort pxf_fdw extension install (the existing pattern).
	assert.Contains(t, script, "CREATE EXTENSION IF NOT EXISTS pxf_fdw")

	// EX.8 direct query: the persistent foreign table is queryable directly.
	assert.Contains(t, script, "SELECT count(*) FROM \"foreign_fdw_ingest\"")
	// EX.8 INSERT (equivalent shape) INTO the target from the foreign table.
	assert.Contains(t, script,
		"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\"")
	// Rowcount marker + ANALYZE on the read path.
	assert.Contains(t, script, "DATALOAD_ROWS=")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, "ANALYZE \"public\".\"events\"")

	// SC103-EX7-PERSIST: the FDW objects are NEVER dropped.
	assert.NotContains(t, script, "DROP FOREIGN TABLE")
	assert.NotContains(t, script, "DROP SERVER")
	assert.NotContains(t, script, "DROP EXTERNAL TABLE")
	// No external-table DDL is emitted on the fdw path.
	assert.NotContains(t, script, "CREATE EXTERNAL TABLE")
}

// TestBuildDataLoadJob_FDWScript_Where (SC103-EX8-WHERE) asserts a fdw read job
// carrying a sourceFilter emits the INSERT with the WHERE predicate via the
// shared quoted-heredoc path (single-quote-safe), and still has no DROP.
func TestBuildDataLoadJob_FDWScript_Where(t *testing.T) {
	b := NewBuilder()
	job := fdwReadJob()
	job.PxfJob.SourceFilter = "region='us-east'"
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// The filtered INSERT carries the WHERE predicate (quoted-heredoc form).
	assert.Contains(t, script,
		"INSERT INTO \"public\".\"events\" SELECT * FROM \"foreign_fdw_ingest\" WHERE region='us-east'")
	// The single-quote-safe quoted heredoc is used for the filtered INSERT.
	assert.Contains(t, script, "<<'_CBK_INSERT_EOF_'")
	// Still persistent: no DROP.
	assert.NotContains(t, script, "DROP")
}

// TestBuildFDWDataLoadScript_ByteExact pins the full FDW load script byte-for-
// byte (no filter), locking the EX.5-EX.8 ordering and the persistent shape.
func TestBuildFDWDataLoadScript_ByteExact(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
	job := fdwReadJob()

	got, err := buildFDWDataLoadScript(cluster, job)
	require.NoError(t, err)

	want := "set -euo pipefail\n" +
		gpEnvPreamble +
		"psql -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS pxf_fdw' " +
		"|| echo 'dataload: pxf_fdw extension unavailable (best-effort, continuing)'\n" +
		"psql -v ON_ERROR_STOP=1 <<'_CBK_FDW_DDL_EOF_'\n" +
		"CREATE SERVER IF NOT EXISTS \"foreign_s3_datalake\"\n" +
		"  FOREIGN DATA WRAPPER s3_pxf_fdw\n" +
		"  OPTIONS (config 's3-datalake');\n" +
		"CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"\n" +
		"  SERVER \"foreign_s3_datalake\";\n" +
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_fdw_ingest\" (LIKE \"public\".\"events\")\n" +
		"  SERVER \"foreign_s3_datalake\"\n" +
		"  OPTIONS (resource 's3a://cloudberry-data/events/', format 'parquet');\n" +
		"_CBK_FDW_DDL_EOF_\n" +
		"psql -v ON_ERROR_STOP=1 -c 'SELECT count(*) FROM \"foreign_fdw_ingest\"'\n" +
		"rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO \"public\".\"events\" " +
		"SELECT * FROM \"foreign_fdw_ingest\"' | awk '{print $NF}')\n" +
		"rows=${rows:-0}\n" +
		"echo \"DATALOAD_ROWS=${rows}\"\n" +
		"printf '%s%s' 'DATALOAD_ROWS=' \"${rows}\" > /dev/termination-log 2>/dev/null || true\n" +
		"psql -v ON_ERROR_STOP=1 -c 'ANALYZE \"public\".\"events\"'\n"

	assert.Equal(t, want, got)

	// Determinism: same input => byte-identical output.
	again, err := buildFDWDataLoadScript(cluster, job)
	require.NoError(t, err)
	assert.Equal(t, got, again)
}

// TestBuildDataLoadJob_NonFDWScriptUnchanged (SC103: non-FDW byte-identical)
// confirms a normal external-table read job's script is UNCHANGED by the FDW
// feature: it still emits the transient CREATE EXTERNAL TABLE + DROP path and
// carries NO FDW DDL. The byte-exact external-table goldens live in
// TestBuildExternalTableDDL_ByteExact (unedited); this guards the routing branch.
func TestBuildDataLoadJob_NonFDWScriptUnchanged(t *testing.T) {
	b := NewBuilder()
	job := pxfTestJob() // s3:parquet read, loadMethod unset
	cluster := dataLoadJobCluster([]cbv1alpha1.DataLoadingJob{job}, nil)

	out := b.BuildDataLoadJob(cluster, job)
	require.NotNil(t, out)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// External-table path retained.
	assert.Contains(t, script, "CREATE EXTERNAL TABLE")
	assert.Contains(t, script, "DROP EXTERNAL TABLE IF EXISTS")
	assert.Contains(t, script, "pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake")
	// NO FDW DDL leaks onto the non-fdw path.
	assert.NotContains(t, script, "CREATE SERVER IF NOT EXISTS")
	assert.NotContains(t, script, "CREATE FOREIGN TABLE")
	assert.NotContains(t, script, "FOREIGN DATA WRAPPER")
	assert.False(t, strings.Contains(script, "foreign_"),
		"non-fdw script must carry no FDW foreign_* identifier")

	// And an explicit external-table loadMethod renders the SAME script bytes as
	// the unset default (byte-identical non-fdw path).
	jobExt := pxfTestJob()
	jobExt.PxfJob.LoadMethod = "external-table"
	outExt := b.BuildDataLoadJob(cluster, jobExt)
	require.NotNil(t, outExt)
	assert.Equal(t, script, outExt.Spec.Template.Spec.Containers[0].Args[0])
}

package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// dataLoadDDLCluster returns a minimal cluster usable by the DDL generator (it
// only needs Name + the optional gpfdist port).
func dataLoadDDLCluster(gpfdistPort int32) *cbv1alpha1.CloudberryCluster {
	dl := &cbv1alpha1.DataLoadingSpec{Enabled: true}
	if gpfdistPort > 0 {
		dl.Gpfdist = &cbv1alpha1.GpfdistSpec{Enabled: true, Port: gpfdistPort}
	}
	return &cbv1alpha1.CloudberryCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ddl-cluster", Namespace: "default"},
		Spec: cbv1alpha1.CloudberryClusterSpec{
			Image:       "cloudberrydb/cloudberry:2.1.0",
			DataLoading: dl,
		},
	}
}

// TestBuildExternalTableDDL_ByteExact is the byte-exact DDL matrix: every
// protocol/profile/partition/error-handling combination is asserted against the
// EXACT generated statement so the live-load DDL stays deterministic.
func TestBuildExternalTableDDL_ByteExact(t *testing.T) {
	cluster := dataLoadDDLCluster(8080)

	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
		want string
	}{
		{
			name: "pxf s3 parquet minimal",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-parquet-loader",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3_parquet_loader\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf://s3a://data-lake/events/?PROFILE=s3:parquet&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			name: "pxf with filter pushdown and projection",
			job: cbv1alpha1.DataLoadingJob{
				Name: "proj",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:           "s3srv",
					Profile:          "s3:parquet",
					Resource:         "s3a://bucket/data/",
					TargetTable:      "sales",
					FilterPushdown:   util.Ptr(true),
					ColumnProjection: util.Ptr(true),
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_proj\" (LIKE \"sales\")\n" +
				"LOCATION ('pxf://s3a://bucket/data/?PROFILE=s3:parquet&SERVER=s3srv&FILTER_PUSHDOWN=true&PROJECT=true')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// FE.2 (Scenario 98) — jdbc profile with FilterPushdown=true emits
			// FILTER_PUSHDOWN=true in the LOCATION (the strongest source-log proof
			// path: the WHERE predicate reaches the DB). ColumnProjection unset =>
			// PROJECT absent (it is only emitted when explicitly true here; the
			// mutating webhook defaults it, but the builder is exercised directly).
			name: "FE.2 pxf jdbc filter pushdown only",
			job: cbv1alpha1.DataLoadingJob{
				Name: "jdbc-fp",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:         "mysql-oltp",
					Profile:        "jdbc",
					Resource:       "sales.orders",
					TargetTable:    "public.orders",
					FilterPushdown: util.Ptr(true),
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_jdbc_fp\" (LIKE \"public\".\"orders\")\n" +
				"LOCATION ('pxf://sales.orders?PROFILE=jdbc&SERVER=mysql-oltp&FILTER_PUSHDOWN=true')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// FE.1/FE.4 NEGATIVE (Scenario 98) — FilterPushdown=false AND
			// ColumnProjection=false on an object-store profile emit NEITHER
			// FILTER_PUSHDOWN nor PROJECT in the LOCATION (an explicit user false
			// is honored; the URI carries only PROFILE+SERVER).
			name: "FE.1/FE.4 pxf s3 parquet pushdown and projection explicit false omitted",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-off",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:           "s3-datalake",
					Profile:          "s3:parquet",
					Resource:         "s3a://bucket/data/",
					TargetTable:      "public.events",
					FilterPushdown:   util.Ptr(false),
					ColumnProjection: util.Ptr(false),
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3_off\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf://s3a://bucket/data/?PROFILE=s3:parquet&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// FE.4/FE.5 (Scenario 98) — ColumnProjection=true ONLY (FilterPushdown
			// unset) on a wide-parquet read emits PROJECT=true but NOT
			// FILTER_PUSHDOWN, confirming the two knobs are independent and the
			// fixed option order (FILTER_PUSHDOWN before PROJECT) is irrelevant
			// when pushdown is absent.
			name: "FE.4 pxf s3 parquet projection only",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-proj-only",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:           "s3-datalake",
					Profile:          "s3:parquet",
					Resource:         "s3a://bucket/wide/",
					TargetTable:      "public.wide",
					ColumnProjection: util.Ptr(true),
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3_proj_only\" (LIKE \"public\".\"wide\")\n" +
				"LOCATION ('pxf://s3a://bucket/wide/?PROFILE=s3:parquet&SERVER=s3-datalake&PROJECT=true')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// FE.12 (Scenario 98) — SegmentRejectLimit>0 with LogErrors UNSET
			// emits the reject-limit suffix WITHOUT the "LOG ERRORS " prefix (the
			// prefix is gated on an explicit LogErrors=true). Default ROWS unit
			// when type is empty.
			name: "FE.12 reject limit rows without log errors prefix default unit",
			job: cbv1alpha1.DataLoadingJob{
				Name: "rl-noprefix",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:csv",
					Resource:    "s3a://b/malformed/",
					TargetTable: "public.t",
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit: 25,
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_rl_noprefix\" (LIKE \"public\".\"t\")\n" +
				"LOCATION ('pxf://s3a://b/malformed/?PROFILE=s3:csv&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')\n" +
				"SEGMENT REJECT LIMIT 25 ROWS;",
		},
		{
			// FE.12 (Scenario 98) — SegmentRejectLimit=0 (unset) emits NO
			// reject-limit suffix at all, even if LogErrors=true is set (LOG ERRORS
			// alone is invalid in the engine grammar, so both are gated on a
			// positive limit). The DDL ends at the FORMAT clause.
			name: "FE.12 zero reject limit omits suffix even with log errors true",
			job: cbv1alpha1.DataLoadingJob{
				Name: "rl-zero",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:csv",
					Resource:    "s3a://b/clean/",
					TargetTable: "public.t",
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit: 0,
						LogErrors:          util.Ptr(true),
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_rl_zero\" (LIKE \"public\".\"t\")\n" +
				"LOCATION ('pxf://s3a://b/clean/?PROFILE=s3:csv&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			name: "pxf jdbc with partitioning",
			job: cbv1alpha1.DataLoadingJob{
				Name: "jdbc-part",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "mysql-oltp",
					Profile:     "jdbc",
					Resource:    "sales.orders",
					TargetTable: "public.orders",
					Partitioning: &cbv1alpha1.PartitioningSpec{
						Column:   "order_date",
						Range:    "2024-01-01:2026-12-31",
						Interval: "1:month",
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_jdbc_part\" (LIKE \"public\".\"orders\")\n" +
				"LOCATION ('pxf://sales.orders?PROFILE=jdbc&SERVER=mysql-oltp" +
				"&PARTITION_BY=order_date&RANGE=2024-01-01:2026-12-31&INTERVAL=1:month')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			name: "pxf hive orc with log errors rows",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hive",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hadoop",
					Profile:     "hive:orc",
					Resource:    "warehouse.events",
					TargetTable: "public.hive_events",
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     100,
						SegmentRejectLimitType: "rows",
						LogErrors:              util.Ptr(true),
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_hive\" (LIKE \"public\".\"hive_events\")\n" +
				"LOCATION ('pxf://warehouse.events?PROFILE=hive:orc&SERVER=hadoop')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')\n" +
				"LOG ERRORS SEGMENT REJECT LIMIT 100 ROWS;",
		},
		{
			// HP.1 (Scenario 97) — hdfs read DDL golden: a read/import external
			// table on the hadoop-cluster server with pxfwritable_import.
			name: "pxf hdfs:text read import",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hdfs-text-read",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hadoop-cluster",
					Profile:     "hdfs:text",
					Resource:    "/warehouse/raw/events",
					TargetTable: "public.events",
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_hdfs_text_read\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf:///warehouse/raw/events?PROFILE=hdfs:text&SERVER=hadoop-cluster')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// HV.1 (Scenario 97) — bare hive (auto-detect, caveat C1) read DDL:
			// admitted and read as an import external table from a
			// metastore-resolved table.
			name: "pxf hive bare auto-detect read import",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hive-auto-read",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hadoop-cluster",
					Profile:     "hive",
					Resource:    "warehouse.events",
					TargetTable: "public.hive_events",
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_hive_auto_read\" (LIKE \"public\".\"hive_events\")\n" +
				"LOCATION ('pxf://warehouse.events?PROFILE=hive&SERVER=hadoop-cluster')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			// HB.1 (Scenario 97) — HBase read DDL (case-insensitive profile head).
			name: "pxf HBase read import",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hbase-read",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hbase-store",
					Profile:     "HBase",
					Resource:    "events_table",
					TargetTable: "public.hbase_events",
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_hbase_read\" (LIKE \"public\".\"hbase_events\")\n" +
				"LOCATION ('pxf://events_table?PROFILE=HBase&SERVER=hbase-store')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');",
		},
		{
			name: "pxf reject limit percent without log errors",
			job: cbv1alpha1.DataLoadingJob{
				Name: "pct",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "srv",
					Profile:     "s3:csv",
					Resource:    "s3a://b/p/",
					TargetTable: "t",
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     5,
						SegmentRejectLimitType: "percent",
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_pct\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://s3a://b/p/?PROFILE=s3:csv&SERVER=srv')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import')\n" +
				"SEGMENT REJECT LIMIT 5 PERCENT;",
		},
		{
			name: "native gpfdist from bare path csv",
			job: cbv1alpha1.DataLoadingJob{
				Name: "csv-bulk-load",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.bulk_data",
					Format:      "csv",
					FilePaths:   []string{"/data/incoming/*.csv"},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_csv_bulk_load\" (LIKE \"public\".\"bulk_data\")\n" +
				"LOCATION ('gpfdist://ddl-cluster-gpfdist:8080/data/incoming/*.csv')\n" +
				"FORMAT 'CSV';",
		},
		{
			name: "native explicit gpfdist uri text format",
			job: cbv1alpha1.DataLoadingJob{
				Name: "txt",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "t",
					Format:      "text",
					FilePaths:   []string{"gpfdist://host:9000/a.txt"},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_txt\" (LIKE \"t\")\n" +
				"LOCATION ('gpfdist://host:9000/a.txt')\n" +
				"FORMAT 'TEXT';",
		},
		{
			// Bare-path gpfdist with an UNSET Format exercises the default-CSV
			// branch. NOTE: file:// is no longer a supported CR input for gpload
			// jobs (admission-rejected by webhook W.16), so the builder-direct
			// native DDL tests use the supported native schemes (bare path served
			// via gpfdist, gpfdist://, s3://).
			name: "native bare path default format csv",
			job: cbv1alpha1.DataLoadingJob{
				Name: "filej",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.t",
					FilePaths:   []string{"/mnt/data/x.csv"},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_filej\" (LIKE \"public\".\"t\")\n" +
				"LOCATION ('gpfdist://ddl-cluster-gpfdist:8080/mnt/data/x.csv')\n" +
				"FORMAT 'CSV';",
		},
		{
			// Builder-direct-only fixture: file:// is NOT a valid CR input (W.16
			// rejects it at admission) but buildNativeLocations keeps the verbatim
			// passthrough defensively for a single-host/in-container caller. This
			// asserts the builder does NOT silently rewrite a file:// scheme.
			name: "native file uri verbatim passthrough builder-direct only",
			job: cbv1alpha1.DataLoadingJob{
				Name: "filej",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.t",
					FilePaths:   []string{"file:///mnt/data/x.csv"},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_filej\" (LIKE \"public\".\"t\")\n" +
				"LOCATION ('file:///mnt/data/x.csv')\n" +
				"FORMAT 'CSV';",
		},
		{
			name: "native s3 uri with reject limit rows",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3n",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "t",
					Format:      "csv",
					FilePaths:   []string{"s3://bucket/prefix/ config=/cfg/s3.conf"},
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     50,
						SegmentRejectLimitType: "rows",
						LogErrors:              util.Ptr(true),
					},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_s3n\" (LIKE \"t\")\n" +
				"LOCATION ('s3://bucket/prefix/ config=/cfg/s3.conf')\n" +
				"FORMAT 'CSV'\n" +
				"LOG ERRORS SEGMENT REJECT LIMIT 50 ROWS;",
		},
		{
			name: "native multiple gpfdist locations",
			job: cbv1alpha1.DataLoadingJob{
				Name: "multi",
				Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "t",
					Format:      "csv",
					FilePaths:   []string{"gpfdist://h1:8080/a.csv", "gpfdist://h2:8080/b.csv"},
				},
			},
			want: "CREATE EXTERNAL TABLE \"cbk_dataload_ext_multi\" (LIKE \"t\")\n" +
				"LOCATION ('gpfdist://h1:8080/a.csv', 'gpfdist://h2:8080/b.csv')\n" +
				"FORMAT 'CSV';",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildExternalTableDDL(cluster, tc.job)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			// Determinism: same input => byte-identical output.
			again, err := buildExternalTableDDL(cluster, tc.job)
			require.NoError(t, err)
			assert.Equal(t, got, again)
		})
	}
}

// TestBuildPXFLocation_PushdownProjectionMatrix is the focused FE.1/FE.2/FE.4/
// FE.5 (Scenario 98) presence/absence matrix for the FILTER_PUSHDOWN and PROJECT
// LOCATION options. It asserts the byte-stable option ORDER (PROFILE, SERVER,
// FILTER_PUSHDOWN, PROJECT) and that each knob is emitted IFF the corresponding
// *bool is explicitly true (nil/false => absent). buildPXFLocation is exercised
// directly so the URI synthesis is locked in independent of the DDL assembler.
func TestBuildPXFLocation_PushdownProjectionMatrix(t *testing.T) {
	tests := []struct {
		name           string
		fe             string
		profile        string
		filterPushdown *bool
		columnProject  *bool
		wantFilter     bool
		wantProject    bool
		want           string
	}{
		{
			name:           "FE.1 s3:parquet pushdown true projection nil",
			fe:             "FE.1",
			profile:        "s3:parquet",
			filterPushdown: util.Ptr(true),
			wantFilter:     true,
			wantProject:    false,
			want:           "pxf://res?PROFILE=s3:parquet&SERVER=srv&FILTER_PUSHDOWN=true",
		},
		{
			name:           "FE.2 jdbc pushdown true projection nil",
			fe:             "FE.2",
			profile:        "jdbc",
			filterPushdown: util.Ptr(true),
			wantFilter:     true,
			wantProject:    false,
			want:           "pxf://res?PROFILE=jdbc&SERVER=srv&FILTER_PUSHDOWN=true",
		},
		{
			name:           "FE.1 s3:parquet pushdown false omitted",
			fe:             "FE.1",
			profile:        "s3:parquet",
			filterPushdown: util.Ptr(false),
			wantFilter:     false,
			wantProject:    false,
			want:           "pxf://res?PROFILE=s3:parquet&SERVER=srv",
		},
		{
			name:        "FE.1 s3:parquet pushdown nil omitted",
			fe:          "FE.1",
			profile:     "s3:parquet",
			wantFilter:  false,
			wantProject: false,
			want:        "pxf://res?PROFILE=s3:parquet&SERVER=srv",
		},
		{
			name:          "FE.4 s3:parquet projection true pushdown nil",
			fe:            "FE.4",
			profile:       "s3:parquet",
			columnProject: util.Ptr(true),
			wantFilter:    false,
			wantProject:   true,
			want:          "pxf://res?PROFILE=s3:parquet&SERVER=srv&PROJECT=true",
		},
		{
			name:          "FE.5 s3:orc projection true pushdown nil",
			fe:            "FE.5",
			profile:       "s3:orc",
			columnProject: util.Ptr(true),
			wantFilter:    false,
			wantProject:   true,
			want:          "pxf://res?PROFILE=s3:orc&SERVER=srv&PROJECT=true",
		},
		{
			name:          "FE.4 s3:parquet projection false omitted",
			fe:            "FE.4",
			profile:       "s3:parquet",
			columnProject: util.Ptr(false),
			wantFilter:    false,
			wantProject:   false,
			want:          "pxf://res?PROFILE=s3:parquet&SERVER=srv",
		},
		{
			name:           "FE.1/FE.4 both true byte-stable order filter before project",
			fe:             "FE.1+FE.4",
			profile:        "s3:parquet",
			filterPushdown: util.Ptr(true),
			columnProject:  util.Ptr(true),
			wantFilter:     true,
			wantProject:    true,
			want:           "pxf://res?PROFILE=s3:parquet&SERVER=srv&FILTER_PUSHDOWN=true&PROJECT=true",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			pxf := &cbv1alpha1.PxfJobSpec{
				Server:           "srv",
				Profile:          tc.profile,
				Resource:         "res",
				TargetTable:      "t",
				FilterPushdown:   tc.filterPushdown,
				ColumnProjection: tc.columnProject,
			}
			got := buildPXFLocation(pxf)
			assert.Equal(t, tc.want, got, "FE=%s byte-exact LOCATION", tc.fe)

			if tc.wantFilter {
				assert.Contains(t, got, "FILTER_PUSHDOWN=true", "FE=%s expected pushdown", tc.fe)
			} else {
				assert.NotContains(t, got, "FILTER_PUSHDOWN", "FE=%s expected NO pushdown", tc.fe)
			}
			if tc.wantProject {
				assert.Contains(t, got, "PROJECT=true", "FE=%s expected projection", tc.fe)
			} else {
				assert.NotContains(t, got, "PROJECT=", "FE=%s expected NO projection", tc.fe)
			}
		})
	}
}

// TestErrorHandlingClause_FE12Matrix is the focused FE.12 (Scenario 98) matrix
// for the errorHandlingClause suffix: ROWS vs PERCENT unit selection, the
// "LOG ERRORS " prefix gating on an explicit LogErrors=true, and the
// SegmentRejectLimit<=0 / nil cases that suppress the suffix entirely.
func TestErrorHandlingClause_FE12Matrix(t *testing.T) {
	tests := []struct {
		name string
		eh   *cbv1alpha1.ErrorHandlingSpec
		want string
	}{
		{
			name: "FE.12 nil spec no suffix",
			eh:   nil,
			want: "",
		},
		{
			name: "FE.12 zero limit no suffix",
			eh:   &cbv1alpha1.ErrorHandlingSpec{SegmentRejectLimit: 0},
			want: "",
		},
		{
			name: "FE.12 negative limit no suffix",
			eh:   &cbv1alpha1.ErrorHandlingSpec{SegmentRejectLimit: -1},
			want: "",
		},
		{
			name: "FE.12 zero limit with log errors true still no suffix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit: 0,
				LogErrors:          util.Ptr(true),
			},
			want: "",
		},
		{
			name: "FE.12a rows default unit no log errors prefix",
			eh:   &cbv1alpha1.ErrorHandlingSpec{SegmentRejectLimit: 100},
			want: "SEGMENT REJECT LIMIT 100 ROWS",
		},
		{
			name: "FE.12a explicit rows type no log errors prefix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit:     100,
				SegmentRejectLimitType: "rows",
			},
			want: "SEGMENT REJECT LIMIT 100 ROWS",
		},
		{
			name: "FE.12a rows with log errors prefix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit:     50,
				SegmentRejectLimitType: "rows",
				LogErrors:              util.Ptr(true),
			},
			want: "LOG ERRORS SEGMENT REJECT LIMIT 50 ROWS",
		},
		{
			name: "FE.12b percent no log errors prefix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit:     5,
				SegmentRejectLimitType: "percent",
			},
			want: "SEGMENT REJECT LIMIT 5 PERCENT",
		},
		{
			name: "FE.12b percent with log errors prefix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit:     10,
				SegmentRejectLimitType: "percent",
				LogErrors:              util.Ptr(true),
			},
			want: "LOG ERRORS SEGMENT REJECT LIMIT 10 PERCENT",
		},
		{
			name: "FE.12 percent case-insensitive type match",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit:     7,
				SegmentRejectLimitType: "PERCENT",
			},
			want: "SEGMENT REJECT LIMIT 7 PERCENT",
		},
		{
			name: "FE.12 log errors false explicit omits prefix",
			eh: &cbv1alpha1.ErrorHandlingSpec{
				SegmentRejectLimit: 3,
				LogErrors:          util.Ptr(false),
			},
			want: "SEGMENT REJECT LIMIT 3 ROWS",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, errorHandlingClause(tc.eh))
		})
	}
}

// TestBuildExternalTableDDL_Writable is the byte-exact WRITABLE-DDL matrix
// (Scenario 96, FF.1-FF.3). A mode=writable PXF job emits a CREATE WRITABLE
// EXTERNAL TABLE with FORMATTER='pxfwritable_export' and NO LOG ERRORS / SEGMENT
// REJECT LIMIT suffix — even when ErrorHandling is configured (writable tables
// take no reject limit, so it must be skipped). The LOCATION is byte-correct per
// profile×scheme (s3/gs/abfss/wasbs).
func TestBuildExternalTableDDL_Writable(t *testing.T) {
	cluster := dataLoadDDLCluster(8080)

	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
		want string
	}{
		{
			name: "FF.1 s3:text writable export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-text-export",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:text",
					Resource:    "s3a://data-lake/exports/",
					TargetTable: "public.events",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_s3_text_export\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf://s3a://data-lake/exports/?PROFILE=s3:text&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			name: "FF.2 s3:parquet writable export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-parquet-export",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://data-lake/parquet/",
					TargetTable: "public.sales",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_s3_parquet_export\" (LIKE \"public\".\"sales\")\n" +
				"LOCATION ('pxf://s3a://data-lake/parquet/?PROFILE=s3:parquet&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			name: "FF.3 s3:avro writable export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-avro-export",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:avro",
					Resource:    "s3a://data-lake/avro/",
					TargetTable: "metrics",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_s3_avro_export\" (LIKE \"metrics\")\n" +
				"LOCATION ('pxf://s3a://data-lake/avro/?PROFILE=s3:avro&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			// ErrorHandling is set but MUST be skipped for the writable path: a
			// writable external table cannot carry LOG ERRORS / SEGMENT REJECT
			// LIMIT, so the suffix is omitted and the DDL ends at the FORMAT.
			name: "writable skips error-handling reject limit",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-text-eh",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:text",
					Resource:    "s3a://b/exports/",
					TargetTable: "t",
					Mode:        "writable",
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     100,
						SegmentRejectLimitType: "rows",
						LogErrors:              util.Ptr(true),
					},
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_s3_text_eh\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://s3a://b/exports/?PROFILE=s3:text&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			// Case-insensitive Mode match (EqualFold) still selects the writable
			// path and byte-identical export DDL.
			name: "uppercase WRITABLE mode emits writable DDL",
			job: cbv1alpha1.DataLoadingJob{
				Name: "s3-up",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://b/p/",
					TargetTable: "t",
					Mode:        "WRITABLE",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_s3_up\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://s3a://b/p/?PROFILE=s3:parquet&SERVER=s3-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		// CFG.* / OS.* LOCATION-correctness rows: byte-exact LOCATION per
		// profile×scheme. text is writable for every object-store scheme.
		{
			name: "gs:text writable LOCATION correct",
			job: cbv1alpha1.DataLoadingJob{
				Name: "gs-text",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "gcs-datalake",
					Profile:     "gs:text",
					Resource:    "gs://bucket/exports/",
					TargetTable: "t",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_gs_text\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://gs://bucket/exports/?PROFILE=gs:text&SERVER=gcs-datalake')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			name: "abfss:parquet writable LOCATION correct",
			job: cbv1alpha1.DataLoadingJob{
				Name: "abfss-pq",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "adls-gen2",
					Profile:     "abfss:parquet",
					Resource:    "abfss://container@acct.dfs.core.windows.net/out/",
					TargetTable: "t",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_abfss_pq\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://abfss://container@acct.dfs.core.windows.net/out/?PROFILE=abfss:parquet&SERVER=adls-gen2')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		{
			name: "wasbs:text writable LOCATION correct",
			job: cbv1alpha1.DataLoadingJob{
				Name: "wasbs-text",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "azure-blob",
					Profile:     "wasbs:text",
					Resource:    "wasbs://container@acct.blob.core.windows.net/out/",
					TargetTable: "t",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_wasbs_text\" (LIKE \"t\")\n" +
				"LOCATION ('pxf://wasbs://container@acct.blob.core.windows.net/out/?PROFILE=wasbs:text&SERVER=azure-blob')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		// FF.7 (Scenario 97) — hdfs:SequenceFile writable SUCCEEDS: a CREATE
		// WRITABLE EXTERNAL TABLE with pxfwritable_export and NO LOG ERRORS suffix.
		// SequenceFile is the only writable format unique to the hdfs scheme
		// (no object-store SequenceFile).
		{
			name: "FF.7 hdfs:SequenceFile writable export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hdfs-seq-export",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hadoop-cluster",
					Profile:     "hdfs:SequenceFile",
					Resource:    "/warehouse/exports/events",
					TargetTable: "public.events",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_hdfs_seq_export\" (LIKE \"public\".\"events\")\n" +
				"LOCATION ('pxf:///warehouse/exports/events?PROFILE=hdfs:SequenceFile&SERVER=hadoop-cluster')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
		// FF.7t/7p/7a (Scenario 97) — hdfs:text/parquet/avro writable also SUCCEED.
		{
			name: "FF.7t hdfs:text writable export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "hdfs-text-export",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "hadoop-cluster",
					Profile:     "hdfs:text",
					Resource:    "/warehouse/exports/text",
					TargetTable: "t",
					Mode:        "writable",
				},
			},
			want: "CREATE WRITABLE EXTERNAL TABLE \"cbk_dataload_ext_hdfs_text_export\" (LIKE \"t\")\n" +
				"LOCATION ('pxf:///warehouse/exports/text?PROFILE=hdfs:text&SERVER=hadoop-cluster')\n" +
				"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildExternalTableDDL(cluster, tc.job)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)

			// Writable DDL must never carry a reject-limit suffix.
			assert.NotContains(t, got, "SEGMENT REJECT LIMIT")
			assert.NotContains(t, got, "LOG ERRORS")
			assert.Contains(t, got, "CREATE WRITABLE EXTERNAL TABLE")
			assert.Contains(t, got, "pxfwritable_export")
			assert.NotContains(t, got, "pxfwritable_import")

			// Determinism: same input => byte-identical output.
			again, err := buildExternalTableDDL(cluster, tc.job)
			require.NoError(t, err)
			assert.Equal(t, got, again)
		})
	}
}

// TestBuildExternalTableDDL_WritableReadOnlyFormatError is the builder
// defense-in-depth check (Scenario 96, FF.4/FF.5): even if the webhook were
// bypassed, a writable DDL for a read-only format (json/orc) returns an error
// containing "write-unsupported" so a writable export of json/orc can never be
// produced.
func TestBuildExternalTableDDL_WritableReadOnlyFormatError(t *testing.T) {
	cluster := dataLoadDDLCluster(0)

	tests := []struct {
		name    string
		profile string
	}{
		{"FF.4 s3:json writable rejected", "s3:json"},
		{"FF.5 s3:orc writable rejected", "s3:orc"},
		{"gs:json writable rejected (parity)", "gs:json"},
		{"abfss:orc writable rejected (parity)", "abfss:orc"},
		// Scenario 97 — Hadoop read-only profiles rejected at the builder layer
		// (defense in depth, mirrors webhook W.10b WRej.1-7 / FF.6b).
		{"WRej.1 hdfs:json writable rejected", "hdfs:json"},
		{"WRej.2 hdfs:orc writable rejected", "hdfs:orc"},
		{"WRej.3 hive (bare) writable rejected", "hive"},
		// WRej.4: hive:text is write-unsupported. Hive is a read-only SCHEME
		// (Write=No regardless of format), so the builder errors even though
		// "text" is a writable format on hdfs/object stores — the scheme-aware
		// IsProfileWritable("hive:text") is FALSE.
		{"WRej.4 hive:text writable rejected (read-only scheme)", "hive:text"},
		{"WRej.5 hive:orc writable rejected", "hive:orc"},
		{"WRej.6/FF.6b hive:rc writable rejected", "hive:rc"},
		{"WRej.7 HBase writable rejected (case-insensitive)", "HBase"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			job := cbv1alpha1.DataLoadingJob{
				Name: "bad-write",
				Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "srv",
					Profile:     tc.profile,
					Resource:    "s3a://b/p/",
					TargetTable: "t",
					Mode:        "writable",
				},
			}
			_, err := buildExternalTableDDL(cluster, job)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "write-unsupported")
		})
	}
}

// TestBuildExternalTableDDL_HiveTextWritableError pins the Scenario 97 fix
// (WRej.4): because pxfpolicy.IsProfileWritable is now SCHEME-AWARE, "hive:text"
// is write-unsupported (the Hive connector is a read-only SCHEME — Write=No
// regardless of format), so the builder defense-in-depth check ERRORS rather
// than emitting a writable DDL. This guards against a writable hive:text export
// ever being produced even if the webhook were bypassed.
func TestBuildExternalTableDDL_HiveTextWritableError(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
	job := cbv1alpha1.DataLoadingJob{
		Name: "hive-text-export",
		Type: "pxf",
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "hadoop-cluster",
			Profile:     "hive:text",
			Resource:    "warehouse.events",
			TargetTable: "public.events",
			Mode:        "writable",
		},
	}
	got, err := buildExternalTableDDL(cluster, job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write-unsupported")
	assert.Empty(t, got)
}

// TestBuildExternalTableDDL_ReadPathUnchanged asserts the existing read/import
// DDL is unchanged by the Scenario 96 writable feature: an unset Mode (and an
// explicit non-writable Mode) still emits the READABLE external table with
// pxfwritable_import and the error-handling suffix. (The byte-exact read goldens
// live in TestBuildExternalTableDDL_ByteExact; this guards the Mode branch.)
func TestBuildExternalTableDDL_ReadPathUnchanged(t *testing.T) {
	cluster := dataLoadDDLCluster(0)
	base := cbv1alpha1.DataLoadingJob{
		Name: "read-job",
		Type: "pxf",
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      "s3-datalake",
			Profile:     "s3:json",
			Resource:    "s3a://b/p/",
			TargetTable: "public.t",
		},
	}
	wantRead := "CREATE EXTERNAL TABLE \"cbk_dataload_ext_read_job\" (LIKE \"public\".\"t\")\n" +
		"LOCATION ('pxf://s3a://b/p/?PROFILE=s3:json&SERVER=s3-datalake')\n" +
		"FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');"

	t.Run("mode unset is read path (json allowed for read)", func(t *testing.T) {
		got, err := buildExternalTableDDL(cluster, base)
		require.NoError(t, err)
		assert.Equal(t, wantRead, got)
	})

	t.Run("mode insert is read path", func(t *testing.T) {
		job := base
		pxf := *base.PxfJob
		pxf.Mode = "insert"
		job.PxfJob = &pxf
		got, err := buildExternalTableDDL(cluster, job)
		require.NoError(t, err)
		assert.Equal(t, wantRead, got)
	})
}

// TestBuildExternalTableDDL_Errors covers the build-time error branches.
func TestBuildExternalTableDDL_Errors(t *testing.T) {
	cluster := dataLoadDDLCluster(0)

	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
	}{
		{"unknown type", cbv1alpha1.DataLoadingJob{Name: "x", Type: "bogus"}},
		{"pxf nil pxfJob", cbv1alpha1.DataLoadingJob{Name: "x", Type: "pxf"}},
		{"pxf missing target", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "pxf",
			PxfJob: &cbv1alpha1.PxfJobSpec{Profile: "p"},
		}},
		{"pxf missing profile", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "pxf",
			PxfJob: &cbv1alpha1.PxfJobSpec{TargetTable: "t"},
		}},
		{"gpload nil gploadJob", cbv1alpha1.DataLoadingJob{Name: "x", Type: "gpload"}},
		{"gpload missing target", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "gpload",
			GploadJob: &cbv1alpha1.GploadJobSpec{FilePaths: []string{"/a.csv"}},
		}},
		{"gpload no filePaths", cbv1alpha1.DataLoadingJob{
			Name: "x", Type: "gpload",
			GploadJob: &cbv1alpha1.GploadJobSpec{TargetTable: "t"},
		}},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildExternalTableDDL(cluster, tc.job)
			assert.Error(t, err)
		})
	}
}

// TestBuildExternalTableDDL_GpfdistPortDefault asserts the gpfdist default port
// is used for a bare path when gpfdist.port is unset.
func TestBuildExternalTableDDL_GpfdistPortDefault(t *testing.T) {
	cluster := dataLoadDDLCluster(0) // no explicit port
	job := cbv1alpha1.DataLoadingJob{
		Name: "j", Type: "gpload",
		GploadJob: &cbv1alpha1.GploadJobSpec{
			TargetTable: "t", Format: "csv", FilePaths: []string{"/x.csv"},
		},
	}
	got, err := buildExternalTableDDL(cluster, job)
	require.NoError(t, err)
	assert.Contains(t, got, "gpfdist://ddl-cluster-gpfdist:8080/x.csv")
}

// TestIsWritableExportJob covers the writable-export predicate that selects the
// REVERSED load-script direction (and ANALYZE skip): true only for a PXF job
// whose pxfJob.mode == "writable" (case-insensitive). It is false for a read PXF
// job, for a gpload/native job, and when PxfJob is nil.
func TestIsWritableExportJob(t *testing.T) {
	tests := []struct {
		name string
		job  cbv1alpha1.DataLoadingJob
		want bool
	}{
		{
			name: "pxf writable mode is export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "w", Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{Mode: "writable", Profile: "s3:text", TargetTable: "t"},
			},
			want: true,
		},
		{
			name: "pxf uppercase WRITABLE mode is export (case-insensitive)",
			job: cbv1alpha1.DataLoadingJob{
				Name: "w", Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{Mode: "WRITABLE", Profile: "s3:text", TargetTable: "t"},
			},
			want: true,
		},
		{
			name: "pxf unset mode is read (not export)",
			job: cbv1alpha1.DataLoadingJob{
				Name: "r", Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{Profile: "s3:parquet", TargetTable: "t"},
			},
			want: false,
		},
		{
			name: "pxf insert mode is read (not export)",
			job: cbv1alpha1.DataLoadingJob{
				Name: "r", Type: "pxf",
				PxfJob: &cbv1alpha1.PxfJobSpec{Mode: "insert", Profile: "s3:parquet", TargetTable: "t"},
			},
			want: false,
		},
		{
			name: "gpload native job is never an export",
			job: cbv1alpha1.DataLoadingJob{
				Name: "g", Type: "gpload",
				GploadJob: &cbv1alpha1.GploadJobSpec{TargetTable: "t", FilePaths: []string{"/a.csv"}},
			},
			want: false,
		},
		{
			name: "pxf with nil PxfJob is not an export",
			job:  cbv1alpha1.DataLoadingJob{Name: "n", Type: "pxf"},
			want: false,
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isWritableExportJob(tc.job))
		})
	}
}

// TestWriteDataLoadInsert (Scenario 99) is the focused byte-exact matrix for the
// writeDataLoadInsert helper that emits the INSERT...SELECT line driving the
// load/export:
//   - WITHOUT a WHERE predicate it emits the historical single-line
//     `rows=$(psql -tA -c 'INSERT INTO <a> SELECT * FROM <b>' | awk ...)` form
//     VERBATIM (byte-stable, so existing read/writable goldens are unchanged).
//   - WITH a WHERE predicate (only reachable on the writable export path) it
//     switches to a quoted heredoc piped to `psql -tA`, so a predicate carrying
//     single quotes survives; the `awk '{print $NF}'` rowcount capture is
//     preserved either way.
func TestWriteDataLoadInsert(t *testing.T) {
	tests := []struct {
		name        string
		insertInto  string
		selectFrom  string
		whereClause string
		want        string
	}{
		{
			name:       "no filter single-line psql -c form (byte-stable)",
			insertInto: `"public"."events"`,
			selectFrom: `"cbk_dataload_ext_read"`,
			want: "rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO \"public\".\"events\" " +
				"SELECT * FROM \"cbk_dataload_ext_read\"' | awk '{print $NF}')\n",
		},
		{
			name:       "writable no filter single-line reversed direction",
			insertInto: `"cbk_dataload_ext_exp"`,
			selectFrom: `"public"."events"`,
			want: "rows=$(psql -v ON_ERROR_STOP=1 -tA -c 'INSERT INTO \"cbk_dataload_ext_exp\" " +
				"SELECT * FROM \"public\".\"events\"' | awk '{print $NF}')\n",
		},
		{
			name:        "filtered export quoted heredoc with WHERE predicate",
			insertInto:  `"cbk_dataload_ext_exp"`,
			selectFrom:  `"public"."events"`,
			whereClause: " WHERE region='us-east'",
			want: "rows=$(psql -v ON_ERROR_STOP=1 -tA <<'_CBK_INSERT_EOF_' | awk '{print $NF}'\n" +
				"INSERT INTO \"cbk_dataload_ext_exp\" SELECT * FROM \"public\".\"events\" " +
				"WHERE region='us-east'\n" +
				"_CBK_INSERT_EOF_\n)\n",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			writeDataLoadInsert(&b, tc.insertInto, tc.selectFrom, tc.whereClause)
			assert.Equal(t, tc.want, b.String())
		})
	}
}

// TestQuoteSQLIdentifier_Injection asserts identifiers with embedded quotes are
// escaped (no injection surface).
func TestQuoteSQLIdentifier_Injection(t *testing.T) {
	assert.Equal(t, `"public"."events"`, quoteSQLIdentifier("public.events"))
	assert.Equal(t, `"weird""name"`, quoteSQLIdentifier(`weird"name`))
	assert.Equal(t, `"t"`, quoteSQLIdentifier("  t  "))
}

// TestQuoteSQLLiteral_Injection asserts string literals with embedded single
// quotes are doubled.
func TestQuoteSQLLiteral_Injection(t *testing.T) {
	assert.Equal(t, `'a''b'`, quoteSQLLiteral("a'b"))
	assert.Equal(t, `'plain'`, quoteSQLLiteral("plain"))
}

// TestSanitizeSQLIdentBody covers the temp-table identifier-body sanitization.
func TestSanitizeSQLIdentBody(t *testing.T) {
	assert.Equal(t, "s3_parquet_loader", sanitizeSQLIdentBody("s3-parquet-loader"))
	assert.Equal(t, "job", sanitizeSQLIdentBody(""))
	assert.Equal(t, "a_b_c", sanitizeSQLIdentBody("A.b/C"))
	assert.Equal(t, "_______", sanitizeSQLIdentBody("!@#$%^&"))
}

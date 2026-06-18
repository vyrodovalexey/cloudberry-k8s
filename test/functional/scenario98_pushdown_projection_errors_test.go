//go:build functional

package functional

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/utils/ptr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 98: Filter Pushdown, Column Projection, Per-Row Error Handling
// (functional)
// ============================================================================
//
// NUMBERING NOTE: see cases.Scenario98Cases — this is Scenario 98, the
// RUNTIME-BEHAVIOR + HONEST-OBSERVABILITY verification scenario for the three
// PXF/load DDL knobs that ALREADY SHIP (FILTER_PUSHDOWN, PROJECT, SEGMENT REJECT
// LIMIT). It mirrors the Scenario 96/97 functional SHAPE exactly.
//
// This suite drives the BUILDER (DDL) and the mutating DEFAULTER WITHOUT a live
// cluster, asserting the shipped production contract:
//
//   - FilterPushdown (FE.1/FE.2/FE.3): FilterPushdown=true on s3:parquet (object
//     store), jdbc (mysql-oltp/postgres-source) and hive → the built read-Job
//     DDL/LOCATION carries "FILTER_PUSHDOWN=true"; FilterPushdown=false → absent.
//
//   - ColumnProjection (FE.4/FE.5): ColumnProjection=true on WIDE parquet + orc →
//     LOCATION carries "PROJECT=true"; false → absent.
//
//   - ErrorHandling (FE.12): SegmentRejectLimit=N → the READ DDL carries
//     "SEGMENT REJECT LIMIT N ROWS" (and a PERCENT variant); LogErrors=true →
//     "LOG ERRORS". The WRITABLE export path OMITS error handling (consistency).
//
//   - MutatingDefaults: a job with FilterPushdown/ColumnProjection UNSET → after
//     the mutating defaulter both default to true → the DDL carries both options.
//     An explicit false is preserved.
//
//   - CatalogHonest: resolve each cases.Scenario98Cases() row against the REAL
//     built artifact (the DDL carries the row's DDLContains option / clause).
//
// METRIC HONESTY: cloudberry_pxf_bytes_transferred_total stays PLANNED and is
// NEVER asserted here. This functional layer asserts the DDL-knob contract only;
// the live HONEST signals (row-count reduction, EXPLAIN, job_status) are at e2e.
// ============================================================================

// Scenario98Suite exercises the filter-pushdown / projection / error-handling
// DDL-knob contract at the builder + defaulter layer.
type Scenario98Suite struct {
	suite.Suite
	ctx       context.Context
	builder   *builder.DefaultBuilder
	defaulter *webhook.CloudberryClusterDefaulter
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario98(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario98Suite))
}

func (s *Scenario98Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
	// NewCloudberryClusterDefaulter is variadic: call with no args.
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario98Servers returns the PXF server set the Scenario 98 sample CR ships:
// the MinIO-backed object-store servers (s3-datalake, minio-warehouse), the two
// JDBC servers (mysql-oltp, postgres-source) and the combined hadoop-cluster
// (carrying hive.metastore.uris). Values mirror the sample CR.
func scenario98Servers() []cbv1alpha1.PxfServerSpec {
	return []cbv1alpha1.PxfServerSpec{
		{
			Name:   cases.Scenario98ServerS3Datalake,
			Type:   "s3",
			Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"},
		},
		{
			Name: cases.Scenario98ServerMinioWarehouse,
			Type: "s3",
			Config: map[string]string{
				"fs.s3a.endpoint":          "http://minio:9000",
				"fs.s3a.path.style.access": "true",
			},
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
	}
}

// scenario98Cluster builds a running cluster carrying the Scenario 98 server set
// plus the supplied jobs.
func scenario98Cluster(name string, jobs ...cbv1alpha1.DataLoadingJob) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: scenario98Servers(),
		},
		Jobs: jobs,
	}
	return cluster
}

// scenario98Resource returns a deterministic external resource for a profile so
// the synthesized LOCATION is byte-stable.
func scenario98Resource(profile string) string {
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

// scenario98ReadJob builds a read/import PXF job with the supplied knobs.
func scenario98ReadJob(
	id, profile, server string,
	filterPushdown, columnProjection *bool,
	eh *cbv1alpha1.ErrorHandlingSpec,
) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    "s98-" + strings.ToLower(strings.ReplaceAll(id, ".", "-")),
		Type:    "pxf",
		Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:           server,
			Profile:          profile,
			Resource:         scenario98Resource(profile),
			TargetTable:      "public.s98_" + strings.ToLower(strings.ReplaceAll(id, ".", "_")),
			FilterPushdown:   filterPushdown,
			ColumnProjection: columnProjection,
			ErrorHandling:    eh,
		},
	}
}

// scenario98JobScript builds the load Job for a job and returns its script
// (container args[0]), failing the test if the Job/container is missing.
func (s *Scenario98Suite) scenario98JobScript(
	cluster *cbv1alpha1.CloudberryCluster, job cbv1alpha1.DataLoadingJob,
) string {
	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNilf(s.T(), out, "BuildDataLoadJob must produce a Job for %q", job.Name)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// ----------------------------------------------------------------------------
// FE.1/FE.2/FE.3 — FILTER_PUSHDOWN present/absent
// ----------------------------------------------------------------------------

// TestFunctional_Scenario98_FilterPushdown builds read jobs with
// FilterPushdown=true on s3:parquet (object store), jdbc (mysql + postgres) and
// hive → asserts the generated read-Job DDL/LOCATION carries
// "FILTER_PUSHDOWN=true". FilterPushdown=false → the option is ABSENT.
func (s *Scenario98Suite) TestFunctional_Scenario98_FilterPushdown() {
	cluster := scenario98Cluster("s98-fpd")
	cases98 := []struct{ id, profile, server string }{
		{"FE.1", "s3:parquet", cases.Scenario98ServerS3Datalake},
		{"FE.2-mysql", "jdbc", cases.Scenario98ServerMySQLOLTP},
		{"FE.2-pg", "jdbc", cases.Scenario98ServerPostgresSource},
		{"FE.3", "hive", cases.Scenario98ServerHadoopCluster},
	}
	for _, tc := range cases98 {
		tc := tc
		s.Run(tc.id, func() {
			// FilterPushdown=true → the option is present.
			onJob := scenario98ReadJob(tc.id+"-on", tc.profile, tc.server, ptr.To(true), nil, nil)
			onScript := s.scenario98JobScript(cluster, onJob)
			assert.Containsf(s.T(), onScript, cases.Scenario98FilterPushdownOpt,
				"%s filterPushdown=true must emit FILTER_PUSHDOWN=true", tc.id)
			assert.Contains(s.T(), onScript, "pxfwritable_import")

			// FilterPushdown=false → the option is ABSENT.
			offJob := scenario98ReadJob(tc.id+"-off", tc.profile, tc.server, ptr.To(false), nil, nil)
			offScript := s.scenario98JobScript(cluster, offJob)
			assert.NotContainsf(s.T(), offScript, cases.Scenario98FilterPushdownOpt,
				"%s filterPushdown=false must NOT emit FILTER_PUSHDOWN=true", tc.id)
		})
	}
}

// ----------------------------------------------------------------------------
// FE.4/FE.5 — PROJECT present/absent
// ----------------------------------------------------------------------------

// TestFunctional_Scenario98_ColumnProjection builds read jobs with
// ColumnProjection=true on WIDE parquet + orc → asserts the LOCATION carries
// "PROJECT=true"; false → the option is ABSENT.
func (s *Scenario98Suite) TestFunctional_Scenario98_ColumnProjection() {
	cluster := scenario98Cluster("s98-proj")
	cases98 := []struct{ id, profile string }{
		{"FE.4", "s3:parquet"},
		{"FE.5", "s3:orc"},
	}
	for _, tc := range cases98 {
		tc := tc
		s.Run(tc.id, func() {
			onJob := scenario98ReadJob(
				tc.id+"-on", tc.profile, cases.Scenario98ServerS3Datalake, nil, ptr.To(true), nil)
			onScript := s.scenario98JobScript(cluster, onJob)
			assert.Containsf(s.T(), onScript, cases.Scenario98ProjectOpt,
				"%s columnProjection=true must emit PROJECT=true", tc.id)

			offJob := scenario98ReadJob(
				tc.id+"-off", tc.profile, cases.Scenario98ServerS3Datalake, nil, ptr.To(false), nil)
			offScript := s.scenario98JobScript(cluster, offJob)
			assert.NotContainsf(s.T(), offScript, cases.Scenario98ProjectOpt,
				"%s columnProjection=false must NOT emit PROJECT=true", tc.id)
		})
	}
}

// ----------------------------------------------------------------------------
// FE.12 — SEGMENT REJECT LIMIT (ROWS / PERCENT) + LOG ERRORS; writable omits
// ----------------------------------------------------------------------------

// TestFunctional_Scenario98_ErrorHandling asserts the error-handling DDL clause:
// SegmentRejectLimit=N → the READ DDL carries "SEGMENT REJECT LIMIT N ROWS" (and
// a PERCENT variant); LogErrors=true → "LOG ERRORS". It also asserts the
// WRITABLE export path OMITS the error-handling suffix (consistency).
func (s *Scenario98Suite) TestFunctional_Scenario98_ErrorHandling() {
	cluster := scenario98Cluster("s98-eh")

	// FE.12a — ROWS limit ABOVE the bad-row count, LOG ERRORS on → tolerated.
	s.Run("FE.12a-rows-tolerated", func() {
		eh := &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitTolerated,
			SegmentRejectLimitType: "rows",
			LogErrors:              ptr.To(true),
		}
		job := scenario98ReadJob("FE.12a", "s3:text", cases.Scenario98ServerS3Datalake, nil, nil, eh)
		script := s.scenario98JobScript(cluster, job)
		assert.Contains(s.T(), script, "LOG ERRORS")
		assert.Contains(s.T(), script, "SEGMENT REJECT LIMIT 10 ROWS")
		assert.Contains(s.T(), script, "pxfwritable_import")
	})

	// FE.12b — ROWS limit BELOW the bad-row count → failure threshold.
	s.Run("FE.12b-rows-fail", func() {
		eh := &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitFail,
			SegmentRejectLimitType: "rows",
			LogErrors:              ptr.To(true),
		}
		job := scenario98ReadJob("FE.12b", "s3:text", cases.Scenario98ServerS3Datalake, nil, nil, eh)
		script := s.scenario98JobScript(cluster, job)
		assert.Contains(s.T(), script, "SEGMENT REJECT LIMIT 2 ROWS")
	})

	// PERCENT variant — SegmentRejectLimitType=percent renders PERCENT.
	s.Run("percent-variant", func() {
		eh := &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     5,
			SegmentRejectLimitType: "percent",
			LogErrors:              ptr.To(true),
		}
		job := scenario98ReadJob("FE.12pct", "s3:text", cases.Scenario98ServerS3Datalake, nil, nil, eh)
		script := s.scenario98JobScript(cluster, job)
		assert.Contains(s.T(), script, "SEGMENT REJECT LIMIT 5 PERCENT")
		assert.NotContains(s.T(), script, "SEGMENT REJECT LIMIT 5 ROWS")
	})

	// LogErrors omitted → SEGMENT REJECT LIMIT present but NO "LOG ERRORS".
	s.Run("no-log-errors", func() {
		eh := &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitTolerated,
			SegmentRejectLimitType: "rows",
		}
		job := scenario98ReadJob("FE.12nolog", "s3:text", cases.Scenario98ServerS3Datalake, nil, nil, eh)
		script := s.scenario98JobScript(cluster, job)
		assert.Contains(s.T(), script, "SEGMENT REJECT LIMIT 10 ROWS")
		assert.NotContains(s.T(), script, "LOG ERRORS")
	})

	// Writable export path OMITS error handling (writable tables cannot carry
	// LOG ERRORS / SEGMENT REJECT LIMIT). s3:text is a writable format, so the
	// writable Job is built and MUST NOT carry the error-handling suffix.
	s.Run("writable-omits-error-handling", func() {
		eh := &cbv1alpha1.ErrorHandlingSpec{
			SegmentRejectLimit:     cases.Scenario98RejectLimitTolerated,
			SegmentRejectLimitType: "rows",
			LogErrors:              ptr.To(true),
		}
		job := scenario98ReadJob("FE.12w", "s3:text", cases.Scenario98ServerS3Datalake, nil, nil, eh)
		job.PxfJob.Mode = "writable"
		script := s.scenario98JobScript(cluster, job)
		assert.Contains(s.T(), script, "CREATE WRITABLE EXTERNAL TABLE")
		assert.Contains(s.T(), script, "pxfwritable_export")
		assert.NotContains(s.T(), script, "LOG ERRORS")
		assert.NotContains(s.T(), script, "SEGMENT REJECT LIMIT")
	})
}

// ----------------------------------------------------------------------------
// MutatingDefaults — FilterPushdown/ColumnProjection default to true when unset
// ----------------------------------------------------------------------------

// TestFunctional_Scenario98_MutatingDefaults builds a job with
// FilterPushdown/ColumnProjection UNSET (nil) and asserts that after the mutating
// defaulter runs, both default to true → the built DDL carries BOTH options.
// It also asserts an explicit false is preserved (absent from the DDL).
func (s *Scenario98Suite) TestFunctional_Scenario98_MutatingDefaults() {
	// UNSET → both default to true.
	s.Run("unset-defaults-to-true", func() {
		job := scenario98ReadJob("FE.def", "s3:parquet", cases.Scenario98ServerS3Datalake, nil, nil, nil)
		cluster := scenario98Cluster("s98-def", job)

		require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

		// The defaulter mutated the cluster's job in place.
		pxf := cluster.Spec.DataLoading.Jobs[0].PxfJob
		require.NotNil(s.T(), pxf.FilterPushdown)
		require.NotNil(s.T(), pxf.ColumnProjection)
		assert.True(s.T(), *pxf.FilterPushdown, "FilterPushdown must default to true")
		assert.True(s.T(), *pxf.ColumnProjection, "ColumnProjection must default to true")

		script := s.scenario98JobScript(cluster, cluster.Spec.DataLoading.Jobs[0])
		assert.Contains(s.T(), script, cases.Scenario98FilterPushdownOpt)
		assert.Contains(s.T(), script, cases.Scenario98ProjectOpt)
	})

	// Explicit false is PRESERVED → both options absent from the DDL.
	s.Run("explicit-false-preserved", func() {
		job := scenario98ReadJob(
			"FE.false", "s3:parquet", cases.Scenario98ServerS3Datalake,
			ptr.To(false), ptr.To(false), nil)
		cluster := scenario98Cluster("s98-false", job)

		require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

		pxf := cluster.Spec.DataLoading.Jobs[0].PxfJob
		require.NotNil(s.T(), pxf.FilterPushdown)
		require.NotNil(s.T(), pxf.ColumnProjection)
		assert.False(s.T(), *pxf.FilterPushdown, "explicit FilterPushdown=false preserved")
		assert.False(s.T(), *pxf.ColumnProjection, "explicit ColumnProjection=false preserved")

		script := s.scenario98JobScript(cluster, cluster.Spec.DataLoading.Jobs[0])
		assert.NotContains(s.T(), script, cases.Scenario98FilterPushdownOpt)
		assert.NotContains(s.T(), script, cases.Scenario98ProjectOpt)
	})
}

// ----------------------------------------------------------------------------
// CatalogHonest — resolve every Scenario98Cases() row against the artifact
// ----------------------------------------------------------------------------

// TestFunctional_Scenario98_CatalogHonest iterates cases.Scenario98Cases() and
// resolves EVERY row against the REAL built artifact: filter-pushdown rows assert
// FILTER_PUSHDOWN=true; column-projection rows assert PROJECT=true; error-handling
// rows assert the SEGMENT REJECT LIMIT clause. This keeps the catalog honest
// against the implementation. bytes_transferred is NEVER asserted.
func (s *Scenario98Suite) TestFunctional_Scenario98_CatalogHonest() {
	catalog := cases.Scenario98Cases()
	require.Len(s.T(), catalog, 7, "FE.1-5 + FE.12a/b")

	cluster := scenario98Cluster("s98-catalog")

	seen := map[string]bool{}
	for _, tc := range catalog {
		tc := tc
		s.Run(tc.ID, func() {
			assert.Falsef(s.T(), seen[tc.ID], "duplicate catalog ID %s", tc.ID)
			seen[tc.ID] = true

			require.NotEmptyf(s.T(), tc.DDLContains, "%s must name a DDLContains", tc.ID)

			var job cbv1alpha1.DataLoadingJob
			switch tc.Expected {
			case cases.Scenario98ExpectFilterPushdown:
				job = scenario98ReadJob(tc.ID, tc.Profile, tc.Server, ptr.To(true), nil, nil)
			case cases.Scenario98ExpectColumnProjection:
				job = scenario98ReadJob(tc.ID, tc.Profile, tc.Server, nil, ptr.To(true), nil)
			case cases.Scenario98ExpectErrorTolerated:
				eh := &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit:     cases.Scenario98RejectLimitTolerated,
					SegmentRejectLimitType: "rows",
					LogErrors:              ptr.To(true),
				}
				job = scenario98ReadJob(tc.ID, tc.Profile, tc.Server, nil, nil, eh)
			case cases.Scenario98ExpectErrorFailed:
				eh := &cbv1alpha1.ErrorHandlingSpec{
					SegmentRejectLimit:     cases.Scenario98RejectLimitFail,
					SegmentRejectLimitType: "rows",
					LogErrors:              ptr.To(true),
				}
				job = scenario98ReadJob(tc.ID, tc.Profile, tc.Server, nil, nil, eh)
			default:
				s.T().Fatalf("%s: unknown Expected token %q", tc.ID, tc.Expected)
			}

			script := s.scenario98JobScript(cluster, job)
			assert.Containsf(s.T(), script, tc.DDLContains,
				"%s built DDL must carry %q", tc.ID, tc.DDLContains)
			assert.Contains(s.T(), script, "pxfwritable_import",
				"%s read DDL must use the import formatter", tc.ID)

			if strings.Contains(tc.Description, "[CONFIG-ONLY]") {
				s.T().Logf("scenario98 %s: [CONFIG-ONLY] — DDL knob correctness only", tc.ID)
			}
			if strings.Contains(tc.Description, "[EXPLAIN-ONLY]") {
				s.T().Logf("scenario98 %s: [EXPLAIN-ONLY] — DDL + live EXPLAIN, no byte meter", tc.ID)
			}
		})
	}
	assert.Len(s.T(), seen, len(catalog), "all catalog IDs must be unique")
}

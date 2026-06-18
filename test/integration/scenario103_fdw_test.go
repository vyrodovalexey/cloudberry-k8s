//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 103: FDW-Based Loading Path against the REAL stack — integration
// ============================================================================
//
// This suite gates on MinIO being reachable (skips cleanly when MinIO is down).
// No LIVE cluster is required: it proves, builder-level, that the operator-BUILT
// artifacts for the scenario103-fdw-test sample CR are well-formed —
//   - the FDW DDL/script (delivered as the dataload Job Args[0] of the
//     loadMethod:fdw job) carries the per-scheme wrapper s3_pxf_fdw + the
//     PERSISTENT FDW objects (CREATE SERVER / USER MAPPING / FOREIGN TABLE,
//     IF NOT EXISTS, NO DROP) + the direct query + the INSERT INTO <target>
//     SELECT * FROM the foreign table (EX.5-EX.8),
//   - the external-table leg + the FDW leg both INSERT INTO their target via the
//     EQUIVALENT shape (the EQUIVALENCE proof's builder half),
// and that the s3 dataset is staged —
//   - the CSV dataset cloudberry-data/text/data.csv is present in MinIO so the
//     live FDW + external-table loads have a SHARED source.
//
// METRIC HONESTY: NO new metric. An FDW load reuses cloudberry_data_loading_*.
// The live "data loads" + EQUIVALENCE proofs (count(ext)==count(fdw)) are at
// e2e Part B. Isolation: read-only probes + pure builder calls; safe for
// parallel CI re-runs.
// ============================================================================

const (
	// scenario103DataBucket is the MinIO bucket the s3 dataset is staged in.
	scenario103DataBucket = "cloudberry-data"
	// scenario103DataKey is the CSV dataset object key within the bucket.
	scenario103DataKey = "text/data.csv"
	// scenario103Timeout bounds each probe.
	scenario103Timeout = 60 * time.Second
)

// Scenario103FDWSuite drives the builder-level FDW loading-path contract for the
// scenario103-fdw-test sample CR, gated on MinIO reachability.
type Scenario103FDWSuite struct {
	suite.Suite
	ctx     context.Context
	cancel  context.CancelFunc
	s3      *testutil.S3TestClient
	builder *builder.DefaultBuilder
}

func TestIntegration_Scenario103(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario103FDWSuite))
}

func (s *Scenario103FDWSuite) SetupSuite() {
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 5*time.Minute)
	s.s3 = testutil.NewS3TestClientFromEnv()
	s.builder = builder.NewBuilder()
}

func (s *Scenario103FDWSuite) TearDownSuite() {
	if s.cancel != nil {
		s.cancel()
	}
}

// scenario103MinIOAvailable reports whether MinIO is reachable.
func (s *Scenario103FDWSuite) scenario103MinIOAvailable(ctx context.Context) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.s3.IsAvailable(probeCtx)
}

// scenario103SampleCluster builds a cluster mirroring the scenario103-fdw-test
// sample CR: the PXF sidecar + the s3-datalake server + the three equivalence
// jobs (external-table, FDW, filtered FDW) over the SAME s3 dataset. An S3
// backup destination supplies the s3 credentials.
func scenario103SampleCluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(cases.Scenario103ClusterName,
		cases.Scenario103Namespace).Build()
	cluster.Spec.DataLoading = &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:2.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
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
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			scenario103ExtJob(), scenario103FDWJob(), scenario103FilteredJob(),
		},
	}
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:   scenario103DataBucket,
				Endpoint: "http://minio:9000",
				Region:   "us-east-1",
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
	return cluster
}

// scenario103ExtJob returns the external-table leg (loadMethod unset).
func scenario103ExtJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: cases.Scenario103ExtJobName, Type: "pxf", Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario103Server,
			Profile:     cases.Scenario103Profile,
			Resource:    cases.Scenario103Resource,
			TargetTable: cases.Scenario103ExtTargetTable,
		},
	}
}

// scenario103FDWJob returns the FDW leg (loadMethod:fdw).
func scenario103FDWJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: cases.Scenario103FDWJobName, Type: "pxf", Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:      cases.Scenario103Server,
			Profile:     cases.Scenario103Profile,
			Resource:    cases.Scenario103Resource,
			TargetTable: cases.Scenario103FDWTargetTable,
			LoadMethod:  "fdw",
		},
	}
}

// scenario103FilteredJob returns the filtered FDW leg (loadMethod:fdw + filter).
func scenario103FilteredJob() cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name: cases.Scenario103FilteredJobName, Type: "pxf", Enabled: true,
		PxfJob: &cbv1alpha1.PxfJobSpec{
			Server:       cases.Scenario103Server,
			Profile:      cases.Scenario103Profile,
			Resource:     cases.Scenario103Resource,
			TargetTable:  cases.Scenario103FilteredTargetTable,
			LoadMethod:   "fdw",
			SourceFilter: cases.Scenario103SourceFilter,
		},
	}
}

// scenario103Script builds the dataload Job for a job against the sample cluster
// and returns its single container's Args[0] (the load script).
func (s *Scenario103FDWSuite) scenario103Script(job cbv1alpha1.DataLoadingJob) string {
	out := s.builder.BuildDataLoadJob(scenario103SampleCluster(), job)
	require.NotNil(s.T(), out)
	require.Len(s.T(), out.Spec.Template.Spec.Containers, 1)
	require.Len(s.T(), out.Spec.Template.Spec.Containers[0].Args, 1)
	return out.Spec.Template.Spec.Containers[0].Args[0]
}

// TestIntegration_Scenario103_DatasetStaged asserts the s3 CSV dataset is
// listable in MinIO at cloudberry-data/text/data.csv (the SHARED source for the
// FDW + external-table loads). Gated on MinIO reachability; skips cleanly when
// MinIO is down or the dataset has not been staged.
func (s *Scenario103FDWSuite) TestIntegration_Scenario103_DatasetStaged() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario103Timeout)
	defer cancel()
	if !s.scenario103MinIOAvailable(ctx) {
		s.T().Skip("MinIO not available, skipping Scenario 103 s3-dataset staging probe")
	}

	exists, err := s.s3.BucketExists(ctx, scenario103DataBucket)
	require.NoError(s.T(), err, "HEAD %s must succeed with MinIO credentials", scenario103DataBucket)
	require.Truef(s.T(), exists, "bucket %q must be provisioned", scenario103DataBucket)

	keys, err := s.s3.ListObjects(ctx, scenario103DataBucket, "text/")
	require.NoError(s.T(), err, "list text/ in %s", scenario103DataBucket)
	if !scenario103Contains(keys, scenario103DataKey) {
		s.T().Skipf("s3 dataset %s/%s not staged — DevOps must stage the 1000-row CSV "+
			"(id,name,value) for the FDW+external-table equivalence loads [CONFIG-ONLY until staged]",
			scenario103DataBucket, scenario103DataKey)
	}
	s.T().Logf("scenario103: s3 dataset present at s3://%s/%s (listable; the SHARED FDW + "+
		"external-table load source)", scenario103DataBucket, scenario103DataKey)
}

// TestIntegration_Scenario103_FDWArtifactsWellFormed asserts the BUILT FDW
// DDL/script for the sample CR's s3-fdw-load job is well-formed (EX.5-EX.8): the
// per-scheme wrapper s3_pxf_fdw, the PERSISTENT FDW objects (CREATE SERVER /
// USER MAPPING / FOREIGN TABLE, IF NOT EXISTS, NO DROP), the direct foreign-table
// query, and the INSERT INTO <target> SELECT * FROM the foreign table. Gated on
// MinIO reachability.
func (s *Scenario103FDWSuite) TestIntegration_Scenario103_FDWArtifactsWellFormed() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario103Timeout)
	defer cancel()
	if !s.scenario103MinIOAvailable(ctx) {
		s.T().Skip("MinIO not available, skipping Scenario 103 FDW-artifact probe")
	}

	cluster := scenario103SampleCluster()
	job := scenario103FDWJob()

	out := s.builder.BuildDataLoadJob(cluster, job)
	require.NotNil(s.T(), out)
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, job.Name), out.Name)
	script := out.Spec.Template.Spec.Containers[0].Args[0]

	// EX.5: CREATE SERVER + the per-scheme wrapper (s3 -> s3_pxf_fdw) + OPTIONS
	// (config '<pxf-server>'). The SERVER carries config (the named PXF server
	// config) — NOT resource/format (the pxf_fdw validator rejects resource at the
	// pg_foreign_server level). The resource/format live on the FOREIGN TABLE.
	assert.Contains(s.T(), script,
		"CREATE SERVER IF NOT EXISTS \""+cases.Scenario103FDWServerName+"\"")
	assert.Contains(s.T(), script, "FOREIGN DATA WRAPPER "+cases.Scenario103S3Wrapper)
	assert.Contains(s.T(), script, "OPTIONS (config '"+cases.Scenario103Server+"')")
	assert.NotContains(s.T(), script,
		"FOREIGN DATA WRAPPER "+cases.Scenario103S3Wrapper+"\n  OPTIONS (resource")
	// The FOREIGN TABLE OPTIONS carry resource + format 'csv' (s3:text -> csv: the
	// object-store text dataset is comma-delimited CSV).
	assert.Contains(s.T(), script, "OPTIONS (resource '"+cases.Scenario103Resource+"', format 'csv')")
	// EX.6: USER MAPPING for gpadmin.
	assert.Contains(s.T(), script, "CREATE USER MAPPING IF NOT EXISTS FOR \"gpadmin\"")
	// EX.7: persistent FOREIGN TABLE (LIKE target). The derived foreign-table
	// name is foreign_<sanitize(job)> (the job s3-fdw-load -> foreign_s3_fdw_load).
	assert.Contains(s.T(), script,
		"CREATE FOREIGN TABLE IF NOT EXISTS \"foreign_s3_fdw_load\" "+
			"(LIKE \"public\".\"events_fdw\")")
	// EX.8: direct query + INSERT INTO <target> SELECT * FROM the foreign table.
	assert.Contains(s.T(), script, "SELECT count(*) FROM ")
	assert.Contains(s.T(), script,
		"INSERT INTO \"public\".\"events_fdw\" SELECT * FROM ")
	// Persistent: NO DROP of the FDW objects.
	assert.NotContains(s.T(), script, "DROP FOREIGN TABLE")
	assert.NotContains(s.T(), script, "DROP SERVER")
	assert.NotContains(s.T(), script, "CREATE EXTERNAL TABLE")

	s.T().Logf("scenario103: FDW DDL/script well-formed for %s (wrapper %s, persistent FDW "+
		"objects, INSERT INTO %s)", out.Name, cases.Scenario103S3Wrapper,
		cases.Scenario103FDWTargetTable)
}

// TestIntegration_Scenario103_FilteredFDWCarriesWhere asserts the BUILT FDW
// script for the s3-fdw-filtered job carries the sourceFilter WHERE predicate
// (EX.8 filtered). Gated on MinIO reachability.
func (s *Scenario103FDWSuite) TestIntegration_Scenario103_FilteredFDWCarriesWhere() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario103Timeout)
	defer cancel()
	if !s.scenario103MinIOAvailable(ctx) {
		s.T().Skip("MinIO not available, skipping Scenario 103 filtered-FDW probe")
	}

	script := s.scenario103Script(scenario103FilteredJob())
	assert.Contains(s.T(), script,
		"INSERT INTO \"public\".\"events_fdw_filtered\" SELECT * FROM ")
	assert.Contains(s.T(), script, "WHERE "+cases.Scenario103SourceFilter)
	assert.NotContains(s.T(), script, "DROP")
	s.T().Logf("scenario103: filtered FDW script carries WHERE %s", cases.Scenario103SourceFilter)
}

// TestIntegration_Scenario103_EquivalenceShape asserts the external-table leg and
// the FDW leg both INSERT INTO their target via the EQUIVALENT shape (the
// builder half of the EQUIVALENCE proof; the live count(ext)==count(fdw) is at
// e2e Part B). Gated on MinIO reachability.
func (s *Scenario103FDWSuite) TestIntegration_Scenario103_EquivalenceShape() {
	ctx, cancel := context.WithTimeout(s.ctx, scenario103Timeout)
	defer cancel()
	if !s.scenario103MinIOAvailable(ctx) {
		s.T().Skip("MinIO not available, skipping Scenario 103 equivalence-shape probe")
	}

	extScript := s.scenario103Script(scenario103ExtJob())
	fdwScript := s.scenario103Script(scenario103FDWJob())

	// Both INSERT INTO their target via SELECT * FROM <source>.
	assert.Contains(s.T(), extScript,
		"INSERT INTO \"public\".\"events_ext\" SELECT * FROM ")
	assert.Contains(s.T(), fdwScript,
		"INSERT INTO \"public\".\"events_fdw\" SELECT * FROM ")
	// The ext-table leg sources from a transient external table (DROP present);
	// the FDW leg sources from a persistent foreign table (NO DROP).
	assert.Contains(s.T(), extScript, "CREATE EXTERNAL TABLE")
	assert.Contains(s.T(), extScript, "DROP EXTERNAL TABLE IF EXISTS")
	assert.Contains(s.T(), fdwScript, "CREATE FOREIGN TABLE IF NOT EXISTS")
	assert.NotContains(s.T(), fdwScript, "DROP")

	s.T().Logf("scenario103: equivalence shape — both legs INSERT INTO <target> SELECT * " +
		"FROM <source>; ext=transient external table, fdw=persistent foreign table")
}

// scenario103Contains reports whether ss contains target.
func scenario103Contains(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}

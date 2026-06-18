//go:build functional

package functional

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 89: Webhook PXF / data-loading validation negative tests
// ============================================================================
//
// Each rule W.1..W.15 builds an otherwise-valid cluster with dataLoading
// enabled (PXF + gpload) and then mutates exactly one offending field, calls
// ValidateCreate, and asserts the create is rejected with a descriptive error
// mentioning the field path.
//
// "Not persisted": a ValidateCreate error is what makes the API server REJECT
// the object, so it is never persisted. We assert the error is returned here.
// The live/e2e kubectl-apply rejection is verified in the Scenario 89 e2e
// scenario (KUBECONFIG-gated) and at live deployment time.
// ============================================================================

// Scenario89Suite exercises the PXF / data-loading webhook validation rules.
type Scenario89Suite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario89(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario89Suite))
}

func (s *Scenario89Suite) SetupTest() {
	s.ctx = context.Background()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario89ValidDataLoading returns a fully valid dataLoading spec: PXF enabled
// with an image, one valid s3 server (fs.s3a.endpoint + credentialSecrets), one
// valid hdfs server (fs.defaultFS), one valid jdbc server (jdbc.driver +
// jdbc.url), a valid pxf job referencing the s3 server with a valid profile +
// targetTable + a complete partitioning triple + a valid segmentRejectLimitType,
// and a valid gpload job with a targetTable. Callers mutate exactly one field to
// produce a negative case.
func scenario89ValidDataLoading() *cbv1alpha1.DataLoadingSpec {
	return &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "cloudberry-pxf:7.1.0",
			Servers: []cbv1alpha1.PxfServerSpec{
				{
					Name: "s3-datalake",
					Type: "s3",
					Config: map[string]string{
						"fs.s3a.endpoint": "http://minio:9000",
					},
					CredentialSecrets: []cbv1alpha1.SecretReference{
						{Name: "s3-creds"},
					},
				},
				{
					Name: "hdfs-warehouse",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
					},
				},
				{
					Name: "mysql-oltp",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/db",
					},
				},
			},
		},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:     "s3-csv-loader",
				Type:     "pxf",
				Enabled:  true,
				Schedule: "*/30 * * * *",
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:text",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
					Partitioning: &cbv1alpha1.PartitioningSpec{
						Column:   "id",
						Range:    "1:1000000",
						Interval: "100000",
					},
					ErrorHandling: &cbv1alpha1.ErrorHandlingSpec{
						SegmentRejectLimit:     100,
						SegmentRejectLimitType: "rows",
					},
				},
			},
			{
				Name:    "csv-bulk-load",
				Type:    "gpload",
				Enabled: true,
				GploadJob: &cbv1alpha1.GploadJobSpec{
					TargetTable: "public.bulk_data",
					Format:      "csv",
					FilePaths:   []string{"/data/incoming/*.csv"},
				},
			},
		},
	}
}

// scenario89Cluster builds a valid cluster with a valid dataLoading spec, then
// applies the supplied mutator to introduce a single offending field.
func scenario89Cluster(
	name string, mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	dl := scenario89ValidDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// assertRejected runs ValidateCreate and asserts a descriptive error.
func (s *Scenario89Suite) assertRejected(
	cluster *cbv1alpha1.CloudberryCluster, substr string,
) {
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), substr)
}

// --- positive baseline ---

// TestFunctional_Scenario89_ValidDataLoading_Accepted verifies the valid
// dataLoading spec passes validation (no offending field).
func (s *Scenario89Suite) TestFunctional_Scenario89_ValidDataLoading_Accepted() {
	cluster := scenario89Cluster("s89-valid", nil)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

// --- W.1: pxf.image required ---

// W.1: pxf.enabled with empty pxf.image -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W1_MissingPxfImage() {
	cluster := scenario89Cluster("s89-w1", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Image = ""
	})
	s.assertRejected(cluster, "dataLoading.pxf.image")
}

// --- W.2: server name (empty + duplicate) ---

// W.2: empty server name -> rejected ("required").
func (s *Scenario89Suite) TestFunctional_Scenario89_W2_EmptyServerName() {
	cluster := scenario89Cluster("s89-w2-empty", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Servers[0].Name = ""
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.pxf.servers")
	assert.Contains(s.T(), err.Error(), "name")
	assert.Contains(s.T(), err.Error(), "required")
}

// W.2: duplicate server name -> rejected ("duplicate").
func (s *Scenario89Suite) TestFunctional_Scenario89_W2_DuplicateServerName() {
	cluster := scenario89Cluster("s89-w2-dup", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.pxf.servers")
	assert.Contains(s.T(), err.Error(), "name")
	assert.Contains(s.T(), err.Error(), "duplicate")
}

// --- W.3: server type enum ---

// W.3: invalid server type "ftp" -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W3_InvalidServerType() {
	cluster := scenario89Cluster("s89-w3", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Servers[0].Type = "ftp"
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.pxf.servers")
	assert.Contains(s.T(), err.Error(), "type")
}

// --- W.4: s3 server missing endpoint OR credentials ---

// W.4: s3 server missing fs.s3a.endpoint -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W4_S3MissingEndpoint() {
	cluster := scenario89Cluster("s89-w4-endpoint", func(dl *cbv1alpha1.DataLoadingSpec) {
		delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
	})
	s.assertRejected(cluster, "fs.s3a.endpoint")
}

// W.4: s3 server missing credentialSecrets -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W4_S3MissingCredentials() {
	cluster := scenario89Cluster("s89-w4-creds", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Servers[0].CredentialSecrets = nil
	})
	s.assertRejected(cluster, "credentialSecrets")
}

// --- W.5: jdbc server missing driver OR url ---

// W.5: jdbc server missing jdbc.driver -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W5_JDBCMissingDriver() {
	cluster := scenario89Cluster("s89-w5-driver", func(dl *cbv1alpha1.DataLoadingSpec) {
		delete(dl.Pxf.Servers[2].Config, "jdbc.driver")
	})
	s.assertRejected(cluster, "jdbc.driver")
}

// W.5: jdbc server missing jdbc.url -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W5_JDBCMissingURL() {
	cluster := scenario89Cluster("s89-w5-url", func(dl *cbv1alpha1.DataLoadingSpec) {
		delete(dl.Pxf.Servers[2].Config, "jdbc.url")
	})
	s.assertRejected(cluster, "jdbc.url")
}

// --- W.6: hdfs server missing fs.defaultFS ---

// W.6: hdfs server missing fs.defaultFS -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W6_HDFSMissingDefaultFS() {
	cluster := scenario89Cluster("s89-w6", func(dl *cbv1alpha1.DataLoadingSpec) {
		delete(dl.Pxf.Servers[1].Config, "fs.defaultFS")
	})
	s.assertRejected(cluster, "fs.defaultFS")
}

// --- W.7: job name (empty + duplicate) ---

// W.7: empty job name -> rejected ("required").
func (s *Scenario89Suite) TestFunctional_Scenario89_W7_EmptyJobName() {
	cluster := scenario89Cluster("s89-w7-empty", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].Name = ""
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.jobs")
	assert.Contains(s.T(), err.Error(), "name")
	assert.Contains(s.T(), err.Error(), "required")
}

// W.7: duplicate job name -> rejected ("duplicate").
func (s *Scenario89Suite) TestFunctional_Scenario89_W7_DuplicateJobName() {
	cluster := scenario89Cluster("s89-w7-dup", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[1].Name = dl.Jobs[0].Name
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.jobs")
	assert.Contains(s.T(), err.Error(), "name")
	assert.Contains(s.T(), err.Error(), "duplicate")
}

// --- W.8: job type enum ---

// W.8: invalid job type "spark" -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W8_InvalidJobType() {
	cluster := scenario89Cluster("s89-w8", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].Type = "spark"
	})
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "dataLoading.jobs")
	assert.Contains(s.T(), err.Error(), "type")
}

// --- W.9: pxfJob.server references undefined server ---

// W.9: pxfJob.server referencing an undefined server -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W9_UndefinedServer() {
	cluster := scenario89Cluster("s89-w9", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].PxfJob.Server = "does-not-exist"
	})
	s.assertRejected(cluster, "pxfJob.server")
}

// --- W.10: pxfJob.profile invalid ---

// W.10: pxfJob.profile "s3:nonsense" -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W10_InvalidProfile() {
	cluster := scenario89Cluster("s89-w10", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
	})
	s.assertRejected(cluster, "pxfJob.profile")
}

// --- W.11: pxfJob.targetTable required ---

// W.11: pxf job with no targetTable -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W11_MissingPxfTargetTable() {
	cluster := scenario89Cluster("s89-w11", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].PxfJob.TargetTable = ""
	})
	s.assertRejected(cluster, "pxfJob.targetTable")
}

// --- W.12: gploadJob.targetTable required ---

// W.12: gpload job with no targetTable -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W12_MissingGploadTargetTable() {
	cluster := scenario89Cluster("s89-w12", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[1].GploadJob.TargetTable = ""
	})
	s.assertRejected(cluster, "gploadJob.targetTable")
}

// --- W.13: schedule must be a valid cron ---

// W.13: job schedule "not a cron" -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W13_InvalidCron() {
	cluster := scenario89Cluster("s89-w13", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].Schedule = "not a cron"
	})
	s.assertRejected(cluster, "schedule")
}

// --- W.14: partitioning column without range/interval ---

// W.14: partitioning column but no range/interval -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W14_PartitioningIncomplete() {
	cluster := scenario89Cluster("s89-w14", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].PxfJob.Partitioning.Range = ""
		dl.Jobs[0].PxfJob.Partitioning.Interval = ""
	})
	s.assertRejected(cluster, "partitioning")
}

// --- W.15: segmentRejectLimitType enum ---

// W.15: segmentRejectLimitType "fraction" -> rejected.
func (s *Scenario89Suite) TestFunctional_Scenario89_W15_InvalidSegmentRejectLimitType() {
	cluster := scenario89Cluster("s89-w15", func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Jobs[0].PxfJob.ErrorHandling.SegmentRejectLimitType = "fraction"
	})
	s.assertRejected(cluster, "segmentRejectLimitType")
}

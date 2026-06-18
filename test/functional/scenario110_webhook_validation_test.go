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
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 110: Webhook Validation (All Rules) (W.1–W.15) — functional
// ============================================================================
//
// Scenario 110 is the COMPLETE, systematic W.1–W.15 admission matrix. This
// functional layer drives the SAME public admission entrypoint the real
// admission chain uses — CloudberryClusterValidator.ValidateCreate — over a
// base-valid CloudberryCluster carrying EXACTLY ONE violation, asserting the
// create is DENIED with the descriptive (field-path + reason) substring.
//
// It complements (does NOT duplicate) Scenario 89's functional suite: it is
// catalog-driven by cases.Scenario110WebhookCases() (the -F rows), it adds an
// explicit 110-CONTROL-admit (a base-valid CR PASSES — no false-positive), and
// it keeps the catalog honest against the live validator entrypoint. The LIVE
// source-aware reject matrix + the no-persist guarantee are the e2e Part B.
//
// "Not persisted": a ValidateCreate error is what makes the API server REJECT
// the object so it is never persisted; the live kubectl-apply + GET-NotFound
// proof is the Scenario 110 e2e Part B (KUBECONFIG/SCENARIO110_LIVE gated).
// ============================================================================

// Scenario110Suite exercises the data-loading webhook validation rules via the
// CloudberryClusterValidator.ValidateCreate admission entrypoint.
type Scenario110Suite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario110(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario110Suite))
}

func (s *Scenario110Suite) SetupTest() {
	s.ctx = context.Background()
	// reader=nil: skip the duplicate-name List so each case isolates the
	// data-loading validation under test (mirrors the existing harness).
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario110ValidDataLoading returns a fully valid dataLoading spec used as the
// base-valid baseline for the negative cases. Callers mutate exactly one field
// to produce a single-rule violation.
func scenario110ValidDataLoading() *cbv1alpha1.DataLoadingSpec {
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
					Name: "mysql-oltp",
					Type: "jdbc",
					Config: map[string]string{
						"jdbc.driver": "com.mysql.cj.jdbc.Driver",
						"jdbc.url":    "jdbc:mysql://mysql:3306/db",
					},
				},
				{
					Name: "hdfs-warehouse",
					Type: "hdfs",
					Config: map[string]string{
						"fs.defaultFS": "hdfs://namenode:8020",
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

// scenario110Cluster builds a base-valid cluster with a valid dataLoading spec,
// then applies the mutator to introduce a single offending field.
func scenario110Cluster(
	name string, mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	dl := scenario110ValidDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario110FunctionalMutators maps each functional (-F) catalog ID to the
// single mutation that triggers it. The OR sub-cases (-empty/-dup,
// -endpoint/-creds, -driver/-url) each have their own ID + mutation.
//
//nolint:funlen // an exhaustive per-rule mutator table.
func scenario110FunctionalMutators() map[string]func(*cbv1alpha1.DataLoadingSpec) {
	return map[string]func(*cbv1alpha1.DataLoadingSpec){
		"110-W1-F": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Image = "" },
		"110-W2-empty-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Pxf.Servers[0].Name = ""
		},
		"110-W2-dup-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
		},
		"110-W3-F": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Servers[0].Type = "ftp" },
		"110-W4-endpoint-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
		},
		"110-W4-creds-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Pxf.Servers[0].CredentialSecrets = nil
		},
		"110-W5-driver-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[1].Config, "jdbc.driver")
		},
		"110-W5-url-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[1].Config, "jdbc.url")
		},
		"110-W6-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[2].Config, "fs.defaultFS")
		},
		"110-W7-empty-F": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Name = "" },
		"110-W7-dup-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[1].Name = dl.Jobs[0].Name
		},
		"110-W8-F": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Type = "spark" },
		"110-W9-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Server = "does-not-exist"
		},
		"110-W10-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
		},
		"110-W11-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.TargetTable = ""
		},
		"110-W12-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[1].GploadJob.TargetTable = ""
		},
		"110-W13-F": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Schedule = "not a cron" },
		"110-W14-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Partitioning.Range = ""
			dl.Jobs[0].PxfJob.Partitioning.Interval = ""
		},
		"110-W15-F": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.ErrorHandling.SegmentRejectLimitType = "fraction"
		},
	}
}

// TestFunctional_Scenario110_ControlAdmits is 110-CONTROL-admit-F: the
// base-valid CR (no violation) PASSES ValidateCreate — proving the matrix does
// not reject everything (no false-positive).
func (s *Scenario110Suite) TestFunctional_Scenario110_ControlAdmits() {
	cluster := scenario110Cluster("s110-control", nil)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err, "110-CONTROL-admit-F: the base-valid CR must ADMIT")
}

// TestFunctional_Scenario110_AdmissionMatrix iterates the functional (-F) rows of
// the Scenario 110 catalog. For EACH row it builds the base-valid CR, applies the
// single mutation, drives ValidateCreate (the real admission entrypoint), and
// asserts the create is DENIED with ALL catalogued descriptive substrings. Every
// -F row MUST have a wired mutator (the catalog stays honest).
func (s *Scenario110Suite) TestFunctional_Scenario110_AdmissionMatrix() {
	mutators := scenario110FunctionalMutators()
	for _, c := range cases.Scenario110WebhookCases() {
		c := c
		if c.Layer != cases.Scenario110LayerFunctional || c.Req == "CONTROL" {
			continue
		}
		s.Run(c.ID, func() {
			mutate, ok := mutators[c.ID]
			require.Truef(s.T(), ok, "no mutator wired for functional catalog case %s", c.ID)

			cluster := scenario110Cluster("s110-"+c.ID, mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "%s: admission must DENY the single violation", c.ID)
			for _, substr := range c.ErrorSubstrings {
				assert.Containsf(s.T(), err.Error(), substr,
					"%s: rejection must be descriptive and contain %q; got %q",
					c.ID, substr, err.Error())
			}
			s.T().Logf("scenario110 %s (source=%s, field=%s): admission denied → %v",
				c.ID, c.Source, c.OffendingField, err)
		})
	}
}

// TestFunctional_Scenario110_CatalogCoversFunctionalRows asserts every functional
// (-F) catalog row (except the CONTROL row) has a wired mutator, so the matrix
// cannot silently drop a rule.
func (s *Scenario110Suite) TestFunctional_Scenario110_CatalogCoversFunctionalRows() {
	mutators := scenario110FunctionalMutators()
	for _, c := range cases.Scenario110WebhookCases() {
		if c.Layer != cases.Scenario110LayerFunctional || c.Req == "CONTROL" {
			continue
		}
		_, ok := mutators[c.ID]
		assert.Truef(s.T(), ok, "functional catalog row %s must have a wired mutator", c.ID)
		assert.NotEmptyf(s.T(), c.ErrorSubstrings, "%s must carry descriptive substrings", c.ID)
	}
}

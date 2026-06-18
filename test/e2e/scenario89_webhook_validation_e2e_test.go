//go:build e2e

package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 89: Webhook PXF / data-loading validation negative tests (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 89 negative cases at the e2e
// layer by exercising the validator directly (infra-free, deterministic). When
// a live cluster is available (KUBECONFIG set), the optional live test ALSO
// submits each invalid CR to the real API server through a controller-runtime
// client and asserts the create is rejected (apierrors) and a subsequent Get
// returns NotFound — proving the object was never persisted. The live test is
// skipped when no cluster/KUBECONFIG is available, consistent with the other
// live-gated e2e tests (e.g. Scenario 69 webhook validation).
//
// liveSkip analysis: the mutating webhook (setDataLoadingDefaults) only repairs
// pxf.port, pxf.jvmOpts, pxf.logLevel, pxf.extensions.*, per-job mode/
// filterPushdown/columnProjection, and the jobTemplate scalars. NONE of the
// offending fields for W.1..W.15 (pxf.image, server name/type/config, job
// name/type/schedule, pxfJob.server/profile/targetTable/partitioning/
// errorHandling.segmentRejectLimitType, gploadJob.targetTable) are defaulted, so
// every rule still reaches the validator on a live API server. Hence no rule is
// marked liveSkip. The liveSkip flag is retained on the case struct for parity
// with Scenario 69 and to make any future defaulting-repaired rule explicit.
// ============================================================================

// envKubeconfigS89 gates the live API-server rejection test.
const envKubeconfigS89 = "KUBECONFIG"

// Scenario89WebhookValidationE2ESuite tests the PXF / data-loading webhook
// validation negative rules end-to-end.
type Scenario89WebhookValidationE2ESuite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario89(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario89WebhookValidationE2ESuite))
}

func (s *Scenario89WebhookValidationE2ESuite) SetupTest() {
	s.ctx = context.Background()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario89E2EValidDataLoading returns a fully valid dataLoading spec used as
// the basis for negative cases.
func scenario89E2EValidDataLoading() *cbv1alpha1.DataLoadingSpec {
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

// scenario89E2ECluster builds a valid cluster with a valid dataLoading spec and
// then applies the mutator to introduce one offending field.
func scenario89E2ECluster(
	name string, mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	dl := scenario89E2EValidDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario89Case is a single negative rule under test.
type scenario89Case struct {
	id      string
	name    string
	mutate  func(*cbv1alpha1.DataLoadingSpec)
	substrs []string
	// liveSkip marks a rule that a LIVE API server no longer rejects because
	// the mutating webhook runs first and repairs the offending field before
	// validation. For Scenario 89 no rule is repaired by defaulting (see the
	// liveSkip analysis above), so every case keeps liveSkip false; the direct
	// validator tests exercise every rule regardless.
	liveSkip bool
}

// scenario89Cases returns the negative rules W.1..W.15 (with both variants for
// W.2/W.4/W.5/W.7) and their expected rejection substrings.
func scenario89Cases() []scenario89Case {
	return []scenario89Case{
		{
			id: "W.1", name: "missing pxf image",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Image = "" },
			substrs: []string{"dataLoading.pxf.image"},
		},
		{
			id: "W.2", name: "empty server name",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Servers[0].Name = "" },
			substrs: []string{"dataLoading.pxf.servers", "name", "required"},
		},
		{
			id: "W.2", name: "duplicate server name",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
			},
			substrs: []string{"dataLoading.pxf.servers", "name", "duplicate"},
		},
		{
			id: "W.3", name: "invalid server type",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Servers[0].Type = "ftp" },
			substrs: []string{"dataLoading.pxf.servers", "type"},
		},
		{
			id: "W.4", name: "s3 missing endpoint",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
			},
			substrs: []string{"fs.s3a.endpoint"},
		},
		{
			id: "W.4", name: "s3 missing credentials",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Pxf.Servers[0].CredentialSecrets = nil
			},
			substrs: []string{"credentialSecrets"},
		},
		{
			id: "W.5", name: "jdbc missing driver",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[2].Config, "jdbc.driver")
			},
			substrs: []string{"jdbc.driver"},
		},
		{
			id: "W.5", name: "jdbc missing url",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[2].Config, "jdbc.url")
			},
			substrs: []string{"jdbc.url"},
		},
		{
			id: "W.6", name: "hdfs missing defaultFS",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				delete(dl.Pxf.Servers[1].Config, "fs.defaultFS")
			},
			substrs: []string{"fs.defaultFS"},
		},
		{
			id: "W.7", name: "empty job name",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Name = "" },
			substrs: []string{"dataLoading.jobs", "name", "required"},
		},
		{
			id: "W.7", name: "duplicate job name",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].Name = dl.Jobs[0].Name
			},
			substrs: []string{"dataLoading.jobs", "name", "duplicate"},
		},
		{
			id: "W.8", name: "invalid job type",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Type = "spark" },
			substrs: []string{"dataLoading.jobs", "type"},
		},
		{
			id: "W.9", name: "pxfJob undefined server",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Server = "does-not-exist"
			},
			substrs: []string{"pxfJob.server"},
		},
		{
			id: "W.10", name: "pxfJob invalid profile",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
			},
			substrs: []string{"pxfJob.profile"},
		},
		{
			id: "W.11", name: "pxfJob missing targetTable",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.TargetTable = ""
			},
			substrs: []string{"pxfJob.targetTable"},
		},
		{
			id: "W.12", name: "gploadJob missing targetTable",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[1].GploadJob.TargetTable = ""
			},
			substrs: []string{"gploadJob.targetTable"},
		},
		{
			id: "W.13", name: "invalid cron schedule",
			mutate:  func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Schedule = "not a cron" },
			substrs: []string{"schedule"},
		},
		{
			id: "W.14", name: "partitioning without range/interval",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.Partitioning.Range = ""
				dl.Jobs[0].PxfJob.Partitioning.Interval = ""
			},
			substrs: []string{"partitioning"},
		},
		{
			id: "W.15", name: "invalid segmentRejectLimitType",
			mutate: func(dl *cbv1alpha1.DataLoadingSpec) {
				dl.Jobs[0].PxfJob.ErrorHandling.SegmentRejectLimitType = "fraction"
			},
			substrs: []string{"segmentRejectLimitType"},
		},
	}
}

// TestE2E_Scenario89_ValidatorRejectsInvalidCRs exercises the validator directly
// for all negative rules W.1..W.15 (parity with the functional suite).
func (s *Scenario89WebhookValidationE2ESuite) TestE2E_Scenario89_ValidatorRejectsInvalidCRs() {
	for _, tc := range scenario89Cases() {
		s.Run(tc.id+"_"+tc.name, func() {
			cluster := scenario89E2ECluster("e2e-s89-"+tc.id, tc.mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Error(s.T(), err)
			for _, substr := range tc.substrs {
				assert.Contains(s.T(), err.Error(), substr)
			}
		})
	}
}

// TestE2E_Scenario89_CatalogSubstringsMatch iterates the shared catalog
// (cases.Scenario89ValidationCases) and asserts each catalogued rule's offending
// mutation produces a rejection containing the catalogued substrings. This keeps
// the cross-layer catalog honest against the live validator behavior.
func (s *Scenario89WebhookValidationE2ESuite) TestE2E_Scenario89_CatalogSubstringsMatch() {
	mutators := scenario89CatalogMutators()
	for _, c := range cases.Scenario89ValidationCases() {
		c := c
		mutate, ok := mutators[c.ID]
		require.Truef(s.T(), ok, "no mutator wired for catalog case %s", c.ID)
		s.Run(c.ID+"_"+c.Name, func() {
			cluster := scenario89E2ECluster("e2e-s89-cat-"+c.ID, mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Error(s.T(), err)
			for _, substr := range c.ErrorSubstrings {
				assert.Contains(s.T(), err.Error(), substr)
			}
		})
	}
}

// scenario89CatalogMutators maps each catalog rule ID to the single mutation
// that triggers it (the primary variant for two-variant rules).
func scenario89CatalogMutators() map[string]func(*cbv1alpha1.DataLoadingSpec) {
	return map[string]func(*cbv1alpha1.DataLoadingSpec){
		"W.1": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Image = "" },
		"W.2": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Pxf.Servers[1].Name = dl.Pxf.Servers[0].Name
		},
		"W.3": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Pxf.Servers[0].Type = "ftp" },
		"W.4": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[0].Config, "fs.s3a.endpoint")
		},
		"W.5": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[2].Config, "jdbc.driver")
		},
		"W.6": func(dl *cbv1alpha1.DataLoadingSpec) {
			delete(dl.Pxf.Servers[1].Config, "fs.defaultFS")
		},
		"W.7": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[1].Name = dl.Jobs[0].Name
		},
		"W.8": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Type = "spark" },
		"W.9": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Server = "does-not-exist"
		},
		"W.10": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Profile = "s3:nonsense"
		},
		"W.11": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.TargetTable = ""
		},
		"W.12": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[1].GploadJob.TargetTable = ""
		},
		"W.13": func(dl *cbv1alpha1.DataLoadingSpec) { dl.Jobs[0].Schedule = "not a cron" },
		"W.14": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.Partitioning.Range = ""
			dl.Jobs[0].PxfJob.Partitioning.Interval = ""
		},
		"W.15": func(dl *cbv1alpha1.DataLoadingSpec) {
			dl.Jobs[0].PxfJob.ErrorHandling.SegmentRejectLimitType = "fraction"
		},
	}
}

// TestE2E_Scenario89_ValidDataLoadingAccepted verifies the valid dataLoading
// spec passes the validator.
func (s *Scenario89WebhookValidationE2ESuite) TestE2E_Scenario89_ValidDataLoadingAccepted() {
	valid := scenario89E2ECluster("e2e-s89-valid", nil)
	_, err := s.validator.ValidateCreate(s.ctx, valid)
	require.NoError(s.T(), err)
}

// TestE2E_Scenario89_LiveAPIServerRejection submits each invalid CR to a live
// API server (via KUBECONFIG) and asserts the create is rejected and the object
// is not persisted. Skipped when no cluster/KUBECONFIG is available; the
// kubectl-apply rejection is otherwise verified at deploy time.
func (s *Scenario89WebhookValidationE2ESuite) TestE2E_Scenario89_LiveAPIServerRejection() {
	kubeconfig := os.Getenv(envKubeconfigS89)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live API-server rejection test")
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		s.T().Skipf("could not build kubeconfig %q: %v", kubeconfig, err)
	}

	scheme := testutil.NewTestK8sEnv().Scheme
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		s.T().Skipf("could not build live client: %v", err)
	}

	const ns = "default"
	for _, tc := range scenario89Cases() {
		s.Run("live_"+tc.id+"_"+tc.name, func() {
			if tc.liveSkip {
				s.T().Skip("mutating webhook repairs this field before validation; " +
					"live API server accepts the CR")
			}
			cluster := scenario89E2ECluster("live-s89-"+tc.id, tc.mutate)
			cluster.Namespace = ns

			createErr := cl.Create(s.ctx, cluster)
			require.Error(s.T(), createErr, "live API server should reject invalid CR")
			assert.True(s.T(), apierrors.IsInvalid(createErr) ||
				apierrors.IsBadRequest(createErr) || apierrors.IsForbidden(createErr),
				"reject reason should be admission-related, got %v", createErr)

			// Prove it was not persisted.
			got := &cbv1alpha1.CloudberryCluster{}
			getErr := cl.Get(s.ctx, types.NamespacedName{
				Name:      cluster.Name,
				Namespace: ns,
			}, got)
			require.Error(s.T(), getErr)
			assert.True(s.T(), apierrors.IsNotFound(getErr),
				"rejected CR must not be persisted, got %v", getErr)
		})
	}
}

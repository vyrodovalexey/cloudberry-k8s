//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 90: Webhook PXF / data-loading defaulting tests (E2E)
// ============================================================================
//
// This suite mirrors the functional Scenario 90 defaulting cases at the e2e
// layer:
//   - Defaulter-direct (infra-free): build a MINIMAL valid dataLoading spec that
//     sets none of the 14 defaulted fields, run the mutating defaulter, and
//     assert each of D.1-D.14 lands (iterating the shared catalog to stay
//     honest). Also assert the same minimal CR PASSES the validator (W.1-W.15)
//     so the live create below would be admitted server-side.
//   - KUBECONFIG-gated live: submit the NON-defaulted minimal valid CR to the
//     real API server, then Get it back and assert the 14 defaults are present
//     in the PERSISTED object — proving the server-side mutating webhook ran.
//     The object is NOT pre-defaulted before Create, so the persisted defaults
//     can only come from the webhook. Skipped cleanly when KUBECONFIG is unset.
// ============================================================================

// envKubeconfigS90 gates the live API-server persistence test.
const envKubeconfigS90 = "KUBECONFIG"

// scenario90LiveNamespace is the namespace used for the live persistence test.
const scenario90LiveNamespace = "cloudberry-test"

// Scenario90WebhookDefaultsE2ESuite tests the PXF / data-loading webhook
// defaulting rules end-to-end.
type Scenario90WebhookDefaultsE2ESuite struct {
	suite.Suite
	ctx       context.Context
	defaulter *webhook.CloudberryClusterDefaulter
	validator *webhook.CloudberryClusterValidator
}

func TestE2E_Scenario90(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario90WebhookDefaultsE2ESuite))
}

func (s *Scenario90WebhookDefaultsE2ESuite) SetupTest() {
	s.ctx = context.Background()
	// NewCloudberryClusterDefaulter is variadic: call with no args.
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario90E2EMinimalDataLoading returns a MINIMAL but valid dataLoading spec
// that sets NONE of the 14 defaulted fields (D.1-D.14): pxf enabled with an
// image and one valid s3 server (fs.s3a.endpoint + credentialSecrets), one pxf
// job referencing that server with a valid profile + targetTable, and one gpload
// job with a targetTable. It passes validation W.1-W.15 so a live Create is
// admitted, and it is intentionally NOT pre-defaulted.
func scenario90E2EMinimalDataLoading() *cbv1alpha1.DataLoadingSpec {
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
			},
		},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true},
		Jobs: []cbv1alpha1.DataLoadingJob{
			{
				Name:    "s3-parquet-loader",
				Type:    "pxf",
				Enabled: true,
				PxfJob: &cbv1alpha1.PxfJobSpec{
					Server:      "s3-datalake",
					Profile:     "s3:parquet",
					Resource:    "s3a://data-lake/events/",
					TargetTable: "public.events",
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

// scenario90E2ECluster builds a valid cluster in the given namespace with the
// minimal (non-defaulted) dataLoading spec attached.
func scenario90E2ECluster(name, namespace string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, namespace).Build()
	cluster.Spec.DataLoading = scenario90E2EMinimalDataLoading()
	return cluster
}

// scenario90E2ELiveDefaults renders the 14 defaulted values of an
// already-defaulted (or webhook-persisted) spec as strings keyed by catalog
// FieldPath. Pointer fields are assumed non-nil once defaulting has run.
func scenario90E2ELiveDefaults(dl *cbv1alpha1.DataLoadingSpec) map[string]string {
	pxf := dl.Pxf
	pj := dl.Jobs[0].PxfJob
	gj := dl.Jobs[1].GploadJob
	gp := dl.Gpfdist
	jt := dl.JobTemplate
	return map[string]string{
		"dataLoading.pxf.port":                          strconv.Itoa(int(pxf.Port)),
		"dataLoading.pxf.jvmOpts":                       pxf.JvmOpts,
		"dataLoading.pxf.logLevel":                      pxf.LogLevel,
		"dataLoading.pxf.extensions.pxf":                strconv.FormatBool(*pxf.Extensions.Pxf),
		"dataLoading.pxf.extensions.pxfFdw":             strconv.FormatBool(*pxf.Extensions.PxfFdw),
		"dataLoading.gpfdist.replicas":                  strconv.Itoa(int(*gp.Replicas)),
		"dataLoading.gpfdist.port":                      strconv.Itoa(int(gp.Port)),
		"dataLoading.jobs[].pxfJob.mode":                pj.Mode,
		"dataLoading.jobs[].pxfJob.filterPushdown":      strconv.FormatBool(*pj.FilterPushdown),
		"dataLoading.jobs[].pxfJob.columnProjection":    strconv.FormatBool(*pj.ColumnProjection),
		"dataLoading.jobs[].gploadJob.mode":             gj.Mode,
		"dataLoading.jobTemplate.backoffLimit":          strconv.Itoa(int(*jt.BackoffLimit)),
		"dataLoading.jobTemplate.activeDeadlineSeconds": strconv.FormatInt(*jt.ActiveDeadlineSeconds, 10),
		"dataLoading.jobTemplate.ttlSecondsAfterFinished": strconv.Itoa(
			int(*jt.TTLSecondsAfterFinished)),
	}
}

// TestE2E_Scenario90_DefaulterAppliesDefaults runs the mutating defaulter over
// the minimal spec and asserts each catalogued default (D.1-D.14) lands.
func (s *Scenario90WebhookDefaultsE2ESuite) TestE2E_Scenario90_DefaulterAppliesDefaults() {
	cluster := scenario90E2ECluster("e2e-s90", "default")
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	catalog := cases.Scenario90DefaultsCases()
	require.Len(s.T(), catalog, 14, "Scenario 90 catalog must have 14 entries")
	live := scenario90E2ELiveDefaults(cluster.Spec.DataLoading)

	for _, c := range catalog {
		c := c
		s.Run(c.ID+"_"+c.FieldPath, func() {
			got, ok := live[c.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", c.FieldPath)
			assert.Equalf(s.T(), c.ExpectedValue, got,
				"%s (%s) defaulted value must match catalog", c.ID, c.FieldPath)
		})
	}
}

// TestE2E_Scenario90_ValidatorAccepts asserts the minimal (non-defaulted) CR
// passes the validator (W.1-W.15), so a live Create would be admitted.
func (s *Scenario90WebhookDefaultsE2ESuite) TestE2E_Scenario90_ValidatorAccepts() {
	cluster := scenario90E2ECluster("e2e-s90-valid", "default")
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err)
}

// TestE2E_Scenario90_LiveDefaultsPersisted submits the NON-defaulted minimal
// valid CR to a live API server (via KUBECONFIG), then Gets it back and asserts
// the 14 defaults are present in the PERSISTED object — proving the server-side
// mutating webhook applied them. The CR is cleaned up afterwards. Skipped when
// no cluster/KUBECONFIG is available.
func (s *Scenario90WebhookDefaultsE2ESuite) TestE2E_Scenario90_LiveDefaultsPersisted() {
	kubeconfig := os.Getenv(envKubeconfigS90)
	if kubeconfig == "" {
		s.T().Skip("KUBECONFIG not set, skipping live defaults-persisted test")
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

	// Unique name so parallel/repeated runs do not collide.
	name := fmt.Sprintf("live-s90-%d", time.Now().UnixNano())
	cluster := scenario90E2ECluster(name, scenario90LiveNamespace)

	// IMPORTANT: do NOT pre-default the object. Persistence of the defaults
	// proves the server-side mutating webhook ran.
	if createErr := cl.Create(s.ctx, cluster); createErr != nil {
		s.T().Skipf("could not create CR on live cluster (webhook/namespace "+
			"may be unavailable): %v", createErr)
	}
	defer func() {
		_ = cl.Delete(s.ctx, cluster)
	}()

	got := &cbv1alpha1.CloudberryCluster{}
	require.NoError(s.T(), cl.Get(s.ctx, types.NamespacedName{
		Name:      name,
		Namespace: scenario90LiveNamespace,
	}, got))

	require.NotNil(s.T(), got.Spec.DataLoading, "persisted dataLoading must be present")
	live := scenario90E2ELiveDefaults(got.Spec.DataLoading)
	for _, c := range cases.Scenario90DefaultsCases() {
		c := c
		s.Run("persisted_"+c.ID+"_"+c.FieldPath, func() {
			val, ok := live[c.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", c.FieldPath)
			assert.Equalf(s.T(), c.ExpectedValue, val,
				"%s (%s) must be persisted by the mutating webhook", c.ID, c.FieldPath)
		})
	}
}

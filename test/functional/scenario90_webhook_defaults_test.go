//go:build functional

package functional

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 90: Webhook PXF / data-loading defaulting tests
// ============================================================================
//
// The mutating webhook (setDataLoadingDefaults) applies 14 defaults (D.1-D.14)
// when dataLoading is enabled and the corresponding field is unset/zero/nil.
// Pointer fields (*bool/*int32/*int64) default only when nil so explicit user
// values (including explicit false) are preserved.
//
// This suite drives the defaulter directly (infra-free, deterministic) over a
// MINIMAL but valid dataLoading spec that sets NONE of the 14 defaulted fields,
// then asserts every default lands. A preservation (negative) test proves
// explicit values are not overwritten, and a disabled no-op test proves the
// gate on dataLoading.enabled. The persisted-by-webhook proof lives in the
// KUBECONFIG-gated Scenario 90 e2e test.
// ============================================================================

// Scenario90Suite exercises the PXF / data-loading webhook defaulting rules.
type Scenario90Suite struct {
	suite.Suite
	ctx       context.Context
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestFunctional_Scenario90(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario90Suite))
}

func (s *Scenario90Suite) SetupTest() {
	s.ctx = context.Background()
	// NewCloudberryClusterDefaulter is variadic: call with no args.
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// scenario90MinimalDataLoading returns a MINIMAL but valid dataLoading spec that
// sets NONE of the 14 defaulted fields (D.1-D.14): pxf enabled with an image and
// one valid s3 server (fs.s3a.endpoint + credentialSecrets), one pxf job
// referencing that server with a valid profile + targetTable, and one gpload job
// with a targetTable. This passes validation W.1-W.15 and lets the defaulter
// populate all 14 fields.
func scenario90MinimalDataLoading() *cbv1alpha1.DataLoadingSpec {
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
			// Extensions left nil so D.4/D.5 default.
		},
		// Gpfdist left non-nil but empty so D.6/D.7 default; keep it enabled so
		// the gpfdist defaulter runs over a realistic, validatable shape.
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
					// Mode/FilterPushdown/ColumnProjection left unset so
					// D.8/D.9/D.10 default.
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
					// Mode left unset so D.11 defaults.
				},
			},
		},
		// JobTemplate left nil so D.12/D.13/D.14 default (allocated by webhook).
	}
}

// scenario90Cluster builds a valid cluster, attaches the minimal dataLoading
// spec, and applies the supplied mutator (if any) before returning. The mutator
// runs BEFORE defaulting so tests can set explicit values to be preserved.
func scenario90Cluster(
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder("s90", "default").Build()
	dl := scenario90MinimalDataLoading()
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// TestFunctional_Scenario90_DefaultsApplied asserts ALL 14 defaults (D.1-D.14)
// land on a minimal, enabled spec that sets none of them. Pointer fields are
// NotNil-checked before dereference.
func (s *Scenario90Suite) TestFunctional_Scenario90_DefaultsApplied() {
	cluster := scenario90Cluster(nil)
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	dl := cluster.Spec.DataLoading
	require.NotNil(s.T(), dl)

	// --- PXF service (D.1-D.5) ---
	pxf := dl.Pxf
	require.NotNil(s.T(), pxf)
	assert.Equal(s.T(), int32(5888), pxf.Port, "D.1 pxf.port")
	assert.Equal(s.T(), "-Xmx1g -Xms256m", pxf.JvmOpts, "D.2 pxf.jvmOpts")
	assert.Equal(s.T(), "INFO", pxf.LogLevel, "D.3 pxf.logLevel")
	require.NotNil(s.T(), pxf.Extensions)
	require.NotNil(s.T(), pxf.Extensions.Pxf, "D.4 pxf.extensions.pxf must be set")
	assert.True(s.T(), *pxf.Extensions.Pxf, "D.4 pxf.extensions.pxf")
	require.NotNil(s.T(), pxf.Extensions.PxfFdw, "D.5 pxf.extensions.pxfFdw must be set")
	assert.True(s.T(), *pxf.Extensions.PxfFdw, "D.5 pxf.extensions.pxfFdw")

	// --- gpfdist (D.6-D.7) ---
	gp := dl.Gpfdist
	require.NotNil(s.T(), gp)
	require.NotNil(s.T(), gp.Replicas, "D.6 gpfdist.replicas must be set")
	assert.Equal(s.T(), int32(1), *gp.Replicas, "D.6 gpfdist.replicas")
	assert.Equal(s.T(), int32(8080), gp.Port, "D.7 gpfdist.port")

	// --- pxf job (D.8-D.10) ---
	pj := dl.Jobs[0].PxfJob
	require.NotNil(s.T(), pj)
	assert.Equal(s.T(), "insert", pj.Mode, "D.8 pxfJob.mode")
	require.NotNil(s.T(), pj.FilterPushdown, "D.9 pxfJob.filterPushdown must be set")
	assert.True(s.T(), *pj.FilterPushdown, "D.9 pxfJob.filterPushdown")
	require.NotNil(s.T(), pj.ColumnProjection, "D.10 pxfJob.columnProjection must be set")
	assert.True(s.T(), *pj.ColumnProjection, "D.10 pxfJob.columnProjection")

	// --- gpload job (D.11) ---
	gj := dl.Jobs[1].GploadJob
	require.NotNil(s.T(), gj)
	assert.Equal(s.T(), "insert", gj.Mode, "D.11 gploadJob.mode")

	// --- job template (D.12-D.14) ---
	jt := dl.JobTemplate
	require.NotNil(s.T(), jt)
	require.NotNil(s.T(), jt.BackoffLimit, "D.12 jobTemplate.backoffLimit must be set")
	assert.Equal(s.T(), int32(3), *jt.BackoffLimit, "D.12 jobTemplate.backoffLimit")
	require.NotNil(s.T(), jt.ActiveDeadlineSeconds, "D.13 jobTemplate.activeDeadlineSeconds must be set")
	assert.Equal(s.T(), int64(14400), *jt.ActiveDeadlineSeconds, "D.13 jobTemplate.activeDeadlineSeconds")
	require.NotNil(s.T(), jt.TTLSecondsAfterFinished, "D.14 jobTemplate.ttlSecondsAfterFinished must be set")
	assert.Equal(s.T(), int32(86400), *jt.TTLSecondsAfterFinished, "D.14 jobTemplate.ttlSecondsAfterFinished")
}

// TestFunctional_Scenario90_ExplicitValuesPreserved sets a representative subset
// of fields to explicit (non-default) values — including explicit false on a
// *bool — and asserts defaulting does NOT overwrite them.
func (s *Scenario90Suite) TestFunctional_Scenario90_ExplicitValuesPreserved() {
	cluster := scenario90Cluster(func(dl *cbv1alpha1.DataLoadingSpec) {
		// D.1 scalar override.
		dl.Pxf.Port = 9999
		// D.4 explicit false on *bool must survive (NOT re-enabled to true).
		dl.Pxf.Extensions = &cbv1alpha1.PxfExtensionsSpec{
			Pxf: util.Ptr(false),
		}
		// D.9 explicit false on *bool must survive.
		dl.Jobs[0].PxfJob.FilterPushdown = util.Ptr(false)
		// D.12 explicit *int32 override.
		dl.JobTemplate = &cbv1alpha1.DataLoadingJobTemplate{
			BackoffLimit: util.Ptr(int32(7)),
		}
	})
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	dl := cluster.Spec.DataLoading
	assert.Equal(s.T(), int32(9999), dl.Pxf.Port, "explicit pxf.port preserved")
	require.NotNil(s.T(), dl.Pxf.Extensions.Pxf)
	assert.False(s.T(), *dl.Pxf.Extensions.Pxf, "explicit pxf.extensions.pxf=false preserved")
	require.NotNil(s.T(), dl.Jobs[0].PxfJob.FilterPushdown)
	assert.False(s.T(), *dl.Jobs[0].PxfJob.FilterPushdown,
		"explicit pxfJob.filterPushdown=false preserved")
	require.NotNil(s.T(), dl.JobTemplate.BackoffLimit)
	assert.Equal(s.T(), int32(7), *dl.JobTemplate.BackoffLimit,
		"explicit jobTemplate.backoffLimit preserved")

	// The OTHER pointer that was left unset on the same Extensions/JobTemplate
	// struct must still default, proving partial overrides only protect the
	// explicitly-set fields.
	require.NotNil(s.T(), dl.Pxf.Extensions.PxfFdw)
	assert.True(s.T(), *dl.Pxf.Extensions.PxfFdw,
		"D.5 still defaults when only D.4 was set explicitly")
	require.NotNil(s.T(), dl.JobTemplate.TTLSecondsAfterFinished)
	assert.Equal(s.T(), int32(86400), *dl.JobTemplate.TTLSecondsAfterFinished,
		"D.14 still defaults when only D.12 was set explicitly")
}

// TestFunctional_Scenario90_DisabledNoOp asserts that with dataLoading.enabled
// false the defaulter applies NONE of the defaults (pxf.port stays 0, the
// JobTemplate stays nil).
func (s *Scenario90Suite) TestFunctional_Scenario90_DisabledNoOp() {
	cluster := scenario90Cluster(func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false
	})
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	dl := cluster.Spec.DataLoading
	require.NotNil(s.T(), dl)
	require.NotNil(s.T(), dl.Pxf)
	assert.Equal(s.T(), int32(0), dl.Pxf.Port, "pxf.port stays 0 when disabled")
	assert.Nil(s.T(), dl.Pxf.Extensions, "pxf.extensions stays nil when disabled")
	assert.Nil(s.T(), dl.JobTemplate, "jobTemplate stays nil when disabled")
}

// TestFunctional_Scenario90_CatalogHonest iterates cases.Scenario90DefaultsCases
// and asserts the catalog has exactly 14 entries whose ExpectedValue matches the
// live defaulted value on the mutated object. This keeps the cross-layer catalog
// honest against the actual defaulter behavior.
func (s *Scenario90Suite) TestFunctional_Scenario90_CatalogHonest() {
	catalog := cases.Scenario90DefaultsCases()
	require.Len(s.T(), catalog, 14, "Scenario 90 catalog must have 14 entries")

	// Default a minimal cluster once, then resolve each catalog FieldPath.
	cluster := scenario90Cluster(nil)
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	live := scenario90LiveDefaults(cluster.Spec.DataLoading)

	seen := make(map[string]bool, len(catalog))
	for _, c := range catalog {
		c := c
		s.Run(c.ID+"_"+c.FieldPath, func() {
			assert.False(s.T(), seen[c.ID], "duplicate catalog ID %s", c.ID)
			seen[c.ID] = true
			got, ok := live[c.FieldPath]
			require.Truef(s.T(), ok, "no live accessor wired for %s", c.FieldPath)
			assert.Equalf(s.T(), c.ExpectedValue, got,
				"%s (%s) catalog value must match live default", c.ID, c.FieldPath)
		})
	}
	assert.Len(s.T(), seen, 14, "all 14 catalog IDs must be unique")
}

// scenario90LiveDefaults renders the 14 defaulted values of an ALREADY-DEFAULTED
// spec as strings keyed by their catalog FieldPath. Pointer fields are assumed
// non-nil (the defaulter populated them); a nil would surface as a panic the
// suite would report.
func scenario90LiveDefaults(dl *cbv1alpha1.DataLoadingSpec) map[string]string {
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

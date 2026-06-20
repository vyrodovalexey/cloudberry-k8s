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
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/webhook"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 114: Mutating Webhook Defaults — storage-recommendation scan
// (D.1–D.6) — functional
// ============================================================================
//
// The mutating webhook (setStorageManagementDefaults) injects six defaults
// D.1–D.6 onto spec.storage.recommendationScan when the scan is ENABLED
// (enabled:true) and the corresponding field is unset/zero. Explicit
// user-supplied values are always preserved, and a disabled scan is a no-op.
//
// This functional layer drives the PUBLIC defaulter entrypoint
// CloudberryClusterDefaulter.Default(ctx, cluster) (the same path the real
// mutating admission chain uses) over a MINIMAL but valid cluster whose
// recommendation scan is enabled with all six defaulted fields omitted, then
// asserts every default lands. It is catalog-driven by cases.Scenario114Cases()
// (the -F rows): per-D, ALL-omitted, PRESERVE, DISABLED-noop, and the CONTROL
// (nil storage stays nil). A catalog-coverage honesty test keeps the -F matrix
// from silently dropping a rule. The persisted-by-webhook proof is the
// KUBECONFIG/SCENARIO114_LIVE-gated Scenario 114 e2e Part B.
// ============================================================================

// Scenario114Suite exercises the storage-recommendation webhook defaulting rules
// via the public CloudberryClusterDefaulter.Default entrypoint.
type Scenario114Suite struct {
	suite.Suite
	ctx       context.Context
	defaulter *webhook.CloudberryClusterDefaulter
}

func TestFunctional_Scenario114(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario114Suite))
}

func (s *Scenario114Suite) SetupTest() {
	s.ctx = context.Background()
	// NewCloudberryClusterDefaulter is variadic: call with no args.
	s.defaulter = webhook.NewCloudberryClusterDefaulter()
}

// scenario114EnabledScan returns a minimal storage-management spec whose
// recommendation scan is ENABLED with ALL six defaulted fields omitted, so the
// public Default() populates every D.1–D.6 default.
func scenario114EnabledScan() *cbv1alpha1.StorageManagementSpec {
	return &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: true,
		},
	}
}

// scenario114Cluster builds a base-valid cluster with the supplied storage spec
// attached (nil leaves storage unset for the CONTROL row).
func scenario114Cluster(name string, storage *cbv1alpha1.StorageManagementSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	if storage != nil {
		cluster.Spec.Storage = storage
	}
	return cluster
}

// scenario114LiveDefaults renders the six defaulted recommendation-scan values
// of an ALREADY-DEFAULTED scan as strings keyed by their catalog Field path.
func scenario114LiveDefaults(scan *cbv1alpha1.RecommendationScanSpec) map[string]string {
	return map[string]string{
		"storage.recommendationScan.schedule":            scan.Schedule,
		"storage.recommendationScan.bloatThreshold":      strconv.Itoa(int(scan.BloatThreshold)),
		"storage.recommendationScan.skewThreshold":       strconv.Itoa(int(scan.SkewThreshold)),
		"storage.recommendationScan.ageThreshold":        strconv.FormatInt(scan.AgeThreshold, 10),
		"storage.recommendationScan.indexBloatThreshold": strconv.Itoa(int(scan.IndexBloatThreshold)),
		"storage.recommendationScan.scanDuration":        scan.ScanDuration,
	}
}

// TestFunctional_Scenario114_AllOmittedDefaultsApplied is 114-ALL-omitted-F: an
// enabled scan with all six fields omitted gets ALL six defaults after the public
// Default().
func (s *Scenario114Suite) TestFunctional_Scenario114_AllOmittedDefaultsApplied() {
	cluster := scenario114Cluster("s114-all", scenario114EnabledScan())
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	require.NotNil(s.T(), cluster.Spec.Storage)
	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(s.T(), scan)
	assert.Equal(s.T(), "0 3 * * 0", scan.Schedule, "D.1 schedule")
	assert.Equal(s.T(), int32(20), scan.BloatThreshold, "D.2 bloatThreshold")
	assert.Equal(s.T(), int32(50), scan.SkewThreshold, "D.3 skewThreshold")
	assert.Equal(s.T(), int64(500000000), scan.AgeThreshold, "D.4 ageThreshold")
	assert.Equal(s.T(), int32(30), scan.IndexBloatThreshold, "D.5 indexBloatThreshold")
	assert.Equal(s.T(), "2h", scan.ScanDuration, "D.6 scanDuration")
}

// TestFunctional_Scenario114_PerFieldDefaults is 114-D1..114-D6-F: per-D
// granularity. It defaults a minimal enabled+omitted scan ONCE, then iterates the
// per-D functional catalog rows asserting each Field equals its catalogued
// ExpectedValue on the live defaulted object.
func (s *Scenario114Suite) TestFunctional_Scenario114_PerFieldDefaults() {
	cluster := scenario114Cluster("s114-perfield", scenario114EnabledScan())
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	require.NotNil(s.T(), cluster.Spec.Storage)
	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(s.T(), scan)
	live := scenario114LiveDefaults(scan)

	for _, c := range cases.Scenario114Cases() {
		c := c
		if c.Layer != cases.Scenario114LayerFunctional || c.Field == "" {
			continue
		}
		s.Run(c.ID+"_"+c.Field, func() {
			got, ok := live[c.Field]
			require.Truef(s.T(), ok, "no live accessor wired for %s", c.Field)
			assert.Equalf(s.T(), c.ExpectedValue, got,
				"%s (%s) defaulted value must match catalog", c.ID, c.Field)
			s.T().Logf("scenario114 %s (gate=%s): %s = %s injected", c.ID, c.Gate, c.Field, got)
		})
	}
}

// TestFunctional_Scenario114_ExplicitValuesPreserved is 114-PRESERVE-F: an
// enabled scan with explicit NON-default values is not overwritten by Default().
func (s *Scenario114Suite) TestFunctional_Scenario114_ExplicitValuesPreserved() {
	storage := &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 1 * * *",
			BloatThreshold:      10,
			SkewThreshold:       25,
			AgeThreshold:        100000000,
			IndexBloatThreshold: 15,
			ScanDuration:        "1h",
		},
	}
	cluster := scenario114Cluster("s114-preserve", storage)
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(s.T(), scan)
	assert.Equal(s.T(), "0 1 * * *", scan.Schedule, "D.1 explicit schedule preserved")
	assert.Equal(s.T(), int32(10), scan.BloatThreshold, "D.2 explicit bloatThreshold preserved")
	assert.Equal(s.T(), int32(25), scan.SkewThreshold, "D.3 explicit skewThreshold preserved")
	assert.Equal(s.T(), int64(100000000), scan.AgeThreshold, "D.4 explicit ageThreshold preserved")
	assert.Equal(s.T(), int32(15), scan.IndexBloatThreshold, "D.5 explicit indexBloatThreshold preserved")
	assert.Equal(s.T(), "1h", scan.ScanDuration, "D.6 explicit scanDuration preserved")
}

// TestFunctional_Scenario114_DisabledNoOp is 114-DISABLED-noop-F: a disabled scan
// with omitted fields gets NONE of the defaults applied after Default() (the
// enabled gate holds).
func (s *Scenario114Suite) TestFunctional_Scenario114_DisabledNoOp() {
	storage := &cbv1alpha1.StorageManagementSpec{
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled: false,
		},
	}
	cluster := scenario114Cluster("s114-disabled", storage)
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))

	scan := cluster.Spec.Storage.RecommendationScan
	require.NotNil(s.T(), scan)
	assert.Equal(s.T(), "", scan.Schedule, "D.1 not applied when disabled")
	assert.Equal(s.T(), int32(0), scan.BloatThreshold, "D.2 not applied when disabled")
	assert.Equal(s.T(), int32(0), scan.SkewThreshold, "D.3 not applied when disabled")
	assert.Equal(s.T(), int64(0), scan.AgeThreshold, "D.4 not applied when disabled")
	assert.Equal(s.T(), int32(0), scan.IndexBloatThreshold, "D.5 not applied when disabled")
	assert.Equal(s.T(), "", scan.ScanDuration, "D.6 not applied when disabled")
}

// TestFunctional_Scenario114_ControlNilStorage is 114-CONTROL: a cluster with nil
// storage is left with nil storage by Default() (no panic, no allocation).
func (s *Scenario114Suite) TestFunctional_Scenario114_ControlNilStorage() {
	cluster := scenario114Cluster("s114-control", nil)
	require.Nil(s.T(), cluster.Spec.Storage)

	require.NotPanics(s.T(), func() {
		require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	})
	assert.Nil(s.T(), cluster.Spec.Storage, "nil storage stays nil")
}

// TestFunctional_Scenario114_CatalogCoversFunctionalRows asserts every functional
// (-F) catalog row is honest: the per-D rows carry a Field + ExpectedValue with a
// wired live accessor, and the aggregate rows (ALL/PRESERVE/DISABLED/CONTROL)
// carry a known Req/Gate — so the matrix cannot silently drop a rule.
func (s *Scenario114Suite) TestFunctional_Scenario114_CatalogCoversFunctionalRows() {
	cluster := scenario114Cluster("s114-coverage", scenario114EnabledScan())
	require.NoError(s.T(), s.defaulter.Default(s.ctx, cluster))
	require.NotNil(s.T(), cluster.Spec.Storage)
	live := scenario114LiveDefaults(cluster.Spec.Storage.RecommendationScan)

	knownAggregates := map[string]bool{
		"ALL": true, "PRESERVE": true, "DISABLED": true, "CONTROL": true,
	}
	dSeen := map[string]bool{}
	for _, c := range cases.Scenario114Cases() {
		if c.Layer != cases.Scenario114LayerFunctional {
			continue
		}
		assert.NotEmptyf(s.T(), c.Gate, "%s must carry a Gate", c.ID)
		if c.Field != "" {
			_, ok := live[c.Field]
			assert.Truef(s.T(), ok, "per-D catalog row %s must have a wired live accessor", c.ID)
			assert.NotEmptyf(s.T(), c.ExpectedValue, "%s must carry an ExpectedValue", c.ID)
			dSeen[c.Req] = true
			continue
		}
		assert.Truef(s.T(), knownAggregates[c.Req],
			"aggregate functional row %s must be a known family; got %q", c.ID, c.Req)
	}
	for i := 1; i <= 6; i++ {
		req := "D." + strconv.Itoa(i)
		assert.Truef(s.T(), dSeen[req], "functional catalog must cover per-field rule %s", req)
	}
}

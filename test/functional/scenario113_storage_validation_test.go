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
// Scenario 113: Validation Rules (Negative Tests) — storage-recommendation
// thresholds (W.1–W.4) — functional
// ============================================================================
//
// Scenario 113 is the COMPLETE, systematic W.1–W.4 admission matrix for the
// storage-recommendation threshold rules. This functional layer drives the SAME
// public admission entrypoint the real admission chain uses —
// CloudberryClusterValidator.ValidateCreate — over a base-valid
// CloudberryCluster (recommendationScan ENABLED, in-range thresholds) carrying
// EXACTLY ONE out-of-range threshold, asserting the create is DENIED with the
// descriptive (field-path + bad-value) substring.
//
// It is catalog-driven by cases.Scenario113Cases() (the -F rows): the reject
// rows (W.1–W.4, including the bloat 150 AND -1 sub-cases), the BOUNDARY accepts
// (bloat/skew/indexBloat = 0 and 100; age = 0), and an explicit CONTROL admit (a
// base-valid enabled-scan CR PASSES — no false-positive). The LIVE reject matrix
// + the no-persist guarantee are the e2e Part B.
//
// "Not persisted": a ValidateCreate error is what makes the API server REJECT the
// object so it is never persisted; the live kubectl-apply + GET-NotFound proof is
// the Scenario 113 e2e Part B (KUBECONFIG/SCENARIO113_LIVE gated).
// ============================================================================

// Scenario113Suite exercises the storage-recommendation webhook validation rules
// via the CloudberryClusterValidator.ValidateCreate admission entrypoint.
type Scenario113Suite struct {
	suite.Suite
	ctx       context.Context
	validator *webhook.CloudberryClusterValidator
}

func TestFunctional_Scenario113(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario113Suite))
}

func (s *Scenario113Suite) SetupTest() {
	s.ctx = context.Background()
	// reader=nil: skip the duplicate-name List so each case isolates the
	// storage-management validation under test (mirrors the existing harness).
	s.validator = webhook.NewCloudberryClusterValidator(nil)
}

// scenario113ValidStorage returns a fully valid storage-management spec with the
// recommendation scan ENABLED and all four thresholds in range. Callers mutate
// exactly one threshold to produce a single-rule violation (or a boundary value).
func scenario113ValidStorage() *cbv1alpha1.StorageManagementSpec {
	return &cbv1alpha1.StorageManagementSpec{
		DiskMonitoring: true,
		RecommendationScan: &cbv1alpha1.RecommendationScanSpec{
			Enabled:             true,
			Schedule:            "0 3 * * 0",
			BloatThreshold:      20,
			SkewThreshold:       50,
			IndexBloatThreshold: 30,
			AgeThreshold:        500000000,
		},
	}
}

// scenario113Cluster builds a base-valid cluster with a valid enabled-scan
// storage spec, then applies the mutator to introduce a single offending field.
func scenario113Cluster(
	name string, mutate func(*cbv1alpha1.RecommendationScanSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "default").Build()
	storage := scenario113ValidStorage()
	if mutate != nil {
		mutate(storage.RecommendationScan)
	}
	cluster.Spec.Storage = storage
	return cluster
}

// scenario113RejectMutators maps each reject (-F) catalog ID to the single
// mutation that triggers it.
func scenario113RejectMutators() map[string]func(*cbv1alpha1.RecommendationScanSpec) {
	return map[string]func(*cbv1alpha1.RecommendationScanSpec){
		"113-W1-150-F":  func(scan *cbv1alpha1.RecommendationScanSpec) { scan.BloatThreshold = 150 },
		"113-W1-neg1-F": func(scan *cbv1alpha1.RecommendationScanSpec) { scan.BloatThreshold = -1 },
		"113-W2-F":      func(scan *cbv1alpha1.RecommendationScanSpec) { scan.SkewThreshold = 101 },
		"113-W3-F":      func(scan *cbv1alpha1.RecommendationScanSpec) { scan.IndexBloatThreshold = 200 },
		"113-W4-F":      func(scan *cbv1alpha1.RecommendationScanSpec) { scan.AgeThreshold = -5 },
	}
}

// scenario113BoundaryMutators maps each BOUNDARY (-F) catalog ID to the
// inclusive-bound mutation that MUST still ADMIT.
func scenario113BoundaryMutators() map[string]func(*cbv1alpha1.RecommendationScanSpec) {
	return map[string]func(*cbv1alpha1.RecommendationScanSpec){
		"113-BOUNDARY-bloat0-F":      func(scan *cbv1alpha1.RecommendationScanSpec) { scan.BloatThreshold = 0 },
		"113-BOUNDARY-bloat100-F":    func(scan *cbv1alpha1.RecommendationScanSpec) { scan.BloatThreshold = 100 },
		"113-BOUNDARY-skew0-F":       func(scan *cbv1alpha1.RecommendationScanSpec) { scan.SkewThreshold = 0 },
		"113-BOUNDARY-skew100-F":     func(scan *cbv1alpha1.RecommendationScanSpec) { scan.SkewThreshold = 100 },
		"113-BOUNDARY-indexBloat0-F": func(scan *cbv1alpha1.RecommendationScanSpec) { scan.IndexBloatThreshold = 0 },
		"113-BOUNDARY-indexBloat100-F": func(scan *cbv1alpha1.RecommendationScanSpec) {
			scan.IndexBloatThreshold = 100
		},
		"113-BOUNDARY-age0-F": func(scan *cbv1alpha1.RecommendationScanSpec) { scan.AgeThreshold = 0 },
	}
}

// TestFunctional_Scenario113_ControlAdmits is 113-CONTROL-admit-F: the base-valid
// enabled-scan CR (no violation) PASSES ValidateCreate — proving the matrix does
// not reject everything (no false-positive).
func (s *Scenario113Suite) TestFunctional_Scenario113_ControlAdmits() {
	cluster := scenario113Cluster("s113-control", nil)
	_, err := s.validator.ValidateCreate(s.ctx, cluster)
	require.NoError(s.T(), err, "113-CONTROL-admit-F: the base-valid CR must ADMIT")
}

// TestFunctional_Scenario113_RejectMatrix iterates the reject (-F) rows of the
// Scenario 113 catalog. For EACH row it builds the base-valid CR, applies the
// single out-of-range mutation, drives ValidateCreate (the real admission
// entrypoint), and asserts the create is DENIED with ALL catalogued descriptive
// substrings. Every reject -F row MUST have a wired mutator (the catalog stays
// honest).
func (s *Scenario113Suite) TestFunctional_Scenario113_RejectMatrix() {
	mutators := scenario113RejectMutators()
	for _, c := range cases.Scenario113Cases() {
		c := c
		if c.Layer != cases.Scenario113LayerFunctional {
			continue
		}
		if c.Req == "BOUNDARY" || c.Req == "CONTROL" {
			continue
		}
		s.Run(c.ID, func() {
			mutate, ok := mutators[c.ID]
			require.Truef(s.T(), ok, "no mutator wired for reject catalog case %s", c.ID)

			cluster := scenario113Cluster("s113-"+c.ID, mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.Errorf(s.T(), err, "%s: admission must DENY the single violation", c.ID)
			for _, substr := range c.ErrorSubstrings {
				assert.Containsf(s.T(), err.Error(), substr,
					"%s: rejection must be descriptive and contain %q; got %q",
					c.ID, substr, err.Error())
			}
			s.T().Logf("scenario113 %s (source=%s, field=%s): admission denied → %v",
				c.ID, c.Source, c.OffendingField, err)
		})
	}
}

// TestFunctional_Scenario113_BoundaryAdmits iterates the BOUNDARY (-F) rows and
// asserts each inclusive-bound value (bloat/skew/indexBloat = 0 and 100; age = 0)
// ADMITS — proving the threshold checks accept the exact bounds.
func (s *Scenario113Suite) TestFunctional_Scenario113_BoundaryAdmits() {
	mutators := scenario113BoundaryMutators()
	for _, c := range cases.Scenario113Cases() {
		c := c
		if c.Layer != cases.Scenario113LayerFunctional || c.Req != "BOUNDARY" {
			continue
		}
		s.Run(c.ID, func() {
			mutate, ok := mutators[c.ID]
			require.Truef(s.T(), ok, "no mutator wired for boundary catalog case %s", c.ID)

			cluster := scenario113Cluster("s113-"+c.ID, mutate)
			_, err := s.validator.ValidateCreate(s.ctx, cluster)
			require.NoErrorf(s.T(), err, "%s: boundary value must ADMIT", c.ID)
			s.T().Logf("scenario113 %s: boundary value admitted", c.ID)
		})
	}
}

// TestFunctional_Scenario113_CatalogCoversFunctionalRows asserts every functional
// (-F) catalog row has a wired mutator (reject or boundary) or is the CONTROL row,
// so the matrix cannot silently drop a rule.
func (s *Scenario113Suite) TestFunctional_Scenario113_CatalogCoversFunctionalRows() {
	rejects := scenario113RejectMutators()
	boundaries := scenario113BoundaryMutators()
	for _, c := range cases.Scenario113Cases() {
		if c.Layer != cases.Scenario113LayerFunctional {
			continue
		}
		switch c.Req {
		case "CONTROL":
			continue
		case "BOUNDARY":
			_, ok := boundaries[c.ID]
			assert.Truef(s.T(), ok, "boundary catalog row %s must have a wired mutator", c.ID)
		default:
			_, ok := rejects[c.ID]
			assert.Truef(s.T(), ok, "reject catalog row %s must have a wired mutator", c.ID)
			assert.NotEmptyf(s.T(), c.ErrorSubstrings, "%s must carry descriptive substrings", c.ID)
		}
	}
}

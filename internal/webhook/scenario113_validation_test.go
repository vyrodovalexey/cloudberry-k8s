package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// Scenario 113 — Validation Rules (Negative Tests) for the storage-management
// recommendation-scan rule family (W.1–W.4).
//
// This is the COMPLETE, systematic unit matrix proving that EACH of the four
// storage-recommendation threshold rules rejects an otherwise-valid
// CloudberryCluster that carries EXACTLY ONE out-of-range threshold, with a
// DESCRIPTIVE (field-path + reason + bad-value) error — AND that the symmetric
// boundary values (0 and 100, and ageThreshold=0) ADMIT.
//
// It drives the SAME public validator entrypoint the real admission chain uses
// — CloudberryClusterValidator.ValidateCreate (plus a ValidateUpdate path) — so
// the assertions exercise validateCreate → validateCluster →
// validateStorageManagement end-to-end (NOT the validator function in
// isolation, unlike the existing TestValidateStorageManagement in
// validating_test.go, which this file intentionally does not duplicate).
//
// The two CONTROL rows prove the surrounding behaviour:
//   - 113-CONTROL-admit: the all-valid baseline (Enabled=true, in-range
//     defaults) ADMITS, so every negative row isolates a single threshold.
//   - 113-CONTROL-disabled: out-of-range thresholds with Enabled=false ADMIT,
//     proving the enabled-gate (scan == nil || !scan.Enabled) short-circuits
//     before any threshold check runs.
//
// Rule IDs (stable, from validateStorageManagement doc comment / catalog):
//   - W.1: storage.recommendationScan.bloatThreshold      (0..100)
//   - W.2: storage.recommendationScan.skewThreshold       (0..100)
//   - W.3: storage.recommendationScan.indexBloatThreshold (0..100)
//   - W.4: storage.recommendationScan.ageThreshold        (>= 0)
//
// Catalog IDs covered:
//
//	113-W1-150, 113-W1-neg1, 113-W2, 113-W3, 113-W4,
//	113-BOUNDARY-bloat0, 113-BOUNDARY-bloat100, 113-BOUNDARY-skew0,
//	113-BOUNDARY-skew100, 113-BOUNDARY-indexBloat0, 113-BOUNDARY-indexBloat100,
//	113-BOUNDARY-age0, 113-CONTROL-admit, 113-CONTROL-disabled.

// valid113Cluster returns a fully-valid CloudberryCluster with the storage
// recommendation scan ENABLED and all four thresholds set to valid, in-range
// defaults. Every Scenario 113 case mutates a COPY of this baseline to set
// EXACTLY ONE threshold (or flips Enabled for the disabled control), so each
// test isolates a single rule. The 113-CONTROL-admit case proves this baseline
// is itself valid (passes ValidateCreate with no error).
func valid113Cluster() *cbv1alpha1.CloudberryCluster {
	c := newValidCluster()
	c.Spec.Storage = &cbv1alpha1.StorageManagementSpec{
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
	return c
}

// mutate113 returns a valid113Cluster baseline with the single-field mutation
// applied by fn (operating on the RecommendationScanSpec).
func mutate113(fn func(scan *cbv1alpha1.RecommendationScanSpec)) *cbv1alpha1.CloudberryCluster {
	c := valid113Cluster()
	if fn != nil {
		fn(c.Spec.Storage.RecommendationScan)
	}
	return c
}

// TestScenario113_StorageRecommendationValidationMatrix is the systematic
// W.1–W.4 negative + boundary-accept + control matrix. Every row builds the
// shared valid113Cluster baseline, applies EXACTLY ONE mutation, and asserts
// ValidateCreate's outcome: the negative rows must return a non-nil error whose
// message contains the field path, the reason, AND the offending value; the
// boundary/control rows must ADMIT (no error).
func TestScenario113_StorageRecommendationValidationMatrix(t *testing.T) {
	tests := []struct {
		// id is the Scenario 113 catalog ID.
		id string
		// cluster is the CR under test (baseline + one mutation, or the
		// untouched baseline for the admit control).
		cluster *cbv1alpha1.CloudberryCluster
		// expectErr is true for the reject rows, false for boundary/control.
		expectErr bool
		// wantSubstrings are ALL required to appear in the error message — the
		// field path, the reason, AND the bad value — proving the error is
		// descriptive.
		wantSubstrings []string
	}{
		// ---- CONTROL --------------------------------------------------------

		// 113-CONTROL-admit: the untouched valid baseline passes ValidateCreate.
		{
			id:        "113-CONTROL-admit",
			cluster:   valid113Cluster(),
			expectErr: false,
		},
		// 113-CONTROL-disabled: out-of-range thresholds but Enabled=false. The
		// enabled-gate short-circuits before any threshold check, so this ADMITS
		// — proving the thresholds are only enforced for an enabled scan.
		{
			id: "113-CONTROL-disabled",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.Enabled = false
				scan.BloatThreshold = 150
				scan.SkewThreshold = -1
				scan.IndexBloatThreshold = 200
				scan.AgeThreshold = -5
			}),
			expectErr: false,
		},

		// ---- REJECTS (W.1–W.4) ---------------------------------------------

		// 113-W1-150: bloatThreshold above the upper bound.
		{
			id: "113-W1-150",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.BloatThreshold = 150
			}),
			expectErr: true,
			wantSubstrings: []string{
				"storage.recommendationScan.bloatThreshold",
				"must be between 0 and 100",
				"150",
			},
		},
		// 113-W1-neg1: bloatThreshold below the lower bound.
		{
			id: "113-W1-neg1",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.BloatThreshold = -1
			}),
			expectErr: true,
			wantSubstrings: []string{
				"storage.recommendationScan.bloatThreshold",
				"must be between 0 and 100",
				"-1",
			},
		},
		// 113-W2: skewThreshold above the upper bound.
		{
			id: "113-W2",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.SkewThreshold = 101
			}),
			expectErr: true,
			wantSubstrings: []string{
				"storage.recommendationScan.skewThreshold",
				"must be between 0 and 100",
				"101",
			},
		},
		// 113-W3: indexBloatThreshold above the upper bound.
		{
			id: "113-W3",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.IndexBloatThreshold = 200
			}),
			expectErr: true,
			wantSubstrings: []string{
				"storage.recommendationScan.indexBloatThreshold",
				"must be between 0 and 100",
				"200",
			},
		},
		// 113-W4: ageThreshold negative.
		{
			id: "113-W4",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.AgeThreshold = -5
			}),
			expectErr: true,
			wantSubstrings: []string{
				"storage.recommendationScan.ageThreshold",
				"must be non-negative",
				"-5",
			},
		},

		// ---- BOUNDARY ACCEPTS (lower/upper inclusive bounds admit) ----------

		{
			id: "113-BOUNDARY-bloat0",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.BloatThreshold = 0
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-bloat100",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.BloatThreshold = 100
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-skew0",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.SkewThreshold = 0
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-skew100",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.SkewThreshold = 100
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-indexBloat0",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.IndexBloatThreshold = 0
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-indexBloat100",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.IndexBloatThreshold = 100
			}),
			expectErr: false,
		},
		{
			id: "113-BOUNDARY-age0",
			cluster: mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
				scan.AgeThreshold = 0
			}),
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			// reader=nil: skip the duplicate-name List so the test isolates the
			// storage-management validation under test (mirrors the existing
			// Scenario 110 harness).
			v := NewCloudberryClusterValidator(nil)
			warnings, err := v.ValidateCreate(context.Background(), tt.cluster)

			if !tt.expectErr {
				require.NoError(t, err, "%s: expected ADMIT (no error)", tt.id)
				return
			}

			require.Error(t, err, "%s: expected a rejection", tt.id)
			for _, want := range tt.wantSubstrings {
				assert.Contains(t, err.Error(), want,
					"%s: error must be descriptive and contain %q; got %q",
					tt.id, want, err.Error())
			}
			_ = warnings
		})
	}
}

// TestScenario113_ValidateUpdate_Reject proves the storage-recommendation rule
// family also guards the UPDATE admission path: ValidateUpdate routes through
// validateUpdate → validateCluster → validateStorageManagement, so an
// out-of-range threshold on the new object is rejected exactly as on create.
func TestScenario113_ValidateUpdate_Reject(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)

	oldCluster := valid113Cluster()
	newCluster := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		// 113-W1 on the update path: bloatThreshold out of range.
		scan.BloatThreshold = 150
	})

	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	require.Error(t, err, "ValidateUpdate must reject an out-of-range bloatThreshold")
	assert.Contains(t, err.Error(), "storage.recommendationScan.bloatThreshold")
	assert.Contains(t, err.Error(), "must be between 0 and 100")
	assert.Contains(t, err.Error(), "150")
}

// TestScenario113_ValidateUpdate_Admit proves the valid baseline is admitted on
// the update path too, so the reject above isolates the single bad threshold.
func TestScenario113_ValidateUpdate_Admit(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)

	_, err := v.ValidateUpdate(context.Background(), valid113Cluster(), valid113Cluster())
	require.NoError(t, err, "valid baseline must ADMIT on update")
}

// TestScenario113_DeniedAdmissionMetric proves a Scenario 113 rejection records
// the denied-admission metric cloudberry_webhook_admission_total with
// labels webhook="validating", operation="create", result="denied" (via
// recordAdmission), mirroring the pattern in validating_metrics_test.go.
func TestScenario113_DeniedAdmissionMetric(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	// 113-W4 reject: negative ageThreshold on an enabled scan.
	c := mutate113(func(scan *cbv1alpha1.RecommendationScanSpec) {
		scan.AgeThreshold = -5
	})

	_, err := v.ValidateCreate(context.Background(), c)
	require.Error(t, err, "out-of-range ageThreshold must be denied")

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookValidating, rec.lastWebhook)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionDenied, rec.lastResult)
}

// TestScenario113_AllowedAdmissionMetric is the metric counterpart of the admit
// control: a valid enabled scan records result="allowed", confirming the denied
// case above is attributable to the threshold rejection, not a baseline defect.
func TestScenario113_AllowedAdmissionMetric(t *testing.T) {
	rec := newCapturingRecorder()
	v := NewCloudberryClusterValidator(nil, rec)

	_, err := v.ValidateCreate(context.Background(), valid113Cluster())
	require.NoError(t, err)

	assert.Equal(t, 1, rec.admissions)
	assert.Equal(t, webhookValidating, rec.lastWebhook)
	assert.Equal(t, admissionOpCreate, rec.lastOperation)
	assert.Equal(t, admissionAllowed, rec.lastResult)
}

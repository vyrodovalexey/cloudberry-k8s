package webhook

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// Scenario 118 — Scan Scheduling and Duration Limit (webhook layer, W.5).
//
// validateStorageManagement gains rule W.5: when the recommendation scan is
// ENABLED and scanDuration is non-empty, it MUST parse via time.ParseDuration,
// otherwise the create/update admission is rejected with a descriptive,
// field-specific error ("scanDuration ... must be a valid Go duration"). The
// rule lives inside the existing enabled-gate, so a disabled scan's bad
// duration (and an empty duration) ADMIT.
//
// These tests drive the public CloudberryClusterValidator.ValidateCreate
// entrypoint (same chain the real admission webhook uses), mirroring the
// Scenario 113 harness.
//
// Catalog IDs covered: 118-W5-reject, 118-W5-accept, 118-W5-disabled,
// 118-W5-empty.

// scan118Cluster returns a valid baseline cluster (reusing the Scenario 113
// valid baseline) with the recommendation-scan enable flag and scanDuration set
// as specified.
func scan118Cluster(enabled bool, scanDuration string) *cbv1alpha1.CloudberryCluster {
	c := valid113Cluster()
	c.Spec.Storage.RecommendationScan.Enabled = enabled
	c.Spec.Storage.RecommendationScan.ScanDuration = scanDuration
	return c
}

// TestScenario118_W5_ScanDurationValidationMatrix is the systematic W.5
// reject/accept/gate matrix. Each row builds the shared valid baseline with a
// single (enabled, scanDuration) combination and asserts ValidateCreate's
// outcome: reject rows return a descriptive error containing the field path AND
// the "valid Go duration" reason; accept rows ADMIT.
func TestScenario118_W5_ScanDurationValidationMatrix(t *testing.T) {
	tests := []struct {
		id           string
		enabled      bool
		scanDuration string
		expectErr    bool
		// wantSubstrings must all appear in a reject error.
		wantSubstrings []string
	}{
		// ---- 118-W5-reject: enabled + unparseable -> DENIED ----------------
		{
			id:           "118-W5-reject-banana",
			enabled:      true,
			scanDuration: "not-a-duration",
			expectErr:    true,
			wantSubstrings: []string{
				"storage.recommendationScan.scanDuration",
				"not-a-duration",
				"valid Go duration",
			},
		},
		{
			id:           "118-W5-reject-bareNumber",
			enabled:      true,
			scanDuration: "30",
			expectErr:    true,
			wantSubstrings: []string{
				"scanDuration",
				"valid Go duration",
			},
		},

		// ---- 118-W5-accept: enabled + valid duration -> ADMIT --------------
		{id: "118-W5-accept-30s", enabled: true, scanDuration: "30s", expectErr: false},
		{id: "118-W5-accept-2h", enabled: true, scanDuration: "2h", expectErr: false},
		{id: "118-W5-accept-10ms", enabled: true, scanDuration: "10ms", expectErr: false},

		// ---- 118-W5-empty: enabled + empty scanDuration -> ADMIT (gated) ---
		{id: "118-W5-empty", enabled: true, scanDuration: "", expectErr: false},

		// ---- 118-W5-disabled: disabled + bad duration -> ADMIT (gated) -----
		{id: "118-W5-disabled", enabled: false, scanDuration: "not-a-duration", expectErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			// reader=nil: skip the duplicate-name List so the test isolates the
			// storage-management validation under test (mirrors Scenario 113).
			v := NewCloudberryClusterValidator(nil)
			_, err := v.ValidateCreate(context.Background(), scan118Cluster(tt.enabled, tt.scanDuration))

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
		})
	}
}

// TestScenario118_W5_ValidateUpdate_Reject proves W.5 also guards the UPDATE
// admission path: an invalid scanDuration on the new object is rejected exactly
// as on create.
func TestScenario118_W5_ValidateUpdate_Reject(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)

	oldCluster := scan118Cluster(true, "2h")
	newCluster := scan118Cluster(true, "banana")

	_, err := v.ValidateUpdate(context.Background(), oldCluster, newCluster)
	require.Error(t, err, "ValidateUpdate must reject an unparseable scanDuration")
	assert.Contains(t, err.Error(), "storage.recommendationScan.scanDuration")
	assert.Contains(t, err.Error(), "valid Go duration")
	assert.Contains(t, err.Error(), "banana")
}

// TestScenario118_W5_ValidateUpdate_Admit proves a valid scanDuration is
// admitted on the update path too, isolating the reject above.
func TestScenario118_W5_ValidateUpdate_Admit(t *testing.T) {
	v := NewCloudberryClusterValidator(nil)

	_, err := v.ValidateUpdate(context.Background(),
		scan118Cluster(true, "2h"), scan118Cluster(true, "45m"))
	require.NoError(t, err, "a valid scanDuration must ADMIT on update")
}

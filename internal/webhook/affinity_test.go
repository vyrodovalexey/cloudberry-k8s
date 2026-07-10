package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// TestValidateAffinity covers the enum checks, the required+segment Warning, and
// the nil no-op.
func TestValidateAffinity(t *testing.T) {
	tests := []struct {
		name        string
		affinity    *cbv1alpha1.ClusterAffinitySpec
		expectErr   bool
		errContains string
		wantWarning bool
	}{
		{
			name:      "nil affinity is a no-op (allowed, no warning)",
			affinity:  nil,
			expectErr: false,
		},
		{
			name:      "empty affinity (mode/type unset) is valid",
			affinity:  &cbv1alpha1.ClusterAffinitySpec{},
			expectErr: false,
		},
		{
			name: "valid segment-mirror preferred",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
				Type: cbv1alpha1.AntiAffinityPreferred,
			},
			expectErr: false,
		},
		{
			name: "valid full preferred",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
				Type: cbv1alpha1.AntiAffinityPreferred,
			},
			expectErr: false,
		},
		{
			name: "invalid mode rejected",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityMode("bogus"),
			},
			expectErr:   true,
			errContains: "affinity.mode must be segment-mirror or full",
		},
		{
			name: "invalid type rejected",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
				Type: cbv1alpha1.AntiAffinityType("hard"),
			},
			expectErr:   true,
			errContains: "affinity.type must be preferred or required",
		},
		{
			name: "required + segment-mirror => warning (allowed)",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
				Type: cbv1alpha1.AntiAffinityRequired,
			},
			expectErr:   false,
			wantWarning: true,
		},
		{
			name: "required + full => warning (allowed)",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
				Type: cbv1alpha1.AntiAffinityRequired,
			},
			expectErr:   false,
			wantWarning: true,
		},
		{
			name: "required with no mode => no warning (segment mode not active)",
			affinity: &cbv1alpha1.ClusterAffinitySpec{
				Type: cbv1alpha1.AntiAffinityRequired,
			},
			expectErr:   false,
			wantWarning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := newValidCluster()
			cluster.Spec.Affinity = tt.affinity
			var warnings admission.Warnings

			err := validateAffinity(cluster, &warnings)

			if tt.expectErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)
			if tt.wantWarning {
				assert.NotEmpty(t, warnings, "expected a non-fatal warning")
			} else {
				assert.Empty(t, warnings)
			}
		})
	}
}

// TestValidateAffinity_ViaValidateCluster confirms the required+full path is
// surfaced through the top-level validateCluster as an ALLOWED admission with a
// warning (not a denial).
func TestValidateAffinity_ViaValidateCluster(t *testing.T) {
	cluster := newValidCluster()
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		Type: cbv1alpha1.AntiAffinityRequired,
	}
	warnings, err := validateCluster(cluster)
	require.NoError(t, err, "required-segment must be allowed (warning, not denial)")
	assert.NotEmpty(t, warnings)
}

// TestValidateAffinity_ViaValidateCluster_InvalidModeDenied confirms an invalid
// mode is a hard denial through validateCluster.
func TestValidateAffinity_ViaValidateCluster_InvalidModeDenied(t *testing.T) {
	cluster := newValidCluster()
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode: cbv1alpha1.ClusterAntiAffinityMode("nope"),
	}
	_, err := validateCluster(cluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "affinity.mode")
}

// TestSetAffinityDefaults covers filling Type/TopologyKey, preserving explicit
// values, and the nil no-op (no allocation).
func TestSetAffinityDefaults(t *testing.T) {
	t.Run("nil affinity => no allocation", func(t *testing.T) {
		cluster := newValidCluster()
		cluster.Spec.Affinity = nil
		setAffinityDefaults(cluster)
		assert.Nil(t, cluster.Spec.Affinity)
	})

	t.Run("empty affinity => Type and TopologyKey filled", func(t *testing.T) {
		cluster := newValidCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
		}
		setAffinityDefaults(cluster)
		require.NotNil(t, cluster.Spec.Affinity)
		assert.Equal(t, cbv1alpha1.AntiAffinityPreferred, cluster.Spec.Affinity.Type)
		assert.Equal(t, "kubernetes.io/hostname", cluster.Spec.Affinity.TopologyKey)
	})

	t.Run("explicit Type and TopologyKey preserved", func(t *testing.T) {
		cluster := newValidCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode:        cbv1alpha1.ClusterAntiAffinityModeFull,
			Type:        cbv1alpha1.AntiAffinityRequired,
			TopologyKey: "topology.kubernetes.io/zone",
		}
		setAffinityDefaults(cluster)
		assert.Equal(t, cbv1alpha1.AntiAffinityRequired, cluster.Spec.Affinity.Type)
		assert.Equal(t, "topology.kubernetes.io/zone", cluster.Spec.Affinity.TopologyKey)
	})

	t.Run("mode->toggle materialization is NOT done by defaulter", func(t *testing.T) {
		cluster := newValidCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		}
		setAffinityDefaults(cluster)
		// Toggles remain nil (resolved at build time, single source of truth).
		assert.Nil(t, cluster.Spec.Affinity.SegmentMirrorAntiAffinity)
		assert.Nil(t, cluster.Spec.Affinity.CoordinatorBackupAntiAffinity)
		assert.Nil(t, cluster.Spec.Affinity.CoordinatorStandbyAntiAffinity)
	})
}

// TestSetAffinityDefaults_ViaSetClusterDefaults confirms wiring through the
// top-level defaulter entrypoint.
func TestSetAffinityDefaults_ViaSetClusterDefaults(t *testing.T) {
	cluster := newMinimalCluster()
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
	}
	setClusterDefaults(cluster)
	require.NotNil(t, cluster.Spec.Affinity)
	assert.Equal(t, cbv1alpha1.AntiAffinityPreferred, cluster.Spec.Affinity.Type)
	assert.Equal(t, "kubernetes.io/hostname", cluster.Spec.Affinity.TopologyKey)
}

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestResolveAffinity is the resolver truth table: nil, mode presets, explicit
// per-field overrides (including explicit false), and Type/TopologyKey
// defaults/overrides.
func TestResolveAffinity(t *testing.T) {
	tests := []struct {
		name string
		spec *cbv1alpha1.ClusterAffinitySpec
		want resolvedAffinity
	}{
		{
			name: "nil spec => historical default (segment only, preferred, hostname)",
			spec: nil,
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "empty spec => historical default (segment only)",
			spec: &cbv1alpha1.ClusterAffinitySpec{},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "mode=segment-mirror => segment only",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "mode=full => all three toggles",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  true,
				coordinatorStandby: true,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "unknown mode leaves historical default",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityMode("bogus"),
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "explicit false overrides full-mode segment toggle",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode:                      cbv1alpha1.ClusterAntiAffinityModeFull,
				SegmentMirrorAntiAffinity: boolPtr(false),
			},
			want: resolvedAffinity{
				segmentMirror:      false,
				coordinatorBackup:  true,
				coordinatorStandby: true,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "explicit false overrides full-mode backup+standby toggles",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode:                           cbv1alpha1.ClusterAntiAffinityModeFull,
				CoordinatorBackupAntiAffinity:  boolPtr(false),
				CoordinatorStandbyAntiAffinity: boolPtr(false),
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "explicit true toggles on top of segment-mirror mode",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode:                           cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
				CoordinatorBackupAntiAffinity:  boolPtr(true),
				CoordinatorStandbyAntiAffinity: boolPtr(true),
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  true,
				coordinatorStandby: true,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "type=required sets required flag",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
				Type: cbv1alpha1.AntiAffinityRequired,
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  true,
				coordinatorStandby: true,
				required:           true,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "type=preferred keeps required false",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
				Type: cbv1alpha1.AntiAffinityPreferred,
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  true,
				coordinatorStandby: true,
				required:           false,
				topologyKey:        "kubernetes.io/hostname",
			},
		},
		{
			name: "topologyKey override honored",
			spec: &cbv1alpha1.ClusterAffinitySpec{
				Mode:        cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
				TopologyKey: "topology.kubernetes.io/zone",
			},
			want: resolvedAffinity{
				segmentMirror:      true,
				coordinatorBackup:  false,
				coordinatorStandby: false,
				required:           false,
				topologyKey:        "topology.kubernetes.io/zone",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveAffinity(tt.spec)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestCrossRoleAntiAffinityTerm verifies the term selects only the two identity
// labels (cluster + component) and carries the supplied topology key.
func TestCrossRoleAntiAffinityTerm(t *testing.T) {
	cluster := newTestCluster()
	term := crossRoleAntiAffinityTerm(cluster, util.ComponentCoordinator, "topology.kubernetes.io/zone")

	require.NotNil(t, term.LabelSelector)
	assert.Equal(t, map[string]string{
		util.LabelCluster:   "test-cluster",
		util.LabelComponent: util.ComponentCoordinator,
	}, term.LabelSelector.MatchLabels)
	assert.Equal(t, "topology.kubernetes.io/zone", term.TopologyKey)
	// The managed-by label must NOT be present in the selector.
	_, hasManagedBy := term.LabelSelector.MatchLabels[util.LabelManagedBy]
	assert.False(t, hasManagedBy)
}

// TestMergeAntiAffinityTerm covers allocation-only-when-nil, required vs
// preferred append, additive merge (never clobbering existing terms), and
// deterministic order.
func TestMergeAntiAffinityTerm(t *testing.T) {
	term := corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
		TopologyKey:   "kubernetes.io/hostname",
	}

	t.Run("allocates affinity + podAntiAffinity when nil (preferred)", func(t *testing.T) {
		got := mergeAntiAffinityTerm(nil, false, term)
		require.NotNil(t, got)
		require.NotNil(t, got.PodAntiAffinity)
		require.Len(t, got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
		assert.Equal(t, int32(100),
			got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].Weight)
		assert.Equal(t, term,
			got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].PodAffinityTerm)
		// Never allocates NodeAffinity/PodAffinity siblings.
		assert.Nil(t, got.NodeAffinity)
		assert.Nil(t, got.PodAffinity)
	})

	t.Run("required appends to required slice", func(t *testing.T) {
		got := mergeAntiAffinityTerm(nil, true, term)
		require.NotNil(t, got.PodAntiAffinity)
		require.Len(t, got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
		assert.Equal(t, term,
			got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0])
		assert.Empty(t, got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	})

	t.Run("never clobbers pre-existing NodeAffinity/PodAffinity/PodAntiAffinity", func(t *testing.T) {
		existingNodeAff := &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key: "disk", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"},
					}},
				}},
			},
		}
		existingPodAff := &corev1.PodAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight:          50,
				PodAffinityTerm: corev1.PodAffinityTerm{TopologyKey: "zone"},
			}},
		}
		preExistingTerm := corev1.PodAffinityTerm{TopologyKey: "pre-existing"}
		affinity := &corev1.Affinity{
			NodeAffinity: existingNodeAff,
			PodAffinity:  existingPodAff,
			PodAntiAffinity: &corev1.PodAntiAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
					Weight:          70,
					PodAffinityTerm: preExistingTerm,
				}},
			},
		}

		got := mergeAntiAffinityTerm(affinity, false, term)

		// NodeAffinity/PodAffinity are untouched (same pointers).
		assert.Same(t, existingNodeAff, got.NodeAffinity)
		assert.Same(t, existingPodAff, got.PodAffinity)
		// Pre-existing PodAntiAffinity term preserved AND new term appended
		// AFTER it (deterministic order).
		preferred := got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution
		require.Len(t, preferred, 2)
		assert.Equal(t, preExistingTerm, preferred[0].PodAffinityTerm)
		assert.Equal(t, int32(70), preferred[0].Weight)
		assert.Equal(t, term, preferred[1].PodAffinityTerm)
		assert.Equal(t, int32(100), preferred[1].Weight)
	})

	t.Run("deterministic append order across two merges", func(t *testing.T) {
		term1 := corev1.PodAffinityTerm{TopologyKey: "first"}
		term2 := corev1.PodAffinityTerm{TopologyKey: "second"}
		got := mergeAntiAffinityTerm(nil, true, term1)
		got = mergeAntiAffinityTerm(got, true, term2)
		req := got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		require.Len(t, req, 2)
		assert.Equal(t, "first", req[0].TopologyKey)
		assert.Equal(t, "second", req[1].TopologyKey)
	})
}

// TestBuildStandbyAffinity_ToggleOff verifies the standby pod keeps NO affinity
// when coordinatorStandby is not enabled (byte-identical historical default).
func TestBuildStandbyAffinity_ToggleOff(t *testing.T) {
	t.Run("nil spec => nil affinity", func(t *testing.T) {
		cluster := newTestCluster()
		assert.Nil(t, buildStandbyAffinity(cluster))
	})

	t.Run("segment-mirror mode alone => nil (standby toggle off)", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
		}
		assert.Nil(t, buildStandbyAffinity(cluster))
	})

	t.Run("explicit false disables standby term under full mode", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode:                           cbv1alpha1.ClusterAntiAffinityModeFull,
			CoordinatorStandbyAntiAffinity: boolPtr(false),
		}
		assert.Nil(t, buildStandbyAffinity(cluster))
	})
}

// TestBuildStandbyAffinity_On covers the required + preferred variants of the
// coordinator<->standby term applied to the standby pod template.
func TestBuildStandbyAffinity_On(t *testing.T) {
	t.Run("preferred variant selects component=coordinator", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			Type: cbv1alpha1.AntiAffinityPreferred,
		}
		aff := buildStandbyAffinity(cluster)
		require.NotNil(t, aff)
		require.NotNil(t, aff.PodAntiAffinity)
		require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
		wt := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0]
		assert.Equal(t, int32(100), wt.Weight)
		assert.Equal(t, util.ComponentCoordinator,
			wt.PodAffinityTerm.LabelSelector.MatchLabels[util.LabelComponent])
		assert.Equal(t, "kubernetes.io/hostname", wt.PodAffinityTerm.TopologyKey)
	})

	t.Run("required variant selects component=coordinator", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			Type: cbv1alpha1.AntiAffinityRequired,
		}
		aff := buildStandbyAffinity(cluster)
		require.NotNil(t, aff)
		require.NotNil(t, aff.PodAntiAffinity)
		require.Len(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
		term := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
		assert.Equal(t, util.ComponentCoordinator,
			term.LabelSelector.MatchLabels[util.LabelComponent])
		assert.Equal(t, "test-cluster", term.LabelSelector.MatchLabels[util.LabelCluster])
	})

	t.Run("explicit standby toggle true without full mode", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			CoordinatorStandbyAntiAffinity: boolPtr(true),
		}
		aff := buildStandbyAffinity(cluster)
		require.NotNil(t, aff)
		require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	})
}

// TestBuildBackupAntiAffinity covers the toggle-off passthrough (nil stays nil,
// existing affinity preserved unchanged) and the additive merge when enabled.
func TestBuildBackupAntiAffinity(t *testing.T) {
	t.Run("toggle off => input returned unchanged (nil)", func(t *testing.T) {
		cluster := newTestCluster()
		assert.Nil(t, buildBackupAntiAffinity(cluster, nil))
	})

	t.Run("toggle off => existing affinity returned unchanged", func(t *testing.T) {
		cluster := newTestCluster()
		existing := &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{}}
		got := buildBackupAntiAffinity(cluster, existing)
		assert.Same(t, existing, got)
	})

	t.Run("enabled preferred => merges coordinator term", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		}
		got := buildBackupAntiAffinity(cluster, nil)
		require.NotNil(t, got)
		require.Len(t, got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
		term := got.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0]
		assert.Equal(t, util.ComponentCoordinator,
			term.PodAffinityTerm.LabelSelector.MatchLabels[util.LabelComponent])
	})

	t.Run("enabled required => merges required coordinator term additively", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			Type: cbv1alpha1.AntiAffinityRequired,
		}
		existingNodeAff := &corev1.NodeAffinity{}
		existing := &corev1.Affinity{NodeAffinity: existingNodeAff}
		got := buildBackupAntiAffinity(cluster, existing)
		require.NotNil(t, got)
		// Existing NodeAffinity preserved.
		assert.Same(t, existingNodeAff, got.NodeAffinity)
		require.Len(t, got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
		assert.Equal(t, util.ComponentCoordinator,
			got.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].
				LabelSelector.MatchLabels[util.LabelComponent])
	})
}

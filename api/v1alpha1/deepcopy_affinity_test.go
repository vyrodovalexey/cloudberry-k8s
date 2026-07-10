package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

// TestClusterAffinitySpec_DeepCopy_FullyPopulated (D1) exercises every non-nil
// *bool branch of ClusterAffinitySpec.DeepCopyInto and proves the copy is
// independent (distinct pointers, mutating the original does not affect it).
func TestClusterAffinitySpec_DeepCopy_FullyPopulated(t *testing.T) {
	src := &ClusterAffinitySpec{
		Mode:                           ClusterAntiAffinityModeFull,
		SegmentMirrorAntiAffinity:      boolPtr(true),
		CoordinatorBackupAntiAffinity:  boolPtr(false),
		CoordinatorStandbyAntiAffinity: boolPtr(true),
		Type:                           AntiAffinityRequired,
		TopologyKey:                    "topology.kubernetes.io/zone",
	}

	copied := src.DeepCopy()
	require.NotNil(t, copied)
	// Value equality.
	assert.Equal(t, src.Mode, copied.Mode)
	assert.Equal(t, src.Type, copied.Type)
	assert.Equal(t, src.TopologyKey, copied.TopologyKey)
	require.NotNil(t, copied.SegmentMirrorAntiAffinity)
	require.NotNil(t, copied.CoordinatorBackupAntiAffinity)
	require.NotNil(t, copied.CoordinatorStandbyAntiAffinity)
	assert.Equal(t, *src.SegmentMirrorAntiAffinity, *copied.SegmentMirrorAntiAffinity)
	assert.Equal(t, *src.CoordinatorBackupAntiAffinity, *copied.CoordinatorBackupAntiAffinity)
	assert.Equal(t, *src.CoordinatorStandbyAntiAffinity, *copied.CoordinatorStandbyAntiAffinity)

	// Pointers must be distinct (deep, not shallow) copies.
	assert.NotSame(t, src.SegmentMirrorAntiAffinity, copied.SegmentMirrorAntiAffinity)
	assert.NotSame(t, src.CoordinatorBackupAntiAffinity, copied.CoordinatorBackupAntiAffinity)
	assert.NotSame(t, src.CoordinatorStandbyAntiAffinity, copied.CoordinatorStandbyAntiAffinity)

	// Mutating the original's *bool + scalars must not affect the copy.
	*src.SegmentMirrorAntiAffinity = false
	*src.CoordinatorBackupAntiAffinity = true
	src.Mode = ClusterAntiAffinityModeSegmentMirror
	src.TopologyKey = "changed"
	assert.True(t, *copied.SegmentMirrorAntiAffinity, "copy independent of original *bool")
	assert.False(t, *copied.CoordinatorBackupAntiAffinity)
	assert.Equal(t, ClusterAntiAffinityModeFull, copied.Mode)
	assert.Equal(t, "topology.kubernetes.io/zone", copied.TopologyKey)
}

// TestClusterAffinitySpec_DeepCopy_NilPointers exercises the nil *bool branches
// of DeepCopyInto (each pointer left nil).
func TestClusterAffinitySpec_DeepCopy_NilPointers(t *testing.T) {
	src := &ClusterAffinitySpec{
		Mode: ClusterAntiAffinityModeSegmentMirror,
		Type: AntiAffinityPreferred,
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Nil(t, copied.SegmentMirrorAntiAffinity)
	assert.Nil(t, copied.CoordinatorBackupAntiAffinity)
	assert.Nil(t, copied.CoordinatorStandbyAntiAffinity)
	assert.Equal(t, ClusterAntiAffinityModeSegmentMirror, copied.Mode)
}

// TestClusterAffinitySpec_DeepCopy_NilReceiver (D4) verifies the nil-receiver
// branch returns nil without panicking.
func TestClusterAffinitySpec_DeepCopy_NilReceiver(t *testing.T) {
	var s *ClusterAffinitySpec
	assert.Nil(t, s.DeepCopy())
}

// TestClusterAffinitySpec_DeepCopyInto exercises DeepCopyInto directly.
func TestClusterAffinitySpec_DeepCopyInto(t *testing.T) {
	src := &ClusterAffinitySpec{
		Mode:                          ClusterAntiAffinityModeFull,
		CoordinatorBackupAntiAffinity: boolPtr(true),
	}
	var dst ClusterAffinitySpec
	src.DeepCopyInto(&dst)
	require.NotNil(t, dst.CoordinatorBackupAntiAffinity)
	assert.True(t, *dst.CoordinatorBackupAntiAffinity)
	assert.NotSame(t, src.CoordinatorBackupAntiAffinity, dst.CoordinatorBackupAntiAffinity)
}

// TestCloudberryClusterSpec_DeepCopy_Affinity (D2) proves Spec.DeepCopy handles
// both a populated and a nil Affinity pointer nil-safely.
func TestCloudberryClusterSpec_DeepCopy_Affinity(t *testing.T) {
	t.Run("populated Affinity is deep-copied independently", func(t *testing.T) {
		src := &CloudberryClusterSpec{
			Affinity: &ClusterAffinitySpec{
				Mode:                      ClusterAntiAffinityModeFull,
				SegmentMirrorAntiAffinity: boolPtr(true),
			},
		}
		copied := src.DeepCopy()
		require.NotNil(t, copied.Affinity)
		assert.NotSame(t, src.Affinity, copied.Affinity)
		require.NotNil(t, copied.Affinity.SegmentMirrorAntiAffinity)
		// Mutate original — copy stays intact.
		*src.Affinity.SegmentMirrorAntiAffinity = false
		src.Affinity.Mode = ClusterAntiAffinityModeSegmentMirror
		assert.True(t, *copied.Affinity.SegmentMirrorAntiAffinity)
		assert.Equal(t, ClusterAntiAffinityModeFull, copied.Affinity.Mode)
	})

	t.Run("nil Affinity => nil copy, no panic", func(t *testing.T) {
		src := &CloudberryClusterSpec{Affinity: nil}
		copied := src.DeepCopy()
		require.NotNil(t, copied)
		assert.Nil(t, copied.Affinity)
	})
}

// TestStandbySpec_DeepCopy_Tolerations (D3) verifies StandbySpec.Tolerations is
// deep-copied (contents equal, backing arrays distinct).
func TestStandbySpec_DeepCopy_Tolerations(t *testing.T) {
	seconds := int64(30)
	src := &StandbySpec{
		Enabled: true,
		Tolerations: []Toleration{
			{Key: "k1", Operator: "Equal", Value: "v1", Effect: "NoSchedule", TolerationSeconds: &seconds},
			{Key: "k2", Operator: "Exists", Effect: "NoExecute"},
		},
	}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	require.Len(t, copied.Tolerations, 2)
	assert.Equal(t, "k1", copied.Tolerations[0].Key)
	require.NotNil(t, copied.Tolerations[0].TolerationSeconds)
	assert.Equal(t, int64(30), *copied.Tolerations[0].TolerationSeconds)

	// Backing arrays distinct: mutating the original slice/element must not
	// leak into the copy.
	assert.NotSame(t, &src.Tolerations[0], &copied.Tolerations[0])
	src.Tolerations[0].Key = "mutated"
	*src.Tolerations[0].TolerationSeconds = 999
	assert.Equal(t, "k1", copied.Tolerations[0].Key)
	assert.Equal(t, int64(30), *copied.Tolerations[0].TolerationSeconds)
}

// TestStandbySpec_DeepCopy_NilTolerations covers the nil-Tolerations branch.
func TestStandbySpec_DeepCopy_NilTolerations(t *testing.T) {
	src := &StandbySpec{Enabled: true}
	copied := src.DeepCopy()
	require.NotNil(t, copied)
	assert.Nil(t, copied.Tolerations)
}

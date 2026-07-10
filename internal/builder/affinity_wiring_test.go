package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// ---------------------------------------------------------------------------
// REGRESSION: spec.affinity == nil ⇒ byte-identical cross-component preferred
// segment term; coordinator/standby/backup have NO affinity; standby has no
// tolerations. Legacy segments.antiAffinity: required still produces the
// required term.
// ---------------------------------------------------------------------------

// assertDefaultSegmentTerm asserts the historical cross-component preferred
// term (weight 100, hostname topology, selecting the OPPOSITE role's component).
func assertDefaultSegmentTerm(t *testing.T, aff *corev1.Affinity, wantComponent string) {
	t.Helper()
	require.NotNil(t, aff)
	require.NotNil(t, aff.PodAntiAffinity)
	require.Nil(t, aff.NodeAffinity)
	require.Nil(t, aff.PodAffinity)
	require.Empty(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
	require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	wt := aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0]
	assert.Equal(t, int32(100), wt.Weight)
	assert.Equal(t, "kubernetes.io/hostname", wt.PodAffinityTerm.TopologyKey)
	require.NotNil(t, wt.PodAffinityTerm.LabelSelector)
	assert.Equal(t, map[string]string{
		util.LabelCluster:   "test-cluster",
		util.LabelComponent: wantComponent,
	}, wt.PodAffinityTerm.LabelSelector.MatchLabels)
}

func TestSegmentAffinity_NilSpec_ByteIdenticalBaseline(t *testing.T) {
	cluster := newTestCluster()
	require.Nil(t, cluster.Spec.Affinity)

	// Primary STS pod repels the MIRROR component.
	primary := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	assertDefaultSegmentTerm(t, primary, util.ComponentSegmentMirror)

	// Mirror STS pod repels the PRIMARY component.
	mirror := buildSegmentAffinity(cluster, util.ComponentSegmentPrimary)
	assertDefaultSegmentTerm(t, mirror, util.ComponentSegmentPrimary)
}

func TestSegmentAffinity_NilSpec_OtherRolesHaveNoAffinity(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled: true,
		Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
	}
	cluster.Spec.Backup = newBackupCluster().Spec.Backup

	coord, err := b.BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	assert.Nil(t, coord.Spec.Template.Spec.Affinity, "coordinator must have NO affinity by default")

	standby, err := b.BuildStandbyStatefulSet(cluster)
	require.NoError(t, err)
	assert.Nil(t, standby.Spec.Template.Spec.Affinity, "standby must have NO affinity by default")
	assert.Nil(t, standby.Spec.Template.Spec.Tolerations, "standby must have NO tolerations by default")

	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000", Type: "full"})
	require.NotNil(t, job)
	assert.Nil(t, job.Spec.Template.Spec.Affinity, "backup Job pod must have NO affinity by default")

	cj := b.BuildBackupCronJob(cluster)
	require.NotNil(t, cj)
	assert.Nil(t, cj.Spec.JobTemplate.Spec.Template.Spec.Affinity,
		"backup CronJob pod must have NO affinity by default")
}

func TestSegmentAffinity_LegacyRequired_StillProducesRequiredTerm(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.AntiAffinity = cbv1alpha1.AntiAffinityRequired

	aff := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	require.NotNil(t, aff)
	require.NotNil(t, aff.PodAntiAffinity)
	require.Len(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
	assert.Empty(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	term := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	assert.Equal(t, "kubernetes.io/hostname", term.TopologyKey)
	assert.Equal(t, util.ComponentSegmentMirror,
		term.LabelSelector.MatchLabels[util.LabelComponent])
}

// ---------------------------------------------------------------------------
// Segment term: forced preferred even when spec.affinity.type=required;
// topologyKey override honored; segment toggle off returns nil.
// ---------------------------------------------------------------------------

func TestSegmentAffinity_SpecTypeRequired_ForcedPreferred(t *testing.T) {
	cluster := newTestCluster()
	// Legacy field stays preferred; new spec requests required — the segment
	// term must NOT be upgraded to required.
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		Type: cbv1alpha1.AntiAffinityRequired,
	}

	aff := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	require.NotNil(t, aff)
	require.NotNil(t, aff.PodAntiAffinity)
	assert.Empty(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution,
		"spec.affinity.type=required must NOT force the segment term to required")
	require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
}

func TestSegmentAffinity_TopologyKeyOverride(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode:        cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
		TopologyKey: "topology.kubernetes.io/zone",
	}

	aff := buildSegmentAffinity(cluster, util.ComponentSegmentPrimary)
	require.NotNil(t, aff)
	require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	assert.Equal(t, "topology.kubernetes.io/zone",
		aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].
			PodAffinityTerm.TopologyKey)
}

func TestSegmentAffinity_SegmentToggleOff_ReturnsNil(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode:                      cbv1alpha1.ClusterAntiAffinityModeFull,
		SegmentMirrorAntiAffinity: boolPtr(false),
	}
	assert.Nil(t, buildSegmentAffinity(cluster, util.ComponentSegmentMirror))
}

// TestSegmentAffinity_LegacyRequired_TopologyOverrideHonored proves the legacy
// required placement combines with the new topologyKey override.
func TestSegmentAffinity_LegacyRequired_TopologyOverrideHonored(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Segments.AntiAffinity = cbv1alpha1.AntiAffinityRequired
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode:        cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
		TopologyKey: "topology.kubernetes.io/zone",
	}
	aff := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	require.NotNil(t, aff)
	require.Len(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
	assert.Equal(t, "topology.kubernetes.io/zone",
		aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey)
}

// ---------------------------------------------------------------------------
// coordinator↔standby wiring: standby STS pod gains the term; standby
// tolerations propagate; coordinator STS unchanged.
// ---------------------------------------------------------------------------

func TestBuildStandbyStatefulSet_CoordinatorStandbyAffinityWiring(t *testing.T) {
	b := NewBuilder()

	t.Run("full mode required => standby pod gains required coordinator term", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
			Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
		}
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			Type: cbv1alpha1.AntiAffinityRequired,
		}
		sts, err := b.BuildStandbyStatefulSet(cluster)
		require.NoError(t, err)
		aff := sts.Spec.Template.Spec.Affinity
		require.NotNil(t, aff)
		require.Len(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
		assert.Equal(t, util.ComponentCoordinator,
			aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].
				LabelSelector.MatchLabels[util.LabelComponent])

		// Coordinator STS remains untouched.
		coord, err := b.BuildCoordinatorStatefulSet(cluster)
		require.NoError(t, err)
		assert.Nil(t, coord.Spec.Template.Spec.Affinity)
	})

	t.Run("full mode preferred => standby pod gains preferred coordinator term", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
			Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
		}
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		}
		sts, err := b.BuildStandbyStatefulSet(cluster)
		require.NoError(t, err)
		aff := sts.Spec.Template.Spec.Affinity
		require.NotNil(t, aff)
		require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	})
}

func TestBuildStandbyStatefulSet_TolerationsPropagate(t *testing.T) {
	b := NewBuilder()

	t.Run("standby tolerations flow into pod Tolerations", func(t *testing.T) {
		seconds := int64(120)
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled: true,
			Storage: &cbv1alpha1.StorageSpec{Size: "10Gi"},
			Tolerations: []cbv1alpha1.Toleration{
				{
					Key:               "standby-only",
					Operator:          "Equal",
					Value:             "true",
					Effect:            "NoSchedule",
					TolerationSeconds: &seconds,
				},
			},
		}
		sts, err := b.BuildStandbyStatefulSet(cluster)
		require.NoError(t, err)
		tols := sts.Spec.Template.Spec.Tolerations
		require.Len(t, tols, 1)
		assert.Equal(t, "standby-only", tols[0].Key)
		assert.Equal(t, corev1.TolerationOpEqual, tols[0].Operator)
		assert.Equal(t, corev1.TaintEffectNoSchedule, tols[0].Effect)
		require.NotNil(t, tols[0].TolerationSeconds)
		assert.Equal(t, int64(120), *tols[0].TolerationSeconds)
	})

	t.Run("empty standby tolerations => no tolerations", func(t *testing.T) {
		cluster := newTestCluster()
		cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
			Enabled:     true,
			Storage:     &cbv1alpha1.StorageSpec{Size: "10Gi"},
			Tolerations: []cbv1alpha1.Toleration{},
		}
		sts, err := b.BuildStandbyStatefulSet(cluster)
		require.NoError(t, err)
		assert.Nil(t, sts.Spec.Template.Spec.Tolerations)
	})
}

// ---------------------------------------------------------------------------
// coordinator↔backup wiring: on-demand Job + scheduled CronJob pods gain the
// coordinator term; additive to jobTemplate nodeSelector/tolerations.
// ---------------------------------------------------------------------------

func TestBuildBackupJob_CoordinatorBackupAffinityWiring(t *testing.T) {
	b := NewBuilder()

	assertBackupCoordinatorTerm := func(t *testing.T, aff *corev1.Affinity, required bool) {
		t.Helper()
		require.NotNil(t, aff)
		require.NotNil(t, aff.PodAntiAffinity)
		if required {
			require.Len(t, aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, 1)
			assert.Equal(t, util.ComponentCoordinator,
				aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].
					LabelSelector.MatchLabels[util.LabelComponent])
			return
		}
		require.Len(t, aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
		assert.Equal(t, util.ComponentCoordinator,
			aff.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0].
				PodAffinityTerm.LabelSelector.MatchLabels[util.LabelComponent])
	}

	t.Run("on-demand Job pod gains preferred coordinator term", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000", Type: "full"})
		require.NotNil(t, job)
		assertBackupCoordinatorTerm(t, job.Spec.Template.Spec.Affinity, false)
	})

	t.Run("scheduled CronJob pod gains required coordinator term", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
			Type: cbv1alpha1.AntiAffinityRequired,
		}
		cj := b.BuildBackupCronJob(cluster)
		require.NotNil(t, cj)
		assertBackupCoordinatorTerm(t, cj.Spec.JobTemplate.Spec.Template.Spec.Affinity, true)
	})

	t.Run("additive to jobTemplate nodeSelector and tolerations", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
		}
		cluster.Spec.Backup.JobTemplate = &cbv1alpha1.BackupJobTemplate{
			NodeSelector: map[string]string{"role": "backup"},
			Tolerations: []cbv1alpha1.Toleration{
				{Key: "backup", Operator: "Exists", Effect: "NoSchedule"},
			},
		}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000", Type: "full"})
		require.NotNil(t, job)
		podSpec := job.Spec.Template.Spec
		// Anti-affinity term present.
		assertBackupCoordinatorTerm(t, podSpec.Affinity, false)
		// jobTemplate nodeSelector + tolerations preserved (additive).
		assert.Equal(t, "backup", podSpec.NodeSelector["role"])
		require.Len(t, podSpec.Tolerations, 1)
		assert.Equal(t, "backup", podSpec.Tolerations[0].Key)
	})

	t.Run("backup toggle off => no affinity on Job pod", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
			Mode: cbv1alpha1.ClusterAntiAffinityModeSegmentMirror,
		}
		job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000", Type: "full"})
		require.NotNil(t, job)
		assert.Nil(t, job.Spec.Template.Spec.Affinity)
	})
}

// TestBuildStatefulSets_FullMode_StandbyAndBackupTerms verifies the full-mode
// composition: standby term + backup term present together, and standby
// tolerations propagate alongside the affinity.
func TestBuildStatefulSets_FullMode_StandbyAndBackupTerms(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{
		Enabled:     true,
		Storage:     &cbv1alpha1.StorageSpec{Size: "10Gi"},
		Tolerations: []cbv1alpha1.Toleration{{Key: "s", Operator: "Exists"}},
	}
	cluster.Spec.Affinity = &cbv1alpha1.ClusterAffinitySpec{
		Mode: cbv1alpha1.ClusterAntiAffinityModeFull,
	}

	standby, err := b.BuildStandbyStatefulSet(cluster)
	require.NoError(t, err)
	require.NotNil(t, standby.Spec.Template.Spec.Affinity)
	require.Len(t, standby.Spec.Template.Spec.Affinity.PodAntiAffinity.
		PreferredDuringSchedulingIgnoredDuringExecution, 1)
	require.Len(t, standby.Spec.Template.Spec.Tolerations, 1)

	job := b.BuildBackupJob(cluster, &BackupJobOptions{Timestamp: "20260519020000", Type: "full"})
	require.NotNil(t, job)
	require.NotNil(t, job.Spec.Template.Spec.Affinity)
	require.Len(t, job.Spec.Template.Spec.Affinity.PodAntiAffinity.
		PreferredDuringSchedulingIgnoredDuringExecution, 1)

	// Segment terms still emitted (full mode keeps segment on).
	seg := buildSegmentAffinity(cluster, util.ComponentSegmentMirror)
	require.NotNil(t, seg)
	require.Len(t, seg.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
}

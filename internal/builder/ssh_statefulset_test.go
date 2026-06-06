package builder

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// mainContainerMounts returns the VolumeMounts of the main "cloudberry"
// container in the StatefulSet pod template.
func mainContainerMounts(t *testing.T, sts *appsv1.StatefulSet) []string {
	t.Helper()
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name == containerName {
			names := make([]string, 0, len(c.VolumeMounts))
			for _, m := range c.VolumeMounts {
				if m.Name == sshSecretVolumeName && m.MountPath == sshSecretMountPath {
					names = append(names, m.Name)
				}
			}
			return names
		}
	}
	return nil
}

// assertHasClusterSSH asserts that the StatefulSet mounts the shared cluster-ssh
// Secret volume and that the main container has the corresponding mount.
func assertHasClusterSSH(t *testing.T, sts *appsv1.StatefulSet) {
	t.Helper()
	require.NotNil(t, sts)
	require.True(t,
		hasVolume(sts.Spec.Template.Spec.Volumes, sshSecretVolumeName),
		"pod must include the cluster-ssh volume")
	require.NotEmpty(t,
		mainContainerMounts(t, sts),
		"main container must mount cluster-ssh at the SSH staging path")
}

// mirrorEnabledCluster returns a cluster with standby and segment mirroring
// enabled so all four StatefulSet builders produce non-nil results.
func mirrorEnabledCluster() *cbv1alpha1.CloudberryCluster {
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}
	return cluster
}

func TestStatefulSets_IncludeClusterSSHVolume(t *testing.T) {
	b := NewBuilder()
	cluster := mirrorEnabledCluster()

	t.Run("coordinator", func(t *testing.T) {
		sts, err := b.BuildCoordinatorStatefulSet(cluster)
		require.NoError(t, err)
		assertHasClusterSSH(t, sts)
	})

	t.Run("standby", func(t *testing.T) {
		sts, err := b.BuildStandbyStatefulSet(cluster)
		require.NoError(t, err)
		assertHasClusterSSH(t, sts)
	})

	t.Run("segment-primary", func(t *testing.T) {
		sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
		require.NoError(t, err)
		assertHasClusterSSH(t, sts)
	})

	t.Run("segment-mirror", func(t *testing.T) {
		sts, err := b.BuildSegmentMirrorStatefulSet(cluster)
		require.NoError(t, err)
		assertHasClusterSSH(t, sts)
	})
}

//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 81: Local Destination Backup/Restore (integration)
// ============================================================================
//
// These integration tests exercise the REAL component interactions for the
// local (PVC-backed) backup destination: the builder renders the local backup
// Job (PVC volume + --backup-dir, no S3 plugin), the (fake) Kubernetes client
// persists it, and we read the materialized Job object back from the API server's
// client to assert the persisted spec carries the local PVC volume mounted at the
// path and the --backup-dir args (no --plugin-config / no s3-plugin-config
// volume). It also asserts the operator-side BuildBackupS3ConfigMap is a no-op for
// a local destination, so reconcileBackup creates NO S3 ConfigMap.
//
//	81a/e : a local-destination cluster's backup Job is persisted with the
//	        backup-data PVC volume (claimName backup-pvc) mounted at /backups and
//	        gpbackup args carrying --backup-dir /backups (no --plugin-config).
//	81d   : the persisted restore Job carries the same PVC volume + --backup-dir.
//	81g   : BuildBackupS3ConfigMap returns nil for local (no S3 ConfigMap created).
//
// The builder + k8s client wiring is real; only the cluster/k8s backend is a fake
// client (no live MPP cluster). This mirrors the scenario80 integration harness.
// The real local gpbackup/gprestore --backup-dir run (and the per-segment
// files-land proof) is exercised by the e2e live script via coordinator-exec,
// since the standalone backup Job pod is NOT a segment host and a single RWO PVC
// cannot hold every segment's backup set.
// ============================================================================

const (
	scenario81IntNamespace = "cloudberry-test"
	scenario81IntCluster   = "scenario81-local"
	scenario81IntTS        = "20260608020000"
	scenario81IntPVC       = "backup-pvc"
	scenario81IntPath      = "/backups"

	scenario81IntLocalVolume    = "backup-data"
	scenario81IntS3ConfigVolume = "s3-plugin-config"
	scenario81IntS3ConfigMount  = "/etc/gpbackup"
)

// Scenario81IntegrationSuite drives the builder + fake k8s backend for the local
// backup destination.
type Scenario81IntegrationSuite struct {
	suite.Suite
	env *testutil.TestK8sEnv
	ctx context.Context
}

func TestIntegration_Scenario81(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario81IntegrationSuite))
}

func (s *Scenario81IntegrationSuite) SetupTest() {
	s.ctx = context.Background()
}

// cluster builds a local-destination backup cluster mirroring the scenario81-local
// sample CR (HA + segment mirroring, local destination, PVC backup-pvc).
func (s *Scenario81IntegrationSuite) cluster() *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(scenario81IntCluster, scenario81IntNamespace).
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		WithSegments(2).
		WithMirroring(true, cbv1alpha1.MirroringLayoutGroup).
		WithStandby(true).
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   "cloudberry-backup:2.1.0",
		Retention: cbv1alpha1.BackupRetention{
			FullCount:        3,
			IncrementalCount: 10,
			MaxAge:           "30d",
		},
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			S3:   nil,
			Local: &cbv1alpha1.LocalDestination{
				Path:                  scenario81IntPath,
				PersistentVolumeClaim: scenario81IntPVC,
			},
		},
	}
	return cluster
}

func scenario81IntFindVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func scenario81IntHasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

// --- 81a/e: persisted local backup Job carries the PVC volume + --backup-dir ---

// TestIntegration_Scenario81_BackupJobPersistsLocalPVC builds the local backup
// Job, persists it through the fake client, reads it back and asserts the
// persisted spec carries the backup-data PVC volume (claimName backup-pvc)
// mounted at /backups and gpbackup args carrying --backup-dir (no --plugin-config
// / no s3-plugin-config volume / no /etc/gpbackup mount).
func (s *Scenario81IntegrationSuite) TestIntegration_Scenario81_BackupJobPersistsLocalPVC() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	job := s.env.Builder.BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81IntTS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NoError(s.T(), s.env.Client.Create(s.ctx, job),
		"the local backup Job must persist in the fake API server")

	got := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.BackupJobName(scenario81IntCluster, scenario81IntTS),
		Namespace: scenario81IntNamespace,
	}, got), "the persisted local backup Job must be readable from k8s")

	podSpec := got.Spec.Template.Spec
	pvcVol := scenario81IntFindVolume(podSpec.Volumes, scenario81IntLocalVolume)
	require.NotNil(s.T(), pvcVol, "persisted local Job must carry the backup-data PVC volume")
	require.NotNil(s.T(), pvcVol.PersistentVolumeClaim)
	assert.Equal(s.T(), scenario81IntPVC, pvcVol.PersistentVolumeClaim.ClaimName)

	assert.Nil(s.T(), scenario81IntFindVolume(podSpec.Volumes, scenario81IntS3ConfigVolume),
		"persisted local Job must NOT carry the s3-plugin-config volume")

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81IntHasMount(c.VolumeMounts, scenario81IntLocalVolume, scenario81IntPath),
		"persisted local container must mount backup-data at /backups")
	for _, m := range c.VolumeMounts {
		assert.NotEqual(s.T(), scenario81IntS3ConfigMount, m.MountPath,
			"persisted local container must NOT mount the s3 config at /etc/gpbackup")
	}

	require.NotEmpty(s.T(), c.Args)
	script := c.Args[0]
	assert.Contains(s.T(), script, "--backup-dir")
	assert.Contains(s.T(), script, scenario81IntPath)
	assert.NotContains(s.T(), script, "--plugin-config")
	assert.NotContains(s.T(), script, "/tmp/s3-config.yaml")
}

// --- 81d: persisted local restore Job carries the PVC volume + --backup-dir ---

// TestIntegration_Scenario81_RestoreJobPersistsLocalPVC builds + persists the
// local restore Job and asserts the read-back spec carries the PVC volume mounted
// at /backups and --backup-dir args (no --plugin-config).
func (s *Scenario81IntegrationSuite) TestIntegration_Scenario81_RestoreJobPersistsLocalPVC() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	job := s.env.Builder.BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:  scenario81IntTS,
		Databases:  []string{"mydb"},
		RedirectDb: "mydb_restore",
	})
	require.NotNil(s.T(), job)
	require.NoError(s.T(), s.env.Client.Create(s.ctx, job))

	got := &batchv1.Job{}
	require.NoError(s.T(), s.env.Client.Get(s.ctx, types.NamespacedName{
		Name:      util.RestoreJobName(scenario81IntCluster, scenario81IntTS),
		Namespace: scenario81IntNamespace,
	}, got))

	podSpec := got.Spec.Template.Spec
	pvcVol := scenario81IntFindVolume(podSpec.Volumes, scenario81IntLocalVolume)
	require.NotNil(s.T(), pvcVol, "persisted local restore Job must carry the backup-data PVC volume")
	require.NotNil(s.T(), pvcVol.PersistentVolumeClaim)
	assert.Equal(s.T(), scenario81IntPVC, pvcVol.PersistentVolumeClaim.ClaimName)
	assert.Nil(s.T(), scenario81IntFindVolume(podSpec.Volumes, scenario81IntS3ConfigVolume))

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81IntHasMount(c.VolumeMounts, scenario81IntLocalVolume, scenario81IntPath))

	require.NotEmpty(s.T(), c.Args)
	script := c.Args[0]
	assert.Contains(s.T(), script, "--backup-dir")
	assert.Contains(s.T(), script, scenario81IntPath)
	assert.NotContains(s.T(), script, "--plugin-config")
}

// --- 81g: no S3 ConfigMap for a local destination ---

// TestIntegration_Scenario81_NoS3ConfigMapForLocal asserts BuildBackupS3ConfigMap
// is a no-op for a local destination, so the operator's reconcileBackup creates
// NO <cluster>-backup-s3-config ConfigMap.
func (s *Scenario81IntegrationSuite) TestIntegration_Scenario81_NoS3ConfigMapForLocal() {
	cluster := s.cluster()
	s.env = testutil.NewTestK8sEnv(cluster)

	assert.Nil(s.T(), s.env.Builder.BuildBackupS3ConfigMap(cluster),
		"BuildBackupS3ConfigMap must return nil for a local destination (no S3 ConfigMap)")
}

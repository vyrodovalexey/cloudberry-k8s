//go:build e2e

package e2e

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 81: Local Destination Backup/Restore (E2E)
// ============================================================================
//
// User journey: an operator-managed cluster with backup.destination.{type:local,
// local:{path:/backups, persistentVolumeClaim:backup-pvc}} takes a backup that
// writes to a --backup-dir on the mounted PVC (NO S3 plugin), then restores from
// that local backup:
//
//	81a backup Job spec : the operator renders a backup Job that mounts backup-pvc
//	    at /backups, the gpbackup args carry --backup-dir /backups and NOT
//	    --plugin-config, and there is NO s3-plugin-config ConfigMap volume / no
//	    /etc/gpbackup mount / no S3 env (the headline operator-PVC-wiring check).
//	81b PVC writable    : a Job mounting backup-pvc at /backups can write+ls a
//	    file (PVC mounts read-write at the path).
//	81c real local backup: a real gpbackup with --backup-dir into a segment-visible
//	    path (/tmp/scenario81-backups) via coordinator-exec completes and lands
//	    per-segment backup files (NO --plugin-config).
//	81d real local restore: a real gprestore --backup-dir --create-db into a fresh
//	    db completes and the restored row counts match the source baseline.
//
// What THIS Go test verifies (infra-free, deterministic):
//   - Builder parity: BuildBackupJob/BuildRestoreJob for a local cluster mount the
//     named PVC at the path, carry --backup-dir (not --plugin-config) and omit the
//     s3-plugin-config volume / S3 env.
//   - A live-cluster portion gated on KUBECONFIG that self-skips. When live it
//     shells out to scenario81-local-destination.sh, which drives the full
//     Job-spec + PVC-writable + real local gpbackup/gprestore --backup-dir
//     lifecycle on a running cluster (real gpbackup/gprestore via coordinator-exec
//     into a segment-visible --backup-dir; the PVC-mount assertions prove the
//     operator's PVC wiring).
//
// MPP NOTE: gpbackup --backup-dir writes per-segment backup sets on the
// coordinator AND every segment host; the standalone backup Job pod mounting the
// single RWO backup-pvc is NOT a segment host. So the Job-SPEC assertions
// (81a/81b) prove the operator behaviour on the PVC, while the real local
// gpbackup/gprestore run (81c/81d) targets a --backup-dir present on ALL cluster
// pods (/tmp/scenario81-backups) to prove the toolchain end-to-end and that
// per-segment files land — split exactly as the live script documents.
// ============================================================================

const (
	// envS81Cluster overrides the live local-destination cluster name.
	envS81Cluster = "SCENARIO81_LOCAL_CLUSTER"
	// envS81Script overrides the live script path.
	envS81Script = "SCENARIO81_SCRIPT"

	scenario81E2EBackupImage = "cloudberry-backup:2.1.0"
	scenario81E2EPVC         = "backup-pvc"
	scenario81E2EPath        = "/backups"
	scenario81E2ETS          = "20260608020000"

	scenario81E2ELocalVolume    = "backup-data"
	scenario81E2ES3ConfigVolume = "s3-plugin-config"
	scenario81E2ES3ConfigMount  = "/etc/gpbackup"
)

// Scenario81LocalDestinationE2ESuite tests the local-destination backup/restore
// Job rendering (builder parity) and the KUBECONFIG-gated live portion.
type Scenario81LocalDestinationE2ESuite struct {
	suite.Suite
	ctx context.Context
}

func TestE2E_Scenario81(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario81LocalDestinationE2ESuite))
}

func (s *Scenario81LocalDestinationE2ESuite) SetupTest() {
	s.ctx = context.Background()
}

// scenario81E2ECluster builds a running cluster with a LOCAL backup destination
// (PVC backup-pvc at /backups) mirroring the scenario81-local sample CR.
func scenario81E2ECluster(name string) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario81E2EBackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "local",
			S3:   nil,
			Local: &cbv1alpha1.LocalDestination{
				Path:                  scenario81E2EPath,
				PersistentVolumeClaim: scenario81E2EPVC,
			},
		},
	}
	return cluster
}

func scenario81E2EFindVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func scenario81E2EHasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

// --- 81.1: builder parity (infra-free) — local backup Job spec + script ---

// TestE2E_Scenario81_BackupJobParity verifies BuildBackupJob for a local cluster
// mounts backup-pvc at /backups, carries --backup-dir /backups (not
// --plugin-config) and omits the s3-plugin-config volume / S3 env.
func (s *Scenario81LocalDestinationE2ESuite) TestE2E_Scenario81_BackupJobParity() {
	cluster := scenario81E2ECluster("test-s81e2e-backup")

	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81E2ETS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	pvcVol := scenario81E2EFindVolume(podSpec.Volumes, scenario81E2ELocalVolume)
	require.NotNil(s.T(), pvcVol, "local Job must include the backup-data PVC volume")
	require.NotNil(s.T(), pvcVol.PersistentVolumeClaim)
	assert.Equal(s.T(), scenario81E2EPVC, pvcVol.PersistentVolumeClaim.ClaimName)
	assert.Nil(s.T(), scenario81E2EFindVolume(podSpec.Volumes, scenario81E2ES3ConfigVolume),
		"local Job must NOT include the s3-plugin-config volume")

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81E2EHasMount(c.VolumeMounts, scenario81E2ELocalVolume, scenario81E2EPath),
		"local container must mount backup-data at /backups")
	for _, m := range c.VolumeMounts {
		assert.NotEqual(s.T(), scenario81E2ES3ConfigMount, m.MountPath)
	}
	for _, e := range c.Env {
		assert.NotEqual(s.T(), "S3_BUCKET", e.Name)
		assert.NotEqual(s.T(), "AWS_ACCESS_KEY_ID", e.Name)
		assert.NotEqual(s.T(), "AWS_SECRET_ACCESS_KEY", e.Name)
	}

	require.NotEmpty(s.T(), c.Args)
	script := c.Args[0]
	assert.Contains(s.T(), script, "--backup-dir")
	assert.Contains(s.T(), script, scenario81E2EPath)
	assert.NotContains(s.T(), script, "--plugin-config")
	assert.NotContains(s.T(), script, "/tmp/s3-config.yaml")
	assert.NotContains(s.T(), script, scenario81E2ES3ConfigMount,
		"local backup script must NOT render the S3 config from /etc/gpbackup")
}

// TestE2E_Scenario81_RestoreJobParity verifies BuildRestoreJob for a local cluster
// mounts the PVC at the path and carries --backup-dir (not --plugin-config).
func (s *Scenario81LocalDestinationE2ESuite) TestE2E_Scenario81_RestoreJobParity() {
	cluster := scenario81E2ECluster("test-s81e2e-restore")

	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:  scenario81E2ETS,
		Databases:  []string{"mydb"},
		RedirectDb: "mydb_restore",
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	pvcVol := scenario81E2EFindVolume(podSpec.Volumes, scenario81E2ELocalVolume)
	require.NotNil(s.T(), pvcVol, "local restore Job must include the backup-data PVC volume")
	require.NotNil(s.T(), pvcVol.PersistentVolumeClaim)
	assert.Equal(s.T(), scenario81E2EPVC, pvcVol.PersistentVolumeClaim.ClaimName)
	assert.Nil(s.T(), scenario81E2EFindVolume(podSpec.Volumes, scenario81E2ES3ConfigVolume))

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81E2EHasMount(c.VolumeMounts, scenario81E2ELocalVolume, scenario81E2EPath))
	require.NotEmpty(s.T(), c.Args)
	script := c.Args[0]
	assert.Contains(s.T(), script, "--backup-dir")
	assert.Contains(s.T(), script, scenario81E2EPath)
	assert.NotContains(s.T(), script, "--plugin-config")
}

// TestE2E_Scenario81_NoS3ConfigMapParity verifies BuildBackupS3ConfigMap is a
// no-op for a local destination (no S3 ConfigMap rendered).
func (s *Scenario81LocalDestinationE2ESuite) TestE2E_Scenario81_NoS3ConfigMapParity() {
	cluster := scenario81E2ECluster("test-s81e2e-nocm")
	assert.Nil(s.T(), builder.NewBuilder().BuildBackupS3ConfigMap(cluster),
		"local destination must NOT render an S3 ConfigMap")
}

// --- 81.2: live cluster part (gated on KUBECONFIG) ---

// TestE2E_Scenario81_LiveLocalDestination is the live-cluster portion. It
// self-skips when KUBECONFIG is unset so the suite never requires a real cluster
// or gpbackup/gprestore binaries. When live, it shells out to the scenario81 live
// script, which drives the full Job-spec + PVC-writable + real local
// gpbackup/gprestore --backup-dir lifecycle: it asserts the operator backup Job
// mounts backup-pvc at /backups with --backup-dir (no --plugin-config / no S3
// ConfigMap volume), probes that the PVC mounts read-write at the path, runs a
// real local gpbackup into a segment-visible --backup-dir (NO plugin) and asserts
// per-segment files land, then a real gprestore --backup-dir into a fresh db with
// a row-count match.
func (s *Scenario81LocalDestinationE2ESuite) TestE2E_Scenario81_LiveLocalDestination() {
	if os.Getenv(envKubeconfig) == "" {
		s.T().Skip("KUBECONFIG not set, skipping live local-destination verification")
	}

	cluster := os.Getenv(envS81Cluster)
	if cluster == "" {
		cluster = "scenario81-local"
	}

	script := os.Getenv(envS81Script)
	if script == "" {
		wd, err := os.Getwd()
		require.NoError(s.T(), err)
		script = filepath.Join(wd, "scripts", "scenario81-local-destination.sh")
	}
	if _, err := os.Stat(script); err != nil {
		s.T().Skipf("live script %s not found: %v", script, err)
	}

	// The live script is self-contained, idempotent and re-runnable; it drives
	// the full local-destination lifecycle and prints a per-check PASS/FAIL
	// summary.
	ctx, cancel := context.WithTimeout(s.ctx, 45*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", script,
		"--cluster", cluster,
		"--namespace", "cloudberry-test",
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	s.T().Logf("scenario81 live script output:\n%s", string(out))
	require.NoError(s.T(), err, "scenario81 live script must pass all local-destination checks")
	assert.Contains(s.T(), string(out), "PASS",
		"live script must print a PASS summary")
}

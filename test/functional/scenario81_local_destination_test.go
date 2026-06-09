//go:build functional

package functional

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 81: Local Destination Backup/Restore (functional)
// ============================================================================
//
// Scenario 81 covers the LOCAL (PVC-backed) backup destination at the builder
// layer (deterministic, no live cluster). The operator wires gpbackup/gprestore
// to write to a `--backup-dir` on a mounted PVC instead of the S3 plugin:
//
//	81a backup Job spec : BuildBackupJob for a local cluster mounts the named PVC
//	    (backup-pvc) at Local.Path (/backups), the container args carry
//	    --backup-dir /backups and NOT --plugin-config, and there is NO
//	    s3-plugin-config ConfigMap volume / no /etc/gpbackup mount / no S3_*/AWS_*
//	    env (the headline operator-behaviour check on the PVC).
//	81b restore Job spec: BuildRestoreJob for a local cluster passes --backup-dir
//	    and NOT --plugin-config, mounting the same PVC at the path.
//	81c tool script     : the rendered backup tool script for local does NOT
//	    render the S3 plugin config (no `cat /etc/gpbackup/...`, no envsubst, no
//	    > /tmp/s3-config.yaml) so the Job does not crash under set -euo pipefail
//	    reading a missing S3 ConfigMap.
//	81d S3 regression   : an S3 cluster still carries --plugin-config (and NEVER
//	    --backup-dir, which gpbackup rejects together with --plugin-config), the
//	    s3-plugin-config ConfigMap volume and the /etc/gpbackup render, and runs
//	    the tool inside the coordinator pod via kubectl exec (coordinator-exec).
//
// These tests black-box the operator through the public builder (BuildBackupJob /
// BuildRestoreJob rendered spec + script). They are deterministic and
// self-contained (no live infra). The real local gpbackup/gprestore --backup-dir
// run (and the per-segment files-land proof) is exercised by the e2e live script
// via coordinator-exec, since the standalone backup Job pod is NOT a segment host
// and a single RWO PVC cannot hold every segment's backup set.
// ============================================================================

const (
	scenario81BackupImage = "cloudberry-backup:2.1.0"
	// scenario81BackupPVC is the PVC the local destination mounts at the path.
	scenario81BackupPVC = "backup-pvc"
	// scenario81LocalPath is the on-pod local backup directory (--backup-dir).
	scenario81LocalPath = "/backups"
	// scenario81TS is a pinned 14-digit gpbackup-style timestamp.
	scenario81TS = "20260608020000"

	// Container arg / volume / mount markers asserted across the suite.
	scenario81BackupDirFlag  = "--backup-dir"
	scenario81PluginCfgFlag  = "--plugin-config"
	scenario81S3RenderedPath = "/tmp/s3-config.yaml"
	scenario81S3ConfigVolume = "s3-plugin-config"
	scenario81S3ConfigMount  = "/etc/gpbackup"
	scenario81LocalVolume    = "backup-data"
)

// scenario81CoordPod returns the coordinator pod name the S3 coordinator-exec
// backup/restore Jobs kubectl-exec into.
func scenario81CoordPod(cluster *cbv1alpha1.CloudberryCluster) string {
	return util.CoordinatorPodName(cluster.Name)
}

// Scenario81Suite exercises the local-destination backup/restore Job spec and the
// rendered tool script, plus the S3 regression guard.
type Scenario81Suite struct {
	suite.Suite
}

func TestFunctional_Scenario81(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario81Suite))
}

// scenario81LocalBackupSpec returns a LOCAL destination BackupSpec mirroring the
// scenario81-local sample CR (path /backups, PVC backup-pvc, NO S3).
func scenario81LocalBackupSpec(path, pvc string) *cbv1alpha1.BackupSpec {
	if path == "" {
		path = scenario81LocalPath
	}
	if pvc == "" {
		pvc = scenario81BackupPVC
	}
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario81BackupImage,
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
				Path:                  path,
				PersistentVolumeClaim: pvc,
			},
		},
	}
}

// scenario81S3BackupSpec returns an S3 (MinIO) destination BackupSpec used by the
// regression sub-cases.
func scenario81S3BackupSpec() *cbv1alpha1.BackupSpec {
	return &cbv1alpha1.BackupSpec{
		Enabled: true,
		Image:   scenario81BackupImage,
		Gpbackup: &cbv1alpha1.GpbackupOptions{
			CompressionLevel: 1,
			CompressionType:  "gzip",
			Jobs:             1,
		},
		Destination: cbv1alpha1.BackupDestination{
			Type: "s3",
			S3: &cbv1alpha1.S3Destination{
				Bucket:         "cloudberry-backups",
				Endpoint:       "http://minio:9000",
				Region:         "us-east-1",
				Folder:         "/backups",
				Encryption:     "on",
				ForcePathStyle: true,
				CredentialSecret: &cbv1alpha1.S3CredentialSecret{
					Name:           "backup-s3-credentials",
					AccessKeyField: "aws_access_key_id",
					SecretKeyField: "aws_secret_access_key",
				},
			},
		},
	}
}

// scenario81Cluster builds a Running cluster (pending generation) with the given
// backup spec, mirroring the functional harness used by scenario80.
func scenario81Cluster(name string, backup *cbv1alpha1.BackupSpec) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, "cloudberry-test").
		WithFinalizer().
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithPendingGeneration().
		Build()
	cluster.Spec.Backup = backup
	return cluster
}

// findVolume returns the named volume from a pod's volume list, or nil.
func scenario81FindVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

// hasMount reports whether the mounts contain a {name, path} pair.
func scenario81HasMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, m := range mounts {
		if m.Name == name && m.MountPath == path {
			return true
		}
	}
	return false
}

// assertLocalPodSpec asserts the Scenario 81 local-destination pod wiring on a
// rendered Job pod spec: a PVC volume named backup-data with the configured claim
// mounted at the path, NO s3-plugin-config volume, NO /etc/gpbackup mount and NO
// S3_*/AWS_* env (the buildBackupEnv non-S3 path).
func (s *Scenario81Suite) assertLocalPodSpec(podSpec corev1.PodSpec, path, claim string) {
	pvcVol := scenario81FindVolume(podSpec.Volumes, scenario81LocalVolume)
	require.NotNil(s.T(), pvcVol, "local Job must include the backup-data PVC volume")
	require.NotNil(s.T(), pvcVol.PersistentVolumeClaim, "backup-data volume must be a PVC")
	assert.Equal(s.T(), claim, pvcVol.PersistentVolumeClaim.ClaimName)

	assert.Nil(s.T(), scenario81FindVolume(podSpec.Volumes, scenario81S3ConfigVolume),
		"local Job must NOT include the s3-plugin-config volume")

	require.NotEmpty(s.T(), podSpec.Containers)
	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81HasMount(c.VolumeMounts, scenario81LocalVolume, path),
		"container must mount backup-data at %s", path)
	for _, m := range c.VolumeMounts {
		assert.NotEqual(s.T(), scenario81S3ConfigMount, m.MountPath,
			"local container must NOT mount the s3 config at /etc/gpbackup")
	}
	for _, e := range c.Env {
		assert.NotEqual(s.T(), "S3_BUCKET", e.Name)
		assert.NotEqual(s.T(), "S3_ENDPOINT", e.Name)
		assert.NotEqual(s.T(), "AWS_ACCESS_KEY_ID", e.Name)
		assert.NotEqual(s.T(), "AWS_SECRET_ACCESS_KEY", e.Name)
	}
}

// --- 81a: local backup Job mounts the PVC at the path; --backup-dir; no plugin ---

// TestFunctional_Scenario81_BackupJobMountsPVC asserts BuildBackupJob for a local
// cluster mounts backup-pvc at /backups, the gpbackup args carry --backup-dir
// /backups and NOT --plugin-config, and there is no s3-plugin-config volume.
func (s *Scenario81Suite) TestFunctional_Scenario81_BackupJobMountsPVC() {
	cluster := scenario81Cluster("s81-backup",
		scenario81LocalBackupSpec(scenario81LocalPath, scenario81BackupPVC))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81TS,
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	s.assertLocalPodSpec(podSpec, scenario81LocalPath, scenario81BackupPVC)

	require.NotEmpty(s.T(), podSpec.Containers[0].Args)
	script := podSpec.Containers[0].Args[0]
	assert.Contains(s.T(), script, scenario81BackupDirFlag,
		"local gpbackup invocation must carry --backup-dir")
	assert.Contains(s.T(), script, scenario81LocalPath)
	assert.NotContains(s.T(), script, scenario81PluginCfgFlag,
		"local gpbackup invocation must NOT carry --plugin-config")
	assert.NotContains(s.T(), script, scenario81S3RenderedPath)
}

// TestFunctional_Scenario81_BackupJobCustomPath asserts the PVC mount + args track
// a custom Local.Path.
func (s *Scenario81Suite) TestFunctional_Scenario81_BackupJobCustomPath() {
	cluster := scenario81Cluster("s81-backup-custom",
		scenario81LocalBackupSpec("/data/backups", scenario81BackupPVC))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	s.assertLocalPodSpec(podSpec, "/data/backups", scenario81BackupPVC)
	script := podSpec.Containers[0].Args[0]
	assert.Contains(s.T(), script, scenario81BackupDirFlag)
	assert.Contains(s.T(), script, "/data/backups")
	assert.NotContains(s.T(), script, scenario81PluginCfgFlag)
}

// --- 81b: local restore Job mounts the PVC at the path; --backup-dir; no plugin ---

// TestFunctional_Scenario81_RestoreJobMountsPVC asserts BuildRestoreJob for a
// local cluster mounts the PVC at the path and passes --backup-dir (not
// --plugin-config).
func (s *Scenario81Suite) TestFunctional_Scenario81_RestoreJobMountsPVC() {
	cluster := scenario81Cluster("s81-restore",
		scenario81LocalBackupSpec(scenario81LocalPath, scenario81BackupPVC))
	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp:  scenario81TS,
		Databases:  []string{"mydb"},
		RedirectDb: "mydb_restore",
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	s.assertLocalPodSpec(podSpec, scenario81LocalPath, scenario81BackupPVC)

	script := podSpec.Containers[0].Args[0]
	assert.Contains(s.T(), script, scenario81BackupDirFlag,
		"local gprestore invocation must carry --backup-dir")
	assert.Contains(s.T(), script, scenario81LocalPath)
	assert.Contains(s.T(), script, scenario81TS)
	assert.Contains(s.T(), script, "mydb_restore")
	assert.NotContains(s.T(), script, scenario81PluginCfgFlag,
		"local gprestore invocation must NOT carry --plugin-config")
}

// --- 81c: rendered backup tool script for local does NOT render the S3 config ---

// TestFunctional_Scenario81_BackupScriptOmitsS3Render asserts the rendered local
// backup tool script does NOT reference /etc/gpbackup, the S3 template, an
// envsubst render or > /tmp/s3-config.yaml (so the Job won't crash under
// set -euo pipefail reading a missing S3 ConfigMap), while still invoking
// gpbackup with --backup-dir.
func (s *Scenario81Suite) TestFunctional_Scenario81_BackupScriptOmitsS3Render() {
	cluster := scenario81Cluster("s81-script",
		scenario81LocalBackupSpec(scenario81LocalPath, scenario81BackupPVC))
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)
	require.NotEmpty(s.T(), job.Spec.Template.Spec.Containers[0].Args)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	// No S3-config render for local.
	assert.NotContains(s.T(), script, scenario81S3ConfigMount,
		"local backup script must NOT read /etc/gpbackup")
	assert.NotContains(s.T(), script, "s3-plugin-config.yaml.tpl")
	assert.NotContains(s.T(), script, "envsubst <")
	assert.NotContains(s.T(), script, "> "+scenario81S3RenderedPath)

	// The script must still be a guarded shell that invokes gpbackup --backup-dir.
	assert.True(s.T(), strings.HasPrefix(script, "set -euo pipefail"),
		"local backup script must start with set -euo pipefail")
	assert.Contains(s.T(), script, "gpbackup")
	assert.Contains(s.T(), script, scenario81BackupDirFlag)
}

// --- 81d: S3 cluster regression — plugin-config + s3 volume + s3 render ---

// TestFunctional_Scenario81_S3RegressionBackup asserts an S3 cluster's backup Job
// still carries --plugin-config, the s3-plugin-config ConfigMap volume mounted at
// /etc/gpbackup, and renders the S3 config — and does NOT carry the local PVC
// volume. It also asserts the S3 backup uses the coordinator-exec model (kubectl
// exec into the coordinator pod) and that it NEVER combines --plugin-config with
// --backup-dir (gpbackup rejects that pairing). See backupDestinationArgs /
// coordinatorExecScript and spec 11 §MPP Dispatch.
func (s *Scenario81Suite) TestFunctional_Scenario81_S3RegressionBackup() {
	cluster := scenario81Cluster("s81-s3", scenario81S3BackupSpec())
	job := builder.NewBuilder().BuildBackupJob(cluster, &builder.BackupJobOptions{
		Timestamp: scenario81TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	s3Vol := scenario81FindVolume(podSpec.Volumes, scenario81S3ConfigVolume)
	require.NotNil(s.T(), s3Vol, "s3 Job must include the s3-plugin-config volume")
	require.NotNil(s.T(), s3Vol.ConfigMap, "s3-plugin-config volume must be a ConfigMap")
	assert.Nil(s.T(), scenario81FindVolume(podSpec.Volumes, scenario81LocalVolume),
		"s3 Job must NOT include the local backup PVC volume")

	c := podSpec.Containers[0]
	assert.True(s.T(), scenario81HasMount(c.VolumeMounts, scenario81S3ConfigVolume, scenario81S3ConfigMount),
		"s3 container must mount the plugin config at /etc/gpbackup")

	script := c.Args[0]
	assert.Contains(s.T(), script, scenario81PluginCfgFlag,
		"s3 gpbackup invocation must carry --plugin-config")
	assert.Contains(s.T(), script, scenario81S3RenderedPath)
	assert.Contains(s.T(), script, scenario81S3ConfigMount,
		"s3 backup script must render the S3 config from /etc/gpbackup")
	assert.Contains(s.T(), script, "envsubst <")
	// Coordinator-exec: the real gpbackup runs INSIDE the coordinator pod.
	assert.Contains(s.T(), script, "exec -i",
		"s3 backup must kubectl-exec into the coordinator pod")
	assert.Contains(s.T(), script, scenario81CoordPod(cluster),
		"s3 backup must target the coordinator pod")
	// gpbackup REJECTS --plugin-config together with --backup-dir.
	assert.NotContains(s.T(), script, scenario81BackupDirFlag,
		"s3 gpbackup invocation must NOT carry --backup-dir")
}

// TestFunctional_Scenario81_S3RegressionRestore asserts an S3 restore Job still
// carries --plugin-config and the s3-plugin-config volume, uses the
// coordinator-exec model, and never carries --backup-dir.
func (s *Scenario81Suite) TestFunctional_Scenario81_S3RegressionRestore() {
	cluster := scenario81Cluster("s81-s3-restore", scenario81S3BackupSpec())
	job := builder.NewBuilder().BuildRestoreJob(cluster, &builder.RestoreJobOptions{
		Timestamp: scenario81TS,
		Databases: []string{"mydb"},
	})
	require.NotNil(s.T(), job)

	podSpec := job.Spec.Template.Spec
	require.NotNil(s.T(), scenario81FindVolume(podSpec.Volumes, scenario81S3ConfigVolume),
		"s3 restore Job must include the s3-plugin-config volume")
	assert.Nil(s.T(), scenario81FindVolume(podSpec.Volumes, scenario81LocalVolume),
		"s3 restore Job must NOT include the local backup PVC volume")

	script := podSpec.Containers[0].Args[0]
	assert.Contains(s.T(), script, scenario81PluginCfgFlag)
	assert.Contains(s.T(), script, "exec -i",
		"s3 restore must kubectl-exec into the coordinator pod")
	assert.Contains(s.T(), script, scenario81CoordPod(cluster),
		"s3 restore must target the coordinator pod")
	assert.NotContains(s.T(), script, scenario81BackupDirFlag,
		"s3 gprestore invocation must NOT carry --backup-dir")
}

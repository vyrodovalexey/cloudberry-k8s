package builder

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

// TestGpEnvPreambleSetUSafe verifies the gpEnvPreamble is safe under `set -u`:
// every GPHOME reference is guarded with the `:-` default-expansion form and the
// whole block is wrapped in an `if [ -n "${GPHOME:-}" ]` guard, so a missing
// GPHOME never aborts the script with "GPHOME: unbound variable".
func TestGpEnvPreambleSetUSafe(t *testing.T) {
	// Guard present.
	assert.Contains(t, gpEnvPreamble, `if [ -n "${GPHOME:-}" ]`)
	// Default-expansion form used.
	assert.Contains(t, gpEnvPreamble, "${GPHOME:-}")

	// No bare ${GPHOME} usage that is immediately followed by a path separator
	// (e.g. ${GPHOME}/bin) — those would be unbound-variable hazards under set -u.
	// We allow ${GPHOME:-...} forms; only a bare ${GPHOME}/ is unsafe.
	bareGphomePath := regexp.MustCompile(`\$\{GPHOME\}/`)
	assert.False(t, bareGphomePath.MatchString(gpEnvPreamble),
		"gpEnvPreamble must not contain a bare ${GPHOME}/ outside the :- default form")
}

// TestS3ConfigTemplateNoSignatureVersion verifies the rendered S3 plugin config
// template intentionally omits aws_signature_version (the version-matched plugin
// rejects the unknown field) while keeping the canonical executablepath.
func TestS3ConfigTemplateNoSignatureVersion(t *testing.T) {
	b := NewBuilder()
	cm := b.BuildBackupS3ConfigMap(newBackupCluster())
	require.NotNil(t, cm)
	tmpl := cm.Data[s3ConfigTemplateKey]

	assert.NotContains(t, tmpl, "aws_signature_version")
	assert.NotContains(t, tmpl, "S3_AWS_SIGNATURE_VERSION")
	assert.Contains(t, tmpl, "executablepath: /usr/local/bin/gpbackup_s3_plugin")
}

// TestBuildS3EnvNoSignatureVersion verifies buildS3Env no longer emits
// S3_AWS_SIGNATURE_VERSION.
func TestBuildS3EnvNoSignatureVersion(t *testing.T) {
	cluster := newBackupCluster()
	env := buildS3Env(cluster, cluster.Spec.Backup.Destination.S3)
	for _, e := range env {
		assert.NotEqual(t, "S3_AWS_SIGNATURE_VERSION", e.Name,
			"S3_AWS_SIGNATURE_VERSION must not be emitted")
	}
}

// findVolume returns the named volume from a pod spec, or nil.
func findVolume(vols []corev1.Volume, name string) *corev1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

// assertBackupSSHWiring asserts the Scenario 71 backup/restore pod wiring:
//   - mounts the cluster-ssh Secret at /etc/cloudberry/ssh
//   - mounts the backup-history emptyDir at /var/lib/gpbackup
//   - sets COORDINATOR_DATA_DIRECTORY to the backup-history path
func assertBackupSSHWiring(t *testing.T, podSpec corev1.PodSpec) {
	t.Helper()

	// cluster-ssh volume must be a Secret volume.
	sshVol := findVolume(podSpec.Volumes, sshSecretVolumeName)
	require.NotNil(t, sshVol, "pod must include the cluster-ssh volume")
	require.NotNil(t, sshVol.Secret, "cluster-ssh volume must be backed by a Secret")

	// backup-history volume must be an emptyDir.
	histVol := findVolume(podSpec.Volumes, backupHistoryVolumeName)
	require.NotNil(t, histVol, "pod must include the backup-history volume")
	require.NotNil(t, histVol.EmptyDir, "backup-history volume must be an emptyDir")

	require.NotEmpty(t, podSpec.Containers)
	c := podSpec.Containers[0]

	require.True(t, hasMount(c.VolumeMounts, sshSecretVolumeName, sshSecretMountPath),
		"container must mount cluster-ssh at /etc/cloudberry/ssh")
	require.True(t, hasMount(c.VolumeMounts, backupHistoryVolumeName, backupHistoryMountPath),
		"container must mount backup-history at /var/lib/gpbackup")

	var coordDataDir string
	var found bool
	for _, e := range c.Env {
		if e.Name == "COORDINATOR_DATA_DIRECTORY" {
			coordDataDir = e.Value
			found = true
		}
	}
	require.True(t, found, "COORDINATOR_DATA_DIRECTORY must be set")
	assert.Equal(t, backupHistoryMountPath, coordDataDir)
}

func TestBuildBackupJobSSHAndHistoryWiring(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{
		Timestamp: "20260519020000",
		Type:      "full",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)
	assertBackupSSHWiring(t, job.Spec.Template.Spec)
}

func TestBuildRestoreJobSSHAndHistoryWiring(t *testing.T) {
	b := NewBuilder()
	job := b.BuildRestoreJob(newBackupCluster(), &RestoreJobOptions{
		Timestamp: "20260519020000",
		Databases: []string{"mydb"},
	})
	require.NotNil(t, job)
	assertBackupSSHWiring(t, job.Spec.Template.Spec)
}

// TestRenderToolScriptSSHPreamble verifies the rendered tool script includes the
// SSH-setup preamble (installing the shared keys into ~/.ssh and a silent ssh
// config) and the gpbackup plugin-path preamble/symlink, and that it references
// the canonical /usr/local/bin/gpbackup_s3_plugin path.
func TestRenderToolScriptSSHPreamble(t *testing.T) {
	script := renderToolScript("gpbackup", []string{"--dbname", "mydb"})

	// SSH setup preamble.
	assert.Contains(t, script, "/etc/cloudberry/ssh/id_ed25519")
	assert.Contains(t, script, `mkdir -p "${HOME}/.ssh"`)
	assert.Contains(t, script, "StrictHostKeyChecking no")
	assert.Contains(t, script, "UserKnownHostsFile /dev/null")

	// gpbackup plugin path preamble / symlink.
	assert.Contains(t, script, "/usr/local/bin/gpbackup_s3_plugin")
	assert.Contains(t, script, "ln -sf")

	// gpEnvPreamble carried into the script and still set-u safe.
	assert.Contains(t, script, `if [ -n "${GPHOME:-}" ]`)
	assert.True(t, strings.HasPrefix(script, "set -euo pipefail"))
}

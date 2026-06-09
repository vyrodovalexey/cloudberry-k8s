package builder

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestDropLeadingPluginConfig verifies the helper strips a leading
// "--plugin-config <path>" pair (and only that) so the coordinator-exec inner
// tool never re-emits the Job-pod plugin-config path.
func TestDropLeadingPluginConfig(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "drops leading plugin-config pair",
			in:   []string{pluginConfigFlag, s3RenderedConfigPath, "--dbname", "mydb"},
			want: []string{"--dbname", "mydb"},
		},
		{
			name: "no plugin-config leading pair is left intact",
			in:   []string{backupDirFlag, "/backups", "--dbname", "mydb"},
			want: []string{backupDirFlag, "/backups", "--dbname", "mydb"},
		},
		{
			name: "empty args",
			in:   nil,
			want: nil,
		},
		{
			name: "lone plugin-config flag without value is left intact",
			in:   []string{pluginConfigFlag},
			want: []string{pluginConfigFlag},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, dropLeadingPluginConfig(tc.in))
		})
	}
}

// TestCoordinatorExecScriptShape asserts the S3 coordinator-exec wrapper:
//   - renders the S3 plugin config in the Job pod (envsubst, /etc/gpbackup),
//   - stages it into the coordinator pod and execs the tool there,
//   - keeps the gpbackup flags VISIBLE in the script (e2e args[0] assertions),
//   - never combines --plugin-config with --backup-dir, and
//   - never re-emits the Job-pod /tmp/s3-config.yaml as a tool arg.
func TestCoordinatorExecScriptShape(t *testing.T) {
	cluster := newBackupCluster()
	args := buildGpbackupArgs(cluster, &cbv1alpha1.GpbackupOptions{
		CompressionLevel: 6,
		Jobs:             4,
	}, &BackupJobOptions{Databases: []string{"mydb"}})
	script := renderToolScript(cluster, "gpbackup", args)

	// S3 config render in the Job pod.
	assert.Contains(t, script, "envsubst <")
	assert.Contains(t, script, s3ConfigMountPath)
	assert.Contains(t, script, "> "+s3RenderedConfigPath)

	// Coordinator-exec wiring.
	assert.Contains(t, script, "KUBECTL="+kubectlBin)
	assert.Contains(t, script, "\"${KUBECTL}\" exec")
	assert.Contains(t, script, util.CoordinatorPodName(cluster.Name))

	// The gpbackup invocation + flags are visible (for the e2e Job-arg checks),
	// run against the coordinator-side ${COORD_CFG} (not the Job-pod path).
	assert.Contains(t, script, "gpbackup --plugin-config \"${COORD_CFG}\"")
	assert.Contains(t, script, "--dbname")
	assert.Contains(t, script, "--compression-level")
	assert.Contains(t, script, "--jobs")

	// gpbackup rejects --plugin-config together with --backup-dir.
	assert.NotContains(t, script, backupDirFlag)
}

// TestCoordinatorExecRestoreScriptShape asserts the restore wrapper runs
// gprestore inside the coordinator pod with the request flags visible.
func TestCoordinatorExecRestoreScriptShape(t *testing.T) {
	cluster := newBackupCluster()
	args := buildGprestoreArgs(cluster, &cbv1alpha1.GprestoreOptions{
		CreateDb:      true,
		ResizeCluster: true,
	}, &RestoreJobOptions{
		Timestamp: "20260608020000",
		Databases: []string{"mydb"},
	})
	script := renderToolScript(cluster, "gprestore", args)

	assert.Contains(t, script, "\"${KUBECTL}\" exec")
	assert.Contains(t, script, util.CoordinatorPodName(cluster.Name))
	assert.Contains(t, script, "gprestore --plugin-config \"${COORD_CFG}\"")
	assert.Contains(t, script, "--timestamp")
	assert.Contains(t, script, "20260608020000")
	assert.Contains(t, script, "--create-db")
	assert.Contains(t, script, "--resize-cluster")
	assert.NotContains(t, script, backupDirFlag)
}

// TestCoordinatorExecScriptIsValidBash runs `bash -n` over the rendered S3
// wrapper (and the inner heredoc) so a quoting/syntax regression fails fast.
func TestCoordinatorExecScriptIsValidBash(t *testing.T) {
	shell, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available; skipping syntax check")
	}
	cluster := newBackupCluster()
	args := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup,
		&BackupJobOptions{Databases: []string{"my db'with$pecial"}})
	script := renderToolScript(cluster, "gpbackup", args)

	cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
	cmd.Stdin = strings.NewReader(script)
	out, runErr := cmd.CombinedOutput()
	require.NoError(t, runErr, "bash -n reported a syntax error: %s", string(out))
}

// TestLocalDestinationStaysInPod is a regression guard: the LOCAL destination
// keeps the standalone in-pod --backup-dir model (no kubectl-exec wrapper), per
// spec 11 §Local Backup Destination.
func TestLocalDestinationStaysInPod(t *testing.T) {
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	args := buildGpbackupArgs(cluster, cluster.Spec.Backup.Gpbackup,
		&BackupJobOptions{Databases: []string{"mydb"}})
	script := renderToolScript(cluster, "gpbackup", args)

	assert.NotContains(t, script, "\"${KUBECTL}\" exec")
	assert.Contains(t, script, backupDirFlag)
	assert.Contains(t, script, shellQuote("/backups"))
}

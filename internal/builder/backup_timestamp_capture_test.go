package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRenderToolScriptGpbackupLocalCapturesTimestamp verifies the LOCAL
// (standalone --backup-dir) gpbackup script injects the real-timestamp capture
// wrapper: it greps gpbackup's emitted "Backup Timestamp = <14-digit>" line,
// writes the BACKUP_TIMESTAMP=<ts> marker to /dev/termination-log, and runs
// under `set -o pipefail` so a real backup failure still fails the Job.
func TestRenderToolScriptGpbackupLocalCapturesTimestamp(t *testing.T) {
	cluster := newLocalBackupCluster("/backups", "backup-pvc")
	args := []string{backupDirFlag, "/backups", "--dbname", "postgres"}

	script := renderToolScript(cluster, gpbackupTool, args)

	// The 14-digit "Backup Timestamp" grep capture is present.
	assert.Contains(t, script, "Backup Timestamp = [0-9]{14}",
		"gpbackup local script must grep gpbackup's emitted Backup Timestamp line")
	// The BACKUP_TIMESTAMP= marker the controller parses is written.
	assert.Contains(t, script, backupTimestampMarker)
	assert.Contains(t, script, "BACKUP_TIMESTAMP=")
	// The marker is surfaced on the pod termination log.
	assert.Contains(t, script, "/dev/termination-log",
		"captured timestamp must be written to the termination log")
	// pipefail keeps the real gpbackup exit code through the tee.
	assert.Contains(t, script, "pipefail")
	// The captured output is tee'd so the Job log still shows gpbackup output.
	assert.Contains(t, script, "tee")
	// The gpbackup tool itself is invoked.
	assert.Contains(t, script, gpbackupTool)
}

// TestCoordinatorExecScriptGpbackupCapturesTimestamp verifies the S3
// coordinator-exec gpbackup wrapper ALSO injects the real-timestamp capture:
// grep + BACKUP_TIMESTAMP= marker + termination-log write. The S3 path renders
// via coordinatorExecScript (tool != "" and non-local destination).
func TestCoordinatorExecScriptGpbackupCapturesTimestamp(t *testing.T) {
	cluster := newBackupCluster() // S3 destination
	args := []string{pluginConfigFlag, s3RenderedConfigPath, "--dbname", "postgres"}

	// renderToolScript delegates to coordinatorExecScript for S3 + non-empty tool.
	script := renderToolScript(cluster, gpbackupTool, args)

	assert.Contains(t, script, "Backup Timestamp = [0-9]{14}",
		"coordinator-exec gpbackup script must grep the emitted Backup Timestamp line")
	assert.Contains(t, script, backupTimestampMarker)
	assert.Contains(t, script, "BACKUP_TIMESTAMP=")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, "pipefail")
	assert.Contains(t, script, "tee")
}

// TestRenderToolScriptGprestoreNoTimestampCapture verifies the timestamp-capture
// wrapper is NOT injected for gprestore: the BACKUP_TIMESTAMP marker and the
// gpbackup "Backup Timestamp" grep belong to the backup path only.
func TestRenderToolScriptGprestoreNoTimestampCapture(t *testing.T) {
	t.Run("local destination", func(t *testing.T) {
		cluster := newLocalBackupCluster("/backups", "backup-pvc")
		args := []string{backupDirFlag, "/backups", "--timestamp", "20260620092039"}

		script := renderToolScript(cluster, gprestoreTool, args)

		assert.NotContains(t, script, backupTimestampMarker,
			"gprestore must not emit the BACKUP_TIMESTAMP marker")
		assert.NotContains(t, script, "Backup Timestamp = [0-9]{14}",
			"gprestore must not inject the gpbackup timestamp grep")
		// gprestore is still invoked.
		assert.Contains(t, script, gprestoreTool)
	})

	t.Run("s3 coordinator-exec destination", func(t *testing.T) {
		cluster := newBackupCluster() // S3 destination
		args := []string{pluginConfigFlag, s3RenderedConfigPath, "--timestamp", "20260620092039"}

		script := renderToolScript(cluster, gprestoreTool, args)

		assert.NotContains(t, script, backupTimestampMarker)
		assert.NotContains(t, script, "Backup Timestamp = [0-9]{14}")
	})
}

// TestGpbackupTimestampGrepShape sanity-checks the shared grep pattern used by
// both capture paths so the two render sites stay consistent: it must extract a
// 14-digit timestamp from a "Backup Timestamp = <ts>" line and is embedded in
// the rendered scripts verbatim.
func TestGpbackupTimestampGrepShape(t *testing.T) {
	assert.True(t, strings.Contains(gpbackupTimestampGrep, "Backup Timestamp = [0-9]{14}"))
	assert.True(t, strings.Contains(gpbackupTimestampGrep, "[0-9]{14}"))
}

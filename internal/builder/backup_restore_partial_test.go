package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// withStatsRestoreOpts returns restore options that explicitly request the
// statistics restore (--with-stats), enabling the exit-2 tolerance wrapper.
func withStatsRestoreOpts() *RestoreJobOptions {
	return &RestoreJobOptions{
		Timestamp: "20260101020000",
		Gprestore: &cbv1alpha1.GprestoreOptions{WithStats: util.Ptr(true)},
	}
}

func TestStatsPartialTolerated(t *testing.T) {
	tests := []struct {
		name string
		tool string
		args []string
		want bool
	}{
		{name: "gprestore with stats", tool: "gprestore", args: []string{"--timestamp", "x", "--with-stats"}, want: true},
		{name: "gprestore without stats", tool: "gprestore", args: []string{"--timestamp", "x"}, want: false},
		{name: "gpbackup with stats flag", tool: "gpbackup", args: []string{"--with-stats"}, want: false},
		{name: "empty tool", tool: "", args: []string{"--with-stats"}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, statsPartialTolerated(tc.tool, tc.args))
		})
	}
}

// TestBuildRestoreJob_StatsExitGuard_S3 pins the coordinator-exec (S3) restore
// script contract when statistics restore is requested: the kubectl exec exit
// code is captured, exit code 2 is downgraded to success-with-warning (with
// the GPRESTORE_PARTIAL termination marker), and any other code is propagated.
func TestBuildRestoreJob_StatsExitGuard_S3(t *testing.T) {
	b := NewBuilder()
	job := b.BuildRestoreJob(newBackupCluster(), withStatsRestoreOpts())
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "'--with-stats'")
	assert.Contains(t, script, "rc=0\n", "exit code must be captured")
	assert.Contains(t, script, "|| rc=$?", "kubectl exec failure must not abort under set -e")
	assert.Contains(t, script, `if [ "${rc}" -eq 2 ]; then`, "exit 2 must be tolerated")
	assert.Contains(t, script, restorePartialMarker, "termination marker must be written")
	assert.Contains(t, script, "/dev/termination-log")
	assert.Contains(t, script, `exit "${rc}"`, "other exit codes must propagate")
	// Cleanup of the staged coordinator config still runs.
	assert.Contains(t, script, "rm -f")
}

// TestBuildRestoreJob_NoStats_NoExitGuard pins that the default restore (no
// statistics restore — withStats defaults to false) does NOT carry the exit-2
// tolerance wrapper: a gprestore failure of any kind fails the Job.
func TestBuildRestoreJob_NoStats_NoExitGuard(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	job := b.BuildRestoreJob(cluster, &RestoreJobOptions{
		Timestamp: "20260101020000",
		Gprestore: &cbv1alpha1.GprestoreOptions{},
	})
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.NotContains(t, script, "--with-stats",
		"unset withStats must default to NOT restoring statistics")
	assert.NotContains(t, script, restorePartialMarker)
	assert.NotContains(t, script, `if [ "${rc}" -eq 2 ]; then`)
}

// TestBuildRestoreJob_StatsExitGuard_Local pins the local-destination restore
// script contract: the in-pod gprestore invocation is wrapped with the same
// exit-2 tolerance when statistics restore is requested.
func TestBuildRestoreJob_StatsExitGuard_Local(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "")
	job := b.BuildRestoreJob(cluster, withStatsRestoreOpts())
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "gprestore")
	assert.Contains(t, script, "'--with-stats'")
	assert.Contains(t, script, "|| rc=$?")
	assert.Contains(t, script, `if [ "${rc}" -eq 2 ]; then`)
	assert.Contains(t, script, restorePartialMarker)
	assert.Contains(t, script, `exit "${rc}"`)
}

// TestBuildRestoreJob_StatsExitGuard_Local_NoStats pins that the local restore
// without statistics keeps the plain invocation (no wrapper).
func TestBuildRestoreJob_StatsExitGuard_Local_NoStats(t *testing.T) {
	b := NewBuilder()
	cluster := newLocalBackupCluster("/backups", "")
	job := b.BuildRestoreJob(cluster, &RestoreJobOptions{
		Timestamp: "20260101020000",
		Gprestore: &cbv1alpha1.GprestoreOptions{},
	})
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.NotContains(t, script, "--with-stats")
	assert.NotContains(t, script, restorePartialMarker)
}

// TestBuildBackupJob_NeverCarriesExitGuard pins that gpbackup Jobs are NEVER
// wrapped with the gprestore exit-2 tolerance, even though gpbackup emits
// --with-stats by default.
func TestBuildBackupJob_NeverCarriesExitGuard(t *testing.T) {
	b := NewBuilder()
	job := b.BuildBackupJob(newBackupCluster(), &BackupJobOptions{Timestamp: "20260101020000"})
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "'--with-stats'", "gpbackup still backs up statistics by default")
	assert.NotContains(t, script, restorePartialMarker)
	assert.NotContains(t, script, `if [ "${rc}" -eq 2 ]; then`)
}

// TestRunAnalyzeSuppressesStatsAndGuard pins the run-analyze precedence: when
// both runAnalyze and withStats are requested, --with-stats is omitted (the
// gprestore mutual-exclusivity rule) and therefore no exit-2 guard is emitted.
func TestRunAnalyzeSuppressesStatsAndGuard(t *testing.T) {
	b := NewBuilder()
	job := b.BuildRestoreJob(newBackupCluster(), &RestoreJobOptions{
		Timestamp: "20260101020000",
		Gprestore: &cbv1alpha1.GprestoreOptions{
			WithStats:  util.Ptr(true),
			RunAnalyze: true,
		},
	})
	require.NotNil(t, job)
	script := job.Spec.Template.Spec.Containers[0].Args[0]

	assert.Contains(t, script, "'--run-analyze'")
	assert.NotContains(t, script, "--with-stats")
	assert.NotContains(t, script, restorePartialMarker)
}

// TestGprestoreStatsExitGuard_ShellSemantics sanity-checks the guard snippet
// shape: tolerates exactly exit code 2, logs the human-readable warning and
// clears rc so the Job pod exits 0.
func TestGprestoreStatsExitGuard_ShellSemantics(t *testing.T) {
	assert.True(t, strings.HasPrefix(gprestoreStatsExitGuard, `if [ "${rc}" -eq 2 ]; then`))
	assert.Contains(t, gprestoreStatsExitGuard, "gprestore-partial:")
	assert.Contains(t, gprestoreStatsExitGuard, restorePartialMarker)
	assert.Contains(t, gprestoreStatsExitGuard, "rc=0; fi\n")
}

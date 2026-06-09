package builder

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// TestPreBackupDestinationCheckScenario77 is a table-driven test for the
// Scenario 77 destination checks built by preBackupDestinationCheck /
// s3ReachabilityCheckScript. It asserts:
//   - S3 destinations emit the new fail-closed SigV4 HEAD reachability snippet.
//   - local destinations emit the df-based free-space check (and NOT the S3
//     snippet) as a regression guard.
//   - an unknown destination type emits no destination check (default branch).
//   - a nil Backup spec yields an empty destination check (no panic).
func TestPreBackupDestinationCheckScenario77(t *testing.T) {
	tests := []struct {
		name           string
		mutate         func(c *cbv1alpha1.CloudberryCluster)
		wantContains   []string
		wantNotContain []string
		wantEmpty      bool
	}{
		{
			name:   "s3 destination emits fail-closed SigV4 HEAD reachability check",
			mutate: func(_ *cbv1alpha1.CloudberryCluster) {}, // default cluster is S3.
			wantContains: []string{
				// New marker echoed before the HEAD request.
				"verifying s3 bucket reachability",
				// SigV4 HEAD curl wiring.
				"-X HEAD",
				"AWS4-HMAC-SHA256",
				"--max-time",
				// Reads only already-injected env vars (no interpolation).
				"${S3_ENDPOINT",
				"${S3_BUCKET}",
				"${S3_REGION:-us-east-1}",
				"${AWS_ACCESS_KEY_ID}",
				"${AWS_SECRET_ACCESS_KEY}",
				// Fail-closed: any non-2xx/3xx code blocks the backup.
				"s3 bucket unreachable",
				"exit 1",
			},
			wantNotContain: []string{
				// The S3 branch must NOT emit the local df free-space check.
				"df -Pk",
				// And must NOT fall back to the prior best-effort "aws s3 ls".
				"aws s3 ls",
			},
		},
		{
			name: "local destination emits df free-space check and no s3 snippet",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = destinationTypeLocal
				c.Spec.Backup.Destination.S3 = nil
				c.Spec.Backup.Destination.Local = &cbv1alpha1.LocalDestination{
					PersistentVolumeClaim: "backup-pvc",
					Path:                  "/data/backups",
				}
			},
			wantContains: []string{
				"verifying free disk space",
				"df -Pk",
				"/data/backups",
				"insufficient free space",
				"exit 1",
			},
			wantNotContain: []string{
				// Regression guard: the S3 reachability snippet must be ABSENT.
				"verifying s3 bucket reachability",
				"AWS4-HMAC-SHA256",
				"-X HEAD",
			},
		},
		{
			name: "local destination without path uses default mount path",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = destinationTypeLocal
				c.Spec.Backup.Destination.S3 = nil
				c.Spec.Backup.Destination.Local = &cbv1alpha1.LocalDestination{
					PersistentVolumeClaim: "backup-pvc",
				}
			},
			wantContains: []string{
				"df -Pk",
				localBackupMountPath,
			},
			wantNotContain: []string{
				"verifying s3 bucket reachability",
			},
		},
		{
			name: "unknown destination type emits no destination check",
			mutate: func(c *cbv1alpha1.CloudberryCluster) {
				c.Spec.Backup.Destination.Type = "gcs"
				c.Spec.Backup.Destination.S3 = nil
			},
			wantEmpty: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := newBackupCluster()
			tc.mutate(cluster)

			got := preBackupDestinationCheck(cluster)

			if tc.wantEmpty {
				assert.Empty(t, got)
				return
			}
			for _, want := range tc.wantContains {
				assert.Contains(t, got, want, "destination check must contain %q", want)
			}
			for _, notWant := range tc.wantNotContain {
				assert.NotContains(t, got, notWant, "destination check must NOT contain %q", notWant)
			}
		})
	}
}

// TestPreBackupDestinationCheckNilBackup verifies that a cluster without a
// Backup spec produces an empty destination check and does not panic.
func TestPreBackupDestinationCheckNilBackup(t *testing.T) {
	cluster := newTestCluster()
	cluster.Spec.Backup = nil
	assert.NotPanics(t, func() {
		assert.Empty(t, preBackupDestinationCheck(cluster))
	})
}

// TestPreBackupCheckScriptRegressionGuards asserts that the 77a (segment-down,
// status='d') and 77b (long-running transaction) checks remain present in the
// pre-backup script for both S3 and local destinations, alongside the new
// destination checks.
func TestPreBackupCheckScriptRegressionGuards(t *testing.T) {
	t.Run("s3 destination keeps 77a and 77b checks", func(t *testing.T) {
		script := preBackupCheckScript(newBackupCluster())
		// 77a: segments-up check via gp_segment_configuration status='d'.
		assert.Contains(t, script, "gp_segment_configuration")
		assert.Contains(t, script, "status='d'")
		assert.Contains(t, script, "down segment(s)")
		// 77b: long-running transaction check via pg_stat_activity.
		assert.Contains(t, script, "pg_stat_activity")
		assert.Contains(t, script, "long-running transaction(s)")
		// 77c: S3 reachability snippet is present for the S3 destination.
		assert.Contains(t, script, "verifying s3 bucket reachability")
	})

	t.Run("local destination keeps 77a and 77b checks", func(t *testing.T) {
		cluster := newBackupCluster()
		cluster.Spec.Backup.Destination.Type = destinationTypeLocal
		cluster.Spec.Backup.Destination.S3 = nil
		cluster.Spec.Backup.Destination.Local = &cbv1alpha1.LocalDestination{
			PersistentVolumeClaim: "backup-pvc",
			Path:                  "/data/backups",
		}
		script := preBackupCheckScript(cluster)
		assert.Contains(t, script, "gp_segment_configuration")
		assert.Contains(t, script, "status='d'")
		assert.Contains(t, script, "pg_stat_activity")
		// 77d: free disk-space check is present for the local destination.
		assert.Contains(t, script, "df -Pk")
	})
}

// TestS3ReachabilityCheckScriptSyntax validates that the generated S3
// reachability snippet is syntactically valid POSIX sh (sh -n), guarding
// against accidental quoting/heredoc breakage in the SigV4 signing block. The
// test self-skips when /bin/sh is unavailable.
func TestS3ReachabilityCheckScriptSyntax(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available; skipping POSIX syntax check")
	}

	snippet := s3ReachabilityCheckScript()
	// The snippet relies on env vars; sh -n only parses (does not execute), so a
	// minimal prelude keeps the parse self-contained without running anything.
	script := "set -eu\n" +
		"S3_REGION=us-east-1\nS3_ENDPOINT=http://minio:9000\nS3_BUCKET=b\n" +
		"AWS_ACCESS_KEY_ID=a\nAWS_SECRET_ACCESS_KEY=s\n" + snippet

	cmd := exec.Command(sh, "-n", "-c", script) //nolint:gosec // static test script.
	out, runErr := cmd.CombinedOutput()
	require.NoError(t, runErr, "S3 reachability snippet must be valid POSIX sh: %s", string(out))
}

// TestS3ReachabilityCheckScriptStable verifies the snippet is deterministic so
// the rendered init-container args do not churn across reconciles.
func TestS3ReachabilityCheckScriptStable(t *testing.T) {
	a := s3ReachabilityCheckScript()
	b := s3ReachabilityCheckScript()
	assert.Equal(t, a, b)
	// The configured signing-region default and curl timeout are wired in.
	assert.True(t, strings.Contains(a, defaultS3SigningRegion))
	assert.Contains(t, a, "--max-time")
}

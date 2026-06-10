package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// TestBuildGpbackupArgs_WithStatsPointer pins the *bool WithStats contract for
// the gpbackup arg builder: a nil pointer follows the webhook default of true
// (so --with-stats is emitted even without the webhook running), an explicit
// false OMITS the flag, and an explicit true emits it. This guards the
// bool->*bool migration where util.DerefOr(opts.WithStats, true) decides the
// flag — a plain bool would have made withStats:false indistinguishable from
// "unset".
func TestBuildGpbackupArgs_WithStatsPointer(t *testing.T) {
	tests := []struct {
		name      string
		withStats *bool
		wantFlag  bool
	}{
		{name: "nil defaults to true (flag emitted)", withStats: nil, wantFlag: true},
		{name: "explicit false omits flag", withStats: util.Ptr(false), wantFlag: false},
		{name: "explicit true emits flag", withStats: util.Ptr(true), wantFlag: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := buildGpbackupArgs(newBackupCluster(), &cbv1alpha1.GpbackupOptions{
				WithStats: tc.withStats,
			}, nil)
			joined := strings.Join(args, " ")
			if tc.wantFlag {
				assert.Contains(t, joined, "--with-stats")
			} else {
				assert.NotContains(t, joined, "--with-stats")
			}
		})
	}
}

// TestBuildGprestoreArgs_WithStatsPointer pins the *bool WithStats contract
// for the gprestore arg builder, with run-analyze disabled so the
// mutual-exclusivity rule does not interfere: nil defaults to FALSE (the flag
// is OMITTED — restores skip statistics unless explicitly requested, see the
// upstream gpbackup statistics.sql exit-2 bug), explicit false omits, explicit
// true emits.
func TestBuildGprestoreArgs_WithStatsPointer(t *testing.T) {
	tests := []struct {
		name      string
		withStats *bool
		wantFlag  bool
	}{
		{name: "nil defaults to false (flag omitted)", withStats: nil, wantFlag: false},
		{name: "explicit false omits flag", withStats: util.Ptr(false), wantFlag: false},
		{name: "explicit true emits flag", withStats: util.Ptr(true), wantFlag: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{
				WithStats:  tc.withStats,
				RunAnalyze: false,
			}, &RestoreJobOptions{Timestamp: "20260607020000"})
			joined := strings.Join(args, " ")
			if tc.wantFlag {
				assert.Contains(t, joined, "--with-stats")
			} else {
				assert.NotContains(t, joined, "--with-stats")
			}
			// run-analyze disabled, so it must never appear in these cases.
			assert.NotContains(t, joined, "--run-analyze")
		})
	}
}

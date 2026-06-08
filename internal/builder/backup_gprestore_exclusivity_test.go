package builder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

// Scenario 74: gprestore mutual-exclusivity precedence.
//
// These focused tests pin the two precedence rules enforced in
// buildGprestoreArgs / appendGprestoreBoolFlags. They complement the sub-cases
// already in TestBuildGprestoreArgs (both-filters, schema-only, with-stats-only)
// by adding the remaining table-only and run-analyze-only positive sub-cases so
// every branch of both rules is asserted with an explicit negative companion.

// TestBuildGprestoreArgsIncludeTablePrecedence pins the include-table /
// include-schema mutual-exclusivity rule on the RESTORE path: --include-table
// and --include-schema may never both appear.
func TestBuildGprestoreArgsIncludeTablePrecedence(t *testing.T) {
	tests := []struct {
		name           string
		includeSchemas []string
		includeTables  []string
		wantContains   []string
		wantAbsent     []string
	}{
		{
			// Both filters set: --include-table wins, --include-schema omitted.
			name:           "both: include-table wins, include-schema omitted",
			includeSchemas: []string{"public", "analytics"},
			includeTables:  []string{"public.users", "public.orders"},
			wantContains:   []string{"--include-table public.users", "--include-table public.orders"},
			wantAbsent:     []string{"--include-schema"},
		},
		{
			// Schema-only: --include-schema emitted, no --include-table.
			name:           "schema-only: include-schema emitted, no include-table",
			includeSchemas: []string{"public", "analytics"},
			includeTables:  nil,
			wantContains:   []string{"--include-schema public", "--include-schema analytics"},
			wantAbsent:     []string{"--include-table"},
		},
		{
			// Table-only: --include-table emitted, no --include-schema.
			name:          "table-only: include-table emitted, no include-schema",
			includeTables: []string{"public.users", "public.orders"},
			wantContains:  []string{"--include-table public.users", "--include-table public.orders"},
			wantAbsent:    []string{"--include-schema"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := buildGprestoreArgs(newBackupCluster(), &cbv1alpha1.GprestoreOptions{}, &RestoreJobOptions{
				Timestamp:      "20260607020000",
				IncludeSchemas: tc.includeSchemas,
				IncludeTables:  tc.includeTables,
			})
			joined := strings.Join(args, " ")
			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want)
			}
			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, joined, absent)
			}
		})
	}
}

// TestAppendGprestoreBoolFlagsRunAnalyzePrecedence pins the run-analyze /
// with-stats mutual-exclusivity rule: --run-analyze and --with-stats may never
// both appear; when both are requested --run-analyze takes precedence.
func TestAppendGprestoreBoolFlagsRunAnalyzePrecedence(t *testing.T) {
	tests := []struct {
		name         string
		withStats    bool
		runAnalyze   bool
		wantContains []string
		wantAbsent   []string
	}{
		{
			// Both set: --run-analyze wins, --with-stats omitted.
			name:         "both: run-analyze wins, with-stats omitted",
			withStats:    true,
			runAnalyze:   true,
			wantContains: []string{"--run-analyze"},
			wantAbsent:   []string{"--with-stats"},
		},
		{
			// with-stats only: --with-stats emitted, no --run-analyze.
			name:         "with-stats only: with-stats emitted, no run-analyze",
			withStats:    true,
			runAnalyze:   false,
			wantContains: []string{"--with-stats"},
			wantAbsent:   []string{"--run-analyze"},
		},
		{
			// run-analyze only: --run-analyze emitted, no --with-stats.
			name:         "run-analyze only: run-analyze emitted, no with-stats",
			withStats:    false,
			runAnalyze:   true,
			wantContains: []string{"--run-analyze"},
			wantAbsent:   []string{"--with-stats"},
		},
		{
			// Neither set: both flags absent.
			name:       "neither: both flags absent",
			withStats:  false,
			runAnalyze: false,
			wantAbsent: []string{"--with-stats", "--run-analyze"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := appendGprestoreBoolFlags(nil, &cbv1alpha1.GprestoreOptions{
				WithStats:  tc.withStats,
				RunAnalyze: tc.runAnalyze,
			})
			joined := strings.Join(args, " ")
			for _, want := range tc.wantContains {
				assert.Contains(t, joined, want)
			}
			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, joined, absent)
			}
		})
	}
}

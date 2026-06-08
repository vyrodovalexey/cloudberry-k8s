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

// countSubstr counts the number of non-overlapping occurrences of sub in s.
func countSubstr(s, sub string) int {
	return strings.Count(s, sub)
}

// assertValidShell parses the rendered script with `sh -n` (parse-only, never
// executed) so a syntax regression in the generated validation script is caught
// at unit-test time. It is skipped when no POSIX shell is available.
func assertValidShell(t *testing.T, script string) {
	t.Helper()
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("sh not available")
	}
	cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
	cmd.Stdin = strings.NewReader(script)
	out, runErr := cmd.CombinedOutput()
	require.NoError(t, runErr, "sh -n reported a syntax error: %s", string(out))
}

// TestPostRestoreValidationScript_Scenario80 is a table-driven test over the
// rendered post-restore validation script. It asserts the presence/ordering of
// the ANALYZE step, the per-table row-count compare (deterministic sorted
// order), the ROW_COUNT_MATCH/MISMATCH markers, the aggregate exit-on-mismatch,
// the must-pass invalid-index scan, and the health-check query.
func TestPostRestoreValidationScript_Scenario80(t *testing.T) {
	tests := []struct {
		name        string
		opts        *ValidationJobOptions
		contains    []string
		notContains []string
		assertFn    func(t *testing.T, script string)
	}{
		{
			name: "expected counts with run-analyze renders full compare",
			opts: &ValidationJobOptions{
				ExpectedRowCounts: map[string]int64{
					"public.users":  150000,
					"public.orders": 300000,
				},
				RunAnalyze: true,
			},
			contains: []string{
				"ANALYZE",
				validateMarkerPrefix + "ANALYZE_OK",
				validateMarkerPrefix + "row-count compare vs gpbackup history",
				"ROW_COUNT_MISMATCH",
				"ROW_COUNT_MATCH",
				"150000",
				"300000",
				"public.users",
				"public.orders",
				// aggregate exit-on-mismatch.
				`if [ "${rowcount_mismatch}" -gt 0 ]`,
				"exit 1",
				// must-pass invalid-index scan kept.
				"indisvalid",
				// default health-check query.
				defaultHealthCheckQuery,
				validateMarkerPrefix + "passed",
			},
			notContains: []string{
				"ROW_COUNT_PROBE_SKIPPED",
			},
			assertFn: func(t *testing.T, script string) {
				t.Helper()
				// Sorted order: orders (300000) compared before users (150000).
				ordersIdx := strings.Index(script, "expected='300000'")
				usersIdx := strings.Index(script, "expected='150000'")
				require.NotEqual(t, -1, ordersIdx, "orders compare missing")
				require.NotEqual(t, -1, usersIdx, "users compare missing")
				assert.Less(t, ordersIdx, usersIdx,
					"orders (300000) must be compared before users (150000) in sorted order")
				// Exactly two per-table compare blocks.
				assert.Equal(t, 2, countSubstr(script, "actual=$(psql -tA -c"),
					"expected exactly two per-table compare blocks")
				// ANALYZE precedes the row-count compare which precedes invalid-index.
				analyzeIdx := strings.Index(script, validateMarkerPrefix+"ANALYZE_OK")
				compareIdx := strings.Index(script,
					validateMarkerPrefix+"row-count compare vs gpbackup history")
				invalidIdx := strings.Index(script, "indisvalid")
				healthIdx := strings.Index(script, defaultHealthCheckQuery)
				require.NotEqual(t, -1, analyzeIdx)
				assert.Less(t, analyzeIdx, compareIdx, "ANALYZE must precede row-count compare")
				assert.Less(t, compareIdx, invalidIdx,
					"row-count compare must precede invalid-index scan")
				assert.Less(t, invalidIdx, healthIdx,
					"invalid-index scan must precede health-check")
			},
		},
		{
			name: "empty expected counts uses best-effort probe and skips strict compare",
			opts: &ValidationJobOptions{
				ExpectedRowCounts: map[string]int64{},
				RunAnalyze:        false,
			},
			contains: []string{
				validateMarkerPrefix + "ROW_COUNT_PROBE_SKIPPED",
				validateMarkerPrefix + "row-count probe (best-effort, no expected counts)",
				// invalid-index scan still present.
				"indisvalid",
				defaultHealthCheckQuery,
			},
			notContains: []string{
				"ROW_COUNT_MISMATCH",
				"ROW_COUNT_MATCH",
				"row-count compare vs gpbackup history",
				"rowcount_mismatch",
			},
			assertFn: func(t *testing.T, script string) {
				t.Helper()
				// No per-table compare block.
				assert.Equal(t, 0, countSubstr(script, "actual=$(psql -tA -c"))
			},
		},
		{
			name: "run-analyze false omits analyze step",
			opts: &ValidationJobOptions{
				ExpectedRowCounts: map[string]int64{"public.users": 10},
				RunAnalyze:        false,
			},
			contains: []string{
				"ROW_COUNT_MATCH",
				"public.users",
			},
			notContains: []string{
				validateMarkerPrefix + "ANALYZE_OK",
				`psql -c "ANALYZE"`,
				"run-analyze (refreshing planner stats)",
			},
		},
		{
			name: "custom health-check query is shell-quoted",
			opts: &ValidationJobOptions{
				HealthCheckQuery: "SELECT count(*) FROM app.heartbeat",
			},
			contains: []string{
				// shell-quoted via -c '...'.
				"psql -tA -c 'SELECT count(*) FROM app.heartbeat'",
			},
			notContains: []string{
				"psql -tA -c 'SELECT 1'",
			},
		},
		{
			name: "nil-like default options render default health query",
			opts: &ValidationJobOptions{},
			contains: []string{
				"psql -tA -c 'SELECT 1'",
				"ROW_COUNT_PROBE_SKIPPED",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			script := postRestoreValidationScript(tc.opts)

			for _, want := range tc.contains {
				assert.Contains(t, script, want)
			}
			for _, unwant := range tc.notContains {
				assert.NotContains(t, script, unwant)
			}
			if tc.assertFn != nil {
				tc.assertFn(t, script)
			}
			// Every rendered script must be valid POSIX shell.
			assertValidShell(t, script)
		})
	}
}

// TestWriteAnalyzeStep_Scenario80 exercises writeAnalyzeStep directly for both
// toggle states.
func TestWriteAnalyzeStep_Scenario80(t *testing.T) {
	t.Run("enabled renders analyze and marker", func(t *testing.T) {
		var b strings.Builder
		writeAnalyzeStep(&b, true)
		out := b.String()
		assert.Contains(t, out, `psql -c "ANALYZE"`)
		assert.Contains(t, out, validateMarkerPrefix+"ANALYZE_OK")
		assert.Contains(t, out, "run-analyze (refreshing planner stats)")
	})
	t.Run("disabled renders nothing", func(t *testing.T) {
		var b strings.Builder
		writeAnalyzeStep(&b, false)
		assert.Empty(t, b.String())
	})
}

// TestWriteRowCountStep_Scenario80 exercises writeRowCountStep for the empty and
// non-empty expected map paths, including deterministic ordering.
func TestWriteRowCountStep_Scenario80(t *testing.T) {
	t.Run("empty map renders best-effort probe only", func(t *testing.T) {
		var b strings.Builder
		writeRowCountStep(&b, nil)
		out := b.String()
		assert.Contains(t, out, "ROW_COUNT_PROBE_SKIPPED")
		assert.NotContains(t, out, "rowcount_mismatch")
		assert.NotContains(t, out, "ROW_COUNT_MISMATCH")
	})

	t.Run("non-empty map renders deterministic sorted compare with aggregate exit", func(t *testing.T) {
		var b strings.Builder
		writeRowCountStep(&b, map[string]int64{
			"public.z_table": 1,
			"public.a_table": 2,
		})
		out := b.String()
		assert.Contains(t, out, "rowcount_mismatch=0")
		assert.Contains(t, out, `if [ "${rowcount_mismatch}" -gt 0 ]`)
		assert.Contains(t, out, "exit 1")
		// a_table compared before z_table (sorted).
		aIdx := strings.Index(out, "public.a_table")
		zIdx := strings.Index(out, "public.z_table")
		require.NotEqual(t, -1, aIdx)
		require.NotEqual(t, -1, zIdx)
		assert.Less(t, aIdx, zIdx, "tables must compare in sorted order")
	})
}

// TestWriteRowCountTableCompare_Scenario80 exercises a single per-table compare
// block, asserting the parsable markers and shell-quoted identifiers.
func TestWriteRowCountTableCompare_Scenario80(t *testing.T) {
	var b strings.Builder
	writeRowCountTableCompare(&b, "public.users", 150000)
	out := b.String()
	assert.Contains(t, out, "actual=$(psql -tA -c 'SELECT count(*) FROM public.users')")
	assert.Contains(t, out, "expected='150000'")
	assert.Contains(t, out, "table='public.users'")
	assert.Contains(t, out, validateMarkerPrefix+"ROW_COUNT_MISMATCH")
	assert.Contains(t, out, validateMarkerPrefix+"ROW_COUNT_MATCH")
	assert.Contains(t, out, "rowcount_mismatch=$((rowcount_mismatch + 1))")
}

// TestWriteInvalidIndexStep_Scenario80 verifies the must-pass invalid-index scan
// is rendered with the exit-1 guard.
func TestWriteInvalidIndexStep_Scenario80(t *testing.T) {
	var b strings.Builder
	writeInvalidIndexStep(&b)
	out := b.String()
	assert.Contains(t, out, "relkind='i'")
	assert.Contains(t, out, "NOT i.indisvalid")
	assert.Contains(t, out, "invalid index(es)")
	assert.Contains(t, out, "exit 1")
}

// TestBuildPostRestoreValidationJob_Scenario80 verifies the validation Job
// metadata: the validate operation label, the script run via the shell, the
// PGDATABASE env when a Database is given, the owner ref and the deterministic
// Job name. It also confirms the rendered script reflects the options.
func TestBuildPostRestoreValidationJob_Scenario80(t *testing.T) {
	b := NewBuilder()
	cluster := newBackupCluster()
	job := b.BuildPostRestoreValidationJob(cluster, &ValidationJobOptions{
		Timestamp: "20260608130000",
		Database:  "mydb",
		ExpectedRowCounts: map[string]int64{
			"public.orders": 300000,
		},
		RunAnalyze:       true,
		HealthCheckQuery: "SELECT count(*) FROM app.heartbeat",
	})
	require.NotNil(t, job)

	assert.Equal(t,
		util.PostRestoreValidationJobName(cluster.Name, "20260608130000"), job.Name)
	assert.Equal(t, util.BackupOperationValidate, job.Labels[util.LabelBackupOperation])

	// Owner ref points at the cluster.
	require.Len(t, job.OwnerReferences, 1)
	assert.Equal(t, cluster.Name, job.OwnerReferences[0].Name)

	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	c := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, validateContainerName, c.Name)

	// Command runs the script via sh -c.
	require.NotEmpty(t, c.Command)
	assert.Equal(t, shellCommand, c.Command[0])
	assert.Equal(t, shellFlag, c.Command[1])
	require.NotEmpty(t, c.Args)
	script := c.Args[0]
	assert.Contains(t, script, "ROW_COUNT_MATCH")
	assert.Contains(t, script, "300000")
	assert.Contains(t, script, validateMarkerPrefix+"ANALYZE_OK")
	assert.Contains(t, script, "psql -tA -c 'SELECT count(*) FROM app.heartbeat'")

	// PGDATABASE set from opts.Database.
	var pgdb string
	for _, e := range c.Env {
		if e.Name == "PGDATABASE" {
			pgdb = e.Value
		}
	}
	assert.Equal(t, "mydb", pgdb)
}

// TestBuildPostRestoreValidationJob_NoDatabase_Scenario80 verifies that when no
// Database is supplied PGDATABASE keeps the default coordinator database (set by
// buildBackupEnv) rather than being overridden to a per-request value, and is not
// duplicated.
func TestBuildPostRestoreValidationJob_NoDatabase_Scenario80(t *testing.T) {
	job := NewBuilder().BuildPostRestoreValidationJob(newBackupCluster(),
		&ValidationJobOptions{Timestamp: "20260608130000"})
	require.NotNil(t, job)
	var count int
	var value string
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "PGDATABASE" {
			count++
			value = e.Value
		}
	}
	assert.Equal(t, 1, count, "PGDATABASE must appear exactly once")
	assert.Equal(t, defaultCoordinatorDatabase, value,
		"PGDATABASE must stay the default coordinator database when no Database is supplied")
}

// Ensure the BackupValidation type compiles against the cluster spec wiring used
// by the controller tests (keeps the builder test package honest about the API).
var _ = cbv1alpha1.BackupValidation{}

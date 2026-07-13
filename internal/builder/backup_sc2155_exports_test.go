package builder

// Test pinning the DEV-12 / L-8 SC2155-safe export shape in the generated
// coordinator-exec script (handoff gap UT-30): every connection variable must
// be assigned FIRST and exported SEPARATELY (`VAR=$(...); export VAR`) so a
// failing `base64 -d` is not masked by `export` under `set -euo pipefail`. A
// regression to the combined `export VAR=$(...)` form (the original bug)
// fails this test.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
)

func TestCoordinatorExecScript_SC2155SafeExports(t *testing.T) {
	// Arrange: render the S3 coordinator-exec wrapper.
	cluster := newBackupCluster()
	args := mustGpbackupArgs(t, cluster, &cbv1alpha1.GpbackupOptions{},
		&BackupJobOptions{Databases: []string{"mydb"}})

	// Act.
	script := renderToolScript(cluster, "gpbackup", args)

	// Assert: the six connection variables are delivered as base64 positional
	// parameters $1..$6 and decoded with the split assign-then-export shape.
	vars := []string{"COORD_CFG", "PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"}
	for i, name := range vars {
		position := i + 1
		wantLine := fmt.Sprintf(`%s=$(printf '%%s' "$%d" | base64 -d); export %s`,
			name, position, name)

		assert.Contains(t, script, wantLine,
			"%s must use the SC2155-safe assign-then-export shape from $%d", name, position)
		assert.NotContains(t, script, fmt.Sprintf("export %s=$(", name),
			"%s must not regress to the exit-code-masking combined `export VAR=$(...)` form",
			name)
	}

	// The decode failures must be able to abort the inner tool script.
	require.Contains(t, script, "set -euo pipefail",
		"the split-export shape only matters under errexit/pipefail")
}

package builder

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMigrationOpts returns MigrationJobOptions wired with the given target DB
// settings (RedirectDb/Database) and a table-filtered include list, mirroring the
// shape handleMigrate/migrateJobOptions construct for a cross-cluster migration.
func newMigrationOpts(database, redirectDb string) *MigrationJobOptions {
	src := newBackupCluster()
	src.Name = "test-mig-src"
	dst := newBackupCluster()
	dst.Name = "test-mig-dst"

	validationDB := redirectDb
	if validationDB == "" {
		validationDB = database
	}
	return &MigrationJobOptions{
		Timestamp:          "20260601020000",
		Source:             src,
		Target:             dst,
		Database:           database,
		RedirectDb:         redirectDb,
		IncludeTables:      []string{"public.users", "public.orders"},
		SingleDataFile:     true,
		Truncate:           true,
		Jobs:               4,
		ValidationDatabase: validationDB,
	}
}

// TestMigrationRestoreCleanRecreatesTargetDB verifies the migration restore phase
// (1) CLEANs+recreates the target database (Truncate=true) via psql on the TARGET
// coordinator (terminate backends -> DROP IF EXISTS -> CREATE) and (2) NO LONGER
// emits --truncate-table NOR --create-db. A fresh-DB migration restores BOTH
// metadata AND data, and --truncate-table would TRUNCATE not-yet-existing objects
// during the pre-data metadata phase (42P01) — the user-facing "clean target"
// intent is satisfied at the DB level instead (see writeMigrationEnsureTargetDB).
func TestMigrationRestoreCleanRecreatesTargetDB(t *testing.T) {
	// newMigrationOpts sets Truncate=true -> clean+recreate target DB.
	script := migrationScript(newMigrationOpts("mydb", ""))

	// (1) The clean+recreate step is rendered: terminate backends, DROP IF EXISTS,
	// then CREATE, for the target DB ("mydb").
	assert.Contains(t, script,
		validateMarkerPrefix+"clean+recreate target database (target coordinator)")
	assert.Contains(t, script,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "+
			"WHERE datname='${MIG_TARGET_DB}' AND pid <> pg_backend_pid()")
	assert.Contains(t, script,
		"DROP DATABASE IF EXISTS \\\"${MIG_TARGET_DB}\\\"")
	assert.Contains(t, script,
		"CREATE DATABASE \\\"${MIG_TARGET_DB}\\\"")
	assert.Contains(t, script, "MIG_TARGET_DB='mydb'",
		"target DB name must be a single-quoted shell literal")
	// The prepare-DB exec connects via the always-present "postgres" maintenance
	// DB (the target DB may not exist yet) and runs on the target coordinator.
	assert.Contains(t, script, "EDB_DB_B64=$(printf '%s' \"postgres\" | base64")
	assert.Contains(t, script,
		"\"${KUBECTL}\" exec -i \"${DST_COORD_POD}\"")

	// (2) The gprestore args keep redirect/include/timestamp but DROP both
	// --truncate-table (breaks the pre-data metadata phase of a fresh-DB restore)
	// and --create-db (the DB is pre-created via psql).
	assert.Contains(t, script, "'--redirect-db' 'mydb'")
	assert.Contains(t, script, "'--include-table' 'public.users'")
	assert.Contains(t, script, "'--include-table' 'public.orders'")
	assert.Contains(t, script, "--timestamp \"$7\"")
	assert.NotContains(t, script, "'--truncate-table'",
		"migration restore must NOT pass --truncate-table; it breaks the pre-data "+
			"metadata phase of a fresh-DB restore (42P01)")
	assert.NotContains(t, script, "--create-db",
		"migration restore must not pass --create-db; the DB is pre-created via psql")

	// Ordering: the prepare-DB step must precede the gprestore exec so the catalog
	// entry exists where gprestore looks.
	ensureIdx := strings.Index(script, "clean+recreate target database")
	restoreIdx := strings.Index(script, "gprestore --plugin-config")
	require.Positive(t, ensureIdx)
	require.Positive(t, restoreIdx)
	assert.Less(t, ensureIdx, restoreIdx,
		"prepare target DB must run BEFORE gprestore")
	// And the prepare-DB step must run AFTER the gpbackup-timestamp capture.
	captureIdx := strings.Index(script, "grep -oE 'Backup Timestamp = [0-9]{14}'")
	require.Positive(t, captureIdx)
	assert.Less(t, captureIdx, ensureIdx,
		"prepare target DB must run AFTER the gpbackup timestamp is captured")
}

// TestMigrationRestoreEnsuresTargetDBWhenNotTruncate verifies that WITHOUT
// Truncate the prepare-DB step is the idempotent CREATE-if-absent path (no DROP),
// and that --truncate-table is still never emitted (a fresh-DB migration restores
// metadata + data and must not truncate not-yet-existing objects).
func TestMigrationRestoreEnsuresTargetDBWhenNotTruncate(t *testing.T) {
	opts := newMigrationOpts("mydb", "")
	opts.Truncate = false
	script := migrationScript(opts)

	assert.Contains(t, script,
		validateMarkerPrefix+"ensure target database exists (target coordinator)")
	assert.Contains(t, script,
		"SELECT 1 FROM pg_database WHERE datname='${MIG_TARGET_DB}'")
	assert.Contains(t, script,
		"grep -q 1 || psql -c \"CREATE DATABASE \\\"${MIG_TARGET_DB}\\\"\"")
	// No clean path: a non-truncate migration does not DROP the target DB.
	assert.NotContains(t, script, "DROP DATABASE IF EXISTS")
	assert.NotContains(t, script, "clean+recreate target database")
	// --truncate-table is never emitted regardless of Truncate.
	assert.NotContains(t, script, "'--truncate-table'")
}

// TestMigrationRestoreTargetDBUsesRedirectDb verifies the pre-created database is
// the gprestore --redirect-db value (not the source Database) when redirectDb is
// set, matching the migrateTargetDatabase / --redirect-db target.
func TestMigrationRestoreTargetDBUsesRedirectDb(t *testing.T) {
	script := migrationScript(newMigrationOpts("mydb", "otherdb"))

	assert.Contains(t, script, "MIG_TARGET_DB='otherdb'")
	assert.Contains(t, script, "'--redirect-db' 'otherdb'")
	assert.NotContains(t, script, "MIG_TARGET_DB='mydb'")
}

// TestMigrationScriptIsValidBash runs `bash -n` over the FULL rendered migration
// script (including the prepare-target-DB heredoc), proving no quoting/syntax
// regression. It exercises BOTH branches — the clean+recreate (Truncate=true:
// terminate backends -> DROP IF EXISTS -> CREATE) and the CREATE-if-absent
// (Truncate=false) — each with a deliberately hostile DB name (a single quote AND
// a space) to prove the shellQuote-based interpolation is shell-safe (no
// injection / no broken syntax in either the DROP or the CREATE statement).
func TestMigrationScriptIsValidBash(t *testing.T) {
	shell, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not available; skipping syntax check")
	}

	// A deliberately hostile DB name: a single quote and a space. (The API
	// validates real DB names as identifiers; this only exercises shell-safety of
	// the rendering — the value never reaches a real cluster from this path.)
	for _, tc := range []struct {
		name     string
		truncate bool
		wantSQL  string
	}{
		{name: "clean_recreate", truncate: true, wantSQL: "DROP DATABASE IF EXISTS"},
		{name: "create_if_absent", truncate: false, wantSQL: "SELECT 1 FROM pg_database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := newMigrationOpts("my db'name", "")
			opts.Truncate = tc.truncate
			script := migrationScript(opts)

			// The hostile name must be emitted as a single-quoted shell literal
			// with the embedded quote escaped (shellQuote: ' -> '\''), so it cannot
			// break out.
			assert.Contains(t, script, `MIG_TARGET_DB='my db'\''name'`)
			assert.Contains(t, script, tc.wantSQL,
				"the %s branch must be rendered", tc.name)

			cmd := exec.Command(shell, "-n") //nolint:gosec // fixed shell, script via stdin
			cmd.Stdin = strings.NewReader(script)
			out, runErr := cmd.CombinedOutput()
			require.NoError(t, runErr,
				"bash -n reported a syntax error (%s): %s", tc.name, string(out))
		})
	}
}

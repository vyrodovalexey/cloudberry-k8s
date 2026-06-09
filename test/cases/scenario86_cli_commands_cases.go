package cases

// ============================================================================
// Scenario 86 — All CLI Commands (cloudberry-ctl backup ...): test-case catalog
// ============================================================================
//
// This catalog enumerates the per-command test cases (TC-86a..TC-86k) for every
// `cloudberry-ctl backup ...` subcommand. It is shared by the Scenario 86
// functional/integration/e2e suites and documents the cobra path, the operator
// REST request the command issues (method + path suffix + body), and — for the
// Job-producing commands — the gpbackup/gprestore flags the resulting Job must
// carry. The CLI talks to the operator REST API (prefix /api/v1alpha1) using an
// OIDC bearer token; the live script drives the same commands end-to-end.
// ============================================================================

// BackupCLICommandCase describes a single cloudberry-ctl backup subcommand contract.
type BackupCLICommandCase struct {
	// ID is the scenario sub-id (86a..86k).
	ID string
	// CobraArgs is the cobra subcommand path under `backup` (e.g. ["create"]).
	CobraArgs []string
	// Method is the HTTP method the command issues against the operator API.
	Method string
	// PathSuffix is appended to /api/v1alpha1/clusters/{name} (e.g. "/backups").
	PathSuffix string
	// ExpectedFlags are the gpbackup/gprestore flags the created Job must carry
	// (Job-producing commands only; nil otherwise).
	ExpectedFlags []string
	// ForbiddenFlags are flags that must NOT appear (mutual-exclusivity).
	ForbiddenFlags []string
	// Streams is true for `backup jobs logs` which streams text/plain output.
	Streams bool
	// Description documents the command's behavior.
	Description string
}

// Scenario86CLICommands is the catalog of the 11 CLI commands (TC-86a..k).
//
// The three create variants (86a) share the same method/path; they differ only
// in the request body / resulting Job args, captured in Scenario86CreateVariants.
var Scenario86CLICommands = []BackupCLICommandCase{
	{
		ID:         "86a",
		CobraArgs:  []string{"create"},
		Method:     "POST",
		PathSuffix: "/backups",
		ExpectedFlags: []string{
			"--compression-level", "--compression-type", "--jobs",
			"--include-schema", "--exclude-table", "--with-stats", "--without-globals",
		},
		ForbiddenFlags: []string{"--single-data-file", "--incremental"},
		Description:    "Create a backup; full variant maps all primary gpbackup flags.",
	},
	{
		ID:          "86b",
		CobraArgs:   []string{"list"},
		Method:      "GET",
		PathSuffix:  "/backups",
		Description: "List all backups (JSON with backups + total).",
	},
	{
		ID:          "86c",
		CobraArgs:   []string{"status"},
		Method:      "GET",
		PathSuffix:  "/backups/{timestamp}",
		Description: "Show one backup's detail by timestamp.",
	},
	{
		ID:            "86d",
		CobraArgs:     []string{"delete"},
		Method:        "DELETE",
		PathSuffix:    "/backups/{timestamp}",
		ExpectedFlags: []string{"backup-delete"},
		Description:   "Delete a backup; creates a cleanup Job.",
	},
	{
		ID:         "86e",
		CobraArgs:  []string{"restore"},
		Method:     "POST",
		PathSuffix: "/backups/{timestamp}/restore",
		ExpectedFlags: []string{
			"--timestamp", "--jobs", "--redirect-db", "--redirect-schema",
			"--create-db", "--run-analyze", "--on-error-continue",
			"--truncate-table", "--resize-cluster",
		},
		ForbiddenFlags: []string{"--include-schema", "--with-stats"},
		Description:    "Restore a backup; honors --resize-cluster.",
	},
	{
		ID:          "86f",
		CobraArgs:   []string{"schedule"},
		Method:      "GET",
		PathSuffix:  "/backups/schedule",
		Description: "Show CronJob status (schedule + nextScheduleTime).",
	},
	{
		ID:          "86g",
		CobraArgs:   []string{"schedule", "set"},
		Method:      "PATCH",
		PathSuffix:  "/backups/schedule",
		Description: "Set the backup cron schedule (PATCH {schedule}).",
	},
	{
		ID:          "86h",
		CobraArgs:   []string{"schedule", "suspend"},
		Method:      "PATCH",
		PathSuffix:  "/backups/schedule",
		Description: "Suspend the schedule (PATCH {suspend:true}).",
	},
	{
		ID:          "86i",
		CobraArgs:   []string{"schedule", "resume"},
		Method:      "PATCH",
		PathSuffix:  "/backups/schedule",
		Description: "Resume the schedule (PATCH {suspend:false}).",
	},
	{
		ID:          "86j",
		CobraArgs:   []string{"jobs"},
		Method:      "GET",
		PathSuffix:  "/backups/jobs",
		Description: "List all backup/restore/cleanup Jobs.",
	},
	{
		ID:          "86k",
		CobraArgs:   []string{"jobs", "logs"},
		Method:      "GET",
		PathSuffix:  "/backups/jobs/{job}/logs",
		Streams:     true,
		Description: "Stream a backup Job's pod logs (text/plain), with kubectl fallback.",
	},
}

// BackupCreateVariantCase describes one of the three 86a `backup create` invocations.
type BackupCreateVariantCase struct {
	// ID is the variant id (86a-1, 86a-2, 86a-3).
	ID string
	// Name is a short human-readable label.
	Name string
	// ExpectedJobArgs are the gpbackup flags the created Job's script must carry.
	ExpectedJobArgs []string
	// ForbiddenJobArgs are flags that must NOT appear (mutual-exclusivity).
	ForbiddenJobArgs []string
	// Description documents the variant.
	Description string
}

// Scenario86CreateVariants enumerates the three 86a create invocations.
var Scenario86CreateVariants = []BackupCreateVariantCase{
	{
		ID:   "86a-1",
		Name: "full (all primary flags)",
		ExpectedJobArgs: []string{
			"'--compression-level' '6'", "'--compression-type' 'zstd'",
			"'--jobs' '4'", "'--include-schema' 'public'",
			"'--exclude-table' 'public.temp'", "'--with-stats'", "'--without-globals'",
		},
		ForbiddenJobArgs: []string{"'--single-data-file'", "'--incremental'"},
		Description:      "Full backup with compression/jobs/include-schema/exclude-table/stats.",
	},
	{
		ID:               "86a-2",
		Name:             "single-data-file",
		ExpectedJobArgs:  []string{"'--single-data-file'", "'--copy-queue-size' '4'"},
		ForbiddenJobArgs: []string{"'--jobs'"},
		Description:      "single-data-file is mutually exclusive with --jobs.",
	},
	{
		ID:   "86a-3",
		Name: "incremental",
		ExpectedJobArgs: []string{
			"'--incremental'", "'--leaf-partition-data'",
		},
		Description: "Incremental backup from a prior full timestamp.",
	},
}

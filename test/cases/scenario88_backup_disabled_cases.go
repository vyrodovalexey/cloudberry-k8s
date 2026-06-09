package cases

// ============================================================================
// Scenario 88 — Backup Disabled / No Schedule: catalog
// ============================================================================
//
// This catalog enumerates the per-sub-case test cases (88a-1..88a-7, 88b-1..88b-5)
// for the already-implemented "backup disabled / no schedule" behavior. It is
// shared by the Scenario 88 functional/integration/e2e suites and documents the
// cluster precondition (backup enabled/disabled, schedule), the operator REST
// request (method + path suffix), and the expected per-cluster observable
// effects (CronJob presence, Status.CronJobName, on-demand Job presence, the
// schedule endpoint's "scheduled" flag, and the API status/error codes).
//
// WHERE THE BEHAVIOR LIVES (verified source of truth):
//   - controller internal/controller/admin_controller.go:
//       * reconcileBackup gates the whole backup reconcile on
//         cluster.Spec.Backup != nil && Enabled. When backup is DISABLED it now
//         DELETES the per-cluster CronJob "<cluster>-backup-schedule" (via the
//         shared removeBackupCronJob path) and CLEARS Status.CronJobName
//         (idempotent), and performs no S3 ConfigMap / retention cleanup.
//       * ensureBackupCronJob: an EMPTY schedule deletes/skips the CronJob and
//         clears Status.CronJobName (BuildBackupCronJob returns nil); a non-empty
//         schedule creates/updates the CronJob and sets Status.CronJobName.
//       * ensureRetentionCleanup: no-op when backup disabled or retention inactive.
//   - api internal/api/server.go:
//       * handleCreateBackup => 400 BACKUP_NOT_ENABLED when Spec.Backup == nil ||
//         !Enabled; otherwise (EVEN WITH AN EMPTY SCHEDULE) builds + creates an
//         on-demand backup Job and returns 202.
//       * handleListBackups => 200 with {cluster, enabled, backups, total,
//         lastBackup*}; "enabled" reflects backupEnabled(cluster).
//       * handleGetBackupSchedule => 200 {cluster, scheduled:false, enabled} when
//         the CronJob is NotFound; otherwise scheduled:true + nextRun + enabled.
//   - util internal/util/names.go:
//       * util.BackupCronJobName(cluster) == "<cluster>-backup-schedule".
//
// GAP NOTES (so a future reader does not "fix" the tests to a fictional shape):
//
//   GAP-1 (SA/Role are CHART-level, NOT per-cluster). The backup ServiceAccount
//     "cloudberry-backup-sa", Role "cloudberry-backup-role" and RoleBinding are
//     created ONLY by the Helm chart (deploy/helm/.../backup-rbac.yaml), gated by
//     the Helm value `backup.rbac.create`, in the OPERATOR namespace, and are
//     SHARED across every cluster. The per-cluster reconcile NEVER creates or
//     deletes them. Flipping a single cluster's `backup.enabled: false` does NOT
//     and should NOT remove them. Therefore Scenario 88a asserts ONLY the
//     per-cluster effects (no CronJob, no backup/retention Jobs,
//     Status.CronJobName empty, API create => 400, schedule => scheduled:false)
//     and NEVER a per-cluster SA/Role removal. The SA/Role toggle is governed by
//     `backup.rbac.create` and verified separately at the Helm-template level.
//
//   GAP-2 (list returns 200 + enabled:false, NOT an error). handleListBackups is
//     UNCONDITIONAL: it returns 200 with the (possibly empty) backup history plus
//     a boolean "enabled" field. It does NOT return BACKUP_NOT_ENABLED. The
//     authoritative "disabled" signals are create => 400 BACKUP_NOT_ENABLED
//     (88a-4) and schedule => scheduled:false (88a-6); the list endpoint surfaces
//     the disabled state via the "enabled":false field, but the status stays 200.
//
//   GAP-3 (disable now REMOVES the CronJob). The production fix landed: when
//     backup is flipped to disabled, reconcileBackup deletes a previously-created
//     CronJob and clears Status.CronJobName (idempotent). The clean / never-
//     enabled disabled state therefore has no CronJob and an empty
//     Status.CronJobName regardless of whether one existed before.
// ============================================================================

// BackupDisabledCase describes one Scenario 88 sub-case. It carries an explicit
// Layer + the cluster precondition + the expected per-cluster effects so the
// functional/integration/e2e suites can share a single contract.
type BackupDisabledCase struct {
	// ID is the scenario sub-id (88a-1 .. 88b-5).
	ID string
	// Layer is the verifying layer: "controller" | "api" | "cli" | "builder".
	Layer string
	// Enabled is the cluster's backup.enabled value (ignored when NilBackupSpec).
	Enabled bool
	// NilBackupSpec true => Spec.Backup == nil (88a-3), distinct from Enabled=false.
	NilBackupSpec bool
	// Schedule is the cluster's backup.schedule ("" for the empty-schedule cases).
	Schedule string
	// Method is the HTTP method for api/cli rows ("" otherwise).
	Method string
	// PathSuffix is appended to /clusters/{name} ("/backups", "/backups/schedule").
	PathSuffix string
	// ExpectStatus is the HTTP code for api/cli rows (0 otherwise).
	ExpectStatus int
	// ExpectCode is the error code (e.g. "BACKUP_NOT_ENABLED"; "" otherwise).
	ExpectCode string
	// ExpectCronJob true => the CronJob must exist after the action.
	ExpectCronJob bool
	// ExpectJob true => an on-demand backup Job must exist after the action.
	ExpectJob bool
	// Scheduled is the expected value of the schedule endpoint's "scheduled".
	Scheduled bool
	// Description documents the sub-case's behavior.
	Description string
}

// Scenario88BackupDisabledCases is the catalog of all Scenario 88 sub-cases.
var Scenario88BackupDisabledCases = []BackupDisabledCase{
	// ----- 88a — backup.enabled = false (or Spec.Backup == nil) -----
	{
		ID:            "88a-1",
		Layer:         "controller",
		Enabled:       false,
		Schedule:      "0 2 * * *",
		ExpectCronJob: false,
		Description: "Disabled backup reconcile: returns nil, NO CronJob " +
			"\"<cluster>-backup-schedule\", NO backup S3 ConfigMap, " +
			"Status.CronJobName == \"\".",
	},
	{
		ID:            "88a-2",
		Layer:         "controller",
		Enabled:       false,
		Schedule:      "0 2 * * *",
		ExpectCronJob: false,
		Description: "Disabled backup with a retention policy: ensureRetentionCleanup " +
			"is a no-op; NO cleanup Job (operation=cleanup) is created.",
	},
	{
		ID:            "88a-3",
		Layer:         "controller",
		NilBackupSpec: true,
		ExpectCronJob: false,
		Description: "Nil backup spec (Spec.Backup == nil): reconcileBackup returns " +
			"nil; NO CronJob; Status.CronJobName == \"\" (the nil-guard branch, " +
			"distinct from Enabled=false).",
	},
	{
		ID:           "88a-4",
		Layer:        "api",
		Enabled:      false,
		Method:       "POST",
		PathSuffix:   "/backups",
		ExpectStatus: 400,
		ExpectCode:   "BACKUP_NOT_ENABLED",
		Description: "POST /clusters/{name}/backups on a disabled cluster => 400 with " +
			"code BACKUP_NOT_ENABLED and message \"backup is not enabled for this cluster\".",
	},
	{
		ID:           "88a-5",
		Layer:        "api",
		Enabled:      false,
		Method:       "GET",
		PathSuffix:   "/backups",
		ExpectStatus: 200,
		// GAP-2: list is UNCONDITIONAL 200; it surfaces the disabled state via the
		// boolean "enabled":false field, NOT a BACKUP_NOT_ENABLED error. Assert the
		// REAL behavior: 200 + empty history + total 0 + "enabled":false.
		Description: "GET /clusters/{name}/backups on a disabled cluster => 200 with " +
			"\"enabled\":false, empty backups and total 0 (GAP-2: list never errors).",
	},
	{
		ID:           "88a-6",
		Layer:        "api",
		Enabled:      false,
		Method:       "GET",
		PathSuffix:   "/backups/schedule",
		ExpectStatus: 200,
		Scheduled:    false,
		Description: "GET /clusters/{name}/backups/schedule on a disabled cluster => 200 " +
			"with {scheduled:false, enabled:false} (no CronJob exists).",
	},
	{
		ID:            "88a-7",
		Layer:         "controller",
		Enabled:       true, // the re-enabled target state
		Schedule:      "0 2 * * *",
		ExpectCronJob: true,
		Description: "Re-enable TRANSITION: starting disabled (no CronJob, empty " +
			"CronJobName), setting Enabled=true AND Schedule=\"0 2 * * *\" and " +
			"reconciling again recreates the CronJob with that schedule and sets " +
			"Status.CronJobName == util.BackupCronJobName(name).",
	},

	// ----- 88b — enabled = true, schedule = "" -----
	{
		ID:            "88b-1",
		Layer:         "controller",
		Enabled:       true,
		Schedule:      "",
		ExpectCronJob: false,
		Description: "Enabled + empty schedule: the full reconcileBackup creates NO " +
			"CronJob and Status.CronJobName == \"\" (BuildBackupCronJob returns nil " +
			"for an empty schedule).",
	},
	{
		ID:           "88b-2",
		Layer:        "api",
		Enabled:      true,
		Schedule:     "",
		Method:       "POST",
		PathSuffix:   "/backups",
		ExpectStatus: 202,
		ExpectJob:    true,
		Description: "On-demand POST /clusters/{name}/backups with enabled + empty " +
			"schedule => 202 {status:\"backup started\", cluster, job, timestamp, " +
			"type}; a backup Job (operation=backup) exists. Schedule plays no role " +
			"in on-demand create.",
	},
	{
		ID:           "88b-3",
		Layer:        "cli",
		Enabled:      true,
		Schedule:     "",
		Method:       "POST",
		PathSuffix:   "/backups",
		ExpectStatus: 202,
		ExpectJob:    true,
		Description: "cloudberry-ctl backup create with enabled + empty schedule => the " +
			"same 202 envelope as 88b-2 (no schedule needed); live the on-demand Job " +
			"runs to Completion (coordinator-exec).",
	},
	{
		ID:           "88b-4",
		Layer:        "api",
		Enabled:      true,
		Schedule:     "",
		Method:       "GET",
		PathSuffix:   "/backups/schedule",
		ExpectStatus: 200,
		Scheduled:    false,
		Description: "GET /clusters/{name}/backups/schedule with enabled + empty " +
			"schedule => 200 with {scheduled:false, enabled:true} (no CronJob exists).",
	},
	{
		ID:            "88b-5",
		Layer:         "builder",
		Enabled:       true,
		Schedule:      "",
		ExpectCronJob: false,
		ExpectJob:     true,
		Description: "Builder parity: BuildBackupCronJob returns nil for an empty " +
			"schedule while BuildBackupJob returns a non-nil Job carrying the " +
			"operation=backup label (the on-demand path does not depend on a schedule).",
	},
}

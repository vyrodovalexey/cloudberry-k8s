package cases

import "github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"

// ============================================================================
// Scenario 87 — Cross-Cluster Migration (cloudberry-ctl migrate ...): catalog
// ============================================================================
//
// This catalog enumerates the per-sub-case test cases (87a..87h) for the
// already-implemented cross-cluster database migration feature. It is shared by
// the Scenario 87 functional/integration/e2e suites and documents the cobra
// path, the operator REST request the `migrate` command issues (method + path
// suffix + body), and — for the Job-producing sub-cases — the gpbackup/
// gprestore flags (or validation-script markers) the resulting Job must carry.
//
// The CLI POSTs to the SOURCE cluster's /migrate subresource
// (POST /api/v1alpha1/clusters/{source}/migrate) over an OIDC bearer token.
// The route is ADMIN-gated; a non-Admin identity is rejected with 403.
//
// THE FINAL CROSS-CLUSTER FIX (single coordinated Job). The server creates ONE
// migration Job (<source>-migration-<ts>, operation=migrate) that runs the whole
// sequence: it execs gpbackup INSIDE the source coordinator and CAPTURES the
// real gpbackup "Backup Timestamp = <14-digit>" from stdout, then execs
// gprestore `--timestamp <captured>` INSIDE the target coordinator, then runs
// validation against the target. gpbackup generates its own timestamp and
// offers no flag to pin it, so the operator timestamp can only NAME the Job —
// the restore must use the captured one or it fails with a NotFound (the bug).
// The single-Job topology propagates the captured timestamp in-process. The 202
// envelope's backupJob/restoreJob/validationJob fields all reference this one
// Job; an explicit migrationJob field names it unambiguously.
//
// NOTE on validation: the migration validation phase uses the best-effort
// row-count PROBE path (no ExpectedRowCounts). There is NO literal "checksum"
// string; its data-integrity gate is the row-count probe + invalid-index scan +
// health-check query. Assertions target the real markers ("post-restore-validate:",
// "row-count", "invalid", "SELECT 1", "post-restore-validate: passed").
// ============================================================================

// MigrationCase describes a single cloudberry-ctl migrate positive sub-case.
type MigrationCase struct {
	// ID is the scenario sub-id (87a..87e).
	ID string
	// CobraArgs is the cobra subcommand path (e.g. ["migrate"]).
	CobraArgs []string
	// Method is the HTTP method the command issues against the operator API.
	Method string
	// PathSuffix is appended to /api/v1alpha1/clusters/{source} (e.g. "/migrate").
	PathSuffix string
	// ExpectedFlags are the gpbackup/gprestore flags (or validation-script
	// markers) the produced Job must carry; nil for request-mapping cases.
	ExpectedFlags []string
	// ForbiddenFlags are flags that must NOT appear (mutual-exclusivity).
	ForbiddenFlags []string
	// Cluster identifies which cluster's Job ("source" | "target" | "").
	Cluster string
	// Description documents the sub-case's behavior.
	Description string
}

// MigrationNegativeCase describes a negative/RBAC migration test case. The
// Permission field documents the identity's permission level for the request —
// crucially the Admin requirement of the /migrate route.
type MigrationNegativeCase struct {
	// ID is the negative case id (e.g. 87f-neg1).
	ID string
	// Method / PathSuffix identify the route.
	Method     string
	PathSuffix string
	// Body is the request body POSTed to the endpoint.
	Body string
	// Permission is the identity's permission level for the request.
	Permission auth.PermissionLevel
	// ExpectStatus is the documented failure status (400/403/404).
	ExpectStatus int
	// Description documents the failure mode.
	Description string
}

// Scenario87MigrationCases is the catalog of the positive sub-cases (87a..87e).
var Scenario87MigrationCases = []MigrationCase{
	{
		ID:            "87a",
		CobraArgs:     []string{"migrate"},
		Method:        "POST",
		PathSuffix:    "/migrate",
		ExpectedFlags: nil,
		Cluster:       "",
		Description: "cloudberry-ctl migrate maps --source-cluster/--target-cluster/" +
			"--database/--tables/--truncate/--redirect-db/--redirect-schema/--jobs to " +
			"the POST /clusters/{source}/migrate JSON body (sourceCluster, targetCluster, " +
			"database, tables, truncate, redirectDb, redirectSchema, jobs).",
	},
	{
		ID:         "87b",
		CobraArgs:  []string{"migrate"},
		Method:     "POST",
		PathSuffix: "/migrate",
		Cluster:    "source",
		ExpectedFlags: []string{
			"--include-table", "public.users", "public.orders",
			"--single-data-file", "--plugin-config", "--dbname",
			// The FINAL fix: the backup phase captures the REAL gpbackup timestamp.
			"Backup Timestamp = ",
		},
		ForbiddenFlags: []string{"--incremental"},
		Description: "Migration backup phase (inside the single migration Job) runs gpbackup " +
			"with two repeated --include-table flags, --single-data-file, --plugin-config " +
			"(S3) and --dbname, and CAPTURES gpbackup's real run-time Backup Timestamp.",
	},
	{
		ID:         "87c",
		CobraArgs:  []string{"migrate"},
		Method:     "POST",
		PathSuffix: "/migrate",
		Cluster:    "target",
		ExpectedFlags: []string{
			"--timestamp", "--redirect-db", "--plugin-config", "--include-table",
		},
		// --truncate-table is NOT used by the migration restore: a fresh-DB restore
		// (metadata + data) must not TRUNCATE not-yet-existing objects during the
		// pre-data metadata phase (it aborts with 42P01). --metadata-only is also
		// absent (the migration restores both metadata AND data). The user-facing
		// --truncate "clean target" intent is honored at the DB level instead
		// (the target DB is DROPped+recreated empty before gprestore).
		ForbiddenFlags: []string{"--metadata-only", "--truncate-table"},
		Description: "Migration restore phase (inside the single migration Job) runs gprestore " +
			"with --timestamp set to the CAPTURED gpbackup timestamp (NOT the operator one), " +
			"--redirect-db, --plugin-config and --include-table, and does a FULL metadata+data " +
			"restore WITHOUT --truncate-table (the migration cleans the target at the DB level " +
			"— DROP+recreate the empty target DB — when --truncate is passed).",
	},
	{
		ID:            "87d",
		CobraArgs:     []string{"migrate"},
		Method:        "POST",
		PathSuffix:    "/migrate",
		Cluster:       "",
		ExpectedFlags: nil,
		Description: "When source and target share the same S3 bucket, migration is accepted " +
			"(202) and the single migration Job's backup + restore phases reference the same " +
			"bucket and the SOURCE folder (where gpbackup wrote).",
	},
	{
		ID:         "87e",
		CobraArgs:  []string{"migrate"},
		Method:     "POST",
		PathSuffix: "/migrate",
		Cluster:    "target",
		// Validation-script markers (NOT gpbackup flags); there is no literal
		// "checksum" — the row-count probe + invalid-index scan are the gate.
		ExpectedFlags: []string{
			"post-restore-validate:", "row-count", "invalid", "SELECT 1",
		},
		Description: "The migration Job's validation phase runs the row-count probe, the " +
			"invalid-index scan and the health-check query against the target (the migration's " +
			"row-count/integrity gate) and emits 'post-restore-validate: passed'.",
	},
}

// Scenario87MigrationNegatives is the catalog of negative/RBAC cases. Each is
// server-side; Permission documents the Admin requirement of the route.
var Scenario87MigrationNegatives = []MigrationNegativeCase{
	{
		ID:           "87f-neg1",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Source/target on different S3 buckets -> 400 (same S3 bucket).",
	},
	{
		ID:           "87f-neg2",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Missing targetCluster -> 400 (required).",
	},
	{
		ID:           "87f-neg3",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"src"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "sourceCluster == targetCluster -> 400 (must differ).",
	},
	{
		ID:           "87f-neg4",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 404,
		Description:  "Target cluster not found -> 404.",
	},
	{
		ID:           "87f-neg5",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 404,
		Description:  "Source cluster not found -> 404.",
	},
	{
		ID:           "87f-neg6",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"bad name"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Invalid database identifier -> 400.",
	},
	{
		ID:           "87f-neg7",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{bad`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Malformed JSON body -> 400.",
	},
	{
		ID:           "87g-neg8",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst"}`,
		Permission:   auth.PermissionOperator,
		ExpectStatus: 403,
		Description:  "Operator identity on POST /migrate -> 403 (Admin required).",
	},
	{
		ID:           "87h-neg9",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Source cluster not backup-enabled (no S3 destination) -> 400.",
	},
	{
		ID:           "87h-neg10",
		Method:       "POST",
		PathSuffix:   "/migrate",
		Body:         `{"targetCluster":"dst","database":"mydb"}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "Target cluster not backup-enabled (no S3 destination) -> 400.",
	},
}

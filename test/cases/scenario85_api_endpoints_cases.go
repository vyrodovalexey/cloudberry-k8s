package cases

import "github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"

// ============================================================================
// Scenario 85 — All Backup REST API Endpoints: test-case catalog
// ============================================================================
//
// This catalog enumerates the test cases (TC-85a..TC-85g + negatives) for the
// operator's 7 backup REST API endpoints, used by the Scenario 85
// functional/integration/e2e suites and the live exercise script. Each endpoint
// has a required RBAC permission, an expected success status, and (for the two
// write endpoints) the set of gpbackup/gprestore flags the request must map to.
// ============================================================================

// BackupAPIEndpointCase describes a single backup REST API endpoint contract.
type BackupAPIEndpointCase struct {
	// ID is the scenario sub-id (85a..85g).
	ID string
	// Method is the HTTP method (GET/POST/DELETE).
	Method string
	// PathSuffix is appended to /api/v1alpha1/clusters/{name}/backups.
	PathSuffix string
	// RequiredPermission is the minimum auth permission the route enforces.
	RequiredPermission auth.PermissionLevel
	// SuccessStatus is the HTTP status returned on the happy path.
	SuccessStatus int
	// ExpectedFlags are the gpbackup/gprestore flags the created Job must carry
	// (write endpoints only; nil for read endpoints).
	ExpectedFlags []string
	// ForbiddenFlags are flags that must NOT appear (mutual-exclusivity).
	ForbiddenFlags []string
	// Description documents the endpoint's behavior.
	Description string
}

// BackupAPINegativeCase describes a negative/RBAC test case for an endpoint.
type BackupAPINegativeCase struct {
	// ID is the negative case id (e.g. TC-85b-neg1).
	ID string
	// Endpoint is the parent endpoint sub-id (85a..85g).
	Endpoint string
	// Method / PathSuffix identify the route.
	Method     string
	PathSuffix string
	// Body is the request body (empty for GET/DELETE).
	Body string
	// Permission is the identity's permission level for the request.
	Permission auth.PermissionLevel
	// ExpectStatus is the documented failure status (400/403/404).
	ExpectStatus int
	// Description documents the failure mode.
	Description string
}

// Scenario85BackupAPIEndpoints is the catalog of the 7 endpoints (TC-85a..g).
var Scenario85BackupAPIEndpoints = []BackupAPIEndpointCase{
	{
		ID:                 "85a",
		Method:             "GET",
		PathSuffix:         "/backups",
		RequiredPermission: auth.PermissionBasic,
		SuccessStatus:      200,
		Description:        "List backups from status.BackupHistory; response has backups + total.",
	},
	{
		ID:                 "85b",
		Method:             "POST",
		PathSuffix:         "/backups",
		RequiredPermission: auth.PermissionOperator,
		SuccessStatus:      202,
		ExpectedFlags: []string{
			"--dbname",
			"--single-data-file",
			"--copy-queue-size",
			"--include-schema",
			"--exclude-table",
			"--leaf-partition-data", // GAP-B: emitted on a FULL backup too.
			"--with-stats",
			"--without-globals",
		},
		ForbiddenFlags: []string{"--jobs", "--incremental"},
		Description:    "Create a FULL backup Job whose args match every gpbackupOption.",
	},
	{
		ID:                 "85c",
		Method:             "GET",
		PathSuffix:         "/backups/{timestamp}",
		RequiredPermission: auth.PermissionBasic,
		SuccessStatus:      200,
		Description:        "Get details for a specific backup timestamp.",
	},
	{
		ID:                 "85d",
		Method:             "DELETE",
		PathSuffix:         "/backups/{timestamp}",
		RequiredPermission: auth.PermissionAdmin,
		SuccessStatus:      202,
		ExpectedFlags:      []string{"backup-delete"},
		Description:        "Create a cleanup Job (operation=cleanup) running gpbackman backup-delete.",
	},
	{
		ID:                 "85e",
		Method:             "POST",
		PathSuffix:         "/backups/{timestamp}/restore",
		RequiredPermission: auth.PermissionAdmin,
		SuccessStatus:      202,
		ExpectedFlags: []string{
			"--timestamp",
			"--jobs",
			"--redirect-db",
			"--create-db",
			"--with-globals",
			"--run-analyze",
			"--on-error-continue",
			"--truncate-table",
			"--data-only",
			"--resize-cluster",
		},
		ForbiddenFlags: []string{"--metadata-only", "--with-stats"},
		Description:    "Create a restore Job whose args match every gprestoreOption.",
	},
	{
		ID:                 "85f",
		Method:             "GET",
		PathSuffix:         "/backups/jobs",
		RequiredPermission: auth.PermissionBasic,
		SuccessStatus:      200,
		Description:        "List backup/restore/cleanup Job statuses (jobs + total).",
	},
	{
		ID:                 "85g",
		Method:             "GET",
		PathSuffix:         "/backups/schedule",
		RequiredPermission: auth.PermissionBasic,
		SuccessStatus:      200,
		Description:        "Get the CronJob status + computed nextScheduleTime.",
	},
}

// Scenario85BackupAPINegatives is the catalog of negative/RBAC cases.
var Scenario85BackupAPINegatives = []BackupAPINegativeCase{
	{
		ID:           "TC-85a-neg1",
		Endpoint:     "85a",
		Method:       "GET",
		PathSuffix:   "/backups",
		Permission:   auth.PermissionBasic,
		ExpectStatus: 404,
		Description:  "List against a missing cluster -> 404.",
	},
	{
		ID:           "TC-85b-neg1",
		Endpoint:     "85b",
		Method:       "POST",
		PathSuffix:   "/backups",
		Body:         `{"type":"bogus"}`,
		Permission:   auth.PermissionOperator,
		ExpectStatus: 400,
		Description:  "Invalid backup type -> 400.",
	},
	{
		ID:           "TC-85b-neg2",
		Endpoint:     "85b",
		Method:       "POST",
		PathSuffix:   "/backups",
		Body:         `{"databases":["bad name"]}`,
		Permission:   auth.PermissionOperator,
		ExpectStatus: 400,
		Description:  "Invalid database identifier -> 400.",
	},
	{
		ID:           "TC-85b-neg3",
		Endpoint:     "85b",
		Method:       "POST",
		PathSuffix:   "/backups",
		Body:         `{not-json`,
		Permission:   auth.PermissionOperator,
		ExpectStatus: 400,
		Description:  "Malformed JSON -> 400.",
	},
	{
		ID:           "TC-85b-neg6",
		Endpoint:     "85b",
		Method:       "POST",
		PathSuffix:   "/backups",
		Body:         `{"type":"full"}`,
		Permission:   auth.PermissionBasic,
		ExpectStatus: 403,
		Description:  "Basic identity on POST backups -> 403 (Operator required).",
	},
	{
		ID:           "TC-85c-neg2",
		Endpoint:     "85c",
		Method:       "GET",
		PathSuffix:   "/backups/bk-1",
		Permission:   auth.PermissionBasic,
		ExpectStatus: 400,
		Description:  "Invalid (non-14-digit) timestamp -> 400.",
	},
	{
		ID:           "TC-85d-neg3",
		Endpoint:     "85d",
		Method:       "DELETE",
		PathSuffix:   "/backups/20260101010101",
		Permission:   auth.PermissionOperator,
		ExpectStatus: 403,
		Description:  "Operator identity on DELETE -> 403 (Admin required).",
	},
	{
		ID:           "TC-85e-neg1",
		Endpoint:     "85e",
		Method:       "POST",
		PathSuffix:   "/backups/20260101010101/restore",
		Body:         `{"gprestoreOptions":{"dataOnly":true,"metadataOnly":true}}`,
		Permission:   auth.PermissionAdmin,
		ExpectStatus: 400,
		Description:  "dataOnly + metadataOnly both true -> 400 (mutual exclusivity).",
	},
	{
		ID:           "TC-85e-neg6",
		Endpoint:     "85e",
		Method:       "POST",
		PathSuffix:   "/backups/20260101010101/restore",
		Body:         `{}`,
		Permission:   auth.PermissionOperator,
		ExpectStatus: 403,
		Description:  "Operator identity on restore -> 403 (Admin required).",
	},
}

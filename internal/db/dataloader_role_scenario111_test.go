package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Scenario 111 — SE.6: EnsureDataLoaderRole (dedicated minimal-privilege role)
//
// Catalog IDs: 111-SE6-U (dedicated role gets ONLY pxf protocol grants;
// NOSUPERUSER least-privilege; gpadmin fallback preserved when opt-in unset).
//
// These mirror the existing SetupExporterRole / SetupPXFExtensions pgxmock
// tests: a mock PostgreSQL backend answers the existence probe + Exec
// statements, and a recording responder lets us assert exactly which SQL was
// (or was NOT) executed.
// ============================================================================

// TestEnsureDataLoaderRole_NoOpGpadmin proves the gpadmin path is unchanged:
// the dedicated-role flow is a no-op (NO SQL executed at all) so the existing
// SetupPXFExtensions RP.11 grant to gpadmin remains the sole behavior.
// (111-SE6-U honesty: gpadmin path unchanged when unset.)
func TestEnsureDataLoaderRole_NoOpGpadmin(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		return execResponse("OK")
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "gpadmin")
	require.NoError(t, err)

	// No-op: NOT even the existence probe is issued.
	assert.Empty(t, *queries, "gpadmin must be a no-op: no SQL executed")
	assert.Zero(t, countQueriesContaining(*queries, "CREATE ROLE"))
	assert.Zero(t, countQueriesContaining(*queries, "GRANT"))
	assert.Zero(t, countQueriesContaining(*queries, "pg_roles"))
}

// TestEnsureDataLoaderRole_NoOpEmpty proves the empty role name is a no-op too
// (back-compat: an unset DataLoaderRole keeps the gpadmin behavior).
func TestEnsureDataLoaderRole_NoOpEmpty(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		return execResponse("OK")
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "")
	require.NoError(t, err)

	assert.Empty(t, *queries, "empty role name must be a no-op: no SQL executed")
}

// TestEnsureDataLoaderRole_CreateAndGrant covers the core SE.6 behavior: when a
// dedicated role is absent it is CREATEd as a minimal-privilege LOGIN role, then
// GRANTed ONLY the pxf protocol privileges (SELECT + INSERT). It asserts the
// mock saw the CREATE plus BOTH GRANTs, that the identifier is sanitized, and
// that the least-privilege attributes (NOSUPERUSER/NOCREATEDB/NOCREATEROLE) are
// present in the CREATE statement. (111-SE6-U)
func TestEnsureDataLoaderRole_CreateAndGrant(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			// Role absent → CREATE branch.
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return execResponse("CREATE ROLE")
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "cb_dataload")
	require.NoError(t, err)

	// CREATE ROLE issued exactly once, with the sanitized identifier.
	assert.Equal(t, 1, countQueriesContaining(*queries, `CREATE ROLE "cb_dataload"`),
		"role must be created with a sanitized identifier")

	// Least-privilege: the CREATE statement carries all three NO* attributes
	// plus LOGIN — and is NOT a superuser.
	var createStmt string
	for _, q := range *queries {
		if strings.Contains(q, "CREATE ROLE") {
			createStmt = q
			break
		}
	}
	require.NotEmpty(t, createStmt)
	assert.Contains(t, createStmt, "NOSUPERUSER")
	assert.Contains(t, createStmt, "NOCREATEDB")
	assert.Contains(t, createStmt, "NOCREATEROLE")
	assert.Contains(t, createStmt, "LOGIN")
	// Least-privilege: SUPERUSER only ever appears as part of NOSUPERUSER, never
	// as a bare grant.
	assert.NotContains(t, strings.ReplaceAll(createStmt, "NOSUPERUSER", ""), "SUPERUSER",
		"must never grant SUPERUSER")

	// ONLY the two pxf protocol grants are issued (SELECT + INSERT on PROTOCOL
	// pxf) — no other GRANT.
	assert.Equal(t, 1,
		countQueriesContaining(*queries, `GRANT SELECT ON PROTOCOL pxf TO "cb_dataload"`))
	assert.Equal(t, 1,
		countQueriesContaining(*queries, `GRANT INSERT ON PROTOCOL pxf TO "cb_dataload"`))
	assert.Equal(t, 2, countQueriesContaining(*queries, "GRANT"),
		"exactly two GRANTs (the pxf protocol privileges) and nothing else")
	// Every GRANT is ON PROTOCOL pxf (no unrelated grant leaks in).
	assert.Equal(t, countQueriesContaining(*queries, "GRANT"),
		countQueriesContaining(*queries, "ON PROTOCOL pxf"),
		"every grant must be on PROTOCOL pxf")
}

// TestEnsureDataLoaderRole_RoleExistsSkipsCreate proves idempotency: when the
// role already exists the CREATE is skipped, but the protocol GRANTs are still
// (re-)applied. (111-SE6-U)
func TestEnsureDataLoaderRole_RoleExistsSkipsCreate(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			// Role already present → CREATE skipped.
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"t"})
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "cb_dataload")
	require.NoError(t, err)

	assert.Zero(t, countQueriesContaining(*queries, "CREATE ROLE"),
		"CREATE must be skipped when the role already exists")
	assert.Equal(t, 2, countQueriesContaining(*queries, "GRANT"),
		"protocol GRANTs are still applied for an existing role")
}

// TestEnsureDataLoaderRole_ProbeError proves a hard connectivity/probe failure
// is surfaced (mirrors SetupExporterRole's existence-check-error contract).
func TestEnsureDataLoaderRole_ProbeError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("server closed the connection")
	})
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "cb_dataload")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probing data-loader role existence")
}

// TestEnsureDataLoaderRole_GrantFailureNonFatal proves the protocol GRANTs are
// best-effort: the role is created but the GRANTs error (e.g. PROTOCOL pxf
// absent on a stub image) and the method still returns nil. (111-SE6-U)
func TestEnsureDataLoaderRole_GrantFailureNonFatal(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return execResponse("CREATE ROLE")
		case strings.Contains(query, "GRANT"):
			return errorResponseMsg(`protocol "pxf" does not exist`)
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "cb_dataload")
	require.NoError(t, err, "GRANT failure must be non-fatal")
	// Both GRANTs were still attempted best-effort.
	assert.Equal(t, 2, countQueriesContaining(*queries, "GRANT"))
}

// TestEnsureDataLoaderRole_CreateFailureNonFatal proves a benign CREATE failure
// (e.g. a racing concurrent create) is tolerated: the method logs and continues
// to the best-effort GRANTs, returning nil.
func TestEnsureDataLoaderRole_CreateFailureNonFatal(t *testing.T) {
	responder, queries := recordingResponder(func(query string) []byte {
		switch {
		case strings.Contains(query, "SELECT EXISTS"):
			return singleRowResponseTyped([]fieldDesc{boolField("exists")}, []string{"f"})
		case strings.Contains(query, "CREATE ROLE"):
			return errorResponseMsg("role already exists")
		case strings.Contains(query, "GRANT"):
			return execResponse("GRANT")
		default:
			return execResponse("OK")
		}
	})
	client, cleanup := newMockPgxClient(t, responder)
	defer cleanup()

	err := client.EnsureDataLoaderRole(context.Background(), "cb_dataload")
	require.NoError(t, err, "a benign CREATE failure must be non-fatal")
	assert.Equal(t, 2, countQueriesContaining(*queries, "GRANT"),
		"GRANTs are still attempted after a tolerated CREATE failure")
}

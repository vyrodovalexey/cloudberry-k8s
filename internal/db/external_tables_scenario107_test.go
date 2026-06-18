package db

// Scenario 107 — ListExternalTables (P.15 backing query).
//
// These tests exercise the read-only, observed-only honesty contract of
// pgxClient.ListExternalTables against the in-process PostgreSQL mock used by
// the rest of the pgxClient suite. They mirror the ListPXFExtensions tests:
//   - both an external (pg_exttable) row and a foreign (pg_foreign_table) row
//     are parsed into ExternalTableInfo with the correct kind/server;
//   - zero rows → an empty (nil) slice and no error (reachable DB, nothing
//     present) — never synthesized;
//   - a query/connectivity error is SURFACED (wrapped) so the caller treats the
//     probe as UNOBSERVABLE rather than as "none present";
//   - a row-scan mismatch is likewise surfaced.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// extTableFields are the four columns the listExternalTablesQuery projects.
var extTableFields = []string{"schema", "name", "kind", "server"}

// TestPgxClient_ListExternalTables_ExternalAndForeign covers 107-P15-F (query
// path): a reachable DB returns one external table (no server) and one foreign
// table (with its backing server), each parsed into the right kind/server.
func TestPgxClient_ListExternalTables_ExternalAndForeign(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return multiRowResponse(extTableFields, [][]string{
			{"public", "ext_events", "external", ""},
			{"public", "foreign_loadfdw", "foreign", "s3srv"},
		})
	})
	defer cleanup()

	tables, err := client.ListExternalTables(context.Background())
	require.NoError(t, err)
	require.Len(t, tables, 2)

	assert.Equal(t, ExternalTableInfo{
		Schema: "public", Name: "ext_events", Kind: "external", Server: "",
	}, tables[0])
	assert.Equal(t, ExternalTableInfo{
		Schema: "public", Name: "foreign_loadfdw", Kind: "foreign", Server: "s3srv",
	}, tables[1])
}

// TestPgxClient_ListExternalTables_None covers 107-P15-EMPTY (query path): a
// reachable DB with no external/foreign tables → an empty slice + nil error.
// The probe was OBSERVABLE and honestly reports nothing.
func TestPgxClient_ListExternalTables_None(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return emptyRowResponse(extTableFields)
	})
	defer cleanup()

	tables, err := client.ListExternalTables(context.Background())
	require.NoError(t, err)
	assert.Empty(t, tables)
}

// TestPgxClient_ListExternalTables_QueryError covers 107-P15-DBERR (query path):
// a query/connectivity error is SURFACED (wrapped) so the caller treats the
// probe as UNOBSERVABLE (observed ABSENT) rather than "none present".
func TestPgxClient_ListExternalTables_QueryError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		return errorResponseMsg("server closed the connection")
	})
	defer cleanup()

	tables, err := client.ListExternalTables(context.Background())
	require.Error(t, err)
	assert.Nil(t, tables)
	assert.Contains(t, err.Error(), "querying catalog for external/foreign tables")
}

// TestPgxClient_ListExternalTables_ScanError covers 107-P15-DBERR (row-scan
// path): a row with the wrong column count triggers a scan error, surfaced
// (wrapped) so the caller treats the probe as UNOBSERVABLE rather than empty.
func TestPgxClient_ListExternalTables_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		// Two columns where the scan expects four → scan error.
		return multiRowResponse([]string{"schema", "name"}, [][]string{{"public", "x"}})
	})
	defer cleanup()

	tables, err := client.ListExternalTables(context.Background())
	require.Error(t, err)
	assert.Nil(t, tables)
	assert.Contains(t, err.Error(), "scanning external/foreign table row")
}

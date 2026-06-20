package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Scenario 120 — Usage Reporting (C.11) DB-layer tests.
//
// GetUsageReport now fires TWO queries through the mock PostgreSQL server
// (newMockPgxClient): the per-DATABASE usage query (pg_database_size) AND, as a
// best-effort enrichment, the per-TABLE usage query (usageReportTablesQuery,
// pg_total_relation_size). Responses are routed by SQL content so each query
// gets its own canned rows. The mock client's connected database is "testdb"
// (see newMockPgxClient), so the per-table breakdown is attached to the
// "testdb" entry only — honestly, the pool is single-database.
// ============================================================================

// usageReportDBFields mirrors the per-database usage projection:
// datname, size_bytes, size_human, connections.
func usageReportDBFields() []fieldDesc {
	return []fieldDesc{
		textField("datname"), int8Field("size_bytes"),
		textField("size_human"), int8Field("connections"),
	}
}

// usageReportTableFields mirrors usageReportTablesQuery's projection:
// schema, tbl, size_bytes, size_human.
func usageReportTableFields() []fieldDesc {
	return []fieldDesc{
		textField("schema"), textField("tbl"),
		int8Field("size_bytes"), textField("size_human"),
	}
}

// isUsageReportTablesQuery reports whether the SQL is the per-table enrichment
// query (it joins pg_class to pg_namespace via pg_total_relation_size), as
// opposed to the per-database usage query (pg_database_size).
func isUsageReportTablesQuery(query string) bool {
	return strings.Contains(query, "pg_total_relation_size") &&
		strings.Contains(query, "pg_class")
}

// 120-C11-DB-pertable — happy path: the per-db usage query returns DB rows AND
// the per-table query returns table rows. Assert the UsageReportEntry has the
// Month label plus the per-db fields, AND the connected-db ("testdb") entry's
// Tables slice is populated (schema, table, sizeBytes, sizeHuman), size-desc.
func TestScenario120_GetUsageReport_PerTable(t *testing.T) {
	// Arrange: route the two queries by SQL content.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isUsageReportTablesQuery(query) {
			return multiRowResponseTyped(usageReportTableFields(), [][]string{
				{"public", "orders", "2147483648", "2 GB"},
				{"public", "lineitem", "1073741824", "1 GB"},
			})
		}
		return multiRowResponseTyped(usageReportDBFields(), [][]string{
			{"testdb", "3221225472", "3 GB", "10"},
			{"postgres", "8388608", "8 MB", "2"},
		})
	})
	defer cleanup()

	// Act
	entries, err := client.GetUsageReport(context.Background(), "2026-05")

	// Assert: per-database content present, month label stamped.
	require.NoError(t, err)
	require.Len(t, entries, 2)

	connected := entries[0]
	assert.Equal(t, "2026-05", connected.Month)
	assert.Equal(t, "testdb", connected.Database)
	assert.Equal(t, int64(3221225472), connected.SizeBytes)
	assert.Equal(t, "3 GB", connected.SizeHuman)
	assert.Equal(t, int64(10), connected.Connections)

	// growth/queryCount remain an honest 0 (on-demand, no persisted history).
	assert.Equal(t, int64(0), connected.GrowthBytes)
	assert.Empty(t, connected.GrowthHuman)
	assert.Equal(t, int64(0), connected.QueryCount)

	// Per-table breakdown attached to the connected-db entry, size-desc.
	require.Len(t, connected.Tables, 2)
	assert.Equal(t, "public", connected.Tables[0].Schema)
	assert.Equal(t, "orders", connected.Tables[0].Table)
	assert.Equal(t, int64(2147483648), connected.Tables[0].SizeBytes)
	assert.Equal(t, "2 GB", connected.Tables[0].SizeHuman)
	assert.Equal(t, "lineitem", connected.Tables[1].Table)
	assert.Equal(t, int64(1073741824), connected.Tables[1].SizeBytes)

	// The non-connected database carries no per-table breakdown (honest empty):
	// the pool is single-database, so we cannot size tables we are not in.
	assert.Equal(t, "postgres", entries[1].Database)
	assert.Empty(t, entries[1].Tables)
}

// 120-C11-DB-tables-fallback — the per-table enrichment query errors (42P01
// undefined relation, 42703 undefined column, or a generic failure). The report
// must STILL return its per-database entries with Tables left empty and NO error
// surfaced — an honest best-effort fallback, never failing the whole report.
func TestScenario120_GetUsageReport_TablesFallback(t *testing.T) {
	tests := []struct {
		name string
		code string
		msg  string
	}{
		{"undefined relation 42P01", "42P01",
			`relation "pg_class" does not exist`},
		{"undefined column 42703", "42703",
			`column "size_bytes" does not exist`},
		{"generic error", "",
			"per-table query failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: per-db usage query succeeds, per-table enrichment errors.
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				if isUsageReportTablesQuery(query) {
					if tt.code == "" {
						return errorResponseMsg(tt.msg)
					}
					return errorResponseWithCode(tt.code, tt.msg)
				}
				return multiRowResponseTyped(usageReportDBFields(), [][]string{
					{"testdb", "1073741824", "1 GB", "5"},
					{"postgres", "8388608", "8 MB", "1"},
				})
			})
			defer cleanup()

			// Act
			entries, err := client.GetUsageReport(context.Background(), "2026-05")

			// Assert: honest fallback — entries still returned, Tables empty.
			require.NoError(t, err,
				"a per-table query failure must NOT fail the whole report")
			require.Len(t, entries, 2)
			assert.Equal(t, "testdb", entries[0].Database)
			assert.Empty(t, entries[0].Tables,
				"Tables must be honestly empty when the per-table query fails")
			assert.Empty(t, entries[1].Tables)
		})
	}
}

// 120-C11-DB-tables-scan-error — the per-table query returns a row whose
// size_bytes is non-numeric, triggering a scan error inside the enrichment.
// The enrichment swallows it (honest empty Tables) without failing the report.
func TestScenario120_GetUsageReport_TablesScanError(t *testing.T) {
	// Arrange: per-table query returns a bad row (non-numeric size_bytes).
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isUsageReportTablesQuery(query) {
			return multiRowResponseTyped(usageReportTableFields(), [][]string{
				{"public", "orders", "not-a-number", "2 GB"},
			})
		}
		return multiRowResponseTyped(usageReportDBFields(), [][]string{
			{"testdb", "1073741824", "1 GB", "5"},
		})
	})
	defer cleanup()

	// Act
	entries, err := client.GetUsageReport(context.Background(), "2026-05")

	// Assert: report still returns, Tables honestly empty.
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "testdb", entries[0].Database)
	assert.Empty(t, entries[0].Tables)
}

// 120-C11-DB-tables-no-connected-match — the per-db usage rows do NOT include
// the connected database ("testdb"); the enrichment finds no matching entry and
// leaves every entry's Tables empty (no fabrication, no mis-attribution).
func TestScenario120_GetUsageReport_TablesNoConnectedMatch(t *testing.T) {
	// Arrange: rows for other databases only; per-table query returns rows.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isUsageReportTablesQuery(query) {
			return multiRowResponseTyped(usageReportTableFields(), [][]string{
				{"public", "orders", "2147483648", "2 GB"},
			})
		}
		return multiRowResponseTyped(usageReportDBFields(), [][]string{
			{"postgres", "8388608", "8 MB", "2"},
			{"otherdb", "4194304", "4 MB", "1"},
		})
	})
	defer cleanup()

	// Act
	entries, err := client.GetUsageReport(context.Background(), "2026-05")

	// Assert: no entry matches the connected db, so none carry Tables.
	require.NoError(t, err)
	require.Len(t, entries, 2)
	for _, e := range entries {
		assert.Empty(t, e.Tables,
			"no connected-db match must leave every entry's Tables empty")
	}
}

// 120-C11-DB-no-connected-db — when the client has no configured database name
// (config.Database == ""), attachUsageReportTables is a no-op: it must not even
// run the per-table query, leaving every entry's Tables empty. This exercises
// the connected-db guard directly.
func TestScenario120_GetUsageReport_NoConnectedDatabase(t *testing.T) {
	// Arrange: a row exists for "testdb", but the client's config.Database is
	// blanked so the connected-db guard short-circuits the enrichment.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isUsageReportTablesQuery(query) {
			t.Error("per-table query must not run when no database is configured")
			return errorResponseMsg("unexpected per-table query")
		}
		return multiRowResponseTyped(usageReportDBFields(), [][]string{
			{"testdb", "1073741824", "1 GB", "5"},
		})
	})
	defer cleanup()
	client.config.Database = ""

	// Act
	entries, err := client.GetUsageReport(context.Background(), "2026-05")

	// Assert: report returns, no entry carries a per-table breakdown.
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Empty(t, entries[0].Tables)
}

// 120-C11-DB-month-label — the month argument is stamped as a scope label on
// every per-database entry (the report is scoped/labeled by month, not parsed).
func TestScenario120_GetUsageReport_MonthLabel(t *testing.T) {
	tests := []struct {
		name  string
		month string
	}{
		{"explicit month", "2026-05"},
		{"empty month", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				if isUsageReportTablesQuery(query) {
					return multiRowResponseTyped(usageReportTableFields(), [][]string{
						{"public", "orders", "1024", "1 KB"},
					})
				}
				return multiRowResponseTyped(usageReportDBFields(), [][]string{
					{"testdb", "1073741824", "1 GB", "10"},
					{"postgres", "8388608", "8 MB", "2"},
				})
			})
			defer cleanup()

			// Act
			entries, err := client.GetUsageReport(context.Background(), tt.month)

			// Assert: the month label is stamped on every entry.
			require.NoError(t, err)
			require.Len(t, entries, 2)
			for _, e := range entries {
				assert.Equal(t, tt.month, e.Month,
					"month must be stamped as a scope label on each entry")
			}
		})
	}
}

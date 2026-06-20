package db

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Scenario 119 — Storage API DB-layer tests (P.2 GetTables, P.3 GetTableDetails
// bug fix + index sizes). These exercise the pgx-level query paths through the
// mock PostgreSQL server (newMockPgxClient), routing per-query by SQL content
// since GetTables fires two queries (table storage + skew enrichment) and
// GetTableDetails fires three (detail + skew + index sizes).
// ============================================================================

// tableStorageFields mirrors the projection of tableStorageQuery:
// schemaname, relname, size_bytes, size_human, row_count, bloat_percent.
func tableStorageFields() []fieldDesc {
	return []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("size_bytes"), textField("size_human"),
		int8Field("row_count"), int4Field("bloat_percent"),
	}
}

// tableSkewFields mirrors the projection of tableSkewQuery:
// skcnamespace, skcrelname, skccoeff (float8).
func tableSkewFields() []fieldDesc {
	return []fieldDesc{
		textField("skcnamespace"), textField("skcrelname"), float8Field("skccoeff"),
	}
}

// indexSizeFields mirrors the projection of indexSizesQuery:
// index_name, size_bytes, size_human.
func indexSizeFields() []fieldDesc {
	return []fieldDesc{
		textField("index_name"), int8Field("size_bytes"), textField("size_human"),
	}
}

// isSkewQuery reports whether the SQL is the gp_toolkit skew enrichment query.
func isSkewQuery(query string) bool {
	return strings.Contains(query, "gp_skew_coefficients")
}

// isIndexQuery reports whether the SQL is the per-index size query.
func isIndexQuery(query string) bool {
	return strings.Contains(query, "pg_index")
}

// 119b-DB-GetTables-ok — happy path: the primary query returns two rows and the
// gp_toolkit skew enrichment populates SkewPercent by (schema, table). Assert
// every scanned field (schema, table, sizeBytes, sizeHuman, bloatPercent,
// skewPercent, rowCount).
func TestScenario119_GetTables_OK(t *testing.T) {
	// Arrange
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isSkewQuery(query) {
			return multiRowResponseTyped(tableSkewFields(), [][]string{
				{"public", "events", "42"},
				{"public", "users", "7"},
			})
		}
		return multiRowResponseTyped(tableStorageFields(), [][]string{
			{"public", "events", "2147483648", "2 GB", "5000000", "55"},
			{"public", "users", "1073741824", "1 GB", "1000000", "10"},
		})
	})
	defer cleanup()

	// Act
	tables, err := client.GetTables(context.Background())

	// Assert
	require.NoError(t, err)
	require.Len(t, tables, 2)

	events := tables[0]
	assert.Equal(t, "public", events.Schema)
	assert.Equal(t, "events", events.Table)
	assert.Equal(t, int64(2147483648), events.SizeBytes)
	assert.Equal(t, "2 GB", events.SizeHuman)
	assert.Equal(t, int32(55), events.BloatPercent)
	assert.Equal(t, int32(42), events.SkewPercent) // enriched from gp_toolkit
	assert.Equal(t, int64(5000000), events.RowCount)

	users := tables[1]
	assert.Equal(t, "users", users.Table)
	assert.Equal(t, int32(10), users.BloatPercent)
	assert.Equal(t, int32(7), users.SkewPercent)
	assert.Equal(t, int64(1000000), users.RowCount)
}

// 119b-DB-GetTables-skew-fallback — gp_skew_coefficients errors with 42P01
// (undefined table): GetTables must NOT fail and must honestly leave skew 0
// (no fabrication), while still returning the base rows.
func TestScenario119_GetTables_SkewFallback(t *testing.T) {
	tests := []struct {
		name string
		code string
		msg  string
	}{
		{"undefined table 42P01", "42P01",
			`relation "gp_toolkit.gp_skew_coefficients" does not exist`},
		{"undefined column 42703", "42703",
			`column "skccoeff" does not exist`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: primary query succeeds, skew enrichment errors.
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				if isSkewQuery(query) {
					return errorResponseWithCode(tt.code, tt.msg)
				}
				return multiRowResponseTyped(tableStorageFields(), [][]string{
					{"public", "events", "2048", "2 KB", "100", "30"},
				})
			})
			defer cleanup()

			// Act
			tables, err := client.GetTables(context.Background())

			// Assert: honest skip, no error, skew stays 0.
			require.NoError(t, err,
				"missing gp_toolkit view must be an honest skip, not an error")
			require.Len(t, tables, 1)
			assert.Equal(t, "events", tables[0].Table)
			assert.Equal(t, int32(30), tables[0].BloatPercent)
			assert.Equal(t, int32(0), tables[0].SkewPercent,
				"skew must be honestly 0 when gp_toolkit is absent")
		})
	}
}

// 119b-DB-GetTables-error — the primary table storage query errors: GetTables
// returns a wrapped error (no skew enrichment is attempted).
func TestScenario119_GetTables_Error(t *testing.T) {
	// Arrange: every query errors; the primary query fails first.
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("table storage query failed")
	})
	defer cleanup()

	// Act
	tables, err := client.GetTables(context.Background())

	// Assert
	require.Error(t, err)
	assert.Nil(t, tables)
	assert.Contains(t, err.Error(), "querying tables")
}

// 119b-DB-GetTables-scan-error — a row with a non-numeric size_bytes triggers a
// scan error, surfaced as a wrapped "scanning table storage row" error.
func TestScenario119_GetTables_ScanError(t *testing.T) {
	// Arrange: size_bytes is declared int8 but a non-numeric value is returned.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isSkewQuery(query) {
			return multiRowResponseTyped(tableSkewFields(), nil)
		}
		return multiRowResponseTyped(tableStorageFields(), [][]string{
			{"public", "events", "not-a-number", "2 GB", "100", "10"},
		})
	})
	defer cleanup()

	// Act
	_, err := client.GetTables(context.Background())

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning table storage row")
}

// 119b-DB-GetTables-empty — the primary query returns no rows: GetTables returns
// an empty (nil) slice and no error; skew enrichment over an empty set is a
// no-op.
func TestScenario119_GetTables_Empty(t *testing.T) {
	// Arrange
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		if isSkewQuery(query) {
			return multiRowResponseTyped(tableSkewFields(), nil)
		}
		return multiRowResponseTyped(tableStorageFields(), nil)
	})
	defer cleanup()

	// Act
	tables, err := client.GetTables(context.Background())

	// Assert
	require.NoError(t, err)
	assert.Empty(t, tables)
}

// 119c-DB-GetTableDetails-fix — the bug-fix pin: GetTableDetails must populate
// BOTH BloatPercent AND SkewPercent (skew enriched from gp_toolkit), and
// IndexSizes from the per-index query.
func TestScenario119_GetTableDetails_Fix(t *testing.T) {
	detailFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("size_bytes"), textField("size_human"),
		int8Field("row_count"), int4Field("bloat_percent"),
		textField("last_vacuum"), textField("last_analyze"),
	}
	// Arrange: route the three queries by content.
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case isSkewQuery(query):
			return multiRowResponseTyped(tableSkewFields(), [][]string{
				{"public", "users", "37"},
			})
		case isIndexQuery(query):
			return multiRowResponseTyped(indexSizeFields(), [][]string{
				{"users_pkey", "1048576", "1 MB"},
				{"users_email_idx", "524288", "512 kB"},
			})
		default:
			return singleRowResponseTyped(detailFields, []string{
				"public", "users", "2147483648", "2 GB", "50000000", "18",
				"2025-01-01", "2025-01-02",
			})
		}
	})
	defer cleanup()

	// Act
	detail, err := client.GetTableDetails(context.Background(), "public", "users")

	// Assert: base detail.
	require.NoError(t, err)
	require.NotNil(t, detail)
	assert.Equal(t, "public", detail.Schema)
	assert.Equal(t, "users", detail.Table)
	assert.Equal(t, int64(2147483648), detail.SizeBytes)
	assert.Equal(t, int64(50000000), detail.RowCount)

	// Assert: the bug fix — BOTH bloat AND skew are populated.
	assert.Equal(t, int32(18), detail.BloatPercent,
		"bloat_percent must map to BloatPercent")
	assert.Equal(t, int32(37), detail.SkewPercent,
		"SkewPercent must be enriched from gp_toolkit (the fix)")

	// Assert: index sizes populated from the index query (largest first).
	require.Len(t, detail.IndexSizes, 2)
	assert.Equal(t, "users_pkey", detail.IndexSizes[0].Name)
	assert.Equal(t, int64(1048576), detail.IndexSizes[0].SizeBytes)
	assert.Equal(t, "1 MB", detail.IndexSizes[0].SizeHuman)
	assert.Equal(t, "users_email_idx", detail.IndexSizes[1].Name)
	assert.Equal(t, int64(524288), detail.IndexSizes[1].SizeBytes)
}

// 119c-DB-GetTableDetails-skew-fallback — skew enrichment 42P01: BloatPercent
// still populated, SkewPercent honestly 0, base detail and index sizes intact.
func TestScenario119_GetTableDetails_SkewFallback(t *testing.T) {
	detailFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("size_bytes"), textField("size_human"),
		int8Field("row_count"), int4Field("bloat_percent"),
		textField("last_vacuum"), textField("last_analyze"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case isSkewQuery(query):
			return errorResponseWithCode("42P01",
				`relation "gp_toolkit.gp_skew_coefficients" does not exist`)
		case isIndexQuery(query):
			return multiRowResponseTyped(indexSizeFields(), [][]string{
				{"users_pkey", "1024", "1 kB"},
			})
		default:
			return singleRowResponseTyped(detailFields, []string{
				"public", "users", "4096", "4 KB", "100", "22",
				"never", "never",
			})
		}
	})
	defer cleanup()

	detail, err := client.GetTableDetails(context.Background(), "public", "users")

	require.NoError(t, err)
	require.NotNil(t, detail)
	assert.Equal(t, int32(22), detail.BloatPercent)
	assert.Equal(t, int32(0), detail.SkewPercent,
		"skew must be honestly 0 when gp_toolkit is absent")
	require.Len(t, detail.IndexSizes, 1)
	assert.Equal(t, "users_pkey", detail.IndexSizes[0].Name)
}

// 119c-DB-GetTableDetails-index-fallback — the index-size query errors: the base
// detail (incl. bloat + skew) is still returned with an empty IndexSizes slice
// (best-effort, never an error).
func TestScenario119_GetTableDetails_IndexFallback(t *testing.T) {
	detailFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("size_bytes"), textField("size_human"),
		int8Field("row_count"), int4Field("bloat_percent"),
		textField("last_vacuum"), textField("last_analyze"),
	}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case isSkewQuery(query):
			return multiRowResponseTyped(tableSkewFields(), [][]string{
				{"public", "users", "5"},
			})
		case isIndexQuery(query):
			return errorResponseMsg("index query failed")
		default:
			return singleRowResponseTyped(detailFields, []string{
				"public", "users", "4096", "4 KB", "100", "12",
				"2025-01-01", "2025-01-02",
			})
		}
	})
	defer cleanup()

	detail, err := client.GetTableDetails(context.Background(), "public", "users")

	require.NoError(t, err, "index query failure must be best-effort, not fatal")
	require.NotNil(t, detail)
	assert.Equal(t, int32(12), detail.BloatPercent)
	assert.Equal(t, int32(5), detail.SkewPercent)
	assert.Empty(t, detail.IndexSizes,
		"IndexSizes must be empty when the index query fails")
}

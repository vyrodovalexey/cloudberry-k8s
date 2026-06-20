package main

// Scenario 121 — All Storage CLI Commands (L.1–L.6).
//
// This suite drives the six storage-recommendations CLI commands end-to-end
// through the cobra command tree against the Scenario 108 recording httptest
// server (runCtl + newCtlRecorderServer). For each command it asserts the REAL
// HTTP effect the CLI produced — the request METHOD, the PATH (including the
// query string) — plus the L.3 `tables detail` flag/positional/validation
// matrix and the L.6 `usage-report --month` reporting-period round-trip.
//
// This storage-recommendations CLI family (L.1–L.6) is DISTINCT from the
// data-loading L.1–L.16 CLI family of Scenario 108. The harness here is reused
// verbatim from scenario108_cli_test.go (newCtlRecorderServer/runCtl); no new
// infrastructure is introduced.
//
// Note: runCtl drives the command with --cluster test-cluster and
// --namespace default, so every recorded path is rooted at
//   /api/v1alpha1/clusters/test-cluster/storage/...
// and the query carries namespace=default.
//
// Catalog IDs covered:
//   121a-L1-disk-usage      storage disk-usage            → GET  .../storage/disk-usage
//   121b-L2-tables-list     storage tables list           → GET  .../storage/tables
//   121c-L3-detail-flags    tables detail --schema --table→ GET  .../storage/tables/public/orders
//   121c-L3-detail-positional  tables detail public orders→ GET  .../storage/tables/public/orders
//   121c-L3-detail-precedence  flags + positional         → FLAGS win
//   121c-L3-detail-missing  no flags / no positional       → error, NO HTTP call
//   121c-L3-detail-partial  only --schema                  → error, NO HTTP call
//   121d-L4-recs-list       recommendations list           → GET  .../storage/recommendations
//   121e-L5-recs-scan       recommendations scan           → POST .../storage/recommendations/scan
//   121f-L6-usage-month     usage-report --month 2026-05   → GET  .../storage/usage-report?month=2026-05
//   121f-L6-usage-nomonth   usage-report (no --month)      → no month key
// Plus a direct unit test of the pure helper resolveTableDetail.

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const storageCtlPathPrefix = "/api/v1alpha1/clusters/test-cluster/storage"

// --- 121a-L1: storage disk-usage -------------------------------------------

// 121a-L1-disk-usage — `storage disk-usage` builds exactly one GET to
// .../storage/disk-usage with the namespace encoded in the query.
func TestScenario121_DiskUsage(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "disk-usage")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/disk-usage", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "default", values.Get("namespace"),
		"namespace must always encode")
}

// --- 121b-L2: storage tables list ------------------------------------------

// 121b-L2-tables-list — `storage tables list` builds a single GET to
// .../storage/tables.
func TestScenario121_TablesList(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "tables", "list")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/tables", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "default", values.Get("namespace"))
}

// --- 121c-L3: storage tables detail ----------------------------------------

// 121c-L3-detail-flags — `tables detail --schema public --table orders`
// (no positional args) builds the GET path .../storage/tables/public/orders.
// The FLAG values become the {schema}/{table} path segments.
func TestScenario121_TablesDetail_Flags(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL,
		"storage", "tables", "detail",
		"--schema", "public", "--table", "orders")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/tables/public/orders", req.path,
		"flags must build the {schema}/{table} path segments")

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "default", values.Get("namespace"))
}

// 121c-L3-detail-positional — BACKWARD COMPAT: positional args
// (`tables detail public orders`) still resolve to
// .../storage/tables/public/orders.
func TestScenario121_TablesDetail_Positional(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL,
		"storage", "tables", "detail", "public", "orders")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/tables/public/orders", req.path,
		"positional args must still resolve the path (backward compat)")
}

// 121c-L3-detail-precedence — when BOTH flags AND positional args are supplied
// with DIFFERENT values, the FLAGS win: the path uses the flag values.
func TestScenario121_TablesDetail_Precedence(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act: flags (public/orders) AND positional (sales/legacy) both present.
	err, _ := runCtl(t, srv.URL,
		"storage", "tables", "detail",
		"--schema", "public", "--table", "orders",
		"sales", "legacy")

	// Assert: the path reflects the FLAG values, not the positional ones.
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/tables/public/orders", req.path,
		"flags must take precedence over positional args")
	assert.NotContains(t, req.path, "sales")
	assert.NotContains(t, req.path, "legacy")
}

// 121c-L3-detail-missing — neither flags nor positional args → a clean usage
// error BEFORE any HTTP call (recorder stays empty); the message names BOTH
// input forms.
func TestScenario121_TablesDetail_Missing(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "tables", "detail")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema and table are required")
	assert.Equal(t, 0, rr.count(),
		"no HTTP request must be made when schema/table are missing")
}

// 121c-L3-detail-partial — only --schema (no --table, no positional) → error
// (table required); no HTTP call.
func TestScenario121_TablesDetail_Partial(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL,
		"storage", "tables", "detail", "--schema", "public")

	// Assert
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema and table are required")
	assert.Equal(t, 0, rr.count(),
		"no HTTP request must be made when only the schema is supplied")
}

// --- 121d-L4: storage recommendations list ---------------------------------

// 121d-L4-recs-list — `storage recommendations list` builds a single GET to
// .../storage/recommendations.
func TestScenario121_RecommendationsList(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "recommendations", "list")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/recommendations", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "default", values.Get("namespace"))
}

// --- 121e-L5: storage recommendations scan ---------------------------------

// 121e-L5-recs-scan — `storage recommendations scan` builds a POST (NOT GET)
// to .../storage/recommendations/scan.
func TestScenario121_RecommendationsScan(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "recommendations", "scan")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodPost, req.method,
		"recommendations scan must POST, not GET")
	assert.Equal(t, storageCtlPathPrefix+"/recommendations/scan", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "default", values.Get("namespace"))
}

// --- 121f-L6: storage usage-report --month ---------------------------------

// 121f-L6-usage-month — `storage usage-report --month 2026-05` builds a GET to
// .../storage/usage-report whose query carries month=2026-05 (the correct
// reporting period) AND namespace=. Cross-references 120-C13-cli-month.
func TestScenario121_UsageReportMonth(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL,
		"storage", "usage-report", "--month", "2026-05")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/usage-report", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	assert.Equal(t, "2026-05", values.Get("month"),
		"--month must thread through as the ?month= reporting period")
	assert.Equal(t, "default", values.Get("namespace"))
}

// 121f-L6-usage-nomonth — without --month, the query carries no month key.
func TestScenario121_UsageReportNoMonth(t *testing.T) {
	// Arrange
	srv, rr := newCtlRecorderServer(t)

	// Act
	err, _ := runCtl(t, srv.URL, "storage", "usage-report")

	// Assert
	require.NoError(t, err)
	require.Equal(t, 1, rr.count())
	req := rr.last()
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, storageCtlPathPrefix+"/usage-report", req.path)

	values, parseErr := url.ParseQuery(req.query)
	require.NoError(t, parseErr)
	_, hasMonth := values["month"]
	assert.False(t, hasMonth, "without --month the query must carry no month key")
	assert.Equal(t, "default", values.Get("namespace"))
}

// --- resolveTableDetail — direct pure-function unit test --------------------

// TestScenario121_ResolveTableDetail exercises the pure resolution helper
// directly: flags win over positional, positional fallback, the precedence rule
// when both are present, and the missing/partial error paths (which must NOT
// leak a half-resolved schema/table).
func TestScenario121_ResolveTableDetail(t *testing.T) {
	tests := []struct {
		name       string
		flagSchema string
		flagTable  string
		args       []string
		wantSchema string
		wantTable  string
		wantErr    bool
	}{
		{
			name:       "flags only",
			flagSchema: "public",
			flagTable:  "orders",
			args:       nil,
			wantSchema: "public",
			wantTable:  "orders",
		},
		{
			name:       "positional fallback",
			flagSchema: "",
			flagTable:  "",
			args:       []string{"public", "users"},
			wantSchema: "public",
			wantTable:  "users",
		},
		{
			name:       "flags win over positional (precedence)",
			flagSchema: "public",
			flagTable:  "orders",
			args:       []string{"sales", "legacy"},
			wantSchema: "public",
			wantTable:  "orders",
		},
		{
			name:       "schema flag, table positional (mixed)",
			flagSchema: "public",
			flagTable:  "",
			args:       []string{"ignored", "users"},
			wantSchema: "public",
			wantTable:  "users",
		},
		{
			name:       "table flag, schema positional (mixed)",
			flagSchema: "",
			flagTable:  "orders",
			args:       []string{"sales"},
			wantSchema: "sales",
			wantTable:  "orders",
		},
		{
			name:    "missing both - nil args",
			args:    nil,
			wantErr: true,
		},
		{
			name:    "missing both - empty args",
			args:    []string{},
			wantErr: true,
		},
		{
			name:       "only schema flag - table missing",
			flagSchema: "public",
			args:       nil,
			wantErr:    true,
		},
		{
			name:      "only table flag - schema missing",
			flagTable: "orders",
			args:      nil,
			wantErr:   true,
		},
		{
			name:    "only one positional - table missing",
			args:    []string{"public"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schema, table, err := resolveTableDetail(tc.flagSchema, tc.flagTable, tc.args)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "schema and table are required")
				assert.Contains(t, err.Error(), "--schema/--table or positional args",
					"error must name BOTH input forms")
				// On error both outputs must be empty (no half-built request).
				assert.Empty(t, schema)
				assert.Empty(t, table)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantSchema, schema)
			assert.Equal(t, tc.wantTable, table)
		})
	}
}

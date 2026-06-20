package db

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario 117 — Recommendation Scan Across All Four Types (DB layer).
//
// These unit tests exercise the threshold-aware recommendation queries directly
// against the mock PostgreSQL backend. Because the mock client uses the simple
// query protocol, pgx interpolates the bound $1 threshold into the query string
// BEFORE it reaches the responder. The responders below therefore parse the
// threshold literal out of the query and simulate the server-side WHERE gate
// (>= threshold), so the tests genuinely prove the gate carries the CRD
// threshold rather than merely echoing pre-filtered rows.

// thresholdFromQuery extracts the integer threshold pgx interpolated for the
// `>= <n>` gate in a simple-protocol query string. Returns ok=false when no such
// gate is present (e.g. the query failed to render).
func thresholdFromQuery(query string) (int64, bool) {
	idx := strings.LastIndex(query, ">=")
	if idx < 0 {
		return 0, false
	}
	rest := query[idx+2:]
	// pgx (simple protocol) interpolates the bound value as a quoted literal with
	// surrounding whitespace, e.g. ">=  '30' ". Trim spaces and quotes, then read
	// the leading signed-integer literal.
	rest = strings.TrimSpace(rest)
	rest = strings.TrimLeft(rest, "'")
	end := 0
	for end < len(rest) {
		c := rest[end]
		if (c >= '0' && c <= '9') || (end == 0 && (c == '-' || c == '+')) {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return 0, false
	}
	v, err := strconv.ParseInt(rest[:end], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// ---------------------------------------------------------------------------
// 117a-C6 — Bloat query gates dead_pct on th.Bloat (>= inclusive).
// ---------------------------------------------------------------------------

func TestScenario117_BloatRecommendations_ThresholdGate(t *testing.T) {
	// Candidate rows with their dead_pct. The responder emits only those at or
	// above the interpolated threshold, mirroring the SQL WHERE gate.
	type row struct {
		schema, table string
		deadTup       int
		deadPct       int
	}
	candidates := []row{
		{"public", "low_bloat", 100, 10},   // below a 30 threshold
		{"public", "mid_bloat", 50000, 30}, // exactly at 30 (>= inclusive)
		{"public", "hot_bloat", 200000, 55},
	}

	tests := []struct {
		name      string
		threshold int32
		wantTabs  []string
		wantRatio map[string]float64
	}{
		{
			name:      "threshold zero returns all",
			threshold: 0,
			wantTabs:  []string{"hot_bloat", "mid_bloat", "low_bloat"},
		},
		{
			name:      "boundary at 30 includes exactly-30 row, excludes below",
			threshold: 30,
			wantTabs:  []string{"hot_bloat", "mid_bloat"},
			wantRatio: map[string]float64{"hot_bloat": 55, "mid_bloat": 30},
		},
		{
			name:      "high threshold excludes the boundary row",
			threshold: 31,
			wantTabs:  []string{"hot_bloat"},
		},
		{
			name:      "threshold above all yields none",
			threshold: 99,
			wantTabs:  nil,
		},
	}

	bloatFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("n_dead_tup"), int8Field("dead_pct"),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				th, ok := thresholdFromQuery(query)
				require.True(t, ok, "bloat query must carry an interpolated >= gate: %s", query)
				assert.Equal(t, int64(tt.threshold), th, "threshold must be bound into the gate")
				var emit [][]string
				for _, c := range candidates {
					if int64(c.deadPct) >= th {
						emit = append(emit, []string{
							c.schema, c.table,
							strconv.Itoa(c.deadTup), strconv.Itoa(c.deadPct),
						})
					}
				}
				return multiRowResponseTyped(bloatFields, emit)
			})
			defer cleanup()

			recs, err := client.GetBloatRecommendations(
				context.Background(), RecommendationThresholds{Bloat: tt.threshold})
			require.NoError(t, err)

			gotTabs := make([]string, 0, len(recs))
			for _, r := range recs {
				assert.Equal(t, "bloat", r.Type)
				gotTabs = append(gotTabs, r.Table)
				if want, ok := tt.wantRatio[r.Table]; ok {
					assert.InDelta(t, want, r.Ratio, 0.001,
						"Ratio must equal dead_pct (M.4) for %s", r.Table)
				}
			}
			assert.ElementsMatch(t, tt.wantTabs, gotTabs)
		})
	}
}

// ---------------------------------------------------------------------------
// 117b-C7 — Skew query gates skccoeff on th.Skew; honest 42P01 fallback.
// ---------------------------------------------------------------------------

func TestScenario117_SkewRecommendations_ThresholdGate(t *testing.T) {
	type row struct {
		schema, table string
		coeff         float64
	}
	candidates := []row{
		{"public", "even_table", 5},
		{"public", "boundary_table", 20}, // exactly at 20
		{"public", "skewed_table", 80},
	}

	tests := []struct {
		name      string
		threshold int32
		wantTabs  []string
	}{
		{name: "threshold zero returns all", threshold: 0,
			wantTabs: []string{"skewed_table", "boundary_table", "even_table"}},
		{name: "boundary at 20 inclusive", threshold: 20,
			wantTabs: []string{"skewed_table", "boundary_table"}},
		{name: "one tick above boundary excludes it", threshold: 21,
			wantTabs: []string{"skewed_table"}},
		{name: "threshold above all", threshold: 90, wantTabs: nil},
	}

	skewFields := []fieldDesc{
		textField("skcnamespace"), textField("skcrelname"), float8Field("skccoeff"),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				th, ok := thresholdFromQuery(query)
				require.True(t, ok, "skew query must carry an interpolated >= gate: %s", query)
				assert.Equal(t, int64(tt.threshold), th)
				var emit [][]string
				for _, c := range candidates {
					if int64(c.coeff) >= th {
						emit = append(emit, []string{
							c.schema, c.table, strconv.FormatFloat(c.coeff, 'f', -1, 64),
						})
					}
				}
				return multiRowResponseTyped(skewFields, emit)
			})
			defer cleanup()

			recs, err := client.GetSkewRecommendations(
				context.Background(), RecommendationThresholds{Skew: tt.threshold})
			require.NoError(t, err)

			gotTabs := make([]string, 0, len(recs))
			for _, r := range recs {
				assert.Equal(t, "skew", r.Type)
				gotTabs = append(gotTabs, r.Table)
			}
			assert.ElementsMatch(t, tt.wantTabs, gotTabs)
		})
	}
}

// TestScenario117_SkewRecommendations_HonestFallback covers the honest skip when
// gp_toolkit.gp_skew_coefficients is absent (SQLSTATE 42P01 / 42703): the method
// returns (nil, nil) — no error, no fabricated rows.
func TestScenario117_SkewRecommendations_HonestFallback(t *testing.T) {
	tests := []struct {
		name string
		code string
		msg  string
	}{
		{name: "undefined table 42P01", code: "42P01",
			msg: `relation "gp_toolkit.gp_skew_coefficients" does not exist`},
		{name: "undefined column 42703", code: "42703",
			msg: `column "skccoeff" does not exist`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(_ string) []byte {
				return errorResponseWithCode(tt.code, tt.msg)
			})
			defer cleanup()

			recs, err := client.GetSkewRecommendations(
				context.Background(), RecommendationThresholds{Skew: 20})
			require.NoError(t, err, "missing gp_toolkit view must be an honest skip, not an error")
			assert.Empty(t, recs, "no skew rows must be fabricated when the view is absent")
		})
	}
}

// TestScenario117_SkewRecommendations_GenericError verifies a non-undefined error
// is surfaced (NOT swallowed as an honest fallback).
func TestScenario117_SkewRecommendations_GenericError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("connection reset")
	})
	defer cleanup()

	_, err := client.GetSkewRecommendations(
		context.Background(), RecommendationThresholds{Skew: 20})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying skew recommendations")
}

// ---------------------------------------------------------------------------
// 117c-C8 — Age query gates age(relfrozenxid) on th.Age; honest fallback.
// ---------------------------------------------------------------------------

func TestScenario117_AgeRecommendations_ThresholdGate(t *testing.T) {
	type row struct {
		schema, table string
		age           int64
	}
	candidates := []row{
		{"public", "fresh_table", 1000},
		{"public", "boundary_table", 100000000}, // exactly at 100000000
		{"public", "ancient_table", 600000000},
	}

	tests := []struct {
		name      string
		threshold int64
		wantTabs  []string
	}{
		{name: "threshold zero returns all", threshold: 0,
			wantTabs: []string{"ancient_table", "boundary_table", "fresh_table"}},
		{name: "boundary inclusive", threshold: 100000000,
			wantTabs: []string{"ancient_table", "boundary_table"}},
		{name: "one above boundary excludes it", threshold: 100000001,
			wantTabs: []string{"ancient_table"}},
		{name: "threshold above all", threshold: 900000000, wantTabs: nil},
	}

	ageFields := []fieldDesc{
		textField("nspname"), textField("relname"), int8Field("xid_age"),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				th, ok := thresholdFromQuery(query)
				require.True(t, ok, "age query must carry an interpolated >= gate: %s", query)
				assert.Equal(t, tt.threshold, th)
				var emit [][]string
				for _, c := range candidates {
					if c.age >= th {
						emit = append(emit, []string{
							c.schema, c.table, strconv.FormatInt(c.age, 10),
						})
					}
				}
				return multiRowResponseTyped(ageFields, emit)
			})
			defer cleanup()

			recs, err := client.GetAgeRecommendations(
				context.Background(), RecommendationThresholds{Age: tt.threshold})
			require.NoError(t, err)

			gotTabs := make([]string, 0, len(recs))
			for _, r := range recs {
				assert.Equal(t, "age", r.Type)
				gotTabs = append(gotTabs, r.Table)
			}
			assert.ElementsMatch(t, tt.wantTabs, gotTabs)
		})
	}
}

// TestScenario117_AgeRecommendations_HonestFallback covers the honest skip when
// the age catalog query fails with an undefined-relation/column SQLSTATE.
func TestScenario117_AgeRecommendations_HonestFallback(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseWithCode("42P01", `relation "pg_class" does not exist`)
	})
	defer cleanup()

	recs, err := client.GetAgeRecommendations(
		context.Background(), RecommendationThresholds{Age: 100000000})
	require.NoError(t, err)
	assert.Empty(t, recs)
}

// ---------------------------------------------------------------------------
// 117d-C9 — Index bloat query gates bloat_pct on th.IndexBloat; honest fallback.
// ---------------------------------------------------------------------------

func TestScenario117_IndexBloatRecommendations_ThresholdGate(t *testing.T) {
	type row struct {
		schema, table, index string
		bloatPct             float64
	}
	candidates := []row{
		{"public", "users", "users_pkey", 10},
		{"public", "events", "events_boundary_idx", 40}, // exactly at 40
		{"public", "logs", "logs_bloated_idx", 75},
	}

	tests := []struct {
		name      string
		threshold int32
		wantTabs  []string
		wantRatio map[string]float64
	}{
		{name: "threshold zero returns all", threshold: 0,
			wantTabs: []string{"logs", "events", "users"}},
		{name: "boundary inclusive at 40", threshold: 40,
			wantTabs:  []string{"logs", "events"},
			wantRatio: map[string]float64{"logs": 75, "events": 40}},
		{name: "one above boundary excludes it", threshold: 41,
			wantTabs: []string{"logs"}},
		{name: "threshold above all", threshold: 90, wantTabs: nil},
	}

	idxFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		textField("indexrelname"), float8Field("bloat_pct"),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				th, ok := thresholdFromQuery(query)
				require.True(t, ok, "index bloat query must carry an interpolated >= gate: %s", query)
				assert.Equal(t, int64(tt.threshold), th)
				var emit [][]string
				for _, c := range candidates {
					if int64(c.bloatPct) >= th {
						emit = append(emit, []string{
							c.schema, c.table, c.index,
							strconv.FormatFloat(c.bloatPct, 'f', -1, 64),
						})
					}
				}
				return multiRowResponseTyped(idxFields, emit)
			})
			defer cleanup()

			recs, err := client.GetIndexBloatRecommendations(
				context.Background(), RecommendationThresholds{IndexBloat: tt.threshold})
			require.NoError(t, err)

			gotTabs := make([]string, 0, len(recs))
			for _, r := range recs {
				assert.Equal(t, "index_bloat", r.Type)
				assert.NotEmpty(t, r.Description)
				if want, ok := tt.wantRatio[r.Table]; ok {
					assert.InDelta(t, want, r.Ratio, 0.001)
				}
				gotTabs = append(gotTabs, r.Table)
			}
			assert.ElementsMatch(t, tt.wantTabs, gotTabs)
		})
	}
}

// TestScenario117_IndexBloatRecommendations_HonestFallback covers the honest skip
// when the estimate relies on a catalog column absent on this version.
func TestScenario117_IndexBloatRecommendations_HonestFallback(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseWithCode("42703", `column "reltuples" does not exist`)
	})
	defer cleanup()

	recs, err := client.GetIndexBloatRecommendations(
		context.Background(), RecommendationThresholds{IndexBloat: 40})
	require.NoError(t, err)
	assert.Empty(t, recs)
}

// TestScenario117_RecommendationRowIterationError covers the rows.Err() path for
// each type via a truncated/garbled stream so the iteration-error branches are
// exercised (defensive coverage of the rows.Err() guards).
func TestScenario117_BloatRecommendations_RowError(t *testing.T) {
	bloatFields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		int8Field("n_dead_tup"), int8Field("dead_pct"),
	}
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		// Emit a valid row then an ErrorResponse mid-stream to trigger rows.Err().
		return rowErrorResponse(bloatFields, []string{"public", "t", "10", "10"})
	})
	defer cleanup()

	_, err := client.GetBloatRecommendations(
		context.Background(), RecommendationThresholds{Bloat: 0})
	require.Error(t, err)
}

// rowErrorResponse emits one valid row of the given typed fields, then an
// ErrorResponse mid-stream, to trigger the rows.Err() iteration-error branch.
func rowErrorResponse(fields []fieldDesc, row []string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	dr := &pgproto3.DataRow{}
	for _, v := range row {
		dr.Values = append(dr.Values, []byte(v))
	}
	buf = append(buf, mustEncode(dr)...)
	buf = append(buf, mustEncode(&pgproto3.ErrorResponse{
		Severity: "ERROR", Message: "stream aborted",
	})...)
	return buf
}

func TestScenario117_SkewRecommendations_RowError(t *testing.T) {
	fields := []fieldDesc{
		textField("skcnamespace"), textField("skcrelname"), float8Field("skccoeff"),
	}
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return rowErrorResponse(fields, []string{"public", "t", "30"})
	})
	defer cleanup()

	_, err := client.GetSkewRecommendations(
		context.Background(), RecommendationThresholds{Skew: 0})
	require.Error(t, err)
}

func TestScenario117_AgeRecommendations_RowError(t *testing.T) {
	fields := []fieldDesc{
		textField("nspname"), textField("relname"), int8Field("xid_age"),
	}
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return rowErrorResponse(fields, []string{"public", "t", "100000000"})
	})
	defer cleanup()

	_, err := client.GetAgeRecommendations(
		context.Background(), RecommendationThresholds{Age: 0})
	require.Error(t, err)
}

func TestScenario117_IndexBloatRecommendations_RowError(t *testing.T) {
	fields := []fieldDesc{
		textField("schemaname"), textField("relname"),
		textField("indexrelname"), float8Field("bloat_pct"),
	}
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return rowErrorResponse(fields, []string{"public", "t", "idx", "40"})
	})
	defer cleanup()

	_, err := client.GetIndexBloatRecommendations(
		context.Background(), RecommendationThresholds{IndexBloat: 0})
	require.Error(t, err)
}

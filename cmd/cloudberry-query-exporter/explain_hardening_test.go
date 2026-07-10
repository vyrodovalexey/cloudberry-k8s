package main

// Tests for the post-review EXPLAIN sandbox / shutdown hardening handoff:
//   - T-3 (F-4.8a): SET LOCAL statement_timeout failure — recordError(explain)
//     exactly once, no EXPLAIN issued, ROLLBACK still the last statement.
//   - T-4 (F-4.8c): isExplainSafe rejects top-level ';' statement separators
//     (multi-statement chains) while keeping semicolons inside string
//     literals, quoted identifiers and comments allowlisted.
//   - T-11 (F-4.8b): ROLLBACK-failure log branch.
//   - T-7 (F-4.8d): closeConn Close-error branch.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// T-3 — SET LOCAL failure path
// ----------------------------------------------------------------------------

func TestCollectExplainPlan_SetLocalFailure(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		switch {
		case strings.HasPrefix(query, "BEGIN"):
			return execResponse("BEGIN")
		case strings.HasPrefix(query, "SET LOCAL"):
			return errorResponseMsg("set local refused")
		case strings.HasPrefix(query, "EXPLAIN"):
			return rowsResponseTyped([]fieldDesc{textField("QUERY PLAN")}, [][]string{
				{"Seq Scan on t"},
			})
		default:
			return execResponse("ROLLBACK")
		}
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), true, time.Second, m.historyErrors)

	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT 1")
	assert.Empty(t, plan, "a failed SET LOCAL must abort the plan collection")

	stmts := rec.all()
	require.Len(t, stmts, 3,
		"exactly BEGIN/SET LOCAL/ROLLBACK must be issued on SET LOCAL failure: %v", stmts)
	assert.Equal(t, "BEGIN READ ONLY", stmts[0])
	assert.True(t, strings.HasPrefix(stmts[1], "SET LOCAL statement_timeout"),
		"second statement must be the SET LOCAL: %s", stmts[1])
	assert.Equal(t, "ROLLBACK", stmts[2],
		"ROLLBACK must still be issued when SET LOCAL fails: %v", stmts)
	for _, s := range stmts {
		assert.NotContains(t, s, "EXPLAIN",
			"EXPLAIN must not run when the statement timeout could not be set")
	}
	assert.InDelta(t, 1.0, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "explain"), 0.0001,
		"exactly one explain-stage error must be recorded")
}

// ----------------------------------------------------------------------------
// T-11 — ROLLBACK failure log branch
// ----------------------------------------------------------------------------

func TestCollectExplainPlan_RollbackFailureLoggedPlanKept(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		switch {
		case strings.HasPrefix(query, "BEGIN"):
			return execResponse("BEGIN")
		case strings.HasPrefix(query, "SET LOCAL"):
			return execResponse("SET")
		case strings.HasPrefix(query, "EXPLAIN"):
			return rowsResponseTyped([]fieldDesc{textField("QUERY PLAN")}, [][]string{
				{"Seq Scan on orders"},
			})
		default: // ROLLBACK
			return errorResponseMsg("rollback refused")
		}
	})
	defer cleanup()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(logger, true, time.Second, m.historyErrors)

	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT * FROM orders")
	assert.Contains(t, plan, "Seq Scan on orders",
		"a ROLLBACK failure is log-only and must not discard the collected plan")

	stmts := rec.all()
	require.NotEmpty(t, stmts)
	assert.Equal(t, "ROLLBACK", stmts[len(stmts)-1], "ROLLBACK must still be attempted")
	assert.Contains(t, buf.String(), "failed to roll back EXPLAIN sandbox transaction")
	assert.Contains(t, buf.String(), "rollback refused")
	assert.Zero(t, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "explain"),
		"a rollback hiccup after a successful EXPLAIN is not an explain-stage error")
}

// ----------------------------------------------------------------------------
// T-4 — multi-statement (top-level ';') allowlist hardening
// ----------------------------------------------------------------------------

func TestHasTopLevelStatementSeparator(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "no semicolon", in: "SELECT 1", want: false},
		{name: "classic chain", in: "SELECT 1; DROP TABLE t", want: true},
		{name: "sandbox escape chain", in: "SELECT 1; ROLLBACK; DROP TABLE x", want: true},
		{name: "string literal after separator is content", in: "SELECT 1; 'x'", want: true},
		{name: "quoted identifier after separator is content", in: `SELECT 1; "x"`, want: true},
		{name: "comment then content after separator", in: "SELECT 1; -- c\nDROP TABLE t", want: true},
		{name: "double separator then content", in: "SELECT 1;;DROP TABLE t", want: true},
		{name: "semicolon inside string literal", in: "SELECT ';'", want: false},
		{name: "injection text inside string literal", in: "SELECT ' ; DROP TABLE x; ' FROM t", want: false},
		{name: "escaped quote keeps literal open", in: "SELECT 'a''; DROP TABLE t'", want: false},
		{name: "semicolon inside quoted identifier", in: `SELECT ";" FROM t`, want: false},
		{name: "escaped double quote keeps identifier open", in: `SELECT "a""; DROP" FROM t`, want: false},
		{name: "semicolon inside line comment", in: "SELECT 1 -- ; DROP TABLE t", want: false},
		{name: "semicolon inside block comment", in: "SELECT 1 /* ; DROP TABLE t */", want: false},
		{name: "semicolon inside nested block comment", in: "SELECT 1 /* a /* ;x */ b */", want: false},
		{name: "trailing semicolon", in: "SELECT 1;", want: false},
		{name: "trailing semicolon and whitespace", in: "SELECT 1;  \n\t", want: false},
		{name: "trailing semicolon then comment only", in: "SELECT 1;\n-- done", want: false},
		{name: "trailing semicolon then block comment only", in: "SELECT 1; /* done */", want: false},
		{name: "consecutive trailing separators", in: "SELECT 1;;", want: false},
		{name: "unterminated literal swallows separator", in: "SELECT 'abc; DROP TABLE t", want: false},
		{name: "unterminated block comment swallows separator", in: "SELECT 1 /* ; DROP", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, hasTopLevelStatementSeparator(tt.in), "input: %q", tt.in)
		})
	}
}

func TestIsExplainSafe_MultiStatementPolicy(t *testing.T) {
	allowed := []string{
		"SELECT ';'",                        // separator inside a string literal
		"SELECT ' ; DROP TABLE x; ' FROM t", // injection text stays literal
		`SELECT ";" FROM t`,                 // separator inside a quoted identifier
		"SELECT 1 -- ; DROP TABLE t",        // separator inside a line comment
		"SELECT 1 /* ; DROP TABLE t */",     // separator inside a block comment
		"SELECT 1;",                         // trailing separator of a typed query
		"WITH q AS (SELECT ';') SELECT * FROM q;",
	}
	blocked := []string{
		"SELECT 1; DROP TABLE t",
		"SELECT 1; ROLLBACK; DROP TABLE x",
		"WITH q AS (SELECT 1) SELECT * FROM q; DELETE FROM t",
		"select 1; rollback; delete from t", // lower case chain
		"SELECT 1; -- hide\nTRUNCATE t",     // comment cannot resurrect the chain
		"/* c */ SELECT 1; DROP TABLE t",    // leading comment + chain
	}

	for _, q := range allowed {
		assert.True(t, isExplainSafe(q), "expected allowlisted: %q", q)
	}
	for _, q := range blocked {
		assert.False(t, isExplainSafe(q), "expected blocked: %q", q)
	}
}

func TestCollectExplainPlan_MultiStatementNeverTouchesDB(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		return execResponse("SELECT 0")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), true, time.Second, m.historyErrors)

	for _, q := range []string{
		"SELECT 1; ROLLBACK; DROP TABLE x",
		"SELECT 1; DROP TABLE t",
	} {
		assert.Empty(t, hc.collectExplainPlan(context.Background(), conn, q))
	}

	assert.Empty(t, rec.all(),
		"multi-statement chains must not reach the database at all (no BEGIN, no EXPLAIN)")
	assert.Zero(t, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "explain"),
		"an allowlist skip is not an error (honest metrics)")
}

// ----------------------------------------------------------------------------
// T-7 — closeConn error branch
// ----------------------------------------------------------------------------

func TestCloseConn_CloseErrorLogged(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	// Kill the underlying socket first: pgx's Close then fails when it
	// tears down the already-closed network connection.
	require.NoError(t, conn.PgConn().Conn().Close())

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	closeConn(conn, logger)

	assert.Contains(t, buf.String(), "error closing database connection",
		"a failing Close must be logged, not swallowed")
}

func TestCloseConn_NilAndCleanCloseNoWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	// Nil connection: silent no-op.
	assert.NotPanics(t, func() { closeConn(nil, logger) })
	assert.Empty(t, buf.String())

	// Healthy connection: closed without a warning.
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	closeConn(conn, logger)
	assert.True(t, conn.IsClosed(), "closeConn must actually close the connection")
	assert.Empty(t, buf.String(), "a clean close must not log a warning")
}

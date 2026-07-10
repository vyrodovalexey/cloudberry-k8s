package main

// Tests for the exporter review-remediation fixes:
//   - T6/B3: shutdown correctness — the collect loop owns and closes the
//     current connection (original or reconnected) and signals via done.
//   - T11/C2: EXPLAIN sandbox — comment-stripped SELECT/WITH allowlist,
//     BEGIN READ ONLY + SET LOCAL statement_timeout + ROLLBACK on all paths.
//   - T13/E2: cloudberry_query_exporter_history_errors_total{stage}.
//   - T18/D2: resource_group is bound as '' (dead struct field removed).
//   - T19/D3: shared DDL const (golden-string drift guard).

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/dbschema"
)

// ----------------------------------------------------------------------------
// T6 — shutdown / connection ownership
// ----------------------------------------------------------------------------

func TestCollectLoop_ClosesOwnedConnOnShutdown(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, nil)
	cfg := &exporterConfig{
		dsn:              "host=x",
		samplingInterval: time.Hour, // no ticks — isolate the shutdown path
		historyRetention: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go collectLoop(ctx, cfg, conn, m, testLogger(), hc, done)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collectLoop did not close done after cancel")
	}

	assert.True(t, conn.IsClosed(),
		"the loop owns the connection and must close it on shutdown")
}

// trackedMockPGServer is a mock PG server that counts opened and finished
// (client-closed) sessions, so ownership tests can assert exactly-once close
// behavior at the wire level.
func trackedMockPGServer(
	t *testing.T, responder func(string) []byte,
) (addr string, opened, finished *atomic.Int32, cleanup func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	opened = &atomic.Int32{}
	finished = &atomic.Int32{}

	var wg sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			opened.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				// handleConn returns when the client sends Terminate
				// (pgx Close) or the socket drops.
				handleConn(c, responder)
				finished.Add(1)
			}()
		}
	}()

	cleanup = func() {
		_ = ln.Close()
		<-acceptDone
		wg.Wait()
	}
	return ln.Addr().String(), opened, finished, cleanup
}

func TestCollectLoop_ReconnectedConnClosedExactlyOnce(t *testing.T) {
	// Tracked server: target for the loop's reconnection.
	addr, opened, finished, cleanup := trackedMockPGServer(t, func(query string) []byte {
		if strings.Contains(query, "INSERT") {
			return execResponse("INSERT 0 1")
		}
		return countResponder(query)
	})
	defer cleanup()

	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	dsn := "host=" + host + " port=" + port +
		" dbname=testdb user=testuser password=testpass sslmode=disable" +
		" default_query_exec_mode=simple_protocol"

	// The ORIGINAL conn is already closed (stale pointer): the first tick's
	// ping fails and the loop reconnects to the tracked server.
	staleConn, staleCleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer staleCleanup()
	require.NoError(t, staleConn.Close(context.Background()))

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, nil)
	cfg := &exporterConfig{
		dsn:              dsn,
		samplingInterval: 5 * time.Millisecond,
		historyRetention: time.Hour,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go collectLoop(ctx, cfg, staleConn, m, testLogger(), hc, done)

	// Wait for the reconnection to the tracked server.
	require.Eventually(t, func() bool { return opened.Load() >= 1 },
		5*time.Second, 5*time.Millisecond, "loop never reconnected")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("collectLoop did not exit after cancel")
	}

	// The loop must close the RECONNECTED conn on exit: every session the
	// tracked server saw ends (finished == opened), i.e. nothing is leaked
	// and nothing is double-closed (each session finishes exactly once).
	require.Eventually(t, func() bool { return finished.Load() == opened.Load() },
		2*time.Second, 5*time.Millisecond,
		"reconnected connection was not closed by the loop (opened=%d finished=%d)",
		opened.Load(), finished.Load())
	assert.True(t, staleConn.IsClosed(), "original stale conn stays closed")
}

// ----------------------------------------------------------------------------
// T11 — EXPLAIN sandbox
// ----------------------------------------------------------------------------

// explainRecorder captures the statement sequence the mock server received.
type explainRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *explainRecorder) add(q string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, q)
}

func (r *explainRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.statements))
	copy(out, r.statements)
	return out
}

func TestStripLeadingSQLComments(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "no comments", in: "SELECT 1", want: "SELECT 1"},
		{name: "leading whitespace", in: "  \n\t SELECT 1", want: "SELECT 1"},
		{name: "line comment", in: "-- c\nSELECT 1", want: "SELECT 1"},
		{name: "line comment no newline", in: "-- only a comment", want: ""},
		{name: "block comment", in: "/* c */ SELECT 1", want: "SELECT 1"},
		{name: "nested block comment", in: "/* a /* b */ c */SELECT 1", want: "SELECT 1"},
		{name: "unterminated block comment", in: "/* never closed SELECT 1", want: ""},
		{name: "stacked comments", in: "--x\n /*y*/ --z\nWITH q AS (SELECT 1) SELECT * FROM q",
			want: "WITH q AS (SELECT 1) SELECT * FROM q"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, stripLeadingSQLComments(tt.in))
		})
	}
}

func TestIsExplainSafe(t *testing.T) {
	allowed := []string{
		"SELECT * FROM t",
		"  select 1",
		"WITH q AS (SELECT 1) SELECT * FROM q",
		"/* hint */ SELECT 1",
		"-- comment\nselect 1",
	}
	blocked := []string{
		"DELETE FROM t",
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DROP TABLE t",
		"/* c */ DELETE FROM t", // comment-prefixed bypass attempt
		"-- c\nDROP TABLE t",    // line-comment bypass attempt
		"/*x*/DROP TABLE users", // no space after comment
		"VACUUM",
		"TRUNCATE t",
		"SELECTx FROM t",           // not the SELECT keyword
		"WITHDRAW",                 // not the WITH keyword
		"",                         // empty
		"-- only a comment",        // nothing after comments
		"/* unterminated SELECT 1", // unterminated comment
	}

	for _, q := range allowed {
		assert.True(t, isExplainSafe(q), "expected allowlisted: %q", q)
	}
	for _, q := range blocked {
		assert.False(t, isExplainSafe(q), "expected blocked: %q", q)
	}
}

func TestCollectExplainPlan_SandboxStatementSequence(t *testing.T) {
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
		default:
			return execResponse("ROLLBACK")
		}
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second, nil)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT * FROM orders")
	assert.Contains(t, plan, "Seq Scan on orders")

	stmts := rec.all()
	require.Len(t, stmts, 4, "sandbox must issue exactly BEGIN/SET/EXPLAIN/ROLLBACK: %v", stmts)
	assert.Equal(t, "BEGIN READ ONLY", stmts[0])
	assert.Equal(t, "SET LOCAL statement_timeout = '2000ms'", stmts[1])
	assert.Equal(t, "EXPLAIN (FORMAT TEXT) SELECT * FROM orders", stmts[2])
	assert.Equal(t, "ROLLBACK", stmts[3])
}

func TestCollectExplainPlan_BlockedStatementsNeverTouchDB(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		return execResponse("SELECT 0")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second, nil)
	for _, q := range []string{
		"/* c */ DELETE FROM t",
		"-- c\nDROP TABLE t",
		"INSERT INTO t VALUES (1)",
		"VACUUM",
		"CREATE TABLE t (x int)",
	} {
		assert.Empty(t, hc.collectExplainPlan(context.Background(), conn, q))
	}

	assert.Empty(t, rec.all(),
		"blocked statements must not reach the database at all (no BEGIN, no EXPLAIN)")
}

func TestCollectExplainPlan_RollbackOnExplainError(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		switch {
		case strings.HasPrefix(query, "BEGIN"):
			return execResponse("BEGIN")
		case strings.HasPrefix(query, "SET LOCAL"):
			return execResponse("SET")
		case strings.HasPrefix(query, "EXPLAIN"):
			return errorResponseMsg("explain failed")
		default:
			return execResponse("ROLLBACK")
		}
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), true, time.Second, m.historyErrors)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT 1")
	assert.Empty(t, plan)

	stmts := rec.all()
	require.NotEmpty(t, stmts)
	assert.Equal(t, "ROLLBACK", stmts[len(stmts)-1],
		"ROLLBACK must be issued even when the EXPLAIN fails: %v", stmts)
	assert.InDelta(t, 1.0, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "explain"), 0.0001)
}

func TestCollectExplainPlan_RollbackOnBeginError(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		if strings.HasPrefix(query, "BEGIN") {
			return errorResponseMsg("begin refused")
		}
		return execResponse("OK")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second, nil)
	assert.Empty(t, hc.collectExplainPlan(context.Background(), conn, "SELECT 1"))

	for _, s := range rec.all() {
		assert.NotContains(t, s, "EXPLAIN",
			"EXPLAIN must not run when the sandbox transaction could not start")
	}
}

// ----------------------------------------------------------------------------
// T13 — history error counter
// ----------------------------------------------------------------------------

// counterValue gathers a labeled counter value from the registry (0 when the
// series does not exist).
func counterValue(t *testing.T, reg *prometheus.Registry, family, labelName, labelValue string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestHistoryErrors_SnapshotStage(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("snapshot boom")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, m.historyErrors)

	hc.collectHistory(context.Background(), conn)

	assert.InDelta(t, 1.0, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "snapshot"), 0.0001)
	assert.Zero(t, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "insert"))
}

func TestHistoryErrors_InsertStage(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "INSERT") {
			return errorResponseMsg("insert boom")
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, m.historyErrors)

	err := hc.insertHistoryEntry(context.Background(), conn, "q-1", 1,
		&sessionSnapshot{Username: "u", Database: "d", QueryText: "SELECT 1"},
		time.Now(), 100, "")
	require.Error(t, err)

	// The stage counter is incremented by the caller (recordCompletedQuery);
	// drive it through the caller path for the honest end-to-end increment.
	ok := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{State: "active", QueryText: "SELECT 1", QueryStart: time.Now()},
		time.Now())
	assert.False(t, ok)
	assert.InDelta(t, 1.0, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "insert"), 0.0001)
}

func TestHistoryErrors_SuccessPathNoIncrement(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "INSERT") {
			return execResponse("INSERT 0 1")
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, m.historyErrors)

	ok := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{State: "active", QueryText: "SELECT 1", QueryStart: time.Now()},
		time.Now())
	require.True(t, ok)

	for _, stage := range []string{"snapshot", "insert", "explain"} {
		assert.Zero(t, counterValue(t, reg,
			"cloudberry_query_exporter_history_errors_total", "stage", stage),
			"no increment on success (honest metrics), stage=%s", stage)
	}
}

func TestHistoryErrors_NilCounterVecIsSafe(t *testing.T) {
	hc := newHistoryCollector(testLogger(), false, time.Second, nil)
	assert.NotPanics(t, func() { hc.recordError(historyStageSnapshot) })
}

// ----------------------------------------------------------------------------
// T18 — resource_group bound as ''
// ----------------------------------------------------------------------------

func TestInsertHistoryEntry_BindsEmptyResourceGroup(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second, nil)
	err := hc.insertHistoryEntry(context.Background(), conn, "q-9", 9,
		&sessionSnapshot{Username: "u", Database: "d", QueryText: "SELECT 1",
			WaitEventType: "IO"},
		time.Now(), 42, "")
	require.NoError(t, err)

	stmts := rec.all()
	require.Len(t, stmts, 1)
	// Simple protocol interpolates parameters: the resource_group position
	// (last parameter, after wait_events 'IO') must be the empty literal.
	assert.Regexp(t, `'IO'\s*,\s*''\s*\)$`, stmts[0],
		"resource_group must be bound as the empty string: %s", stmts[0])
}

// ----------------------------------------------------------------------------
// T19 — shared DDL drift guard
// ----------------------------------------------------------------------------

func TestHistoryTableDDL_SharedGolden(t *testing.T) {
	// Golden-string guard: the exporter's DDL must BE the shared dbschema
	// definition (single source of truth), and the shared definition must
	// still describe the cloudberry_query_history table this collector
	// inserts into.
	assert.Equal(t, dbschema.QueryHistoryDDL, historyTableDDL)
	assert.Contains(t, historyTableDDL, "CREATE TABLE IF NOT EXISTS cloudberry_query_history")
	assert.Contains(t, historyTableDDL, "resource_group   TEXT DEFAULT ''")
	assert.Contains(t, historyTableDDL, "DISTRIBUTED BY (id)")
}

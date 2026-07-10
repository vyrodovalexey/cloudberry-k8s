package main

// Tests for the Cycle-2 query-history resilience fix: cloudberry_query_history
// is a DISTRIBUTED table, so the INSERT (and the retention DELETE) dispatch to
// segments over the Motion/Interconnect layer. When the interconnect is
// degraded those statements used to hang the shared connection — and with it
// the whole collect loop — until gp_interconnect_setup_timeout. The fix bounds
// both statements with client-side deadlines so failures surface fast as
// cloudberry_query_exporter_history_errors_total{stage="insert"} (insert) or a
// warn log (cleanup) instead of wedging the exporter.

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

// hangingResponderSleep simulates a wedged interconnect: long enough that a
// test finishing sooner PROVES the client-side deadline fired, short enough
// that the detached mock-server goroutine drains quickly after the suite.
const hangingResponderSleep = 3 * time.Second

// boundedStatementBudget is the elapsed-time ceiling asserted on the bounded
// paths: far above the 50ms test deadlines (CI jitter safe) and far below
// hangingResponderSleep (proves we never waited out the server).
const boundedStatementBudget = 2 * time.Second

// TestNewHistoryCollector_StatementTimeoutsWired proves production wiring: the
// collector defaults its per-statement budgets to the package constants (the
// struct fields exist only as a per-instance test seam).
func TestNewHistoryCollector_StatementTimeoutsWired(t *testing.T) {
	hc := newHistoryCollector(testLogger(), false, time.Second, nil)

	assert.Equal(t, historyInsertTimeout, hc.insertTimeout,
		"insert budget must default to historyInsertTimeout")
	assert.Equal(t, collectorQueryTimeout, hc.insertTimeout,
		"insert budget must stay aligned with the shared collector query budget")
	assert.Equal(t, historyCleanupTimeout, hc.cleanupTimeout,
		"cleanup budget must default to historyCleanupTimeout")
}

// TestRecordCompletedQuery_InsertDeadlineBounded drives the full completed-query
// path against a mock server that hangs on the INSERT: the bounded Exec must
// fail fast (well under the server hang), the failure must be counted under
// stage="insert", and the INSERT statement must actually have been issued.
func TestRecordCompletedQuery_InsertDeadlineBounded(t *testing.T) {
	rec := &explainRecorder{}
	conn, cleanup := newMockConn(t, func(query string) []byte {
		rec.add(query)
		if strings.Contains(query, "INSERT INTO cloudberry_query_history") {
			time.Sleep(hangingResponderSleep) // wedged interconnect
		}
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	reg := prometheus.NewRegistry()
	m := newExporterMetrics(reg)
	hc := newHistoryCollector(testLogger(), false, time.Second, m.historyErrors)
	hc.insertTimeout = 50 * time.Millisecond

	start := time.Now()
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{
			State: "active", QueryText: "SELECT 1",
			QueryStart: time.Now().Add(-2 * time.Second),
		}, time.Now())
	elapsed := time.Since(start)

	assert.False(t, got, "a timed-out insert must not count as recorded")
	assert.Less(t, elapsed, boundedStatementBudget,
		"the insert must be bounded by the deadline, not by the hanging server")
	assert.InDelta(t, 1.0, counterValue(t, reg,
		"cloudberry_query_exporter_history_errors_total", "stage", "insert"), 0.0001,
		"the timeout must surface as exactly one insert-stage error")

	stmts := rec.all()
	require.Len(t, stmts, 1, "exactly the INSERT must have been issued: %v", stmts)
	assert.Contains(t, stmts[0], "INSERT INTO cloudberry_query_history")
}

// TestInsertHistoryEntry_WithPlan_DeadlineBounded covers the with-plan INSERT
// variant under the same bound: the returned error must unwrap to the context
// deadline (proving the client-side timeout, not a server error, fired).
func TestInsertHistoryEntry_WithPlan_DeadlineBounded(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		time.Sleep(hangingResponderSleep) // wedged interconnect
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second, nil)
	hc.insertTimeout = 50 * time.Millisecond

	start := time.Now()
	err := hc.insertHistoryEntry(context.Background(), conn, "q-1", 1,
		&sessionSnapshot{State: "active", QueryText: "SELECT 1", QueryStart: time.Now()},
		time.Now(), 100, "Seq Scan on t")
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"the failure must be the bounded deadline, not a server-side error")
	assert.Less(t, elapsed, boundedStatementBudget)
}

// TestCleanupHistory_DeadlineBoundsHangingDelete proves the retention DELETE on
// the same distributed table is equally bounded: a hanging server must not
// wedge the hourly cleanup tick — the failure is logged and the pass aborted.
func TestCleanupHistory_DeadlineBoundsHangingDelete(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		time.Sleep(hangingResponderSleep) // wedged interconnect
		return execResponse("DELETE 0")
	})
	defer cleanup()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	hc := newHistoryCollector(logger, false, time.Second, nil)
	hc.cleanupTimeout = 50 * time.Millisecond

	start := time.Now()
	hc.cleanupHistory(context.Background(), conn, 30*24*time.Hour)
	elapsed := time.Since(start)

	assert.Less(t, elapsed, boundedStatementBudget,
		"cleanup must be bounded by the deadline, not by the hanging server")
	assert.Contains(t, buf.String(), "failed to cleanup query history",
		"the aborted cleanup must be logged for the next tick to retry")
}

package main

// Error-path tests for the history collector that were previously uncovered
// (E-2): snapshot failure aborting a collection cycle, scan / rows.Err
// failures during the session snapshot, and EXPLAIN-plan edge cases.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshotFields matches the column list of snapshotSessionsSQL.
func snapshotFields() []fieldDesc {
	return []fieldDesc{
		int8Field("pid"), textField("usename"), textField("datname"),
		textField("query"), {name: "query_start", oid: 1184}, // timestamptz
		textField("state"), textField("wait_event_type"),
	}
}

func TestCollectHistory_SnapshotError_AbortsCycle(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("snapshot failed")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	// Pre-seed the previous cycle so a (wrong) completion insert would be
	// observable: a failed snapshot must NOT mark queries as completed.
	hc.lastSeenPIDs = map[int32]*sessionSnapshot{
		42: {PID: 42, State: "active", QueryText: "SELECT 1", QueryStart: time.Now()},
	}

	hc.collectHistory(context.Background(), conn)

	assert.Len(t, hc.lastSeenPIDs, 1,
		"a failed snapshot must leave the previous PID set untouched")
	assert.Contains(t, hc.lastSeenPIDs, int32(42))
}

func TestSnapshotSessions_ScanError_RowSkipped(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(snapshotFields(), [][]string{
			{"7", "alice", "db1", "SELECT 1", "2026-01-01 00:00:00+00", "active", ""},
			// Non-numeric PID → scan error; pgx closes the rows, so this is
			// last and surfaces through rows.Err() → ok=false.
			{"not-a-pid", "bob", "db1", "SELECT 2", "2026-01-01 00:00:00+00", "active", ""},
		})
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	pids, ok := hc.snapshotSessions(context.Background(), conn)

	assert.False(t, ok, "a scan failure mid-snapshot must invalidate the snapshot")
	assert.Nil(t, pids)
}

func TestSnapshotSessions_RowsErrMidStream(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsThenError(snapshotFields(),
			[][]string{{"7", "alice", "db1", "SELECT 1", "2026-01-01 00:00:00+00", "active", ""}},
			"backend died")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	pids, ok := hc.snapshotSessions(context.Background(), conn)

	assert.False(t, ok)
	assert.Nil(t, pids)
}

func TestCollectExplainPlan_ScanError_ReturnsEmpty(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "EXPLAIN") {
			// Two-column result set: Scan(&line) into one variable fails.
			return rowsResponseTyped(
				[]fieldDesc{textField("a"), textField("b")},
				[][]string{{"x", "y"}},
			)
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT * FROM t")
	assert.Empty(t, plan, "a scan failure must yield an empty plan, not a partial one")
}

func TestCollectExplainPlan_RowsErr_ReturnsEmpty(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(query)), "EXPLAIN") {
			return rowsThenError(
				[]fieldDesc{textField("QUERY PLAN")},
				[][]string{{"Seq Scan on t"}},
				"explain interrupted")
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT * FROM t")
	assert.Empty(t, plan)
}

func TestCollectExplainPlan_SkipsAllUtilityPrefixes(t *testing.T) {
	// No connection required: utility statements short-circuit before any
	// query is issued. A panic here would mean the guard is gone.
	hc := newHistoryCollector(testLogger(), true, time.Second)
	for _, stmt := range []string{
		"CREATE TABLE t (i int)", "ALTER TABLE t ADD COLUMN j int",
		"DROP TABLE t", "COPY t FROM '/tmp/x'", "GRANT ALL ON t TO u",
		"REVOKE ALL ON t FROM u", "SET work_mem = '1GB'", "RESET work_mem",
		"VACUUM t", "ANALYZE t", "REINDEX TABLE t",
		"  vacuum full t", // case/whitespace insensitivity
	} {
		assert.Empty(t, hc.collectExplainPlan(context.Background(), nil, stmt),
			"utility statement %q must be skipped", stmt)
	}
}

func TestCollectHistory_InsertedCountLogged(t *testing.T) {
	// One PID disappears between cycles and its insert succeeds → the
	// insertedCount>0 branch runs and lastSeenPIDs is replaced.
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "INSERT INTO cloudberry_query_history") {
			return execResponse("INSERT 0 1")
		}
		// Current snapshot: only PID 8 remains.
		return rowsResponseTyped(snapshotFields(), [][]string{
			{"8", "alice", "db1", "SELECT 2", "2026-01-01 00:00:00+00", "active", ""},
		})
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	hc.lastSeenPIDs = map[int32]*sessionSnapshot{
		7: {PID: 7, State: "active", QueryText: "SELECT 1", QueryStart: time.Now().Add(-time.Minute)},
		8: {PID: 8, State: "active", QueryText: "SELECT 2", QueryStart: time.Now()},
	}

	hc.collectHistory(context.Background(), conn)

	require.Len(t, hc.lastSeenPIDs, 1, "lastSeenPIDs must be replaced by the new snapshot")
	assert.Contains(t, hc.lastSeenPIDs, int32(8))
}

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHistoryCollector(t *testing.T) {
	t.Parallel()
	hc := newHistoryCollector(testLogger(), true, time.Second)
	require.NotNil(t, hc)
	assert.True(t, hc.planCollection)
	assert.Equal(t, time.Second, hc.slowQueryThreshold)
	assert.NotNil(t, hc.lastSeenPIDs)
}

func TestEnsureTable_NilConn(t *testing.T) {
	t.Parallel()
	hc := newHistoryCollector(testLogger(), false, time.Second)
	err := hc.ensureTable(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database connection")
}

func TestEnsureTable_Success(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("CREATE TABLE")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	err := hc.ensureTable(context.Background(), conn)
	require.NoError(t, err)
}

func TestEnsureTable_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("permission denied")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	err := hc.ensureTable(context.Background(), conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ensuring query history table")
}

func TestCollectHistory_NilConn(t *testing.T) {
	t.Parallel()
	hc := newHistoryCollector(testLogger(), false, time.Second)
	// Should be a no-op and not panic.
	hc.collectHistory(context.Background(), nil)
}

func TestSnapshotSessions(t *testing.T) {
	fields := []fieldDesc{
		int8Field("pid"), textField("usename"), textField("datname"),
		textField("query"), {name: "query_start", oid: 1184},
		textField("state"), textField("wait_event_type"),
	}
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped(fields, [][]string{
			{"100", "user1", "db1", "SELECT 1", "2025-01-01 00:00:00+00", "active", ""},
			{"101", "user2", "db1", "SELECT 2", "2025-01-01 00:00:00+00", "idle", ""},
		})
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	pids, ok := hc.snapshotSessions(context.Background(), conn)
	require.True(t, ok)
	require.Len(t, pids, 2)
	assert.Equal(t, "user1", pids[100].Username)
	assert.Equal(t, "active", pids[100].State)
}

func TestSnapshotSessions_QueryError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("boom")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	_, ok := hc.snapshotSessions(context.Background(), conn)
	assert.False(t, ok)
}

func TestCollectHistory_DetectsCompleted(t *testing.T) {
	fields := []fieldDesc{
		int8Field("pid"), textField("usename"), textField("datname"),
		textField("query"), {name: "query_start", oid: 1184},
		textField("state"), textField("wait_event_type"),
	}
	callNum := 0
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "INSERT INTO cloudberry_query_history") {
			return execResponse("INSERT 0 1")
		}
		if strings.Contains(query, "pg_stat_activity") {
			callNum++
			if callNum == 1 {
				// First snapshot: pid 100 active.
				return rowsResponseTyped(fields, [][]string{
					{"100", "user1", "db1", "SELECT 1", "2025-01-01 00:00:00+00", "active", ""},
				})
			}
			// Second snapshot: pid 100 gone (completed).
			return rowsResponseTyped(fields, [][]string{})
		}
		return execResponse("SELECT 0")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	// First cycle records active pid.
	hc.collectHistory(context.Background(), conn)
	require.Len(t, hc.lastSeenPIDs, 1)
	// Second cycle detects completion and inserts.
	hc.collectHistory(context.Background(), conn)
	assert.Empty(t, hc.lastSeenPIDs)
}

func TestRecordCompletedQuery_SkipNonActive(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{State: "idle", QueryText: "SELECT 1"}, time.Now())
	assert.False(t, got)
}

func TestRecordCompletedQuery_SkipEmptyQuery(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{State: "active", QueryText: ""}, time.Now())
	assert.False(t, got)
}

func TestRecordCompletedQuery_Success(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{
			State: "active", QueryText: "SELECT 1",
			QueryStart: time.Now().Add(-2 * time.Second),
		}, time.Now())
	assert.True(t, got)
}

func TestRecordCompletedQuery_InsertError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("insert failed")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{
			State: "active", QueryText: "SELECT 1",
			QueryStart: time.Now().Add(-2 * time.Second),
		}, time.Now())
	assert.False(t, got)
}

func TestRecordCompletedQuery_WithPlanCollection(t *testing.T) {
	conn, cleanup := newMockConn(t, func(query string) []byte {
		if strings.Contains(query, "EXPLAIN") {
			return rowsResponseTyped([]fieldDesc{textField("QUERY PLAN")}, [][]string{
				{"Seq Scan on t"},
			})
		}
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Millisecond)
	// duration >> threshold so plan collection triggers.
	got := hc.recordCompletedQuery(context.Background(), conn, 1,
		&sessionSnapshot{
			State: "active", QueryText: "SELECT * FROM t",
			QueryStart: time.Now().Add(-10 * time.Second),
		}, time.Now())
	assert.True(t, got)
}

func TestCollectExplainPlan_SkipDDL(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("SELECT 0")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second)
	for _, q := range []string{"CREATE TABLE t (x int)", "DROP TABLE t", "VACUUM", "set x = 1"} {
		plan := hc.collectExplainPlan(context.Background(), conn, q)
		assert.Empty(t, plan)
	}
}

func TestCollectExplainPlan_Success(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return rowsResponseTyped([]fieldDesc{textField("QUERY PLAN")}, [][]string{
			{"Seq Scan on orders"},
			{"  Filter: (id > 0)"},
		})
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT * FROM orders")
	assert.Contains(t, plan, "Seq Scan on orders")
	assert.Contains(t, plan, "Filter")
}

func TestCollectExplainPlan_QueryError(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("explain failed")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), true, time.Second)
	plan := hc.collectExplainPlan(context.Background(), conn, "SELECT 1")
	assert.Empty(t, plan)
}

func TestInsertHistoryEntry_WithPlan(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("INSERT 0 1")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	err := hc.insertHistoryEntry(context.Background(), conn, "q-1", 1,
		&sessionSnapshot{Username: "u", Database: "d", QueryText: "SELECT 1"},
		time.Now(), 100, "the plan")
	require.NoError(t, err)
}

func TestCleanupHistory_NilConn(t *testing.T) {
	t.Parallel()
	hc := newHistoryCollector(testLogger(), false, time.Second)
	hc.cleanupHistory(context.Background(), nil, time.Hour)
}

func TestCleanupHistory_Success(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return execResponse("DELETE 5")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	hc.cleanupHistory(context.Background(), conn, time.Hour)
}

func TestCleanupHistory_Error(t *testing.T) {
	conn, cleanup := newMockConn(t, func(_ string) []byte {
		return errorResponseMsg("delete failed")
	})
	defer cleanup()

	hc := newHistoryCollector(testLogger(), false, time.Second)
	hc.cleanupHistory(context.Background(), conn, time.Hour)
}

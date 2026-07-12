package db

// Tests for the L-3 best-effort branches of GetQueryDetail's collectQueryLocks
// / collectAccessedTables helpers (handoff gap UT-22): a failure in either
// auxiliary query — query error, per-row scan error or a rows-iteration error
// — must degrade to partial results while GetQueryDetail still succeeds with
// the session info.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// queryDetailSessionFields is the pg_stat_activity row shape GetQueryDetail
// scans (mirrors TestPgxClient_GetQueryDetail_Success).
func queryDetailSessionFields() []fieldDesc {
	return []fieldDesc{
		int4Field("pid"), textField("usename"), textField("datname"),
		textField("state"), textField("query"),
		{name: "query_start", oid: 1184}, // timestamptz
		textField("duration"), textField("wait_event_type"), textField("wait_event"),
		textField("backend_type"),
	}
}

// queryDetailSessionRow returns a healthy pg_stat_activity response for PID 100.
func queryDetailSessionRow() []byte {
	return singleRowResponseTyped(queryDetailSessionFields(), []string{
		"100", "app_user", "testdb", "active", "SELECT 1",
		"2024-01-01 00:00:00+00", "0", "", "", "client backend",
	})
}

// queryDetailLockFields is the pg_locks row shape collectQueryLocks scans.
func queryDetailLockFields() []fieldDesc {
	return []fieldDesc{
		textField("locktype"), textField("mode"),
		boolField("granted"), textField("relation"),
	}
}

// goodLockRows returns one healthy pg_locks row.
func goodLockRows() []byte {
	return multiRowResponseTyped(queryDetailLockFields(),
		[][]string{{"relation", "AccessShareLock", "t", "public.orders"}})
}

// goodTableRows returns one healthy pg_stat_user_tables row.
func goodTableRows() []byte {
	return multiRowResponse([]string{"table"}, [][]string{{"public.orders"}})
}

// rowsThenErrorTyped starts a result set, delivers rows, then fails with an
// ErrorResponse INSTEAD of CommandComplete — driving the rows.Err() branch.
func rowsThenErrorTyped(fields []fieldDesc, rows [][]string, errMsg string) []byte {
	buf := mustEncode(buildRowDesc(fields))
	for _, row := range rows {
		dr := &pgproto3.DataRow{}
		for _, v := range row {
			dr.Values = append(dr.Values, []byte(v))
		}
		buf = append(buf, mustEncode(dr)...)
	}
	buf = append(buf, mustEncode(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Message:  errMsg,
	})...)
	return buf
}

func TestGetQueryDetail_BestEffortBranches(t *testing.T) {
	tests := []struct {
		name        string
		lockResp    []byte
		tableResp   []byte
		wantLocks   int
		wantTables  int
		description string
	}{
		{
			name:        "pg_locks query error yields empty locks",
			lockResp:    errorResponseMsg("pg_locks unavailable"),
			tableResp:   goodTableRows(),
			wantLocks:   0,
			wantTables:  1,
			description: "a failing lock query must not fail the detail (L-3)",
		},
		{
			name:        "pg_stat_user_tables query error yields empty tables",
			lockResp:    goodLockRows(),
			tableResp:   errorResponseMsg("pg_stat_user_tables unavailable"),
			wantLocks:   1,
			wantTables:  0,
			description: "a failing table query must not fail the detail (L-3)",
		},
		{
			name:        "both auxiliary queries fail: session info only",
			lockResp:    errorResponseMsg("locks down"),
			tableResp:   errorResponseMsg("tables down"),
			wantLocks:   0,
			wantTables:  0,
			description: "the detail must still carry the session info",
		},
		{
			name: "unscannable lock row stops iteration, prior rows kept",
			lockResp: multiRowResponseTyped(queryDetailLockFields(), [][]string{
				{"relation", "AccessShareLock", "t", "public.good"},
				{"relation", "RowExclusiveLock", "zz", "public.bad"}, // invalid bool -> scan error
			}),
			tableResp:   goodTableRows(),
			wantLocks:   1,
			wantTables:  1,
			description: "a per-row scan error must keep the rows scanned before it",
		},
		{
			name: "lock rows iteration error keeps partial locks",
			lockResp: rowsThenErrorTyped(queryDetailLockFields(),
				[][]string{{"relation", "AccessShareLock", "t", "public.orders"}},
				"connection reset mid-stream"),
			tableResp:   goodTableRows(),
			wantLocks:   1,
			wantTables:  1,
			description: "a rows.Err() failure must keep the rows read so far",
		},
		{
			name:     "unscannable table row stops iteration, prior rows kept",
			lockResp: goodLockRows(),
			// The second row has two columns instead of one -> scan error.
			tableResp: append(
				mustEncode(buildRowDesc([]fieldDesc{textField("table")})),
				func() []byte {
					good := mustEncode(&pgproto3.DataRow{Values: [][]byte{[]byte("public.kept")}})
					bad := mustEncode(&pgproto3.DataRow{Values: [][]byte{[]byte("a"), []byte("b")}})
					done := mustEncode(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 2")})
					return append(append(good, bad...), done...)
				}()...),
			wantLocks:   1,
			wantTables:  1,
			description: "a per-row scan error must keep the table rows scanned before it",
		},
		{
			name:     "table rows iteration error keeps partial tables",
			lockResp: goodLockRows(),
			tableResp: rowsThenErrorTyped([]fieldDesc{textField("table")},
				[][]string{{"public.partial"}}, "connection reset mid-stream"),
			wantLocks:   1,
			wantTables:  1,
			description: "a rows.Err() failure must keep the table rows read so far",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: route each of the three GetQueryDetail statements to
			// its per-case response.
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				switch {
				case strings.Contains(query, "pg_stat_activity"):
					return queryDetailSessionRow()
				case strings.Contains(query, "pg_locks"):
					return tt.lockResp
				case strings.Contains(query, "pg_stat_user_tables"):
					return tt.tableResp
				default:
					return execResponse("SELECT 1")
				}
			})
			defer cleanup()

			// Act.
			detail, err := client.GetQueryDetail(context.Background(), 100)

			// Assert: best-effort — never an error, session info always
			// present, auxiliary data possibly partial/empty.
			require.NoError(t, err, tt.description)
			require.NotNil(t, detail)
			assert.Equal(t, int32(100), detail.PID)
			assert.Equal(t, "app_user", detail.Username)
			assert.Len(t, detail.Locks, tt.wantLocks)
			assert.Len(t, detail.TablesAccessed, tt.wantTables)
		})
	}
}

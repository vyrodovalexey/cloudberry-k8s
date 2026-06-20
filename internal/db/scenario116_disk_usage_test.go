package db

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorResponseWithCode returns a PostgreSQL ErrorResponse carrying an explicit
// SQLSTATE code, so pgx surfaces it as a *pgconn.PgError with that Code. This
// lets the Scenario-116 unavailable cases drive isUndefinedRelationOrColumn.
func errorResponseWithCode(code, msg string) []byte {
	return mustEncode(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     code,
		Message:  msg,
	})
}

// TestPgxClient_GetDiskUsagePercent_Mock covers 116-DB-ok: gp_disk_free yields a
// worst-case percentage and GetDiskUsagePercent returns it verbatim.
func TestPgxClient_GetDiskUsagePercent_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("usage_percent")},
			[]string{"73"},
		)
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(73), pct)
}

// TestPgxClient_GetDiskUsagePercent_Empty covers 116-DB-empty: with no rows / a
// COALESCE-driven 0, the method returns 0 and no error.
func TestPgxClient_GetDiskUsagePercent_Empty(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int4Field("usage_percent")},
			[]string{"0"},
		)
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int32(0), pct)
}

// TestPgxClient_GetDiskUsagePercent_UnavailableTable covers 116-DB-unavailable:
// an undefined-table SQLSTATE (42P01) maps to the ErrDiskUsageUnavailable
// sentinel so the caller skips honestly.
func TestPgxClient_GetDiskUsagePercent_UnavailableTable(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseWithCode("42P01", "relation \"gp_toolkit.gp_disk_free\" does not exist")
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDiskUsageUnavailable),
		"expected ErrDiskUsageUnavailable, got %v", err)
	assert.Equal(t, int32(0), pct)
}

// TestPgxClient_GetDiskUsagePercent_UnavailableColumn covers the column variant
// of 116-DB-unavailable: an undefined-column SQLSTATE (42703) also maps to the
// sentinel.
func TestPgxClient_GetDiskUsagePercent_UnavailableColumn(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseWithCode("42703", "column \"df_total\" does not exist")
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrDiskUsageUnavailable),
		"expected ErrDiskUsageUnavailable, got %v", err)
	assert.Equal(t, int32(0), pct)
}

// TestPgxClient_GetDiskUsagePercent_Error covers 116-DB-error: a generic query
// error is wrapped and returned, and is NOT the unavailable sentinel.
func TestPgxClient_GetDiskUsagePercent_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("disk free query failed")
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrDiskUsageUnavailable),
		"generic error must not be the unavailable sentinel: %v", err)
	assert.Contains(t, err.Error(), "querying disk usage percent")
	assert.Equal(t, int32(0), pct)
}

// TestPgxClient_GetDiskUsagePercent_ScanError covers a malformed result: a row
// that cannot be scanned into int32 returns a wrapped (non-sentinel) error.
func TestPgxClient_GetDiskUsagePercent_ScanError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{textField("usage_percent")},
			[]string{"not-a-number"},
		)
	})
	defer cleanup()

	pct, err := client.GetDiskUsagePercent(context.Background())
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrDiskUsageUnavailable))
	assert.Equal(t, int32(0), pct)
}

// TestPgxClient_GetClusterDataSizeBytes_Mock covers 116-DB-clustersize-ok: the
// portable logical-size query (sum of pg_database_size) returns a bigint and
// GetClusterDataSizeBytes returns it verbatim.
func TestPgxClient_GetClusterDataSizeBytes_Mock(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return singleRowResponseTyped(
			[]fieldDesc{int8Field("size_bytes")},
			[]string{"180145815"},
		)
	})
	defer cleanup()

	size, err := client.GetClusterDataSizeBytes(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(180145815), size)
}

// TestPgxClient_GetClusterDataSizeBytes_Error covers 116-DB-clustersize-error: a
// query error is wrapped (with the "querying cluster data size" context) and the
// returned size is zero.
func TestPgxClient_GetClusterDataSizeBytes_Error(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(_ string) []byte {
		return errorResponseMsg("cluster data size query failed")
	})
	defer cleanup()

	size, err := client.GetClusterDataSizeBytes(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "querying cluster data size")
	assert.Equal(t, int64(0), size)
}

// TestClampPercent covers 116-DB-clamp directly: values below 0 and above 100
// are constrained to the inclusive 0..100 range, in-range values pass through.
func TestClampPercent(t *testing.T) {
	tests := []struct {
		name string
		in   int32
		want int32
	}{
		{name: "negative clamps to zero", in: -5, want: 0},
		{name: "min boundary zero", in: 0, want: 0},
		{name: "mid value passes through", in: 42, want: 42},
		{name: "max boundary hundred", in: 100, want: 100},
		{name: "over hundred clamps to hundred", in: 150, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, clampPercent(tt.in))
		})
	}
}

// TestIsUndefinedRelationOrColumn covers the SQLSTATE classifier used by the
// honest-fallback path, including the negative (non-pg / unrelated code) cases.
func TestIsUndefinedRelationOrColumn(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil error", err: nil, want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
		{
			name: "undefined table 42P01",
			err:  &pgconn.PgError{Code: sqlStateUndefinedTable, Message: "no such relation"},
			want: true,
		},
		{
			name: "undefined column 42703",
			err:  &pgconn.PgError{Code: sqlStateUndefinedColumn, Message: "no such column"},
			want: true,
		},
		{
			name: "wrapped undefined table",
			err:  fmt.Errorf("query: %w", &pgconn.PgError{Code: sqlStateUndefinedTable}),
			want: true,
		},
		{
			name: "unrelated pg error",
			err:  &pgconn.PgError{Code: "08006", Message: "connection failure"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isUndefinedRelationOrColumn(tt.err))
		})
	}
}

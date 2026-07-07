package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIsSegmentCatalogNotSeeded verifies the D8 terminal-error classifier used
// by the scale controller to abort a scale-out (instead of retrying forever)
// when a new segment's catalog was never seeded from the coordinator.
func TestIsSegmentCatalogNotSeeded(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection refused"), false},
		{"direct sentinel", ErrSegmentCatalogNotSeeded, true},
		{
			name: "wrapped sentinel",
			err:  fmt.Errorf("redistributing data: %w", ErrSegmentCatalogNotSeeded),
			want: true,
		},
		{
			name: "doubly wrapped sentinel",
			err: fmt.Errorf("outer: %w",
				fmt.Errorf("inner: %w", ErrSegmentCatalogNotSeeded)),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSegmentCatalogNotSeeded(tt.err))
		})
	}
}

// TestVerifyRelationsSeeded_TerminalError proves the D8 preflight: when a
// pre-existing user relation is present on the coordinator but MISSING on one or
// more of the target segments (gp_dist_random returns a per-segment count below
// the target width), RedistributeData surfaces the typed, terminal
// ErrSegmentCatalogNotSeeded instead of attempting EXPAND TABLE (which would
// fail with a raw 42P01 and be retried forever).
func TestVerifyRelationsSeeded_TerminalError(t *testing.T) {
	segCount := func(n string) []byte {
		return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
	}
	expandFields := []fieldDesc{
		textField("schema_name"), textField("table_name"), int4Field("numsegments"),
	}
	expandAttempted := false

	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			// Target width is 3 primary segments.
			return segCount("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(expandFields, [][]string{
				{"public", "customers", "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			// The relation is only present on 2 of the 3 segments — the new
			// segment's catalog was never seeded from the coordinator.
			return segCount("2")
		case strings.Contains(query, "EXPAND TABLE"):
			expandAttempted = true
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.Error(t, err)
	assert.True(t, IsSegmentCatalogNotSeeded(err),
		"an under-seeded relation must surface ErrSegmentCatalogNotSeeded, got: %v", err)
	assert.Contains(t, err.Error(), "public.customers")
	assert.False(t, expandAttempted,
		"EXPAND TABLE must NOT be attempted when the relation is not seeded on all segments")
}

// TestVerifyRelationsSeeded_SkippedWhenUnavailable proves that when
// gp_dist_random is not available on the server (SQLSTATE 42P01 on the helper
// itself), the D8 preflight is skipped HONESTLY and EXPAND is still attempted,
// preserving behavior on engines without the helper.
func TestVerifyRelationsSeeded_SkippedWhenUnavailable(t *testing.T) {
	segCount := func(n string) []byte {
		return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
	}
	expandFields := []fieldDesc{
		textField("schema_name"), textField("table_name"), int4Field("numsegments"),
	}
	expandAttempted := false

	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			return segCount("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(expandFields, [][]string{
				{"public", "customers", "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			// Simulate the helper being unavailable (undefined relation).
			return errorResponseWithCode(sqlStateUndefinedTable, "gp_dist_random not available")
		case strings.Contains(query, "EXPAND TABLE"):
			expandAttempted = true
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.NoError(t, err)
	assert.True(t, expandAttempted,
		"EXPAND must still be attempted when gp_dist_random is unavailable (honest skip)")
}

// TestSeedNewSegmentCatalog_NoUserDatabases verifies the seeding fast-path: with
// no user databases the operation is a no-op that seeds nothing and returns nil.
func TestSeedNewSegmentCatalog_NoUserDatabases(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			// No user databases.
			return multiRowResponse([]string{"datname"}, [][]string{})
		case strings.Contains(query, "allow_system_table_mods"):
			return execResponse("SET")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	seeded, err := client.SeedNewSegmentCatalog(context.Background(), SegmentRegistrationOptions{
		OldCount: 2, NewCount: 3, ClusterName: "c", SegmentService: "svc", Port: 6000,
	})
	require.NoError(t, err)
	assert.Zero(t, seeded)
}

// TestSeedNewSegmentCatalog_ListDatabasesError verifies that a failure to
// enumerate the coordinator's user databases is wrapped and surfaced (0 seeded),
// so the caller never proceeds to EXPAND against an unverified catalog.
func TestSeedNewSegmentCatalog_ListDatabasesError(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		if strings.Contains(query, "datistemplate") {
			return errorResponseMsg("boom: catalog unavailable")
		}
		return execResponse("SELECT 1")
	})
	defer cleanup()

	seeded, err := client.SeedNewSegmentCatalog(context.Background(), SegmentRegistrationOptions{
		OldCount: 2, NewCount: 3, ClusterName: "c", SegmentService: "svc", Port: 6000,
	})
	require.Error(t, err)
	assert.Zero(t, seeded)
	assert.Contains(t, err.Error(), "listing user databases for catalog seeding")
}

// TestSeedNewSegmentCatalog_SetSystemModsError verifies that when enabling
// catalog fan-out (SET allow_system_table_mods) fails, seeding aborts with 0
// seeded and a wrapped error — the guard that keeps the coordinator dispatch
// consistent before any per-database work is attempted.
func TestSeedNewSegmentCatalog_SetSystemModsError(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "allow_system_table_mods"):
			return errorResponseMsg("permission denied for SET")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	seeded, err := client.SeedNewSegmentCatalog(context.Background(), SegmentRegistrationOptions{
		OldCount: 0, NewCount: 1, ClusterName: "c", SegmentService: "svc", Port: 6000,
	})
	require.Error(t, err)
	assert.Zero(t, seeded)
	assert.Contains(t, err.Error(), "enabling system table modifications")
}

// TestSeedNewSegmentCatalog_SegmentProbeError drives the full seeding helper
// chain (SeedNewSegmentCatalog -> seedDatabaseOnNewSegments ->
// newSegmentsMissingDatabase -> segmentHasDatabase -> newUtilitySegmentPool)
// against an UNRESOLVABLE segment host. The utility-mode probe of the new
// segment fails to connect, so the per-database seed error is aggregated and
// surfaced (best-effort: it is logged, collected, and returned as an aggregate,
// never panics). This is the "segment probe error" branch of D8 seeding.
func TestSeedNewSegmentCatalog_SegmentProbeError(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "allow_system_table_mods"):
			return execResponse("SET")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// SegmentService is an unresolvable TLD so the utility-mode dial to
	// "c-segment-primary-0.svc.invalid" fails fast with a DNS error.
	seeded, err := client.SeedNewSegmentCatalog(ctx, SegmentRegistrationOptions{
		OldCount: 0, NewCount: 1, ClusterName: "c", SegmentService: "svc.invalid", Port: 6000,
	})
	require.Error(t, err)
	assert.Zero(t, seeded)
	assert.Contains(t, err.Error(), "catalog seeding failed for 1 of 1 databases")
	assert.Contains(t, err.Error(), "probing segment 0")
}

// TestSeedNewSegmentCatalog_ContextCanceled verifies the cooperative
// cancellation guard in the seeding loop: a context that is already canceled
// makes SeedNewSegmentCatalog return the context error before probing segments.
func TestSeedNewSegmentCatalog_ContextCanceled(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "allow_system_table_mods"):
			return execResponse("SET")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the loop runs

	seeded, err := client.SeedNewSegmentCatalog(ctx, SegmentRegistrationOptions{
		OldCount: 0, NewCount: 1, ClusterName: "c", SegmentService: "svc", Port: 6000,
	})
	require.Error(t, err)
	assert.Zero(t, seeded)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestNewSegmentsMissingDatabase_ContextCanceled covers the per-segment
// cancellation guard inside newSegmentsMissingDatabase: with a canceled context
// the probe loop returns ctx.Err() without dialing any segment.
func TestNewSegmentsMissingDatabase_ContextCanceled(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	missing, err := client.newSegmentsMissingDatabase(ctx, "mydb", SegmentRegistrationOptions{
		OldCount: 0, NewCount: 2, ClusterName: "c", SegmentService: "svc", Port: 6000,
	})
	require.Error(t, err)
	assert.Nil(t, missing)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestSeedDatabaseOnNewSegments_NoNewSegments covers the fast path of
// seedDatabaseOnNewSegments: when OldCount == NewCount there are no new segments
// to probe, so newSegmentsMissingDatabase returns an empty set and the helper
// reports (false, nil) — no seeding action, no coordinator dispatch.
func TestSeedDatabaseOnNewSegments_NoNewSegments(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	didSeed, err := client.seedDatabaseOnNewSegments(context.Background(), "mydb",
		SegmentRegistrationOptions{
			OldCount: 3, NewCount: 3, ClusterName: "c", SegmentService: "svc", Port: 6000,
		})
	require.NoError(t, err)
	assert.False(t, didSeed, "no new segments means no seeding action is taken")
}

// TestNewSegmentsMissingDatabase_NoNewSegments verifies the empty-range case:
// with OldCount == NewCount the probe loop never runs and an empty (nil) missing
// set is returned with no error.
func TestNewSegmentsMissingDatabase_NoNewSegments(t *testing.T) {
	client, cleanup := newMockPgxClientExtended(t, func(_ string) []byte {
		return execResponse("SELECT 1")
	})
	defer cleanup()

	missing, err := client.newSegmentsMissingDatabase(context.Background(), "mydb",
		SegmentRegistrationOptions{
			OldCount: 2, NewCount: 2, ClusterName: "c", SegmentService: "svc", Port: 6000,
		})
	require.NoError(t, err)
	assert.Empty(t, missing)
}

// TestVerifyRelationsSeeded_AllSeeded proves the happy-path preflight: when
// gp_dist_random reports every candidate relation present on ALL target
// segments, verifyRelationsSeeded returns nil and RedistributeData proceeds to
// EXPAND TABLE.
func TestVerifyRelationsSeeded_AllSeeded(t *testing.T) {
	segCount := func(n string) []byte {
		return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
	}
	expandFields := []fieldDesc{
		textField("schema_name"), textField("table_name"), int4Field("numsegments"),
	}
	expandAttempted := false

	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			return segCount("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(expandFields, [][]string{
				{"public", "customers", "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			// Relation present on all 3 target segments.
			return segCount("3")
		case strings.Contains(query, "EXPAND TABLE"):
			expandAttempted = true
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.NoError(t, err)
	assert.True(t, expandAttempted,
		"EXPAND must be attempted when every relation is seeded on all segments")
}

// TestVerifyRelationsSeeded_ProbeError covers the non-terminal error branch of
// the preflight: a gp_dist_random probe failure that is NOT an
// undefined-relation/column error (e.g. a transient query error) is wrapped and
// surfaced verbatim (NOT ErrSegmentCatalogNotSeeded and NOT an honest skip).
func TestVerifyRelationsSeeded_ProbeError(t *testing.T) {
	segCount := func(n string) []byte {
		return singleRowResponseTyped([]fieldDesc{int4Field("count")}, []string{n})
	}
	expandFields := []fieldDesc{
		textField("schema_name"), textField("table_name"), int4Field("numsegments"),
	}
	expandAttempted := false

	client, cleanup := newMockPgxClientExtended(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "datistemplate"):
			return multiRowResponse([]string{"datname"}, [][]string{{"mydb"}})
		case strings.Contains(query, "gp_segment_configuration"):
			return segCount("3")
		case strings.Contains(query, "gp_distribution_policy"):
			return multiRowResponseTyped(expandFields, [][]string{
				{"public", "customers", "2"},
			})
		case strings.Contains(query, "gp_dist_random"):
			// A generic query error (not 42P01/42703) must be surfaced.
			return errorResponseWithCode("58030", "io error reading segment catalog")
		case strings.Contains(query, "EXPAND TABLE"):
			expandAttempted = true
			return execResponse("ALTER TABLE")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	err := client.RedistributeData(context.Background(), RedistributionOptions{})
	require.Error(t, err)
	assert.False(t, IsSegmentCatalogNotSeeded(err),
		"a generic probe error must NOT be classified as the terminal seeding error")
	assert.Contains(t, err.Error(), "verifying relation public.customers")
	assert.False(t, expandAttempted,
		"EXPAND must NOT be attempted when the preflight probe errors")
}

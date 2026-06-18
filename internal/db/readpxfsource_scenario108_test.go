package db

// Scenario 108 — ReadPXFSourceSample (L.15 backing query).
//
// These tests exercise the HONEST transient-read contract of
// pgxClient.ReadPXFSourceSample against the in-process PostgreSQL mock used by
// the rest of the pgxClient suite. They assert:
//   - the columns + rows of a successful read are parsed from the live SELECT;
//   - the transient external table is ALWAYS dropped (cleanup) — both on the
//     happy path AND when the SELECT errors mid-read;
//   - a connect/DDL/query failure is SURFACED (wrapped) so the caller maps it to
//     available:false (never fabricated rows);
//   - missing profile/resource is rejected before any DDL;
//   - the row limit is clamped defensively (server-side cap).

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pxfSampleQueryLog records the queries the mock PG server saw, so a test can
// assert that the CREATE/SELECT/DROP statements were actually issued.
type pxfSampleQueryLog struct {
	mu      sync.Mutex
	queries []string
}

func (l *pxfSampleQueryLog) add(q string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.queries = append(l.queries, q)
}

func (l *pxfSampleQueryLog) sawContaining(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, q := range l.queries {
		if strings.Contains(q, substr) {
			return true
		}
	}
	return false
}

// TestPgxClient_ReadPXFSourceSample_HappyPath covers 108-L15 (query path): a
// reachable source yields the real columns + rows; the transient external table
// is created and dropped.
func TestPgxClient_ReadPXFSourceSample_HappyPath(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return multiRowResponse([]string{"line"}, [][]string{
				{"a,1"},
				{"b,2"},
				{"c,3"},
			})
		default:
			// BEGIN / SET LOCAL / ROLLBACK / COMMIT.
			return execResponse("SET")
		}
	})
	defer cleanup()

	sample, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 10)
	require.NoError(t, err)
	require.NotNil(t, sample)

	assert.Equal(t, []string{"line"}, sample.Columns)
	require.Len(t, sample.Rows, 3)
	assert.Equal(t, []string{"a,1"}, sample.Rows[0])
	assert.Equal(t, []string{"c,3"}, sample.Rows[2])

	// The transient external table was created AND dropped (cleanup).
	assert.True(t, log.sawContaining("CREATE EXTERNAL TABLE"), "must create the transient table")
	assert.True(t, log.sawContaining("DROP EXTERNAL TABLE"), "must always drop the transient table")
}

// TestPgxClient_ReadPXFSourceSample_DropsTableOnSelectError covers the cleanup
// invariant: even when the SELECT fails mid-read, the transient external table
// is STILL dropped, and the error is surfaced (available:false at the caller).
func TestPgxClient_ReadPXFSourceSample_DropsTableOnSelectError(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return errorResponseMsg("PXF agent unreachable")
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	sample, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 10)
	require.Error(t, err)
	assert.Nil(t, sample)
	assert.Contains(t, err.Error(), "reading PXF source sample")

	// Cleanup still ran: the transient table was dropped despite the read error.
	assert.True(t, log.sawContaining("DROP EXTERNAL TABLE"),
		"transient table must be dropped even when the SELECT errors")
}

// TestPgxClient_ReadPXFSourceSample_CreateError covers a DDL failure: the
// CREATE EXTERNAL TABLE error is surfaced (wrapped) so the caller treats the
// source as unreachable (available:false), never fabricated.
func TestPgxClient_ReadPXFSourceSample_CreateError(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return errorResponseMsg("pxf extension not installed")
		default:
			return execResponse("SELECT 1")
		}
	})
	defer cleanup()

	sample, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 10)
	require.Error(t, err)
	assert.Nil(t, sample)
	assert.Contains(t, err.Error(), "creating transient PXF preview table")
}

// TestPgxClient_ReadPXFSourceSample_Empty covers a reachable-but-empty source:
// the columns are still parsed and rows is empty — an honest "no rows", never an
// error.
func TestPgxClient_ReadPXFSourceSample_Empty(t *testing.T) {
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return emptyRowResponse([]string{"line"})
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	sample, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 10)
	require.NoError(t, err)
	require.NotNil(t, sample)
	assert.Equal(t, []string{"line"}, sample.Columns)
	assert.Empty(t, sample.Rows)
}

// TestPgxClient_ReadPXFSourceSample_MissingParams rejects an empty profile or
// resource BEFORE any DDL is issued — a clean argument error.
func TestPgxClient_ReadPXFSourceSample_MissingParams(t *testing.T) {
	tests := []struct {
		name     string
		profile  string
		resource string
	}{
		{"empty profile", "", "data/events.csv"},
		{"empty resource", "s3:text", ""},
		{"both empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &pxfSampleQueryLog{}
			client, cleanup := newMockPgxClient(t, func(query string) []byte {
				log.add(query)
				return execResponse("SELECT 1")
			})
			defer cleanup()

			sample, err := client.ReadPXFSourceSample(
				context.Background(), "s3srv", tt.profile, tt.resource, 10)
			require.Error(t, err)
			assert.Nil(t, sample)
			assert.Contains(t, err.Error(), "profile and resource are required")

			// No DDL was issued for an invalid request.
			assert.False(t, log.sawContaining("CREATE EXTERNAL TABLE"))
		})
	}
}

// TestPgxClient_ReadPXFSourceSample_ClampsLimit verifies the defensive
// server-side limit clamp: a request above the hard cap is rounded down so the
// SELECT carries at most pxfSampleMaxLimit.
func TestPgxClient_ReadPXFSourceSample_ClampsLimit(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return emptyRowResponse([]string{"line"})
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	_, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 5000)
	require.NoError(t, err)

	// The emitted SELECT carries the clamped limit (LIMIT 1000), not 5000.
	assert.True(t, log.sawContaining("LIMIT 1000"), "limit must be clamped to the server-side cap")
	assert.False(t, log.sawContaining("LIMIT 5000"))
}

// TestPgxClient_ReadPXFSourceSample_DefaultLimit verifies a non-positive limit
// defaults to pxfSampleDefaultLimit (10).
func TestPgxClient_ReadPXFSourceSample_DefaultLimit(t *testing.T) {
	log := &pxfSampleQueryLog{}
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		log.add(query)
		switch {
		case strings.Contains(query, "CREATE EXTERNAL TABLE"):
			return execResponse("CREATE EXTERNAL TABLE")
		case strings.Contains(query, "DROP EXTERNAL TABLE"):
			return execResponse("DROP EXTERNAL TABLE")
		case strings.HasPrefix(strings.TrimSpace(query), "SELECT *"):
			return emptyRowResponse([]string{"line"})
		default:
			return execResponse("SET")
		}
	})
	defer cleanup()

	_, err := client.ReadPXFSourceSample(
		context.Background(), "s3srv", "s3:text", "data/events.csv", 0)
	require.NoError(t, err)
	assert.True(t, log.sawContaining("LIMIT 10"), "non-positive limit must default to 10")
}

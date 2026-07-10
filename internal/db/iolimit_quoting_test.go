package db

// Tests for T4/C1a: the io_limit string in AlterResourceGroup embeds the
// free-form CRD Tablespace value and must be rendered via quoteLiteral so a
// malicious tablespace cannot break out of the SQL literal.

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureAlterQueries runs AlterResourceGroup against the mock PG server and
// returns every "ALTER RESOURCE GROUP" statement the server received.
func captureAlterQueries(t *testing.T, opts ResourceGroupOptions) []string {
	t.Helper()

	var mu sync.Mutex
	var queries []string
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		mu.Lock()
		queries = append(queries, query)
		mu.Unlock()
		return execResponse("ALTER RESOURCE GROUP")
	})
	defer cleanup()

	require.NoError(t, client.AlterResourceGroup(context.Background(), opts))

	mu.Lock()
	defer mu.Unlock()
	var alters []string
	for _, q := range queries {
		if strings.HasPrefix(q, "ALTER RESOURCE GROUP") {
			alters = append(alters, q)
		}
	}
	return alters
}

func TestPgxClient_AlterResourceGroup_IOLimitInjectionEscaped(t *testing.T) {
	// A malicious tablespace value attempting a literal breakout. The webhook
	// rejects it upstream (C1b); this test proves the client-side quoting is
	// an independent, sufficient guard (defense in depth).
	malicious := "x'; DROP TABLE users--"

	alters := captureAlterQueries(t, ResourceGroupOptions{
		Name: "analytics",
		IOLimits: []IOLimitOption{
			{Tablespace: malicious, ReadBytesPerSec: 1, WriteBytesPerSec: 2, ReadIOPS: 3, WriteIOPS: 4},
		},
	})

	require.Len(t, alters, 1, "exactly one io_limit statement expected")
	// The exact-SQL equality below is the strongest guard: the injected quote
	// is doubled ('') so the whole io_limit value stays ONE literal and the
	// DROP never becomes a separate statement.
	assert.Equal(t,
		`ALTER RESOURCE GROUP "analytics" SET io_limit `+
			`'x''; DROP TABLE users--:rbps=1:wbps=2:riops=3:wiops=4'`,
		alters[0],
		"the single quote must be doubled so the whole io_limit stays ONE literal")
	assert.Equal(t, 1, strings.Count(alters[0], "''"),
		"the injected quote must appear exactly once, in escaped form")
}

func TestPgxClient_AlterResourceGroup_IOLimitHappyPath(t *testing.T) {
	alters := captureAlterQueries(t, ResourceGroupOptions{
		Name: "analytics",
		IOLimits: []IOLimitOption{
			{Tablespace: "data_ts", ReadBytesPerSec: 1048576, WriteBytesPerSec: 2097152, ReadIOPS: 100, WriteIOPS: 200},
			{Tablespace: "*", ReadIOPS: 50},
		},
	})

	require.Len(t, alters, 1)
	assert.Equal(t,
		`ALTER RESOURCE GROUP "analytics" SET io_limit `+
			`'data_ts:rbps=1048576:wbps=2097152:riops=100:wiops=200;*:rbps=0:wbps=0:riops=50:wiops=0'`,
		alters[0],
		"previous io_limit semantics must be preserved, now safely quoted")
}

package db

// Tests for the M-2 SetParameter scope validation (handoff gap UT-21 / B-5):
// every invalid scope must be rejected with ErrInvalidParameterScope and —
// the safety property — WITHOUT a single SQL statement reaching the server.

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newQueryRecordingPgxClient builds a pgxClient whose mock server records
// every SQL statement it receives (mutex-guarded; the server goroutine and
// the test goroutine race otherwise).
func newQueryRecordingPgxClient(t *testing.T) (*pgxClient, func() []string, func()) {
	t.Helper()

	var mu sync.Mutex
	var queries []string
	client, cleanup := newMockPgxClient(t, func(query string) []byte {
		mu.Lock()
		queries = append(queries, query)
		mu.Unlock()
		return execResponse("ALTER SYSTEM")
	})

	received := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), queries...)
	}
	return client, received, cleanup
}

func TestSetParameter_InvalidScope_RejectedWithoutSQL(t *testing.T) {
	tests := []struct {
		name        string
		scope       ParameterScope
		errContains string
	}{
		{
			name:        "unknown scope level",
			scope:       ParameterScope{Level: "bogus"},
			errContains: "unknown scope level",
		},
		{
			name:        "typo'd scope level must not escalate to ALTER SYSTEM",
			scope:       ParameterScope{Level: "databsae", Target: "mydb"},
			errContains: "unknown scope level",
		},
		{
			name:        "case-sensitive match: Cluster is not cluster",
			scope:       ParameterScope{Level: "Cluster"},
			errContains: "unknown scope level",
		},
		{
			name:        "database scope without target",
			scope:       ParameterScope{Level: ScopeLevelDatabase, Target: ""},
			errContains: "requires a non-empty target",
		},
		{
			name:        "role scope without target",
			scope:       ParameterScope{Level: ScopeLevelRole, Target: ""},
			errContains: "requires a non-empty target",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange: a live mock server recording every received statement.
			client, received, cleanup := newQueryRecordingPgxClient(t)
			defer cleanup()

			// Act.
			err := client.SetParameter(context.Background(), "work_mem", "64MB", tt.scope)

			// Assert: the exported sentinel matches via errors.Is, the message
			// names the problem, and ZERO SQL reached the server (M-2 safety
			// property: no Exec on invalid input).
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidParameterScope,
				"the exported sentinel must match via errors.Is")
			assert.Contains(t, err.Error(), tt.errContains)
			assert.Empty(t, received(),
				"no SQL statement may reach the server on invalid scope input")
		})
	}
}

// TestValidateParameterScope_ValidLevels pins the accepted level spellings
// (including the empty default) at the validator level.
func TestValidateParameterScope_ValidLevels(t *testing.T) {
	tests := []struct {
		name  string
		scope ParameterScope
	}{
		{name: "empty level defaults to cluster", scope: ParameterScope{}},
		{name: "explicit cluster level", scope: ParameterScope{Level: ScopeLevelCluster}},
		{name: "database level with target", scope: ParameterScope{Level: ScopeLevelDatabase, Target: "db"}},
		{name: "role level with target", scope: ParameterScope{Level: ScopeLevelRole, Target: "analyst"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, validateParameterScope(tt.scope))
		})
	}
}

package db

// Golden-string drift guard for T19/D3: the db client's query-history DDL is
// the shared internal/dbschema definition, byte-for-byte identical to what
// the duplicated const contained before the extraction.

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/dbschema"
)

func TestQueryHistoryDDL_SharedGolden(t *testing.T) {
	assert.Equal(t, dbschema.QueryHistoryDDL, queryHistoryDDL,
		"the db client must use the shared DDL single source of truth")

	// Structural pins: renames/drops of columns the SELECT/INSERT code paths
	// depend on must be caught here, not at runtime.
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS cloudberry_query_history",
		"query_id         TEXT NOT NULL",
		"duration_ms      DOUBLE PRECISION NOT NULL",
		"resource_group   TEXT DEFAULT ''",
		"explain_plan     TEXT DEFAULT ''",
		"DISTRIBUTED BY (id)",
		"CREATE INDEX IF NOT EXISTS idx_query_history_start",
		"CREATE INDEX IF NOT EXISTS idx_query_history_user",
		"CREATE INDEX IF NOT EXISTS idx_query_history_db",
	} {
		assert.Contains(t, queryHistoryDDL, want)
	}
}

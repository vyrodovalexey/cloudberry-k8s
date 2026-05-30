package db

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// buildHistoryWhereClause Tests
// ============================================================================

func TestBuildQueryHistorySQL_NoFilters(t *testing.T) {
	filter := QueryHistoryFilter{}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	assert.Empty(t, conditions)
	assert.Empty(t, args)
}

func TestBuildQueryHistorySQL_WithRegexPattern(t *testing.T) {
	filter := QueryHistoryFilter{
		Pattern:     "SELECT.*FROM orders",
		PatternType: "regex",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_text ~ $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "SELECT.*FROM orders", args[0])
}

func TestBuildQueryHistorySQL_WithRegexPattern_DefaultPatternType(t *testing.T) {
	// When patternType is empty, it defaults to regex.
	filter := QueryHistoryFilter{
		Pattern: "SELECT.*FROM users",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_text ~ $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "SELECT.*FROM users", args[0])
}

func TestBuildQueryHistorySQL_WithWildcardPattern(t *testing.T) {
	filter := QueryHistoryFilter{
		Pattern:     "SELECT * FROM ?rders",
		PatternType: "wildcard",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_text LIKE $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "SELECT % FROM _rders", args[0])
}

func TestBuildQueryHistorySQL_WithUserFilter(t *testing.T) {
	filter := QueryHistoryFilter{
		Username: "analyst",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "username = $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "analyst", args[0])
}

func TestBuildQueryHistorySQL_WithDatabaseFilter(t *testing.T) {
	filter := QueryHistoryFilter{
		Database: "mydb",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "database_name = $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "mydb", args[0])
}

func TestBuildQueryHistorySQL_WithResourceGroupFilter(t *testing.T) {
	filter := QueryHistoryFilter{
		ResourceGroup: "analytics",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "resource_group = $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "analytics", args[0])
}

func TestBuildQueryHistorySQL_WithTimeRange(t *testing.T) {
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)

	filter := QueryHistoryFilter{
		Since: since,
		Until: until,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 2)
	assert.Equal(t, "query_start >= $1", conditions[0])
	assert.Equal(t, "query_start <= $2", conditions[1])
	require.Len(t, args, 2)
	assert.Equal(t, since, args[0])
	assert.Equal(t, until, args[1])
}

func TestBuildQueryHistorySQL_WithSinceOnly(t *testing.T) {
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)

	filter := QueryHistoryFilter{
		Since: since,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_start >= $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, since, args[0])
}

func TestBuildQueryHistorySQL_WithStateFilter(t *testing.T) {
	filter := QueryHistoryFilter{
		State: "completed",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "state = $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "completed", args[0])
}

func TestBuildQueryHistorySQL_WithMinDuration(t *testing.T) {
	filter := QueryHistoryFilter{
		MinDuration: 1000.0,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "duration_ms >= $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, 1000.0, args[0])
}

func TestBuildQueryHistorySQL_WithMinDuration_Zero(t *testing.T) {
	// MinDuration of 0 should not add a filter.
	filter := QueryHistoryFilter{
		MinDuration: 0,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	assert.Empty(t, conditions)
	assert.Empty(t, args)
}

func TestBuildQueryHistorySQL_WithAllFilters(t *testing.T) {
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)

	filter := QueryHistoryFilter{
		Pattern:       "SELECT.*",
		PatternType:   "regex",
		Username:      "analyst",
		Database:      "mydb",
		ResourceGroup: "analytics",
		State:         "completed",
		MinDuration:   500.0,
		Since:         since,
		Until:         until,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	// pattern(1) + username(2) + database(3) + resource_group(4) + state(5) + min_duration(6) + since(7) + until(8) = 8
	require.Len(t, conditions, 8)
	require.Len(t, args, 8)

	// Verify order: pattern, username, database, resource_group, state, min_duration, since, until
	assert.Equal(t, "query_text ~ $1", conditions[0])
	assert.Equal(t, "username = $2", conditions[1])
	assert.Equal(t, "database_name = $3", conditions[2])
	assert.Equal(t, "resource_group = $4", conditions[3])
	assert.Equal(t, "state = $5", conditions[4])
	assert.Equal(t, "duration_ms >= $6", conditions[5])
	assert.Equal(t, "query_start >= $7", conditions[6])
	assert.Equal(t, "query_start <= $8", conditions[7])
}

func TestBuildQueryHistorySQL_WithAllFilters_FullCount(t *testing.T) {
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)

	filter := QueryHistoryFilter{
		Pattern:       "SELECT.*",
		PatternType:   "regex",
		Username:      "analyst",
		Database:      "mydb",
		ResourceGroup: "analytics",
		State:         "completed",
		MinDuration:   500.0,
		Since:         since,
		Until:         until,
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	// pattern + username + database + resource_group + state + min_duration + since + until = 8
	assert.Len(t, conditions, 8)
	assert.Len(t, args, 8)
}

func TestBuildQueryHistorySQL_InvalidRegex(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{"unclosed bracket", "[invalid"},
		{"unclosed paren", "(unclosed"},
		{"invalid quantifier", "*invalid"},
		{"bad escape", `\p{Invalid}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := QueryHistoryFilter{
				Pattern:     tt.pattern,
				PatternType: "regex",
			}
			_, _, err := buildHistoryWhereClause(filter, slog.Default())
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "invalid regex pattern")
		})
	}
}

func TestBuildQueryHistorySQL_InvalidRegex_DefaultPatternType(t *testing.T) {
	// When patternType is empty (defaults to regex), invalid regex should still error.
	filter := QueryHistoryFilter{
		Pattern: "[invalid",
	}
	_, _, err := buildHistoryWhereClause(filter, slog.Default())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex pattern")
}

func TestBuildQueryHistorySQL_UnknownPatternType(t *testing.T) {
	// Unknown pattern type defaults to regex behavior.
	filter := QueryHistoryFilter{
		Pattern:     "SELECT.*",
		PatternType: "unknown",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_text ~ $1", conditions[0])
	require.Len(t, args, 1)
	assert.Equal(t, "SELECT.*", args[0])
}

func TestBuildQueryHistorySQL_UnknownPatternType_InvalidRegex(t *testing.T) {
	filter := QueryHistoryFilter{
		Pattern:     "[invalid",
		PatternType: "unknown",
	}
	_, _, err := buildHistoryWhereClause(filter, slog.Default())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex pattern")
}

// ============================================================================
// convertWildcardToLike Tests
// ============================================================================

func TestConvertWildcardToLike(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "asterisk to percent",
			input:    "SELECT *",
			expected: "SELECT %",
		},
		{
			name:     "question mark to underscore",
			input:    "SELECT ? FROM orders",
			expected: "SELECT _ FROM orders",
		},
		{
			name:     "both wildcards",
			input:    "SELECT * FROM ?rders",
			expected: "SELECT % FROM _rders",
		},
		{
			name:     "escape existing percent",
			input:    "100%",
			expected: `100\%`,
		},
		{
			name:     "escape existing underscore",
			input:    "my_table",
			expected: `my\_table`,
		},
		{
			name:     "no wildcards",
			input:    "SELECT 1",
			expected: "SELECT 1",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "multiple asterisks",
			input:    "*.*.txt",
			expected: "%.%.txt",
		},
		{
			name:     "mixed wildcards and escapes",
			input:    "*_table?%",
			expected: `%\_table_\%`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertWildcardToLike(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// ============================================================================
// normalizePagination Tests
// ============================================================================

func TestNormalizePagination(t *testing.T) {
	tests := []struct {
		name           string
		inputLimit     int
		inputOffset    int
		expectedLimit  int
		expectedOffset int
	}{
		{
			name:           "default values when zero",
			inputLimit:     0,
			inputOffset:    0,
			expectedLimit:  defaultHistoryLimit,
			expectedOffset: 0,
		},
		{
			name:           "negative limit uses default",
			inputLimit:     -1,
			inputOffset:    0,
			expectedLimit:  defaultHistoryLimit,
			expectedOffset: 0,
		},
		{
			name:           "limit exceeds max is capped",
			inputLimit:     500,
			inputOffset:    0,
			expectedLimit:  maxHistoryLimit,
			expectedOffset: 0,
		},
		{
			name:           "limit at max is kept",
			inputLimit:     100,
			inputOffset:    0,
			expectedLimit:  100,
			expectedOffset: 0,
		},
		{
			name:           "valid limit and offset",
			inputLimit:     25,
			inputOffset:    50,
			expectedLimit:  25,
			expectedOffset: 50,
		},
		{
			name:           "negative offset becomes zero",
			inputLimit:     10,
			inputOffset:    -5,
			expectedLimit:  10,
			expectedOffset: 0,
		},
		{
			name:           "limit of 1 is valid",
			inputLimit:     1,
			inputOffset:    0,
			expectedLimit:  1,
			expectedOffset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limit, offset := normalizePagination(tt.inputLimit, tt.inputOffset)
			assert.Equal(t, tt.expectedLimit, limit)
			assert.Equal(t, tt.expectedOffset, offset)
		})
	}
}

// ============================================================================
// QueryHistoryEntry Construction Tests
// ============================================================================

func TestQueryHistoryEntry_Construction(t *testing.T) {
	now := time.Now()
	entry := QueryHistoryEntry{
		ID:             1,
		QueryID:        "q-1234-5678",
		PID:            12345,
		Username:       "analyst",
		DatabaseName:   "analytics",
		QueryText:      "SELECT * FROM orders WHERE amount > 100",
		QueryStart:     now.Add(-time.Minute),
		QueryEnd:       now,
		DurationMs:     60000.0,
		State:          "completed",
		RowsAffected:   1500,
		CPUTimeMs:      45000.0,
		MemoryBytes:    1073741824,
		SpillBytes:     0,
		DiskReadBytes:  524288000,
		DiskWriteBytes: 0,
		WaitEvents:     "IO:DataFileRead",
		ResourceGroup:  "analytics",
		ExplainPlan:    "Seq Scan on orders",
		ErrorMessage:   "",
		CreatedAt:      now,
	}

	assert.Equal(t, int64(1), entry.ID)
	assert.Equal(t, "q-1234-5678", entry.QueryID)
	assert.Equal(t, int32(12345), entry.PID)
	assert.Equal(t, "analyst", entry.Username)
	assert.Equal(t, "analytics", entry.DatabaseName)
	assert.Contains(t, entry.QueryText, "SELECT")
	assert.Equal(t, 60000.0, entry.DurationMs)
	assert.Equal(t, "completed", entry.State)
	assert.Equal(t, int64(1500), entry.RowsAffected)
	assert.Equal(t, 45000.0, entry.CPUTimeMs)
	assert.Equal(t, int64(1073741824), entry.MemoryBytes)
	assert.Equal(t, int64(0), entry.SpillBytes)
	assert.Equal(t, int64(524288000), entry.DiskReadBytes)
	assert.Equal(t, int64(0), entry.DiskWriteBytes)
	assert.Equal(t, "IO:DataFileRead", entry.WaitEvents)
	assert.Equal(t, "analytics", entry.ResourceGroup)
	assert.Equal(t, "Seq Scan on orders", entry.ExplainPlan)
	assert.Empty(t, entry.ErrorMessage)
}

func TestQueryHistoryEntry_WithError(t *testing.T) {
	entry := QueryHistoryEntry{
		QueryID:      "q-err-001",
		State:        "error",
		ErrorMessage: "division by zero",
	}
	assert.Equal(t, "error", entry.State)
	assert.Equal(t, "division by zero", entry.ErrorMessage)
}

// ============================================================================
// QueryHistoryFilter Construction Tests
// ============================================================================

func TestQueryHistoryFilter_Construction(t *testing.T) {
	since := time.Date(2026, 5, 28, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 5, 29, 0, 0, 0, 0, time.UTC)

	filter := QueryHistoryFilter{
		Pattern:       "SELECT.*",
		PatternType:   "regex",
		Username:      "analyst",
		Database:      "mydb",
		ResourceGroup: "analytics",
		State:         "completed",
		MinDuration:   1000.0,
		Since:         since,
		Until:         until,
		Limit:         25,
		Offset:        50,
	}

	assert.Equal(t, "SELECT.*", filter.Pattern)
	assert.Equal(t, "regex", filter.PatternType)
	assert.Equal(t, "analyst", filter.Username)
	assert.Equal(t, "mydb", filter.Database)
	assert.Equal(t, "analytics", filter.ResourceGroup)
	assert.Equal(t, "completed", filter.State)
	assert.Equal(t, 1000.0, filter.MinDuration)
	assert.Equal(t, since, filter.Since)
	assert.Equal(t, until, filter.Until)
	assert.Equal(t, 25, filter.Limit)
	assert.Equal(t, 50, filter.Offset)
}

// ============================================================================
// QueryHistoryResult Construction Tests
// ============================================================================

func TestQueryHistoryResult_Construction(t *testing.T) {
	result := QueryHistoryResult{
		Entries: []QueryHistoryEntry{
			{QueryID: "q-1", State: "completed"},
			{QueryID: "q-2", State: "error"},
		},
		Total:  100,
		Limit:  50,
		Offset: 0,
	}

	assert.Len(t, result.Entries, 2)
	assert.Equal(t, 100, result.Total)
	assert.Equal(t, 50, result.Limit)
	assert.Equal(t, 0, result.Offset)
}

// ============================================================================
// CSV Header Tests
// ============================================================================

func TestExportQueryHistoryCSV_Headers(t *testing.T) {
	expectedHeaders := []string{
		"query_id", "username", "database", "query_text",
		"start_time", "end_time", "duration_ms", "rows_affected",
		"cpu_time_ms", "memory_bytes", "spill_bytes", "state",
	}

	assert.Equal(t, expectedHeaders, csvHeader)
	assert.Len(t, csvHeader, 12)
}

// ============================================================================
// DDL Constant Tests
// ============================================================================

func TestQueryHistoryDDL_ContainsTableCreation(t *testing.T) {
	assert.Contains(t, queryHistoryDDL, "CREATE TABLE IF NOT EXISTS cloudberry_query_history")
	assert.Contains(t, queryHistoryDDL, "DISTRIBUTED BY (id)")
}

func TestQueryHistoryDDL_ContainsAllColumns(t *testing.T) {
	columns := []string{
		"id", "query_id", "pid", "username", "database_name",
		"query_text", "query_start", "query_end", "duration_ms",
		"state", "rows_affected", "cpu_time_ms", "memory_bytes",
		"spill_bytes", "disk_read_bytes", "disk_write_bytes",
		"wait_events", "resource_group", "explain_plan",
		"error_message", "created_at",
	}
	for _, col := range columns {
		assert.Contains(t, queryHistoryDDL, col, "DDL should contain column: %s", col)
	}
}

func TestQueryHistoryDDL_ContainsIndexes(t *testing.T) {
	assert.Contains(t, queryHistoryDDL, "idx_query_history_start")
	assert.Contains(t, queryHistoryDDL, "idx_query_history_user")
	assert.Contains(t, queryHistoryDDL, "idx_query_history_db")
}

// ============================================================================
// Pagination Constants Tests
// ============================================================================

func TestPaginationConstants(t *testing.T) {
	assert.Equal(t, 50, defaultHistoryLimit)
	assert.Equal(t, 100, maxHistoryLimit)
}

// ============================================================================
// Table-driven filter combination tests
// ============================================================================

func TestBuildHistoryWhereClause_ParameterIndexing(t *testing.T) {
	// Verify that parameter indices are sequential when multiple filters are used.
	tests := []struct {
		name           string
		filter         QueryHistoryFilter
		expectedParams int
		firstCondition string
		lastCondition  string
	}{
		{
			name: "username and database",
			filter: QueryHistoryFilter{
				Username: "user1",
				Database: "db1",
			},
			expectedParams: 2,
			firstCondition: "username = $1",
			lastCondition:  "database_name = $2",
		},
		{
			name: "pattern and username",
			filter: QueryHistoryFilter{
				Pattern:     "SELECT.*",
				PatternType: "regex",
				Username:    "user1",
			},
			expectedParams: 2,
			firstCondition: "query_text ~ $1",
			lastCondition:  "username = $2",
		},
		{
			name: "all simple filters",
			filter: QueryHistoryFilter{
				Username:      "user1",
				Database:      "db1",
				ResourceGroup: "rg1",
				State:         "completed",
			},
			expectedParams: 4,
			firstCondition: "username = $1",
			lastCondition:  "state = $4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conditions, args, err := buildHistoryWhereClause(tt.filter, slog.Default())
			require.NoError(t, err)
			assert.Len(t, args, tt.expectedParams)
			assert.Equal(t, tt.firstCondition, conditions[0])
			assert.Equal(t, tt.lastCondition, conditions[len(conditions)-1])
		})
	}
}

func TestBuildHistoryWhereClause_EmptyPattern(t *testing.T) {
	// Empty pattern should not add any condition.
	filter := QueryHistoryFilter{
		Pattern:     "",
		PatternType: "regex",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	assert.Empty(t, conditions)
	assert.Empty(t, args)
}

func TestBuildHistoryWhereClause_WildcardWithSpecialChars(t *testing.T) {
	// Wildcard pattern with SQL special characters should be escaped.
	filter := QueryHistoryFilter{
		Pattern:     "100%_complete*",
		PatternType: "wildcard",
	}
	conditions, args, err := buildHistoryWhereClause(filter, slog.Default())

	require.NoError(t, err)
	require.Len(t, conditions, 1)
	assert.Equal(t, "query_text LIKE $1", conditions[0])
	// 100\%\_complete%
	assert.Equal(t, `100\%\_complete%`, args[0])
}

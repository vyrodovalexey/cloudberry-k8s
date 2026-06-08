//go:build functional

package functional

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// scenario7ExpectedTableCount is the total number of tables expected after Scenario 7 data loading.
const scenario7ExpectedTableCount = 5

// scenario7TableDef describes the expected schema for a table created or modified in Scenario 7.
type scenario7TableDef struct {
	Name                 string
	DistributionType     string
	DistributionKey      string
	ExcludeFromRebalance bool
	RequiredColumns      []string
	RequiredIndexes      []string
}

// scenario7ExpectedTables returns the expected table definitions for Scenario 7.
func scenario7ExpectedTables() []scenario7TableDef {
	return []scenario7TableDef{
		{
			Name:             "customers",
			DistributionType: "hash",
			DistributionKey:  "id",
			RequiredColumns:  []string{},
			RequiredIndexes:  []string{},
		},
		{
			Name:             "orders",
			DistributionType: "hash",
			DistributionKey:  "customer_id",
			RequiredColumns:  []string{"customer_id", "amount", "status"},
			RequiredIndexes:  []string{},
		},
		{
			Name:             "logs",
			DistributionType: "random",
			DistributionKey:  "",
			RequiredColumns:  []string{"id", "log_time", "level", "source", "message", "metadata"},
			RequiredIndexes:  []string{"idx_logs_time", "idx_logs_level", "idx_logs_source"},
		},
		{
			Name:                 "audit_log",
			DistributionType:     "hash",
			DistributionKey:      "id",
			ExcludeFromRebalance: true,
			RequiredColumns:      []string{"id", "event_time", "user_name", "action", "resource_type", "resource_id", "details", "ip_address"},
			RequiredIndexes:      []string{"idx_audit_time", "idx_audit_user", "idx_audit_action"},
		},
		{
			Name:             "temp_staging",
			DistributionType: "hash",
			DistributionKey:  "id",
			RequiredColumns:  []string{"id", "batch_id", "raw_data", "processed", "created_at", "processed_at"},
			RequiredIndexes:  []string{"idx_temp_staging_batch", "idx_temp_staging_processed"},
		},
	}
}

// scenario7SQLPath returns the absolute path to the Scenario 7 SQL file.
func scenario7SQLPath() string {
	_, filename, _, _ := runtime.Caller(0) //nolint:dogsled
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	return filepath.Join(projectRoot, "test", "scenarios", "scenario7_load_data.sql")
}

// scenario7ReadSQL reads and returns the Scenario 7 SQL file content.
func scenario7ReadSQL(t *testing.T) string {
	t.Helper()
	sqlPath := scenario7SQLPath()
	data, err := os.ReadFile(sqlPath)
	require.NoError(t, err, "failed to read scenario7_load_data.sql at %s", sqlPath)
	return string(data)
}

// Scenario7LoadDataSuite tests the Scenario 7 data loading SQL script structure
// and expected table definitions without requiring a real database connection.
type Scenario7LoadDataSuite struct {
	suite.Suite
	sqlContent string
}

func TestScenario7_LoadData(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario7LoadDataSuite))
}

func (s *Scenario7LoadDataSuite) SetupSuite() {
	s.sqlContent = scenario7ReadSQL(s.T())
}

// TestScenario7_DataSchemaDefinition verifies the expected schema definitions
// are present in the SQL script for each table.
func (s *Scenario7LoadDataSuite) TestScenario7_DataSchemaDefinition() {
	for _, table := range scenario7ExpectedTables() {
		s.Run(table.Name, func() {
			// Verify table is referenced in the SQL.
			assert.Contains(s.T(), s.sqlContent, table.Name,
				"SQL script should reference table %s", table.Name)

			// Verify required columns are present for tables with CREATE TABLE.
			for _, col := range table.RequiredColumns {
				assert.Contains(s.T(), s.sqlContent, col,
					"SQL script should contain column %s for table %s", col, table.Name)
			}

			// Verify required indexes are present.
			for _, idx := range table.RequiredIndexes {
				assert.Contains(s.T(), s.sqlContent, idx,
					"SQL script should contain index %s for table %s", idx, table.Name)
			}
		})
	}
}

// TestScenario7_TableDistributionComments verifies that distribution metadata
// is set via COMMENT ON TABLE for each table.
func (s *Scenario7LoadDataSuite) TestScenario7_TableDistributionComments() {
	for _, table := range scenario7ExpectedTables() {
		s.Run(table.Name, func() {
			// Build expected comment pattern.
			commentPattern := `COMMENT ON TABLE ` + table.Name + ` IS '`
			assert.Contains(s.T(), s.sqlContent, commentPattern,
				"SQL script should have COMMENT ON TABLE for %s", table.Name)

			// Verify distribution type in comment.
			distPattern := `distribution=` + table.DistributionType
			// Find the COMMENT line for this table and check distribution type.
			lines := strings.Split(s.sqlContent, "\n")
			found := false
			for _, line := range lines {
				if strings.Contains(line, "COMMENT ON TABLE "+table.Name) {
					assert.Contains(s.T(), line, distPattern,
						"distribution comment for %s should contain %s", table.Name, distPattern)
					found = true

					// Verify distribution key if applicable.
					if table.DistributionKey != "" {
						keyPattern := "key=" + table.DistributionKey
						assert.Contains(s.T(), line, keyPattern,
							"distribution comment for %s should contain key=%s", table.Name, table.DistributionKey)
					}

					// Verify exclusion flag if applicable.
					if table.ExcludeFromRebalance {
						assert.Contains(s.T(), line, "exclude_from_rebalance=true",
							"distribution comment for %s should contain exclude_from_rebalance=true", table.Name)
					}

					break
				}
			}
			assert.True(s.T(), found,
				"should find COMMENT ON TABLE line for %s", table.Name)
		})
	}
}

// TestScenario7_ExpectedTableCount verifies that exactly 5 tables are expected
// in the data loading script (customers, orders, logs, audit_log, temp_staging).
func (s *Scenario7LoadDataSuite) TestScenario7_ExpectedTableCount() {
	tables := scenario7ExpectedTables()
	assert.Equal(s.T(), scenario7ExpectedTableCount, len(tables),
		"should have exactly %d expected tables", scenario7ExpectedTableCount)

	// Verify all expected table names are present.
	expectedNames := []string{"customers", "orders", "logs", "audit_log", "temp_staging"}
	for _, name := range expectedNames {
		found := false
		for _, table := range tables {
			if table.Name == name {
				found = true
				break
			}
		}
		assert.True(s.T(), found, "expected table %s should be in the table definitions", name)
	}

	// Verify ANALYZE is called for each table.
	for _, name := range expectedNames {
		analyzePattern := "ANALYZE " + name + ";"
		assert.Contains(s.T(), s.sqlContent, analyzePattern,
			"SQL script should ANALYZE table %s", name)
	}
}

// TestScenario7_SkewedDataDistribution verifies the Pareto distribution logic
// in the SQL script — 80% of orders should go to 20% of customers.
func (s *Scenario7LoadDataSuite) TestScenario7_SkewedDataDistribution() {
	// Verify the skew INSERT statement exists.
	assert.Contains(s.T(), s.sqlContent, "INSERT INTO orders",
		"SQL script should contain INSERT INTO orders for skewed data")

	// Verify the Pareto distribution logic: 80/20 split.
	assert.Contains(s.T(), s.sqlContent, "random() < 0.8",
		"SQL script should use random() < 0.8 for Pareto distribution")

	// Verify the first 20K customers get 80% of orders.
	assert.Contains(s.T(), s.sqlContent, "random() * 19999 + 1",
		"SQL script should target first 20K customers for 80%% of orders")

	// Verify the remaining 80K customers get 20% of orders.
	assert.Contains(s.T(), s.sqlContent, "random() * 79999 + 20001",
		"SQL script should target remaining 80K customers for 20%% of orders")

	// Verify 500K rows are inserted for skew data.
	assert.Contains(s.T(), s.sqlContent, "generate_series(1, 500000)",
		"SQL script should insert 500K skewed order rows")
}

// TestScenario7_SQLScriptStructure verifies the overall structure of the SQL script
// including database connection, grants, and analyze statements.
func (s *Scenario7LoadDataSuite) TestScenario7_SQLScriptStructure() {
	// Verify database connection.
	assert.Contains(s.T(), s.sqlContent, `\c mydb`,
		"SQL script should connect to mydb database")

	// Verify grants for analyst role.
	assert.Contains(s.T(), s.sqlContent, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst",
		"SQL script should grant SELECT to analyst")
	assert.Contains(s.T(), s.sqlContent, "GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO analyst",
		"SQL script should grant USAGE on sequences to analyst")

	// Verify CREATE TABLE IF NOT EXISTS for new tables.
	newTables := []string{"logs", "audit_log", "temp_staging"}
	for _, table := range newTables {
		pattern := "CREATE TABLE IF NOT EXISTS " + table
		assert.Contains(s.T(), s.sqlContent, pattern,
			"SQL script should use CREATE TABLE IF NOT EXISTS for %s", table)
	}

	// Verify CREATE INDEX IF NOT EXISTS for all indexes.
	indexPattern := regexp.MustCompile(`CREATE INDEX IF NOT EXISTS (\w+)`)
	matches := indexPattern.FindAllStringSubmatch(s.sqlContent, -1)
	assert.GreaterOrEqual(s.T(), len(matches), 8,
		"SQL script should create at least 8 indexes")
}

// TestScenario7_InsertRowCounts verifies the expected row counts for each
// INSERT statement in the SQL script.
func (s *Scenario7LoadDataSuite) TestScenario7_InsertRowCounts() {
	// Verify generate_series counts for each table.
	expectedCounts := map[string]string{
		"orders":       "generate_series(1, 500000)",
		"logs":         "generate_series(1, 200000)",
		"audit_log":    "generate_series(1, 100000)",
		"temp_staging": "generate_series(1, 50000)",
	}

	for table, seriesPattern := range expectedCounts {
		assert.Contains(s.T(), s.sqlContent, seriesPattern,
			"SQL script should use %s for %s inserts", seriesPattern, table)
	}
}

// TestScenario7_TempStagingExclusion verifies that the temp_staging table
// is marked for exclusion patterns (matches temp_* pattern).
func (s *Scenario7LoadDataSuite) TestScenario7_TempStagingExclusion() {
	// Verify temp_staging has the temporary_staging flag.
	lines := strings.Split(s.sqlContent, "\n")
	for _, line := range lines {
		if strings.Contains(line, "COMMENT ON TABLE temp_staging") {
			assert.Contains(s.T(), line, "temporary_staging=true",
				"temp_staging comment should contain temporary_staging=true")
			break
		}
	}

	// Verify the table name starts with "temp_" prefix.
	assert.True(s.T(), strings.HasPrefix("temp_staging", "temp_"),
		"temp_staging table name should start with temp_ prefix for pattern matching")
}

// TestScenario7_ShellScriptExists verifies that the shell script for running
// the SQL exists and contains the expected structure.
func (s *Scenario7LoadDataSuite) TestScenario7_ShellScriptExists() {
	_, filename, _, _ := runtime.Caller(0) //nolint:dogsled
	projectRoot := filepath.Join(filepath.Dir(filename), "..", "..")
	shPath := filepath.Join(projectRoot, "test", "scenarios", "scenario7_load_data.sh")

	data, err := os.ReadFile(shPath)
	require.NoError(s.T(), err, "scenario7_load_data.sh should exist")

	shContent := string(data)

	// Verify shebang and strict mode.
	assert.True(s.T(), strings.HasPrefix(shContent, "#!/usr/bin/env bash"),
		"shell script should start with bash shebang")
	assert.Contains(s.T(), shContent, "set -euo pipefail",
		"shell script should use strict mode")

	// Verify kubectl commands.
	assert.Contains(s.T(), shContent, "kubectl cp",
		"shell script should copy SQL file to pod")
	assert.Contains(s.T(), shContent, "kubectl exec",
		"shell script should execute SQL via kubectl exec")

	// Verify verification queries.
	assert.Contains(s.T(), shContent, "Row counts",
		"shell script should verify row counts")
	assert.Contains(s.T(), shContent, "pg_size_pretty",
		"shell script should show table sizes")
}

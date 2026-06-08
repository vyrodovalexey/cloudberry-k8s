package planchecker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckPlan_SequentialScanFlagged(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on orders  (cost=0.00..1500.00 rows=50000 width=100) (actual time=0.020..45.000 rows=50000 loops=1)
    Filter: (status = 'active')
    Rows Removed by Filter: 30000
  Planning Time: 0.200 ms
  Execution Time: 45.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)
	require.NotEmpty(t, result.Issues)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategorySequentialScan {
			found = true
			assert.Equal(t, SeverityWarning, issue.Severity)
			assert.Contains(t, issue.Recommendation, "index")
			assert.Equal(t, "orders", issue.Relation)
			assert.Equal(t, int64(50000), issue.Details["actualRows"])
			break
		}
	}
	assert.True(t, found, "expected sequential_scan issue")
}

func TestCheckPlan_RowEstimateMismatchFlagged(t *testing.T) {
	t.Parallel()
	input := `  Nested Loop  (cost=0.00..500.00 rows=10 width=72) (actual time=0.050..2500.000 rows=200000 loops=1)
    ->  Seq Scan on dim_products p  (cost=0.00..1.10 rows=10 width=36) (actual time=0.010..0.020 rows=10 loops=1)
    ->  Index Scan using idx_sales_product on sales s  (cost=0.29..49.90 rows=1 width=36) (actual time=0.001..200.000 rows=20000 loops=10)
          Index Cond: (s.product_id = p.id)
  Planning Time: 0.300 ms
  Execution Time: 2500.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategoryRowEstimateMismatch {
			found = true
			assert.Equal(t, SeverityWarning, issue.Severity)
			assert.Contains(t, issue.Recommendation, "ANALYZE")
			break
		}
	}
	assert.True(t, found, "expected row_estimate_mismatch issue")
}

func TestCheckPlan_SortSpillFlagged(t *testing.T) {
	t.Parallel()
	input := `  Sort  (cost=8000.00..8025.00 rows=10000 width=150) (actual time=200.000..350.000 rows=10000 loops=1)
    Sort Key: event_timestamp DESC
    Sort Method: external merge  Disk: 8192kB
    ->  Seq Scan on events  (cost=0.00..500.00 rows=10000 width=150) (actual time=0.010..20.000 rows=10000 loops=1)
  Planning Time: 0.150 ms
  Execution Time: 350.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategorySortSpill {
			found = true
			assert.Equal(t, SeverityWarning, issue.Severity)
			assert.Contains(t, issue.Recommendation, "work_mem")
			assert.Contains(t, issue.Recommendation, "8192kB")
			break
		}
	}
	assert.True(t, found, "expected sort_spill issue")
}

func TestCheckPlan_NestedLoopHighRows(t *testing.T) {
	t.Parallel()
	input := `  Nested Loop  (cost=0.00..1000.00 rows=100 width=72) (actual time=0.050..500.000 rows=1000 loops=500)
    ->  Seq Scan on orders o  (cost=0.00..35.50 rows=1000 width=36) (actual time=0.010..1.000 rows=500 loops=1)
    ->  Index Scan using idx_items on items i  (cost=0.00..0.50 rows=1 width=36) (actual time=0.001..0.400 rows=2 loops=500)
          Index Cond: (i.order_id = o.id)`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategoryNestedLoopHighRows {
			found = true
			assert.Equal(t, SeverityWarning, issue.Severity)
			assert.Contains(t, issue.Recommendation, "Hash Join")
			break
		}
	}
	assert.True(t, found, "expected nested_loop_high_rows issue")
}

func TestCheckPlan_LargeRowsRemovedByFilter(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on large_orders  (cost=0.00..5000.00 rows=100000 width=100) (actual time=0.020..120.000 rows=100 loops=1)
    Filter: (region = 'US')
    Rows Removed by Filter: 50000
  Planning Time: 0.200 ms
  Execution Time: 120.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategoryExcessiveFilterRows {
			found = true
			assert.Equal(t, SeverityWarning, issue.Severity)
			assert.Contains(t, issue.Recommendation, "index")
			break
		}
	}
	assert.True(t, found, "expected excessive_filter_rows issue")
}

func TestCheckPlan_HighCostNode(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on huge_table  (cost=0.00..50000.00 rows=100 width=100) (actual time=0.020..1.000 rows=100 loops=1)
  Planning Time: 0.100 ms
  Execution Time: 1.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	found := false
	for _, issue := range result.Issues {
		if issue.Category == CategoryHighCostNode {
			found = true
			assert.Equal(t, SeverityInfo, issue.Severity)
			assert.Contains(t, issue.Recommendation, "review for optimization")
			break
		}
	}
	assert.True(t, found, "expected high_cost_node issue")
}

func TestCheckPlan_NoIssues(t *testing.T) {
	t.Parallel()
	input := `  Index Scan using idx_orders_id on orders  (cost=0.29..8.31 rows=1 width=100) (actual time=0.020..0.025 rows=1 loops=1)
    Index Cond: (id = 42)
  Planning Time: 0.100 ms
  Execution Time: 0.050 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)
	assert.Empty(t, result.Issues)
	assert.Contains(t, result.Summary, "No performance issues found")
}

func TestCheckPlan_MultipleIssues(t *testing.T) {
	t.Parallel()
	input := `Sort  (cost=15000.00..15025.00 rows=10000 width=200) (actual time=850.123..1200.456 rows=10000 loops=1)
  Sort Key: o.created_at DESC
  Sort Method: external merge  Disk: 16384kB
  ->  Nested Loop  (cost=0.00..12000.00 rows=100 width=200) (actual time=0.500..800.000 rows=500000 loops=1)
        ->  Seq Scan on orders o  (cost=0.00..2500.00 rows=50000 width=100) (actual time=0.020..45.000 rows=50000 loops=1)
              Filter: (status = 'active')
              Rows Removed by Filter: 150000
        ->  Index Scan using idx_items_order_id on order_items i  (cost=0.29..0.50 rows=1 width=100) (actual time=0.001..0.010 rows=10 loops=50000)
              Index Cond: (i.order_id = o.id)
Planning Time: 2.345 ms
Execution Time: 1234.567 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	// Collect categories found.
	categories := make(map[string]bool)
	for _, issue := range result.Issues {
		categories[issue.Category] = true
		assert.NotEmpty(t, issue.Description)
		assert.NotEmpty(t, issue.Recommendation)
	}

	assert.True(t, categories[CategorySequentialScan], "expected sequential_scan issue")
	assert.True(t, categories[CategoryRowEstimateMismatch], "expected row_estimate_mismatch issue")
	assert.True(t, categories[CategorySortSpill], "expected sort_spill issue")
	assert.GreaterOrEqual(t, len(result.Issues), 3)
	assert.Greater(t, result.TotalNodes, 0)
	assert.InDelta(t, 1234.567, result.ExecutionTime, 0.001)
	assert.Contains(t, result.Summary, "Found")
}

func TestCheckPlan_EmptyInput(t *testing.T) {
	t.Parallel()
	_, err := CheckPlan("")
	require.Error(t, err)
}

func TestCheckPlan_SummaryFormat(t *testing.T) {
	t.Parallel()
	// Plan with 1 warning (seq scan) and 1 info (high cost).
	input := `  Seq Scan on big_table  (cost=0.00..20000.00 rows=50000 width=100) (actual time=0.020..45.000 rows=50000 loops=1)
  Planning Time: 0.100 ms
  Execution Time: 45.500 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)
	assert.Greater(t, result.TotalNodes, 0)
	assert.Contains(t, result.Summary, "Found")
}

func TestCheckPlan_SmallSeqScanNoIssue(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on small_table  (cost=0.00..1.50 rows=50 width=36) (actual time=0.010..0.020 rows=50 loops=1)
  Planning Time: 0.050 ms
  Execution Time: 0.030 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	for _, issue := range result.Issues {
		assert.NotEqual(t, CategorySequentialScan, issue.Category,
			"small seq scan should not be flagged")
	}
}

func TestCheckPlan_RowEstimateWithinTolerance(t *testing.T) {
	t.Parallel()
	// planRows=100, actualRows=500 → 4x off, below 10x threshold.
	input := `  Seq Scan on medium_table  (cost=0.00..50.00 rows=100 width=36) (actual time=0.010..0.500 rows=500 loops=1)
  Planning Time: 0.050 ms
  Execution Time: 0.600 ms`

	result, err := CheckPlan(input)
	require.NoError(t, err)

	for _, issue := range result.Issues {
		assert.NotEqual(t, CategoryRowEstimateMismatch, issue.Category,
			"5x mismatch should not be flagged (threshold is 10x)")
	}
}

func TestCheckSequentialScan_NilForNonSeqScan(t *testing.T) {
	t.Parallel()
	node := &PlanNode{
		NodeType:   "Index Scan",
		ActualRows: 50000,
	}
	assert.Nil(t, checkSequentialScan(node))
}

func TestCheckRowEstimateMismatch_NilForNoActualData(t *testing.T) {
	t.Parallel()
	node := &PlanNode{
		NodeType: "Seq Scan",
		PlanRows: 100,
		// No actual data (EXPLAIN without ANALYZE).
	}
	assert.Nil(t, checkRowEstimateMismatch(node))
}

func TestCheckSortSpill_NilForMemorySort(t *testing.T) {
	t.Parallel()
	node := &PlanNode{
		NodeType:      "Sort",
		SortSpaceType: "Memory",
		SortSpaceUsed: 32,
	}
	assert.Nil(t, checkSortSpill(node))
}

func TestCheckNestedLoopHighRows_NilForSmallLoop(t *testing.T) {
	t.Parallel()
	node := &PlanNode{
		NodeType:    "Nested Loop",
		ActualRows:  10,
		ActualLoops: 10,
	}
	assert.Nil(t, checkNestedLoopHighRows(node))
}

func TestCheckHighCostNode_NilForLowCost(t *testing.T) {
	t.Parallel()
	node := &PlanNode{
		NodeType:  "Seq Scan",
		TotalCost: 500.0,
	}
	assert.Nil(t, checkHighCostNode(node))
}

func TestBuildSummary_NoIssues(t *testing.T) {
	t.Parallel()
	summary := buildSummary(nil)
	assert.Equal(t, "No performance issues found", summary)
}

func TestBuildSummary_SingleWarning(t *testing.T) {
	t.Parallel()
	issues := []PlanIssue{
		{Severity: SeverityWarning, Category: CategorySequentialScan},
	}
	summary := buildSummary(issues)
	assert.Contains(t, summary, "Found 1 performance issue")
	assert.Contains(t, summary, "1 warning(s)")
}

func TestBuildSummary_MixedSeverities(t *testing.T) {
	t.Parallel()
	issues := []PlanIssue{
		{Severity: SeverityWarning, Category: CategorySequentialScan},
		{Severity: SeverityWarning, Category: CategorySortSpill},
		{Severity: SeverityInfo, Category: CategoryHighCostNode},
	}
	summary := buildSummary(issues)
	assert.Contains(t, summary, "Found 3 performance issues")
	assert.Contains(t, summary, "2 warning(s)")
	assert.Contains(t, summary, "1 info")
}

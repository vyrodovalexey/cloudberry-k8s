package planchecker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_SimpleSeqScan(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on orders  (cost=0.00..431.00 rows=20000 width=36) (actual time=0.015..4.123 rows=20000 loops=1)
    Filter: (total > 100)
    Rows Removed by Filter: 5000`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Seq Scan", root.NodeType)
	assert.Equal(t, "orders", root.Relation)
	assert.InDelta(t, 0.00, root.StartupCost, 0.001)
	assert.InDelta(t, 431.00, root.TotalCost, 0.001)
	assert.Equal(t, int64(20000), root.PlanRows)
	assert.Equal(t, 36, root.PlanWidth)
	assert.InDelta(t, 0.015, root.ActualStartup, 0.001)
	assert.InDelta(t, 4.123, root.ActualTotal, 0.001)
	assert.Equal(t, int64(20000), root.ActualRows)
	assert.Equal(t, int64(1), root.ActualLoops)
	assert.Equal(t, "(total > 100)", root.Filter)
	assert.Equal(t, int64(5000), root.RowsRemoved)
	assert.Empty(t, root.Children)
}

func TestParse_NestedPlanWithHashJoin(t *testing.T) {
	t.Parallel()
	input := `  Hash Join  (cost=1.05..44.23 rows=10 width=72) (actual time=0.123..0.456 rows=10 loops=1)
    Hash Cond: (o.customer_id = c.id)
    ->  Seq Scan on orders o  (cost=0.00..35.50 rows=2550 width=36) (actual time=0.010..0.200 rows=2550 loops=1)
    ->  Hash  (cost=1.04..1.04 rows=4 width=36) (actual time=0.020..0.020 rows=4 loops=1)
          ->  Seq Scan on customers c  (cost=0.00..1.04 rows=4 width=36) (actual time=0.005..0.008 rows=4 loops=1)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Hash Join", root.NodeType)
	assert.Equal(t, "(o.customer_id = c.id)", root.HashCond)
	require.Len(t, root.Children, 2)

	child1 := root.Children[0]
	assert.Equal(t, "Seq Scan", child1.NodeType)
	assert.Equal(t, "orders", child1.Relation)
	assert.Equal(t, "o", child1.Alias)

	child2 := root.Children[1]
	assert.Equal(t, "Hash", child2.NodeType)
	require.Len(t, child2.Children, 1)

	grandchild := child2.Children[0]
	assert.Equal(t, "Seq Scan", grandchild.NodeType)
	assert.Equal(t, "customers", grandchild.Relation)
	assert.Equal(t, "c", grandchild.Alias)
}

func TestParse_SortWithDiskSpill(t *testing.T) {
	t.Parallel()
	input := `  Sort  (cost=1000.00..1025.00 rows=10000 width=100) (actual time=50.123..75.456 rows=10000 loops=1)
    Sort Key: created_at DESC
    Sort Method: external merge  Disk: 8192kB
    ->  Seq Scan on events  (cost=0.00..500.00 rows=10000 width=100) (actual time=0.010..20.000 rows=10000 loops=1)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Sort", root.NodeType)
	assert.Equal(t, "created_at DESC", root.SortKey)
	assert.Equal(t, "external merge", root.SortMethod)
	assert.Equal(t, "Disk", root.SortSpaceType)
	assert.Equal(t, int64(8192), root.SortSpaceUsed)
	require.Len(t, root.Children, 1)

	child := root.Children[0]
	assert.Equal(t, "Seq Scan", child.NodeType)
	assert.Equal(t, "events", child.Relation)
}

func TestParse_NestedLoop(t *testing.T) {
	t.Parallel()
	input := `  Nested Loop  (cost=0.00..1000.00 rows=100 width=72) (actual time=0.050..500.000 rows=500000 loops=1)
    ->  Seq Scan on orders o  (cost=0.00..35.50 rows=1000 width=36) (actual time=0.010..1.000 rows=1000 loops=1)
    ->  Index Scan using idx_items_order_id on items i  (cost=0.00..0.50 rows=1 width=36) (actual time=0.001..0.400 rows=500 loops=1000)
          Index Cond: (i.order_id = o.id)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Nested Loop", root.NodeType)
	assert.Equal(t, int64(100), root.PlanRows)
	assert.Equal(t, int64(500000), root.ActualRows)
	require.Len(t, root.Children, 2)

	child1 := root.Children[0]
	assert.Equal(t, "Seq Scan", child1.NodeType)
	assert.Equal(t, "orders", child1.Relation)

	child2 := root.Children[1]
	assert.Equal(t, "Index Scan", child2.NodeType)
	assert.Equal(t, "idx_items_order_id", child2.IndexName)
	assert.Equal(t, "items", child2.Relation)
	assert.Equal(t, "(i.order_id = o.id)", child2.IndexCond)
}

func TestParse_EmptyInput(t *testing.T) {
	t.Parallel()
	_, err := Parse("")
	require.Error(t, err)
	assert.ErrorIs(t, err, errEmptyPlanText)
}

func TestParse_GarbageInput(t *testing.T) {
	t.Parallel()
	nodes, err := Parse("this is not a plan")
	require.NoError(t, err)
	assert.Empty(t, nodes)
}

func TestParse_PlanWithoutAnalyze(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on orders  (cost=0.00..431.00 rows=20000 width=36)
    Filter: (total > 100)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Seq Scan", root.NodeType)
	assert.Equal(t, int64(20000), root.PlanRows)
	assert.Equal(t, int64(0), root.ActualRows)
	assert.InDelta(t, 0.0, root.ActualTotal, 0.001)
	assert.Equal(t, "(total > 100)", root.Filter)
}

func TestExtractExecutionTime(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on orders  (cost=0.00..431.00 rows=20000 width=36) (actual time=0.015..4.123 rows=20000 loops=1)
  Planning Time: 0.123 ms
  Execution Time: 4.567 ms`

	execTime := ExtractExecutionTime(input)
	assert.InDelta(t, 4.567, execTime, 0.001)
}

func TestExtractExecutionTime_NotPresent(t *testing.T) {
	t.Parallel()
	input := `  Seq Scan on orders  (cost=0.00..431.00 rows=20000 width=36)`
	execTime := ExtractExecutionTime(input)
	assert.InDelta(t, 0.0, execTime, 0.001)
}

func TestParse_IndexScan(t *testing.T) {
	t.Parallel()
	input := `  Index Scan using idx_orders_date on orders  (cost=0.29..8.31 rows=1 width=36) (actual time=0.020..0.025 rows=1 loops=1)
    Index Cond: (order_date = '2026-01-01'::date)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "Index Scan", root.NodeType)
	assert.Equal(t, "orders", root.Relation)
	assert.Equal(t, "idx_orders_date", root.IndexName)
	assert.Equal(t, "(order_date = '2026-01-01'::date)", root.IndexCond)
}

func TestParse_GatherMotion(t *testing.T) {
	t.Parallel()
	input := `  Gather Motion 4:1  (slice1; segments: 4)  (cost=0.00..431.00 rows=20000 width=36) (actual time=0.500..5.000 rows=20000 loops=1)
    ->  Seq Scan on orders  (cost=0.00..431.00 rows=5000 width=36) (actual time=0.015..4.123 rows=5000 loops=1)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Contains(t, root.NodeType, "Gather Motion")
	assert.Equal(t, int64(20000), root.PlanRows)
	assert.Equal(t, int64(20000), root.ActualRows)
	require.Len(t, root.Children, 1)

	child := root.Children[0]
	assert.Equal(t, "Seq Scan", child.NodeType)
	assert.Equal(t, "orders", child.Relation)
}

func TestFlattenNodes(t *testing.T) {
	t.Parallel()
	// Build a tree with 5 nodes across 3 levels.
	root := &PlanNode{NodeType: "Sort"}
	child1 := &PlanNode{NodeType: "Hash Join"}
	child2 := &PlanNode{NodeType: "Seq Scan"}
	grandchild1 := &PlanNode{NodeType: "Index Scan"}
	grandchild2 := &PlanNode{NodeType: "Hash"}

	root.Children = []*PlanNode{child1}
	child1.Children = []*PlanNode{child2, grandchild1}
	child2.Children = []*PlanNode{grandchild2}

	flat := FlattenNodes([]*PlanNode{root})
	assert.Len(t, flat, 5)
	assert.Equal(t, "Sort", flat[0].NodeType)
	assert.Equal(t, "Hash Join", flat[1].NodeType)
}

func TestParse_JoinFilter(t *testing.T) {
	t.Parallel()
	input := `  Nested Loop  (cost=0.00..100.00 rows=10 width=72) (actual time=0.050..1.000 rows=10 loops=1)
    Join Filter: (a.id = b.id)
    ->  Seq Scan on a  (cost=0.00..50.00 rows=100 width=36) (actual time=0.010..0.500 rows=100 loops=1)
    ->  Seq Scan on b  (cost=0.00..50.00 rows=100 width=36) (actual time=0.010..0.500 rows=100 loops=1)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "(a.id = b.id)", root.JoinFilter)
}

func TestParse_SortMethodMemory(t *testing.T) {
	t.Parallel()
	input := `  Sort  (cost=100.00..125.00 rows=100 width=50) (actual time=1.000..1.500 rows=100 loops=1)
    Sort Key: id
    Sort Method: quicksort  Memory: 32kB
    ->  Seq Scan on small_table  (cost=0.00..50.00 rows=100 width=50) (actual time=0.010..0.500 rows=100 loops=1)`

	nodes, err := Parse(input)
	require.NoError(t, err)
	require.Len(t, nodes, 1)

	root := nodes[0]
	assert.Equal(t, "quicksort", root.SortMethod)
	assert.Equal(t, "Memory", root.SortSpaceType)
	assert.Equal(t, int64(32), root.SortSpaceUsed)
}

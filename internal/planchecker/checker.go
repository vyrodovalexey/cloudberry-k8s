package planchecker

import (
	"fmt"
	"math"
	"strings"
)

// CheckPlan analyzes an EXPLAIN ANALYZE plan text and returns identified issues.
// This is the main entry point for the plan checker.
func CheckPlan(planText string) (*PlanCheckResult, error) {
	roots, err := Parse(planText)
	if err != nil {
		return nil, fmt.Errorf("parsing plan: %w", err)
	}

	allNodes := FlattenNodes(roots)
	execTime := ExtractExecutionTime(planText)

	var issues []PlanIssue

	// Apply all detection rules to each node.
	checkers := []func(*PlanNode) *PlanIssue{
		checkSequentialScan,
		checkRowEstimateMismatch,
		checkSortSpill,
		checkNestedLoopHighRows,
		checkLargeRowsRemovedByFilter,
		checkHighCostNode,
	}

	for _, node := range allNodes {
		for _, check := range checkers {
			if issue := check(node); issue != nil {
				issues = append(issues, *issue)
			}
		}
	}

	if issues == nil {
		issues = []PlanIssue{}
	}

	return &PlanCheckResult{
		Issues:        issues,
		Summary:       buildSummary(issues),
		TotalNodes:    len(allNodes),
		ExecutionTime: execTime,
	}, nil
}

// checkSequentialScan checks for sequential scans on large tables.
// Triggers when a Seq Scan has actualRows > seqScanRowThreshold.
func checkSequentialScan(node *PlanNode) *PlanIssue {
	if !strings.HasPrefix(node.NodeType, "Seq Scan") {
		return nil
	}
	if node.ActualRows <= seqScanRowThreshold {
		return nil
	}

	recommendation := fmt.Sprintf(
		"Consider creating an index on %s", node.Relation)
	if node.Filter != "" {
		recommendation = fmt.Sprintf(
			"Consider creating an index on %s for filter condition %s",
			node.Relation, node.Filter)
	}

	return &PlanIssue{
		Severity: SeverityWarning,
		Category: CategorySequentialScan,
		NodeType: node.NodeType,
		Relation: node.Relation,
		Description: fmt.Sprintf(
			"Sequential scan on %s returned %d rows",
			node.Relation, node.ActualRows),
		Recommendation: recommendation,
		Details: map[string]interface{}{
			detailKeyActualRows: node.ActualRows,
			detailKeyFilter:     node.Filter,
			detailKeyTotalCost:  node.TotalCost,
		},
	}
}

// checkRowEstimateMismatch checks for significant differences between
// estimated and actual rows.
// Triggers when |planRows - actualRows| / max(planRows, 1) > rowEstimateMismatchFactor.
func checkRowEstimateMismatch(node *PlanNode) *PlanIssue {
	// Only check nodes that have actual data (ANALYZE was run).
	if node.ActualRows == 0 && node.ActualLoops == 0 {
		return nil
	}

	planRows := float64(node.PlanRows)
	actualRows := float64(node.ActualRows)
	denominator := math.Max(planRows, 1)
	ratio := math.Abs(planRows-actualRows) / denominator

	if ratio <= rowEstimateMismatchFactor {
		return nil
	}

	relation := node.Relation
	if relation == "" {
		relation = node.NodeType
	}

	return &PlanIssue{
		Severity: SeverityWarning,
		Category: CategoryRowEstimateMismatch,
		NodeType: node.NodeType,
		Relation: relation,
		Description: fmt.Sprintf(
			"Row estimate mismatch on %s: estimated %d rows, "+
				"actual %d rows (%.0fx off)",
			relation, node.PlanRows, node.ActualRows, ratio),
		Recommendation: "Run ANALYZE on the tables involved to update statistics",
		Details: map[string]interface{}{
			detailKeyPlanRows:   node.PlanRows,
			detailKeyActualRows: node.ActualRows,
			detailKeyRatio:      ratio,
		},
	}
}

// checkSortSpill checks for sort operations that spill to disk.
// Triggers when SortSpaceType == "Disk".
func checkSortSpill(node *PlanNode) *PlanIssue {
	if node.SortSpaceType != "Disk" {
		return nil
	}

	return &PlanIssue{
		Severity: SeverityWarning,
		Category: CategorySortSpill,
		NodeType: node.NodeType,
		Relation: node.Relation,
		Description: fmt.Sprintf(
			"Sort spilled to disk using %dkB", node.SortSpaceUsed),
		Recommendation: fmt.Sprintf(
			"Increase work_mem (current sort used %dkB on disk)",
			node.SortSpaceUsed),
		Details: map[string]interface{}{
			detailKeySortMethod: node.SortMethod,
			detailKeySortUsed:   node.SortSpaceUsed,
			detailKeySortType:   node.SortSpaceType,
		},
	}
}

// checkNestedLoopHighRows checks for nested loops processing excessive rows.
// Triggers when actualRows * actualLoops > nestedLoopRowThreshold.
func checkNestedLoopHighRows(node *PlanNode) *PlanIssue {
	if !strings.HasPrefix(node.NodeType, "Nested Loop") {
		return nil
	}

	totalRows := node.ActualRows * max(node.ActualLoops, 1)
	if totalRows <= nestedLoopRowThreshold {
		return nil
	}

	return &PlanIssue{
		Severity: SeverityWarning,
		Category: CategoryNestedLoopHighRows,
		NodeType: node.NodeType,
		Relation: node.Relation,
		Description: fmt.Sprintf(
			"Nested loop processed %d total rows (%d rows x %d loops)",
			totalRows, node.ActualRows, node.ActualLoops),
		Recommendation: fmt.Sprintf(
			"Consider Hash Join or Merge Join; "+
				"nested loop processed %d rows", totalRows),
		Details: map[string]interface{}{
			detailKeyActualRows:  node.ActualRows,
			detailKeyActualLoops: node.ActualLoops,
			detailKeyTotalRows:   totalRows,
		},
	}
}

// checkLargeRowsRemovedByFilter checks for filters removing disproportionate rows.
// Triggers when RowsRemoved > filterRowsRemovedFactor * ActualRows
// AND RowsRemoved > filterRowsRemovedMinimum.
func checkLargeRowsRemovedByFilter(node *PlanNode) *PlanIssue {
	if node.RowsRemoved == 0 {
		return nil
	}
	if node.RowsRemoved < filterRowsRemovedMinimum {
		return nil
	}

	actualRows := max(node.ActualRows, 1)
	if node.RowsRemoved <= filterRowsRemovedFactor*actualRows {
		return nil
	}

	ratio := node.RowsRemoved / actualRows

	return &PlanIssue{
		Severity: SeverityWarning,
		Category: CategoryExcessiveFilterRows,
		NodeType: node.NodeType,
		Relation: node.Relation,
		Description: fmt.Sprintf(
			"Filter removed %dx more rows than returned "+
				"(%d removed vs %d returned)",
			ratio, node.RowsRemoved, node.ActualRows),
		Recommendation: fmt.Sprintf(
			"Filter removed %dx more rows than returned; "+
				"consider adding index on filter column", ratio),
		Details: map[string]interface{}{
			detailKeyRowsRemoved: node.RowsRemoved,
			detailKeyActualRows:  node.ActualRows,
			detailKeyRatio:       ratio,
			detailKeyFilter:      node.Filter,
		},
	}
}

// checkHighCostNode checks for nodes with very high total cost.
// Triggers when TotalCost > highCostThreshold.
func checkHighCostNode(node *PlanNode) *PlanIssue {
	if node.TotalCost <= highCostThreshold {
		return nil
	}

	return &PlanIssue{
		Severity: SeverityInfo,
		Category: CategoryHighCostNode,
		NodeType: node.NodeType,
		Relation: node.Relation,
		Description: fmt.Sprintf(
			"High-cost node %s (cost=%.2f)",
			node.NodeType, node.TotalCost),
		Recommendation: fmt.Sprintf(
			"High-cost node (%.0f); review for optimization",
			node.TotalCost),
		Details: map[string]interface{}{
			detailKeyTotalCost: node.TotalCost,
		},
	}
}

// buildSummary generates a human-readable summary of the issues found.
func buildSummary(issues []PlanIssue) string {
	if len(issues) == 0 {
		return "No performance issues found"
	}

	var warnings, infos int
	for _, issue := range issues {
		switch issue.Severity {
		case SeverityWarning:
			warnings++
		case SeverityInfo:
			infos++
		}
	}

	parts := make([]string, 0, 2)
	if warnings > 0 {
		parts = append(parts, fmt.Sprintf("%d warning(s)", warnings))
	}
	if infos > 0 {
		parts = append(parts, fmt.Sprintf("%d info", infos))
	}

	issueWord := "issue"
	if len(issues) != 1 {
		issueWord = "issues"
	}

	return fmt.Sprintf("Found %d performance %s: %s",
		len(issues), issueWord, strings.Join(parts, ", "))
}

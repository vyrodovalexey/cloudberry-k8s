// Package planchecker provides static analysis of PostgreSQL/Cloudberry EXPLAIN ANALYZE
// output. It parses plan text into a tree of PlanNode structs and identifies performance
// issues with actionable recommendations. No database connection is required — the
// analysis is purely text-based.
package planchecker

// Severity levels for plan issues.
const (
	SeverityWarning = "warning"
	SeverityInfo    = "info"
)

// Issue category constants.
const (
	CategorySequentialScan      = "sequential_scan"
	CategoryRowEstimateMismatch = "row_estimate_mismatch"
	CategorySortSpill           = "sort_spill"
	CategoryNestedLoopHighRows  = "nested_loop_high_rows"
	CategoryExcessiveFilterRows = "excessive_filter_rows"
	CategoryHighCostNode        = "high_cost_node"
)

// Detail key constants for issue details maps.
const (
	detailKeyActualRows  = "actualRows"
	detailKeyFilter      = "filter"
	detailKeyTotalCost   = "totalCost"
	detailKeyPlanRows    = "planRows"
	detailKeyRatio       = "ratio"
	detailKeySortMethod  = "sortMethod"
	detailKeySortUsed    = "sortSpaceUsed"
	detailKeySortType    = "sortSpaceType"
	detailKeyActualLoops = "actualLoops"
	detailKeyTotalRows   = "totalRows"
	detailKeyRowsRemoved = "rowsRemoved"
)

// Threshold constants for issue detection rules.
const (
	// seqScanRowThreshold is the minimum actual rows for a sequential scan to be flagged.
	seqScanRowThreshold int64 = 10000

	// rowEstimateMismatchFactor is the minimum ratio of |planRows - actualRows| / max(planRows, 1)
	// for a row estimate mismatch to be flagged.
	rowEstimateMismatchFactor float64 = 10.0

	// nestedLoopRowThreshold is the minimum actualRows * actualLoops for a nested loop to be flagged.
	nestedLoopRowThreshold int64 = 100000

	// filterRowsRemovedFactor is the minimum ratio of RowsRemoved / ActualRows for excessive filter to be flagged.
	filterRowsRemovedFactor int64 = 10

	// filterRowsRemovedMinimum is the minimum RowsRemoved for excessive filter to be flagged.
	filterRowsRemovedMinimum int64 = 1000

	// highCostThreshold is the minimum TotalCost for a high-cost node to be flagged.
	highCostThreshold float64 = 10000.0
)

// PlanNode represents a node in the query execution plan tree.
type PlanNode struct {
	// NodeType is the plan node type (e.g., "Seq Scan", "Index Scan", "Nested Loop").
	NodeType      string      `json:"nodeType"`
	Relation      string      `json:"relation,omitempty"`      // table name if applicable
	Alias         string      `json:"alias,omitempty"`         // table alias
	StartupCost   float64     `json:"startupCost"`             // estimated startup cost
	TotalCost     float64     `json:"totalCost"`               // estimated total cost
	PlanRows      int64       `json:"planRows"`                // estimated rows
	PlanWidth     int         `json:"planWidth"`               // estimated row width
	ActualStartup float64     `json:"actualStartup,omitempty"` // actual startup time (ms)
	ActualTotal   float64     `json:"actualTotal,omitempty"`   // actual total time (ms)
	ActualRows    int64       `json:"actualRows,omitempty"`    // actual rows returned
	ActualLoops   int64       `json:"actualLoops,omitempty"`   // number of loops
	Filter        string      `json:"filter,omitempty"`        // filter condition
	RowsRemoved   int64       `json:"rowsRemoved,omitempty"`   // rows removed by filter
	SortKey       string      `json:"sortKey,omitempty"`       // sort key(s)
	SortMethod    string      `json:"sortMethod,omitempty"`    // e.g., "external merge  Disk"
	SortSpaceUsed int64       `json:"sortSpaceUsed,omitempty"` // sort space in kB
	SortSpaceType string      `json:"sortSpaceType,omitempty"` // "Disk" or "Memory"
	JoinType      string      `json:"joinType,omitempty"`      // for join nodes
	HashCond      string      `json:"hashCond,omitempty"`      // hash condition
	JoinFilter    string      `json:"joinFilter,omitempty"`    // join filter condition
	IndexName     string      `json:"indexName,omitempty"`     // index used
	IndexCond     string      `json:"indexCond,omitempty"`     // index condition
	Children      []*PlanNode `json:"children,omitempty"`      // child nodes
	Depth         int         `json:"depth"`                   // indentation depth
	RawLines      []string    `json:"-"`                       // original text lines (not serialized)
}

// PlanIssue represents a performance issue found in the plan.
type PlanIssue struct {
	Severity string `json:"severity"` // "warning", "info"
	// Category is the issue category (e.g., "sequential_scan", "sort_spill").
	Category       string                 `json:"category"`
	NodeType       string                 `json:"nodeType"`       // the plan node type where issue was found
	Relation       string                 `json:"relation"`       // table name if applicable
	Description    string                 `json:"description"`    // human-readable description
	Recommendation string                 `json:"recommendation"` // actionable recommendation
	Details        map[string]interface{} `json:"details"`        // additional details
}

// PlanCheckResult is the response from the plan checker.
type PlanCheckResult struct {
	Issues        []PlanIssue `json:"issues"`
	Summary       string      `json:"summary"`
	TotalNodes    int         `json:"totalNodes"`
	PlanText      string      `json:"planText,omitempty"`
	ExecutionTime float64     `json:"executionTime,omitempty"` // total execution time from plan (ms)
}

// PlanCheckRequest is the API request body for plan checking.
type PlanCheckRequest struct {
	PlanText string `json:"planText"`
}

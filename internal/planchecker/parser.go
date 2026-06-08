package planchecker

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// errEmptyPlanText is returned when the plan text is empty.
var errEmptyPlanText = errors.New("empty plan text")

// Compiled regex patterns for parsing EXPLAIN ANALYZE output.
var (
	// nodePattern matches a plan node line with cost and optional actual time.
	// Groups: 1=leading whitespace, 2=arrow prefix, 3=node type,
	//         4=relation, 5=alias, 6-9=cost/rows/width
	nodePattern = regexp.MustCompile(
		`^(\s*)(->)?\s*([\w][\w\s]*?)\s+` +
			`(?:on\s+(\w+)\s+(?:(\w+)\s+)?)?` +
			`\(cost=([0-9.]+)\.\.([0-9.]+)\s+rows=(\d+)\s+width=(\d+)\)`)

	// actualPattern matches the actual time portion of a plan node line.
	actualPattern = regexp.MustCompile(
		`\(actual time=([0-9.]+)\.\.([0-9.]+)\s+rows=(\d+)\s+loops=(\d+)\)`)

	// sortMethodPattern matches Sort Method lines.
	// Example: "Sort Method: external merge  Disk: 8192kB"
	sortMethodPattern = regexp.MustCompile(
		`^\s*Sort Method:\s+(.+?)\s+(Memory|Disk):\s+(\d+)kB`)

	// filterPattern matches Filter lines.
	filterPattern = regexp.MustCompile(`^\s*Filter:\s+(.+)`)

	// rowsRemovedPattern matches Rows Removed by Filter lines.
	rowsRemovedPattern = regexp.MustCompile(`^\s*Rows Removed by Filter:\s+(\d+)`)

	// sortKeyPattern matches Sort Key lines.
	sortKeyPattern = regexp.MustCompile(`^\s*Sort Key:\s+(.+)`)

	// hashCondPattern matches Hash Cond lines.
	hashCondPattern = regexp.MustCompile(`^\s*Hash Cond:\s+(.+)`)

	// indexCondPattern matches Index Cond lines.
	indexCondPattern = regexp.MustCompile(`^\s*Index Cond:\s+(.+)`)

	// indexNamePattern matches Index Name lines.
	indexNamePattern = regexp.MustCompile(`^\s*Index Name:\s+(\w+)`)

	// joinFilterPattern matches Join Filter lines.
	joinFilterPattern = regexp.MustCompile(`^\s*Join Filter:\s+(.+)`)

	// execTimePattern matches the Execution Time footer line.
	execTimePattern = regexp.MustCompile(`Execution Time:\s+([0-9.]+)\s+ms`)

	// indexUsingPattern extracts the index name from "Index Scan using idx_name on table" node types.
	indexUsingPattern = regexp.MustCompile(`^(Index\s+(?:Only\s+)?Scan)\s+using\s+(\w+)`)

	// motionPattern matches Cloudberry-specific motion nodes.
	// Example: "Gather Motion 4:1  (slice1; segments: 4)  (cost=...)"
	motionPattern = regexp.MustCompile(
		`^(\s*)(->)?\s*` +
			`((?:Gather|Redistribute|Broadcast)\s+Motion` +
			`\s+[^(]+(?:\([^)]*\)\s+)*)` +
			`\(cost=([0-9.]+)\.\.([0-9.]+)\s+` +
			`rows=(\d+)\s+width=(\d+)\)`)
)

// Parse parses EXPLAIN ANALYZE text output into a tree of PlanNode.
// Returns the root nodes and any error encountered.
func Parse(planText string) ([]*PlanNode, error) {
	planText = strings.TrimSpace(planText)
	if planText == "" {
		return nil, errEmptyPlanText
	}

	lines := strings.Split(planText, "\n")
	var nodes []*PlanNode
	var nodeStack []*nodeEntry

	for _, line := range lines {
		// Try to parse as a plan node line.
		node, depth := parseNodeLine(line)
		if node == nil {
			// Try motion pattern for Cloudberry-specific nodes.
			node, depth = parseMotionLine(line)
		}

		if node != nil {
			node.Depth = depth
			node.RawLines = append(node.RawLines, line)

			entry := &nodeEntry{node: node, depth: depth}

			// Find the parent node based on indentation depth.
			for len(nodeStack) > 0 && nodeStack[len(nodeStack)-1].depth >= depth {
				nodeStack = nodeStack[:len(nodeStack)-1]
			}

			if len(nodeStack) > 0 {
				parent := nodeStack[len(nodeStack)-1].node
				parent.Children = append(parent.Children, node)
			} else {
				nodes = append(nodes, node)
			}

			nodeStack = append(nodeStack, entry)
			continue
		}

		// Parse attribute lines and attach to the most recent node.
		if len(nodeStack) > 0 {
			currentNode := nodeStack[len(nodeStack)-1].node
			parseAttributeLine(line, currentNode)
			currentNode.RawLines = append(currentNode.RawLines, line)
		}
	}

	return nodes, nil
}

// nodeEntry tracks a node and its depth for tree building.
type nodeEntry struct {
	node  *PlanNode
	depth int
}

// parseNodeLine attempts to parse a line as a plan node.
// Returns the parsed node and its indentation depth, or nil if the line is not a node.
func parseNodeLine(line string) (node *PlanNode, depth int) {
	matches := nodePattern.FindStringSubmatch(line)
	if matches == nil {
		return nil, 0
	}

	indent := matches[1]
	arrow := matches[2]
	nodeType := strings.TrimSpace(matches[3])
	relation := matches[4]
	alias := matches[5]

	// Calculate depth: count leading spaces, arrow adds to depth.
	depth = len(indent)
	if arrow != "" {
		depth += len(arrow)
	}

	startupCost, _ := strconv.ParseFloat(matches[6], 64)
	totalCost, _ := strconv.ParseFloat(matches[7], 64)
	planRows, _ := strconv.ParseInt(matches[8], 10, 64)
	planWidth, _ := strconv.Atoi(matches[9])

	node = &PlanNode{
		NodeType:    nodeType,
		Relation:    relation,
		Alias:       alias,
		StartupCost: startupCost,
		TotalCost:   totalCost,
		PlanRows:    planRows,
		PlanWidth:   planWidth,
	}

	// Check for "Index Scan using idx_name" pattern in the node type.
	if idxMatch := indexUsingPattern.FindStringSubmatch(nodeType); idxMatch != nil {
		node.NodeType = idxMatch[1]
		node.IndexName = idxMatch[2]
	}

	// Parse actual time if present.
	if actMatches := actualPattern.FindStringSubmatch(line); actMatches != nil {
		node.ActualStartup, _ = strconv.ParseFloat(actMatches[1], 64)
		node.ActualTotal, _ = strconv.ParseFloat(actMatches[2], 64)
		node.ActualRows, _ = strconv.ParseInt(actMatches[3], 10, 64)
		node.ActualLoops, _ = strconv.ParseInt(actMatches[4], 10, 64)
	}

	return node, depth
}

// parseMotionLine attempts to parse a Cloudberry-specific motion node line.
func parseMotionLine(line string) (node *PlanNode, depth int) {
	matches := motionPattern.FindStringSubmatch(line)
	if matches == nil {
		return nil, 0
	}

	indent := matches[1]
	arrow := matches[2]
	nodeType := strings.TrimSpace(matches[3])

	depth = len(indent)
	if arrow != "" {
		depth += len(arrow)
	}

	startupCost, _ := strconv.ParseFloat(matches[4], 64)
	totalCost, _ := strconv.ParseFloat(matches[5], 64)
	planRows, _ := strconv.ParseInt(matches[6], 10, 64)
	planWidth, _ := strconv.Atoi(matches[7])

	node = &PlanNode{
		NodeType:    nodeType,
		StartupCost: startupCost,
		TotalCost:   totalCost,
		PlanRows:    planRows,
		PlanWidth:   planWidth,
	}

	// Parse actual time if present.
	if actMatches := actualPattern.FindStringSubmatch(line); actMatches != nil {
		node.ActualStartup, _ = strconv.ParseFloat(actMatches[1], 64)
		node.ActualTotal, _ = strconv.ParseFloat(actMatches[2], 64)
		node.ActualRows, _ = strconv.ParseInt(actMatches[3], 10, 64)
		node.ActualLoops, _ = strconv.ParseInt(actMatches[4], 10, 64)
	}

	return node, depth
}

// parseAttributeLine extracts additional attributes from a plan line and attaches them to the node.
func parseAttributeLine(line string, node *PlanNode) {
	if m := filterPattern.FindStringSubmatch(line); m != nil {
		node.Filter = strings.TrimSpace(m[1])
		return
	}
	if m := rowsRemovedPattern.FindStringSubmatch(line); m != nil {
		node.RowsRemoved, _ = strconv.ParseInt(m[1], 10, 64)
		return
	}
	if m := sortKeyPattern.FindStringSubmatch(line); m != nil {
		node.SortKey = strings.TrimSpace(m[1])
		return
	}
	if m := sortMethodPattern.FindStringSubmatch(line); m != nil {
		node.SortMethod = strings.TrimSpace(m[1])
		node.SortSpaceType = m[2]
		node.SortSpaceUsed, _ = strconv.ParseInt(m[3], 10, 64)
		return
	}
	if m := hashCondPattern.FindStringSubmatch(line); m != nil {
		node.HashCond = strings.TrimSpace(m[1])
		return
	}
	if m := indexCondPattern.FindStringSubmatch(line); m != nil {
		node.IndexCond = strings.TrimSpace(m[1])
		return
	}
	if m := indexNamePattern.FindStringSubmatch(line); m != nil {
		node.IndexName = strings.TrimSpace(m[1])
		return
	}
	if m := joinFilterPattern.FindStringSubmatch(line); m != nil {
		node.JoinFilter = strings.TrimSpace(m[1])
		return
	}
}

// ExtractExecutionTime extracts the total execution time from the plan footer.
// Returns 0 if no execution time is found.
func ExtractExecutionTime(planText string) float64 {
	if m := execTimePattern.FindStringSubmatch(planText); m != nil {
		val, _ := strconv.ParseFloat(m[1], 64)
		return val
	}
	return 0
}

// FlattenNodes returns all nodes in the tree as a flat slice for iteration.
func FlattenNodes(roots []*PlanNode) []*PlanNode {
	var result []*PlanNode
	for _, root := range roots {
		flattenRecursive(root, &result)
	}
	return result
}

// flattenRecursive recursively collects all nodes into the result slice.
func flattenRecursive(node *PlanNode, result *[]*PlanNode) {
	*result = append(*result, node)
	for _, child := range node.Children {
		flattenRecursive(child, result)
	}
}

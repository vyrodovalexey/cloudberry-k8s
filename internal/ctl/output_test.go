package ctl

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFormatter(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputJSON, &buf)
	require.NotNil(t, f)
	assert.Equal(t, OutputJSON, f.format)
}

func TestFormatter_FormatJSON(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputJSON, &buf)

	data := map[string]interface{}{
		"name":   "test-cluster",
		"status": "running",
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"name": "test-cluster"`)
	assert.Contains(t, output, `"status": "running"`)
}

func TestFormatter_FormatYAML(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputYAML, &buf)

	data := map[string]interface{}{
		"name":   "test-cluster",
		"status": "running",
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "name: test-cluster")
	assert.Contains(t, output, "status: running")
}

func TestFormatter_FormatTable_Map(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	data := map[string]interface{}{
		"name":   "test-cluster",
		"status": "running",
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "name:")
	assert.Contains(t, output, "test-cluster")
	assert.Contains(t, output, "status:")
	assert.Contains(t, output, "running")
}

func TestFormatter_FormatTable_Slice(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	data := []interface{}{
		map[string]interface{}{"name": "cluster1", "status": "running"},
		map[string]interface{}{"name": "cluster2", "status": "stopped"},
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "cluster1")
	assert.Contains(t, output, "cluster2")
	assert.Contains(t, output, "running")
	assert.Contains(t, output, "stopped")
	// Should have header separator
	assert.Contains(t, output, "---")
}

func TestFormatter_FormatTable_EmptySlice(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	data := []interface{}{}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "No items found.")
}

func TestFormatter_FormatTable_SliceOfNonMaps(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	// When slice items are not maps, it falls back to JSON
	data := []interface{}{"item1", "item2"}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "item1")
	assert.Contains(t, output, "item2")
}

func TestFormatter_FormatTable_UnsupportedType(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	// Unsupported type falls back to JSON
	data := "just a string"
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "just a string")
}

func TestFormatter_FormatTable_SliceWithMixedItems(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	// First item is a map, second is not - non-map items are skipped in rows
	data := []interface{}{
		map[string]interface{}{"name": "cluster1"},
		"not-a-map",
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "cluster1")
}

func TestFormatter_FormatStatus(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputJSON, &buf)

	status := map[string]interface{}{
		"phase":         "Running",
		"segmentsReady": 4,
		"segmentsTotal": 4,
	}
	err := f.FormatStatus(status)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "Running")
}

func TestFormatter_FormatMessage(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	f.FormatMessage("Operation completed successfully")
	assert.Equal(t, "Operation completed successfully\n", buf.String())
}

func TestFormatter_FormatError(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	f.FormatError(errors.New("something went wrong"))
	assert.Equal(t, "Error: something went wrong\n", buf.String())
}

func TestFormatter_FormatMapTable_Alignment(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	data := map[string]interface{}{
		"a":          "short",
		"longername": "value",
	}
	err := f.Format(data)
	require.NoError(t, err)

	// Both lines should have aligned colons
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 2)
}

func TestFormatter_FormatSliceTable_ColumnWidths(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	data := []interface{}{
		map[string]interface{}{"name": "a", "status": "running"},
		map[string]interface{}{"name": "very-long-cluster-name", "status": "ok"},
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	// Column widths should accommodate the longest value
	assert.Contains(t, output, "very-long-cluster-name")
}

func TestOutputConstants(t *testing.T) {
	assert.Equal(t, "table", OutputTable)
	assert.Equal(t, "json", OutputJSON)
	assert.Equal(t, "yaml", OutputYAML)
}

func TestFormatter_FormatJSON_Array(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputJSON, &buf)

	data := []interface{}{"a", "b", "c"}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, `"a"`)
	assert.Contains(t, output, `"b"`)
}

func TestFormatter_FormatYAML_Array(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputYAML, &buf)

	data := []interface{}{"a", "b"}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "- a")
	assert.Contains(t, output, "- b")
}

func TestFormatter_DefaultFormat(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter("unknown-format", &buf)

	// Unknown format should default to table
	data := map[string]interface{}{"key": "value"}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	assert.Contains(t, output, "key:")
	assert.Contains(t, output, "value")
}

func TestFormatter_FormatMapTable_SortedKeys(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	// Keys should be sorted alphabetically in the output.
	data := map[string]interface{}{
		"zebra":  "z",
		"apple":  "a",
		"mango":  "m",
		"banana": "b",
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 4)

	// Verify sorted order: apple, banana, mango, zebra.
	assert.True(t, strings.HasPrefix(lines[0], "apple:"))
	assert.True(t, strings.HasPrefix(lines[1], "banana:"))
	assert.True(t, strings.HasPrefix(lines[2], "mango:"))
	assert.True(t, strings.HasPrefix(lines[3], "zebra:"))
}

func TestFormatter_FormatSliceTable_SortedHeaders(t *testing.T) {
	var buf bytes.Buffer
	f := NewFormatter(OutputTable, &buf)

	// Headers should be sorted alphabetically.
	data := []interface{}{
		map[string]interface{}{"zebra": "z1", "apple": "a1", "mango": "m1"},
		map[string]interface{}{"zebra": "z2", "apple": "a2", "mango": "m2"},
	}
	err := f.Format(data)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.GreaterOrEqual(t, len(lines), 3) // header + separator + at least 1 data row

	// First line should have headers in sorted order.
	headerLine := lines[0]
	appleIdx := strings.Index(headerLine, "APPLE")
	mangoIdx := strings.Index(headerLine, "MANGO")
	zebraIdx := strings.Index(headerLine, "ZEBRA")
	assert.Less(t, appleIdx, mangoIdx)
	assert.Less(t, mangoIdx, zebraIdx)
}

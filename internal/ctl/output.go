package ctl

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"go.yaml.in/yaml/v3"
)

const (
	// OutputTable formats output as a human-readable table.
	OutputTable = "table"
	// OutputJSON formats output as JSON.
	OutputJSON = "json"
	// OutputYAML formats output as YAML.
	OutputYAML = "yaml"
)

// Formatter formats API responses for display.
type Formatter struct {
	format string
	writer io.Writer
}

// NewFormatter creates a new Formatter.
func NewFormatter(format string, writer io.Writer) *Formatter {
	return &Formatter{
		format: format,
		writer: writer,
	}
}

// Format outputs the data in the configured format.
func (f *Formatter) Format(data interface{}) error {
	switch f.format {
	case OutputJSON:
		return f.formatJSON(data)
	case OutputYAML:
		return f.formatYAML(data)
	default:
		return f.formatTable(data)
	}
}

// formatJSON outputs data as indented JSON.
func (f *Formatter) formatJSON(data interface{}) error {
	enc := json.NewEncoder(f.writer)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

// formatYAML outputs data as YAML.
func (f *Formatter) formatYAML(data interface{}) error {
	out, err := yaml.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling YAML: %w", err)
	}
	_, writeErr := f.writer.Write(out)
	return writeErr
}

// formatTable outputs data as a human-readable table.
func (f *Formatter) formatTable(data interface{}) error {
	switch v := data.(type) {
	case map[string]interface{}:
		return f.formatMapTable(v)
	case []interface{}:
		return f.formatSliceTable(v)
	default:
		// Fall back to JSON for unsupported types.
		return f.formatJSON(data)
	}
}

// formatMapTable formats a map as a key-value table.
func (f *Formatter) formatMapTable(data map[string]interface{}) error {
	// Find the longest key for alignment.
	maxKeyLen := 0
	for k := range data {
		if len(k) > maxKeyLen {
			maxKeyLen = len(k)
		}
	}

	for k, v := range data {
		padding := strings.Repeat(" ", maxKeyLen-len(k))
		fmt.Fprintf(f.writer, "%s:%s  %v\n", k, padding, v)
	}
	return nil
}

// calcColumnWidths computes the maximum display width for each header
// across all rows.
func calcColumnWidths(
	headers []string, data []interface{},
) map[string]int {
	widths := make(map[string]int, len(headers))
	for _, h := range headers {
		widths[h] = len(h)
	}
	for _, item := range data {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, h := range headers {
			if n := len(fmt.Sprintf("%v", m[h])); n > widths[h] {
				widths[h] = n
			}
		}
	}
	return widths
}

// formatSliceTable formats a slice of maps as a table with headers.
func (f *Formatter) formatSliceTable(data []interface{}) error {
	if len(data) == 0 {
		fmt.Fprintln(f.writer, "No items found.")
		return nil
	}

	firstItem, ok := data[0].(map[string]interface{})
	if !ok {
		return f.formatJSON(data)
	}

	headers := make([]string, 0, len(firstItem))
	for k := range firstItem {
		headers = append(headers, k)
	}

	widths := calcColumnWidths(headers, data)

	// Print header row.
	for _, h := range headers {
		fmt.Fprintf(f.writer, "%-*s  ", widths[h], strings.ToUpper(h))
	}
	fmt.Fprintln(f.writer)

	// Print separator.
	for _, h := range headers {
		fmt.Fprintf(f.writer, "%s  ", strings.Repeat("-", widths[h]))
	}
	fmt.Fprintln(f.writer)

	// Print data rows.
	for _, item := range data {
		if m, ok := item.(map[string]interface{}); ok {
			for _, h := range headers {
				fmt.Fprintf(f.writer, "%-*v  ", widths[h], m[h])
			}
			fmt.Fprintln(f.writer)
		}
	}

	return nil
}

// FormatStatus formats a status response with key-value pairs.
func (f *Formatter) FormatStatus(status map[string]interface{}) error {
	return f.Format(status)
}

// FormatMessage formats a simple message.
func (f *Formatter) FormatMessage(msg string) {
	fmt.Fprintln(f.writer, msg)
}

// FormatError formats an error message.
func (f *Formatter) FormatError(err error) {
	fmt.Fprintf(f.writer, "Error: %v\n", err)
}

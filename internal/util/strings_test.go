package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{
			name:     "no truncation needed",
			input:    "short",
			maxLen:   10,
			expected: "short",
		},
		{
			name:     "exact length",
			input:    "exact",
			maxLen:   5,
			expected: "exact",
		},
		{
			name:     "truncation needed",
			input:    "this-is-a-very-long-name",
			maxLen:   10,
			expected: "this-is-a-",
		},
		{
			name:     "empty string",
			input:    "",
			maxLen:   10,
			expected: "",
		},
		{
			name:     "zero max length",
			input:    "test",
			maxLen:   0,
			expected: "",
		},
		{
			name:     "single character",
			input:    "a",
			maxLen:   1,
			expected: "a",
		},
		{
			name:     "max length 63 for k8s names",
			input:    strings.Repeat("a", 100),
			maxLen:   63,
			expected: strings.Repeat("a", 63),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateName(tt.input, tt.maxLen)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeK8sName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "already valid",
			input:    "my-cluster",
			expected: "my-cluster",
		},
		{
			name:     "uppercase to lowercase",
			input:    "My-Cluster",
			expected: "my-cluster",
		},
		{
			name:     "special characters replaced",
			input:    "my_cluster.test",
			expected: "my-cluster-test",
		},
		{
			name:     "leading and trailing hyphens removed",
			input:    "-my-cluster-",
			expected: "my-cluster",
		},
		{
			name:     "spaces replaced",
			input:    "my cluster",
			expected: "my-cluster",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "all invalid characters",
			input:    "___",
			expected: "",
		},
		{
			name:     "long name truncated to 63",
			input:    strings.Repeat("a", 100),
			expected: strings.Repeat("a", 63),
		},
		{
			name:     "mixed case with underscores",
			input:    "CloudBerry_K8s_Cluster",
			expected: "cloudberry-k8s-cluster",
		},
		{
			name:     "numbers preserved",
			input:    "cluster-123",
			expected: "cluster-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeK8sName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestContainsString(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		s        string
		expected bool
	}{
		{
			name:     "contains the string",
			slice:    []string{"a", "b", "c"},
			s:        "b",
			expected: true,
		},
		{
			name:     "does not contain the string",
			slice:    []string{"a", "b", "c"},
			s:        "d",
			expected: false,
		},
		{
			name:     "empty slice",
			slice:    []string{},
			s:        "a",
			expected: false,
		},
		{
			name:     "nil slice",
			slice:    nil,
			s:        "a",
			expected: false,
		},
		{
			name:     "empty string in slice",
			slice:    []string{"", "a"},
			s:        "",
			expected: true,
		},
		{
			name:     "single element match",
			slice:    []string{"only"},
			s:        "only",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ContainsString(tt.slice, tt.s)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveString(t *testing.T) {
	tests := []struct {
		name     string
		slice    []string
		s        string
		expected []string
	}{
		{
			name:     "remove existing element",
			slice:    []string{"a", "b", "c"},
			s:        "b",
			expected: []string{"a", "c"},
		},
		{
			name:     "remove non-existing element",
			slice:    []string{"a", "b", "c"},
			s:        "d",
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "empty slice",
			slice:    []string{},
			s:        "a",
			expected: []string{},
		},
		{
			name:     "nil slice",
			slice:    nil,
			s:        "a",
			expected: []string{},
		},
		{
			name:     "remove all occurrences",
			slice:    []string{"a", "b", "a", "c"},
			s:        "a",
			expected: []string{"b", "c"},
		},
		{
			name:     "remove only element",
			slice:    []string{"only"},
			s:        "only",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RemoveString(tt.slice, tt.s)
			assert.Equal(t, tt.expected, result)
		})
	}
}

package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeHash(t *testing.T) {
	tests := []struct {
		name     string
		data     map[string]string
		expected string // empty means we just check properties
	}{
		{
			name:     "empty map returns empty string",
			data:     map[string]string{},
			expected: "",
		},
		{
			name: "nil map returns empty string",
			data: nil,
		},
		{
			name: "single key-value pair",
			data: map[string]string{"key": "value"},
		},
		{
			name: "multiple key-value pairs",
			data: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeHash(tt.data)
			if tt.expected != "" {
				assert.Equal(t, tt.expected, result)
			}
			if tt.data == nil || len(tt.data) == 0 {
				assert.Empty(t, result)
			}
		})
	}
}

func TestComputeHash_Deterministic(t *testing.T) {
	data := map[string]string{"z": "last", "a": "first", "m": "middle"}
	hash1 := ComputeHash(data)
	hash2 := ComputeHash(data)
	assert.Equal(t, hash1, hash2, "hash should be deterministic")
}

func TestComputeHash_DifferentInputs(t *testing.T) {
	data1 := map[string]string{"key": "value1"}
	data2 := map[string]string{"key": "value2"}
	hash1 := ComputeHash(data1)
	hash2 := ComputeHash(data2)
	assert.NotEqual(t, hash1, hash2, "different inputs should produce different hashes")
}

func TestComputeHash_OrderIndependent(t *testing.T) {
	// Since keys are sorted, order of insertion shouldn't matter
	data1 := map[string]string{"a": "1", "b": "2"}
	data2 := map[string]string{"b": "2", "a": "1"}
	assert.Equal(t, ComputeHash(data1), ComputeHash(data2))
}

func TestComputeStringHash(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "empty string",
			input: "",
		},
		{
			name:  "simple string",
			input: "hello",
		},
		{
			name:  "string with special characters",
			input: "hello world! @#$%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeStringHash(tt.input)
			require.NotEmpty(t, result)
			assert.Len(t, result, 64, "SHA-256 hex should be 64 characters")
		})
	}
}

func TestComputeStringHash_Deterministic(t *testing.T) {
	hash1 := ComputeStringHash("test")
	hash2 := ComputeStringHash("test")
	assert.Equal(t, hash1, hash2)
}

func TestComputeStringHash_DifferentInputs(t *testing.T) {
	hash1 := ComputeStringHash("hello")
	hash2 := ComputeStringHash("world")
	assert.NotEqual(t, hash1, hash2)
}

func TestComputeSliceHash(t *testing.T) {
	tests := []struct {
		name  string
		items []string
	}{
		{
			name:  "empty slice",
			items: []string{},
		},
		{
			name:  "nil slice",
			items: nil,
		},
		{
			name:  "single item",
			items: []string{"item"},
		},
		{
			name:  "multiple items",
			items: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeSliceHash(tt.items)
			assert.NotEmpty(t, result)
			assert.Len(t, result, 64)
		})
	}
}

func TestComputeSliceHash_OrderMatters(t *testing.T) {
	hash1 := ComputeSliceHash([]string{"a", "b"})
	hash2 := ComputeSliceHash([]string{"b", "a"})
	assert.NotEqual(t, hash1, hash2, "order should matter for slice hash")
}

func TestShortHash(t *testing.T) {
	tests := []struct {
		name     string
		hash     string
		expected string
	}{
		{
			name:     "long hash truncated",
			hash:     "abcdef1234567890",
			expected: "abcdef12",
		},
		{
			name:     "exactly 8 characters",
			hash:     "abcdef12",
			expected: "abcdef12",
		},
		{
			name:     "shorter than 8 characters",
			hash:     "abc",
			expected: "abc",
		},
		{
			name:     "empty string",
			hash:     "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShortHash(tt.hash)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHashesEqual(t *testing.T) {
	tests := []struct {
		name     string
		a        string
		b        string
		expected bool
	}{
		{
			name:     "equal hashes",
			a:        "abc123",
			b:        "abc123",
			expected: true,
		},
		{
			name:     "case insensitive",
			a:        "ABC123",
			b:        "abc123",
			expected: true,
		},
		{
			name:     "different hashes",
			a:        "abc123",
			b:        "def456",
			expected: false,
		},
		{
			name:     "empty strings",
			a:        "",
			b:        "",
			expected: true,
		},
		{
			name:     "one empty",
			a:        "abc",
			b:        "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HashesEqual(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

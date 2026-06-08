package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPtr(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "int pointer",
			fn: func(t *testing.T) {
				p := Ptr(42)
				require.NotNil(t, p)
				assert.Equal(t, 42, *p)
			},
		},
		{
			name: "string pointer",
			fn: func(t *testing.T) {
				p := Ptr("hello")
				require.NotNil(t, p)
				assert.Equal(t, "hello", *p)
			},
		},
		{
			name: "bool pointer",
			fn: func(t *testing.T) {
				p := Ptr(true)
				require.NotNil(t, p)
				assert.True(t, *p)
			},
		},
		{
			name: "int32 pointer",
			fn: func(t *testing.T) {
				p := Ptr(int32(1))
				require.NotNil(t, p)
				assert.Equal(t, int32(1), *p)
			},
		},
		{
			name: "zero value pointer",
			fn: func(t *testing.T) {
				p := Ptr(0)
				require.NotNil(t, p)
				assert.Equal(t, 0, *p)
			},
		},
		{
			name: "empty string pointer",
			fn: func(t *testing.T) {
				p := Ptr("")
				require.NotNil(t, p)
				assert.Equal(t, "", *p)
			},
		},
		{
			name: "float64 pointer",
			fn: func(t *testing.T) {
				p := Ptr(3.14)
				require.NotNil(t, p)
				assert.InDelta(t, 3.14, *p, 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}

func TestDeref(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "non-nil int pointer",
			fn: func(t *testing.T) {
				v := 42
				result := Deref(&v)
				assert.Equal(t, 42, result)
			},
		},
		{
			name: "nil int pointer returns zero",
			fn: func(t *testing.T) {
				var p *int
				result := Deref(p)
				assert.Equal(t, 0, result)
			},
		},
		{
			name: "non-nil string pointer",
			fn: func(t *testing.T) {
				v := "hello"
				result := Deref(&v)
				assert.Equal(t, "hello", result)
			},
		},
		{
			name: "nil string pointer returns empty",
			fn: func(t *testing.T) {
				var p *string
				result := Deref(p)
				assert.Equal(t, "", result)
			},
		},
		{
			name: "nil bool pointer returns false",
			fn: func(t *testing.T) {
				var p *bool
				result := Deref(p)
				assert.False(t, result)
			},
		},
		{
			name: "non-nil bool pointer",
			fn: func(t *testing.T) {
				v := true
				result := Deref(&v)
				assert.True(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}

func TestDerefOr(t *testing.T) {
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			name: "non-nil pointer returns value",
			fn: func(t *testing.T) {
				v := 42
				result := DerefOr(&v, 99)
				assert.Equal(t, 42, result)
			},
		},
		{
			name: "nil pointer returns default",
			fn: func(t *testing.T) {
				var p *int
				result := DerefOr(p, 99)
				assert.Equal(t, 99, result)
			},
		},
		{
			name: "nil string pointer returns default",
			fn: func(t *testing.T) {
				var p *string
				result := DerefOr(p, "default")
				assert.Equal(t, "default", result)
			},
		},
		{
			name: "non-nil string pointer returns value",
			fn: func(t *testing.T) {
				v := "actual"
				result := DerefOr(&v, "default")
				assert.Equal(t, "actual", result)
			},
		},
		{
			name: "nil bool pointer returns default true",
			fn: func(t *testing.T) {
				var p *bool
				result := DerefOr(p, true)
				assert.True(t, result)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}

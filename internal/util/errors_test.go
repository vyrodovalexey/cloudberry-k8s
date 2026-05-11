package util

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"ErrNotFound", ErrNotFound},
		{"ErrAlreadyExists", ErrAlreadyExists},
		{"ErrInvalidInput", ErrInvalidInput},
		{"ErrUnauthorized", ErrUnauthorized},
		{"ErrForbidden", ErrForbidden},
		{"ErrTimeout", ErrTimeout},
		{"ErrConnectionFailed", ErrConnectionFailed},
		{"ErrRetryExhausted", ErrRetryExhausted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotNil(t, tt.err)
			assert.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestClusterNotFoundError(t *testing.T) {
	tests := []struct {
		name      string
		errName   string
		namespace string
	}{
		{
			name:      "basic cluster not found",
			errName:   "my-cluster",
			namespace: "default",
		},
		{
			name:      "empty namespace",
			errName:   "test",
			namespace: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewClusterNotFoundError(tt.errName, tt.namespace)
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), tt.errName)
			assert.Contains(t, err.Error(), tt.namespace)
			assert.True(t, errors.Is(err, ErrNotFound))

			var cnfErr *ClusterNotFoundError
			assert.True(t, errors.As(err, &cnfErr))
			assert.Equal(t, tt.errName, cnfErr.Name)
			assert.Equal(t, tt.namespace, cnfErr.Namespace)
		})
	}
}

func TestSegmentNotFoundError(t *testing.T) {
	err := NewSegmentNotFoundError(5, "my-cluster")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "5")
	assert.Contains(t, err.Error(), "my-cluster")
	assert.True(t, errors.Is(err, ErrNotFound))

	var snfErr *SegmentNotFoundError
	assert.True(t, errors.As(err, &snfErr))
	assert.Equal(t, int32(5), snfErr.ContentID)
	assert.Equal(t, "my-cluster", snfErr.Cluster)
}

func TestValidationError(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		message string
	}{
		{
			name:    "field validation error",
			field:   "spec.segments.count",
			message: "must be >= 1",
		},
		{
			name:    "empty field",
			field:   "",
			message: "required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NewValidationError(tt.field, tt.message)
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), tt.field)
			assert.Contains(t, err.Error(), tt.message)
			assert.True(t, errors.Is(err, ErrInvalidInput))

			var valErr *ValidationError
			assert.True(t, errors.As(err, &valErr))
			assert.Equal(t, tt.field, valErr.Field)
			assert.Equal(t, tt.message, valErr.Message)
		})
	}
}

func TestAuthenticationError(t *testing.T) {
	err := NewAuthenticationError("basic", "invalid credentials")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "basic")
	assert.Contains(t, err.Error(), "invalid credentials")
	assert.True(t, errors.Is(err, ErrUnauthorized))

	var authErr *AuthenticationError
	assert.True(t, errors.As(err, &authErr))
	assert.Equal(t, "basic", authErr.Method)
	assert.Equal(t, "invalid credentials", authErr.Message)
}

func TestPermissionDeniedError(t *testing.T) {
	err := NewPermissionDeniedError("user1", "delete", "admin")
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "user1")
	assert.Contains(t, err.Error(), "delete")
	assert.Contains(t, err.Error(), "admin")
	assert.True(t, errors.Is(err, ErrForbidden))

	var pdErr *PermissionDeniedError
	assert.True(t, errors.As(err, &pdErr))
	assert.Equal(t, "user1", pdErr.User)
	assert.Equal(t, "delete", pdErr.Operation)
	assert.Equal(t, "admin", pdErr.Required)
}

func TestReconcileError(t *testing.T) {
	innerErr := fmt.Errorf("connection refused")
	err := NewReconcileError("create-statefulset", innerErr)
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "create-statefulset")
	assert.Contains(t, err.Error(), "connection refused")
	assert.True(t, errors.Is(err, innerErr))

	var recErr *ReconcileError
	assert.True(t, errors.As(err, &recErr))
	assert.Equal(t, "create-statefulset", recErr.Operation)
	assert.Equal(t, innerErr, recErr.Err)
}

func TestReconcileError_WrapsNotFound(t *testing.T) {
	err := NewReconcileError("get-cluster", ErrNotFound)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestErrorChaining(t *testing.T) {
	// Test that errors can be wrapped and unwrapped correctly
	clusterErr := NewClusterNotFoundError("test", "ns")
	wrappedErr := fmt.Errorf("operation failed: %w", clusterErr)

	assert.True(t, errors.Is(wrappedErr, ErrNotFound))

	var cnfErr *ClusterNotFoundError
	assert.True(t, errors.As(wrappedErr, &cnfErr))
}

package util

import (
	"errors"
	"fmt"
)

// Sentinel errors for common error conditions.
var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = errors.New("not found")
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("already exists")
	// ErrInvalidInput is returned when input validation fails.
	ErrInvalidInput = errors.New("invalid input")
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("unauthorized")
	// ErrForbidden is returned when authorization fails.
	ErrForbidden = errors.New("forbidden")
	// ErrTimeout is returned when an operation times out.
	ErrTimeout = errors.New("timeout")
	// ErrConnectionFailed is returned when a connection cannot be established.
	ErrConnectionFailed = errors.New("connection failed")
	// ErrRetryExhausted is returned when all retry attempts are exhausted.
	ErrRetryExhausted = errors.New("retry attempts exhausted")
)

// ClusterNotFoundError indicates a cluster was not found.
type ClusterNotFoundError struct {
	Name      string
	Namespace string
}

// Error returns the error message.
func (e *ClusterNotFoundError) Error() string {
	return fmt.Sprintf("cluster %q not found in namespace %q", e.Name, e.Namespace)
}

// Unwrap returns the underlying sentinel error.
func (e *ClusterNotFoundError) Unwrap() error {
	return ErrNotFound
}

// SegmentNotFoundError indicates a segment was not found.
type SegmentNotFoundError struct {
	ContentID int32
	Cluster   string
}

// Error returns the error message.
func (e *SegmentNotFoundError) Error() string {
	return fmt.Sprintf("segment with content ID %d not found in cluster %q", e.ContentID, e.Cluster)
}

// Unwrap returns the underlying sentinel error.
func (e *SegmentNotFoundError) Unwrap() error {
	return ErrNotFound
}

// ValidationError indicates a validation failure.
type ValidationError struct {
	Field   string
	Message string
}

// Error returns the error message.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on field %q: %s", e.Field, e.Message)
}

// Unwrap returns the underlying sentinel error.
func (e *ValidationError) Unwrap() error {
	return ErrInvalidInput
}

// AuthenticationError indicates an authentication failure.
type AuthenticationError struct {
	Method  string
	Message string
}

// Error returns the error message.
func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("authentication failed (method=%s): %s", e.Method, e.Message)
}

// Unwrap returns the underlying sentinel error.
func (e *AuthenticationError) Unwrap() error {
	return ErrUnauthorized
}

// PermissionDeniedError indicates an authorization failure.
type PermissionDeniedError struct {
	User      string
	Operation string
	Required  string
}

// Error returns the error message.
func (e *PermissionDeniedError) Error() string {
	return fmt.Sprintf(
		"permission denied: user %q requires %q permission for operation %q",
		e.User, e.Required, e.Operation,
	)
}

// Unwrap returns the underlying sentinel error.
func (e *PermissionDeniedError) Unwrap() error {
	return ErrForbidden
}

// ReconcileError wraps an error that occurred during reconciliation.
type ReconcileError struct {
	Operation string
	Err       error
}

// Error returns the error message.
func (e *ReconcileError) Error() string {
	return fmt.Sprintf("reconcile error during %q: %v", e.Operation, e.Err)
}

// Unwrap returns the underlying error.
func (e *ReconcileError) Unwrap() error {
	return e.Err
}

// NewClusterNotFoundError creates a new ClusterNotFoundError.
func NewClusterNotFoundError(name, namespace string) *ClusterNotFoundError {
	return &ClusterNotFoundError{Name: name, Namespace: namespace}
}

// NewSegmentNotFoundError creates a new SegmentNotFoundError.
func NewSegmentNotFoundError(contentID int32, cluster string) *SegmentNotFoundError {
	return &SegmentNotFoundError{ContentID: contentID, Cluster: cluster}
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field, message string) *ValidationError {
	return &ValidationError{Field: field, Message: message}
}

// NewAuthenticationError creates a new AuthenticationError.
func NewAuthenticationError(method, message string) *AuthenticationError {
	return &AuthenticationError{Method: method, Message: message}
}

// NewPermissionDeniedError creates a new PermissionDeniedError.
func NewPermissionDeniedError(user, operation, required string) *PermissionDeniedError {
	return &PermissionDeniedError{User: user, Operation: operation, Required: required}
}

// NewReconcileError creates a new ReconcileError.
func NewReconcileError(operation string, err error) *ReconcileError {
	return &ReconcileError{Operation: operation, Err: err}
}

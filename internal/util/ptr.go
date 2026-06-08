package util

// Ptr returns a pointer to the given value.
func Ptr[T any](v T) *T {
	return &v
}

// Deref returns the value pointed to by the pointer, or the zero value if the pointer is nil.
func Deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// DerefOr returns the value pointed to by the pointer, or the default value if the pointer is nil.
func DerefOr[T any](p *T, defaultVal T) T {
	if p == nil {
		return defaultVal
	}
	return *p
}

package util

import (
	"crypto/rand"
	"math/big"
	"regexp"
	"strings"
)

const (
	// maxK8sNameLength is the maximum length for Kubernetes resource names.
	maxK8sNameLength = 63
)

var k8sNameRegex = regexp.MustCompile(`[^a-z0-9-]`)

// TruncateName truncates a name to the specified maximum length.
// If the name is already within the limit, it is returned unchanged.
func TruncateName(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}
	return name[:maxLen]
}

// SanitizeK8sName converts a string to a valid Kubernetes resource name.
// It lowercases the string, replaces invalid characters with hyphens,
// removes leading/trailing hyphens, and truncates to 63 characters.
func SanitizeK8sName(name string) string {
	sanitized := strings.ToLower(name)
	sanitized = k8sNameRegex.ReplaceAllString(sanitized, "-")
	sanitized = strings.Trim(sanitized, "-")
	return TruncateName(sanitized, maxK8sNameLength)
}

// ContainsString checks if a string slice contains a specific string.
func ContainsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// RemoveString removes a string from a slice and returns the new slice.
func RemoveString(slice []string, s string) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if item != s {
			result = append(result, item)
		}
	}
	return result
}

const (
	// defaultPasswordLength is the length of generated admin passwords.
	defaultPasswordLength = 32
	// passwordChars is the set of characters used for password generation.
	passwordChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+"
)

// GenerateRandomPassword generates a cryptographically secure random password.
func GenerateRandomPassword() (string, error) {
	password := make([]byte, defaultPasswordLength)
	charsetLen := big.NewInt(int64(len(passwordChars)))
	for i := range password {
		idx, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			return "", err
		}
		password[i] = passwordChars[idx.Int64()]
	}
	return string(password), nil
}

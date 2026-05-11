package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// ComputeHash computes a SHA-256 hash of the given data map.
// Keys are sorted to ensure deterministic output.
func ComputeHash(data map[string]string) string {
	if len(data) == 0 {
		return ""
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, data[k])
	}

	return hex.EncodeToString(h.Sum(nil))
}

// ComputeStringHash computes a SHA-256 hash of a single string.
func ComputeStringHash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// ComputeSliceHash computes a SHA-256 hash of a string slice.
func ComputeSliceHash(items []string) string {
	h := sha256.New()
	for _, item := range items {
		fmt.Fprintf(h, "%s\n", item)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ShortHash returns the first 8 characters of a hash.
func ShortHash(hash string) string {
	if len(hash) <= 8 {
		return hash
	}
	return hash[:8]
}

// HashesEqual compares two hashes for equality in a case-insensitive manner.
func HashesEqual(a, b string) bool {
	return strings.EqualFold(a, b)
}

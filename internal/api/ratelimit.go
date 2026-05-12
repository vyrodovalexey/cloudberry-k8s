// Package api provides the REST API server for the cloudberry operator.
package api

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// defaultRateLimit is the default number of requests allowed per interval.
	defaultRateLimit = 10
	// defaultRateInterval is the default rate limiting interval.
	defaultRateInterval = time.Minute
	// defaultCleanupInterval is the interval for cleaning up expired rate limit entries.
	defaultCleanupInterval = 5 * time.Minute
	// retryAfterHeader is the HTTP header indicating when the client can retry.
	retryAfterHeader = "Retry-After"
)

// rateLimitEntry tracks the token bucket state for a single IP.
type rateLimitEntry struct {
	tokens     float64
	lastRefill time.Time
}

// RateLimiter implements a per-IP token bucket rate limiter.
type RateLimiter struct {
	mu       sync.Mutex
	entries  map[string]*rateLimitEntry
	limit    int
	interval time.Duration
	logger   *slog.Logger
}

// NewRateLimiter creates a new per-IP rate limiter.
// limit is the maximum number of requests allowed per interval.
func NewRateLimiter(limit int, interval time.Duration, logger *slog.Logger) *RateLimiter {
	if limit <= 0 {
		limit = defaultRateLimit
	}
	if interval <= 0 {
		interval = defaultRateInterval
	}
	if logger == nil {
		logger = slog.Default()
	}

	rl := &RateLimiter{
		entries:  make(map[string]*rateLimitEntry),
		limit:    limit,
		interval: interval,
		logger:   logger.With("component", "rate-limiter"),
	}

	// Start background cleanup goroutine to prevent unbounded memory growth.
	go rl.cleanupLoop()

	return rl
}

// Allow checks whether a request from the given IP is allowed.
// Returns true if the request is within the rate limit.
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	entry, exists := rl.entries[ip]
	if !exists {
		rl.entries[ip] = &rateLimitEntry{
			tokens:     float64(rl.limit) - 1,
			lastRefill: now,
		}
		return true
	}

	// Refill tokens based on elapsed time.
	elapsed := now.Sub(entry.lastRefill)
	refillRate := float64(rl.limit) / rl.interval.Seconds()
	entry.tokens += elapsed.Seconds() * refillRate
	entry.lastRefill = now

	// Cap tokens at the limit.
	if entry.tokens > float64(rl.limit) {
		entry.tokens = float64(rl.limit)
	}

	if entry.tokens < 1 {
		return false
	}

	entry.tokens--
	return true
}

// RetryAfterSeconds returns the number of seconds until the next token is available.
func (rl *RateLimiter) RetryAfterSeconds() int {
	refillRate := float64(rl.limit) / rl.interval.Seconds()
	if refillRate <= 0 {
		return 1
	}
	seconds := int(1.0/refillRate) + 1
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

// cleanupLoop periodically removes expired entries to prevent memory leaks.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(defaultCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

// cleanup removes entries that have been fully refilled (inactive clients).
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	for ip, entry := range rl.entries {
		// Remove entries that haven't been used for longer than the interval.
		if now.Sub(entry.lastRefill) > rl.interval*2 {
			delete(rl.entries, ip)
		}
	}
}

// Middleware returns an HTTP middleware that applies rate limiting per client IP.
// Requests that exceed the rate limit receive a 429 Too Many Requests response
// with a Retry-After header.
func (rl *RateLimiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := extractClientIP(r)
		if !rl.Allow(ip) {
			retryAfter := rl.RetryAfterSeconds()
			w.Header().Set(retryAfterHeader, fmt.Sprintf("%d", retryAfter))
			rl.logger.Warn("rate limit exceeded", "ip", ip)
			writeErrorJSON(w, http.StatusTooManyRequests, "RATE_LIMITED",
				"too many requests, please retry later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractClientIP extracts the client IP from the request.
// It prefers X-Forwarded-For, then X-Real-IP, then falls back to RemoteAddr.
func extractClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (first IP in the chain).
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can contain multiple IPs; use the first one.
		for i := range len(xff) {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}

	// Check X-Real-IP header.
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr, stripping the port.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

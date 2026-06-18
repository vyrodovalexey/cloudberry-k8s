// Package api provides the REST API server for the cloudberry operator.
package api

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
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
	mu             sync.Mutex
	entries        map[string]*rateLimitEntry
	limit          int
	interval       time.Duration
	logger         *slog.Logger
	stopCh         chan struct{}
	stopOnce       sync.Once
	trustedProxies []net.IPNet
	// onReject is an optional callback invoked on every 429 rejection. The
	// Server uses it to record cloudberry_api_rate_limit_rejections_total
	// with the matched route template.
	onReject func(r *http.Request)
}

// NewRateLimiter creates a new per-IP rate limiter.
// limit is the maximum number of requests allowed per interval.
// opts can be used to configure trusted proxies via WithTrustedProxies.
func NewRateLimiter(limit int, interval time.Duration, logger *slog.Logger, opts ...RateLimiterOption) *RateLimiter {
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
		stopCh:   make(chan struct{}),
	}

	for _, opt := range opts {
		opt(rl)
	}

	// Start background cleanup goroutine to prevent unbounded memory growth.
	go rl.cleanupLoop()

	return rl
}

// RateLimiterOption configures a RateLimiter.
type RateLimiterOption func(*RateLimiter)

// WithTrustedProxies configures the CIDR ranges whose X-Forwarded-For and
// X-Real-IP headers are trusted. When the list is empty (the default),
// only RemoteAddr is used for client identification.
func WithTrustedProxies(cidrs []string) RateLimiterOption {
	return func(rl *RateLimiter) {
		for _, cidr := range cidrs {
			_, ipNet, err := net.ParseCIDR(cidr)
			if err != nil {
				rl.logger.Warn("ignoring invalid trusted proxy CIDR", "cidr", cidr, "error", err)
				continue
			}
			rl.trustedProxies = append(rl.trustedProxies, *ipNet)
		}
	}
}

// WithRejectionCallback configures a callback invoked for every rejected
// (429) request, used to record rejection metrics with route labels.
func WithRejectionCallback(cb func(r *http.Request)) RateLimiterOption {
	return func(rl *RateLimiter) {
		rl.onReject = cb
	}
}

// Stop stops the background cleanup goroutine. It should be called when the
// RateLimiter is no longer needed to prevent goroutine leaks.
// Stop is safe to call multiple times.
func (rl *RateLimiter) Stop() {
	rl.stopOnce.Do(func() {
		close(rl.stopCh)
	})
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
// It exits when the stopCh channel is closed.
func (rl *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(defaultCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rl.cleanup()
		case <-rl.stopCh:
			return
		}
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
		ip := rl.extractClientIP(r)
		if !rl.Allow(ip) {
			retryAfter := rl.RetryAfterSeconds()
			w.Header().Set(retryAfterHeader, fmt.Sprintf("%d", retryAfter))
			rl.logger.Warn("rate limit exceeded", "ip", ip)
			if rl.onReject != nil {
				rl.onReject(r)
			}
			writeErrorJSON(w, http.StatusTooManyRequests, "RATE_LIMITED",
				"too many requests, please retry later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// extractClientIP extracts the client IP from the request.
// It only trusts X-Forwarded-For and X-Real-IP headers when the direct
// connection comes from a trusted proxy. Otherwise, RemoteAddr is used
// to prevent header spoofing attacks.
func (rl *RateLimiter) extractClientIP(r *http.Request) string {
	remoteHost := extractRemoteHost(r.RemoteAddr)

	// Only trust forwarded headers when the remote address is a trusted proxy.
	if rl.isTrustedProxy(remoteHost) {
		// Check X-Forwarded-For header (first IP in the chain).
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// X-Forwarded-For can contain multiple IPs; use the first one.
			// Trim surrounding whitespace because real-world headers are
			// comma+space separated ("a, b").
			for i := range len(xff) {
				if xff[i] == ',' {
					return strings.TrimSpace(xff[:i])
				}
			}
			return strings.TrimSpace(xff)
		}

		// Check X-Real-IP header.
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}

	return remoteHost
}

// extractRemoteHost extracts the host part from a RemoteAddr, stripping the port.
func extractRemoteHost(remoteAddr string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return remoteAddr
	}
	return host
}

// isTrustedProxy checks whether the given IP is within any of the configured
// trusted proxy CIDR ranges.
func (rl *RateLimiter) isTrustedProxy(ip string) bool {
	if len(rl.trustedProxies) == 0 {
		return false
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for i := range rl.trustedProxies {
		if rl.trustedProxies[i].Contains(parsed) {
			return true
		}
	}
	return false
}

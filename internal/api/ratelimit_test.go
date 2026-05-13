package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRateLimiter(t *testing.T) {
	tests := []struct {
		name         string
		limit        int
		interval     time.Duration
		logger       *slog.Logger
		wantLimit    int
		wantInterval time.Duration
	}{
		{
			name:         "valid config",
			limit:        10,
			interval:     time.Minute,
			logger:       slog.Default(),
			wantLimit:    10,
			wantInterval: time.Minute,
		},
		{
			name:         "zero limit uses default",
			limit:        0,
			interval:     time.Minute,
			logger:       slog.Default(),
			wantLimit:    defaultRateLimit,
			wantInterval: time.Minute,
		},
		{
			name:         "negative limit uses default",
			limit:        -5,
			interval:     time.Minute,
			logger:       slog.Default(),
			wantLimit:    defaultRateLimit,
			wantInterval: time.Minute,
		},
		{
			name:         "zero interval uses default",
			limit:        10,
			interval:     0,
			logger:       slog.Default(),
			wantLimit:    10,
			wantInterval: defaultRateInterval,
		},
		{
			name:         "negative interval uses default",
			limit:        10,
			interval:     -time.Second,
			logger:       slog.Default(),
			wantLimit:    10,
			wantInterval: defaultRateInterval,
		},
		{
			name:         "nil logger uses default",
			limit:        10,
			interval:     time.Minute,
			logger:       nil,
			wantLimit:    10,
			wantInterval: time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.limit, tt.interval, tt.logger)
			require.NotNil(t, rl)
			assert.Equal(t, tt.wantLimit, rl.limit)
			assert.Equal(t, tt.wantInterval, rl.interval)
			assert.NotNil(t, rl.entries)
			assert.NotNil(t, rl.logger)
		})
	}
}

func TestRateLimiter_Allow_WithinLimit(t *testing.T) {
	rl := NewRateLimiter(5, time.Minute, slog.Default())

	// First 5 requests should be allowed
	for i := range 5 {
		assert.True(t, rl.Allow("192.168.1.1"), "request %d should be allowed", i+1)
	}
}

func TestRateLimiter_Allow_OverLimit(t *testing.T) {
	rl := NewRateLimiter(3, time.Minute, slog.Default())

	// First 3 requests should be allowed
	for i := range 3 {
		assert.True(t, rl.Allow("192.168.1.1"), "request %d should be allowed", i+1)
	}

	// 4th request should be denied
	assert.False(t, rl.Allow("192.168.1.1"), "request 4 should be denied")
}

func TestRateLimiter_Allow_DifferentIPs(t *testing.T) {
	rl := NewRateLimiter(2, time.Minute, slog.Default())

	// Each IP gets its own bucket
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.False(t, rl.Allow("192.168.1.1"))

	// Different IP should still be allowed
	assert.True(t, rl.Allow("192.168.1.2"))
	assert.True(t, rl.Allow("192.168.1.2"))
	assert.False(t, rl.Allow("192.168.1.2"))
}

func TestRateLimiter_Allow_TokenRefill(t *testing.T) {
	// Use a very short interval so tokens refill quickly
	rl := NewRateLimiter(2, 100*time.Millisecond, slog.Default())

	// Use up all tokens
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.False(t, rl.Allow("192.168.1.1"))

	// Wait for tokens to refill
	time.Sleep(150 * time.Millisecond)

	// Should be allowed again
	assert.True(t, rl.Allow("192.168.1.1"))
}

func TestRateLimiter_RetryAfterSeconds(t *testing.T) {
	tests := []struct {
		name     string
		limit    int
		interval time.Duration
		wantMin  int
	}{
		{
			name:     "10 per minute",
			limit:    10,
			interval: time.Minute,
			wantMin:  1,
		},
		{
			name:     "1 per minute",
			limit:    1,
			interval: time.Minute,
			wantMin:  1,
		},
		{
			name:     "100 per second",
			limit:    100,
			interval: time.Second,
			wantMin:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rl := NewRateLimiter(tt.limit, tt.interval, slog.Default())
			seconds := rl.RetryAfterSeconds()
			assert.GreaterOrEqual(t, seconds, tt.wantMin)
		})
	}
}

func TestRateLimiter_Middleware_Allowed(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute, slog.Default())

	handlerCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := rl.Middleware(next)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.True(t, handlerCalled)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRateLimiter_Middleware_RateLimited(t *testing.T) {
	rl := NewRateLimiter(1, time.Minute, slog.Default())

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := rl.Middleware(next)

	// First request should pass
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.RemoteAddr = "192.168.1.1:12345"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)
	assert.Equal(t, http.StatusOK, rec1.Code)

	// Second request should be rate limited
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.RemoteAddr = "192.168.1.1:12345"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusTooManyRequests, rec2.Code)
	assert.NotEmpty(t, rec2.Header().Get(retryAfterHeader))
}

func TestExtractClientIP_NoTrustedProxies(t *testing.T) {
	// Without trusted proxies, forwarded headers are ignored.
	rl := NewRateLimiter(10, time.Minute, slog.Default())
	defer rl.Stop()

	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expected   string
	}{
		{
			name:       "X-Forwarded-For ignored without trusted proxies",
			headers:    map[string]string{"X-Forwarded-For": "10.0.0.1"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name:       "X-Real-IP ignored without trusted proxies",
			headers:    map[string]string{"X-Real-IP": "10.0.0.5"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name:       "RemoteAddr with port",
			headers:    map[string]string{},
			remoteAddr: "192.168.1.1:12345",
			expected:   "192.168.1.1",
		},
		{
			name:       "RemoteAddr without port",
			headers:    map[string]string{},
			remoteAddr: "192.168.1.1",
			expected:   "192.168.1.1",
		},
		{
			name:       "IPv6 RemoteAddr",
			headers:    map[string]string{},
			remoteAddr: "[::1]:12345",
			expected:   "::1",
		},
		{
			name:       "empty headers fallback to RemoteAddr",
			headers:    map[string]string{},
			remoteAddr: "10.0.0.100:8080",
			expected:   "10.0.0.100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			result := rl.extractClientIP(req)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractClientIP_WithTrustedProxies(t *testing.T) {
	// With trusted proxies, forwarded headers are trusted from proxy IPs.
	rl := NewRateLimiter(10, time.Minute, slog.Default(),
		WithTrustedProxies([]string{"192.168.1.0/24"}))
	defer rl.Stop()

	tests := []struct {
		name       string
		headers    map[string]string
		remoteAddr string
		expected   string
	}{
		{
			name:       "X-Forwarded-For trusted from proxy",
			headers:    map[string]string{"X-Forwarded-For": "10.0.0.1"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "10.0.0.1",
		},
		{
			name:       "X-Forwarded-For multiple IPs from proxy",
			headers:    map[string]string{"X-Forwarded-For": "10.0.0.1, 10.0.0.2, 10.0.0.3"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "10.0.0.1",
		},
		{
			name:       "X-Real-IP trusted from proxy",
			headers:    map[string]string{"X-Real-IP": "10.0.0.5"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "10.0.0.5",
		},
		{
			name:       "X-Forwarded-For takes precedence over X-Real-IP from proxy",
			headers:    map[string]string{"X-Forwarded-For": "10.0.0.1", "X-Real-IP": "10.0.0.5"},
			remoteAddr: "192.168.1.1:12345",
			expected:   "10.0.0.1",
		},
		{
			name:       "X-Forwarded-For ignored from non-proxy",
			headers:    map[string]string{"X-Forwarded-For": "10.0.0.1"},
			remoteAddr: "10.10.10.10:12345",
			expected:   "10.10.10.10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.RemoteAddr = tt.remoteAddr
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			result := rl.extractClientIP(req)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRateLimiter_ConcurrentAccess(t *testing.T) {
	rl := NewRateLimiter(100, time.Minute, slog.Default())

	var wg sync.WaitGroup
	allowedCount := 0
	var mu sync.Mutex

	// Launch 200 concurrent requests from the same IP
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow("192.168.1.1") {
				mu.Lock()
				allowedCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Exactly 100 should be allowed
	assert.Equal(t, 100, allowedCount)
}

func TestRateLimiter_ConcurrentAccess_DifferentIPs(t *testing.T) {
	rl := NewRateLimiter(5, time.Minute, slog.Default())

	var wg sync.WaitGroup
	results := make(map[string]int)
	var mu sync.Mutex

	// 10 different IPs, each making 10 requests
	for i := range 10 {
		ip := fmt.Sprintf("192.168.1.%d", i)
		for range 10 {
			wg.Add(1)
			go func(ip string) {
				defer wg.Done()
				if rl.Allow(ip) {
					mu.Lock()
					results[ip]++
					mu.Unlock()
				}
			}(ip)
		}
	}

	wg.Wait()

	// Each IP should have at most 5 allowed requests
	for ip, count := range results {
		assert.LessOrEqual(t, count, 5, "IP %s had %d allowed requests", ip, count)
	}
}

func TestRateLimiter_Cleanup(t *testing.T) {
	rl := NewRateLimiter(10, 50*time.Millisecond, slog.Default())

	// Add some entries
	rl.Allow("192.168.1.1")
	rl.Allow("192.168.1.2")

	assert.Len(t, rl.entries, 2)

	// Wait for entries to expire (2x interval)
	time.Sleep(150 * time.Millisecond)

	// Manually trigger cleanup
	rl.cleanup()

	assert.Empty(t, rl.entries)
}

func TestRateLimiter_Cleanup_ActiveEntries(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute, slog.Default())

	// Add entries
	rl.Allow("192.168.1.1")
	rl.Allow("192.168.1.2")

	// Cleanup should not remove active entries
	rl.cleanup()

	assert.Len(t, rl.entries, 2)
}

func TestRateLimiter_TokenCap(t *testing.T) {
	rl := NewRateLimiter(3, 50*time.Millisecond, slog.Default())

	// Use one token
	assert.True(t, rl.Allow("192.168.1.1"))

	// Wait for tokens to refill beyond the limit
	time.Sleep(200 * time.Millisecond)

	// Should still only have 3 tokens (capped at limit)
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.True(t, rl.Allow("192.168.1.1"))
	assert.False(t, rl.Allow("192.168.1.1"))
}

func TestRateLimiter_Middleware_RetryAfterHeader(t *testing.T) {
	rl := NewRateLimiter(1, time.Minute, slog.Default())

	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := rl.Middleware(next)

	// Exhaust the limit
	req1 := httptest.NewRequest(http.MethodGet, "/", nil)
	req1.RemoteAddr = "10.0.0.1:1234"
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	// Rate limited request should have Retry-After header
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.RemoteAddr = "10.0.0.1:1234"
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	assert.Equal(t, http.StatusTooManyRequests, rec2.Code)
	retryAfter := rec2.Header().Get(retryAfterHeader)
	assert.NotEmpty(t, retryAfter)
}

func TestRateLimiterConstants(t *testing.T) {
	assert.Equal(t, 10, defaultRateLimit)
	assert.Equal(t, time.Minute, defaultRateInterval)
	assert.Equal(t, 5*time.Minute, defaultCleanupInterval)
	assert.Equal(t, "Retry-After", retryAfterHeader)
}

func TestLimitBody(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	limitBody(rec, req)
	// After limitBody, the body should be wrapped with MaxBytesReader
	assert.NotNil(t, req.Body)
}

func TestHandleCreateCluster_InvalidDNSName(t *testing.T) {
	s := newTestServer()

	cluster := newTestCluster("INVALID_NAME", "default")
	body, err := json.Marshal(cluster)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, apiPrefix+"/clusters", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.handleCreateCluster(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetCluster_InvalidName(t *testing.T) {
	s := newTestServer()

	result, err := s.getCluster(context.Background(), "INVALID_NAME!", "default")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestGetCluster_InvalidNamespace(t *testing.T) {
	s := newTestServer()

	result, err := s.getCluster(context.Background(), "valid-name", "INVALID!")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestHandleGetTableDetail_NotFound(t *testing.T) {
	s := newTestServer()

	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/storage/tables/public/users?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("schema", "public")
	req.SetPathValue("table", "users")
	rec := httptest.NewRecorder()
	s.handleGetTableDetail(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleUpdateDataLoadingJob_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPut,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs/j1?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("job", "j1")
	rec := httptest.NewRecorder()
	s.handleUpdateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleDeleteDataLoadingJob_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodDelete,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs/j1?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("job", "j1")
	rec := httptest.NewRecorder()
	s.handleDeleteDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleStartDataLoadingJob_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs/j1/start?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("job", "j1")
	rec := httptest.NewRecorder()
	s.handleStartDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleStopDataLoadingJob_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs/j1/stop?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("job", "j1")
	rec := httptest.NewRecorder()
	s.handleStopDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleCreateDataLoadingJob_NotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodPost,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	rec := httptest.NewRecorder()
	s.handleCreateDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleGetDataLoadingJob_ClusterNotFound(t *testing.T) {
	s := newTestServer()
	req := httptest.NewRequest(http.MethodGet,
		apiPrefix+"/clusters/nonexistent/data-loading/jobs/j1?namespace=default", nil)
	req.SetPathValue("name", "nonexistent")
	req.SetPathValue("job", "j1")
	rec := httptest.NewRecorder()
	s.handleGetDataLoadingJob(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRateLimiter_Stop(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute, slog.Default())

	// Stop should not panic and should stop the cleanup goroutine.
	rl.Stop()

	// Calling Stop again should panic (closing closed channel), so we don't do that.
}

func TestWithTrustedProxies_InvalidCIDR(t *testing.T) {
	// Invalid CIDRs should be ignored with a warning.
	rl := NewRateLimiter(10, time.Minute, slog.Default(),
		WithTrustedProxies([]string{"invalid-cidr", "192.168.1.0/24", "also-invalid"}))
	defer rl.Stop()

	// Only the valid CIDR should be added.
	assert.Len(t, rl.trustedProxies, 1)
}

func TestWithTrustedProxies_Empty(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute, slog.Default(),
		WithTrustedProxies([]string{}))
	defer rl.Stop()

	assert.Empty(t, rl.trustedProxies)
}

func TestIsTrustedProxy_InvalidIP(t *testing.T) {
	rl := NewRateLimiter(10, time.Minute, slog.Default(),
		WithTrustedProxies([]string{"192.168.1.0/24"}))
	defer rl.Stop()

	// Invalid IP should not be trusted.
	assert.False(t, rl.isTrustedProxy("not-an-ip"))
}

func TestIsValidDNS1123Name(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid simple name", "my-cluster", true},
		{"valid with numbers", "cluster-123", true},
		{"valid single char", "a", true},
		{"empty string", "", false},
		{"starts with hyphen", "-cluster", false},
		{"ends with hyphen", "cluster-", false},
		{"uppercase", "MyCluster", false},
		{"with dots", "my.cluster", false},
		{"with underscore", "my_cluster", false},
		{"valid all numbers", "123", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidDNS1123Name(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

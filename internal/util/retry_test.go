package util

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRetryOptions(t *testing.T) {
	opts := DefaultRetryOptions()
	assert.Equal(t, 5, opts.MaxRetries)
	assert.Equal(t, time.Second, opts.InitialBackoff)
	assert.Equal(t, 30*time.Second, opts.MaxBackoff)
	assert.InDelta(t, 2.0, opts.Multiplier, 0.001)
	assert.InDelta(t, 0.1, opts.JitterFraction, 0.001)
}

func TestRetryWithBackoff(t *testing.T) {
	tests := []struct {
		name        string
		opts        RetryOptions
		fn          func(attempts *int32) RetryableFunc
		expectErr   bool
		errContains string
		maxAttempts int32
	}{
		{
			name: "succeeds on first attempt",
			opts: RetryOptions{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0},
			fn: func(attempts *int32) RetryableFunc {
				return func(_ context.Context) error {
					atomic.AddInt32(attempts, 1)
					return nil
				}
			},
			expectErr:   false,
			maxAttempts: 1,
		},
		{
			name: "succeeds on second attempt",
			opts: RetryOptions{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0},
			fn: func(attempts *int32) RetryableFunc {
				return func(_ context.Context) error {
					count := atomic.AddInt32(attempts, 1)
					if count < 2 {
						return fmt.Errorf("temporary error")
					}
					return nil
				}
			},
			expectErr:   false,
			maxAttempts: 2,
		},
		{
			name: "exhausts all retries",
			opts: RetryOptions{MaxRetries: 2, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0},
			fn: func(attempts *int32) RetryableFunc {
				return func(_ context.Context) error {
					atomic.AddInt32(attempts, 1)
					return fmt.Errorf("persistent error")
				}
			},
			expectErr:   true,
			errContains: "retry attempts exhausted",
			maxAttempts: 3, // initial + 2 retries
		},
		{
			name: "zero retries means one attempt",
			opts: RetryOptions{MaxRetries: 0, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0},
			fn: func(attempts *int32) RetryableFunc {
				return func(_ context.Context) error {
					atomic.AddInt32(attempts, 1)
					return fmt.Errorf("error")
				}
			},
			expectErr:   true,
			maxAttempts: 1,
		},
		{
			name: "negative retries treated as zero",
			opts: RetryOptions{MaxRetries: -1, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0},
			fn: func(attempts *int32) RetryableFunc {
				return func(_ context.Context) error {
					atomic.AddInt32(attempts, 1)
					return fmt.Errorf("error")
				}
			},
			expectErr:   true,
			maxAttempts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var attempts int32
			ctx := context.Background()
			err := RetryWithBackoff(ctx, tt.opts, tt.fn(&attempts))

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.maxAttempts, atomic.LoadInt32(&attempts))
		})
	}
}

func TestRetryWithBackoff_ContextCancellation(t *testing.T) {
	t.Run("context canceled before first attempt", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		opts := RetryOptions{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0}
		err := RetryWithBackoff(ctx, opts, func(_ context.Context) error {
			return fmt.Errorf("should not be called")
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("context canceled during backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var attempts int32

		opts := RetryOptions{MaxRetries: 5, InitialBackoff: 500 * time.Millisecond, MaxBackoff: time.Second, Multiplier: 2.0}
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := RetryWithBackoff(ctx, opts, func(_ context.Context) error {
			atomic.AddInt32(&attempts, 1)
			return fmt.Errorf("error")
		})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
}

func TestRetryWithBackoff_DefaultOptions(t *testing.T) {
	// Test that invalid options get corrected
	opts := RetryOptions{
		MaxRetries:     1,
		InitialBackoff: 0,  // Should default to 1s
		MaxBackoff:     0,  // Should default to 30s
		Multiplier:     -1, // Should default to 2.0
	}

	var attempts int32
	ctx := context.Background()
	err := RetryWithBackoff(ctx, opts, func(_ context.Context) error {
		atomic.AddInt32(&attempts, 1)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, int32(1), atomic.LoadInt32(&attempts))
}

func TestRetryWithBackoff_ErrRetryExhausted(t *testing.T) {
	opts := RetryOptions{MaxRetries: 1, InitialBackoff: time.Millisecond, MaxBackoff: 10 * time.Millisecond, Multiplier: 2.0}
	innerErr := fmt.Errorf("inner error")

	err := RetryWithBackoff(context.Background(), opts, func(_ context.Context) error {
		return innerErr
	})

	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRetryExhausted))
}

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		name           string
		base           time.Duration
		maxBackoff     time.Duration
		jitterFraction float64
	}{
		{
			name:           "no jitter",
			base:           time.Second,
			maxBackoff:     30 * time.Second,
			jitterFraction: 0,
		},
		{
			name:           "with jitter",
			base:           time.Second,
			maxBackoff:     30 * time.Second,
			jitterFraction: 0.1,
		},
		{
			name:           "base exceeds max",
			base:           60 * time.Second,
			maxBackoff:     30 * time.Second,
			jitterFraction: 0,
		},
		{
			name:           "negative jitter treated as no jitter",
			base:           time.Second,
			maxBackoff:     30 * time.Second,
			jitterFraction: -0.1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateBackoff(tt.base, tt.maxBackoff, tt.jitterFraction)
			assert.Greater(t, result, time.Duration(0))

			if tt.jitterFraction <= 0 {
				expectedBase := tt.base
				if expectedBase > tt.maxBackoff {
					expectedBase = tt.maxBackoff
				}
				assert.Equal(t, expectedBase, result)
			} else {
				expectedBase := tt.base
				if expectedBase > tt.maxBackoff {
					expectedBase = tt.maxBackoff
				}
				maxWithJitter := expectedBase + time.Duration(float64(expectedBase)*tt.jitterFraction)
				assert.GreaterOrEqual(t, result, expectedBase)
				assert.LessOrEqual(t, result, maxWithJitter)
			}
		})
	}
}

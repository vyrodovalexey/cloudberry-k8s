package util

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// RetryOptions configures the retry behavior with exponential backoff.
type RetryOptions struct {
	// MaxRetries is the maximum number of retry attempts.
	MaxRetries int
	// InitialBackoff is the initial backoff duration.
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration.
	MaxBackoff time.Duration
	// Multiplier is the backoff multiplier.
	Multiplier float64
	// JitterFraction is the fraction of the backoff to add as jitter (0.0 to 1.0).
	JitterFraction float64
}

// DefaultRetryOptions returns sensible default retry options.
func DefaultRetryOptions() RetryOptions {
	return RetryOptions{
		MaxRetries:     5,
		InitialBackoff: time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		JitterFraction: 0.1,
	}
}

// RetryableFunc is a function that can be retried. It returns an error if the operation failed.
type RetryableFunc func(ctx context.Context) error

// RetryWithBackoff executes fn with exponential backoff retry logic.
// It respects context cancellation and returns ErrRetryExhausted if all attempts fail.
func RetryWithBackoff(ctx context.Context, opts RetryOptions, fn RetryableFunc) error {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.Multiplier <= 0 {
		opts.Multiplier = 2.0
	}
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = time.Second
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = 30 * time.Second
	}

	var lastErr error
	backoff := opts.InitialBackoff

	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("context canceled after %d attempts: %w", attempt, lastErr)
			}
			return fmt.Errorf("context canceled: %w", err)
		}

		lastErr = fn(ctx)
		if lastErr == nil {
			return nil
		}

		if attempt < opts.MaxRetries {
			sleepDuration := calculateBackoff(backoff, opts.MaxBackoff, opts.JitterFraction)
			select {
			case <-ctx.Done():
				return fmt.Errorf(
					"context canceled during backoff after %d attempts: %w",
					attempt+1, lastErr,
				)
			case <-time.After(sleepDuration):
				// Continue to next attempt.
			}
			backoff = time.Duration(
				math.Min(
					float64(backoff)*opts.Multiplier,
					float64(opts.MaxBackoff),
				),
			)
		}
	}

	return fmt.Errorf(
		"%w: after %d attempts: %w", ErrRetryExhausted, opts.MaxRetries+1, lastErr,
	)
}

// calculateBackoff computes the backoff duration with optional jitter.
func calculateBackoff(base, maxBackoff time.Duration, jitterFraction float64) time.Duration {
	if base > maxBackoff {
		base = maxBackoff
	}
	if jitterFraction <= 0 {
		return base
	}
	jitterRand := rand.Float64() //nolint:gosec // jitter does not need crypto rand
	jitter := time.Duration(float64(base) * jitterFraction * jitterRand)
	return base + jitter
}

package middleware

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/xraph/grove/hook"
)

// RetryHook provides automatic retry with configurable backoff for transient failures.
type RetryHook struct {
	maxAttempts int
	initialWait time.Duration
	maxWait     time.Duration
	jitter      bool
}

var _ hook.PostQueryHook = (*RetryHook)(nil)

// NewRetry creates a retry middleware with the given maximum attempts.
func NewRetry(maxAttempts int, opts ...RetryOption) *RetryHook {
	h := &RetryHook{
		maxAttempts: maxAttempts,
		initialWait: 100 * time.Millisecond,
		maxWait:     5 * time.Second,
		jitter:      true,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// RetryOption configures the retry middleware.
type RetryOption func(*RetryHook)

// WithInitialWait sets the initial wait duration before the first retry.
func WithInitialWait(d time.Duration) RetryOption {
	return func(h *RetryHook) { h.initialWait = d }
}

// WithMaxWait sets the maximum wait duration between retries.
func WithMaxWait(d time.Duration) RetryOption {
	return func(h *RetryHook) { h.maxWait = d }
}

// WithJitter enables or disables random jitter on retry delays.
func WithJitter(enabled bool) RetryOption {
	return func(h *RetryHook) { h.jitter = enabled }
}

func (h *RetryHook) AfterQuery(_ context.Context, qc *hook.QueryContext, result any) error {
	// Store retry config in the context for the store to use.
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	qc.Values["_retry_max"] = h.maxAttempts
	return nil
}

// BackoffDuration calculates the backoff duration for the given attempt (0-indexed).
func (h *RetryHook) BackoffDuration(attempt int) time.Duration {
	if h.initialWait <= 0 || h.maxWait <= 0 {
		return 0
	}
	d := min(h.initialWait, h.maxWait)
	for i := 0; i < attempt && d < h.maxWait; i++ {
		if d > h.maxWait/2 {
			d = h.maxWait
			break
		}
		d *= 2
	}
	if h.jitter && d > 1 {
		// Integer jitter keeps even the largest time.Duration positive.
		half := d / 2
		d = d - half + time.Duration(rand.Int64N(int64(half))) //nolint:gosec // retry jitter does not need crypto-grade randomness
	}
	return d
}

// MaxAttempts returns the configured maximum number of retry attempts.
func (h *RetryHook) MaxAttempts() int {
	return h.maxAttempts
}

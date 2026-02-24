package middleware_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xraph/grove/kv/middleware"
)

func TestRetryHook_MaxAttempts(t *testing.T) {
	h := middleware.NewRetry(3)

	assert.Equal(t, 3, h.MaxAttempts())
}

func TestRetryHook_BackoffDuration_Exponential(t *testing.T) {
	h := middleware.NewRetry(5,
		middleware.WithInitialWait(100*time.Millisecond),
		middleware.WithMaxWait(10*time.Second),
		middleware.WithJitter(false),
	)

	d0 := h.BackoffDuration(0)
	d1 := h.BackoffDuration(1)
	d2 := h.BackoffDuration(2)

	assert.Equal(t, 100*time.Millisecond, d0, "attempt 0: initialWait * 2^0")
	assert.Equal(t, 200*time.Millisecond, d1, "attempt 1: initialWait * 2^1")
	assert.Equal(t, 400*time.Millisecond, d2, "attempt 2: initialWait * 2^2")

	// Verify monotonic growth.
	assert.True(t, d1 > d0)
	assert.True(t, d2 > d1)
}

func TestRetryHook_BackoffDuration_CappedByMaxWait(t *testing.T) {
	maxWait := 500 * time.Millisecond
	h := middleware.NewRetry(10,
		middleware.WithInitialWait(100*time.Millisecond),
		middleware.WithMaxWait(maxWait),
		middleware.WithJitter(false),
	)

	// Attempt 10 would be 100ms * 2^10 = 102400ms without cap.
	d := h.BackoffDuration(10)
	assert.Equal(t, maxWait, d, "backoff should be capped at maxWait")
}

func TestRetryHook_WithInitialWait(t *testing.T) {
	h := middleware.NewRetry(3,
		middleware.WithInitialWait(250*time.Millisecond),
		middleware.WithJitter(false),
	)

	d0 := h.BackoffDuration(0)
	assert.Equal(t, 250*time.Millisecond, d0)
}

func TestRetryHook_WithMaxWait(t *testing.T) {
	h := middleware.NewRetry(3,
		middleware.WithInitialWait(1*time.Second),
		middleware.WithMaxWait(2*time.Second),
		middleware.WithJitter(false),
	)

	// Attempt 5: 1s * 2^5 = 32s, capped to 2s.
	d := h.BackoffDuration(5)
	assert.Equal(t, 2*time.Second, d)
}

func TestRetryHook_WithJitter(t *testing.T) {
	h := middleware.NewRetry(3,
		middleware.WithInitialWait(100*time.Millisecond),
		middleware.WithMaxWait(10*time.Second),
		middleware.WithJitter(true),
	)

	// With jitter, the duration is: d * (0.5 + rand*0.5), so range is [d*0.5, d*1.0).
	// Run multiple times and verify the values are not all identical.
	seen := make(map[time.Duration]bool)
	for i := 0; i < 20; i++ {
		d := h.BackoffDuration(0)
		seen[d] = true

		// Jitter formula: d * (0.5 + rand*0.5), where d = 100ms.
		// Range: [50ms, 100ms).
		assert.GreaterOrEqual(t, d, 50*time.Millisecond, "jittered duration should be >= 50ms")
		assert.Less(t, d, 100*time.Millisecond, "jittered duration should be < 100ms")
	}

	assert.Greater(t, len(seen), 1, "with jitter enabled, backoff values should vary across calls")
}

package middleware_test

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/middleware"
)

func TestRetryBackoffCannotOverflow(t *testing.T) {
	for _, jitter := range []bool{false, true} {
		h := middleware.NewRetry(100, middleware.WithJitter(jitter))
		for _, attempt := range []int{37, 63, 64, 1024, math.MaxInt} {
			wait := h.BackoffDuration(attempt)
			require.GreaterOrEqual(t, wait, 2500*time.Millisecond)
			require.LessOrEqual(t, wait, 5*time.Second)
			if !jitter {
				require.Equal(t, 5*time.Second, wait)
			}
		}
	}
}

func TestRetryBackoffHandlesDurationLimits(t *testing.T) {
	for _, jitter := range []bool{false, true} {
		h := middleware.NewRetry(100, middleware.WithInitialWait(time.Duration(math.MaxInt64/2+1)), middleware.WithMaxWait(time.Duration(math.MaxInt64)), middleware.WithJitter(jitter))
		wait := h.BackoffDuration(2)
		require.GreaterOrEqual(t, wait, time.Duration(math.MaxInt64/2))
		if !jitter {
			require.Equal(t, time.Duration(math.MaxInt64), wait)
		}
	}
}

func TestRetryBackoffHandlesSmallAndDisabledWaits(t *testing.T) {
	for _, wait := range []time.Duration{-1, 0, 1, 2, 3} {
		h := middleware.NewRetry(10, middleware.WithInitialWait(wait), middleware.WithMaxWait(wait))
		for _, attempt := range []int{0, 100} {
			got := h.BackoffDuration(attempt)
			if wait <= 0 {
				require.Zero(t, got)
			} else {
				require.Positive(t, got)
				require.LessOrEqual(t, got, wait)
			}
		}
	}
}

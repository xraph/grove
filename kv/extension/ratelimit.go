package extension

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove/kv"
)

// RateLimiter provides distributed rate limiting backed by a KV store.
type RateLimiter struct {
	store  *kv.Store
	prefix string
	rate   int
	window time.Duration
}

// RateLimitResult contains the result of a rate limit check.
type RateLimitResult struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
}

// NewRateLimiter creates a new rate limiter that allows `rate` requests per `window`.
func NewRateLimiter(store *kv.Store, prefix string, rate int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		store:  store,
		prefix: prefix,
		rate:   rate,
		window: window,
	}
}

// Allow checks if the request identified by key is within the rate limit.
// It uses a sliding window counter approach.
func (rl *RateLimiter) Allow(ctx context.Context, key string) (*RateLimitResult, error) {
	fullKey := fmt.Sprintf("%s:%s", rl.prefix, key)

	// Get current count.
	var count int
	err := rl.store.Get(ctx, fullKey, &count)
	if err != nil && err != kv.ErrNotFound {
		return nil, fmt.Errorf("ratelimit: get: %w", err)
	}

	result := &RateLimitResult{
		Remaining: rl.rate - count - 1,
		ResetAt:   time.Now().Add(rl.window),
	}

	if count >= rl.rate {
		result.Allowed = false
		result.Remaining = 0
		return result, nil
	}

	// Increment counter.
	count++
	if err := rl.store.Set(ctx, fullKey, count, kv.WithTTL(rl.window)); err != nil {
		return nil, fmt.Errorf("ratelimit: set: %w", err)
	}

	result.Allowed = true
	if result.Remaining < 0 {
		result.Remaining = 0
	}
	return result, nil
}

// Reset resets the rate limit for the given key.
func (rl *RateLimiter) Reset(ctx context.Context, key string) error {
	fullKey := fmt.Sprintf("%s:%s", rl.prefix, key)
	return rl.store.Delete(ctx, fullKey)
}

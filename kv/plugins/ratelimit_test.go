package plugins_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/kvtest"
	"github.com/xraph/grove/kv/plugins"
)

func TestRateLimiter_Allow_WithinLimit(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 5, time.Minute)

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		result, err := rl.Allow(ctx, "user:1")
		require.NoError(t, err)
		assert.True(t, result.Allowed, "request %d should be allowed", i+1)
	}
}

func TestRateLimiter_Allow_ExceedsLimit(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 3, time.Minute)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		result, err := rl.Allow(ctx, "user:1")
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	}

	// Fourth request exceeds the limit.
	result, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, result.Allowed, "4th request should be denied")
}

func TestRateLimiter_Allow_Remaining(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 5, time.Minute)

	ctx := context.Background()
	// Use 2 of the 5 allowed requests.
	for i := 0; i < 2; i++ {
		_, err := rl.Allow(ctx, "user:1")
		require.NoError(t, err)
	}

	result, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	// After 3 requests consumed (2 prior + this one), remaining should be 2.
	assert.Equal(t, 2, result.Remaining)
}

func TestRateLimiter_Reset(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 2, time.Minute)

	ctx := context.Background()
	// Exhaust the limit.
	for i := 0; i < 2; i++ {
		_, err := rl.Allow(ctx, "user:1")
		require.NoError(t, err)
	}
	result, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, result.Allowed)

	// Reset the counter.
	err = rl.Reset(ctx, "user:1")
	require.NoError(t, err)

	// Should be allowed again after reset.
	result, err = rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, result.Allowed)
}

func TestRateLimiter_DifferentKeys(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 1, time.Minute)

	ctx := context.Background()
	result1, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, result1.Allowed)

	// user:1 is now exhausted.
	result1b, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, result1b.Allowed)

	// user:2 has a separate counter and should still be allowed.
	result2, err := rl.Allow(ctx, "user:2")
	require.NoError(t, err)
	assert.True(t, result2.Allowed)
}

func TestRateLimiter_WindowExpiry(t *testing.T) {
	store := kvtest.SetupStore(t)
	rl := plugins.NewRateLimiter(store, "api", 1, 50*time.Millisecond)

	ctx := context.Background()
	result, err := rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, result.Allowed)

	// Should be denied immediately.
	result, err = rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.False(t, result.Allowed)

	// Wait for the window TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// Should be allowed again after expiry.
	result, err = rl.Allow(ctx, "user:1")
	require.NoError(t, err)
	assert.True(t, result.Allowed)
}

package extension_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func TestLock_Acquire_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "test-resource", 5*time.Second)

	err := lock.Acquire(context.Background())
	require.NoError(t, err)
}

func TestLock_Acquire_AlreadyHeld(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "test-resource", 5*time.Second)

	ctx := context.Background()
	err := lock.Acquire(ctx)
	require.NoError(t, err)

	// Second acquire on the same key should fail with a wrapped ErrConflict.
	lock2 := extension.NewLock(store, "test-resource", 5*time.Second)
	err = lock2.Acquire(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, kv.ErrConflict), "expected error to wrap kv.ErrConflict, got: %v", err)
}

func TestLock_Release_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "test-resource", 5*time.Second)

	ctx := context.Background()
	err := lock.Acquire(ctx)
	require.NoError(t, err)

	err = lock.Release(ctx)
	require.NoError(t, err)
}

func TestLock_Release_NotAcquired(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "test-resource", 5*time.Second)

	err := lock.Release(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not acquired")
}

func TestLock_AcquireWithRetry_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// First lock holds the resource.
	holder := extension.NewLock(store, "retry-resource", 5*time.Second)
	err := holder.Acquire(ctx)
	require.NoError(t, err)

	// Release after 50ms in a separate goroutine.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = holder.Release(context.Background())
	}()

	// Second lock retries until the first lock is released.
	contender := extension.NewLock(store, "retry-resource", 5*time.Second)
	err = contender.AcquireWithRetry(ctx, 20*time.Millisecond, 10)
	require.NoError(t, err)
}

func TestLock_AcquireWithRetry_MaxRetries(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Hold the lock forever (long TTL, never released).
	holder := extension.NewLock(store, "held-forever", 1*time.Minute)
	err := holder.Acquire(ctx)
	require.NoError(t, err)

	contender := extension.NewLock(store, "held-forever", 5*time.Second)
	err = contender.AcquireWithRetry(ctx, 10*time.Millisecond, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries exceeded")
}

func TestLock_AcquireWithRetry_ContextCancel(t *testing.T) {
	store := kvtest.SetupStore(t)

	holder := extension.NewLock(store, "cancel-resource", 1*time.Minute)
	err := holder.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after 30ms while retries are in progress.
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	contender := extension.NewLock(store, "cancel-resource", 5*time.Second)
	err = contender.AcquireWithRetry(ctx, 20*time.Millisecond, 100)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestLock_Extend_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "extend-resource", 5*time.Second)

	ctx := context.Background()
	err := lock.Acquire(ctx)
	require.NoError(t, err)

	err = lock.Extend(ctx, 10*time.Second)
	require.NoError(t, err)
}

func TestLock_Extend_NotAcquired(t *testing.T) {
	store := kvtest.SetupStore(t)
	lock := extension.NewLock(store, "extend-resource", 5*time.Second)

	err := lock.Extend(context.Background(), 10*time.Second)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not acquired")
}

func TestLock_TTL_Expiry(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	lock1 := extension.NewLock(store, "expiry-resource", 50*time.Millisecond)
	err := lock1.Acquire(ctx)
	require.NoError(t, err)

	// Wait for the TTL to expire.
	time.Sleep(100 * time.Millisecond)

	// A new lock on the same key should succeed because the first expired.
	lock2 := extension.NewLock(store, "expiry-resource", 5*time.Second)
	err = lock2.Acquire(ctx)
	require.NoError(t, err)
}

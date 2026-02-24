package kvtest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// RunConformanceSuite runs the full conformance test suite against a KV driver.
// Every driver must pass all tests in this suite.
func RunConformanceSuite(t *testing.T, drv driver.Driver) {
	t.Helper()

	store, err := kv.Open(drv)
	require.NoError(t, err)
	defer store.Close()

	t.Run("Ping", func(t *testing.T) {
		testPing(t, store)
	})
	t.Run("GetSet", func(t *testing.T) {
		testGetSet(t, store)
	})
	t.Run("GetNotFound", func(t *testing.T) {
		testGetNotFound(t, store)
	})
	t.Run("Delete", func(t *testing.T) {
		testDelete(t, store)
	})
	t.Run("Exists", func(t *testing.T) {
		testExists(t, store)
	})
	t.Run("SetWithTTL", func(t *testing.T) {
		testSetWithTTL(t, store)
	})
	t.Run("SetNX", func(t *testing.T) {
		testSetNX(t, store, drv)
	})
	t.Run("SetXX", func(t *testing.T) {
		testSetXX(t, store, drv)
	})

	if _, ok := drv.(driver.BatchDriver); ok {
		t.Run("MGetMSet", func(t *testing.T) {
			testMGetMSet(t, store)
		})
	}
	if _, ok := drv.(driver.TTLDriver); ok {
		t.Run("TTLExpire", func(t *testing.T) {
			testTTLExpire(t, store)
		})
	}
	if _, ok := drv.(driver.ScanDriver); ok {
		t.Run("Scan", func(t *testing.T) {
			testScan(t, store)
		})
	}
}

func testPing(t *testing.T, store *kv.Store) {
	err := store.Ping(context.Background())
	assert.NoError(t, err)
}

func testGetSet(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	type user struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	err := store.Set(ctx, "test:user:1", user{Name: "Alice", Age: 30})
	require.NoError(t, err)

	var got user
	err = store.Get(ctx, "test:user:1", &got)
	require.NoError(t, err)
	assert.Equal(t, "Alice", got.Name)
	assert.Equal(t, 30, got.Age)

	// Cleanup.
	_ = store.Delete(ctx, "test:user:1")
}

func testGetNotFound(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	var got string
	err := store.Get(ctx, "test:nonexistent:key", &got)
	assert.Error(t, err)
}

func testDelete(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	_ = store.Set(ctx, "test:del:1", "value1")
	_ = store.Set(ctx, "test:del:2", "value2")

	err := store.Delete(ctx, "test:del:1", "test:del:2")
	require.NoError(t, err)

	count, err := store.Exists(ctx, "test:del:1", "test:del:2")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func testExists(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	_ = store.Set(ctx, "test:exists:1", "val")
	defer store.Delete(ctx, "test:exists:1")

	count, err := store.Exists(ctx, "test:exists:1", "test:exists:missing")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func testSetWithTTL(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	err := store.Set(ctx, "test:ttl:1", "short-lived", kv.WithTTL(50*time.Millisecond))
	require.NoError(t, err)

	// Should exist immediately.
	count, err := store.Exists(ctx, "test:ttl:1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	var got string
	err = store.Get(ctx, "test:ttl:1", &got)
	assert.Error(t, err)
}

func testSetNX(t *testing.T, store *kv.Store, drv driver.Driver) {
	if _, ok := drv.(driver.CASDriver); !ok {
		t.Skip("driver does not support CAS")
	}

	ctx := context.Background()

	// First set should succeed.
	err := store.Set(ctx, "test:nx:1", "first", kv.WithNX())
	require.NoError(t, err)
	defer store.Delete(ctx, "test:nx:1")

	// Second set should fail with ErrConflict.
	err = store.Set(ctx, "test:nx:1", "second", kv.WithNX())
	assert.ErrorIs(t, err, kv.ErrConflict)
}

func testSetXX(t *testing.T, store *kv.Store, drv driver.Driver) {
	if _, ok := drv.(driver.CASDriver); !ok {
		t.Skip("driver does not support CAS")
	}

	ctx := context.Background()

	// Should fail since key doesn't exist.
	err := store.Set(ctx, "test:xx:1", "value", kv.WithXX())
	assert.ErrorIs(t, err, kv.ErrNotFound)

	// Create the key first.
	_ = store.Set(ctx, "test:xx:1", "original")
	defer store.Delete(ctx, "test:xx:1")

	// Now XX should succeed.
	err = store.Set(ctx, "test:xx:1", "updated", kv.WithXX())
	require.NoError(t, err)
}

func testMGetMSet(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	pairs := map[string]any{
		"test:mset:1": "val1",
		"test:mset:2": "val2",
		"test:mset:3": "val3",
	}
	err := store.MSet(ctx, pairs)
	require.NoError(t, err)

	defer func() {
		store.Delete(ctx, "test:mset:1", "test:mset:2", "test:mset:3")
	}()

	dest := make(map[string]any)
	err = store.MGet(ctx, []string{"test:mset:1", "test:mset:2", "test:mset:missing"}, dest)
	require.NoError(t, err)
	assert.Len(t, dest, 2)
}

func testTTLExpire(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	_ = store.Set(ctx, "test:expire:1", "val", kv.WithTTL(10*time.Second))
	defer store.Delete(ctx, "test:expire:1")

	ttl, err := store.TTL(ctx, "test:expire:1")
	require.NoError(t, err)
	assert.True(t, ttl > 0)

	err = store.Expire(ctx, "test:expire:1", 5*time.Second)
	require.NoError(t, err)

	ttl2, err := store.TTL(ctx, "test:expire:1")
	require.NoError(t, err)
	assert.True(t, ttl2 > 0 && ttl2 <= 5*time.Second)
}

func testScan(t *testing.T, store *kv.Store) {
	ctx := context.Background()

	_ = store.Set(ctx, "test:scan:a", "1")
	_ = store.Set(ctx, "test:scan:b", "2")
	_ = store.Set(ctx, "test:scan:c", "3")
	defer func() {
		store.Delete(ctx, "test:scan:a", "test:scan:b", "test:scan:c")
	}()

	var keys []string
	err := store.Scan(ctx, "test:scan:*", func(key string) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(keys), 3)
}

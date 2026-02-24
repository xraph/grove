package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/middleware"
)

func TestCacheHook_PutAndSize(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)

	cache.Put("key1", []byte("val1"), 0)
	cache.Put("key2", []byte("val2"), 0)
	cache.Put("key3", []byte("val3"), 0)

	assert.Equal(t, 3, cache.Size())
}

func TestCacheHook_Eviction(t *testing.T) {
	cache := middleware.NewCache(2, 5*time.Second)

	cache.Put("key1", []byte("val1"), 0)
	cache.Put("key2", []byte("val2"), 0)
	cache.Put("key3", []byte("val3"), 0)

	assert.Equal(t, 2, cache.Size())
}

func TestCacheHook_Expiry(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)

	cache.Put("key1", []byte("val1"), 50*time.Millisecond)
	assert.Equal(t, 1, cache.Size())

	time.Sleep(100 * time.Millisecond)

	// The entry is still in the map but expired. BeforeQuery should not report a hit.
	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key1",
		Values:    make(map[string]any),
	}

	result, err := cache.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasHit := qc.Values["_cache_hit"]
	assert.False(t, hasHit, "expired entry should not produce a cache hit")
}

func TestCacheHook_Evict(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)

	cache.Put("key1", []byte("val1"), 0)
	assert.Equal(t, 1, cache.Size())

	cache.Evict("key1")
	assert.Equal(t, 0, cache.Size())
}

func TestCacheHook_EvictMultiple(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)

	cache.Put("key1", []byte("val1"), 0)
	cache.Put("key2", []byte("val2"), 0)
	cache.Put("key3", []byte("val3"), 0)
	assert.Equal(t, 3, cache.Size())

	cache.Evict("key1", "key2")
	assert.Equal(t, 1, cache.Size())
}

func TestCacheHook_Flush(t *testing.T) {
	cache := middleware.NewCache(100, 5*time.Second)

	for i := 0; i < 5; i++ {
		cache.Put("key"+string(rune('0'+i)), []byte("val"), 0)
	}
	assert.Equal(t, 5, cache.Size())

	cache.Flush()
	assert.Equal(t, 0, cache.Size())
}

func TestCacheHook_BeforeQuery_CacheHit(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)
	cache.Put("mykey", []byte("hello"), 0)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "mykey",
		Values:    make(map[string]any),
	}

	result, err := cache.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	hit, ok := qc.Values["_cache_hit"].(bool)
	require.True(t, ok, "_cache_hit should be set in Values")
	assert.True(t, hit)

	val, ok := qc.Values["_cache_value"].([]byte)
	require.True(t, ok, "_cache_value should be set in Values")
	assert.Equal(t, []byte("hello"), val)
}

func TestCacheHook_BeforeQuery_CacheMiss(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "nonexistent",
		Values:    make(map[string]any),
	}

	result, err := cache.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasHit := qc.Values["_cache_hit"]
	assert.False(t, hasHit, "cache miss should not set _cache_hit")
}

func TestCacheHook_BeforeQuery_NonGet(t *testing.T) {
	cache := middleware.NewCache(10, 5*time.Second)
	cache.Put("mykey", []byte("hello"), 0)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpSet,
		RawQuery:  "mykey",
		Values:    make(map[string]any),
	}

	result, err := cache.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	_, hasHit := qc.Values["_cache_hit"]
	assert.False(t, hasHit, "non-GET operations should not produce cache hits")
}

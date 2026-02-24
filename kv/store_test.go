package kv_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/codec"
	"github.com/xraph/grove/kv/kvtest"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// denyHook is a PreQueryHook that always denies the operation.
type denyHook struct{}

func (denyHook) BeforeQuery(_ context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	return &hook.HookResult{Decision: hook.Deny, Error: kv.ErrHookDenied}, kv.ErrHookDenied
}

// recordingHook records every pre-query invocation via an atomic counter.
type recordingHook struct {
	calls atomic.Int64
}

func (h *recordingHook) BeforeQuery(_ context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	h.calls.Add(1)
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// testUser is a simple struct used in codec round-trip tests.
type testUser struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

// ---------------------------------------------------------------------------
// Open
// ---------------------------------------------------------------------------

func TestStore_Open_NilDriver(t *testing.T) {
	_, err := kv.Open(nil)
	require.Error(t, err)
}

func TestStore_Open_DefaultCodec(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.Equal(t, "json", store.Codec().Name())
}

func TestStore_Open_WithCodec(t *testing.T) {
	store := kvtest.SetupStore(t, kv.WithCodec(codec.MsgPack()))
	assert.Equal(t, "msgpack", store.Codec().Name())
}

func TestStore_Open_WithHook(t *testing.T) {
	rec := &recordingHook{}
	store := kvtest.SetupStore(t, kv.WithHook(rec))

	ctx := context.Background()
	// Trigger any operation so the hook fires.
	_ = store.Get(ctx, "nonexistent", new(string))

	assert.GreaterOrEqual(t, rec.calls.Load(), int64(1))
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

func TestStore_Get_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "user:1", testUser{Name: "Alice", Age: 30}))

	var got testUser
	require.NoError(t, store.Get(ctx, "user:1", &got))
	assert.Equal(t, "Alice", got.Name)
	assert.Equal(t, 30, got.Age)
}

func TestStore_Get_EmptyKey(t *testing.T) {
	store := kvtest.SetupStore(t)
	err := store.Get(context.Background(), "", new(string))
	assert.ErrorIs(t, err, kv.ErrKeyEmpty)
}

func TestStore_Get_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	var dest string
	err := store.Get(context.Background(), "does-not-exist", &dest)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, kv.ErrNotFound))
}

func TestStore_Get_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Get(context.Background(), "k", new(string))
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

func TestStore_Get_HookDenies(t *testing.T) {
	store := kvtest.SetupStore(t, kv.WithHook(denyHook{}))
	err := store.Get(context.Background(), "k", new(string))
	assert.ErrorIs(t, err, kv.ErrHookDenied)
}

func TestStore_Get_CodecDecodeError(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Store invalid JSON bytes directly.
	require.NoError(t, store.SetRaw(ctx, "bad-json", []byte("{not-valid-json")))

	var dest testUser
	err := store.Get(ctx, "bad-json", &dest)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, kv.ErrCodecDecode))
}

// ---------------------------------------------------------------------------
// GetRaw
// ---------------------------------------------------------------------------

func TestStore_GetRaw_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	data := []byte("raw-bytes-123")
	require.NoError(t, store.SetRaw(ctx, "raw:1", data))

	got, err := store.GetRaw(ctx, "raw:1")
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestStore_GetRaw_EmptyKey(t *testing.T) {
	store := kvtest.SetupStore(t)
	_, err := store.GetRaw(context.Background(), "")
	assert.ErrorIs(t, err, kv.ErrKeyEmpty)
}

func TestStore_GetRaw_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	_, err := store.GetRaw(context.Background(), "k")
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

func TestStore_Set_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "k", "hello"))

	var got string
	require.NoError(t, store.Get(ctx, "k", &got))
	assert.Equal(t, "hello", got)
}

func TestStore_Set_EmptyKey(t *testing.T) {
	store := kvtest.SetupStore(t)
	err := store.Set(context.Background(), "", "val")
	assert.ErrorIs(t, err, kv.ErrKeyEmpty)
}

func TestStore_Set_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Set(context.Background(), "k", "v")
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

func TestStore_Set_WithTTL(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "ttl-key", "short-lived", kv.WithTTL(50*time.Millisecond)))

	// Key should exist immediately.
	count, err := store.Exists(ctx, "ttl-key")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	var got string
	err = store.Get(ctx, "ttl-key", &got)
	assert.Error(t, err)
}

func TestStore_Set_HookDenies(t *testing.T) {
	store := kvtest.SetupStore(t, kv.WithHook(denyHook{}))
	err := store.Set(context.Background(), "k", "v")
	assert.ErrorIs(t, err, kv.ErrHookDenied)
}

func TestStore_Set_WithNX_FirstSucceeds(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	err := store.Set(ctx, "nx-key", "first", kv.WithNX())
	require.NoError(t, err)
}

func TestStore_Set_WithNX_SecondConflicts(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "nx-key", "first", kv.WithNX()))

	err := store.Set(ctx, "nx-key", "second", kv.WithNX())
	assert.ErrorIs(t, err, kv.ErrConflict)
}

func TestStore_Set_WithXX_NoKeyFails(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	err := store.Set(ctx, "xx-missing", "value", kv.WithXX())
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestStore_Set_WithXX_ExistingSucceeds(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "xx-key", "original"))

	err := store.Set(ctx, "xx-key", "updated", kv.WithXX())
	require.NoError(t, err)

	var got string
	require.NoError(t, store.Get(ctx, "xx-key", &got))
	assert.Equal(t, "updated", got)
}

// ---------------------------------------------------------------------------
// SetRaw
// ---------------------------------------------------------------------------

func TestStore_SetRaw_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	data := []byte("hello-bytes")
	require.NoError(t, store.SetRaw(ctx, "raw:set", data))

	got, err := store.GetRaw(ctx, "raw:set")
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestStore_SetRaw_WithNX(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.SetRaw(ctx, "raw:nx", []byte("first"), kv.WithNX()))

	err := store.SetRaw(ctx, "raw:nx", []byte("second"), kv.WithNX())
	assert.ErrorIs(t, err, kv.ErrConflict)
}

func TestStore_SetRaw_WithXX(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// XX on missing key fails.
	err := store.SetRaw(ctx, "raw:xx", []byte("v"), kv.WithXX())
	assert.ErrorIs(t, err, kv.ErrNotFound)

	// Create the key, then XX succeeds.
	require.NoError(t, store.SetRaw(ctx, "raw:xx", []byte("original")))

	err = store.SetRaw(ctx, "raw:xx", []byte("updated"), kv.WithXX())
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestStore_Delete_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "del:1", "v"))
	require.NoError(t, store.Delete(ctx, "del:1"))

	count, err := store.Exists(ctx, "del:1")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStore_Delete_EmptyKeys(t *testing.T) {
	store := kvtest.SetupStore(t)
	err := store.Delete(context.Background())
	assert.NoError(t, err)
}

func TestStore_Delete_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Delete(context.Background(), "k")
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

func TestStore_Delete_HookDenies(t *testing.T) {
	store := kvtest.SetupStore(t, kv.WithHook(denyHook{}))
	err := store.Delete(context.Background(), "k")
	assert.ErrorIs(t, err, kv.ErrHookDenied)
}

// ---------------------------------------------------------------------------
// Exists
// ---------------------------------------------------------------------------

func TestStore_Exists_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "ex:1", "a"))
	require.NoError(t, store.Set(ctx, "ex:2", "b"))
	require.NoError(t, store.Set(ctx, "ex:3", "c"))

	count, err := store.Exists(ctx, "ex:1", "ex:3", "ex:missing")
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestStore_Exists_EmptyKeys(t *testing.T) {
	store := kvtest.SetupStore(t)
	count, err := store.Exists(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStore_Exists_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	_, err := store.Exists(context.Background(), "k")
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// MGet
// ---------------------------------------------------------------------------

func TestStore_MGet_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	pairs := map[string]any{
		"mg:1": "val1",
		"mg:2": "val2",
		"mg:3": "val3",
	}
	require.NoError(t, store.MSet(ctx, pairs))

	dest := make(map[string]any)
	err := store.MGet(ctx, []string{"mg:1", "mg:2", "mg:missing"}, dest)
	require.NoError(t, err)
	assert.Len(t, dest, 2)
	assert.Contains(t, dest, "mg:1")
	assert.Contains(t, dest, "mg:2")
}

func TestStore_MGet_EmptyKeys(t *testing.T) {
	store := kvtest.SetupStore(t)
	err := store.MGet(context.Background(), []string{}, make(map[string]any))
	assert.NoError(t, err)
}

func TestStore_MGet_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.MGet(context.Background(), []string{"k"}, make(map[string]any))
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// MSet
// ---------------------------------------------------------------------------

func TestStore_MSet_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	pairs := map[string]any{
		"ms:a": "alpha",
		"ms:b": "bravo",
	}
	require.NoError(t, store.MSet(ctx, pairs))

	var got string
	require.NoError(t, store.Get(ctx, "ms:a", &got))
	assert.Equal(t, "alpha", got)

	require.NoError(t, store.Get(ctx, "ms:b", &got))
	assert.Equal(t, "bravo", got)
}

func TestStore_MSet_EmptyPairs(t *testing.T) {
	store := kvtest.SetupStore(t)
	err := store.MSet(context.Background(), map[string]any{})
	assert.NoError(t, err)
}

func TestStore_MSet_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.MSet(context.Background(), map[string]any{"k": "v"})
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// TTL
// ---------------------------------------------------------------------------

func TestStore_TTL_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "ttl:k", "v", kv.WithTTL(10*time.Second)))

	ttl, err := store.TTL(ctx, "ttl:k")
	require.NoError(t, err)
	assert.True(t, ttl > 0, "expected positive TTL, got %v", ttl)
}

func TestStore_TTL_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	_, err := store.TTL(context.Background(), "k")
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Expire
// ---------------------------------------------------------------------------

func TestStore_Expire_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "exp:k", "v", kv.WithTTL(10*time.Second)))
	require.NoError(t, store.Expire(ctx, "exp:k", 5*time.Second))

	ttl, err := store.TTL(ctx, "exp:k")
	require.NoError(t, err)
	assert.True(t, ttl > 0 && ttl <= 5*time.Second, "expected TTL <= 5s, got %v", ttl)
}

func TestStore_Expire_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Expire(context.Background(), "k", time.Second)
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Scan
// ---------------------------------------------------------------------------

func TestStore_Scan_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "scan:a", "1"))
	require.NoError(t, store.Set(ctx, "scan:b", "2"))
	require.NoError(t, store.Set(ctx, "scan:c", "3"))

	var keys []string
	err := store.Scan(ctx, "scan:*", func(key string) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, keys, 3)
}

func TestStore_Scan_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Scan(context.Background(), "*", func(_ string) error { return nil })
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Ping
// ---------------------------------------------------------------------------

func TestStore_Ping_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.NoError(t, store.Ping(context.Background()))
}

func TestStore_Ping_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Ping(context.Background())
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func TestStore_Close_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.NoError(t, store.Close())
}

func TestStore_Close_Double(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	err := store.Close()
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

func TestStore_Driver(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.NotNil(t, store.Driver())
}

func TestStore_Hooks(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.NotNil(t, store.Hooks())
}

func TestStore_Codec(t *testing.T) {
	store := kvtest.SetupStore(t)
	assert.Equal(t, "json", store.Codec().Name())
}

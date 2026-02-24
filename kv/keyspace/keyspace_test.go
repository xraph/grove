package keyspace_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/codec"
	"github.com/xraph/grove/kv/keyspace"
	"github.com/xraph/grove/kv/kvtest"
)

type user struct {
	Name  string `json:"name" msgpack:"name"`
	Email string `json:"email" msgpack:"email"`
}

func TestKeyspace_Get_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Set via store using the full prefixed key.
	value := user{Name: "alice", Email: "alice@example.com"}
	raw, err := store.Codec().Encode(value)
	require.NoError(t, err)
	require.NoError(t, store.SetRaw(ctx, "users:u1", raw))

	ks := keyspace.New[user](store, "users")
	got, err := ks.Get(ctx, "u1")
	require.NoError(t, err)
	assert.Equal(t, value, got)
}

func TestKeyspace_Get_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users")
	_, err := ks.Get(ctx, "nonexistent")
	assert.Error(t, err)
}

func TestKeyspace_Set_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users")
	value := user{Name: "bob", Email: "bob@example.com"}
	require.NoError(t, ks.Set(ctx, "u2", value))

	// Verify via store using the full prefixed key.
	raw, err := store.GetRaw(ctx, "users:u2")
	require.NoError(t, err)

	var decoded user
	require.NoError(t, store.Codec().Decode(raw, &decoded))
	assert.Equal(t, value, decoded)
}

func TestKeyspace_Set_DefaultTTL(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users", keyspace.WithTTL(5*time.Second))
	value := user{Name: "carol", Email: "carol@example.com"}
	require.NoError(t, ks.Set(ctx, "u3", value))

	// Verify the TTL was applied.
	ttl, err := store.TTL(ctx, "users:u3")
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, 5*time.Second)
}

func TestKeyspace_Delete_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users")
	require.NoError(t, ks.Set(ctx, "u4", user{Name: "dave", Email: "dave@example.com"}))

	require.NoError(t, ks.Delete(ctx, "u4"))

	count, err := ks.Exists(ctx, "u4")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestKeyspace_Exists_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users")
	require.NoError(t, ks.Set(ctx, "u5", user{Name: "eve"}))
	require.NoError(t, ks.Set(ctx, "u6", user{Name: "frank"}))

	count, err := ks.Exists(ctx, "u5", "u6")
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestKeyspace_MGet_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users")
	require.NoError(t, ks.Set(ctx, "m1", user{Name: "one"}))
	require.NoError(t, ks.Set(ctx, "m2", user{Name: "two"}))
	require.NoError(t, ks.Set(ctx, "m3", user{Name: "three"}))

	result, err := ks.MGet(ctx, []string{"m1", "m3"})
	require.NoError(t, err)
	assert.Len(t, result, 2)
	assert.Equal(t, "one", result["m1"].Name)
	assert.Equal(t, "three", result["m3"].Name)
}

func TestKeyspace_Scan_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "scan")
	require.NoError(t, ks.Set(ctx, "a", user{Name: "a"}))
	require.NoError(t, ks.Set(ctx, "b", user{Name: "b"}))
	require.NoError(t, ks.Set(ctx, "c", user{Name: "c"}))

	var keys []string
	err := ks.Scan(ctx, "*", func(key string) error {
		keys = append(keys, key)
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, keys, 3)

	// Verify that the prefix is stripped from returned keys.
	for _, k := range keys {
		assert.NotContains(t, k, "scan:")
	}
}

func TestKeyspace_Prefix(t *testing.T) {
	store := kvtest.SetupStore(t)
	ks := keyspace.New[user](store, "myprefix")
	assert.Equal(t, "myprefix", ks.Prefix())
}

func TestKeyspace_Store(t *testing.T) {
	store := kvtest.SetupStore(t)
	ks := keyspace.New[user](store, "users")
	assert.Same(t, store, ks.Store())
}

func TestKeyspace_CustomSeparator(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users", keyspace.WithSeparator("/"))
	value := user{Name: "slash", Email: "slash@example.com"}
	require.NoError(t, ks.Set(ctx, "s1", value))

	// Verify the key uses "/" as separator.
	raw, err := store.GetRaw(ctx, "users/s1")
	require.NoError(t, err)

	var decoded user
	require.NoError(t, store.Codec().Decode(raw, &decoded))
	assert.Equal(t, value, decoded)
}

func TestKeyspace_CustomCodec(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	ks := keyspace.New[user](store, "users", keyspace.WithCodec(codec.MsgPack()))
	original := user{Name: "msgpack-user", Email: "mp@example.com"}
	require.NoError(t, ks.Set(ctx, "mp1", original))

	got, err := ks.Get(ctx, "mp1")
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

func TestKeyspace_Set_WithExplicitTTL(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Keyspace has a 10s default TTL, but we override with 2s per-call.
	ks := keyspace.New[user](store, "users", keyspace.WithTTL(10*time.Second))
	require.NoError(t, ks.Set(ctx, "ttl1", user{Name: "ttl"}, kv.WithTTL(2*time.Second)))

	ttl, err := store.TTL(ctx, "users:ttl1")
	require.NoError(t, err)
	assert.LessOrEqual(t, ttl, 2*time.Second)
}

package kv_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/codec"
	"github.com/xraph/grove/kv/kvtest"
)

func TestBatch_SetGet(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Batch.Exec runs Gets before Sets, so pre-populate the keys for the Get
	// portion and use the batch Set for new keys.
	require.NoError(t, store.Set(ctx, "b:1", "alpha"))
	require.NoError(t, store.Set(ctx, "b:2", "bravo"))

	result, err := kv.NewBatch(store).
		Get("b:1", "b:2").
		Set("b:3", "charlie").
		Set("b:4", "delta").
		Exec(ctx)
	require.NoError(t, err)

	assert.Equal(t, int64(2), result.Written)
	assert.Len(t, result.Values, 2)
	assert.Contains(t, result.Values, "b:1")
	assert.Contains(t, result.Values, "b:2")
}

func TestBatch_Delete(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Pre-populate keys via store.
	require.NoError(t, store.Set(ctx, "bd:1", "v1"))
	require.NoError(t, store.Set(ctx, "bd:2", "v2"))

	result, err := kv.NewBatch(store).
		Delete("bd:1", "bd:2").
		Exec(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), result.Deleted)

	count, err := store.Exists(ctx, "bd:1", "bd:2")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestBatch_Decode_Success(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	require.NoError(t, store.Set(ctx, "dec:1", testUser{Name: "Bob", Age: 25}))

	result, err := kv.NewBatch(store).
		Get("dec:1").
		Exec(ctx)
	require.NoError(t, err)

	var got testUser
	require.NoError(t, result.Decode("dec:1", &got, codec.JSON()))
	assert.Equal(t, "Bob", got.Name)
	assert.Equal(t, 25, got.Age)
}

func TestBatch_Decode_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	result, err := kv.NewBatch(store).
		Get("missing-key").
		Exec(ctx)
	require.NoError(t, err)

	var dest string
	err = result.Decode("missing-key", &dest, codec.JSON())
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestBatch_Exec_EmptyBatch(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	result, err := kv.NewBatch(store).Exec(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Empty(t, result.Values)
	assert.Equal(t, int64(0), result.Written)
	assert.Equal(t, int64(0), result.Deleted)
}

func TestBatch_Exec_ClosedStore(t *testing.T) {
	store := kvtest.SetupStore(t)
	require.NoError(t, store.Close())

	_, err := kv.NewBatch(store).
		Set("k", "v").
		Exec(context.Background())
	assert.ErrorIs(t, err, kv.ErrStoreClosed)
}

func TestBatch_WithOptions_TTL(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	result, err := kv.NewBatch(store).
		Set("bttl:1", "val").
		WithOptions(kv.WithTTL(1 * time.Second)).
		Exec(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Written)

	// Key should exist immediately.
	count, err := store.Exists(ctx, "bttl:1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestBatch_Chaining(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	// Verify the fluent chain compiles and runs without error.
	result, err := kv.NewBatch(store).
		Set("chain:a", "1").
		Set("chain:b", "2").
		Get("chain:a", "chain:b").
		Delete("chain:a").
		WithOptions(kv.WithTTL(5 * time.Second)).
		Exec(ctx)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

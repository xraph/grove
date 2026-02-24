package kvcrdt_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestCounter_Increment(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	counter := kvcrdt.NewCounter(store, "crdt:counter:1", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, counter.Increment(ctx, 5))

	val, err := counter.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), val)
}

func TestCounter_IncrementMultiple(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	counter := kvcrdt.NewCounter(store, "crdt:counter:2", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, counter.Increment(ctx, 3))
	require.NoError(t, counter.Increment(ctx, 7))

	val, err := counter.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), val)
}

func TestCounter_Decrement(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	counter := kvcrdt.NewCounter(store, "crdt:counter:3", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, counter.Increment(ctx, 10))
	require.NoError(t, counter.Decrement(ctx, 3))

	val, err := counter.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(7), val)
}

func TestCounter_Value_Initial(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	counter := kvcrdt.NewCounter(store, "crdt:counter:fresh", kvcrdt.WithNodeID("node-1"))

	val, err := counter.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestCounter_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	counter := kvcrdt.NewCounter(store, "crdt:counter:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, counter.Increment(ctx, 42))

	state, err := counter.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, int64(42), state.Value())
}

func TestCounter_Merge(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	counter1 := kvcrdt.NewCounter(store1, "crdt:counter:merge", kvcrdt.WithNodeID("node-1"))
	counter2 := kvcrdt.NewCounter(store2, "crdt:counter:merge", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, counter1.Increment(ctx, 10))
	require.NoError(t, counter2.Increment(ctx, 20))

	// Get state from counter2 and merge into counter1.
	remoteState, err := counter2.State(ctx)
	require.NoError(t, err)

	require.NoError(t, counter1.Merge(ctx, remoteState))

	val, err := counter1.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(30), val)
}

func TestCounter_Merge_Idempotent(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	counter1 := kvcrdt.NewCounter(store1, "crdt:counter:idem", kvcrdt.WithNodeID("node-1"))
	counter2 := kvcrdt.NewCounter(store2, "crdt:counter:idem", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, counter1.Increment(ctx, 5))
	require.NoError(t, counter2.Increment(ctx, 15))

	remoteState, err := counter2.State(ctx)
	require.NoError(t, err)

	// Merge twice.
	require.NoError(t, counter1.Merge(ctx, remoteState))
	require.NoError(t, counter1.Merge(ctx, remoteState))

	val, err := counter1.Value(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(20), val)
}

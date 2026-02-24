package extension_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func TestAtomicCounter_Increment(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	val, err := counter.Increment(context.Background(), 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), val)
}

func TestAtomicCounter_IncrementMultiple(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	ctx := context.Background()
	_, err := counter.Increment(ctx, 3)
	require.NoError(t, err)

	val, err := counter.Increment(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, int64(10), val)
}

func TestAtomicCounter_Decrement(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	ctx := context.Background()
	_, err := counter.Increment(ctx, 10)
	require.NoError(t, err)

	val, err := counter.Decrement(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, int64(7), val)
}

func TestAtomicCounter_Get_Initial(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "fresh")

	val, err := counter.Get(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestAtomicCounter_Get_AfterIncrement(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	ctx := context.Background()
	expected, err := counter.Increment(ctx, 42)
	require.NoError(t, err)

	val, err := counter.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, val)
}

func TestAtomicCounter_Reset(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	ctx := context.Background()
	_, err := counter.Increment(ctx, 99)
	require.NoError(t, err)

	err = counter.Reset(ctx)
	require.NoError(t, err)

	val, err := counter.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), val)
}

func TestAtomicCounter_Set(t *testing.T) {
	store := kvtest.SetupStore(t)
	counter := extension.NewAtomicCounter(store, "hits")

	ctx := context.Background()
	err := counter.Set(ctx, 42)
	require.NoError(t, err)

	val, err := counter.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(42), val)
}

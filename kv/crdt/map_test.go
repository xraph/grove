package kvcrdt_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestMap_Set_Get(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:1", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "name", "Alice"))

	var dest string
	require.NoError(t, m.Get(ctx, "name", &dest))
	assert.Equal(t, "Alice", dest)
}

func TestMap_Set_Overwrites(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:2", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "name", "A"))
	require.NoError(t, m.Set(ctx, "name", "B"))

	var dest string
	require.NoError(t, m.Get(ctx, "name", &dest))
	assert.Equal(t, "B", dest)
}

func TestMap_Get_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:nf", kvcrdt.WithNodeID("node-1"))

	var dest string
	err := m.Get(ctx, "missing", &dest)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestMap_Delete(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:del", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "k", "v"))
	require.NoError(t, m.Delete(ctx, "k"))

	var dest string
	err := m.Get(ctx, "k", &dest)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestMap_Keys(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:keys", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "a", 1))
	require.NoError(t, m.Set(ctx, "b", 2))
	require.NoError(t, m.Set(ctx, "c", 3))

	keys, err := m.Keys(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}

func TestMap_All(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:all", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "x", "one"))
	require.NoError(t, m.Set(ctx, "y", "two"))
	require.NoError(t, m.Set(ctx, "z", "three"))

	all, err := m.All(ctx)
	require.NoError(t, err)
	assert.Len(t, all, 3)
	assert.Contains(t, all, "x")
	assert.Contains(t, all, "y")
	assert.Contains(t, all, "z")
}

func TestMap_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	m := kvcrdt.NewMap(store, "crdt:map:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, m.Set(ctx, "field", "value"))

	state, err := m.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Contains(t, state.Fields, "field")
}

func TestMap_Merge_RemoteWins(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	baseTime := time.Now()
	localClock := crdt.NewHybridClock("node-1", crdt.WithNowFunc(func() time.Time {
		return baseTime
	}))
	remoteClock := crdt.NewHybridClock("node-2", crdt.WithNowFunc(func() time.Time {
		return baseTime.Add(10 * time.Second)
	}))

	localMap := kvcrdt.NewMap(store1, "crdt:map:mrw",
		kvcrdt.WithNodeID("node-1"), kvcrdt.WithClock(localClock))
	remoteMap := kvcrdt.NewMap(store2, "crdt:map:mrw",
		kvcrdt.WithNodeID("node-2"), kvcrdt.WithClock(remoteClock))

	require.NoError(t, localMap.Set(ctx, "color", "red"))
	require.NoError(t, remoteMap.Set(ctx, "color", "blue"))

	remoteState, err := remoteMap.State(ctx)
	require.NoError(t, err)

	require.NoError(t, localMap.Merge(ctx, remoteState))

	var dest string
	require.NoError(t, localMap.Get(ctx, "color", &dest))
	assert.Equal(t, "blue", dest)
}

func TestMap_Merge_LocalWins(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	baseTime := time.Now()
	localClock := crdt.NewHybridClock("node-1", crdt.WithNowFunc(func() time.Time {
		return baseTime.Add(10 * time.Second)
	}))
	remoteClock := crdt.NewHybridClock("node-2", crdt.WithNowFunc(func() time.Time {
		return baseTime
	}))

	localMap := kvcrdt.NewMap(store1, "crdt:map:mlw",
		kvcrdt.WithNodeID("node-1"), kvcrdt.WithClock(localClock))
	remoteMap := kvcrdt.NewMap(store2, "crdt:map:mlw",
		kvcrdt.WithNodeID("node-2"), kvcrdt.WithClock(remoteClock))

	require.NoError(t, localMap.Set(ctx, "color", "red"))
	require.NoError(t, remoteMap.Set(ctx, "color", "blue"))

	remoteState, err := remoteMap.State(ctx)
	require.NoError(t, err)

	require.NoError(t, localMap.Merge(ctx, remoteState))

	var dest string
	require.NoError(t, localMap.Get(ctx, "color", &dest))
	assert.Equal(t, "red", dest)
}

func TestMap_Merge_DisjointFields(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	localMap := kvcrdt.NewMap(store1, "crdt:map:disjoint", kvcrdt.WithNodeID("node-1"))
	remoteMap := kvcrdt.NewMap(store2, "crdt:map:disjoint", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, localMap.Set(ctx, "fieldA", "valueA"))
	require.NoError(t, remoteMap.Set(ctx, "fieldB", "valueB"))

	remoteState, err := remoteMap.State(ctx)
	require.NoError(t, err)

	require.NoError(t, localMap.Merge(ctx, remoteState))

	var destA, destB string
	require.NoError(t, localMap.Get(ctx, "fieldA", &destA))
	require.NoError(t, localMap.Get(ctx, "fieldB", &destB))
	assert.Equal(t, "valueA", destA)
	assert.Equal(t, "valueB", destB)
}

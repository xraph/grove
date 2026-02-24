package kvcrdt_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestSet_Add_Members(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:1", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, s.Add(ctx, "a"))
	require.NoError(t, s.Add(ctx, "b"))

	members, err := s.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, members)
}

func TestSet_Add_Duplicate(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:dup", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, s.Add(ctx, "a"))
	require.NoError(t, s.Add(ctx, "a"))

	members, err := s.Members(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, members)
}

func TestSet_Remove(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:rm", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, s.Add(ctx, "a"))
	require.NoError(t, s.Add(ctx, "b"))
	require.NoError(t, s.Remove(ctx, "a"))

	members, err := s.Members(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, members)
}

func TestSet_Contains_True(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:ct", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, s.Add(ctx, "x"))

	ok, err := s.Contains(ctx, "x")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestSet_Contains_False(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:cf", kvcrdt.WithNodeID("node-1"))

	ok, err := s.Contains(ctx, "y")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestSet_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	s := kvcrdt.NewSet[string](store, "crdt:set:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, s.Add(ctx, "elem"))

	state, err := s.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
}

func TestSet_Merge(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	set1 := kvcrdt.NewSet[string](store1, "crdt:set:merge", kvcrdt.WithNodeID("node-1"))
	set2 := kvcrdt.NewSet[string](store2, "crdt:set:merge", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, set1.Add(ctx, "a"))
	require.NoError(t, set1.Add(ctx, "b"))
	require.NoError(t, set2.Add(ctx, "c"))
	require.NoError(t, set2.Add(ctx, "d"))

	remoteState, err := set2.State(ctx)
	require.NoError(t, err)

	require.NoError(t, set1.Merge(ctx, remoteState))

	members, err := set1.Members(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b", "c", "d"}, members)
}

func TestSet_Merge_AddWins(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	set1 := kvcrdt.NewSet[string](store1, "crdt:set:addwins", kvcrdt.WithNodeID("node-1"))
	set2 := kvcrdt.NewSet[string](store2, "crdt:set:addwins", kvcrdt.WithNodeID("node-2"))

	// Both add "x" initially.
	require.NoError(t, set1.Add(ctx, "x"))
	require.NoError(t, set2.Add(ctx, "x"))

	// Node-1 removes "x".
	require.NoError(t, set1.Remove(ctx, "x"))

	// Node-2 concurrently re-adds "x" (new tag).
	require.NoError(t, set2.Add(ctx, "x"))

	// Merge set2 state into set1. The concurrent add from node-2 should win.
	remoteState, err := set2.State(ctx)
	require.NoError(t, err)

	require.NoError(t, set1.Merge(ctx, remoteState))

	ok, err := set1.Contains(ctx, "x")
	require.NoError(t, err)
	assert.True(t, ok, "add-wins: concurrent add should beat remove")
}

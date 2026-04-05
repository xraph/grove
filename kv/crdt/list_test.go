package kvcrdt_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/crdt"
	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestList_Append(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:append", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, list.Append(ctx, "a"))
	require.NoError(t, list.Append(ctx, "b"))
	require.NoError(t, list.Append(ctx, "c"))

	elems, err := list.Elements(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, elems)
}

func TestList_InsertAfter(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:insertafter", kvcrdt.WithNodeID("node-1"))

	// Append two elements.
	require.NoError(t, list.Append(ctx, "first"))
	require.NoError(t, list.Append(ctx, "third"))

	// Get the first element's ID to insert after it.
	ids, err := list.NodeIDs(ctx)
	require.NoError(t, err)
	require.Len(t, ids, 2)

	// Insert "second" after the first element.
	require.NoError(t, list.InsertAfter(ctx, ids[0], "second"))

	elems, err := list.Elements(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second", "third"}, elems)
}

func TestList_Delete(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:delete", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, list.Append(ctx, "a"))
	require.NoError(t, list.Append(ctx, "b"))
	require.NoError(t, list.Append(ctx, "c"))

	// Delete the second element.
	ids, err := list.NodeIDs(ctx)
	require.NoError(t, err)
	require.Len(t, ids, 3)

	require.NoError(t, list.Delete(ctx, ids[1]))

	elems, err := list.Elements(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "c"}, elems)
}

func TestList_Elements(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[int](store, "crdt:list:elements", kvcrdt.WithNodeID("node-1"))

	for i := 1; i <= 5; i++ {
		require.NoError(t, list.Append(ctx, i))
	}

	elems, err := list.Elements(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, elems)
}

func TestList_Len(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:len", kvcrdt.WithNodeID("node-1"))

	// Empty list.
	n, err := list.Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// After appending.
	require.NoError(t, list.Append(ctx, "a"))
	require.NoError(t, list.Append(ctx, "b"))

	n, err = list.Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// After deleting one.
	ids, err := list.NodeIDs(ctx)
	require.NoError(t, err)
	require.NoError(t, list.Delete(ctx, ids[0]))

	n, err = list.Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestList_Merge(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	list1 := kvcrdt.NewList[string](store1, "crdt:list:merge", kvcrdt.WithNodeID("node-1"))
	list2 := kvcrdt.NewList[string](store2, "crdt:list:merge", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, list1.Append(ctx, "a"))
	require.NoError(t, list1.Append(ctx, "b"))
	require.NoError(t, list2.Append(ctx, "c"))
	require.NoError(t, list2.Append(ctx, "d"))

	remoteState, err := list2.State(ctx)
	require.NoError(t, err)

	require.NoError(t, list1.Merge(ctx, remoteState))

	elems, err := list1.Elements(ctx)
	require.NoError(t, err)
	// After merge, all elements should be present.
	assert.Len(t, elems, 4)
	assert.Contains(t, elems, "a")
	assert.Contains(t, elems, "b")
	assert.Contains(t, elems, "c")
	assert.Contains(t, elems, "d")
}

func TestList_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, list.Append(ctx, "x"))
	require.NoError(t, list.Append(ctx, "y"))

	state, err := list.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, 2, state.Len())
}

func TestList_EmptyList(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	list := kvcrdt.NewList[string](store, "crdt:list:empty", kvcrdt.WithNodeID("node-1"))

	// Elements on empty list returns empty slice.
	elems, err := list.Elements(ctx)
	require.NoError(t, err)
	assert.Empty(t, elems)

	// Len on empty list returns 0.
	n, err := list.Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// NodeIDs on empty list returns empty slice.
	ids, err := list.NodeIDs(ctx)
	require.NoError(t, err)
	assert.Empty(t, ids)

	// State on empty list returns non-nil state.
	state, err := list.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)

	// Merge with nil remote is a no-op.
	require.NoError(t, list.Merge(ctx, crdt.NewRGAListState()))
}

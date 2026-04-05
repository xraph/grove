package kvcrdt_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestDocument_Set_Get(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:setget", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.Set(ctx, "title", "Hello World"))

	var title string
	require.NoError(t, doc.Get(ctx, "title", &title))
	assert.Equal(t, "Hello World", title)
}

func TestDocument_NestedPaths(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:nested", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.Set(ctx, "address.city", "New York"))
	require.NoError(t, doc.Set(ctx, "address.zip", "10001"))
	require.NoError(t, doc.Set(ctx, "name", "Alice"))

	var city string
	require.NoError(t, doc.Get(ctx, "address.city", &city))
	assert.Equal(t, "New York", city)

	var zip string
	require.NoError(t, doc.Get(ctx, "address.zip", &zip))
	assert.Equal(t, "10001", zip)

	var name string
	require.NoError(t, doc.Get(ctx, "name", &name))
	assert.Equal(t, "Alice", name)
}

func TestDocument_Delete(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:delete", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.Set(ctx, "address.city", "NYC"))
	require.NoError(t, doc.Set(ctx, "address.zip", "10001"))
	require.NoError(t, doc.Set(ctx, "name", "Bob"))

	// Delete the entire address subtree.
	require.NoError(t, doc.Delete(ctx, "address"))

	// address.city should now be gone.
	var city string
	err := doc.Get(ctx, "address.city", &city)
	assert.ErrorIs(t, err, kv.ErrNotFound)

	// name should still exist.
	var name string
	require.NoError(t, doc.Get(ctx, "name", &name))
	assert.Equal(t, "Bob", name)
}

func TestDocument_SetCounter(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:counter", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.SetCounter(ctx, "views", 5))
	require.NoError(t, doc.SetCounter(ctx, "views", 3))

	// Resolve and check the counter value.
	resolved, err := doc.Resolve(ctx)
	require.NoError(t, err)
	// Counter value should be the accumulated total.
	views, ok := resolved["views"]
	require.True(t, ok)
	assert.Equal(t, int64(8), views)
}

func TestDocument_Resolve(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:resolve", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.Set(ctx, "user.name", "Alice"))
	require.NoError(t, doc.Set(ctx, "user.email", "alice@example.com"))
	require.NoError(t, doc.Set(ctx, "active", true))

	resolved, err := doc.Resolve(ctx)
	require.NoError(t, err)

	// Check nested structure.
	user, ok := resolved["user"].(map[string]any)
	require.True(t, ok, "user should be a nested map")
	assert.Equal(t, "Alice", user["name"])
	assert.Equal(t, "alice@example.com", user["email"])
	assert.Equal(t, true, resolved["active"])
}

func TestDocument_Merge(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	doc1 := kvcrdt.NewDocument(store1, "crdt:doc:merge", kvcrdt.WithNodeID("node-1"))
	doc2 := kvcrdt.NewDocument(store2, "crdt:doc:merge", kvcrdt.WithNodeID("node-2"))

	require.NoError(t, doc1.Set(ctx, "title", "From Node 1"))
	require.NoError(t, doc1.Set(ctx, "author", "Alice"))
	require.NoError(t, doc2.Set(ctx, "title", "From Node 2"))
	require.NoError(t, doc2.Set(ctx, "category", "Tech"))

	remoteState, err := doc2.State(ctx)
	require.NoError(t, err)

	require.NoError(t, doc1.Merge(ctx, remoteState))

	resolved, err := doc1.Resolve(ctx)
	require.NoError(t, err)

	// "author" should be from node-1 only.
	assert.Equal(t, "Alice", resolved["author"])
	// "category" should be from node-2.
	assert.Equal(t, "Tech", resolved["category"])
	// "title" should be resolved by LWW (whichever HLC is higher).
	assert.NotNil(t, resolved["title"])
}

func TestDocument_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, doc.Set(ctx, "a", 1))
	require.NoError(t, doc.Set(ctx, "b", 2))

	state, err := doc.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Len(t, state.Fields, 2)
}

func TestDocument_EmptyDoc(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	doc := kvcrdt.NewDocument(store, "crdt:doc:empty", kvcrdt.WithNodeID("node-1"))

	// Get on empty document returns ErrNotFound.
	var val string
	err := doc.Get(ctx, "anything", &val)
	assert.ErrorIs(t, err, kv.ErrNotFound)

	// Resolve on empty document returns an empty map.
	resolved, err := doc.Resolve(ctx)
	require.NoError(t, err)
	assert.Empty(t, resolved)

	// State on empty document returns non-nil state.
	state, err := doc.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Empty(t, state.Fields)

	// Merge with empty remote is a no-op.
	require.NoError(t, doc.Merge(ctx, crdt.NewDocumentCRDTState()))
}

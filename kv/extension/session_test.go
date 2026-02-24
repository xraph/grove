package extension_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func TestSessionStore_Create_Get(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	data := map[string]string{"user": "alice", "role": "admin"}
	id, err := ss.Create(ctx, data)
	require.NoError(t, err)
	assert.NotEmpty(t, id)

	var retrieved map[string]any
	err = ss.Get(ctx, id, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, "alice", retrieved["user"])
	assert.Equal(t, "admin", retrieved["role"])
}

func TestSessionStore_Create_UniqueIDs(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	ids := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		id, err := ss.Create(ctx, i)
		require.NoError(t, err)
		ids[id] = struct{}{}
	}
	assert.Len(t, ids, 100, "all 100 session IDs should be unique")
}

func TestSessionStore_Get_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	var data any
	err := ss.Get(context.Background(), "nonexistent-session-id", &data)
	require.Error(t, err)
}

func TestSessionStore_Update(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	id, err := ss.Create(ctx, map[string]string{"key": "old"})
	require.NoError(t, err)

	err = ss.Update(ctx, id, map[string]string{"key": "new"})
	require.NoError(t, err)

	var retrieved map[string]any
	err = ss.Get(ctx, id, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, "new", retrieved["key"])
}

func TestSessionStore_Delete(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	id, err := ss.Create(ctx, "session-data")
	require.NoError(t, err)

	err = ss.Delete(ctx, id)
	require.NoError(t, err)

	var data any
	err = ss.Get(ctx, id, &data)
	require.Error(t, err)
}

func TestSessionStore_Touch(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	id, err := ss.Create(ctx, "session-data")
	require.NoError(t, err)

	err = ss.Touch(ctx, id)
	require.NoError(t, err)
}

func TestSessionStore_Exists_True(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	ctx := context.Background()
	id, err := ss.Create(ctx, "session-data")
	require.NoError(t, err)

	exists, err := ss.Exists(ctx, id)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestSessionStore_Exists_False(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store)

	exists, err := ss.Exists(context.Background(), "nonexistent-id")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestSessionStore_CustomPrefix(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store, extension.WithSessionPrefix("s"))

	ctx := context.Background()
	id, err := ss.Create(ctx, "data-with-prefix")
	require.NoError(t, err)

	var retrieved string
	err = ss.Get(ctx, id, &retrieved)
	require.NoError(t, err)
	assert.Equal(t, "data-with-prefix", retrieved)
}

func TestSessionStore_CustomTTL(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store, extension.WithSessionTTL(time.Hour))

	ctx := context.Background()
	id, err := ss.Create(ctx, "long-lived-session")
	require.NoError(t, err)

	exists, err := ss.Exists(ctx, id)
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestSessionStore_TTLExpiry(t *testing.T) {
	store := kvtest.SetupStore(t)
	ss := extension.NewSessionStore(store, extension.WithSessionTTL(50*time.Millisecond))

	ctx := context.Background()
	id, err := ss.Create(ctx, "ephemeral")
	require.NoError(t, err)

	// Wait for the TTL to expire.
	time.Sleep(100 * time.Millisecond)

	exists, err := ss.Exists(ctx, id)
	require.NoError(t, err)
	assert.False(t, exists, "session should have expired after TTL")
}

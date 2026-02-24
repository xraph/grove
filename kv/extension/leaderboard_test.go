package extension_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/extension"
	"github.com/xraph/grove/kv/kvtest"
)

func TestLeaderboard_Add_Score(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	err := lb.Add(ctx, "alice", 100.0)
	require.NoError(t, err)

	score, err := lb.Score(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, 100.0, score)
}

func TestLeaderboard_Add_UpdateScore(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	err := lb.Add(ctx, "alice", 100.0)
	require.NoError(t, err)

	err = lb.Add(ctx, "alice", 200.0)
	require.NoError(t, err)

	score, err := lb.Score(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, 200.0, score)
}

func TestLeaderboard_Score_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	_, err := lb.Score(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestLeaderboard_TopN(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	members := []struct {
		name  string
		score float64
	}{
		{"alice", 50},
		{"bob", 90},
		{"charlie", 70},
		{"dave", 30},
		{"eve", 80},
	}
	for _, m := range members {
		require.NoError(t, lb.Add(ctx, m.name, m.score))
	}

	top, err := lb.TopN(ctx, 3)
	require.NoError(t, err)
	require.Len(t, top, 3)

	// Verify descending order.
	assert.Equal(t, "bob", top[0].Member)
	assert.Equal(t, 90.0, top[0].Score)
	assert.Equal(t, "eve", top[1].Member)
	assert.Equal(t, 80.0, top[1].Score)
	assert.Equal(t, "charlie", top[2].Member)
	assert.Equal(t, 70.0, top[2].Score)
}

func TestLeaderboard_TopN_LessThanN(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	require.NoError(t, lb.Add(ctx, "alice", 50))
	require.NoError(t, lb.Add(ctx, "bob", 90))

	top, err := lb.TopN(ctx, 5)
	require.NoError(t, err)
	assert.Len(t, top, 2)
}

func TestLeaderboard_Rank(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	require.NoError(t, lb.Add(ctx, "alice", 50))
	require.NoError(t, lb.Add(ctx, "bob", 90))
	require.NoError(t, lb.Add(ctx, "charlie", 70))

	rank, err := lb.Rank(ctx, "bob")
	require.NoError(t, err)
	assert.Equal(t, 1, rank, "bob has the highest score, rank should be 1")

	rank, err = lb.Rank(ctx, "charlie")
	require.NoError(t, err)
	assert.Equal(t, 2, rank, "charlie is second, rank should be 2")

	rank, err = lb.Rank(ctx, "alice")
	require.NoError(t, err)
	assert.Equal(t, 3, rank, "alice is third, rank should be 3")
}

func TestLeaderboard_Rank_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	_, err := lb.Rank(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestLeaderboard_Remove(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	require.NoError(t, lb.Add(ctx, "alice", 100))

	err := lb.Remove(ctx, "alice")
	require.NoError(t, err)

	_, err = lb.Score(ctx, "alice")
	require.Error(t, err)
	assert.ErrorIs(t, err, kv.ErrNotFound)
}

func TestLeaderboard_Size(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	ctx := context.Background()
	require.NoError(t, lb.Add(ctx, "alice", 10))
	require.NoError(t, lb.Add(ctx, "bob", 20))
	require.NoError(t, lb.Add(ctx, "charlie", 30))

	size, err := lb.Size(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, size)
}

func TestLeaderboard_Size_Empty(t *testing.T) {
	store := kvtest.SetupStore(t)
	lb := extension.NewLeaderboard(store, "game")

	size, err := lb.Size(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, size)
}

package kvtest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/driver"
)

// RunCollectionSuite exercises the optional sorted-set, set, and hash
// capabilities.
//
// It is separate from the conformance suite because these are optional:
// a driver that does not advertise a capability skips that section rather
// than failing. What it does enforce is that advertising and providing
// agree — a driver claiming CapSortedSet in Info() must implement
// SortedSetDriver, because a capability flag nothing can consume is worse
// than no flag at all.
func RunCollectionSuite(t *testing.T, drv driver.Driver) {
	t.Helper()

	store, err := kv.Open(drv)
	require.NoError(t, err)

	defer func() { _ = store.Close() }()

	info := drv.Info()

	t.Run("CapabilitiesMatchImplementation", func(t *testing.T) {
		if info.Has(driver.CapSortedSet) {
			assert.True(t, store.SupportsSortedSets(),
				"driver advertises CapSortedSet but does not implement SortedSetDriver")
		}
	})

	if store.SupportsSortedSets() {
		t.Run("SortedSet", func(t *testing.T) { testSortedSet(t, store) })
		t.Run("SortedSetRange", func(t *testing.T) { testSortedSetRange(t, store) })
		t.Run("SortedSetReverse", func(t *testing.T) { testSortedSetReverse(t, store) })
	}

	if store.SupportsSets() {
		t.Run("Set", func(t *testing.T) { testSet(t, store) })
	}

	if store.SupportsHashes() {
		t.Run("Hash", func(t *testing.T) { testHash(t, store) })
	}
}

func testSortedSet(t *testing.T, store *kv.Store) {
	ctx := context.Background()
	key := "kvtest:zset:basic"

	require.NoError(t, store.Delete(ctx, key))

	n, err := store.ZAdd(ctx,
		key,
		driver.ScoredMember{Member: "a", Score: 1},
		driver.ScoredMember{Member: "b", Score: 2},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n, "ZAdd should report two new members")

	card, err := store.ZCard(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(2), card)

	score, ok, err := store.ZScore(ctx, key, "b")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.InDelta(t, 2.0, score, 0.0001)

	_, ok, err = store.ZScore(ctx, key, "absent")
	require.NoError(t, err, "a missing member is not an error")
	assert.False(t, ok)

	removed, err := store.ZRem(ctx, key, "a")
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)
}

// testSortedSetRange covers the read a work queue depends on: everything
// due up to a cutoff, in score order, bounded in number.
func testSortedSetRange(t *testing.T, store *kv.Store) {
	ctx := context.Background()
	key := "kvtest:zset:range"

	require.NoError(t, store.Delete(ctx, key))

	_, err := store.ZAdd(ctx, key,
		driver.ScoredMember{Member: "first", Score: 10},
		driver.ScoredMember{Member: "second", Score: 20},
		driver.ScoredMember{Member: "third", Score: 30},
	)
	require.NoError(t, err)

	all, err := store.ZRange(ctx, key, driver.RangeSpec{})
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second", "third"}, all,
		"an unbounded range returns every member in score order")

	due, err := store.ZRange(ctx, key, driver.RangeSpec{Max: 20, HasMax: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"first", "second"}, due, "Max is inclusive")

	limited, err := store.ZRange(ctx, key, driver.RangeSpec{Count: 1})
	require.NoError(t, err)
	assert.Equal(t, []string{"first"}, limited)

	offset, err := store.ZRange(ctx, key, driver.RangeSpec{Offset: 1, Count: 1})
	require.NoError(t, err)
	assert.Equal(t, []string{"second"}, offset)

	scored, err := store.ZRangeWithScores(ctx, key, driver.RangeSpec{Min: 20, HasMin: true})
	require.NoError(t, err)
	require.Len(t, scored, 2)
	assert.Equal(t, "second", scored[0].Member)
	assert.InDelta(t, 20.0, scored[0].Score, 0.0001)
}

func testSortedSetReverse(t *testing.T, store *kv.Store) {
	ctx := context.Background()
	key := "kvtest:zset:reverse"

	require.NoError(t, store.Delete(ctx, key))

	_, err := store.ZAdd(ctx, key,
		driver.ScoredMember{Member: "old", Score: 1},
		driver.ScoredMember{Member: "new", Score: 2},
	)
	require.NoError(t, err)

	got, err := store.ZRange(ctx, key, driver.RangeSpec{Reverse: true})
	require.NoError(t, err)
	assert.Equal(t, []string{"new", "old"}, got,
		"Reverse returns highest score first")
}

func testSet(t *testing.T, store *kv.Store) {
	ctx := context.Background()
	key := "kvtest:set:basic"

	require.NoError(t, store.Delete(ctx, key))

	n, err := store.SAdd(ctx, key, "a", "b")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	// Re-adding an existing member is not new.
	n, err = store.SAdd(ctx, key, "a")
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	members, err := store.SMembers(ctx, key)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, members)

	card, err := store.SCard(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(2), card)

	ok, err := store.SIsMember(ctx, key, "a")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = store.SIsMember(ctx, key, "absent")
	require.NoError(t, err)
	assert.False(t, ok)

	removed, err := store.SRem(ctx, key, "a")
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)
}

func testHash(t *testing.T, store *kv.Store) {
	ctx := context.Background()
	key := "kvtest:hash:basic"

	require.NoError(t, store.Delete(ctx, key))

	n, err := store.HSet(ctx, key, map[string][]byte{
		"one": []byte("1"),
		"two": []byte("2"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	got, err := store.HGet(ctx, key, "one")
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), got)

	_, err = store.HGet(ctx, key, "absent")
	assert.ErrorIs(t, err, kv.ErrNotFound,
		"a missing field must report ErrNotFound, not an empty value")

	all, err := store.HGetAll(ctx, key)
	require.NoError(t, err)
	assert.Len(t, all, 2)
	assert.Equal(t, []byte("2"), all["two"])

	length, err := store.HLen(ctx, key)
	require.NoError(t, err)
	assert.Equal(t, int64(2), length)

	removed, err := store.HDel(ctx, key, "one")
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)
}

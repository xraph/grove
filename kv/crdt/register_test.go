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

func TestRegister_Set_Get(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	reg := kvcrdt.NewRegister[string](store, "crdt:reg:1", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, reg.Set(ctx, "hello"))

	val, err := reg.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestRegister_Set_Overwrites(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	reg := kvcrdt.NewRegister[string](store, "crdt:reg:2", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, reg.Set(ctx, "a"))
	require.NoError(t, reg.Set(ctx, "b"))

	val, err := reg.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "b", val)
}

func TestRegister_Get_NotFound(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	reg := kvcrdt.NewRegister[string](store, "crdt:reg:missing", kvcrdt.WithNodeID("node-1"))

	_, err := reg.Get(ctx)
	require.ErrorIs(t, err, kv.ErrNotFound)
}

func TestRegister_State(t *testing.T) {
	store := kvtest.SetupStore(t)
	ctx := context.Background()

	reg := kvcrdt.NewRegister[string](store, "crdt:reg:state", kvcrdt.WithNodeID("node-1"))

	require.NoError(t, reg.Set(ctx, "value"))

	state, err := reg.State(ctx)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, "node-1", state.NodeID)
}

func TestRegister_Merge_RemoteWins(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Use clocks with controllable time to ensure remote is later.
	baseTime := time.Now()
	localClock := crdt.NewHybridClock("node-1", crdt.WithNowFunc(func() time.Time {
		return baseTime
	}))
	remoteClock := crdt.NewHybridClock("node-2", crdt.WithNowFunc(func() time.Time {
		return baseTime.Add(10 * time.Second)
	}))

	localReg := kvcrdt.NewRegister[string](store1, "crdt:reg:merge",
		kvcrdt.WithNodeID("node-1"), kvcrdt.WithClock(localClock))
	remoteReg := kvcrdt.NewRegister[string](store2, "crdt:reg:merge",
		kvcrdt.WithNodeID("node-2"), kvcrdt.WithClock(remoteClock))

	require.NoError(t, localReg.Set(ctx, "local-value"))
	require.NoError(t, remoteReg.Set(ctx, "remote-value"))

	remoteState, err := remoteReg.State(ctx)
	require.NoError(t, err)

	require.NoError(t, localReg.Merge(ctx, remoteState))

	val, err := localReg.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "remote-value", val)
}

func TestRegister_Merge_LocalWins(t *testing.T) {
	store1, store2 := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Local clock is later than remote.
	baseTime := time.Now()
	localClock := crdt.NewHybridClock("node-1", crdt.WithNowFunc(func() time.Time {
		return baseTime.Add(10 * time.Second)
	}))
	remoteClock := crdt.NewHybridClock("node-2", crdt.WithNowFunc(func() time.Time {
		return baseTime
	}))

	localReg := kvcrdt.NewRegister[string](store1, "crdt:reg:merge-local",
		kvcrdt.WithNodeID("node-1"), kvcrdt.WithClock(localClock))
	remoteReg := kvcrdt.NewRegister[string](store2, "crdt:reg:merge-local",
		kvcrdt.WithNodeID("node-2"), kvcrdt.WithClock(remoteClock))

	require.NoError(t, localReg.Set(ctx, "local-value"))
	require.NoError(t, remoteReg.Set(ctx, "remote-value"))

	remoteState, err := remoteReg.State(ctx)
	require.NoError(t, err)

	require.NoError(t, localReg.Merge(ctx, remoteState))

	val, err := localReg.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, "local-value", val)
}

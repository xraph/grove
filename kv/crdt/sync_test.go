package kvcrdt_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/crdt"
	kvcrdt "github.com/xraph/grove/kv/crdt"
	"github.com/xraph/grove/kv/kvtest"
)

func TestSyncer_Sync_PrimaryToReplica(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Write a CRDT state to primary under a crdt: prefixed key.
	state := crdt.NewState("test", "pk1")
	state.Fields["field1"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-1"},
		NodeID: "node-1",
		Value:  json.RawMessage(`"hello"`),
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, primary.SetRaw(ctx, "crdt:test:1", raw))

	syncer := kvcrdt.NewSyncer(primary, replica)
	report, err := syncer.Sync(ctx)
	require.NoError(t, err)
	require.NotNil(t, report)

	// Verify the key exists in replica.
	replicaRaw, err := replica.GetRaw(ctx, "crdt:test:1")
	require.NoError(t, err)
	assert.NotNil(t, replicaRaw)
}

func TestSyncer_Sync_ReplicaToPrimary(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Write a key to replica only.
	state := crdt.NewState("test", "pk2")
	state.Fields["field1"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-2"},
		NodeID: "node-2",
		Value:  json.RawMessage(`"world"`),
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, replica.SetRaw(ctx, "crdt:test:2", raw))

	syncer := kvcrdt.NewSyncer(primary, replica)
	report, err := syncer.Sync(ctx)
	require.NoError(t, err)
	assert.True(t, report.Pulled > 0, "should pull from replica to primary")

	// Verify the key exists in primary.
	primaryRaw, err := primary.GetRaw(ctx, "crdt:test:2")
	require.NoError(t, err)
	assert.NotNil(t, primaryRaw)
}

func TestSyncer_Sync_Bidirectional(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Different keys in each store.
	stateA := crdt.NewState("test", "pkA")
	stateA.Fields["f"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-1"},
		NodeID: "node-1",
		Value:  json.RawMessage(`"A"`),
	}
	rawA, err := json.Marshal(stateA)
	require.NoError(t, err)
	require.NoError(t, primary.SetRaw(ctx, "crdt:a", rawA))

	stateB := crdt.NewState("test", "pkB")
	stateB.Fields["f"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-2"},
		NodeID: "node-2",
		Value:  json.RawMessage(`"B"`),
	}
	rawB, err := json.Marshal(stateB)
	require.NoError(t, err)
	require.NoError(t, replica.SetRaw(ctx, "crdt:b", rawB))

	syncer := kvcrdt.NewSyncer(primary, replica)
	_, err = syncer.Sync(ctx)
	require.NoError(t, err)

	// Both stores should have both keys.
	_, err = primary.GetRaw(ctx, "crdt:b")
	require.NoError(t, err)
	_, err = replica.GetRaw(ctx, "crdt:a")
	require.NoError(t, err)
}

func TestSyncer_Sync_Report(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Put a CRDT state in primary.
	state := crdt.NewState("test", "pk-report")
	state.Fields["f"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-1"},
		NodeID: "node-1",
		Value:  json.RawMessage(`"v"`),
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, primary.SetRaw(ctx, "crdt:report", raw))

	syncer := kvcrdt.NewSyncer(primary, replica)
	report, err := syncer.Sync(ctx)
	require.NoError(t, err)
	require.NotNil(t, report)

	// The key should be pushed or merged to replica.
	total := report.Merged + report.Pushed + report.Pulled
	assert.True(t, total > 0, "sync report should record at least one operation")
}

func TestSyncer_StartStop(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	syncer := kvcrdt.NewSyncer(primary, replica,
		kvcrdt.WithSyncInterval(10*time.Millisecond))

	syncer.Start(ctx)

	// Let a few ticks run.
	time.Sleep(50 * time.Millisecond)

	// Stop should return without hanging.
	done := make(chan struct{})
	go func() {
		syncer.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success: goroutine stopped.
	case <-time.After(2 * time.Second):
		t.Fatal("syncer.Stop() did not return within timeout")
	}
}

func TestSyncer_Options_Interval(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	syncer := kvcrdt.NewSyncer(primary, replica,
		kvcrdt.WithSyncInterval(50*time.Millisecond))

	// Start and stop should work with custom interval.
	syncer.Start(ctx)
	time.Sleep(80 * time.Millisecond)
	syncer.Stop()
}

func TestSyncer_Options_KeyPattern(t *testing.T) {
	primary, replica := kvtest.SetupTwoStores(t)
	ctx := context.Background()

	// Put keys with two different prefixes in primary.
	state := crdt.NewState("test", "pk")
	state.Fields["f"] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    crdt.HLC{Timestamp: time.Now().UnixNano(), Counter: 0, NodeID: "node-1"},
		NodeID: "node-1",
		Value:  json.RawMessage(`"v"`),
	}
	raw, err := json.Marshal(state)
	require.NoError(t, err)

	require.NoError(t, primary.SetRaw(ctx, "custom:key1", raw))
	require.NoError(t, primary.SetRaw(ctx, "other:key2", raw))

	syncer := kvcrdt.NewSyncer(primary, replica,
		kvcrdt.WithKeyPattern("custom:*"))

	_, err = syncer.Sync(ctx)
	require.NoError(t, err)

	// Only "custom:key1" should be synced.
	_, err = replica.GetRaw(ctx, "custom:key1")
	require.NoError(t, err)

	// "other:key2" should not be in replica.
	_, err = replica.GetRaw(ctx, "other:key2")
	assert.Error(t, err, "key with non-matching pattern should not be synced")
}

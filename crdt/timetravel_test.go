package crdt

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- TimeTravelConfig option tests ---

func TestWithTimeTravelEnabled(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))
	require.NotNil(t, ctrl.timeTravel)
	assert.True(t, ctrl.timeTravel.Enabled)
}

func TestWithTimeTravelEnabled_False(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(false))
	require.NotNil(t, ctrl.timeTravel)
	assert.False(t, ctrl.timeTravel.Enabled)
}

func TestWithTimeTravelMaxDepth(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin, WithTimeTravelMaxDepth(50))
	require.NotNil(t, ctrl.timeTravel)
	assert.Equal(t, 50, ctrl.timeTravel.MaxHistoryDepth)
}

func TestWithTimeTravelMaxDepth_Combined(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin,
		WithTimeTravelEnabled(true),
		WithTimeTravelMaxDepth(200),
	)
	require.NotNil(t, ctrl.timeTravel)
	assert.True(t, ctrl.timeTravel.Enabled)
	assert.Equal(t, 200, ctrl.timeTravel.MaxHistoryDepth)
}

// --- HandleHistory tests ---

func TestHandleHistory_DisabledReturnsError(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin) // time-travel not enabled

	_, err := ctrl.HandleHistory(context.Background(), &HistoryRequest{
		Table: "docs",
		PK:    "1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

func TestHandleHistory_EnabledWithNilMetadata(t *testing.T) {
	plugin := New(WithNodeID("test"))
	// No executor set, so metadata is nil.
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))

	_, err := ctrl.HandleHistory(context.Background(), &HistoryRequest{
		Table: "docs",
		PK:    "1",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metadata store not initialized")
}

func TestHandleHistory_EnabledReturnsState(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"hello"`),
		NodeID: "node-a",
	})
	plugin := newTestPluginWithChanges([]mockRow{
		{
			pkHash: "doc-1", fieldName: "title", hlcTS: 100, hlcCount: 1,
			nodeID: "node-a", tombstone: false, crdtState: lwwState,
		},
	})
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))

	resp, err := ctrl.HandleHistory(context.Background(), &HistoryRequest{
		Table: "docs",
		PK:    "doc-1",
		AtHLC: HLC{}, // zero = current state
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "docs", resp.Table)
	assert.Equal(t, "doc-1", resp.PK)
	require.NotNil(t, resp.State)
}

func TestHandleHistory_EnabledAtSpecificHLC(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"v1"`),
		NodeID: "node-a",
	})
	plugin := newTestPluginWithChanges([]mockRow{
		{
			pkHash: "doc-1", fieldName: "title", hlcTS: 100, hlcCount: 1,
			nodeID: "node-a", tombstone: false, crdtState: lwwState,
		},
	})
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))

	resp, err := ctrl.HandleHistory(context.Background(), &HistoryRequest{
		Table: "docs",
		PK:    "doc-1",
		AtHLC: HLC{Timestamp: 200, Counter: 0},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int64(200), resp.AtHLC.Timestamp)
}

// --- HandleFieldHistory tests ---

func TestHandleFieldHistory_DisabledReturnsError(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin) // time-travel not enabled

	_, err := ctrl.HandleFieldHistory(context.Background(), &FieldHistoryRequest{
		Table: "docs",
		PK:    "1",
		Field: "title",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not enabled")
}

func TestHandleFieldHistory_EnabledWithNilMetadata(t *testing.T) {
	plugin := New(WithNodeID("test"))
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))

	_, err := ctrl.HandleFieldHistory(context.Background(), &FieldHistoryRequest{
		Table: "docs",
		PK:    "1",
		Field: "title",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "metadata store not initialized")
}

// mockFieldHistoryRows returns rows for ReadFieldHistory (4-column scan).
type mockFieldHistoryRows struct {
	rows   []fieldHistoryRow
	index  int
	closed bool
}

type fieldHistoryRow struct {
	hlcTS    int64
	hlcCount uint32
	nodeID   string
	state    json.RawMessage
}

func (r *mockFieldHistoryRows) Next() bool {
	if r.index >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

func (r *mockFieldHistoryRows) Scan(dest ...any) error {
	row := r.rows[r.index-1]
	if len(dest) >= 4 {
		*dest[0].(*int64) = row.hlcTS
		*dest[1].(*uint32) = row.hlcCount
		*dest[2].(*string) = row.nodeID
		*dest[3].(*json.RawMessage) = row.state
	}
	return nil
}

func (r *mockFieldHistoryRows) Close() error { r.closed = true; return nil }
func (r *mockFieldHistoryRows) Err() error   { return nil }

func TestHandleFieldHistory_EnabledReturnsEntries(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:  TypeLWW,
		Value: json.RawMessage(`"hello"`),
	})
	queryCount := 0
	exec := &mockExecutor{
		queryFn: func(_ context.Context, _ string, _ ...any) (Rows, error) {
			queryCount++
			if queryCount == 1 {
				// ReadFieldHistory query returns 4-column rows.
				return &mockFieldHistoryRows{rows: []fieldHistoryRow{
					{hlcTS: 200, hlcCount: 1, nodeID: "node-a", state: lwwState},
					{hlcTS: 100, hlcCount: 0, nodeID: "node-a", state: lwwState},
				}}, nil
			}
			return &mockRows{}, nil
		},
	}

	plugin := New(WithNodeID("test"))
	plugin.SetExecutor(exec)
	ctrl := NewSyncController(plugin, WithTimeTravelEnabled(true))

	resp, err := ctrl.HandleFieldHistory(context.Background(), &FieldHistoryRequest{
		Table: "docs",
		PK:    "doc-1",
		Field: "title",
		Limit: 10,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "docs", resp.Table)
	assert.Equal(t, "doc-1", resp.PK)
	assert.Equal(t, "title", resp.Field)
	assert.Len(t, resp.Entries, 2)
	assert.Equal(t, int64(200), resp.Entries[0].HLC.Timestamp)
}

func TestHandleFieldHistory_RespectsMaxDepth(t *testing.T) {
	exec := &mockExecutor{
		queryFn: func(_ context.Context, _ string, args ...any) (Rows, error) {
			// The last arg is the limit. Verify it was clamped.
			if len(args) >= 5 {
				limit := args[4].(int)
				assert.Equal(t, 25, limit)
			}
			return &mockRows{}, nil
		},
	}

	plugin := New(WithNodeID("test"))
	plugin.SetExecutor(exec)
	ctrl := NewSyncController(plugin,
		WithTimeTravelEnabled(true),
		WithTimeTravelMaxDepth(25),
	)

	_, err := ctrl.HandleFieldHistory(context.Background(), &FieldHistoryRequest{
		Table: "docs",
		PK:    "1",
		Field: "title",
		Limit: 100, // higher than max depth
	})
	require.NoError(t, err)
}

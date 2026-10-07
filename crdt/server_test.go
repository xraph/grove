package crdt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Executor for testing ---

type mockExecutor struct {
	execFn  func(ctx context.Context, query string, args ...any) (ExecResult, error)
	queryFn func(ctx context.Context, query string, args ...any) (Rows, error)
}

func (m *mockExecutor) ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error) {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return &mockExecResult{}, nil
}

func (m *mockExecutor) QueryContext(ctx context.Context, query string, args ...any) (Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return &mockRows{}, nil
}

type mockExecResult struct{ affected int64 }

func (r *mockExecResult) RowsAffected() (int64, error) { return r.affected, nil }

// mockRows returns no rows by default.
type mockRows struct {
	rows    []mockRow
	index   int
	closed  bool
	scanErr error
}

type mockRow struct {
	pkHash    string
	fieldName string
	hlcTS     int64
	hlcCount  uint32
	nodeID    string
	tombstone bool
	crdtState json.RawMessage
}

func (r *mockRows) Next() bool {
	if r.index >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

func (r *mockRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.rows[r.index-1]
	if len(dest) >= 7 {
		*dest[0].(*string) = row.pkHash
		*dest[1].(*string) = row.fieldName
		*dest[2].(*int64) = row.hlcTS
		*dest[3].(*uint32) = row.hlcCount
		*dest[4].(*string) = row.nodeID
		*dest[5].(*bool) = row.tombstone
		*dest[6].(*json.RawMessage) = row.crdtState
	}
	return nil
}

func (r *mockRows) Close() error { r.closed = true; return nil }
func (r *mockRows) Err() error   { return nil }

// --- Helper to create a test plugin ---

func newTestPlugin() *Plugin {
	p := New(WithNodeID("test-node"))
	exec := &mockExecutor{}
	p.SetExecutor(exec)
	return p
}

func newTestPluginWithChanges(changes []mockRow) *Plugin {
	p := New(WithNodeID("test-node"))
	exec := &mockExecutor{
		queryFn: func(_ context.Context, _ string, _ ...any) (Rows, error) {
			return &mockRows{rows: changes}, nil
		},
		execFn: func(_ context.Context, _ string, _ ...any) (ExecResult, error) {
			return &mockExecResult{affected: 1}, nil
		},
	}
	p.SetExecutor(exec)
	return p
}

// --- SyncController.HandlePull Tests ---

func TestSyncController_HandlePull_ReturnsChanges(t *testing.T) {
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

	ctrl := NewSyncController(plugin)
	resp, err := ctrl.HandlePull(context.Background(), &PullRequest{
		Tables: []string{"documents"},
		Since:  HLC{},
		NodeID: "remote-node",
	})

	require.NoError(t, err)
	assert.Len(t, resp.Changes, 1)
	assert.Equal(t, "doc-1", resp.Changes[0].PK)
	assert.Equal(t, "title", resp.Changes[0].Field)
}

func TestSyncController_HandlePull_EmptyWhenNoChanges(t *testing.T) {
	plugin := newTestPluginWithChanges(nil) // No rows.
	ctrl := NewSyncController(plugin)

	resp, err := ctrl.HandlePull(context.Background(), &PullRequest{
		Tables: []string{"documents"},
		Since:  HLC{Timestamp: 999},
		NodeID: "remote",
	})

	require.NoError(t, err)
	assert.Empty(t, resp.Changes)
}

func TestSyncController_HandlePull_BeforeOutboundReadHook(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"secret"`),
		NodeID: "node-a",
	})
	plugin := newTestPluginWithChanges([]mockRow{
		{pkHash: "1", fieldName: "title", hlcTS: 100, hlcCount: 1, nodeID: "a", crdtState: lwwState},
		{pkHash: "2", fieldName: "data", hlcTS: 200, hlcCount: 1, nodeID: "a", crdtState: lwwState},
	})

	// Add a hook that filters out changes with PK "2".
	ctrl := NewSyncController(plugin, WithControllerSyncHook(&pkFilterHook{blockedPK: "2"}))

	resp, err := ctrl.HandlePull(context.Background(), &PullRequest{
		Tables: []string{"test"},
		NodeID: "remote",
	})
	require.NoError(t, err)
	assert.Len(t, resp.Changes, 1)
	assert.Equal(t, "1", resp.Changes[0].PK)
}

func TestSyncController_HandlePull_NilMetadata(t *testing.T) {
	plugin := New(WithNodeID("test"))
	// Don't set executor — metadata is nil.
	ctrl := NewSyncController(plugin)

	_, err := ctrl.HandlePull(context.Background(), &PullRequest{
		Tables: []string{"t"},
		NodeID: "remote",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata store not initialized")
}

// --- SyncController.HandlePush Tests ---

func TestSyncController_HandlePush_MergesLWWChange(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)

	resp, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{
				Table:    "docs",
				PK:       "1",
				Field:    "title",
				CRDTType: TypeLWW,
				HLC:      HLC{Timestamp: 200, Counter: 0, NodeID: "remote"},
				NodeID:   "remote",
				Value:    json.RawMessage(`"new title"`),
			},
		},
		NodeID: "remote",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, resp.Merged)
}

func TestSyncController_HandlePush_TombstoneChange(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin)

	resp, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{
				Table:     "docs",
				PK:        "1",
				HLC:       HLC{Timestamp: 300, NodeID: "remote"},
				NodeID:    "remote",
				Tombstone: true,
			},
		},
		NodeID: "remote",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, resp.Merged)
}

func TestSyncController_HandlePush_BeforeInboundChangeHook_Skip(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	// Hook that skips changes from "evil-node".
	ctrl := NewSyncController(plugin, WithControllerSyncHook(&nodeFilterHook{blockedNode: "evil-node"}))

	resp, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{Table: "docs", PK: "1", NodeID: "evil-node", CRDTType: TypeLWW, HLC: HLC{Timestamp: 100, NodeID: "evil-node"}, Value: json.RawMessage(`"bad"`)},
			{Table: "docs", PK: "2", NodeID: "good-node", CRDTType: TypeLWW, HLC: HLC{Timestamp: 200, NodeID: "good-node"}, Field: "title", Value: json.RawMessage(`"good"`)},
		},
		NodeID: "mixed",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, resp.Merged) // Only good-node's change should be merged.
}

func TestSyncController_HandlePush_BeforeInboundChangeHook_Error(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin, WithControllerSyncHook(&errorInboundHook{}))

	_, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{Table: "docs", PK: "1", NodeID: "n", CRDTType: TypeLWW, HLC: HLC{Timestamp: 100, NodeID: "n"}, Value: json.RawMessage(`"v"`)},
		},
		NodeID: "n",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inbound change hook")
}

func TestSyncController_HandlePush_NilMetadata(t *testing.T) {
	plugin := New(WithNodeID("test"))
	ctrl := NewSyncController(plugin)

	_, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{{Table: "t", PK: "1", NodeID: "n"}},
		NodeID:  "n",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metadata store not initialized")
}

// --- SyncController.StreamChangesSince Tests ---

func TestSyncController_StreamChangesSince(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"streamed"`),
		NodeID: "node-a",
	})
	callCount := 0
	plugin := New(WithNodeID("test"))
	exec := &mockExecutor{
		queryFn: func(_ context.Context, _ string, _ ...any) (Rows, error) {
			callCount++
			if callCount == 1 {
				// First poll returns a change.
				return &mockRows{rows: []mockRow{
					{pkHash: "1", fieldName: "title", hlcTS: 100, hlcCount: 1, nodeID: "a", crdtState: lwwState},
				}}, nil
			}
			// Subsequent polls return nothing.
			return &mockRows{}, nil
		},
	}
	plugin.SetExecutor(exec)

	ctrl := NewSyncController(plugin, WithStreamPollInterval(50*time.Millisecond))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	ch, err := ctrl.StreamChangesSince(ctx, []string{"docs"}, HLC{})
	require.NoError(t, err)

	// Should receive at least one batch.
	select {
	case changes := <-ch:
		assert.NotEmpty(t, changes)
	case <-ctx.Done():
		t.Fatal("timeout waiting for streamed changes")
	}
}

func TestSyncController_StreamChangesSince_ContextCancel(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	ctrl := NewSyncController(plugin, WithStreamPollInterval(10*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := ctrl.StreamChangesSince(ctx, []string{"docs"}, HLC{})
	require.NoError(t, err)

	cancel()

	// Channel should close after context cancellation.
	select {
	case <-ch:
		// May get empty batch or closed — both fine.
	case <-time.After(1 * time.Second):
		t.Fatal("channel should close after context cancel")
	}
}

// --- HTTP Handler Backward Compatibility Tests ---

func TestNewHTTPHandler_Pull_BackwardCompat(t *testing.T) {
	lwwState, _ := json.Marshal(&FieldState{
		Type:   TypeLWW,
		Value:  json.RawMessage(`"hello"`),
		NodeID: "node-a",
	})
	plugin := newTestPluginWithChanges([]mockRow{
		{pkHash: "1", fieldName: "title", hlcTS: 100, hlcCount: 1, nodeID: "a", crdtState: lwwState},
	})

	handler := NewHTTPHandler(plugin)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody, _ := json.Marshal(PullRequest{
		Tables: []string{"test"},
		NodeID: "client",
	})

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/pull", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var pullResp PullResponse
	err = json.NewDecoder(resp.Body).Decode(&pullResp)
	require.NoError(t, err)
	assert.Len(t, pullResp.Changes, 1)
}

func TestNewHTTPHandler_Push_BackwardCompat(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	handler := NewHTTPHandler(plugin)
	server := httptest.NewServer(handler)
	defer server.Close()

	reqBody, _ := json.Marshal(PushRequest{
		Changes: []ChangeRecord{
			{
				Table:    "docs",
				PK:       "1",
				Field:    "title",
				CRDTType: TypeLWW,
				HLC:      HLC{Timestamp: 100, NodeID: "remote"},
				NodeID:   "remote",
				Value:    json.RawMessage(`"value"`),
			},
		},
		NodeID: "remote",
	})

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/push", bytes.NewReader(reqBody))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var pushResp PushResponse
	err = json.NewDecoder(resp.Body).Decode(&pushResp)
	require.NoError(t, err)
	assert.Equal(t, 1, pushResp.Merged)
}

func TestNewHTTPHandler_Pull_InvalidJSON(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	handler := NewHTTPHandler(plugin)
	server := httptest.NewServer(handler)
	defer server.Close()

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/pull", bytes.NewReader([]byte("invalid json")))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestNewHTTPHandler_Push_InvalidJSON(t *testing.T) {
	plugin := newTestPluginWithChanges(nil)
	handler := NewHTTPHandler(plugin)
	server := httptest.NewServer(handler)
	defer server.Close()

	httpReq, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/push", bytes.NewReader([]byte("{bad")))
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- SyncController Options Tests ---

func TestNewSyncController_DefaultOptions(t *testing.T) {
	plugin := newTestPlugin()
	ctrl := NewSyncController(plugin)

	assert.Equal(t, 1*time.Second, ctrl.streamPollInterval)
	assert.Equal(t, 15*time.Second, ctrl.streamKeepAlive)
	assert.NotNil(t, ctrl.hooks)
}

func TestNewSyncController_WithOptions(t *testing.T) {
	plugin := newTestPlugin()
	hook := &recordingSyncHook{}
	ctrl := NewSyncController(plugin,
		WithControllerSyncHook(hook),
		WithStreamPollInterval(5*time.Second),
		WithStreamKeepAlive(30*time.Second),
	)

	assert.Equal(t, 5*time.Second, ctrl.streamPollInterval)
	assert.Equal(t, 30*time.Second, ctrl.streamKeepAlive)
	// Hook should be in the chain.
	assert.GreaterOrEqual(t, ctrl.hooks.Len(), 1)
}

func TestNewSyncController_InheritsPluginHooks(t *testing.T) {
	plugin := New(
		WithNodeID("test"),
		WithSyncHook(&recordingSyncHook{}),
	)
	exec := &mockExecutor{}
	plugin.SetExecutor(exec)

	ctrl := NewSyncController(plugin)
	// Plugin hook + any controller hooks.
	assert.GreaterOrEqual(t, ctrl.hooks.Len(), 1)
}

// --- Test hook types ---

type pkFilterHook struct {
	BaseSyncHook
	blockedPK string
}

func (h *pkFilterHook) BeforeOutboundRead(_ context.Context, cs []ChangeRecord) ([]ChangeRecord, error) {
	var filtered []ChangeRecord
	for _, c := range cs {
		if c.PK != h.blockedPK {
			filtered = append(filtered, c)
		}
	}
	return filtered, nil
}

type nodeFilterHook struct {
	BaseSyncHook
	blockedNode string
}

func (h *nodeFilterHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	if c.NodeID == h.blockedNode {
		return nil, nil // Skip.
	}
	return c, nil
}

type errorInboundHook struct {
	BaseSyncHook
}

func (h *errorInboundHook) BeforeInboundChange(_ context.Context, _ *ChangeRecord) (*ChangeRecord, error) {
	return nil, errors.New("inbound rejected")
}

// --- Shadow-table fake for pull/push window tests ---

// shadowFake is a mockExecutor backend that honours the shadow-table
// queries the MetadataStore issues: the per-table changes-since cursor
// (including its LIMIT) and the field upsert. Tables are keyed by the
// user-facing table name.
type shadowFake struct {
	mu     sync.Mutex
	tables map[string][]mockRow
	writes []string // "table/pk/field" per field upsert, in order
}

func newShadowFake() *shadowFake {
	return &shadowFake{tables: make(map[string][]mockRow)}
}

func (f *shadowFake) plugin() *Plugin {
	p := New(WithNodeID("test-node"))
	p.SetExecutor(&mockExecutor{queryFn: f.query, execFn: f.exec})
	return p
}

func (f *shadowFake) tableOf(query string) string {
	for name := range f.tables {
		if strings.Contains(query, ShadowTableName(name)) {
			return name
		}
	}
	return ""
}

func (f *shadowFake) query(_ context.Context, query string, args ...any) (Rows, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.tables[f.tableOf(query)]
	if !strings.Contains(query, "hlc_ts > $1") {
		return &mockRows{}, nil // ReadState: start every push from empty state
	}
	ts, counter, limit := args[0].(int64), args[1].(uint32), args[2].(int)
	var out []mockRow
	for _, r := range rows {
		if r.hlcTS > ts || (r.hlcTS == ts && r.hlcCount > counter) {
			out = append(out, r)
		}
		if len(out) == limit {
			break
		}
	}
	return &mockRows{rows: out}, nil
}

func (f *shadowFake) exec(_ context.Context, query string, args ...any) (ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.Contains(query, "INSERT INTO") && len(args) >= 2 {
		f.writes = append(f.writes, fmt.Sprintf("%v/%v", args[0], args[1]))
	}
	return &mockExecResult{affected: 1}, nil
}

func lwwRows(n int, startTS int64) []mockRow {
	state, _ := json.Marshal(&FieldState{Type: TypeLWW, Value: json.RawMessage(`"v"`), NodeID: "a"})
	rows := make([]mockRow, n)
	for i := range rows {
		rows[i] = mockRow{
			pkHash: fmt.Sprintf("pk-%d", i), fieldName: "title",
			hlcTS: startTS + int64(i), nodeID: "a", crdtState: state,
		}
	}
	return rows
}

func TestSyncController_HandlePull_MultiTableBacklogNotSkipped(t *testing.T) {
	fake := newShadowFake()
	// "big" has a backlog larger than one page; "small" has one row newer
	// than all of it. A single max-over-tables watermark would jump past
	// big's second page.
	fake.tables["big"] = lwwRows(DefaultChangesLimit+500, 1_000)
	fake.tables["small"] = lwwRows(1, 1_000_000)
	ctrl := NewSyncController(fake.plugin())

	seen := map[string]bool{}
	since := HLC{}
	for range 5 {
		resp, err := ctrl.HandlePull(context.Background(), &PullRequest{
			Tables: []string{"big", "small"}, Since: since, NodeID: "client",
		})
		require.NoError(t, err)
		for _, c := range resp.Changes {
			seen[c.Table+"/"+c.PK] = true
		}
		if len(resp.Changes) == 0 {
			break
		}
		since = resp.LatestHLC
	}

	assert.Len(t, seen, DefaultChangesLimit+500+1, "every row of both tables must arrive across pulls")
}

func TestSyncController_HandlePush_HookRejectionMergesNothing(t *testing.T) {
	fake := newShadowFake()
	fake.tables["docs"] = nil
	hook := &pkRejectHook{rejectPK: "2"}
	ctrl := NewSyncController(fake.plugin(), WithControllerSyncHook(hook))

	_, err := ctrl.HandlePush(context.Background(), &PushRequest{
		Changes: []ChangeRecord{
			{Table: "docs", PK: "1", Field: "title", NodeID: "n", CRDTType: TypeLWW, HLC: HLC{Timestamp: 100, NodeID: "n"}, Value: json.RawMessage(`"ok"`)},
			{Table: "docs", PK: "2", Field: "title", NodeID: "n", CRDTType: TypeLWW, HLC: HLC{Timestamp: 101, NodeID: "n"}, Value: json.RawMessage(`"bad"`)},
		},
		NodeID: "n",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "inbound change hook")
	assert.Empty(t, fake.writes, "a rejected push must not merge the changes before the rejected one")
}

type pkRejectHook struct {
	BaseSyncHook
	rejectPK string
}

func (h *pkRejectHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	if c.PK == h.rejectPK {
		return nil, errors.New("pk not writable")
	}
	return c, nil
}

func TestSyncController_SubscribePresence_FansOutToEveryConsumer(t *testing.T) {
	ctrl := NewSyncController(newTestPlugin(), WithPresenceEnabled(true))
	defer ctrl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := ctrl.SubscribePresence(ctx)
	b := ctrl.SubscribePresence(ctx)

	_, err := ctrl.HandlePresenceUpdate(ctx, &PresenceUpdate{
		NodeID: "n1", Topic: "room", Data: json.RawMessage(`{"x":1}`),
	})
	require.NoError(t, err)

	for name, ch := range map[string]<-chan PresenceEvent{"a": a, "b": b} {
		select {
		case ev := <-ch:
			assert.Equal(t, "n1", ev.NodeID, name)
		case <-time.After(time.Second):
			t.Fatalf("consumer %s never received the presence event", name)
		}
	}
}

func TestNewHTTPHandler_Push_DeterministicRejectionIs422(t *testing.T) {
	ok := ChangeRecord{
		Table: "docs", PK: "1", Field: "title", CRDTType: TypeLWW, NodeID: "remote",
		HLC: HLC{Timestamp: time.Now().UnixNano(), NodeID: "remote"}, Value: json.RawMessage(`"v"`),
	}
	unknownType := ok
	unknownType.CRDTType = "nope"

	for _, tc := range []struct {
		name    string
		opts    []SyncControllerOption
		changes []ChangeRecord
		wantErr string
	}{
		{"hook rejection", []SyncControllerOption{WithControllerSyncHook(&errorInboundHook{})}, []ChangeRecord{ok}, "crdt: inbound change hook: inbound rejected"},
		{"validation", []SyncControllerOption{WithValidation(DefaultValidationConfig())}, []ChangeRecord{unknownType}, "crdt: change[0]: crdt: unknown crdt type: nope"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(NewHTTPHandler(newTestPluginWithChanges(nil), tc.opts...))
			defer server.Close()

			body, _ := json.Marshal(PushRequest{Changes: tc.changes, NodeID: "remote"})
			resp, err := http.Post(server.URL+"/push", "application/json", bytes.NewReader(body)) //nolint:noctx // test
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
			var errBody map[string]string
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&errBody))
			assert.Equal(t, tc.wantErr, errBody["error"])
		})
	}
}

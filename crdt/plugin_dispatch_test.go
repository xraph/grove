package crdt

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tracePlugin records which hooks the chain calls on it, in order.
type tracePlugin struct {
	BaseCRDTPlugin
	name    string
	mu      sync.Mutex
	calls   []string
	readErr error
	// redact replaces every field of a state read through
	// AfterMetadataRead or AfterHistoryRead.
	redact bool
}

func (p *tracePlugin) Name() string { return p.name }

func (p *tracePlugin) record(s string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, p.name+"."+s)
}

func (p *tracePlugin) BeforeMetadataRead(_ context.Context, ev *MetadataReadEvent) error {
	p.record("BeforeMetadataRead:" + ev.Table + "/" + ev.PK)

	return p.readErr
}

func (p *tracePlugin) AfterMetadataRead(_ context.Context, _ *MetadataReadEvent, s *State) (*State, error) {
	p.record("AfterMetadataRead")
	if p.readErr != nil {
		return nil, p.readErr
	}
	if p.redact {
		return &State{Table: s.Table, PK: s.PK, Fields: map[string]*FieldState{}}, nil
	}

	return s, nil
}

func (p *tracePlugin) AfterPresenceEvent(_ context.Context, ev *PresenceEvent) error {
	p.record("AfterPresenceEvent:" + ev.Type)

	return errors.New("ignored")
}

func (p *tracePlugin) AfterRoomJoin(_ context.Context, room, node string) error {
	p.record("AfterRoomJoin:" + room + ":" + node)

	return errors.New("ignored")
}

func (p *tracePlugin) AfterHistoryRead(_ context.Context, _, _ string, s *State) (*State, error) {
	p.record("AfterHistoryRead")
	if p.readErr != nil {
		return nil, p.readErr
	}
	if p.redact {
		return &State{Table: s.Table, PK: s.PK, Fields: map[string]*FieldState{}}, nil
	}

	return s, nil
}

func (p *tracePlugin) OnClientDisconnect(_ context.Context, node, transport string) error {
	p.record("OnClientDisconnect:" + node + ":" + transport)

	return errors.New("ignored")
}

func (p *tracePlugin) list() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.calls...)
}

// namedOnly implements no interceptor interface, so dispatch skips it.
type namedOnly struct{}

func (namedOnly) Name() string { return "named-only" }

func TestPluginChain_PluginsAndNilSafety(t *testing.T) {
	var nilChain *PluginChain
	ctx := t.Context()
	st := NewState("t", "p")

	assert.Nil(t, nilChain.Plugins())
	assert.Zero(t, nilChain.Len())
	require.NoError(t, nilChain.DispatchBeforeMetadataRead(ctx, &MetadataReadEvent{}))
	got, err := nilChain.DispatchAfterMetadataRead(ctx, &MetadataReadEvent{}, st)
	require.NoError(t, err)
	assert.Same(t, st, got)
	got, err = nilChain.DispatchAfterHistoryRead(ctx, "t", "p", st)
	require.NoError(t, err)
	assert.Same(t, st, got)
	nilChain.DispatchAfterPresenceEvent(ctx, &PresenceEvent{})
	nilChain.DispatchAfterRoomJoin(ctx, "r", "n")
	nilChain.DispatchOnClientDisconnect(ctx, "n", "ws")

	chain := NewPluginChain()
	a := &tracePlugin{name: "a"}
	chain.Add(a)
	chain.Add(nil) // ignored
	chain.Add(namedOnly{})
	require.Len(t, chain.Plugins(), 2)
	assert.Equal(t, "a", chain.Plugins()[0].Name())
}

func TestPluginChain_MetadataReadOrderAndTransform(t *testing.T) {
	chain := NewPluginChain()
	a := &tracePlugin{name: "a"}
	b := &tracePlugin{name: "b", redact: true}
	chain.Add(a)
	chain.Add(namedOnly{})
	chain.Add(b)
	ctx := t.Context()
	ev := &MetadataReadEvent{Table: "notes", PK: "1"}

	require.NoError(t, chain.DispatchBeforeMetadataRead(ctx, ev))

	st := NewState("notes", "1")
	st.Fields["secret"] = &FieldState{Type: TypeLWW}
	out, err := chain.DispatchAfterMetadataRead(ctx, ev, st)
	require.NoError(t, err)
	assert.Empty(t, out.Fields, "the second plugin's redaction is the result")

	assert.Equal(t, []string{"a.BeforeMetadataRead:notes/1", "a.AfterMetadataRead"}, a.list())
	assert.Equal(t, []string{"b.BeforeMetadataRead:notes/1", "b.AfterMetadataRead"}, b.list())
}

func TestPluginChain_MetadataReadErrorsStopChain(t *testing.T) {
	denied := errors.New("denied")
	chain := NewPluginChain()
	a := &tracePlugin{name: "a", readErr: denied}
	b := &tracePlugin{name: "b"}
	chain.Add(a)
	chain.Add(b)
	ctx := t.Context()

	require.ErrorIs(t, chain.DispatchBeforeMetadataRead(ctx, &MetadataReadEvent{}), denied)
	_, err := chain.DispatchAfterMetadataRead(ctx, &MetadataReadEvent{}, NewState("t", "p"))
	require.ErrorIs(t, err, denied)
	_, err = chain.DispatchAfterHistoryRead(ctx, "t", "p", NewState("t", "p"))
	require.ErrorIs(t, err, denied)
	assert.Empty(t, b.list(), "plugins after a failing one are not called")
}

func TestPluginChain_FireAndForgetHooksIgnoreErrors(t *testing.T) {
	chain := NewPluginChain()
	a := &tracePlugin{name: "a"}
	b := &tracePlugin{name: "b"}
	chain.Add(a)
	chain.Add(b)
	ctx := t.Context()

	chain.DispatchAfterPresenceEvent(ctx, &PresenceEvent{Type: PresenceJoin})
	chain.DispatchAfterRoomJoin(ctx, "room", "alice")
	chain.DispatchOnClientDisconnect(ctx, "alice", "ws")

	want := []string{"AfterPresenceEvent:" + string(PresenceJoin), "AfterRoomJoin:room:alice", "OnClientDisconnect:alice:ws"}
	for _, p := range []*tracePlugin{a, b} {
		got := p.list()
		require.Len(t, got, len(want))
		for i := range want {
			assert.Equal(t, p.name+"."+want[i], got[i])
		}
	}
}

// --- SyncController accessors and wiring ---

func TestSyncController_Accessors(t *testing.T) {
	p := newTestPlugin()
	plug := &tracePlugin{name: "x"}

	plain := NewSyncController(p)
	assert.Nil(t, plain.Rooms())
	assert.False(t, plain.TimeTravelEnabled())
	assert.Nil(t, plain.Metrics())
	assert.NotNil(t, plain.Logger())
	assert.Zero(t, plain.PluginChain().Len())

	full := NewSyncController(p,
		WithRoomManager(true),
		WithTimeTravelEnabled(true),
		WithMetrics(),
		WithControllerPlugin(plug),
		WithPresenceBufferSize(4),
	)
	defer full.Close()

	require.NotNil(t, full.Rooms(), "rooms turn presence on")
	assert.NotNil(t, full.Presence())
	assert.NotNil(t, full.Rooms().presence, "the placeholder is replaced by a real manager")
	assert.True(t, full.TimeTravelEnabled())
	assert.NotNil(t, full.Metrics())
	require.Equal(t, 1, full.PluginChain().Len())
	assert.Same(t, plug, full.PluginChain().Plugins()[0])
	assert.Equal(t, 4, full.presenceBufferSize)

	// WithRoomManager(false) leaves rooms and presence off.
	off := NewSyncController(p, WithRoomManager(false))
	assert.Nil(t, off.Rooms())
	assert.Nil(t, off.Presence())
}

func TestApplySyncFilter(t *testing.T) {
	changes := []ChangeRecord{
		{PK: "1", Field: "title"},
		{PK: "1", Field: "body"},
		{PK: "2", Field: "title"},
		{PK: "1", Field: ""}, // record-level (tombstone) change
	}

	assert.Equal(t, changes, applySyncFilter(changes, nil))

	byPK := applySyncFilter(changes, &SyncFilter{PKFilter: []string{"1"}})
	assert.Len(t, byPK, 3)
	for _, c := range byPK {
		assert.Equal(t, "1", c.PK)
	}

	byField := applySyncFilter(changes, &SyncFilter{FieldFilter: []string{"title"}})
	require.Len(t, byField, 3, "record-level changes pass a field filter")
	assert.Equal(t, "title", byField[0].Field)
	assert.Equal(t, "", byField[2].Field)

	both := applySyncFilter(changes, &SyncFilter{PKFilter: []string{"2"}, FieldFilter: []string{"title"}})
	require.Len(t, both, 1)
	assert.Equal(t, "2", both[0].PK)
}

func TestHandlePull_AppliesFilter(t *testing.T) {
	p, _ := newMemPlugin(t)
	ms := p.MetadataStore()
	ctx := t.Context()
	for _, pk := range []string{"a", "b"} {
		require.NoError(t, ms.WriteFieldState(ctx, "notes", pk, "title",
			&FieldState{Type: TypeLWW, HLC: HLC{Timestamp: 5, NodeID: "w"}, NodeID: "w", Value: []byte(`"x"`)}))
	}

	ctrl := NewSyncController(p, WithMetrics())
	resp, err := ctrl.HandlePull(ctx, &PullRequest{Tables: []string{"notes"}, Filter: &SyncFilter{PKFilter: []string{"b"}}})
	require.NoError(t, err)
	require.Len(t, resp.Changes, 1)
	assert.Equal(t, "b", resp.Changes[0].PK)
	assert.Equal(t, int64(1), ctrl.Metrics().PullCount.Load())
	assert.Equal(t, int64(1), ctrl.Metrics().ChangesPulled.Load())
}

func TestNewHTTPHandler_RegistersOptionalRoutes(t *testing.T) {
	p, _ := newMemPlugin(t)

	call := func(h http.Handler, method, path, body string) int {
		var req *http.Request
		if body == "" {
			req = httptest.NewRequestWithContext(t.Context(), method, path, nil)
		} else {
			req = httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		return rec.Code
	}

	bare := NewHTTPHandler(p)
	assert.Equal(t, http.StatusNotFound, call(bare, http.MethodGet, "/presence?topic=x", ""))
	assert.Equal(t, http.StatusNotFound, call(bare, http.MethodGet, "/history?table=notes&pk=1", ""))
	assert.Equal(t, http.StatusNotFound, call(bare, http.MethodGet, "/rooms", ""))

	full := NewHTTPHandler(p, WithRoomManager(true), WithTimeTravelEnabled(true))
	assert.Equal(t, http.StatusOK, call(full, http.MethodGet, "/presence?topic=x", ""))
	assert.Equal(t, http.StatusBadRequest, call(full, http.MethodGet, "/presence", ""))
	assert.Equal(t, http.StatusOK, call(full, http.MethodPost, "/presence", `{"node_id":"n","topic":"x","data":{"a":1}}`))
	assert.Equal(t, http.StatusBadRequest, call(full, http.MethodPost, "/presence", `{`))
	assert.Equal(t, http.StatusOK, call(full, http.MethodGet, "/history?table=notes&pk=1", ""))
	assert.Equal(t, http.StatusOK, call(full, http.MethodGet, "/rooms", ""))
	assert.Equal(t, http.StatusOK, call(full, http.MethodPost, "/pull", `{"tables":["notes"]}`))
	assert.Equal(t, http.StatusBadRequest, call(full, http.MethodPost, "/pull", `{`))
	assert.Equal(t, http.StatusBadRequest, call(full, http.MethodPost, "/push", `{`))
}

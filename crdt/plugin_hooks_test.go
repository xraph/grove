package crdt

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock plugins for testing ---

type mockMergePlugin struct {
	BaseCRDTPlugin
	calls       int
	rejectField string // if set, BeforeMerge returns nil for this field
	errOnBefore error
}

func (p *mockMergePlugin) Name() string { return "mock-merge" }

func (p *mockMergePlugin) BeforeMerge(_ context.Context, ev *MergeEvent) (*FieldState, error) {
	if p.errOnBefore != nil {
		return nil, p.errOnBefore
	}
	if p.rejectField != "" && ev.Field == p.rejectField {
		return nil, nil // skip
	}
	return ev.Remote, nil
}

func (p *mockMergePlugin) AfterMerge(_ context.Context, _ *MergeEvent) error {
	p.calls++
	return nil
}

type mockMetadataPlugin struct {
	BaseCRDTPlugin
	transformValue json.RawMessage // if non-nil, sets on BeforeMetadataWrite
	skipWrite      bool
}

func (p *mockMetadataPlugin) Name() string { return "mock-metadata" }

func (p *mockMetadataPlugin) BeforeMetadataWrite(_ context.Context, ev *MetadataWriteEvent) (*FieldState, error) {
	if p.skipWrite {
		return nil, nil
	}
	if p.transformValue != nil {
		ev.State.Value = p.transformValue
	}
	return ev.State, nil
}

func (p *mockMetadataPlugin) AfterMetadataWrite(_ context.Context, _ *MetadataWriteEvent) error {
	return nil
}

func (p *mockMetadataPlugin) BeforeMetadataRead(_ context.Context, _ *MetadataReadEvent) error {
	return nil
}

func (p *mockMetadataPlugin) AfterMetadataRead(_ context.Context, _ *MetadataReadEvent, s *State) (*State, error) {
	return s, nil
}

type mockPresencePlugin struct {
	BaseCRDTPlugin
	rejectUpdate bool
}

func (p *mockPresencePlugin) Name() string { return "mock-presence" }

func (p *mockPresencePlugin) BeforePresenceUpdate(_ context.Context, u *PresenceUpdate) (*PresenceUpdate, error) {
	if p.rejectUpdate {
		return nil, nil
	}
	return u, nil
}

func (p *mockPresencePlugin) AfterPresenceEvent(_ context.Context, _ *PresenceEvent) error {
	return nil
}

type mockRoomPlugin struct {
	BaseCRDTPlugin
	denyJoin bool
}

func (p *mockRoomPlugin) Name() string { return "mock-room" }

func (p *mockRoomPlugin) BeforeRoomJoin(_ context.Context, _, _ string) error {
	if p.denyJoin {
		return errors.New("join denied")
	}
	return nil
}

func (p *mockRoomPlugin) AfterRoomJoin(_ context.Context, _, _ string) error   { return nil }
func (p *mockRoomPlugin) BeforeRoomLeave(_ context.Context, _, _ string) error { return nil }
func (p *mockRoomPlugin) AfterRoomLeave(_ context.Context, _, _ string) error  { return nil }
func (p *mockRoomPlugin) OnRoomCreated(_ context.Context, _ *Room) error       { return nil }
func (p *mockRoomPlugin) OnRoomDestroyed(_ context.Context, _ *Room) error     { return nil }

type mockTimeTravelPlugin struct {
	BaseCRDTPlugin
	denyHistory bool
}

func (p *mockTimeTravelPlugin) Name() string { return "mock-timetravel" }

func (p *mockTimeTravelPlugin) BeforeHistoryRead(_ context.Context, _, _ string, _ HLC) error {
	if p.denyHistory {
		return errors.New("history access denied")
	}
	return nil
}

func (p *mockTimeTravelPlugin) AfterHistoryRead(_ context.Context, _, _ string, s *State) (*State, error) {
	return s, nil
}

type mockConnectionPlugin struct {
	BaseCRDTPlugin
	blockConnect bool
}

func (p *mockConnectionPlugin) Name() string { return "mock-connection" }

func (p *mockConnectionPlugin) OnClientConnect(_ context.Context, _, _ string) error {
	if p.blockConnect {
		return errors.New("connection blocked")
	}
	return nil
}

func (p *mockConnectionPlugin) OnClientDisconnect(_ context.Context, _, _ string) error {
	return nil
}

// barePlugin doesn't implement any interceptor interfaces beyond CRDTPlugin.Name().
type barePlugin struct{}

func (p *barePlugin) Name() string { return "bare" }

// --- PluginChain Tests ---

func TestPluginChain_Add(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockMergePlugin{})
	pc.Add(&mockMergePlugin{})
	assert.Equal(t, 2, pc.Len())
}

func TestPluginChain_Add_NilIgnored(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(nil)
	assert.Equal(t, 0, pc.Len())
}

func TestPluginChain_Len_NilChain(t *testing.T) {
	var pc *PluginChain
	assert.Equal(t, 0, pc.Len())
}

func TestPluginChain_DispatchBeforeMerge_CallsInOrder(t *testing.T) {
	pc := NewPluginChain()

	// First plugin transforms value
	p1 := &mockMergePlugin{}
	p2 := &mockMergePlugin{}
	pc.Add(p1)
	pc.Add(p2)

	ev := &MergeEvent{
		Field:  "title",
		Remote: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"hello"`)},
	}
	result, err := pc.DispatchBeforeMerge(context.Background(), ev)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestPluginChain_DispatchBeforeMerge_NilReturnsSkip(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockMergePlugin{rejectField: "title"})

	ev := &MergeEvent{
		Field:  "title",
		Remote: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"hello"`)},
	}
	result, err := pc.DispatchBeforeMerge(context.Background(), ev)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestPluginChain_DispatchBeforeMerge_ErrorAbortsChain(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockMergePlugin{errOnBefore: errors.New("merge rejected")})
	pc.Add(&mockMergePlugin{}) // should not be reached

	ev := &MergeEvent{
		Field:  "title",
		Remote: &FieldState{Type: TypeLWW},
	}
	_, err := pc.DispatchBeforeMerge(context.Background(), ev)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "merge rejected")
}

func TestPluginChain_DispatchBeforeMerge_NonMergePluginSkipped(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&barePlugin{}) // doesn't implement MergeInterceptor
	pc.Add(&mockMergePlugin{})

	ev := &MergeEvent{
		Field:  "title",
		Remote: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"v"`)},
	}
	result, err := pc.DispatchBeforeMerge(context.Background(), ev)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestPluginChain_DispatchBeforeMerge_NilChain(t *testing.T) {
	var pc *PluginChain
	ev := &MergeEvent{
		Remote: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"v"`)},
	}
	result, err := pc.DispatchBeforeMerge(context.Background(), ev)
	require.NoError(t, err)
	assert.NotNil(t, result) // returns ev.Remote as-is
}

func TestPluginChain_DispatchAfterMerge(t *testing.T) {
	pc := NewPluginChain()
	p1 := &mockMergePlugin{}
	p2 := &mockMergePlugin{}
	pc.Add(p1)
	pc.Add(p2)

	ev := &MergeEvent{Field: "title", Result: &FieldState{Type: TypeLWW}}
	pc.DispatchAfterMerge(context.Background(), ev)

	assert.Equal(t, 1, p1.calls)
	assert.Equal(t, 1, p2.calls)
}

func TestPluginChain_DispatchAfterMerge_NilChain(_ *testing.T) {
	var pc *PluginChain
	// Should not panic.
	pc.DispatchAfterMerge(context.Background(), &MergeEvent{})
}

func TestPluginChain_DispatchBeforeMetadataWrite_TransformsState(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockMetadataPlugin{transformValue: json.RawMessage(`"transformed"`)})

	ev := &MetadataWriteEvent{
		State: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"original"`)},
	}
	result, err := pc.DispatchBeforeMetadataWrite(context.Background(), ev)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, json.RawMessage(`"transformed"`), result.Value)
}

func TestPluginChain_DispatchBeforeMetadataWrite_NilSkipsWrite(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockMetadataPlugin{skipWrite: true})

	ev := &MetadataWriteEvent{
		State: &FieldState{Type: TypeLWW, Value: json.RawMessage(`"v"`)},
	}
	result, err := pc.DispatchBeforeMetadataWrite(context.Background(), ev)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestPluginChain_DispatchBeforePresenceUpdate_Transforms(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockPresencePlugin{})

	update := &PresenceUpdate{NodeID: "n", Topic: "t", Data: json.RawMessage(`{}`)}
	result, err := pc.DispatchBeforePresenceUpdate(context.Background(), update)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "n", result.NodeID)
}

func TestPluginChain_DispatchBeforePresenceUpdate_NilRejects(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockPresencePlugin{rejectUpdate: true})

	update := &PresenceUpdate{NodeID: "n", Topic: "t"}
	result, err := pc.DispatchBeforePresenceUpdate(context.Background(), update)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestPluginChain_DispatchBeforeRoomJoin_ErrorDeniesJoin(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockRoomPlugin{denyJoin: true})

	err := pc.DispatchBeforeRoomJoin(context.Background(), "room-1", "node-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "join denied")
}

func TestPluginChain_DispatchBeforeRoomJoin_OK(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockRoomPlugin{denyJoin: false})
	assert.NoError(t, pc.DispatchBeforeRoomJoin(context.Background(), "room-1", "node-1"))
}

func TestPluginChain_DispatchBeforeHistoryRead_ErrorDenies(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockTimeTravelPlugin{denyHistory: true})

	err := pc.DispatchBeforeHistoryRead(context.Background(), "docs", "1", HLC{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "history access denied")
}

func TestPluginChain_DispatchBeforeHistoryRead_OK(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockTimeTravelPlugin{denyHistory: false})
	assert.NoError(t, pc.DispatchBeforeHistoryRead(context.Background(), "docs", "1", HLC{}))
}

func TestPluginChain_DispatchOnClientConnect_ErrorBlocks(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockConnectionPlugin{blockConnect: true})

	err := pc.DispatchOnClientConnect(context.Background(), "node-1", "websocket")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection blocked")
}

func TestPluginChain_DispatchOnClientConnect_OK(t *testing.T) {
	pc := NewPluginChain()
	pc.Add(&mockConnectionPlugin{blockConnect: false})
	assert.NoError(t, pc.DispatchOnClientConnect(context.Background(), "node-1", "websocket"))
}

func TestPluginChain_DispatchOnClientConnect_NilChain(t *testing.T) {
	var pc *PluginChain
	assert.NoError(t, pc.DispatchOnClientConnect(context.Background(), "node-1", "ws"))
}

func TestBaseCRDTPlugin_Defaults(t *testing.T) {
	var base BaseCRDTPlugin

	assert.Equal(t, "base", base.Name())

	ctx := context.Background()

	// MergeInterceptor
	ev := &MergeEvent{Remote: &FieldState{Type: TypeLWW}}
	result, err := base.BeforeMerge(ctx, ev)
	assert.NoError(t, err)
	assert.Equal(t, ev.Remote, result)
	assert.NoError(t, base.AfterMerge(ctx, ev))

	// MetadataInterceptor
	mev := &MetadataWriteEvent{State: &FieldState{Type: TypeLWW}}
	mResult, err := base.BeforeMetadataWrite(ctx, mev)
	assert.NoError(t, err)
	assert.Equal(t, mev.State, mResult)
	assert.NoError(t, base.AfterMetadataWrite(ctx, mev))
	assert.NoError(t, base.BeforeMetadataRead(ctx, &MetadataReadEvent{}))
	s := NewState("t", "1")
	sResult, err := base.AfterMetadataRead(ctx, &MetadataReadEvent{}, s)
	assert.NoError(t, err)
	assert.Equal(t, s, sResult)

	// PresenceInterceptor
	u := &PresenceUpdate{NodeID: "n"}
	uResult, err := base.BeforePresenceUpdate(ctx, u)
	assert.NoError(t, err)
	assert.Equal(t, u, uResult)
	assert.NoError(t, base.AfterPresenceEvent(ctx, &PresenceEvent{}))

	// RoomInterceptor
	assert.NoError(t, base.BeforeRoomJoin(ctx, "r", "n"))
	assert.NoError(t, base.AfterRoomJoin(ctx, "r", "n"))
	assert.NoError(t, base.BeforeRoomLeave(ctx, "r", "n"))
	assert.NoError(t, base.AfterRoomLeave(ctx, "r", "n"))
	assert.NoError(t, base.OnRoomCreated(ctx, &Room{}))
	assert.NoError(t, base.OnRoomDestroyed(ctx, &Room{}))

	// TimeTravelInterceptor
	assert.NoError(t, base.BeforeHistoryRead(ctx, "t", "p", HLC{}))
	ttResult, err := base.AfterHistoryRead(ctx, "t", "p", s)
	assert.NoError(t, err)
	assert.Equal(t, s, ttResult)

	// ConnectionInterceptor
	assert.NoError(t, base.OnClientConnect(ctx, "n", "ws"))
	assert.NoError(t, base.OnClientDisconnect(ctx, "n", "ws"))
}

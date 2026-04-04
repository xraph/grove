package crdt

import "context"

// CRDTPlugin is the interface for extending the CRDT server with custom logic.
// Implement any subset of the specialized interfaces below to hook into
// specific parts of the CRDT lifecycle. Use [BaseCRDTPlugin] for no-op defaults.
//
// Plugins are registered via [SyncController.AddPlugin] or
// [WithControllerPlugin] option and are called in registration order.
//
// Example:
//
//	type AuditPlugin struct { crdt.BaseCRDTPlugin }
//
//	func (p *AuditPlugin) Name() string { return "audit" }
//
//	func (p *AuditPlugin) AfterMerge(ctx context.Context, ev *MergeEvent) error {
//	    log.Printf("merged %s/%s field=%s winner=%s", ev.Table, ev.PK, ev.Field, ev.WinnerNodeID)
//	    return nil
//	}
type CRDTPlugin interface { //nolint:revive // CRDTPlugin is the established public API name
	// Name returns a unique identifier for this plugin.
	Name() string
}

// --- Specialized Plugin Interfaces ---
// Implement any combination of these interfaces on your CRDTPlugin.

// MergeInterceptor intercepts merge operations, allowing custom merge
// logic, validation, or auditing of conflict resolution decisions.
type MergeInterceptor interface {
	// BeforeMerge is called before two field states are merged.
	// Return a modified remote state, nil to skip the merge, or an error to abort.
	BeforeMerge(ctx context.Context, ev *MergeEvent) (*FieldState, error)

	// AfterMerge is called after a merge completes with the winning state.
	// Use for audit logging, analytics, or triggering side effects.
	AfterMerge(ctx context.Context, ev *MergeEvent) error
}

// MergeEvent provides context about a merge operation.
type MergeEvent struct {
	Table  string      `json:"table"`
	PK     string      `json:"pk"`
	Field  string      `json:"field"`
	Local  *FieldState `json:"local"`
	Remote *FieldState `json:"remote"`
	Result *FieldState `json:"result"` // Set after merge (in AfterMerge).

	// WinnerNodeID is set in AfterMerge — the node whose value won.
	WinnerNodeID string `json:"winner_node_id,omitempty"`

	// ConflictDetected is true when both local and remote had changes.
	ConflictDetected bool `json:"conflict_detected"`
}

// MetadataInterceptor intercepts reads and writes to the shadow table.
type MetadataInterceptor interface {
	// BeforeMetadataWrite is called before writing field state to the shadow table.
	// Return a modified state, nil to skip the write, or an error to abort.
	BeforeMetadataWrite(ctx context.Context, ev *MetadataWriteEvent) (*FieldState, error)

	// AfterMetadataWrite is called after a successful shadow table write.
	AfterMetadataWrite(ctx context.Context, ev *MetadataWriteEvent) error

	// BeforeMetadataRead is called before reading state from the shadow table.
	// Return an error to deny the read.
	BeforeMetadataRead(ctx context.Context, ev *MetadataReadEvent) error

	// AfterMetadataRead is called after reading state, allowing transformation.
	// Return a modified state or the original.
	AfterMetadataRead(ctx context.Context, ev *MetadataReadEvent, state *State) (*State, error)
}

// MetadataWriteEvent describes a pending shadow table write.
type MetadataWriteEvent struct {
	Table  string      `json:"table"`
	PK     string      `json:"pk"`
	Field  string      `json:"field"`
	State  *FieldState `json:"state"`
	NodeID string      `json:"node_id"`
}

// MetadataReadEvent describes a pending shadow table read.
type MetadataReadEvent struct {
	Table string `json:"table"`
	PK    string `json:"pk"`
	// AtHLC is set for time-travel reads (zero for current state).
	AtHLC HLC `json:"at_hlc,omitempty"`
}

// PresenceInterceptor intercepts presence events for custom logic.
type PresenceInterceptor interface {
	// BeforePresenceUpdate is called before updating presence.
	// Return a modified update, nil to reject, or an error to abort.
	BeforePresenceUpdate(ctx context.Context, update *PresenceUpdate) (*PresenceUpdate, error)

	// AfterPresenceEvent is called after a presence event is emitted.
	AfterPresenceEvent(ctx context.Context, event *PresenceEvent) error
}

// RoomInterceptor intercepts room lifecycle events.
type RoomInterceptor interface {
	// BeforeRoomJoin is called before a participant joins a room.
	// Return an error to deny the join (e.g., access control).
	BeforeRoomJoin(ctx context.Context, roomID, nodeID string) error

	// AfterRoomJoin is called after a participant has joined.
	AfterRoomJoin(ctx context.Context, roomID, nodeID string) error

	// BeforeRoomLeave is called before a participant leaves.
	BeforeRoomLeave(ctx context.Context, roomID, nodeID string) error

	// AfterRoomLeave is called after a participant has left.
	AfterRoomLeave(ctx context.Context, roomID, nodeID string) error

	// OnRoomCreated is called when a new room is created.
	OnRoomCreated(ctx context.Context, room *Room) error

	// OnRoomDestroyed is called when a room is destroyed (last participant leaves).
	OnRoomDestroyed(ctx context.Context, room *Room) error
}

// TimeTravelInterceptor intercepts time-travel queries.
type TimeTravelInterceptor interface {
	// BeforeHistoryRead is called before reading historical state.
	// Return an error to deny the read (e.g., access control on history).
	BeforeHistoryRead(ctx context.Context, table, pk string, atHLC HLC) error

	// AfterHistoryRead is called after reading historical state.
	// Return a modified state for redaction or transformation.
	AfterHistoryRead(ctx context.Context, table, pk string, state *State) (*State, error)
}

// ConnectionInterceptor intercepts client connections (WebSocket, SSE).
type ConnectionInterceptor interface {
	// OnClientConnect is called when a client connects via SSE or WebSocket.
	OnClientConnect(ctx context.Context, nodeID string, transport string) error

	// OnClientDisconnect is called when a client disconnects.
	OnClientDisconnect(ctx context.Context, nodeID string, transport string) error
}

// --- Base Plugin ---

// BaseCRDTPlugin provides no-op defaults for all plugin interfaces.
// Embed this in your plugin struct and override only what you need.
type BaseCRDTPlugin struct{}

func (BaseCRDTPlugin) Name() string { return "base" }

// MergeInterceptor no-ops.
func (BaseCRDTPlugin) BeforeMerge(_ context.Context, ev *MergeEvent) (*FieldState, error) {
	return ev.Remote, nil
}
func (BaseCRDTPlugin) AfterMerge(_ context.Context, _ *MergeEvent) error { return nil }

// MetadataInterceptor no-ops.
func (BaseCRDTPlugin) BeforeMetadataWrite(_ context.Context, ev *MetadataWriteEvent) (*FieldState, error) {
	return ev.State, nil
}
func (BaseCRDTPlugin) AfterMetadataWrite(_ context.Context, _ *MetadataWriteEvent) error {
	return nil
}
func (BaseCRDTPlugin) BeforeMetadataRead(_ context.Context, _ *MetadataReadEvent) error {
	return nil
}
func (BaseCRDTPlugin) AfterMetadataRead(_ context.Context, _ *MetadataReadEvent, s *State) (*State, error) {
	return s, nil
}

// PresenceInterceptor no-ops.
func (BaseCRDTPlugin) BeforePresenceUpdate(_ context.Context, u *PresenceUpdate) (*PresenceUpdate, error) {
	return u, nil
}
func (BaseCRDTPlugin) AfterPresenceEvent(_ context.Context, _ *PresenceEvent) error { return nil }

// RoomInterceptor no-ops.
func (BaseCRDTPlugin) BeforeRoomJoin(_ context.Context, _, _ string) error  { return nil }
func (BaseCRDTPlugin) AfterRoomJoin(_ context.Context, _, _ string) error   { return nil }
func (BaseCRDTPlugin) BeforeRoomLeave(_ context.Context, _, _ string) error { return nil }
func (BaseCRDTPlugin) AfterRoomLeave(_ context.Context, _, _ string) error  { return nil }
func (BaseCRDTPlugin) OnRoomCreated(_ context.Context, _ *Room) error       { return nil }
func (BaseCRDTPlugin) OnRoomDestroyed(_ context.Context, _ *Room) error     { return nil }

// TimeTravelInterceptor no-ops.
func (BaseCRDTPlugin) BeforeHistoryRead(_ context.Context, _, _ string, _ HLC) error { return nil }
func (BaseCRDTPlugin) AfterHistoryRead(_ context.Context, _, _ string, s *State) (*State, error) {
	return s, nil
}

// ConnectionInterceptor no-ops.
func (BaseCRDTPlugin) OnClientConnect(_ context.Context, _, _ string) error    { return nil }
func (BaseCRDTPlugin) OnClientDisconnect(_ context.Context, _, _ string) error { return nil }

// --- Plugin Chain ---

// PluginChain manages registered CRDT plugins and dispatches events.
type PluginChain struct {
	plugins []CRDTPlugin
}

// NewPluginChain creates a new plugin chain.
func NewPluginChain() *PluginChain {
	return &PluginChain{}
}

// Add registers a plugin.
func (pc *PluginChain) Add(p CRDTPlugin) {
	if p != nil {
		pc.plugins = append(pc.plugins, p)
	}
}

// Len returns the number of registered plugins.
func (pc *PluginChain) Len() int {
	if pc == nil {
		return 0
	}
	return len(pc.plugins)
}

// Plugins returns all registered plugins.
func (pc *PluginChain) Plugins() []CRDTPlugin {
	if pc == nil {
		return nil
	}
	return pc.plugins
}

// --- Dispatch Methods ---

// DispatchBeforeMerge calls BeforeMerge on all MergeInterceptor plugins.
func (pc *PluginChain) DispatchBeforeMerge(ctx context.Context, ev *MergeEvent) (*FieldState, error) {
	if pc == nil {
		return ev.Remote, nil
	}
	current := ev.Remote
	for _, p := range pc.plugins {
		mi, ok := p.(MergeInterceptor)
		if !ok {
			continue
		}
		ev.Remote = current
		var err error
		current, err = mi.BeforeMerge(ctx, ev)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil
		}
	}
	return current, nil
}

// DispatchAfterMerge calls AfterMerge on all MergeInterceptor plugins.
func (pc *PluginChain) DispatchAfterMerge(ctx context.Context, ev *MergeEvent) {
	if pc == nil {
		return
	}
	for _, p := range pc.plugins {
		if mi, ok := p.(MergeInterceptor); ok {
			mi.AfterMerge(ctx, ev) //nolint:errcheck // fire-and-forget post-hook
		}
	}
}

// DispatchBeforeMetadataWrite calls BeforeMetadataWrite on all MetadataInterceptor plugins.
func (pc *PluginChain) DispatchBeforeMetadataWrite(ctx context.Context, ev *MetadataWriteEvent) (*FieldState, error) {
	if pc == nil {
		return ev.State, nil
	}
	current := ev.State
	for _, p := range pc.plugins {
		mi, ok := p.(MetadataInterceptor)
		if !ok {
			continue
		}
		ev.State = current
		var err error
		current, err = mi.BeforeMetadataWrite(ctx, ev)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil
		}
	}
	return current, nil
}

// DispatchAfterMetadataWrite calls AfterMetadataWrite on all MetadataInterceptor plugins.
func (pc *PluginChain) DispatchAfterMetadataWrite(ctx context.Context, ev *MetadataWriteEvent) {
	if pc == nil {
		return
	}
	for _, p := range pc.plugins {
		if mi, ok := p.(MetadataInterceptor); ok {
			mi.AfterMetadataWrite(ctx, ev) //nolint:errcheck // fire-and-forget post-hook
		}
	}
}

// DispatchBeforeMetadataRead calls BeforeMetadataRead on all MetadataInterceptor plugins.
func (pc *PluginChain) DispatchBeforeMetadataRead(ctx context.Context, ev *MetadataReadEvent) error {
	if pc == nil {
		return nil
	}
	for _, p := range pc.plugins {
		if mi, ok := p.(MetadataInterceptor); ok {
			if err := mi.BeforeMetadataRead(ctx, ev); err != nil {
				return err
			}
		}
	}
	return nil
}

// DispatchAfterMetadataRead calls AfterMetadataRead on all MetadataInterceptor plugins.
func (pc *PluginChain) DispatchAfterMetadataRead(ctx context.Context, ev *MetadataReadEvent, state *State) (*State, error) {
	if pc == nil {
		return state, nil
	}
	current := state
	for _, p := range pc.plugins {
		if mi, ok := p.(MetadataInterceptor); ok {
			var err error
			current, err = mi.AfterMetadataRead(ctx, ev, current)
			if err != nil {
				return nil, err
			}
		}
	}
	return current, nil
}

// DispatchBeforePresenceUpdate calls BeforePresenceUpdate on all PresenceInterceptor plugins.
func (pc *PluginChain) DispatchBeforePresenceUpdate(ctx context.Context, update *PresenceUpdate) (*PresenceUpdate, error) {
	if pc == nil {
		return update, nil
	}
	current := update
	for _, p := range pc.plugins {
		if pi, ok := p.(PresenceInterceptor); ok {
			var err error
			current, err = pi.BeforePresenceUpdate(ctx, current)
			if err != nil {
				return nil, err
			}
			if current == nil {
				return nil, nil
			}
		}
	}
	return current, nil
}

// DispatchAfterPresenceEvent calls AfterPresenceEvent on all PresenceInterceptor plugins.
func (pc *PluginChain) DispatchAfterPresenceEvent(ctx context.Context, event *PresenceEvent) {
	if pc == nil {
		return
	}
	for _, p := range pc.plugins {
		if pi, ok := p.(PresenceInterceptor); ok {
			pi.AfterPresenceEvent(ctx, event) //nolint:errcheck // fire-and-forget post-hook
		}
	}
}

// DispatchBeforeRoomJoin calls BeforeRoomJoin on all RoomInterceptor plugins.
func (pc *PluginChain) DispatchBeforeRoomJoin(ctx context.Context, roomID, nodeID string) error {
	if pc == nil {
		return nil
	}
	for _, p := range pc.plugins {
		if ri, ok := p.(RoomInterceptor); ok {
			if err := ri.BeforeRoomJoin(ctx, roomID, nodeID); err != nil {
				return err
			}
		}
	}
	return nil
}

// DispatchAfterRoomJoin calls AfterRoomJoin on all RoomInterceptor plugins.
func (pc *PluginChain) DispatchAfterRoomJoin(ctx context.Context, roomID, nodeID string) {
	if pc == nil {
		return
	}
	for _, p := range pc.plugins {
		if ri, ok := p.(RoomInterceptor); ok {
			ri.AfterRoomJoin(ctx, roomID, nodeID) //nolint:errcheck // fire-and-forget post-hook
		}
	}
}

// DispatchBeforeHistoryRead calls BeforeHistoryRead on all TimeTravelInterceptor plugins.
func (pc *PluginChain) DispatchBeforeHistoryRead(ctx context.Context, table, pk string, atHLC HLC) error {
	if pc == nil {
		return nil
	}
	for _, p := range pc.plugins {
		if ti, ok := p.(TimeTravelInterceptor); ok {
			if err := ti.BeforeHistoryRead(ctx, table, pk, atHLC); err != nil {
				return err
			}
		}
	}
	return nil
}

// DispatchAfterHistoryRead calls AfterHistoryRead on all TimeTravelInterceptor plugins.
func (pc *PluginChain) DispatchAfterHistoryRead(ctx context.Context, table, pk string, state *State) (*State, error) {
	if pc == nil {
		return state, nil
	}
	current := state
	for _, p := range pc.plugins {
		if ti, ok := p.(TimeTravelInterceptor); ok {
			var err error
			current, err = ti.AfterHistoryRead(ctx, table, pk, current)
			if err != nil {
				return nil, err
			}
		}
	}
	return current, nil
}

// DispatchOnClientConnect calls OnClientConnect on all ConnectionInterceptor plugins.
func (pc *PluginChain) DispatchOnClientConnect(ctx context.Context, nodeID, transport string) error {
	if pc == nil {
		return nil
	}
	for _, p := range pc.plugins {
		if ci, ok := p.(ConnectionInterceptor); ok {
			if err := ci.OnClientConnect(ctx, nodeID, transport); err != nil {
				return err
			}
		}
	}
	return nil
}

// DispatchOnClientDisconnect calls OnClientDisconnect on all ConnectionInterceptor plugins.
func (pc *PluginChain) DispatchOnClientDisconnect(ctx context.Context, nodeID, transport string) {
	if pc == nil {
		return
	}
	for _, p := range pc.plugins {
		if ci, ok := p.(ConnectionInterceptor); ok {
			ci.OnClientDisconnect(ctx, nodeID, transport) //nolint:errcheck // fire-and-forget post-hook
		}
	}
}

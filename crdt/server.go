package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	log "github.com/xraph/go-utils/log"
)

// SyncController handles CRDT sync operations. It provides handlers for
// pull, push, and streaming endpoints that can be registered with any
// router (Forge, net/http, chi, etc.).
//
// For Forge apps, use grove/extension.WithCRDT() to auto-register routes.
// For standalone use, call NewHTTPHandler() to get an http.Handler.
type SyncController struct {
	plugin             *Plugin
	metadata           *MetadataStore
	hooks              *SyncHookChain
	streamPollInterval time.Duration
	streamKeepAlive    time.Duration
	logger             log.Logger

	// Presence subsystem (nil when disabled).
	presenceEnabled bool
	presenceTTL     time.Duration
	presence        *PresenceManager
	presenceCh      chan PresenceEvent // buffered channel for broadcasting to SSE streams
}

// NewSyncController creates a new sync controller for the given plugin.
func NewSyncController(plugin *Plugin, opts ...SyncControllerOption) *SyncController {
	c := &SyncController{
		plugin:             plugin,
		metadata:           plugin.metadata,
		hooks:              NewSyncHookChain(),
		streamPollInterval: 1 * time.Second,
		streamKeepAlive:    15 * time.Second,
		logger:             log.NewNoopLogger(),
	}
	// Include plugin-level sync hooks.
	if plugin.syncHooks != nil {
		for _, h := range plugin.syncHooks.hooks {
			c.hooks.Add(h)
		}
	}
	for _, opt := range opts {
		opt(c)
	}

	// Initialize presence subsystem if enabled.
	if c.presenceEnabled {
		c.presenceCh = make(chan PresenceEvent, 64)
		c.presence = NewPresenceManager(c.presenceTTL, func(event PresenceEvent) {
			// Non-blocking send to the broadcast channel.
			select {
			case c.presenceCh <- event:
			default:
				c.logger.Warn("crdt: presence event dropped (channel full)",
					log.String("topic", event.Topic),
					log.String("node_id", event.NodeID),
				)
			}
		}, c.logger)
	}

	return c
}

// HandlePull processes a pull request and returns changes since the given HLC.
// This is the core logic used by both Forge and HTTP handlers.
func (c *SyncController) HandlePull(ctx context.Context, req *PullRequest) (*PullResponse, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	var allChanges []ChangeRecord
	var latestHLC HLC

	for _, table := range req.Tables {
		changes, err := c.metadata.ReadChangesSince(ctx, table, req.Since)
		if err != nil {
			return nil, fmt.Errorf("crdt: read changes for %s: %w", table, err)
		}
		allChanges = append(allChanges, changes...)

		for _, ch := range changes {
			if ch.HLC.After(latestHLC) {
				latestHLC = ch.HLC
			}
		}
	}

	// Update our clock with the remote node's timestamp.
	c.plugin.clock.Update(req.Since)

	// Run BeforeOutboundRead hook.
	filtered, err := c.hooks.BeforeOutboundRead(ctx, allChanges)
	if err != nil {
		return nil, fmt.Errorf("crdt: outbound read hook: %w", err)
	}

	return &PullResponse{
		Changes:   filtered,
		LatestHLC: latestHLC,
	}, nil
}

// HandlePush processes a push request, merging remote changes locally.
// This is the core logic used by both Forge and HTTP handlers.
func (c *SyncController) HandlePush(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	merged := 0

	for _, change := range req.Changes {
		// Update our clock with each incoming change.
		c.plugin.clock.Update(change.HLC)

		// Run BeforeInboundChange hook.
		processedChange, err := c.hooks.BeforeInboundChange(ctx, &change)
		if err != nil {
			return nil, fmt.Errorf("crdt: inbound change hook: %w", err)
		}
		if processedChange == nil {
			continue // Hook says skip this change.
		}

		if processedChange.Tombstone {
			if writeErr := c.metadata.WriteTombstone(ctx, processedChange.Table, processedChange.PK, processedChange.HLC, processedChange.NodeID); writeErr != nil {
				return nil, fmt.Errorf("crdt: merge tombstone: %w", writeErr)
			}
			merged++

			// Run AfterInboundChange hook.
			c.hooks.AfterInboundChange(ctx, processedChange) //nolint:errcheck // fire-and-forget post-hook
			continue
		}

		// Read existing local state.
		localState, err := c.metadata.ReadState(ctx, processedChange.Table, processedChange.PK)
		if err != nil {
			return nil, fmt.Errorf("crdt: read state: %w", err)
		}

		remoteFS := &FieldState{
			Type:   processedChange.CRDTType,
			HLC:    processedChange.HLC,
			NodeID: processedChange.NodeID,
			Value:  processedChange.Value,
		}
		if processedChange.CounterDelta != nil {
			cs := NewPNCounterState()
			cs.Increments[processedChange.NodeID] = processedChange.CounterDelta.Increment
			cs.Decrements[processedChange.NodeID] = processedChange.CounterDelta.Decrement
			remoteFS.CounterState = cs
		}

		var localFS *FieldState
		if localState != nil {
			localFS = localState.Fields[processedChange.Field]
		}

		mergedFS, err := c.plugin.merge.MergeField(localFS, remoteFS)
		if err != nil {
			return nil, fmt.Errorf("crdt: merge field: %w", err)
		}

		if err := c.metadata.WriteFieldState(ctx, processedChange.Table, processedChange.PK, processedChange.Field, mergedFS); err != nil {
			return nil, fmt.Errorf("crdt: write state: %w", err)
		}
		merged++

		// Run AfterInboundChange hook.
		c.hooks.AfterInboundChange(ctx, processedChange) //nolint:errcheck // fire-and-forget post-hook
	}

	return &PushResponse{
		Merged:    merged,
		LatestHLC: c.plugin.clock.Now(),
	}, nil
}

// StreamChangesSince returns a channel that yields new changes as they appear.
// The caller should poll or watch for changes. This is used by SSE handlers.
func (c *SyncController) StreamChangesSince(ctx context.Context, tables []string, since HLC) (<-chan []ChangeRecord, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	ch := make(chan []ChangeRecord, 16)
	go func() {
		defer close(ch)

		lastHLC := since
		ticker := time.NewTicker(c.streamPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var allChanges []ChangeRecord
				for _, table := range tables {
					changes, err := c.metadata.ReadChangesSince(ctx, table, lastHLC)
					if err != nil {
						c.logger.Error("crdt: stream read error",
							log.String("error", err.Error()),
						)
						continue
					}
					allChanges = append(allChanges, changes...)
				}

				if len(allChanges) == 0 {
					continue
				}

				// Run BeforeOutboundRead hook.
				filtered, err := c.hooks.BeforeOutboundRead(ctx, allChanges)
				if err != nil {
					c.logger.Error("crdt: stream outbound hook error",
						log.String("error", err.Error()),
					)
					continue
				}

				if len(filtered) > 0 {
					// Update last HLC for next poll.
					for _, ch := range filtered {
						if ch.HLC.After(lastHLC) {
							lastHLC = ch.HLC
						}
					}

					select {
					case ch <- filtered:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

// --- Presence ---

// HandlePresenceUpdate processes a presence update and returns the resulting event.
// Returns nil if presence is not enabled.
func (c *SyncController) HandlePresenceUpdate(_ context.Context, update *PresenceUpdate) (*PresenceEvent, error) {
	if c.presence == nil {
		return nil, fmt.Errorf("crdt: presence is not enabled")
	}
	if update.NodeID == "" {
		return nil, fmt.Errorf("crdt: presence update requires node_id")
	}
	if update.Topic == "" {
		return nil, fmt.Errorf("crdt: presence update requires topic")
	}

	// A null data payload means the client is leaving.
	if update.Data == nil || string(update.Data) == "null" {
		event := c.presence.Remove(update.Topic, update.NodeID)
		if event == nil {
			// Node wasn't present — return a synthetic leave event.
			ev := PresenceEvent{
				Type:   PresenceLeave,
				NodeID: update.NodeID,
				Topic:  update.Topic,
			}
			return &ev, nil
		}
		return event, nil
	}

	event := c.presence.Update(*update)
	return &event, nil
}

// HandleGetPresence returns a snapshot of all active presence for a topic.
func (c *SyncController) HandleGetPresence(_ context.Context, topic string) (*PresenceSnapshot, error) {
	if c.presence == nil {
		return nil, fmt.Errorf("crdt: presence is not enabled")
	}
	if topic == "" {
		return nil, fmt.Errorf("crdt: presence query requires topic")
	}

	states := c.presence.Get(topic)
	if states == nil {
		states = []PresenceState{}
	}
	return &PresenceSnapshot{
		Topic:  topic,
		States: states,
	}, nil
}

// Presence returns the presence manager, or nil if presence is disabled.
func (c *SyncController) Presence() *PresenceManager {
	return c.presence
}

// PresenceChannel returns the channel for receiving presence events to
// broadcast over SSE streams. Returns nil if presence is disabled.
func (c *SyncController) PresenceChannel() <-chan PresenceEvent {
	return c.presenceCh
}

// Close cleans up the controller's resources (presence manager, etc.).
func (c *SyncController) Close() {
	if c.presence != nil {
		c.presence.Close()
	}
}

// --- HTTP Handler (backward-compatible, no Forge dependency) ---

// NewHTTPHandler creates a standard http.Handler for sync endpoints.
// Use this when not running inside a Forge app. For Forge apps, use
// grove/extension.WithCRDT() which auto-registers routes.
//
// Endpoints:
//   - POST /pull  — remote nodes pull changes from this node
//   - POST /push  — remote nodes push changes to this node
func NewHTTPHandler(plugin *Plugin, opts ...SyncControllerOption) http.Handler {
	ctrl := NewSyncController(plugin, opts...)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pull", ctrl.httpHandlePull)
	mux.HandleFunc("POST /push", ctrl.httpHandlePush)
	if ctrl.presence != nil {
		mux.HandleFunc("POST /presence", ctrl.httpHandlePresenceUpdate)
		mux.HandleFunc("GET /presence", ctrl.httpHandleGetPresence)
	}
	return mux
}

func (c *SyncController) httpHandlePull(w http.ResponseWriter, r *http.Request) {
	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	resp, err := c.HandlePull(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandlePush(w http.ResponseWriter, r *http.Request) {
	var req PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	resp, err := c.HandlePush(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandlePresenceUpdate(w http.ResponseWriter, r *http.Request) {
	var update PresenceUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	event, err := c.HandlePresenceUpdate(r.Context(), &update)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(event) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandleGetPresence(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "crdt: missing topic query parameter")
		return
	}

	snapshot, err := c.HandleGetPresence(r.Context(), topic)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot) //nolint:errcheck // HTTP response write
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // HTTP response write
}

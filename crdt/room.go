package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"
)

// RoomManager provides a structured API for managing presence rooms.
// A room is a named collaboration space (a document, a channel, a canvas)
// with tracked participants, metadata, and lifecycle events.
//
// Rooms are backed by the PresenceManager for TTL-based presence, but add:
//   - Room metadata (title, type, max participants, custom data)
//   - Room lifecycle callbacks (created, destroyed, participant limits)
//   - Participant count tracking
//   - Cursor/selection position tracking helpers
//   - Batch join/leave operations
//   - Room listing and querying
type RoomManager struct {
	mu       sync.RWMutex
	presence *PresenceManager
	rooms    map[string]*Room // roomID → Room
	hooks    []RoomHook
	logger   log.Logger
}

// Room represents a collaboration space with metadata and participants.
type Room struct {
	// ID is the unique room identifier (also used as the presence topic).
	ID string `json:"id"`

	// Type classifies the room (e.g., "document", "channel", "canvas").
	Type string `json:"type,omitempty"`

	// Metadata holds arbitrary room-level data (title, permissions, etc.).
	Metadata json.RawMessage `json:"metadata,omitempty"`

	// MaxParticipants limits how many can join (0 = unlimited).
	MaxParticipants int `json:"max_participants,omitempty"`

	// CreatedAt is when the room was first created.
	CreatedAt time.Time `json:"created_at"`

	// CreatedBy is the nodeID that created the room.
	CreatedBy string `json:"created_by,omitempty"`
}

// RoomInfo is the public view of a room including live participant info.
type RoomInfo struct {
	Room
	// ParticipantCount is the current number of participants.
	ParticipantCount int `json:"participant_count"`
	// Participants lists current participant states.
	Participants []PresenceState `json:"participants"`
}

// CursorPosition represents a cursor or selection in a document.
type CursorPosition struct {
	// X/Y for canvas-style cursors.
	X float64 `json:"x,omitempty"`
	Y float64 `json:"y,omitempty"`

	// Offset for text-style cursors (character position).
	Offset int `json:"offset,omitempty"`

	// Line/Column for code-style cursors.
	Line   int `json:"line,omitempty"`
	Column int `json:"column,omitempty"`

	// SelectionStart/End for text selections.
	SelectionStart int `json:"selection_start,omitempty"`
	SelectionEnd   int `json:"selection_end,omitempty"`

	// Field identifies which field/element the cursor is in.
	Field string `json:"field,omitempty"`
}

// ParticipantData is the standard presence payload for room participants.
// Consumers can extend this with custom fields via the Extra map.
type ParticipantData struct {
	// Name is the display name of the participant.
	Name string `json:"name,omitempty"`

	// Color is the assigned collaboration color (hex).
	Color string `json:"color,omitempty"`

	// Avatar is a URL to the participant's avatar image.
	Avatar string `json:"avatar,omitempty"`

	// Cursor is the participant's current cursor position.
	Cursor *CursorPosition `json:"cursor,omitempty"`

	// IsTyping indicates whether the participant is currently typing.
	IsTyping bool `json:"is_typing,omitempty"`

	// ActiveField is the field currently being edited (for form-style UIs).
	ActiveField string `json:"active_field,omitempty"`

	// Status is a custom status string (e.g., "idle", "editing", "viewing").
	Status string `json:"status,omitempty"`

	// Extra holds arbitrary user-defined data.
	Extra map[string]any `json:"extra,omitempty"`
}

// RoomHook is a callback for room lifecycle events.
type RoomHook interface {
	// OnRoomCreated is called when a room is created.
	OnRoomCreated(ctx context.Context, room *Room) error
	// OnRoomDestroyed is called when a room's last participant leaves.
	OnRoomDestroyed(ctx context.Context, room *Room) error
	// OnParticipantJoin is called when a participant joins a room.
	OnParticipantJoin(ctx context.Context, room *Room, nodeID string) error
	// OnParticipantLeave is called when a participant leaves a room.
	OnParticipantLeave(ctx context.Context, room *Room, nodeID string) error
}

// RoomEvent is a rich event emitted by the RoomManager.
type RoomEvent struct {
	Type   RoomEventType   `json:"type"`
	RoomID string          `json:"room_id"`
	NodeID string          `json:"node_id,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// RoomEventType identifies room lifecycle events.
type RoomEventType string

const (
	RoomEventCreated          RoomEventType = "room_created"
	RoomEventDestroyed        RoomEventType = "room_destroyed"
	RoomEventParticipantJoin  RoomEventType = "participant_join"
	RoomEventParticipantLeave RoomEventType = "participant_leave"
	RoomEventMetadataUpdated  RoomEventType = "metadata_updated"
)

// NewRoomManager creates a room manager backed by the given presence manager.
func NewRoomManager(presence *PresenceManager, logger log.Logger) *RoomManager {
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	rm := &RoomManager{
		presence: presence,
		rooms:    make(map[string]*Room),
		logger:   logger,
	}

	return rm
}

// AddHook registers a room lifecycle hook.
func (rm *RoomManager) AddHook(hook RoomHook) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.hooks = append(rm.hooks, hook)
}

// CreateRoom creates or returns an existing room.
func (rm *RoomManager) CreateRoom(ctx context.Context, id, roomType string, opts ...RoomOption) (*Room, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if existing, ok := rm.rooms[id]; ok {
		return existing, nil
	}

	room := &Room{
		ID:        id,
		Type:      roomType,
		CreatedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(room)
	}

	rm.rooms[id] = room

	for _, h := range rm.hooks {
		if err := h.OnRoomCreated(ctx, room); err != nil {
			rm.logger.Error("room hook error", log.String("error", err.Error()))
		}
	}

	return room, nil
}

// GetRoom returns a room by ID, or nil if not found.
func (rm *RoomManager) GetRoom(id string) *Room {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.rooms[id]
}

// GetRoomInfo returns a room with live participant info.
func (rm *RoomManager) GetRoomInfo(id string) *RoomInfo {
	rm.mu.RLock()
	room, ok := rm.rooms[id]
	rm.mu.RUnlock()
	if !ok {
		return nil
	}

	participants := rm.presence.Get(id)
	return &RoomInfo{
		Room:             *room,
		ParticipantCount: len(participants),
		Participants:     participants,
	}
}

// ListRooms returns all active rooms with participant counts.
func (rm *RoomManager) ListRooms() []RoomInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	result := make([]RoomInfo, 0, len(rm.rooms))
	for _, room := range rm.rooms {
		participants := rm.presence.Get(room.ID)
		result = append(result, RoomInfo{
			Room:             *room,
			ParticipantCount: len(participants),
			Participants:     participants,
		})
	}
	return result
}

// ListRoomsByType returns rooms filtered by type.
func (rm *RoomManager) ListRoomsByType(roomType string) []RoomInfo {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var result []RoomInfo
	for _, room := range rm.rooms {
		if room.Type == roomType {
			participants := rm.presence.Get(room.ID)
			result = append(result, RoomInfo{
				Room:             *room,
				ParticipantCount: len(participants),
				Participants:     participants,
			})
		}
	}
	return result
}

// JoinRoom adds a participant to a room. Creates the room if it doesn't exist.
func (rm *RoomManager) JoinRoom(ctx context.Context, roomID, nodeID string, data json.RawMessage) error {
	rm.mu.Lock()

	room, ok := rm.rooms[roomID]
	if !ok {
		room = &Room{
			ID:        roomID,
			CreatedAt: time.Now(),
			CreatedBy: nodeID,
		}
		rm.rooms[roomID] = room
	}

	// Check participant limit.
	if room.MaxParticipants > 0 {
		current := rm.presence.Get(roomID)
		if len(current) >= room.MaxParticipants {
			rm.mu.Unlock()
			return fmt.Errorf("crdt: room %s is full (%d/%d)", roomID, len(current), room.MaxParticipants)
		}
	}

	hooks := make([]RoomHook, len(rm.hooks))
	copy(hooks, rm.hooks)
	rm.mu.Unlock()

	// Update presence (this broadcasts join/update events via SSE).
	rm.presence.Update(PresenceUpdate{
		NodeID: nodeID,
		Topic:  roomID,
		Data:   data,
	})

	for _, h := range hooks {
		if err := h.OnParticipantJoin(ctx, room, nodeID); err != nil {
			rm.logger.Error("room join hook error", log.String("error", err.Error()))
		}
	}

	return nil
}

// LeaveRoom removes a participant from a room.
// If the room becomes empty, it is destroyed.
func (rm *RoomManager) LeaveRoom(ctx context.Context, roomID, nodeID string) {
	rm.presence.Remove(roomID, nodeID)

	rm.mu.Lock()
	room, ok := rm.rooms[roomID]
	if !ok {
		rm.mu.Unlock()
		return
	}

	hooks := make([]RoomHook, len(rm.hooks))
	copy(hooks, rm.hooks)

	// Check if room is now empty.
	remaining := rm.presence.Get(roomID)
	isEmpty := len(remaining) == 0

	if isEmpty {
		delete(rm.rooms, roomID)
	}
	rm.mu.Unlock()

	for _, h := range hooks {
		if err := h.OnParticipantLeave(ctx, room, nodeID); err != nil {
			rm.logger.Error("room leave hook error", log.String("error", err.Error()))
		}
	}

	if isEmpty {
		for _, h := range hooks {
			if err := h.OnRoomDestroyed(ctx, room); err != nil {
				rm.logger.Error("room destroy hook error", log.String("error", err.Error()))
			}
		}
	}
}

// LeaveAllRooms removes a participant from all rooms.
func (rm *RoomManager) LeaveAllRooms(ctx context.Context, nodeID string) {
	topics := rm.presence.GetTopicsForNode(nodeID)
	for _, topic := range topics {
		rm.LeaveRoom(ctx, topic, nodeID)
	}
}

// UpdateCursor is a convenience method to update a participant's cursor position.
func (rm *RoomManager) UpdateCursor(roomID, nodeID string, cursor CursorPosition) {
	// Read current presence data.
	states := rm.presence.Get(roomID)
	var existingData ParticipantData
	for _, s := range states {
		if s.NodeID == nodeID {
			if s.Data != nil {
				json.Unmarshal(s.Data, &existingData) //nolint:errcheck // best-effort read of existing presence data
			}
			break
		}
	}

	existingData.Cursor = &cursor
	data, _ := json.Marshal(existingData) //nolint:errcheck // serialization of known struct won't fail

	rm.presence.Update(PresenceUpdate{
		NodeID: nodeID,
		Topic:  roomID,
		Data:   data,
	})
}

// UpdateTypingStatus is a convenience method to update a participant's typing state.
func (rm *RoomManager) UpdateTypingStatus(roomID, nodeID string, isTyping bool) {
	states := rm.presence.Get(roomID)
	var existingData ParticipantData
	for _, s := range states {
		if s.NodeID == nodeID {
			if s.Data != nil {
				json.Unmarshal(s.Data, &existingData) //nolint:errcheck // best-effort read of existing presence data
			}
			break
		}
	}

	existingData.IsTyping = isTyping
	data, _ := json.Marshal(existingData) //nolint:errcheck // serialization of known struct won't fail

	rm.presence.Update(PresenceUpdate{
		NodeID: nodeID,
		Topic:  roomID,
		Data:   data,
	})
}

// SetRoomMetadata updates the metadata for a room.
func (rm *RoomManager) SetRoomMetadata(roomID string, metadata any) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	room, ok := rm.rooms[roomID]
	if !ok {
		return fmt.Errorf("crdt: room %s not found", roomID)
	}

	raw, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("crdt: marshal metadata: %w", err)
	}
	room.Metadata = raw
	return nil
}

// ParticipantCount returns the number of participants in a room.
func (rm *RoomManager) ParticipantCount(roomID string) int {
	return len(rm.presence.Get(roomID))
}

// Close cleans up the room manager.
func (rm *RoomManager) Close() {
	// Rooms are ephemeral — nothing to persist.
}

// --- Room Options ---

// RoomOption configures a Room during creation.
type RoomOption func(*Room)

// WithRoomType sets the room type.
func WithRoomType(t string) RoomOption {
	return func(r *Room) { r.Type = t }
}

// WithRoomMetadata sets initial room metadata.
func WithRoomMetadata(metadata any) RoomOption {
	return func(r *Room) {
		raw, _ := json.Marshal(metadata) //nolint:errcheck // option helper, error not actionable
		r.Metadata = raw
	}
}

// WithMaxParticipants sets the maximum number of participants.
func WithMaxParticipants(maxCount int) RoomOption {
	return func(r *Room) { r.MaxParticipants = maxCount }
}

// WithRoomCreator sets who created the room.
func WithRoomCreator(nodeID string) RoomOption {
	return func(r *Room) { r.CreatedBy = nodeID }
}

// --- Document Room Helpers ---

// DocumentRoomID returns the standard room ID for a table + primary key.
// Use this for per-document collaboration rooms.
func DocumentRoomID(table, pk string) string {
	return table + ":" + pk
}

// CreateDocumentRoom creates a room for collaborating on a specific document.
func (rm *RoomManager) CreateDocumentRoom(ctx context.Context, table, pk string, opts ...RoomOption) (*Room, error) {
	roomID := DocumentRoomID(table, pk)
	allOpts := append([]RoomOption{WithRoomType("document")}, opts...)
	return rm.CreateRoom(ctx, roomID, "document", allOpts...)
}

// JoinDocumentRoom joins the room for a specific document.
func (rm *RoomManager) JoinDocumentRoom(ctx context.Context, table, pk, nodeID string, data json.RawMessage) error {
	roomID := DocumentRoomID(table, pk)
	return rm.JoinRoom(ctx, roomID, nodeID, data)
}

// LeaveDocumentRoom leaves the room for a specific document.
func (rm *RoomManager) LeaveDocumentRoom(ctx context.Context, table, pk, nodeID string) {
	roomID := DocumentRoomID(table, pk)
	rm.LeaveRoom(ctx, roomID, nodeID)
}

// GetDocumentParticipants returns participants for a document room.
func (rm *RoomManager) GetDocumentParticipants(table, pk string) []PresenceState {
	roomID := DocumentRoomID(table, pk)
	return rm.presence.Get(roomID)
}

// --- HTTP Handlers ---

// RoomHTTPHandler provides HTTP endpoints for room management.
// Mount under the sync server path (e.g., /sync/rooms).
func RoomHTTPHandler(rm *RoomManager) http.Handler {
	mux := http.NewServeMux()

	// List all rooms.
	mux.HandleFunc("GET /rooms", func(w http.ResponseWriter, r *http.Request) {
		roomType := r.URL.Query().Get("type")
		var rooms []RoomInfo
		if roomType != "" {
			rooms = rm.ListRoomsByType(roomType)
		} else {
			rooms = rm.ListRooms()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(rooms) //nolint:errcheck // HTTP response write
	})

	// Get room info.
	mux.HandleFunc("GET /rooms/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		info := rm.GetRoomInfo(id)
		if info == nil {
			writeError(w, http.StatusNotFound, "room not found")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info) //nolint:errcheck // HTTP response write
	})

	// Create a room.
	mux.HandleFunc("POST /rooms", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID              string          `json:"id"`
			Type            string          `json:"type"`
			Metadata        json.RawMessage `json:"metadata,omitempty"`
			MaxParticipants int             `json:"max_participants,omitempty"`
			CreatedBy       string          `json:"created_by,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.ID == "" {
			writeError(w, http.StatusBadRequest, "id is required")
			return
		}

		var opts []RoomOption
		if req.Metadata != nil {
			opts = append(opts, func(room *Room) { room.Metadata = req.Metadata })
		}
		if req.MaxParticipants > 0 {
			opts = append(opts, WithMaxParticipants(req.MaxParticipants))
		}
		if req.CreatedBy != "" {
			opts = append(opts, WithRoomCreator(req.CreatedBy))
		}

		room, err := rm.CreateRoom(r.Context(), req.ID, req.Type, opts...)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(room) //nolint:errcheck // HTTP response write
	})

	// Join a room.
	mux.HandleFunc("POST /rooms/{id}/join", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req struct {
			NodeID string          `json:"node_id"`
			Data   json.RawMessage `json:"data,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.NodeID == "" {
			writeError(w, http.StatusBadRequest, "node_id is required")
			return
		}

		if err := rm.JoinRoom(r.Context(), id, req.NodeID, req.Data); err != nil {
			writeError(w, http.StatusConflict, err.Error())
			return
		}

		info := rm.GetRoomInfo(id)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(info) //nolint:errcheck // HTTP response write
	})

	// Leave a room.
	mux.HandleFunc("POST /rooms/{id}/leave", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if req.NodeID == "" {
			writeError(w, http.StatusBadRequest, "node_id is required")
			return
		}

		rm.LeaveRoom(r.Context(), id, req.NodeID)
		w.WriteHeader(http.StatusNoContent)
	})

	// Update cursor position.
	mux.HandleFunc("POST /rooms/{id}/cursor", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req struct {
			NodeID string         `json:"node_id"`
			Cursor CursorPosition `json:"cursor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		rm.UpdateCursor(id, req.NodeID, req.Cursor)
		w.WriteHeader(http.StatusNoContent)
	})

	// Update typing status.
	mux.HandleFunc("POST /rooms/{id}/typing", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var req struct {
			NodeID   string `json:"node_id"`
			IsTyping bool   `json:"is_typing"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		rm.UpdateTypingStatus(id, req.NodeID, req.IsTyping)
		w.WriteHeader(http.StatusNoContent)
	})

	// Update room metadata.
	mux.HandleFunc("PUT /rooms/{id}/metadata", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		var metadata json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&metadata); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		rm.mu.Lock()
		room, ok := rm.rooms[id]
		if ok {
			room.Metadata = metadata
		}
		rm.mu.Unlock()

		if !ok {
			writeError(w, http.StatusNotFound, "room not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Get room participants.
	mux.HandleFunc("GET /rooms/{id}/participants", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		participants := rm.presence.Get(id)
		if participants == nil {
			participants = []PresenceState{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(participants) //nolint:errcheck // HTTP response write
	})

	return mux
}

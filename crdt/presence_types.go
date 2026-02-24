package crdt

import (
	"encoding/json"
	"time"
)

// PresenceState holds a single client's presence data for a topic.
// Topics are typically "table:pk" for per-document presence, but can be
// any arbitrary string for rooms or channels.
type PresenceState struct {
	// NodeID identifies the client that owns this presence entry.
	NodeID string `json:"node_id"`

	// Topic is the presence scope (e.g. "documents:doc-1" or "lobby").
	Topic string `json:"topic"`

	// Data is the user-defined presence payload (cursor position, typing state, etc.).
	Data json.RawMessage `json:"data"`

	// UpdatedAt is the time this entry was last updated.
	UpdatedAt time.Time `json:"updated_at"`

	// ExpiresAt is the time this entry expires if not refreshed.
	ExpiresAt time.Time `json:"expires_at"`
}

// PresenceUpdate is the request body for POST /sync/presence.
type PresenceUpdate struct {
	// NodeID identifies the client sending the update.
	NodeID string `json:"node_id"`

	// Topic is the presence scope.
	Topic string `json:"topic"`

	// Data is the user-defined presence payload. Set to null to leave.
	Data json.RawMessage `json:"data"`
}

// PresenceEvent is broadcast over SSE to notify clients of presence changes.
type PresenceEvent struct {
	// Type is "join", "update", or "leave".
	Type string `json:"type"`

	// NodeID identifies the client whose presence changed.
	NodeID string `json:"node_id"`

	// Topic is the presence scope.
	Topic string `json:"topic"`

	// Data is the user-defined presence payload (empty for "leave" events).
	Data json.RawMessage `json:"data,omitempty"`
}

// PresenceSnapshot is the response for GET /sync/presence?topic=...
type PresenceSnapshot struct {
	// Topic is the presence scope.
	Topic string `json:"topic"`

	// States is the list of active presence entries for the topic.
	States []PresenceState `json:"states"`
}

// Presence event type constants.
const (
	PresenceJoin      = "join"
	PresenceUpdateEvt = "update"
	PresenceLeave     = "leave"
)

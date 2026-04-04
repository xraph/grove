// Package crdt provides an optional CRDT (Conflict-Free Replicated Data Type) layer
// for Grove. It enables offline-first, multi-node, and eventually-consistent use cases
// by tracking field-level changes with Hybrid Logical Clocks and merging them
// automatically during sync.
//
// Tag model fields with crdt:lww, crdt:counter, or crdt:set to opt into
// CRDT behavior. Fields without CRDT tags are unaffected.
//
//	type Document struct {
//	    grove.BaseModel `grove:"table:documents,alias:d"`
//	    ID        string   `grove:"id,pk"`
//	    Title     string   `grove:"title,crdt:lww"`
//	    ViewCount int64    `grove:"view_count,crdt:counter"`
//	    Tags      []string `grove:"tags,type:jsonb,crdt:set"`
//	}
package crdt

import (
	"encoding/json"
	"fmt"
)

// CRDTType identifies the conflict resolution strategy for a field.
type CRDTType string //nolint:revive // CRDTType is the established public API name

const (
	// TypeLWW is a Last-Writer-Wins register. The value with the highest HLC wins.
	TypeLWW CRDTType = "lww"

	// TypeCounter is a PN-Counter. Each node tracks its own increments and
	// decrements; the global value is the sum across all nodes.
	TypeCounter CRDTType = "counter"

	// TypeSet is an Observed-Remove Set (OR-Set) with add-wins semantics.
	// Concurrent add and remove of the same element results in the element
	// being present (add wins).
	TypeSet CRDTType = "set"

	// TypeList is a Replicated Growable Array (RGA) for ordered sequences.
	// Elements have stable positions and can be inserted, deleted, or moved
	// concurrently without conflicts.
	TypeList CRDTType = "list"

	// TypeDocument is a recursive CRDT map that supports nested paths.
	// Each nested field is independently mergeable with its own CRDT type,
	// enabling JSON-like nested structures.
	TypeDocument CRDTType = "document"
)

// ValidCRDTType returns true if t is a recognized CRDT type.
func ValidCRDTType(t string) bool {
	switch CRDTType(t) {
	case TypeLWW, TypeCounter, TypeSet, TypeList, TypeDocument:
		return true
	default:
		return false
	}
}

// State holds the full CRDT state for a single record (all fields).
type State struct {
	// Table is the source table name.
	Table string `json:"table"`

	// PK is the string-encoded primary key.
	PK string `json:"pk"`

	// Fields maps field name → field-level CRDT state.
	Fields map[string]*FieldState `json:"fields"`

	// Tombstone is true if the record has been deleted.
	Tombstone bool `json:"tombstone"`

	// TombstoneHLC is the clock value of the delete operation (if tombstoned).
	TombstoneHLC HLC `json:"tombstone_hlc,omitempty"`
}

// NewState creates an empty State for the given table and primary key.
func NewState(table, pk string) *State {
	return &State{
		Table:  table,
		PK:     pk,
		Fields: make(map[string]*FieldState),
	}
}

// FieldState holds the CRDT state for a single field.
type FieldState struct {
	// Type is the CRDT type of this field.
	Type CRDTType `json:"type"`

	// HLC is the clock value of the last write (used by LWW).
	HLC HLC `json:"hlc"`

	// NodeID is the node that produced the last write (used by LWW).
	NodeID string `json:"node_id"`

	// Value is the current resolved value (LWW) or nil for Counter/Set.
	Value json.RawMessage `json:"value,omitempty"`

	// CounterState holds per-node increments/decrements (Counter only).
	CounterState *PNCounterState `json:"counter_state,omitempty"`

	// SetState holds the OR-Set state (Set only).
	SetState *ORSetState `json:"set_state,omitempty"`

	// ListState holds the RGA list state (List only).
	ListState *RGAListState `json:"list_state,omitempty"`

	// DocState holds nested document CRDT state (Document only).
	DocState *DocumentCRDTState `json:"doc_state,omitempty"`
}

// ChangeRecord represents a single field-level change for sync transport.
type ChangeRecord struct {
	Table     string          `json:"table"`
	PK        string          `json:"pk"`
	Field     string          `json:"field"`
	CRDTType  CRDTType        `json:"crdt_type"`
	HLC       HLC             `json:"hlc"`
	NodeID    string          `json:"node_id"`
	Value     json.RawMessage `json:"value,omitempty"`
	Tombstone bool            `json:"tombstone,omitempty"`

	// Type-specific payloads (only one is set based on CRDTType).
	CounterDelta *CounterDelta `json:"counter_delta,omitempty"`
	SetOp        *SetOperation `json:"set_op,omitempty"`
	ListOp       *ListOp       `json:"list_op,omitempty"`
}

// CounterDelta represents a single node's counter change.
type CounterDelta struct {
	Increment int64 `json:"inc"`
	Decrement int64 `json:"dec"`
}

// SetOperation represents an add or remove operation on an OR-Set.
type SetOperation struct {
	Op       SetOp           `json:"op"`       // "add" or "remove"
	Elements json.RawMessage `json:"elements"` // JSON array of elements
}

// SetOp is the type of set operation.
type SetOp string

const (
	SetOpAdd    SetOp = "add"
	SetOpRemove SetOp = "remove"
)

// SyncReport summarizes the result of a sync operation.
type SyncReport struct {
	Pulled    int `json:"pulled"`
	Pushed    int `json:"pushed"`
	Merged    int `json:"merged"`
	Conflicts int `json:"conflicts"`
}

func (r *SyncReport) String() string {
	return fmt.Sprintf("pulled=%d pushed=%d merged=%d conflicts=%d",
		r.Pulled, r.Pushed, r.Merged, r.Conflicts)
}

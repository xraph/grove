package crdt

import (
	"encoding/json"
	"strings"
)

// DocumentCRDTState holds nested document CRDT state. Each path
// (e.g., "address.city", "tags", "meta.count") is independently mergeable
// with its own CRDT type, enabling JSON-like nested structures where
// different subtrees can use different conflict resolution strategies.
type DocumentCRDTState struct {
	// Fields maps dot-separated paths to their field-level CRDT state.
	// Paths use "." as the separator for nesting.
	// Example: "address.city" → LWW, "tags" → Set, "views" → Counter.
	Fields map[string]*FieldState `json:"fields"`
}

// NewDocumentCRDTState creates an empty document CRDT state.
func NewDocumentCRDTState() *DocumentCRDTState {
	return &DocumentCRDTState{
		Fields: make(map[string]*FieldState),
	}
}

// SetField sets a field at the given path with LWW semantics.
func (d *DocumentCRDTState) SetField(path string, value any, clock HLC, nodeID string) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	d.Fields[path] = &FieldState{
		Type:   TypeLWW,
		HLC:    clock,
		NodeID: nodeID,
		Value:  raw,
	}
	return nil
}

// SetFieldState sets a field at the given path with an explicit FieldState.
// Use this for non-LWW types (counters, sets, lists).
func (d *DocumentCRDTState) SetFieldState(path string, fs *FieldState) {
	d.Fields[path] = fs
}

// GetField returns the resolved value at the given path.
// Returns nil if the path doesn't exist.
func (d *DocumentCRDTState) GetField(path string) *FieldState {
	return d.Fields[path]
}

// DeleteField removes a field at the given path and all its children.
func (d *DocumentCRDTState) DeleteField(path string) {
	prefix := path + "."
	for key := range d.Fields {
		if key == path || strings.HasPrefix(key, prefix) {
			delete(d.Fields, key)
		}
	}
}

// Paths returns all field paths sorted alphabetically.
func (d *DocumentCRDTState) Paths() []string {
	paths := make([]string, 0, len(d.Fields))
	for p := range d.Fields {
		paths = append(paths, p)
	}
	return paths
}

// Resolve converts the document state to a nested map structure.
// This is the "materialized view" of the document.
func (d *DocumentCRDTState) Resolve() map[string]any {
	result := make(map[string]any)

	for path, fs := range d.Fields {
		parts := strings.Split(path, ".")
		setNestedValue(result, parts, resolveFieldValue(fs))
	}

	return result
}

func setNestedValue(m map[string]any, parts []string, value any) {
	for i := 0; i < len(parts)-1; i++ {
		sub, ok := m[parts[i]]
		if !ok {
			sub = make(map[string]any)
			m[parts[i]] = sub
		}
		if subMap, ok := sub.(map[string]any); ok {
			m = subMap
		} else {
			// Conflict: a leaf value exists at a non-leaf path.
			// The nested path wins.
			newMap := make(map[string]any)
			m[parts[i]] = newMap
			m = newMap
		}
	}
	m[parts[len(parts)-1]] = value
}

func resolveFieldValue(fs *FieldState) any {
	if fs == nil {
		return nil
	}
	switch fs.Type {
	case TypeLWW:
		var v any
		if fs.Value != nil {
			json.Unmarshal(fs.Value, &v) //nolint:errcheck // best-effort display value
		}
		return v
	case TypeCounter:
		if fs.CounterState != nil {
			return fs.CounterState.Value()
		}
		return int64(0)
	case TypeSet:
		if fs.SetState != nil {
			return fs.SetState.Elements()
		}
		return []json.RawMessage{}
	case TypeList:
		if fs.ListState != nil {
			return fs.ListState.Elements()
		}
		return []json.RawMessage{}
	default:
		var v any
		if fs.Value != nil {
			json.Unmarshal(fs.Value, &v) //nolint:errcheck // best-effort display value
		}
		return v
	}
}

// MergeDocument merges two document CRDT states by merging each path
// independently using the MergeEngine. This is commutative, associative,
// and idempotent.
func MergeDocument(local, remote *DocumentCRDTState) (*DocumentCRDTState, error) {
	if local == nil {
		return remote, nil
	}
	if remote == nil {
		return local, nil
	}

	merged := NewDocumentCRDTState()
	engine := NewMergeEngine()

	// Collect all paths.
	allPaths := make(map[string]bool)
	for p := range local.Fields {
		allPaths[p] = true
	}
	for p := range remote.Fields {
		allPaths[p] = true
	}

	for path := range allPaths {
		localFS := local.Fields[path]
		remoteFS := remote.Fields[path]

		result, err := engine.MergeField(localFS, remoteFS)
		if err != nil {
			// Type mismatch at the same path: higher HLC wins.
			switch {
			case localFS != nil && remoteFS != nil:
				if remoteFS.HLC.After(localFS.HLC) {
					result = remoteFS
				} else {
					result = localFS
				}
			case localFS != nil:
				result = localFS
			default:
				result = remoteFS
			}
		}
		merged.Fields[path] = result
	}

	return merged, nil
}

// ToFieldState converts to the generic FieldState representation.
func (d *DocumentCRDTState) ToFieldState(clock HLC, nodeID string) *FieldState {
	resolved := d.Resolve()
	raw, err := json.Marshal(resolved)
	if err != nil {
		return nil
	}
	return &FieldState{
		Type:     TypeDocument,
		HLC:      clock,
		NodeID:   nodeID,
		Value:    raw,
		DocState: d,
	}
}

// DocumentFromFieldState reconstructs a DocumentCRDTState from a FieldState.
func DocumentFromFieldState(fs *FieldState) *DocumentCRDTState {
	if fs == nil || fs.Type != TypeDocument {
		return nil
	}
	if fs.DocState == nil {
		return NewDocumentCRDTState()
	}
	return fs.DocState
}

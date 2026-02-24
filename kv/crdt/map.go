package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Map is a distributed CRDT Map where each field is an independent LWW register.
// It stores a crdt.State with per-field FieldState entries.
type Map struct {
	crdtBase
}

// NewMap creates a new CRDT Map backed by the given store and key.
func NewMap(store *kv.Store, key string, opts ...Option) *Map {
	return &Map{crdtBase: newBase(store, key, opts)}
}

// Set sets a field to a value using LWW semantics.
func (m *Map) Set(ctx context.Context, field string, value any) error {
	state, err := m.load(ctx)
	if err != nil {
		return err
	}

	hlc := m.now()
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("kvcrdt: map set: encode %s: %w", field, err)
	}

	state.Fields[field] = &crdt.FieldState{
		Type:   crdt.TypeLWW,
		HLC:    hlc,
		NodeID: m.nodeID,
		Value:  raw,
	}

	return saveState(ctx, &m.crdtBase, state)
}

// Get reads a field value and decodes it into dest.
func (m *Map) Get(ctx context.Context, field string, dest any) error {
	state, err := m.load(ctx)
	if err != nil {
		return err
	}

	fs, ok := state.Fields[field]
	if !ok {
		return kv.ErrNotFound
	}

	return json.Unmarshal(fs.Value, dest)
}

// Delete removes a field by setting a tombstone.
func (m *Map) Delete(ctx context.Context, field string) error {
	state, err := m.load(ctx)
	if err != nil {
		return err
	}

	delete(state.Fields, field)

	return saveState(ctx, &m.crdtBase, state)
}

// Keys returns all field names in the map.
func (m *Map) Keys(ctx context.Context) ([]string, error) {
	state, err := m.load(ctx)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(state.Fields))
	for k := range state.Fields {
		keys = append(keys, k)
	}
	return keys, nil
}

// All returns all fields as a map of field name to raw JSON values.
func (m *Map) All(ctx context.Context) (map[string]json.RawMessage, error) {
	state, err := m.load(ctx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]json.RawMessage, len(state.Fields))
	for k, fs := range state.Fields {
		result[k] = fs.Value
	}
	return result, nil
}

// Merge merges a remote CRDT State into the local state using per-field LWW merge.
func (m *Map) Merge(ctx context.Context, remote *crdt.State) error {
	local, err := m.load(ctx)
	if err != nil {
		return err
	}

	for field, remoteFS := range remote.Fields {
		localFS, exists := local.Fields[field]
		if !exists || remoteFS.HLC.After(localFS.HLC) {
			local.Fields[field] = remoteFS
		}
	}

	return saveState(ctx, &m.crdtBase, local)
}

// State returns the raw crdt.State for sync purposes.
func (m *Map) State(ctx context.Context) (*crdt.State, error) {
	return m.load(ctx)
}

func (m *Map) load(ctx context.Context) (*crdt.State, error) {
	state, err := loadState[crdt.State](ctx, &m.crdtBase)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return crdt.NewState("kvcrdt", m.key), nil
	}
	return state, nil
}

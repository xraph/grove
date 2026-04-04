package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Document is a distributed nested CRDT document backed by a KV store.
// Each field at a dot-separated path is independently mergeable with its own
// CRDT type (LWW, Counter, Set, List), enabling JSON-like nested structures.
type Document struct {
	crdtBase
}

// NewDocument creates a new CRDT Document backed by the given store and key.
func NewDocument(store *kv.Store, key string, opts ...Option) *Document {
	return &Document{crdtBase: newBase(store, key, opts)}
}

// Set sets a field at the given dot-separated path with LWW semantics.
func (d *Document) Set(ctx context.Context, path string, value any) error {
	state, err := d.load(ctx)
	if err != nil {
		return err
	}

	hlc := d.now()
	if err := state.SetField(path, value, hlc, d.nodeID); err != nil {
		return fmt.Errorf("kvcrdt: doc set %s: %w", path, err)
	}

	return saveState(ctx, &d.crdtBase, state)
}

// Get returns the value at the given path, decoded into dest.
func (d *Document) Get(ctx context.Context, path string, dest any) error {
	state, err := d.load(ctx)
	if err != nil {
		return err
	}

	fs := state.GetField(path)
	if fs == nil {
		return kv.ErrNotFound
	}

	return json.Unmarshal(fs.Value, dest)
}

// Delete removes a field and all its children at the given path.
func (d *Document) Delete(ctx context.Context, path string) error {
	state, err := d.load(ctx)
	if err != nil {
		return err
	}

	state.DeleteField(path)

	return saveState(ctx, &d.crdtBase, state)
}

// SetCounter sets a counter field at the given path, incrementing by delta.
// If the path does not yet have a counter, a new PNCounterState is created.
func (d *Document) SetCounter(ctx context.Context, path string, delta int64) error {
	state, err := d.load(ctx)
	if err != nil {
		return err
	}

	hlc := d.now()
	fs := state.GetField(path)

	var counter *crdt.PNCounterState
	if fs != nil && fs.Type == crdt.TypeCounter && fs.CounterState != nil {
		counter = fs.CounterState
	} else {
		counter = crdt.NewPNCounterState()
	}

	if delta >= 0 {
		counter.Increment(d.nodeID, delta)
	} else {
		counter.Decrement(d.nodeID, -delta)
	}

	state.SetFieldState(path, &crdt.FieldState{
		Type:         crdt.TypeCounter,
		HLC:          hlc,
		NodeID:       d.nodeID,
		CounterState: counter,
	})

	return saveState(ctx, &d.crdtBase, state)
}

// SetFieldState sets a typed field state at a path.
// Use this for advanced use cases where you need to set a specific
// CRDT type (counter, set, list) at a document path.
func (d *Document) SetFieldState(ctx context.Context, path string, fs *crdt.FieldState) error {
	state, err := d.load(ctx)
	if err != nil {
		return err
	}

	state.SetFieldState(path, fs)

	return saveState(ctx, &d.crdtBase, state)
}

// Resolve returns the full document as a nested map by materializing all
// field paths into a tree structure.
func (d *Document) Resolve(ctx context.Context) (map[string]any, error) {
	state, err := d.load(ctx)
	if err != nil {
		return nil, err
	}
	return state.Resolve(), nil
}

// Merge merges a remote document state into the local state.
func (d *Document) Merge(ctx context.Context, remote *crdt.DocumentCRDTState) error {
	local, err := d.load(ctx)
	if err != nil {
		return err
	}

	merged, err := crdt.MergeDocument(local, remote)
	if err != nil {
		return fmt.Errorf("kvcrdt: doc merge: %w", err)
	}

	return saveState(ctx, &d.crdtBase, merged)
}

// State returns the raw DocumentCRDTState for sync purposes.
func (d *Document) State(ctx context.Context) (*crdt.DocumentCRDTState, error) {
	return d.load(ctx)
}

func (d *Document) load(ctx context.Context) (*crdt.DocumentCRDTState, error) {
	state, err := loadState[crdt.DocumentCRDTState](ctx, &d.crdtBase)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return crdt.NewDocumentCRDTState(), nil
	}
	return state, nil
}

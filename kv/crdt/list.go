package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// List is a distributed RGA (Replicated Growable Array) backed by a KV store.
// It stores a crdt.RGAListState under the key, providing ordered sequence
// semantics with support for concurrent inserts, deletes, and moves.
type List[T any] struct {
	crdtBase
}

// NewList creates a new CRDT RGA List backed by the given store and key.
func NewList[T any](store *kv.Store, key string, opts ...Option) *List[T] {
	return &List[T]{crdtBase: newBase(store, key, opts)}
}

// Append adds an element to the end of the list.
// The element is inserted after the last visible element.
func (l *List[T]) Append(ctx context.Context, value T) error {
	state, err := l.load(ctx)
	if err != nil {
		return err
	}

	// Find the last visible node to use as parent.
	var parentID crdt.HLC
	ids := state.NodeIDs()
	if len(ids) > 0 {
		parentID = ids[len(ids)-1]
	}

	hlc := l.now()
	if err := state.Insert(value, parentID, l.nodeID, hlc); err != nil {
		return fmt.Errorf("kvcrdt: list append: %w", err)
	}

	return saveState(ctx, &l.crdtBase, state)
}

// InsertAfter inserts an element after the given position ID.
// Use a zero HLC to insert at the beginning of the list.
func (l *List[T]) InsertAfter(ctx context.Context, afterID crdt.HLC, value T) error {
	state, err := l.load(ctx)
	if err != nil {
		return err
	}

	hlc := l.now()
	if err := state.Insert(value, afterID, l.nodeID, hlc); err != nil {
		return fmt.Errorf("kvcrdt: list insert: %w", err)
	}

	return saveState(ctx, &l.crdtBase, state)
}

// Delete removes the element at the given position ID by marking it as tombstoned.
func (l *List[T]) Delete(ctx context.Context, id crdt.HLC) error {
	state, err := l.load(ctx)
	if err != nil {
		return err
	}

	state.Delete(id)

	return saveState(ctx, &l.crdtBase, state)
}

// Elements returns all visible (non-tombstoned) elements in order.
func (l *List[T]) Elements(ctx context.Context) ([]T, error) {
	state, err := l.load(ctx)
	if err != nil {
		return nil, err
	}

	elements := state.Elements()
	result := make([]T, 0, len(elements))
	for _, raw := range elements {
		var elem T
		if err := json.Unmarshal(raw, &elem); err != nil {
			return nil, fmt.Errorf("kvcrdt: list decode element: %w", err)
		}
		result = append(result, elem)
	}
	return result, nil
}

// NodeIDs returns the HLC IDs of visible elements in order.
// Use these IDs for InsertAfter and Delete operations.
func (l *List[T]) NodeIDs(ctx context.Context) ([]crdt.HLC, error) {
	state, err := l.load(ctx)
	if err != nil {
		return nil, err
	}
	return state.NodeIDs(), nil
}

// Len returns the number of visible (non-tombstoned) elements.
func (l *List[T]) Len(ctx context.Context) (int, error) {
	state, err := l.load(ctx)
	if err != nil {
		return 0, err
	}
	return state.Len(), nil
}

// Merge merges a remote RGA list state into the local state.
func (l *List[T]) Merge(ctx context.Context, remote *crdt.RGAListState) error {
	local, err := l.load(ctx)
	if err != nil {
		return err
	}

	merged := crdt.MergeList(local, remote)
	return saveState(ctx, &l.crdtBase, merged)
}

// State returns the raw RGAListState for sync purposes.
func (l *List[T]) State(ctx context.Context) (*crdt.RGAListState, error) {
	return l.load(ctx)
}

func (l *List[T]) load(ctx context.Context) (*crdt.RGAListState, error) {
	state, err := loadState[crdt.RGAListState](ctx, &l.crdtBase)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return crdt.NewRGAListState(), nil
	}
	return state, nil
}

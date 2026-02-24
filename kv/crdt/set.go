package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Set is a distributed OR-Set (Observed-Remove Set) backed by a KV store.
// Concurrent add and remove of the same element results in the element
// being present (add-wins semantics).
type Set[T any] struct {
	crdtBase
}

// NewSet creates a new CRDT OR-Set backed by the given store and key.
func NewSet[T any](store *kv.Store, key string, opts ...Option) *Set[T] {
	return &Set[T]{crdtBase: newBase(store, key, opts)}
}

// Add inserts an element into the set.
func (s *Set[T]) Add(ctx context.Context, element T) error {
	state, err := s.load(ctx)
	if err != nil {
		return err
	}

	hlc := s.now()
	if err := state.Add(element, s.nodeID, hlc); err != nil {
		return fmt.Errorf("kvcrdt: set add: %w", err)
	}

	return saveState(ctx, &s.crdtBase, state)
}

// Remove removes an element from the set by marking all its current tags as removed.
func (s *Set[T]) Remove(ctx context.Context, element T) error {
	state, err := s.load(ctx)
	if err != nil {
		return err
	}

	if err := state.Remove(element); err != nil {
		return fmt.Errorf("kvcrdt: set remove: %w", err)
	}

	return saveState(ctx, &s.crdtBase, state)
}

// Members returns all elements currently in the set.
func (s *Set[T]) Members(ctx context.Context) ([]T, error) {
	state, err := s.load(ctx)
	if err != nil {
		return nil, err
	}

	elements := state.Elements()
	result := make([]T, 0, len(elements))
	for _, raw := range elements {
		var elem T
		if err := json.Unmarshal(raw, &elem); err != nil {
			return nil, fmt.Errorf("kvcrdt: set decode element: %w", err)
		}
		result = append(result, elem)
	}
	return result, nil
}

// Contains returns true if the element is in the set.
func (s *Set[T]) Contains(ctx context.Context, element T) (bool, error) {
	state, err := s.load(ctx)
	if err != nil {
		return false, err
	}

	return state.Contains(element)
}

// Merge merges a remote OR-Set state into the local state.
func (s *Set[T]) Merge(ctx context.Context, remote *crdt.ORSetState) error {
	local, err := s.load(ctx)
	if err != nil {
		return err
	}

	merged := crdt.MergeSet(local, remote)
	return saveState(ctx, &s.crdtBase, merged)
}

// State returns the raw ORSetState for sync purposes.
func (s *Set[T]) State(ctx context.Context) (*crdt.ORSetState, error) {
	return s.load(ctx)
}

func (s *Set[T]) load(ctx context.Context) (*crdt.ORSetState, error) {
	state, err := loadState[crdt.ORSetState](ctx, &s.crdtBase)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return crdt.NewORSetState(), nil
	}
	return state, nil
}

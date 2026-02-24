package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Register is a distributed LWW-Register backed by a KV store.
// The value with the highest HLC timestamp wins.
type Register[T any] struct {
	crdtBase
}

// NewRegister creates a new CRDT LWW-Register backed by the given store and key.
func NewRegister[T any](store *kv.Store, key string, opts ...Option) *Register[T] {
	return &Register[T]{crdtBase: newBase(store, key, opts)}
}

// Set writes a new value, timestamped with the local HLC.
func (r *Register[T]) Set(ctx context.Context, value T) error {
	hlc := r.now()

	reg, err := crdt.NewLWWRegister(value, hlc, r.nodeID)
	if err != nil {
		return fmt.Errorf("kvcrdt: register set: %w", err)
	}

	return saveState(ctx, &r.crdtBase, reg)
}

// Get reads the current register value.
func (r *Register[T]) Get(ctx context.Context) (T, error) {
	var zero T

	reg, err := r.load(ctx)
	if err != nil {
		return zero, err
	}
	if reg == nil {
		return zero, kv.ErrNotFound
	}

	var value T
	if err := json.Unmarshal(reg.Value, &value); err != nil {
		return zero, fmt.Errorf("kvcrdt: register decode: %w", err)
	}
	return value, nil
}

// Merge merges a remote register, keeping the winner per MergeLWW.
func (r *Register[T]) Merge(ctx context.Context, remote *crdt.LWWRegister) error {
	local, err := r.load(ctx)
	if err != nil {
		return err
	}

	merged := crdt.MergeLWW(local, remote)
	return saveState(ctx, &r.crdtBase, merged)
}

// State returns the raw LWWRegister for sync purposes.
func (r *Register[T]) State(ctx context.Context) (*crdt.LWWRegister, error) {
	return r.load(ctx)
}

func (r *Register[T]) load(ctx context.Context) (*crdt.LWWRegister, error) {
	return loadState[crdt.LWWRegister](ctx, &r.crdtBase)
}

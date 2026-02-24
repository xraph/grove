package kvcrdt

import (
	"context"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Counter is a distributed PNCounter backed by a KV store.
// It stores the full crdt.PNCounterState under the key so that
// merge is always possible across nodes.
type Counter struct {
	crdtBase
}

// NewCounter creates a new CRDT counter backed by the given store and key.
func NewCounter(store *kv.Store, key string, opts ...Option) *Counter {
	return &Counter{crdtBase: newBase(store, key, opts)}
}

// Increment adds delta to this node's increment counter.
func (c *Counter) Increment(ctx context.Context, delta int64) error {
	state, err := c.load(ctx)
	if err != nil {
		return err
	}

	state.Increment(c.nodeID, delta)

	return saveState(ctx, &c.crdtBase, state)
}

// Decrement adds delta to this node's decrement counter.
func (c *Counter) Decrement(ctx context.Context, delta int64) error {
	state, err := c.load(ctx)
	if err != nil {
		return err
	}

	state.Decrement(c.nodeID, delta)

	return saveState(ctx, &c.crdtBase, state)
}

// Value returns the current counter value (sum of all nodes).
func (c *Counter) Value(ctx context.Context) (int64, error) {
	state, err := c.load(ctx)
	if err != nil {
		return 0, err
	}
	return state.Value(), nil
}

// Merge merges a remote counter state into the local state.
func (c *Counter) Merge(ctx context.Context, remote *crdt.PNCounterState) error {
	local, err := c.load(ctx)
	if err != nil {
		return err
	}

	merged := crdt.MergeCounter(local, remote)
	return saveState(ctx, &c.crdtBase, merged)
}

// State returns the raw PNCounterState for sync purposes.
func (c *Counter) State(ctx context.Context) (*crdt.PNCounterState, error) {
	return c.load(ctx)
}

func (c *Counter) load(ctx context.Context) (*crdt.PNCounterState, error) {
	state, err := loadState[crdt.PNCounterState](ctx, &c.crdtBase)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return crdt.NewPNCounterState(), nil
	}
	return state, nil
}

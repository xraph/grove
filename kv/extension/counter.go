package extension

import (
	"context"
	"fmt"

	"github.com/xraph/grove/kv"
)

// AtomicCounter provides a distributed atomic counter backed by a KV store.
// Unlike the CRDT counter, this requires a consistent store (no eventual consistency).
type AtomicCounter struct {
	store *kv.Store
	key   string
}

// NewAtomicCounter creates a new atomic counter at the given key.
func NewAtomicCounter(store *kv.Store, key string) *AtomicCounter {
	return &AtomicCounter{
		store: store,
		key:   "counter:" + key,
	}
}

// Increment adds delta to the counter and returns the new value.
func (c *AtomicCounter) Increment(ctx context.Context, delta int64) (int64, error) {
	var current int64
	err := c.store.Get(ctx, c.key, &current)
	if err != nil && err != kv.ErrNotFound {
		return 0, fmt.Errorf("counter: get: %w", err)
	}
	current += delta
	if err := c.store.Set(ctx, c.key, current); err != nil {
		return 0, fmt.Errorf("counter: set: %w", err)
	}
	return current, nil
}

// Decrement subtracts delta from the counter and returns the new value.
func (c *AtomicCounter) Decrement(ctx context.Context, delta int64) (int64, error) {
	return c.Increment(ctx, -delta)
}

// Get returns the current counter value.
func (c *AtomicCounter) Get(ctx context.Context) (int64, error) {
	var current int64
	err := c.store.Get(ctx, c.key, &current)
	if err != nil {
		if err == kv.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}
	return current, nil
}

// Reset sets the counter to zero.
func (c *AtomicCounter) Reset(ctx context.Context) error {
	return c.store.Set(ctx, c.key, int64(0))
}

// Set sets the counter to a specific value.
func (c *AtomicCounter) Set(ctx context.Context, value int64) error {
	return c.store.Set(ctx, c.key, value)
}

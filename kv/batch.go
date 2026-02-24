package kv

import (
	"context"
	"fmt"

	"github.com/xraph/grove/kv/codec"
	"github.com/xraph/grove/kv/driver"
)

// Batch provides a fluent builder for multi-key operations.
type Batch struct {
	store *Store
	sets  map[string][]byte
	gets  []string
	dels  []string
	opts  []SetOption
}

// NewBatch creates a new batch operation builder.
func NewBatch(store *Store) *Batch {
	return &Batch{
		store: store,
		sets:  make(map[string][]byte),
	}
}

// Set adds a key-value pair to the batch.
func (b *Batch) Set(key string, value any) *Batch {
	raw, err := b.store.codec.Encode(value)
	if err == nil {
		b.sets[key] = raw
	}
	return b
}

// Get adds a key to be retrieved in the batch.
func (b *Batch) Get(keys ...string) *Batch {
	b.gets = append(b.gets, keys...)
	return b
}

// Delete adds keys to be deleted in the batch.
func (b *Batch) Delete(keys ...string) *Batch {
	b.dels = append(b.dels, keys...)
	return b
}

// WithOptions sets options for the batch Set operations.
func (b *Batch) WithOptions(opts ...SetOption) *Batch {
	b.opts = opts
	return b
}

// Exec executes all queued batch operations.
func (b *Batch) Exec(ctx context.Context) (*BatchResult, error) {
	if err := b.store.checkClosed(); err != nil {
		return nil, err
	}

	batchDrv, ok := b.store.drv.(driver.BatchDriver)
	if !ok {
		return nil, ErrNotSupported
	}

	result := &BatchResult{
		Values: make(map[string][]byte),
	}

	// Execute gets.
	if len(b.gets) > 0 {
		vals, err := batchDrv.MGet(ctx, b.gets)
		if err != nil {
			return nil, fmt.Errorf("batch get: %w", err)
		}
		for i, raw := range vals {
			if raw != nil {
				result.Values[b.gets[i]] = raw
			}
		}
	}

	// Execute sets.
	if len(b.sets) > 0 {
		so := applySetOptions(b.opts)
		if err := batchDrv.MSet(ctx, b.sets, so.ttl); err != nil {
			return nil, fmt.Errorf("batch set: %w", err)
		}
		result.Written = int64(len(b.sets))
	}

	// Execute deletes.
	if len(b.dels) > 0 {
		n, err := b.store.drv.Delete(ctx, b.dels...)
		if err != nil {
			return nil, fmt.Errorf("batch delete: %w", err)
		}
		result.Deleted = n
	}

	return result, nil
}

// BatchResult holds the results of a batch operation.
type BatchResult struct {
	// Values holds the raw bytes retrieved by Get operations, keyed by key.
	Values map[string][]byte

	// Written is the number of keys written.
	Written int64

	// Deleted is the number of keys deleted.
	Deleted int64
}

// Decode decodes a value from the batch result into dest.
func (r *BatchResult) Decode(key string, dest any, c codec.Codec) error {
	raw, ok := r.Values[key]
	if !ok {
		return ErrNotFound
	}
	return c.Decode(raw, dest)
}

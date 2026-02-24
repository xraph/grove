package kvcrdt

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/codec"
)

// crdtBase holds shared state for all CRDT KV types.
type crdtBase struct {
	store  *kv.Store
	key    string
	nodeID string
	clock  crdt.Clock
	codec  codec.Codec
}

func newBase(store *kv.Store, key string, opts []Option) crdtBase {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	base := crdtBase{
		store:  store,
		key:    key,
		nodeID: cfg.nodeID,
		codec:  cfg.codec,
	}
	if cfg.clock != nil {
		base.clock = cfg.clock
	} else {
		base.clock = crdt.NewHybridClock(cfg.nodeID)
	}
	return base
}

// now returns the current HLC timestamp and ticks the clock.
func (b *crdtBase) now() crdt.HLC {
	return b.clock.Now()
}

// loadRaw loads raw bytes from the store, returning nil if key not found.
func (b *crdtBase) loadRaw(ctx context.Context) ([]byte, error) {
	raw, err := b.store.GetRaw(ctx, b.key)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("kvcrdt: load %s: %w", b.key, err)
	}
	return raw, nil
}

// saveRaw stores raw bytes to the store.
func (b *crdtBase) saveRaw(ctx context.Context, data []byte) error {
	if err := b.store.SetRaw(ctx, b.key, data); err != nil {
		return fmt.Errorf("kvcrdt: save %s: %w", b.key, err)
	}
	return nil
}

// loadState loads and decodes a value of type T from the store.
func loadState[T any](ctx context.Context, base *crdtBase) (*T, error) {
	raw, err := base.loadRaw(ctx)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}

	var state T
	if err := base.codec.Decode(raw, &state); err != nil {
		return nil, fmt.Errorf("kvcrdt: decode %s: %w", base.key, err)
	}
	return &state, nil
}

// saveState encodes and stores a value to the store.
func saveState[T any](ctx context.Context, base *crdtBase, state *T) error {
	raw, err := base.codec.Encode(state)
	if err != nil {
		return fmt.Errorf("kvcrdt: encode %s: %w", base.key, err)
	}
	return base.saveRaw(ctx, raw)
}

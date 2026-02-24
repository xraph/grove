// Package keyspace provides typed, namespaced key partitions for Grove KV.
//
// A Keyspace[T] binds a key prefix, codec, and TTL policy to a Go type,
// providing type-safe Get/Set operations with automatic serialization.
//
//	users := keyspace.New[User](store, "users",
//	    keyspace.WithTTL(24 * time.Hour),
//	    keyspace.WithCodec(codec.MsgPack()),
//	)
//	user, err := users.Get(ctx, "u_12345")
//	err = users.Set(ctx, "u_12345", user)
package keyspace

import (
	"context"
	"fmt"
	"time"

	"github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/codec"
)

// Keyspace is a typed, namespaced partition of a KV store.
// All keys are automatically prefixed with the keyspace prefix + separator.
type Keyspace[T any] struct {
	store     *kv.Store
	prefix    string
	separator string
	codec     codec.Codec
	ttl       time.Duration
}

// New creates a new Keyspace bound to the given store and prefix.
func New[T any](store *kv.Store, prefix string, opts ...Option) *Keyspace[T] {
	cfg := &config{
		separator: ":",
		codec:     store.Codec(),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ks := &Keyspace[T]{
		store:     store,
		prefix:    prefix,
		separator: cfg.separator,
		codec:     cfg.codec,
		ttl:       cfg.ttl,
	}
	if cfg.codec != nil {
		ks.codec = cfg.codec
	}
	return ks
}

// Get retrieves and decodes the value for the given key within this keyspace.
func (ks *Keyspace[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T
	fullKey := ks.fullKey(key)

	raw, err := ks.store.GetRaw(ctx, fullKey)
	if err != nil {
		return zero, err
	}

	var dest T
	if err := ks.codec.Decode(raw, &dest); err != nil {
		return zero, fmt.Errorf("keyspace %s: decode %s: %w", ks.prefix, key, err)
	}
	return dest, nil
}

// GetEntry retrieves the value with full metadata (TTL, version, etc.).
func (ks *Keyspace[T]) GetEntry(ctx context.Context, key string) (*kv.Entry[T], error) {
	fullKey := ks.fullKey(key)

	raw, err := ks.store.GetRaw(ctx, fullKey)
	if err != nil {
		return nil, err
	}

	var value T
	if err := ks.codec.Decode(raw, &value); err != nil {
		return nil, fmt.Errorf("keyspace %s: decode %s: %w", ks.prefix, key, err)
	}

	entry := &kv.Entry[T]{
		Key:   fullKey,
		Value: value,
	}

	// Try to get TTL if supported.
	ttl, err := ks.store.TTL(ctx, fullKey)
	if err == nil {
		entry.TTL = ttl
	}

	return entry, nil
}

// Set encodes and stores the value under the given key within this keyspace.
func (ks *Keyspace[T]) Set(ctx context.Context, key string, value T, opts ...kv.SetOption) error {
	fullKey := ks.fullKey(key)

	raw, err := ks.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("keyspace %s: encode %s: %w", ks.prefix, key, err)
	}

	setOpts := make([]kv.SetOption, 0, len(opts)+1)
	if ks.ttl > 0 {
		setOpts = append(setOpts, kv.WithTTL(ks.ttl))
	}
	setOpts = append(setOpts, opts...)

	return ks.store.SetRaw(ctx, fullKey, raw, setOpts...)
}

// Delete removes one or more keys within this keyspace.
func (ks *Keyspace[T]) Delete(ctx context.Context, keys ...string) error {
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fullKeys[i] = ks.fullKey(k)
	}
	return ks.store.Delete(ctx, fullKeys...)
}

// Exists returns the count of keys that exist within this keyspace.
func (ks *Keyspace[T]) Exists(ctx context.Context, keys ...string) (int64, error) {
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fullKeys[i] = ks.fullKey(k)
	}
	return ks.store.Exists(ctx, fullKeys...)
}

// MGet retrieves multiple keys within this keyspace.
func (ks *Keyspace[T]) MGet(ctx context.Context, keys []string) (map[string]T, error) {
	fullKeys := make([]string, len(keys))
	for i, k := range keys {
		fullKeys[i] = ks.fullKey(k)
	}

	rawDest := make(map[string]any)
	if err := ks.store.MGet(ctx, fullKeys, rawDest); err != nil {
		return nil, err
	}

	result := make(map[string]T, len(rawDest))
	for i, k := range keys {
		raw, ok := rawDest[fullKeys[i]]
		if !ok {
			continue
		}
		// Re-encode and decode through our keyspace codec.
		rawBytes, err := ks.store.Codec().Encode(raw)
		if err != nil {
			continue
		}
		var v T
		if err := ks.codec.Decode(rawBytes, &v); err != nil {
			_ = k // silence unused warning
			continue
		}
		result[keys[i]] = v
	}
	return result, nil
}

// Scan iterates over keys within this keyspace matching the given pattern suffix.
func (ks *Keyspace[T]) Scan(ctx context.Context, pattern string, fn func(key string) error) error {
	fullPattern := ks.fullKey(pattern)
	return ks.store.Scan(ctx, fullPattern, func(key string) error {
		// Strip prefix from key before calling fn.
		prefixLen := len(ks.prefix) + len(ks.separator)
		if len(key) > prefixLen {
			key = key[prefixLen:]
		}
		return fn(key)
	})
}

// Prefix returns the keyspace prefix.
func (ks *Keyspace[T]) Prefix() string {
	return ks.prefix
}

// Store returns the underlying store.
func (ks *Keyspace[T]) Store() *kv.Store {
	return ks.store
}

func (ks *Keyspace[T]) fullKey(key string) string {
	return ks.prefix + ks.separator + key
}

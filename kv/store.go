package kv

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xraph/grove/hook"
	"github.com/xraph/grove/kv/codec"
	"github.com/xraph/grove/kv/driver"
)

// Store is the top-level KV handle. It manages the driver connection,
// hook engine, default codec, and provides entry points for all KV operations.
type Store struct {
	drv   driver.Driver
	hooks *hook.Engine
	codec codec.Codec

	mu     sync.RWMutex
	closed bool
}

// Open creates a new Store with the given driver and options.
// The driver must already be connected (call driver.Open before kv.Open).
func Open(drv driver.Driver, opts ...Option) (*Store, error) {
	if drv == nil {
		return nil, fmt.Errorf("kv: driver must not be nil")
	}

	o := defaultOptions()
	o.apply(opts)

	s := &Store{
		drv:   drv,
		hooks: hook.NewEngine(),
		codec: o.codec,
	}

	for _, entry := range o.hooks {
		s.hooks.AddHook(entry.hook, entry.scope)
	}

	return s, nil
}

// Get retrieves the value for key and decodes it into dest.
func (s *Store) Get(ctx context.Context, key string, dest any) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}

	qc := newCommandContext(OpGet, []string{key}, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	// Resolve potentially modified key from hooks.
	resolvedKey := resolveKey(qc)

	raw, err := s.drv.Get(ctx, resolvedKey)
	if err != nil {
		_ = s.hooks.RunPostQuery(ctx, qc, nil)
		return err
	}

	if err := s.codec.Decode(raw, dest); err != nil {
		_ = s.hooks.RunPostQuery(ctx, qc, nil)
		return fmt.Errorf("%w: %v", ErrCodecDecode, err)
	}

	return s.hooks.RunPostQuery(ctx, qc, dest)
}

// GetRaw retrieves the raw bytes for key without codec decoding.
// Used by Keyspace[T] to apply its own codec.
func (s *Store) GetRaw(ctx context.Context, key string) ([]byte, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if key == "" {
		return nil, ErrKeyEmpty
	}

	qc := newCommandContext(OpGet, []string{key}, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return nil, err
	} else if result != nil && result.Decision == hook.Deny {
		return nil, ErrHookDenied
	}

	resolvedKey := resolveKey(qc)

	raw, err := s.drv.Get(ctx, resolvedKey)
	if err != nil {
		_ = s.hooks.RunPostQuery(ctx, qc, nil)
		return nil, err
	}

	_ = s.hooks.RunPostQuery(ctx, qc, raw)
	return raw, nil
}

// Set encodes value and stores it under key.
func (s *Store) Set(ctx context.Context, key string, value any, opts ...SetOption) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}

	so := applySetOptions(opts)

	qc := newCommandContext(OpSet, []string{key}, map[string]any{
		"_kv_ttl": so.ttl,
	})
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	resolvedKey := resolveKey(qc)

	raw, err := s.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCodecEncode, err)
	}

	// Handle NX/XX via CASDriver.
	if so.nx || so.xx {
		casDriver, ok := s.drv.(driver.CASDriver)
		if !ok {
			return ErrNotSupported
		}
		if so.nx {
			ok, err := casDriver.SetNX(ctx, resolvedKey, raw, so.ttl)
			if err != nil {
				return err
			}
			if !ok {
				return ErrConflict
			}
			return s.hooks.RunPostQuery(ctx, qc, nil)
		}
		ok2, err := casDriver.SetXX(ctx, resolvedKey, raw, so.ttl)
		if err != nil {
			return err
		}
		if !ok2 {
			return ErrNotFound
		}
		return s.hooks.RunPostQuery(ctx, qc, nil)
	}

	if err := s.drv.Set(ctx, resolvedKey, raw, so.ttl); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

// SetRaw stores raw bytes under key without codec encoding.
// Used by Keyspace[T] to apply its own codec.
func (s *Store) SetRaw(ctx context.Context, key string, value []byte, opts ...SetOption) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if key == "" {
		return ErrKeyEmpty
	}

	so := applySetOptions(opts)

	qc := newCommandContext(OpSet, []string{key}, map[string]any{
		"_kv_ttl": so.ttl,
	})
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	resolvedKey := resolveKey(qc)

	if so.nx || so.xx {
		casDriver, ok := s.drv.(driver.CASDriver)
		if !ok {
			return ErrNotSupported
		}
		if so.nx {
			ok, err := casDriver.SetNX(ctx, resolvedKey, value, so.ttl)
			if err != nil {
				return err
			}
			if !ok {
				return ErrConflict
			}
			return s.hooks.RunPostQuery(ctx, qc, nil)
		}
		ok2, err := casDriver.SetXX(ctx, resolvedKey, value, so.ttl)
		if err != nil {
			return err
		}
		if !ok2 {
			return ErrNotFound
		}
		return s.hooks.RunPostQuery(ctx, qc, nil)
	}

	if err := s.drv.Set(ctx, resolvedKey, value, so.ttl); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

// Delete removes one or more keys.
func (s *Store) Delete(ctx context.Context, keys ...string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	_, err := s.deleteKeys(ctx, keys)

	return err
}

// deleteKeys is Delete returning how many keys the driver removed, which
// Batch reports and Delete does not.
func (s *Store) deleteKeys(ctx context.Context, keys []string) (int64, error) {
	qc := newCommandContext(OpDelete, keys, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return 0, err
	} else if result != nil && result.Decision == hook.Deny {
		return 0, ErrHookDenied
	}

	resolved, err := resolveKeys(qc, keys)
	if err != nil {
		return 0, err
	}

	n, err := s.drv.Delete(ctx, resolved...)
	if err != nil {
		return 0, err
	}

	return n, s.hooks.RunPostQuery(ctx, qc, nil)
}

// Exists returns the count of keys that exist.
func (s *Store) Exists(ctx context.Context, keys ...string) (int64, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}

	qc := newCommandContext(OpExists, keys, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return 0, err
	} else if result != nil && result.Decision == hook.Deny {
		return 0, ErrHookDenied
	}

	resolved, err := resolveKeys(qc, keys)
	if err != nil {
		return 0, err
	}

	count, err := s.drv.Exists(ctx, resolved...)
	if err != nil {
		return 0, err
	}

	_ = s.hooks.RunPostQuery(ctx, qc, count)
	return count, nil
}

// MGet retrieves multiple keys. dest must be a pointer to a map[string]any or similar.
func (s *Store) MGet(ctx context.Context, keys []string, dest map[string]any) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	batchDrv, ok := s.drv.(driver.BatchDriver)
	if !ok {
		return ErrNotSupported
	}

	qc := newCommandContext(OpMGet, keys, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	resolved, err := resolveKeys(qc, keys)
	if err != nil {
		return err
	}

	// Results line up with resolved by position, and so with keys: dest
	// is filled under the caller's keys, not the physical ones.
	results, err := batchDrv.MGet(ctx, resolved)
	if err != nil {
		return err
	}

	for i, raw := range results {
		if raw == nil {
			continue
		}
		var v any
		if err := s.codec.Decode(raw, &v); err != nil {
			return fmt.Errorf("%w: key %s: %v", ErrCodecDecode, keys[i], err)
		}
		dest[keys[i]] = v
	}

	return s.hooks.RunPostQuery(ctx, qc, dest)
}

// MSet sets multiple key-value pairs.
func (s *Store) MSet(ctx context.Context, pairs map[string]any, opts ...SetOption) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}

	batchDrv, ok := s.drv.(driver.BatchDriver)
	if !ok {
		return ErrNotSupported
	}

	rawPairs := make(map[string][]byte, len(pairs))
	for k, v := range pairs {
		raw, err := s.codec.Encode(v)
		if err != nil {
			return fmt.Errorf("%w: key %s: %v", ErrCodecEncode, k, err)
		}
		rawPairs[k] = raw
	}

	return s.msetRaw(ctx, batchDrv, rawPairs, applySetOptions(opts).ttl)
}

// msetRaw writes already-encoded pairs under the keys the hooks resolve
// them to. It is shared by MSet and Batch.
func (s *Store) msetRaw(
	ctx context.Context, batchDrv driver.BatchDriver, pairs map[string][]byte, ttl time.Duration,
) error {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}

	qc := newCommandContext(OpMSet, keys, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	resolved, err := resolveKeys(qc, keys)
	if err != nil {
		return err
	}

	physical := make(map[string][]byte, len(pairs))
	for i, k := range keys {
		physical[resolved[i]] = pairs[k]
	}

	if err := batchDrv.MSet(ctx, physical, ttl); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

// TTL returns the remaining TTL for key.
func (s *Store) TTL(ctx context.Context, key string) (time.Duration, error) {
	if err := s.checkClosed(); err != nil {
		return 0, err
	}

	ttlDrv, ok := s.drv.(driver.TTLDriver)
	if !ok {
		return 0, ErrNotSupported
	}

	qc := newCommandContext(OpTTL, []string{key}, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return 0, err
	} else if result != nil && result.Decision == hook.Deny {
		return 0, ErrHookDenied
	}

	ttl, err := ttlDrv.TTL(ctx, resolveKey(qc))
	if err != nil {
		return 0, err
	}

	_ = s.hooks.RunPostQuery(ctx, qc, ttl)
	return ttl, nil
}

// Expire sets the TTL on an existing key.
func (s *Store) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	ttlDrv, ok := s.drv.(driver.TTLDriver)
	if !ok {
		return ErrNotSupported
	}

	qc := newCommandContext(OpExpire, []string{key}, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	if err := ttlDrv.Expire(ctx, resolveKey(qc), ttl); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

// Scan iterates over keys matching pattern, calling fn for each.
func (s *Store) Scan(ctx context.Context, pattern string, fn func(key string) error) error {
	if err := s.checkClosed(); err != nil {
		return err
	}

	scanDrv, ok := s.drv.(driver.ScanDriver)
	if !ok {
		return ErrNotSupported
	}

	qc := newCommandContext(OpScan, []string{pattern}, nil)
	if result, err := s.hooks.RunPreQuery(ctx, qc); err != nil {
		return err
	} else if result != nil && result.Decision == hook.Deny {
		return ErrHookDenied
	}

	resolved := resolveKey(qc)

	// A namespacing hook hands back the caller's pattern with a prefix in
	// front, and the driver then yields physical keys carrying that
	// prefix. Stripping it again gives the caller keys it can pass
	// straight back to Get or Delete, which would otherwise namespace
	// them a second time. A hook that rewrites the pattern any other way
	// leaves no prefix to strip, and its keys come through as the driver
	// reports them.
	yield := fn
	if prefix, ok := strings.CutSuffix(resolved, pattern); ok && prefix != "" {
		yield = func(key string) error {
			return fn(strings.TrimPrefix(key, prefix))
		}
	}

	if err := scanDrv.Scan(ctx, resolved, yield); err != nil {
		return err
	}

	return s.hooks.RunPostQuery(ctx, qc, nil)
}

// Ping verifies the store is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	return s.drv.Ping(ctx)
}

// Close closes the store and releases all resources.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrStoreClosed
	}
	s.closed = true
	return s.drv.Close()
}

// Driver returns the underlying KV driver.
func (s *Store) Driver() driver.Driver {
	return s.drv
}

// Hooks returns the hook engine for registering additional hooks.
func (s *Store) Hooks() *hook.Engine {
	return s.hooks
}

// Codec returns the default codec.
func (s *Store) Codec() codec.Codec {
	return s.codec
}

func (s *Store) checkClosed() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrStoreClosed
	}
	return nil
}

// resolveKey extracts the (potentially modified) key from a QueryContext.
func resolveKey(qc *hook.QueryContext) string {
	if keys, ok := qc.Values["_kv_keys"].([]string); ok && len(keys) > 0 {
		return keys[0]
	}
	return qc.RawQuery
}

// resolveKeys is resolveKey for a multi-key command: the keys the
// pre-query hooks left in qc, or keys itself when no hook set any.
//
// A hook may rename keys but not add or drop them. Callers line driver
// results up with their own keys by position, so a changed count is an
// error rather than a quietly misaligned result.
func resolveKeys(qc *hook.QueryContext, keys []string) ([]string, error) {
	resolved, ok := qc.Values["_kv_keys"].([]string)
	if !ok {
		return keys, nil
	}

	if len(resolved) != len(keys) {
		return nil, fmt.Errorf("%w: %d keys became %d", ErrHookKeyCount, len(keys), len(resolved))
	}

	return resolved, nil
}

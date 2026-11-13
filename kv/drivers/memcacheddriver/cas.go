package memcacheddriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

// CompareAndSwap performs a Memcached CAS operation.
// It gets the current value with its CAS ID, applies the update function,
// and stores the result only if the CAS ID hasn't changed.
func (db *MemcachedDB) CompareAndSwap(ctx context.Context, key string, update func(current []byte) ([]byte, error)) error {
	_, err := withClient(ctx, db, func(c *memcache.Client) (struct{}, error) {
		item, err := c.Get(key)
		if err != nil {
			return struct{}{}, fmt.Errorf("memcacheddriver: cas get: %w", err)
		}

		newValue, err := update(item.Value)
		if err != nil {
			return struct{}{}, fmt.Errorf("memcacheddriver: cas update: %w", err)
		}

		item.Value = newValue
		if err := c.CompareAndSwap(item); err != nil {
			if errors.Is(err, memcache.ErrCASConflict) {
				return struct{}{}, fmt.Errorf("kv: version conflict (CAS mismatch)")
			}
			return struct{}{}, fmt.Errorf("memcacheddriver: cas: %w", err)
		}
		return struct{}{}, nil
	})
	return err
}

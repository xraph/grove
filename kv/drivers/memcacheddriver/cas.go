package memcacheddriver

import (
	"context"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

// CompareAndSwap performs a Memcached CAS operation.
// It gets the current value with its CAS ID, applies the update function,
// and stores the result only if the CAS ID hasn't changed.
func (db *MemcachedDB) CompareAndSwap(_ context.Context, key string, update func(current []byte) ([]byte, error)) error {
	item, err := db.client.Get(key)
	if err != nil {
		return fmt.Errorf("memcacheddriver: cas get: %w", err)
	}

	newValue, err := update(item.Value)
	if err != nil {
		return fmt.Errorf("memcacheddriver: cas update: %w", err)
	}

	item.Value = newValue
	if err := db.client.CompareAndSwap(item); err != nil {
		if err == memcache.ErrCASConflict {
			return fmt.Errorf("kv: version conflict (CAS mismatch)")
		}
		return fmt.Errorf("memcacheddriver: cas: %w", err)
	}
	return nil
}

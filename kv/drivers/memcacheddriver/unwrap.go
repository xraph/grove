package memcacheddriver

import (
	"github.com/bradfitz/gomemcache/memcache"

	"github.com/xraph/grove/kv"
)

// Unwrap extracts the underlying MemcachedDB from a Store.
func Unwrap(store *kv.Store) *MemcachedDB {
	mdb, ok := store.Driver().(*MemcachedDB)
	if !ok {
		panic("memcacheddriver: driver is not a *MemcachedDB")
	}
	return mdb
}

// UnwrapClient extracts the gomemcache Client from a Store.
func UnwrapClient(store *kv.Store) *memcache.Client {
	return Unwrap(store).Client()
}

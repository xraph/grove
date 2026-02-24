package boltdriver

import (
	"go.etcd.io/bbolt"

	"github.com/xraph/grove/kv"
)

// Unwrap extracts the underlying BoltDB driver from a Store.
func Unwrap(store *kv.Store) *BoltDB {
	if d, ok := store.Driver().(*BoltDB); ok {
		return d
	}
	return nil
}

// UnwrapDB extracts the underlying bbolt.DB from a Store.
func UnwrapDB(store *kv.Store) *bbolt.DB {
	if d := Unwrap(store); d != nil {
		return d.db
	}
	return nil
}

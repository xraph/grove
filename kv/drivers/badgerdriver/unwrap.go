package badgerdriver

import (
	"github.com/dgraph-io/badger/v4"

	"github.com/xraph/grove/kv"
)

// Unwrap extracts the underlying BadgerDB from a Store.
func Unwrap(store *kv.Store) *BadgerDB {
	bdb, ok := store.Driver().(*BadgerDB)
	if !ok {
		panic("badgerdriver: driver is not a *BadgerDB")
	}
	return bdb
}

// UnwrapDB extracts the underlying badger.DB from a Store.
func UnwrapDB(store *kv.Store) *badger.DB {
	return Unwrap(store).DB()
}

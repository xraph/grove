package badgerdriver

import (
	"context"

	"github.com/dgraph-io/badger/v4"
)

// PrefixScan iterates over all keys with the given prefix, calling fn for each key-value pair.
func (db *BadgerDB) PrefixScan(_ context.Context, prefix string, fn func(key string, value []byte) error) error {
	return db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// RangeScan iterates over all keys in the range [start, end), calling fn for each.
func (db *BadgerDB) RangeScan(_ context.Context, start, end string, fn func(key string, value []byte) error) error {
	return db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(start)); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			if key >= end {
				break
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

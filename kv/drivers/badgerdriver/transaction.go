package badgerdriver

import "github.com/dgraph-io/badger/v4"

// ViewTxn executes a read-only transaction.
func (db *BadgerDB) ViewTxn(fn func(txn *badger.Txn) error) error {
	return db.db.View(fn)
}

// UpdateTxn executes a read-write transaction.
func (db *BadgerDB) UpdateTxn(fn func(txn *badger.Txn) error) error {
	return db.db.Update(fn)
}

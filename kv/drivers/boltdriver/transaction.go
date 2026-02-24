package boltdriver

import "go.etcd.io/bbolt"

// ViewTxn executes a read-only transaction.
func (b *BoltDB) ViewTxn(fn func(tx *bbolt.Tx) error) error {
	return b.db.View(fn)
}

// UpdateTxn executes a read-write transaction.
func (b *BoltDB) UpdateTxn(fn func(tx *bbolt.Tx) error) error {
	return b.db.Update(fn)
}

// Batch executes a function within a batch transaction for higher throughput writes.
func (b *BoltDB) Batch(fn func(tx *bbolt.Tx) error) error {
	return b.db.Batch(fn)
}

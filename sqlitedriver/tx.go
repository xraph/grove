package sqlitedriver

import (
	"context"

	"github.com/xraph/grove/driver"
)

// SqliteTx wraps a driver.Tx and exposes query builder methods.
// Queries created from a SqliteTx execute within the transaction.
type SqliteTx struct {
	db *SqliteDB // parent db (for dialect, schema access)
	tx driver.Tx
}

// BeginTxQuery starts a new transaction and returns a SqliteTx that exposes
// query builder methods operating within that transaction.
func (db *SqliteDB) BeginTxQuery(ctx context.Context, opts *driver.TxOptions) (*SqliteTx, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &SqliteTx{db: db, tx: tx}, nil
}

// NewSelect creates a SELECT query that executes within the transaction.
func (t *SqliteTx) NewSelect(model ...any) *SelectQuery {
	q := t.db.NewSelect(model...)
	q.db = t.txDB()
	return q
}

// NewInsert creates an INSERT query that executes within the transaction.
func (t *SqliteTx) NewInsert(model any) *InsertQuery {
	q := t.db.NewInsert(model)
	q.db = t.txDB()
	return q
}

// NewUpdate creates an UPDATE query that executes within the transaction.
func (t *SqliteTx) NewUpdate(model any) *UpdateQuery {
	q := t.db.NewUpdate(model)
	q.db = t.txDB()
	return q
}

// NewDelete creates a DELETE query that executes within the transaction.
func (t *SqliteTx) NewDelete(model any) *DeleteQuery {
	q := t.db.NewDelete(model)
	q.db = t.txDB()
	return q
}

// NewRaw creates a raw SQL query that executes within the transaction.
func (t *SqliteTx) NewRaw(query string, args ...any) *RawQuery {
	q := t.db.NewRaw(query, args...)
	q.db = t.txDB()
	return q
}

// Commit commits the transaction.
func (t *SqliteTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. Safe to call after Commit.
func (t *SqliteTx) Rollback() error {
	return t.tx.Rollback()
}

// txDB creates a thin SqliteDB wrapper that routes Exec/Query/QueryRow through
// the transaction instead of the pool.
func (t *SqliteTx) txDB() *SqliteDB {
	return &SqliteDB{
		dialect:  t.db.dialect,
		opts:     t.db.opts,
		txConn:   t.tx,
		registry: t.db.registry,
	}
}

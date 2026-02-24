package pgdriver

import (
	"context"

	"github.com/xraph/grove/driver"
)

// PgTx wraps a driver.Tx and exposes query builder methods.
// Queries created from a PgTx execute within the transaction.
type PgTx struct {
	db *PgDB // parent db (for dialect, schema access)
	tx driver.Tx
}

// BeginTx starts a new transaction and returns a PgTx that exposes
// query builder methods operating within that transaction.
func (db *PgDB) BeginTxQuery(ctx context.Context, opts *driver.TxOptions) (*PgTx, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &PgTx{db: db, tx: tx}, nil
}

// NewSelect creates a SELECT query that executes within the transaction.
func (t *PgTx) NewSelect(model ...any) *SelectQuery {
	q := t.db.NewSelect(model...)
	q.db = t.txDB()
	return q
}

// NewInsert creates an INSERT query that executes within the transaction.
func (t *PgTx) NewInsert(model any) *InsertQuery {
	q := t.db.NewInsert(model)
	q.db = t.txDB()
	return q
}

// NewUpdate creates an UPDATE query that executes within the transaction.
func (t *PgTx) NewUpdate(model any) *UpdateQuery {
	q := t.db.NewUpdate(model)
	q.db = t.txDB()
	return q
}

// NewDelete creates a DELETE query that executes within the transaction.
func (t *PgTx) NewDelete(model any) *DeleteQuery {
	q := t.db.NewDelete(model)
	q.db = t.txDB()
	return q
}

// NewRaw creates a raw SQL query that executes within the transaction.
func (t *PgTx) NewRaw(query string, args ...any) *RawQuery {
	q := t.db.NewRaw(query, args...)
	q.db = t.txDB()
	return q
}

// Commit commits the transaction.
func (t *PgTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. Safe to call after Commit.
func (t *PgTx) Rollback() error {
	return t.tx.Rollback()
}

// txDB creates a thin PgDB wrapper that routes Exec/Query/QueryRow through
// the transaction instead of the pool.
func (t *PgTx) txDB() *PgDB {
	return &PgDB{
		dialect:  t.db.dialect,
		opts:     t.db.opts,
		txConn:   t.tx,
		registry: t.db.registry,
	}
}

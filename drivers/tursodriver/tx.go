package tursodriver

import (
	"context"

	"github.com/xraph/grove/driver"
)

// TursoTx wraps a driver.Tx and exposes query builder methods.
// Queries created from a TursoTx execute within the transaction.
type TursoTx struct {
	db *TursoDB // parent db (for dialect, schema access)
	tx driver.Tx
}

// BeginTxQuery starts a new transaction and returns a TursoTx that exposes
// query builder methods operating within that transaction.
func (db *TursoDB) BeginTxQuery(ctx context.Context, opts *driver.TxOptions) (*TursoTx, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &TursoTx{db: db, tx: tx}, nil
}

// NewSelect creates a SELECT query that executes within the transaction.
func (t *TursoTx) NewSelect(model ...any) *SelectQuery {
	q := t.db.NewSelect(model...)
	q.db = t.txDB()
	return q
}

// NewInsert creates an INSERT query that executes within the transaction.
func (t *TursoTx) NewInsert(model any) *InsertQuery {
	q := t.db.NewInsert(model)
	q.db = t.txDB()
	return q
}

// NewUpdate creates an UPDATE query that executes within the transaction.
func (t *TursoTx) NewUpdate(model any) *UpdateQuery {
	q := t.db.NewUpdate(model)
	q.db = t.txDB()
	return q
}

// NewDelete creates a DELETE query that executes within the transaction.
func (t *TursoTx) NewDelete(model any) *DeleteQuery {
	q := t.db.NewDelete(model)
	q.db = t.txDB()
	return q
}

// NewRaw creates a raw SQL query that executes within the transaction.
func (t *TursoTx) NewRaw(query string, args ...any) *RawQuery {
	q := t.db.NewRaw(query, args...)
	q.db = t.txDB()
	return q
}

// Commit commits the transaction.
func (t *TursoTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. Safe to call after Commit.
func (t *TursoTx) Rollback() error {
	return t.tx.Rollback()
}

// txDB creates a thin TursoDB wrapper that routes Exec/Query/QueryRow through
// the transaction instead of the pool.
func (t *TursoTx) txDB() *TursoDB {
	return &TursoDB{
		dialect:  t.db.dialect,
		opts:     t.db.opts,
		txConn:   t.tx,
		registry: t.db.registry,
	}
}

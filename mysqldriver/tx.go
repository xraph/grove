package mysqldriver

import (
	"context"

	"github.com/xraph/grove/driver"
)

// MysqlTx wraps a driver.Tx and exposes query builder methods.
// Queries created from a MysqlTx execute within the transaction.
type MysqlTx struct {
	db *MysqlDB // parent db (for dialect, schema access)
	tx driver.Tx
}

// BeginTxQuery starts a new transaction and returns a MysqlTx that exposes
// query builder methods operating within that transaction.
func (db *MysqlDB) BeginTxQuery(ctx context.Context, opts *driver.TxOptions) (*MysqlTx, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &MysqlTx{db: db, tx: tx}, nil
}

// NewSelect creates a SELECT query that executes within the transaction.
func (t *MysqlTx) NewSelect(model ...any) *SelectQuery {
	q := t.db.NewSelect(model...)
	q.db = t.txDB()
	return q
}

// NewInsert creates an INSERT query that executes within the transaction.
func (t *MysqlTx) NewInsert(model any) *InsertQuery {
	q := t.db.NewInsert(model)
	q.db = t.txDB()
	return q
}

// NewUpdate creates an UPDATE query that executes within the transaction.
func (t *MysqlTx) NewUpdate(model any) *UpdateQuery {
	q := t.db.NewUpdate(model)
	q.db = t.txDB()
	return q
}

// NewDelete creates a DELETE query that executes within the transaction.
func (t *MysqlTx) NewDelete(model any) *DeleteQuery {
	q := t.db.NewDelete(model)
	q.db = t.txDB()
	return q
}

// NewRaw creates a raw SQL query that executes within the transaction.
func (t *MysqlTx) NewRaw(query string, args ...any) *RawQuery {
	q := t.db.NewRaw(query, args...)
	q.db = t.txDB()
	return q
}

// Commit commits the transaction.
func (t *MysqlTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. Safe to call after Commit.
func (t *MysqlTx) Rollback() error {
	return t.tx.Rollback()
}

// txDB creates a thin MysqlDB wrapper that routes Exec/Query/QueryRow through
// the transaction instead of the pool.
func (t *MysqlTx) txDB() *MysqlDB {
	return &MysqlDB{
		dialect: t.db.dialect,
		opts:    t.db.opts,
		txConn:  t.tx,
	}
}

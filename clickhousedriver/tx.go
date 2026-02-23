package clickhousedriver

import (
	"context"

	"github.com/xraph/grove/driver"
)

// ClickHouseTx wraps a driver.Tx and exposes query builder methods.
// Queries created from a ClickHouseTx execute within the transaction.
// Note: ClickHouse has limited transaction support.
type ClickHouseTx struct {
	db *ClickHouseDB // parent db (for dialect, schema access)
	tx driver.Tx
}

// BeginTxQuery starts a new transaction and returns a ClickHouseTx that exposes
// query builder methods operating within that transaction.
func (db *ClickHouseDB) BeginTxQuery(ctx context.Context, opts *driver.TxOptions) (*ClickHouseTx, error) {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouseTx{db: db, tx: tx}, nil
}

// NewSelect creates a SELECT query that executes within the transaction.
func (t *ClickHouseTx) NewSelect(model ...any) *SelectQuery {
	q := t.db.NewSelect(model...)
	q.db = t.txDB()
	return q
}

// NewInsert creates an INSERT query that executes within the transaction.
func (t *ClickHouseTx) NewInsert(model any) *InsertQuery {
	q := t.db.NewInsert(model)
	q.db = t.txDB()
	return q
}

// NewUpdate creates an UPDATE query that executes within the transaction.
func (t *ClickHouseTx) NewUpdate(model any) *UpdateQuery {
	q := t.db.NewUpdate(model)
	q.db = t.txDB()
	return q
}

// NewDelete creates a DELETE query that executes within the transaction.
func (t *ClickHouseTx) NewDelete(model any) *DeleteQuery {
	q := t.db.NewDelete(model)
	q.db = t.txDB()
	return q
}

// NewRaw creates a raw SQL query that executes within the transaction.
func (t *ClickHouseTx) NewRaw(query string, args ...any) *RawQuery {
	q := t.db.NewRaw(query, args...)
	q.db = t.txDB()
	return q
}

// NewAlterUpdate creates an ALTER TABLE ... UPDATE query within the transaction.
func (t *ClickHouseTx) NewAlterUpdate(model any) *AlterUpdateQuery {
	q := t.db.NewAlterUpdate(model)
	q.db = t.txDB()
	return q
}

// NewAlterDelete creates an ALTER TABLE ... DELETE query within the transaction.
func (t *ClickHouseTx) NewAlterDelete(model any) *AlterDeleteQuery {
	q := t.db.NewAlterDelete(model)
	q.db = t.txDB()
	return q
}

// Commit commits the transaction.
func (t *ClickHouseTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. Safe to call after Commit.
func (t *ClickHouseTx) Rollback() error {
	return t.tx.Rollback()
}

// txDB creates a thin ClickHouseDB wrapper that routes Exec/Query/QueryRow through
// the transaction instead of the pool.
func (t *ClickHouseTx) txDB() *ClickHouseDB {
	return &ClickHouseDB{
		dialect:  t.db.dialect,
		opts:     t.db.opts,
		txConn:   t.tx,
		registry: t.db.registry,
	}
}

package tursodriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// tursoResult wraps sql.Result to implement driver.Result.
type tursoResult struct {
	res sql.Result
}

var _ driver.Result = (*tursoResult)(nil)

// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or
// DELETE statement.
func (r *tursoResult) RowsAffected() (int64, error) {
	return r.res.RowsAffected()
}

// LastInsertId returns the last auto-generated ID.
func (r *tursoResult) LastInsertId() (int64, error) {
	return r.res.LastInsertId()
}

// tursoRows wraps *sql.Rows to implement driver.Rows.
type tursoRows struct {
	rows *sql.Rows
}

var _ driver.Rows = (*tursoRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *tursoRows) Next() bool {
	return r.rows.Next()
}

// Scan copies the current row's column values into dest.
func (r *tursoRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (r *tursoRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Close releases all resources held by the rows iterator.
func (r *tursoRows) Close() error {
	return r.rows.Close()
}

// Err returns any error encountered during iteration.
func (r *tursoRows) Err() error {
	return r.rows.Err()
}

// tursoRow wraps *sql.Row to implement driver.Row.
type tursoRow struct {
	row *sql.Row
}

var _ driver.Row = (*tursoRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns sql.ErrNoRows.
func (r *tursoRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// tursoTx wraps *sql.Tx to implement driver.Tx.
type tursoTx struct {
	tx *sql.Tx
}

var _ driver.Tx = (*tursoTx)(nil)
var _ driver.Preparer = (*tursoTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *tursoTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: tx exec: %w", err)
	}
	return &tursoResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *tursoTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: tx query: %w", err)
	}
	return &tursoRows{rows: rows}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *tursoTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &tursoRow{row: row}
}

// Commit commits the transaction.
func (t *tursoTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. It is safe to call after Commit; the
// underlying sql driver will return nil if the transaction is already closed.
func (t *tursoTx) Rollback() error {
	return t.tx.Rollback()
}

// Prepare creates a prepared statement within the transaction.
func (t *tursoTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: tx prepare: %w", err)
	}
	return &tursoStmt{stmt: stmt}, nil
}

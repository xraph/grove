package clickhousedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// chResult wraps sql.Result to implement driver.Result.
type chResult struct {
	res sql.Result
}

var _ driver.Result = (*chResult)(nil)

// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or
// DELETE statement.
func (r *chResult) RowsAffected() (int64, error) {
	return r.res.RowsAffected()
}

// LastInsertId returns the last auto-generated ID.
// Note: ClickHouse does not support auto-increment IDs in the traditional sense.
func (r *chResult) LastInsertId() (int64, error) {
	return r.res.LastInsertId()
}

// chRows wraps *sql.Rows to implement driver.Rows.
type chRows struct {
	rows *sql.Rows
}

var _ driver.Rows = (*chRows)(nil)

// Next advances the cursor to the next row.
func (r *chRows) Next() bool {
	return r.rows.Next()
}

// Scan copies the current row's column values into dest.
func (r *chRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (r *chRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Close releases all resources held by the rows iterator.
func (r *chRows) Close() error {
	return r.rows.Close()
}

// Err returns any error encountered during iteration.
func (r *chRows) Err() error {
	return r.rows.Err()
}

// chRow wraps *sql.Row to implement driver.Row.
type chRow struct {
	row *sql.Row
}

var _ driver.Row = (*chRow)(nil)

// Scan copies the single row's columns into dest.
func (r *chRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// chTx wraps *sql.Tx to implement driver.Tx.
// Note: ClickHouse has limited transaction support.
type chTx struct {
	tx *sql.Tx
}

var _ driver.Tx = (*chTx)(nil)
var _ driver.Preparer = (*chTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *chTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: tx exec: %w", err)
	}
	return &chResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *chTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: tx query: %w", err)
	}
	return &chRows{rows: rows}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *chTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &chRow{row: row}
}

// Commit commits the transaction.
func (t *chTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction.
func (t *chTx) Rollback() error {
	return t.tx.Rollback()
}

// Prepare creates a prepared statement within the transaction.
func (t *chTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: tx prepare: %w", err)
	}
	return &chStmt{stmt: stmt}, nil
}

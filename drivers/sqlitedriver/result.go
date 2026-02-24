package sqlitedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// sqliteResult wraps sql.Result to implement driver.Result.
type sqliteResult struct {
	res sql.Result
}

var _ driver.Result = (*sqliteResult)(nil)

// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or
// DELETE statement.
func (r *sqliteResult) RowsAffected() (int64, error) {
	return r.res.RowsAffected()
}

// LastInsertId returns the last auto-generated ID.
func (r *sqliteResult) LastInsertId() (int64, error) {
	return r.res.LastInsertId()
}

// sqliteRows wraps *sql.Rows to implement driver.Rows.
type sqliteRows struct {
	rows *sql.Rows
}

var _ driver.Rows = (*sqliteRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *sqliteRows) Next() bool {
	return r.rows.Next()
}

// Scan copies the current row's column values into dest.
func (r *sqliteRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (r *sqliteRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Close releases all resources held by the rows iterator.
func (r *sqliteRows) Close() error {
	return r.rows.Close()
}

// Err returns any error encountered during iteration.
func (r *sqliteRows) Err() error {
	return r.rows.Err()
}

// sqliteRow wraps *sql.Row to implement driver.Row.
type sqliteRow struct {
	row *sql.Row
}

var _ driver.Row = (*sqliteRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns sql.ErrNoRows.
func (r *sqliteRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// sqliteTx wraps *sql.Tx to implement driver.Tx.
type sqliteTx struct {
	tx *sql.Tx
}

var _ driver.Tx = (*sqliteTx)(nil)
var _ driver.Preparer = (*sqliteTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *sqliteTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: tx exec: %w", err)
	}
	return &sqliteResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *sqliteTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: tx query: %w", err)
	}
	return &sqliteRows{rows: rows}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *sqliteTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &sqliteRow{row: row}
}

// Commit commits the transaction.
func (t *sqliteTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. It is safe to call after Commit; the
// underlying sql driver will return nil if the transaction is already closed.
func (t *sqliteTx) Rollback() error {
	return t.tx.Rollback()
}

// Prepare creates a prepared statement within the transaction.
func (t *sqliteTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: tx prepare: %w", err)
	}
	return &sqliteStmt{stmt: stmt}, nil
}

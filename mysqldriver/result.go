package mysqldriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// mysqlResult wraps sql.Result to implement driver.Result.
type mysqlResult struct {
	res sql.Result
}

var _ driver.Result = (*mysqlResult)(nil)

// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or
// DELETE statement.
func (r *mysqlResult) RowsAffected() (int64, error) {
	return r.res.RowsAffected()
}

// LastInsertId returns the last auto-generated ID from an INSERT statement.
func (r *mysqlResult) LastInsertId() (int64, error) {
	return r.res.LastInsertId()
}

// mysqlRows wraps *sql.Rows to implement driver.Rows.
type mysqlRows struct {
	rows *sql.Rows
}

var _ driver.Rows = (*mysqlRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *mysqlRows) Next() bool {
	return r.rows.Next()
}

// Scan copies the current row's column values into dest.
func (r *mysqlRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (r *mysqlRows) Columns() ([]string, error) {
	return r.rows.Columns()
}

// Close releases all resources held by the rows iterator.
func (r *mysqlRows) Close() error {
	return r.rows.Close()
}

// Err returns any error encountered during iteration.
func (r *mysqlRows) Err() error {
	return r.rows.Err()
}

// mysqlRow wraps *sql.Row to implement driver.Row.
type mysqlRow struct {
	row *sql.Row
}

var _ driver.Row = (*mysqlRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns sql.ErrNoRows.
func (r *mysqlRow) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

// mysqlTx wraps *sql.Tx to implement driver.Tx.
type mysqlTx struct {
	tx *sql.Tx
}

var _ driver.Tx = (*mysqlTx)(nil)
var _ driver.Preparer = (*mysqlTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *mysqlTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	res, err := t.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: tx exec: %w", err)
	}
	return &mysqlResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *mysqlTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: tx query: %w", err)
	}
	return &mysqlRows{rows: rows}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *mysqlTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &mysqlRow{row: row}
}

// Commit commits the transaction.
func (t *mysqlTx) Commit() error {
	return t.tx.Commit()
}

// Rollback rolls back the transaction. It is safe to call after Commit; the
// underlying sql driver will return sql.ErrTxDone if the transaction is
// already closed.
func (t *mysqlTx) Rollback() error {
	return t.tx.Rollback()
}

// Prepare creates a prepared statement within the transaction.
func (t *mysqlTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: tx prepare: %w", err)
	}
	return &mysqlStmt{stmt: stmt}, nil
}

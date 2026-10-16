package mysqldriver

import (
	"context"
	"database/sql"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
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
	op   *observability.Op // ends when the rows are drained or closed
}

var _ driver.Rows = (*mysqlRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *mysqlRows) Next() bool {
	if r.rows.Next() {
		return true
	}
	r.op.End(r.rows.Err())
	return false
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
	err := r.rows.Close()
	if err != nil {
		r.op.End(err)
	} else {
		r.op.End(r.rows.Err())
	}
	return err
}

// Err returns any error encountered during iteration.
func (r *mysqlRows) Err() error {
	return r.rows.Err()
}

// mysqlRow wraps *sql.Row to implement driver.Row.
type mysqlRow struct {
	row *sql.Row
	op  *observability.Op // ends at Scan, where the row's error surfaces
}

var _ driver.Row = (*mysqlRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns sql.ErrNoRows.
func (r *mysqlRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.op.End(err)
	return err
}

// mysqlTx wraps *sql.Tx to implement driver.Tx.
type mysqlTx struct {
	tx  *sql.Tx
	tel *observability.Telemetry

	// parent is the span that was current when the transaction began.
	// Commit and Rollback take no context, so their spans start under it.
	parent trace.Span

	// done is set once Commit or Rollback has run. A later Rollback (every
	// deferred one after a commit) returns sql.ErrTxDone without reaching
	// the database, so it isn't recorded as an operation.
	done bool
}

var _ driver.Tx = (*mysqlTx)(nil)
var _ driver.Preparer = (*mysqlTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *mysqlTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	res, err := t.tx.ExecContext(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: tx exec: %w", err)
	}
	return &mysqlResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *mysqlTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("mysqldriver: tx query: %w", err)
	}
	return &mysqlRows{rows: rows, op: op}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *mysqlTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, op := t.tel.StartSQL(ctx, query)
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &mysqlRow{row: row, op: op}
}

// Commit commits the transaction.
func (t *mysqlTx) Commit() error {
	if t.done {
		return t.tx.Commit()
	}
	t.done = true
	_, op := t.tel.Start(t.parentContext(), "COMMIT", "")
	err := t.tx.Commit()
	op.End(err)
	return err
}

// Rollback rolls back the transaction. It is safe to call after Commit; the
// underlying sql driver will return sql.ErrTxDone if the transaction is
// already closed.
func (t *mysqlTx) Rollback() error {
	if t.done {
		return t.tx.Rollback()
	}
	t.done = true
	_, op := t.tel.Start(t.parentContext(), "ROLLBACK", "")
	err := t.tx.Rollback()
	op.End(err)
	return err
}

// parentContext is a fresh context carrying the span that was current when
// the transaction began.
func (t *mysqlTx) parentContext() context.Context {
	if t.parent == nil {
		return context.Background()
	}
	return trace.ContextWithSpan(context.Background(), t.parent)
}

// Prepare creates a prepared statement within the transaction.
func (t *mysqlTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: tx prepare: %w", err)
	}
	return &mysqlStmt{stmt: stmt, query: query, tel: t.tel}, nil
}

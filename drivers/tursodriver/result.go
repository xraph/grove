package tursodriver

import (
	"context"
	"database/sql"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
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
	op   *observability.Op // ends when the rows are drained or closed
}

var _ driver.Rows = (*tursoRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *tursoRows) Next() bool {
	if r.rows.Next() {
		return true
	}
	r.op.End(r.rows.Err())
	return false
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
	err := r.rows.Close()
	if err != nil {
		r.op.End(err)
	} else {
		r.op.End(r.rows.Err())
	}
	return err
}

// Err returns any error encountered during iteration.
func (r *tursoRows) Err() error {
	return r.rows.Err()
}

// tursoRow wraps *sql.Row to implement driver.Row.
type tursoRow struct {
	row *sql.Row
	op  *observability.Op // ends at Scan, where the row's error surfaces
}

var _ driver.Row = (*tursoRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns sql.ErrNoRows.
func (r *tursoRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.op.End(err)
	return err
}

// tursoTx wraps *sql.Tx to implement driver.Tx.
type tursoTx struct {
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

var _ driver.Tx = (*tursoTx)(nil)
var _ driver.Preparer = (*tursoTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *tursoTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	res, err := t.tx.ExecContext(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: tx exec: %w", err)
	}
	return &tursoResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *tursoTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("tursodriver: tx query: %w", err)
	}
	return &tursoRows{rows: rows, op: op}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *tursoTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, op := t.tel.StartSQL(ctx, query)
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &tursoRow{row: row, op: op}
}

// Commit commits the transaction.
func (t *tursoTx) Commit() error {
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
// underlying sql driver will return nil if the transaction is already closed.
func (t *tursoTx) Rollback() error {
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
func (t *tursoTx) parentContext() context.Context {
	if t.parent == nil {
		return context.Background()
	}
	return trace.ContextWithSpan(context.Background(), t.parent)
}

// Prepare creates a prepared statement within the transaction.
func (t *tursoTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("tursodriver: tx prepare: %w", err)
	}
	return &tursoStmt{stmt: stmt, query: query, tel: t.tel}, nil
}

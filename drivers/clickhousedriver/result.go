package clickhousedriver

import (
	"context"
	"database/sql"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
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
	op   *observability.Op // ends when the rows are drained or closed
}

var _ driver.Rows = (*chRows)(nil)

// Next advances the cursor to the next row.
func (r *chRows) Next() bool {
	if r.rows.Next() {
		return true
	}
	r.op.End(r.rows.Err())
	return false
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
	err := r.rows.Close()
	if err != nil {
		r.op.End(err)
	} else {
		r.op.End(r.rows.Err())
	}
	return err
}

// Err returns any error encountered during iteration.
func (r *chRows) Err() error {
	return r.rows.Err()
}

// chRow wraps *sql.Row to implement driver.Row.
type chRow struct {
	row *sql.Row
	op  *observability.Op // ends at Scan, where the row's error surfaces
}

var _ driver.Row = (*chRow)(nil)

// Scan copies the single row's columns into dest.
func (r *chRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.op.End(err)
	return err
}

// chTx wraps *sql.Tx to implement driver.Tx.
// Note: ClickHouse has limited transaction support.
type chTx struct {
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

var _ driver.Tx = (*chTx)(nil)
var _ driver.Preparer = (*chTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *chTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	res, err := t.tx.ExecContext(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: tx exec: %w", err)
	}
	return &chResult{res: res}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *chTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("clickhousedriver: tx query: %w", err)
	}
	return &chRows{rows: rows, op: op}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *chTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, op := t.tel.StartSQL(ctx, query)
	row := t.tx.QueryRowContext(ctx, query, args...)
	return &chRow{row: row, op: op}
}

// Commit commits the transaction.
func (t *chTx) Commit() error {
	if t.done {
		return t.tx.Commit()
	}
	t.done = true
	_, op := t.tel.Start(t.parentContext(), "COMMIT", "")
	err := t.tx.Commit()
	op.End(err)
	return err
}

// Rollback rolls back the transaction.
func (t *chTx) Rollback() error {
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
func (t *chTx) parentContext() context.Context {
	if t.parent == nil {
		return context.Background()
	}
	return trace.ContextWithSpan(context.Background(), t.parent)
}

// Prepare creates a prepared statement within the transaction.
func (t *chTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: tx prepare: %w", err)
	}
	return &chStmt{stmt: stmt, query: query, tel: t.tel}, nil
}

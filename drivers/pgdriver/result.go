package pgdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
)

// ErrLastInsertIDNotSupported is returned by pgResult.LastInsertId because
// PostgreSQL does not natively support a last-insert-id concept. Use
// INSERT ... RETURNING instead.
var ErrLastInsertIDNotSupported = errors.New("pgdriver: LastInsertId is not supported by PostgreSQL; use RETURNING instead")

// pgResult wraps pgconn.CommandTag to implement driver.Result.
type pgResult struct {
	ct pgconn.CommandTag
}

var _ driver.Result = (*pgResult)(nil)

// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or
// DELETE statement.
func (r *pgResult) RowsAffected() (int64, error) {
	return r.ct.RowsAffected(), nil
}

// LastInsertId always returns 0 and an error because PostgreSQL does not
// provide a last-insert-id. Callers should use INSERT ... RETURNING instead.
func (r *pgResult) LastInsertId() (int64, error) {
	return 0, ErrLastInsertIDNotSupported
}

// pgRows wraps pgx.Rows to implement driver.Rows. The query's span and
// duration run until the rows are drained or closed, since a row error can
// still turn up while iterating.
type pgRows struct {
	rows pgx.Rows
	op   *observability.Op
}

var _ driver.Rows = (*pgRows)(nil)

// Next advances the cursor to the next row. It returns false when there are
// no more rows or an error occurred.
func (r *pgRows) Next() bool {
	if r.rows.Next() {
		return true
	}
	r.op.End(r.rows.Err())
	return false
}

// Scan copies the current row's column values into dest.
func (r *pgRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

// Columns returns the column names from the result set.
func (r *pgRows) Columns() ([]string, error) {
	fds := r.rows.FieldDescriptions()
	cols := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = fd.Name
	}
	return cols, nil
}

// Close releases all resources held by the rows iterator.
func (r *pgRows) Close() error {
	r.rows.Close()
	err := r.rows.Err()
	r.op.End(err)
	return err
}

// Err returns any error encountered during iteration.
func (r *pgRows) Err() error {
	return r.rows.Err()
}

// pgRow wraps pgx.Row to implement driver.Row. pgx holds back a QueryRow's
// error until Scan, so that's where the operation ends.
type pgRow struct {
	row pgx.Row
	op  *observability.Op
}

var _ driver.Row = (*pgRow)(nil)

// Scan copies the single row's columns into dest. If the query returned no
// rows, it returns pgx.ErrNoRows.
func (r *pgRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.op.End(err)
	return err
}

// pgTx wraps pgx.Tx to implement driver.Tx.
type pgTx struct {
	tx  pgx.Tx
	tel *observability.Telemetry

	// parent is the span that was current when the transaction began.
	// Commit and Rollback take no context, so their spans start under it.
	parent trace.Span

	// done is set once Commit or Rollback has run. pgx closes the
	// transaction at either, and a later Rollback (every deferred one
	// after a commit) returns ErrTxClosed without reaching the server, so
	// it isn't recorded as an operation.
	done bool
}

var _ driver.Tx = (*pgTx)(nil)
var _ driver.Preparer = (*pgTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *pgTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	ct, err := t.tx.Exec(ctx, query, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: tx exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

// Query executes a query within the transaction that returns rows.
func (t *pgTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		op.End(err)
		return nil, fmt.Errorf("pgdriver: tx query: %w", err)
	}
	return &pgRows{rows: rows, op: op}, nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *pgTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, op := t.tel.StartSQL(ctx, query)
	row := t.tx.QueryRow(ctx, query, args...)
	return &pgRow{row: row, op: op}
}

// Commit commits the transaction.
func (t *pgTx) Commit() error {
	if t.done {
		return t.tx.Commit(context.Background())
	}
	t.done = true
	ctx, op := t.tel.Start(t.parentContext(), "COMMIT", "")
	err := t.tx.Commit(ctx)
	op.End(err)
	return err
}

// Rollback rolls back the transaction. It is safe to call after Commit; the
// underlying pgx driver will return nil if the transaction is already closed.
func (t *pgTx) Rollback() error {
	if t.done {
		return t.tx.Rollback(context.Background())
	}
	t.done = true
	ctx, op := t.tel.Start(t.parentContext(), "ROLLBACK", "")
	err := t.tx.Rollback(ctx)
	op.End(err)
	return err
}

// parentContext is a fresh context carrying the span that was current when
// the transaction began.
func (t *pgTx) parentContext() context.Context {
	if t.parent == nil {
		return context.Background()
	}
	return trace.ContextWithSpan(context.Background(), t.parent)
}

// Prepare creates a prepared statement within the transaction.
func (t *pgTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	name := fmt.Sprintf("grove_txps_%p", t)
	_, err := t.tx.Prepare(ctx, name, query)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: tx prepare: %w", err)
	}
	return &pgTxStmt{name: name, query: query, tx: t}, nil
}

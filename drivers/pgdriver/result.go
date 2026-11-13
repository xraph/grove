package pgdriver

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/queryctx"
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

// pgTx wraps pgx.Tx to implement driver.Tx.
type pgTx struct {
	opts *driver.DriverOptions
	tx   pgx.Tx
	tel  *observability.Telemetry

	// parent is the span that was current when the transaction began.
	// Commit and Rollback take no context, so their spans start under it.
	parent trace.Span

	// done is set once Commit or Rollback has run. pgx closes the
	// transaction at either, and a later Rollback (every deferred one
	// after a commit) returns ErrTxClosed without reaching the server, so
	// it isn't recorded as an operation.
	done bool

	// stmts holds the names of statements prepared in the transaction and
	// not yet closed. A prepared statement belongs to the session, not the
	// transaction, so Commit and Rollback deallocate whatever is left
	// before the connection goes back to the pool.
	stmts map[string]struct{}
}

var _ driver.Tx = (*pgTx)(nil)
var _ driver.Preparer = (*pgTx)(nil)

// Exec executes a query within the transaction that does not return rows.
func (t *pgTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, t.opts)
	defer cancel()
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
	ctx, cancel := queryctx.WithTimeout(ctx, t.opts)
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.Query(ctx, query, args...)
	if err != nil {
		cancel()
		op.End(err)
		return nil, fmt.Errorf("pgdriver: tx query: %w", err)
	}
	return queryctx.Rows(&pgRows{rows: rows, op: op}, cancel), nil
}

// QueryRow executes a query within the transaction expected to return at most
// one row.
func (t *pgTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	ctx, cancel := queryctx.WithTimeout(ctx, t.opts)
	ctx, op := t.tel.StartSQL(ctx, query)
	rows, err := t.tx.Query(ctx, query, args...)
	return queryctx.NewRow(&pgRows{rows: rows}, err, pgx.ErrNoRows, op, cancel)
}

// Commit commits the transaction.
func (t *pgTx) Commit() error {
	if t.done {
		return t.tx.Commit(context.Background())
	}
	t.done = true
	t.deallocateStmts()
	ctx, cancel := queryctx.WithTimeout(t.parentContext(), t.opts)
	defer cancel()
	ctx, op := t.tel.Start(ctx, "COMMIT", "")
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
	t.deallocateStmts()
	ctx, cancel := queryctx.WithTimeout(t.parentContext(), t.opts)
	defer cancel()
	ctx, op := t.tel.Start(ctx, "ROLLBACK", "")
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

// Prepare creates a prepared statement within the transaction. Close
// deallocates it; any statement still open when the transaction ends is
// deallocated by Commit or Rollback.
func (t *pgTx) Prepare(ctx context.Context, query string) (driver.Stmt, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, t.opts)
	defer cancel()
	name := stmtName("grove_txps_")
	_, err := t.tx.Prepare(ctx, name, query)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: tx prepare: %w", err)
	}
	if t.stmts == nil {
		t.stmts = make(map[string]struct{})
	}
	t.stmts[name] = struct{}{}
	return &pgTxStmt{name: name, query: query, tx: t}, nil
}

// deallocateStmt drops one statement from the session. It goes through
// pgx so pgx forgets the name too, and it uses the protocol's Close
// message, which the server accepts even in an aborted transaction.
func (t *pgTx) deallocateStmt(name string) error {
	delete(t.stmts, name)
	ctx, cancel := queryctx.WithTimeout(context.Background(), t.opts)
	defer cancel()
	return t.tx.Conn().Deallocate(ctx, name)
}

// deallocateStmts drops every statement the caller didn't close. It's best
// effort: a failure here means the connection is broken, and the Commit or
// Rollback that follows reports that.
func (t *pgTx) deallocateStmts() {
	for name := range t.stmts {
		_ = t.deallocateStmt(name) //nolint:errcheck // a broken connection fails the Commit or Rollback next
	}
}

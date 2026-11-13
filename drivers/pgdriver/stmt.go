package pgdriver

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/queryctx"
	"github.com/xraph/grove/observability"
)

// stmtSeq numbers the statements grove prepares. A name has to stay unique
// on its connection for as long as the statement lives, which outlasts the
// transaction or pooled handle that prepared it, so a pointer won't do:
// the runtime hands the same address out again once the old value is gone.
var stmtSeq atomic.Uint64

// stmtName returns a fresh statement name with the given prefix.
func stmtName(prefix string) string {
	return prefix + strconv.FormatUint(stmtSeq.Add(1), 10)
}

// pgPoolStmt wraps a pgxpool.Conn with a prepared statement for repeated execution.
// It holds the acquired connection and releases it on Close.
type pgPoolStmt struct {
	opts  *driver.DriverOptions
	conn  *pgxpool.Conn
	sd    *pgconn.StatementDescription
	query string // the statement's text, for its span name and labels
	tel   *observability.Telemetry
}

var _ driver.Stmt = (*pgPoolStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *pgPoolStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, s.opts)
	defer cancel()
	ctx, op := s.tel.StartSQL(ctx, s.query)
	ct, err := s.conn.Exec(ctx, s.sd.Name, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: stmt exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

// Close deallocates the prepared statement and releases the pooled connection.
// Deallocating through pgx, rather than running DEALLOCATE as SQL, keeps
// pgx's own record of the connection's statements in step with the server.
func (s *pgPoolStmt) Close() error {
	err := s.conn.Conn().Deallocate(context.Background(), s.sd.Name)
	s.conn.Release()
	return err
}

// pgTxStmt wraps a pgx transaction prepared statement for repeated execution.
type pgTxStmt struct {
	name  string
	query string // the statement's text, for its span name and labels
	tx    *pgTx
}

var _ driver.Stmt = (*pgTxStmt)(nil)

// Exec executes the prepared statement within the transaction.
func (s *pgTxStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	ctx, cancel := queryctx.WithTimeout(ctx, s.tx.opts)
	defer cancel()
	ctx, op := s.tx.tel.StartSQL(ctx, s.query)
	ct, err := s.tx.tx.Exec(ctx, s.name, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: tx stmt exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

// Close deallocates the prepared statement. After the transaction has
// ended it does nothing: Commit or Rollback already deallocated it, and the
// connection may be serving someone else by now.
func (s *pgTxStmt) Close() error {
	if s.tx.done {
		return nil
	}
	if _, open := s.tx.stmts[s.name]; !open {
		return nil
	}
	if err := s.tx.deallocateStmt(s.name); err != nil {
		return fmt.Errorf("pgdriver: tx stmt close: %w", err)
	}
	return nil
}

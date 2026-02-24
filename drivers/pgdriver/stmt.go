package pgdriver

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove/driver"
)

// pgPoolStmt wraps a pgxpool.Conn with a prepared statement for repeated execution.
// It holds the acquired connection and releases it on Close.
type pgPoolStmt struct {
	conn *pgxpool.Conn
	sd   *pgconn.StatementDescription
}

var _ driver.Stmt = (*pgPoolStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *pgPoolStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	ct, err := s.conn.Exec(ctx, s.sd.Name, args...)
	if err != nil {
		return nil, fmt.Errorf("pgdriver: stmt exec: %w", err)
	}
	return &pgResult{ct: ct}, nil
}

// Close deallocates the prepared statement and releases the pooled connection.
func (s *pgPoolStmt) Close() error {
	_, err := s.conn.Exec(context.Background(), "DEALLOCATE "+s.sd.Name)
	s.conn.Release()
	return err
}

// pgTxStmt wraps a pgx transaction prepared statement for repeated execution.
type pgTxStmt struct {
	name string
	tx   driver.Tx // the underlying driver.Tx for Exec calls
}

var _ driver.Stmt = (*pgTxStmt)(nil)

// Exec executes the prepared statement within the transaction.
func (s *pgTxStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	return s.tx.Exec(ctx, s.name, args...)
}

// Close is a no-op for transaction statements; the prepared statement
// is automatically cleaned up when the transaction ends.
func (s *pgTxStmt) Close() error {
	return nil
}

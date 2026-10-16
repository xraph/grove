package sqlitedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
)

// sqliteStmt wraps a database/sql prepared statement.
type sqliteStmt struct {
	stmt  *sql.Stmt
	query string // the statement's text, for its span name and labels
	tel   *observability.Telemetry
}

var _ driver.Stmt = (*sqliteStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *sqliteStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	ctx, op := s.tel.StartSQL(ctx, s.query)
	res, err := s.stmt.ExecContext(ctx, utcArgs(args)...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: stmt exec: %w", err)
	}
	return &sqliteResult{res: res}, nil
}

// Close closes the prepared statement.
func (s *sqliteStmt) Close() error {
	return s.stmt.Close()
}

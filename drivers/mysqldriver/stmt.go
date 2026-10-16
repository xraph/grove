package mysqldriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
)

// mysqlStmt wraps a database/sql prepared statement.
type mysqlStmt struct {
	stmt  *sql.Stmt
	query string // the statement's text, for its span name and labels
	tel   *observability.Telemetry
}

var _ driver.Stmt = (*mysqlStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *mysqlStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	ctx, op := s.tel.StartSQL(ctx, s.query)
	res, err := s.stmt.ExecContext(ctx, args...)
	op.End(err)
	if err != nil {
		return nil, fmt.Errorf("mysqldriver: stmt exec: %w", err)
	}
	return &mysqlResult{res: res}, nil
}

// Close closes the prepared statement.
func (s *mysqlStmt) Close() error {
	return s.stmt.Close()
}

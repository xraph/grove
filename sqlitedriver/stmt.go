package sqlitedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// sqliteStmt wraps a database/sql prepared statement.
type sqliteStmt struct {
	stmt *sql.Stmt
}

var _ driver.Stmt = (*sqliteStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *sqliteStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	res, err := s.stmt.ExecContext(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("sqlitedriver: stmt exec: %w", err)
	}
	return &sqliteResult{res: res}, nil
}

// Close closes the prepared statement.
func (s *sqliteStmt) Close() error {
	return s.stmt.Close()
}

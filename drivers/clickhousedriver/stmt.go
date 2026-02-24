package clickhousedriver

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/xraph/grove/driver"
)

// chStmt wraps a database/sql prepared statement.
type chStmt struct {
	stmt *sql.Stmt
}

var _ driver.Stmt = (*chStmt)(nil)

// Exec executes the prepared statement with the given arguments.
func (s *chStmt) Exec(ctx context.Context, args ...any) (driver.Result, error) {
	res, err := s.stmt.ExecContext(ctx, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhousedriver: stmt exec: %w", err)
	}
	return &chResult{res: res}, nil
}

// Close closes the prepared statement.
func (s *chStmt) Close() error {
	return s.stmt.Close()
}

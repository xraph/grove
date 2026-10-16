package telemetrytest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"sync/atomic"
	"testing"
)

// SQLDB returns a *sql.DB backed by an in-memory driver, for testing a grove
// driver's telemetry without a server. fail decides each statement's
// outcome: return nil for success, or the error the database should report.
// A query returns one row with one int64 column, unless fail returns
// sql.ErrNoRows, in which case it returns none.
func SQLDB(t testing.TB, fail func(query string) error) *sql.DB {
	t.Helper()
	if fail == nil {
		fail = func(string) error { return nil }
	}
	db := sql.OpenDB(&fakeConnector{fail: fail})
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type fakeConnector struct {
	fail func(string) error
}

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{fail: c.fail}, nil
}

func (c *fakeConnector) Driver() driver.Driver { return fakeDriver{c} }

type fakeDriver struct{ c *fakeConnector }

func (d fakeDriver) Open(string) (driver.Conn, error) { return &fakeConn{fail: d.c.fail}, nil }

type fakeConn struct {
	fail func(string) error
}

var (
	_ driver.ExecerContext  = (*fakeConn)(nil)
	_ driver.QueryerContext = (*fakeConn)(nil)
	_ driver.ConnBeginTx    = (*fakeConn)(nil)
)

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{c: c, query: query}, nil
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) { return fakeTx{}, nil }

func (c *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return fakeTx{}, nil
}

func (c *fakeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.fail(query); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	err := c.fail(query)
	switch {
	case err == sql.ErrNoRows: //nolint:errorlint // the sentinel itself, as the doc says
		return &fakeRows{}, nil
	case err != nil:
		return nil, err
	}
	r := &fakeRows{}
	r.left.Store(1)
	return r, nil
}

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

type fakeStmt struct {
	c     *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }

func (s *fakeStmt) Exec([]driver.Value) (driver.Result, error) {
	return s.c.ExecContext(context.Background(), s.query, nil)
}

func (s *fakeStmt) Query([]driver.Value) (driver.Rows, error) {
	return s.c.QueryContext(context.Background(), s.query, nil)
}

type fakeRows struct {
	left atomic.Int32
}

func (r *fakeRows) Columns() []string { return []string{"n"} }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.left.Add(-1) < 0 {
		return io.EOF
	}
	dest[0] = int64(1)
	return nil
}

// Package grovetest provides testing utilities for Grove applications.
// It includes a mock driver, fixture loader, and query assertion helpers.
package grovetest

import (
	"context"
	"fmt"
	"sync"

	"github.com/xraph/grove/driver"
)

// RecordedQuery holds information about a query executed through the mock driver.
type RecordedQuery struct {
	Query  string
	Args   []any
	Method string // "Exec", "Query", "QueryRow"
}

// MockDriver is an in-memory driver that records queries for testing.
// It implements driver.Driver but doesn't execute any real queries.
type MockDriver struct {
	mu      sync.Mutex
	queries []RecordedQuery
	dialect *MockDialect
	opts    *driver.DriverOptions

	// ExecFunc optionally handles Exec calls. If nil, returns a default result.
	ExecFunc func(ctx context.Context, query string, args ...any) (driver.Result, error)
	// QueryFunc optionally handles Query calls. If nil, returns empty rows.
	QueryFunc func(ctx context.Context, query string, args ...any) (driver.Rows, error)
	// QueryRowFunc optionally handles QueryRow calls. If nil, returns a mock row.
	QueryRowFunc func(ctx context.Context, query string, args ...any) driver.Row
}

var _ driver.Driver = (*MockDriver)(nil)

// NewMockDriver creates a new mock driver.
func NewMockDriver() *MockDriver {
	return &MockDriver{
		dialect: &MockDialect{},
	}
}

func (d *MockDriver) Name() string            { return "mock" }
func (d *MockDriver) Dialect() driver.Dialect { return d.dialect }
func (d *MockDriver) SupportsReturning() bool { return true }

func (d *MockDriver) Open(_ context.Context, _ string, opts ...driver.Option) error {
	d.opts = driver.ApplyOptions(opts)
	return nil
}

func (d *MockDriver) Close() error { return nil }

func (d *MockDriver) Ping(_ context.Context) error { return nil }

func (d *MockDriver) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	d.record("Exec", query, args)
	if d.ExecFunc != nil {
		return d.ExecFunc(ctx, query, args...)
	}
	return &MockResult{rowsAffected: 1}, nil
}

func (d *MockDriver) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	d.record("Query", query, args)
	if d.QueryFunc != nil {
		return d.QueryFunc(ctx, query, args...)
	}
	return &MockRows{}, nil
}

func (d *MockDriver) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	d.record("QueryRow", query, args)
	if d.QueryRowFunc != nil {
		return d.QueryRowFunc(ctx, query, args...)
	}
	return &MockRow{}
}

func (d *MockDriver) BeginTx(_ context.Context, _ *driver.TxOptions) (driver.Tx, error) {
	return &MockTx{driver: d}, nil
}

// GroveTx satisfies the txBeginner adapter interface.
func (d *MockDriver) GroveTx(ctx context.Context, isolationLevel int, readOnly bool) (any, error) {
	return d.BeginTx(ctx, &driver.TxOptions{
		IsolationLevel: driver.IsolationLevel(isolationLevel),
		ReadOnly:       readOnly,
	})
}

func (d *MockDriver) record(method, query string, args []any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = append(d.queries, RecordedQuery{
		Query:  query,
		Args:   args,
		Method: method,
	})
}

// Queries returns all recorded queries.
func (d *MockDriver) Queries() []RecordedQuery {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]RecordedQuery, len(d.queries))
	copy(result, d.queries)
	return result
}

// LastQuery returns the most recently recorded query, or nil if none.
func (d *MockDriver) LastQuery() *RecordedQuery {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.queries) == 0 {
		return nil
	}
	q := d.queries[len(d.queries)-1]
	return &q
}

// Reset clears all recorded queries.
func (d *MockDriver) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.queries = nil
}

// MockResult implements driver.Result.
type MockResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *MockResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }
func (r *MockResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }

// MockRows implements driver.Rows with no data.
type MockRows struct {
	closed bool
}

func (r *MockRows) Next() bool                 { return false }
func (r *MockRows) Scan(_ ...any) error        { return fmt.Errorf("grovetest: no rows") }
func (r *MockRows) Columns() ([]string, error) { return nil, nil }
func (r *MockRows) Close() error               { r.closed = true; return nil }
func (r *MockRows) Err() error                 { return nil }

// MockRow implements driver.Row.
type MockRow struct{}

func (r *MockRow) Scan(_ ...any) error { return fmt.Errorf("grovetest: no rows") }

// MockTx implements driver.Tx.
type MockTx struct {
	driver     *MockDriver
	committed  bool
	rolledBack bool
}

func (t *MockTx) Exec(ctx context.Context, query string, args ...any) (driver.Result, error) {
	return t.driver.Exec(ctx, query, args...)
}

func (t *MockTx) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return t.driver.Query(ctx, query, args...)
}

func (t *MockTx) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return t.driver.QueryRow(ctx, query, args...)
}

func (t *MockTx) Commit() error   { t.committed = true; return nil }
func (t *MockTx) Rollback() error { t.rolledBack = true; return nil }

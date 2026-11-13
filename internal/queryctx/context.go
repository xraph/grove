// Package queryctx manages per-operation deadlines and result lifetimes.
package queryctx

import (
	"context"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability"
)

// WithTimeout applies the configured query timeout without extending an
// earlier caller deadline. Non-positive timeouts leave the caller in control.
func WithTimeout(ctx context.Context, opts *driver.DriverOptions) (context.Context, context.CancelFunc) {
	if opts == nil {
		opts = driver.DefaultDriverOptions()
	}
	if opts.QueryTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, opts.QueryTimeout)
}

// Rows keeps the deadline alive while results are consumed and releases its
// timer when the caller drains or closes the result set.
func Rows(rows driver.Rows, cancel context.CancelFunc) driver.Rows {
	return &timedRows{Rows: rows, cancel: cancel}
}

type timedRows struct {
	driver.Rows
	cancel context.CancelFunc
}

func (r *timedRows) Next() bool {
	if r.Rows.Next() {
		return true
	}
	r.cancel()
	return false
}

func (r *timedRows) Close() error {
	defer r.cancel()
	return r.Rows.Close()
}

// Row preserves result column metadata for struct scans while retaining the
// driver's no-rows sentinel and ending telemetry only when Scan or Close runs.
type Row struct {
	rows   driver.Rows
	err    error
	noRows error
	op     *observability.Op
	cancel context.CancelFunc
	closed bool
}

// NewRow takes ownership of rows and cancel, including when err is non-nil.
func NewRow(rows driver.Rows, err, noRows error, op *observability.Op, cancel context.CancelFunc) *Row {
	if err != nil {
		rows = nil
	}
	return &Row{rows: rows, err: err, noRows: noRows, op: op, cancel: cancel}
}

// Columns exposes the actual result order to the schema-aware scanner.
func (r *Row) Columns() ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.rows.Columns()
}

// Scan copies the first result into dest and releases the remaining rows.
func (r *Row) Scan(dest ...any) (err error) {
	defer func() {
		if closeErr := r.Close(); err == nil {
			err = closeErr
		}
	}()
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		r.err = r.rows.Err()
		if r.err == nil {
			r.err = r.noRows
		}
		return r.err
	}
	r.err = r.rows.Scan(dest...)
	return r.err
}

// Close allows schema validation failures to release an unscanned row too.
func (r *Row) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	defer r.cancel()
	var closeErr error
	if r.rows != nil {
		closeErr = r.rows.Close()
		if closeErr == nil {
			closeErr = r.rows.Err()
		}
	}
	if r.err != nil {
		r.op.End(r.err)
	} else {
		r.op.End(closeErr)
	}
	return closeErr
}

package queryctx

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/observability/telemetrytest"
)

func TestTimeoutConfiguration(t *testing.T) {
	parent, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	deadline, _ := parent.Deadline()
	ctx, cancel := WithTimeout(parent, &driver.DriverOptions{QueryTimeout: time.Hour})
	defer cancel()
	got, ok := ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, deadline, got, "query timeout must never extend the caller's deadline")
	ctx, cancel = WithTimeout(t.Context(), &driver.DriverOptions{QueryTimeout: 0})
	defer cancel()
	_, ok = ctx.Deadline()
	require.False(t, ok)
	ctx, cancel = WithTimeout(t.Context(), nil)
	defer cancel()
	got, ok = ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, 30, time.Until(got).Seconds(), 1)
}

func TestRowsKeepContextUntilConsumed(t *testing.T) {
	db := telemetrytest.SQLDB(t, nil)
	for _, drain := range []bool{false, true} {
		ctx, cancel := WithTimeout(t.Context(), &driver.DriverOptions{QueryTimeout: time.Second})
		native, err := db.QueryContext(ctx, "SELECT 1") //nolint:rowserrcheck // The wrapper checks terminal errors before cancelling the context.
		require.NoError(t, err)
		observed := &closeObservedRows{Rows: native, ctx: ctx}
		rows := Rows(observed, cancel)
		require.NoError(t, ctx.Err(), "returning rows must not cancel the query")
		require.True(t, rows.Next())
		var n int64
		require.NoError(t, rows.Scan(&n))
		require.Equal(t, int64(1), n)
		if drain {
			require.False(t, rows.Next())
		} else {
			require.NoError(t, rows.Close())
		}
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.NoError(t, rows.Err())
		require.Equal(t, 1, observed.closes)
		require.NoError(t, observed.contextErrAtClose)
		defer native.Close()
		require.NoError(t, rows.Close())
	}
}

type closeObservedRows struct {
	driver.Rows
	ctx               context.Context
	closes            int
	contextErrAtClose error
}

func (r *closeObservedRows) Close() error {
	r.closes++
	r.contextErrAtClose = r.ctx.Err()
	return r.Rows.Close()
}

func TestRowReleasesContextAndPreservesErrors(t *testing.T) {
	failure := errors.New("query failed")
	db := telemetrytest.SQLDB(t, func(q string) error {
		switch q {
		case "empty":
			return sql.ErrNoRows
		case "fail":
			return failure
		}
		return nil
	})
	for _, query := range []string{"ok", "empty", "fail"} {
		ctx, cancel := context.WithCancel(t.Context())
		native, err := db.QueryContext(ctx, query) //nolint:rowserrcheck // The wrapper checks terminal errors before cancelling the context.
		observed := &closeObservedRows{Rows: native, ctx: ctx}
		row := NewRow(observed, err, sql.ErrNoRows, nil, cancel)
		var n int64
		scanErr := row.Scan(&n)
		switch query {
		case "ok":
			require.NoError(t, scanErr)
			require.Equal(t, int64(1), n)
		case "empty":
			require.ErrorIs(t, scanErr, sql.ErrNoRows)
		case "fail":
			require.ErrorIs(t, scanErr, failure)
		}
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.NoError(t, row.Close())
		if native != nil {
			// Inspect the wrapper's ordering directly. database/sql can select
			// either ready cancellation channel after Close returns, so the
			// native Rows.Err is not stable once we cancel its context.
			require.Equal(t, 1, observed.closes)
			require.NoError(t, observed.contextErrAtClose)
			defer native.Close()
		}
	}
}

type cleanupErrorRows struct {
	driver.Rows
	ctx     context.Context
	failure error
}

func (r *cleanupErrorRows) Err() error {
	if r.failure != nil {
		return r.failure
	}
	return r.ctx.Err()
}

func TestRowsCleanupCannotOverwriteQueryResult(t *testing.T) {
	for _, failure := range []error{nil, errors.New("read failed")} {
		db := telemetrytest.SQLDB(t, nil)
		ctx, cancel := context.WithCancel(t.Context())
		native, err := db.QueryContext(ctx, "SELECT 1") //nolint:rowserrcheck // The wrapper checks terminal errors before cancelling the context.
		require.NoError(t, err)
		wrapped := Rows(&cleanupErrorRows{Rows: native, ctx: ctx, failure: failure}, cancel)
		require.NoError(t, wrapped.Close())
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		require.ErrorIs(t, wrapped.Err(), failure)
		require.NoError(t, wrapped.Close())
		require.False(t, wrapped.Next())
		require.ErrorIs(t, wrapped.Err(), failure)
	}
}

func TestRowsConcurrentCloseAndRead(t *testing.T) {
	db := telemetrytest.SQLDB(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	native, err := db.QueryContext(ctx, "SELECT 1") //nolint:rowserrcheck // The wrapper checks terminal errors before cancelling the context.
	require.NoError(t, err)
	wrapped := Rows(native, cancel)
	done := make(chan error, 3)
	go func() { done <- wrapped.Close() }()
	go func() {
		for range 100 {
			if readErr := wrapped.Err(); readErr != nil {
				done <- readErr
				return
			}
		}
		done <- nil
	}()
	go func() {
		for wrapped.Next() {
		}
		done <- wrapped.Err()
	}()
	for range 3 {
		require.NoError(t, <-done)
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

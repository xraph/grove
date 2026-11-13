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
		native, err := db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows := Rows(native, cancel)
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
		require.NoError(t, native.Err())
		defer native.Close()
		require.NoError(t, rows.Close())
	}
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
		native, err := db.QueryContext(ctx, query)
		row := NewRow(native, err, sql.ErrNoRows, nil, cancel)
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
			require.NoError(t, native.Err())
			defer native.Close()
		}
	}
}

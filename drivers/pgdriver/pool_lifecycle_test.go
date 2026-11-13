package pgdriver_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
)

func TestBulkAndStatementPoolLifecycle(t *testing.T) {
	dsn := startPostgres(t)
	open := func(t *testing.T) (*pgdriver.PgDB, *driver.DriverOptions) {
		t.Helper()
		db := pgdriver.New()
		var opts *driver.DriverOptions
		require.NoError(t, db.Open(t.Context(), dsn, driver.WithPoolSize(1), func(o *driver.DriverOptions) { opts = o }))
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db, opts
	}
	t.Run("bulk panic rolls back and frees pool slot", func(t *testing.T) {
		db, _ := open(t)
		_, err := db.Exec(t.Context(), "CREATE TABLE resource_rows (id BIGINT PRIMARY KEY)")
		require.NoError(t, err)
		rows := []*resourceRow{{ID: 1}, nil}
		require.Panics(t, func() { _, _ = db.NewInsert(&rows).Exec(context.Background()) })
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		var count int
		require.NoError(t, db.QueryRow(ctx, "SELECT count(*) FROM resource_rows").Scan(&count))
		require.Zero(t, count, "the row inserted before the panic must be rolled back")
		require.Zero(t, groveStatements(t, db))
	})
	t.Run("pool statement closes once", func(t *testing.T) {
		db, _ := open(t)
		stmt, err := db.Prepare(t.Context(), "SELECT 1")
		require.NoError(t, err)
		require.NoError(t, stmt.Close())
		require.NotPanics(t, func() { require.NoError(t, stmt.Close()) })
		_, err = stmt.Exec(t.Context())
		require.ErrorContains(t, err, "closed")
		require.Zero(t, groveStatements(t, db))
	})
	t.Run("failed pool deallocation retires session", func(t *testing.T) {
		db, opts := open(t)
		stmt, err := db.Prepare(t.Context(), "SELECT 1")
		require.NoError(t, err)
		opts.QueryTimeout = time.Nanosecond
		require.Error(t, stmt.Close())
		opts.QueryTimeout = 30 * time.Second
		require.Zero(t, groveStatements(t, db), "an uncleared server statement must not leak into the pool")
	})
	t.Run("failed transaction deallocation remains tracked", func(t *testing.T) {
		db, opts := open(t)
		tx, prep := beginPreparer(t, db)
		defer tx.Rollback()
		stmt, err := prep.Prepare(t.Context(), "SELECT 1")
		require.NoError(t, err)
		opts.QueryTimeout = time.Nanosecond
		require.Error(t, stmt.Close())
		opts.QueryTimeout = 30 * time.Second
		require.NoError(t, tx.Commit())
		require.Zero(t, groveStatements(t, db), "commit must retry deallocation after an unsuccessful Close")
	})
	t.Run("failed terminal deallocation releases session", func(t *testing.T) {
		db, opts := open(t)
		tx, prep := beginPreparer(t, db)
		_, err := prep.Prepare(t.Context(), "SELECT 1")
		require.NoError(t, err)
		opts.QueryTimeout = time.Nanosecond
		require.Error(t, tx.Commit())
		opts.QueryTimeout = 30 * time.Second
		require.Zero(t, groveStatements(t, db))
	})
}

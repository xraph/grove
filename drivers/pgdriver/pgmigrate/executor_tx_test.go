package pgmigrate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"
)

func TestLive_executorTransaction(t *testing.T) {
	ctx := context.Background()
	applied := &migrate.Migration{Version: "20260101000008", Name: "tx", Group: "app"}

	// held returns an executor that holds the migration lock, so it has the
	// dedicated connection a transaction runs on.
	held := func(t *testing.T) (*Executor, *pgdriver.PgDB) {
		t.Helper()
		db := openLive(t)
		e := New(db)
		require.NoError(t, e.EnsureMigrationTable(ctx))
		require.NoError(t, e.EnsureLockTable(ctx))
		require.NoError(t, e.AcquireLock(ctx, "test"))
		t.Cleanup(func() { _ = e.ReleaseLock(ctx) })
		return e, db
	}

	t.Run("begin without the lock is unsupported", func(t *testing.T) {
		e := newTestExecutor(t)
		err := e.BeginTx(ctx)
		require.ErrorIs(t, err, migrate.ErrTxUnsupported)
	})

	t.Run("rollback with nothing open is a no-op", func(t *testing.T) {
		e := newTestExecutor(t)
		require.NoError(t, e.RollbackTx(ctx), "no lock, no transaction")

		e, _ = held(t)
		require.NoError(t, e.RollbackTx(ctx), "lock held, no transaction")

		require.NoError(t, e.BeginTx(ctx))
		require.NoError(t, e.CommitTx(ctx))
		require.NoError(t, e.RollbackTx(ctx), "the orchestrator rolls back after a failed commit, which may have closed the transaction")
	})

	t.Run("begin twice and commit with nothing open are errors", func(t *testing.T) {
		e, _ := held(t)
		require.Error(t, e.CommitTx(ctx))

		require.NoError(t, e.BeginTx(ctx))
		err := e.BeginTx(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "pgmigrate: ")
		require.NoError(t, e.RollbackTx(ctx))
	})

	t.Run("statements and records share one transaction", func(t *testing.T) {
		e, db := held(t)

		require.NoError(t, e.BeginTx(ctx))
		_, err := e.Exec(ctx, "CREATE TABLE t (id TEXT)")
		require.NoError(t, err)
		require.NoError(t, e.RecordApplied(ctx, applied))
		assert.False(t, tableExists(t, db, "t"), "another connection must not see the open transaction's table")
		assert.Zero(t, recordedVersions(t, db, applied.Version), "or its record")
		require.NoError(t, e.RollbackTx(ctx))
		assert.False(t, tableExists(t, db, "t"))
		assert.Zero(t, recordedVersions(t, db, applied.Version))

		require.NoError(t, e.BeginTx(ctx))
		_, err = e.Exec(ctx, "CREATE TABLE t (id TEXT)")
		require.NoError(t, err)
		require.NoError(t, e.RecordApplied(ctx, applied))
		require.NoError(t, e.CommitTx(ctx))
		assert.True(t, tableExists(t, db, "t"))
		assert.Equal(t, 1, recordedVersions(t, db, applied.Version))

		require.NoError(t, e.BeginTx(ctx))
		_, err = e.Exec(ctx, "DROP TABLE t")
		require.NoError(t, err)
		require.NoError(t, e.RemoveApplied(ctx, applied))
		require.NoError(t, e.RollbackTx(ctx))
		assert.True(t, tableExists(t, db, "t"))
		assert.Equal(t, 1, recordedVersions(t, db, applied.Version))
	})

	t.Run("a failed statement poisons the transaction until rollback", func(t *testing.T) {
		e, db := held(t)
		require.NoError(t, e.BeginTx(ctx))
		_, err := e.Exec(ctx, "SELECT 1/0")
		require.Error(t, err)
		require.NoError(t, e.RollbackTx(ctx))

		// The connection is usable again, and the lock it carries is intact.
		assert.Equal(t, 1, advisoryLocks(t, db))
		var one int
		rows, err := e.Query(ctx, "SELECT 1")
		require.NoError(t, err)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&one))
		require.NoError(t, rows.Close())
		assert.Equal(t, 1, one)
	})

	t.Run("release abandons an open transaction", func(t *testing.T) {
		e, db := held(t)
		require.NoError(t, e.BeginTx(ctx))
		_, err := e.Exec(ctx, "CREATE TABLE t (id TEXT)")
		require.NoError(t, err)

		require.NoError(t, e.ReleaseLock(ctx))
		assert.False(t, tableExists(t, db, "t"))
		assert.Zero(t, advisoryLocks(t, db))
		info, err := e.LockInfo(ctx)
		require.NoError(t, err)
		assert.False(t, info.Held, "the lock record must be cleared even though a transaction was open")

		// A new run takes the lock straight away.
		next := New(db)
		require.NoError(t, next.AcquireLock(ctx, "next"))
		require.NoError(t, next.ReleaseLock(ctx))
	})
}

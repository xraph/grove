package tursodriver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability/telemetrytest"
)

type bulkLifecycleRow struct {
	grove.BaseModel `grove:"table:bulk_lifecycle_rows"`
	ID              int `grove:"id,pk"`
}

func TestBulkInsertPanicReleasesOwnedTransaction(t *testing.T) {
	db := New()
	db.db = telemetrytest.SQLDB(t, nil)
	db.db.SetMaxOpenConns(1)
	// A malformed second element panics after the first INSERT has executed.
	rows := []*bulkLifecycleRow{{ID: 1}, nil}
	require.Panics(t, func() { _, _ = db.NewInsert(&rows).Exec(context.Background()) })
	require.Zero(t, db.db.Stats().InUse, "a recovered panic must not strand the transaction's connection")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, db.Ping(ctx))
}

func TestBulkInsertPanicPreservesCallerTransaction(t *testing.T) {
	db := New()
	db.db = telemetrytest.SQLDB(t, nil)
	db.db.SetMaxOpenConns(1)
	tx, err := db.BeginTxQuery(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	rows := []*bulkLifecycleRow{{ID: 1}, nil}
	require.Panics(t, func() { _, _ = tx.NewInsert(&rows).Exec(t.Context()) })
	require.Equal(t, 1, db.db.Stats().InUse, "the caller retains ownership of its transaction")
	require.NoError(t, tx.Rollback())
	require.Zero(t, db.db.Stats().InUse)
}

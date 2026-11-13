package pgdriver_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
)

func TestListenerResourceLifecycle(t *testing.T) {
	dsn := startPostgres(t)
	admin, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer admin.Close(context.Background())
	newDB := func(t *testing.T) *pgdriver.PgDB {
		t.Helper()
		db := pgdriver.New()
		require.NoError(t, db.Open(t.Context(), dsn, driver.WithPoolSize(1)))
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		return db
	}
	t.Run("fatal connection releases listener", func(t *testing.T) {
		db := newDB(t)
		listener := db.NewListener()
		defer listener.Close()
		require.NoError(t, listener.Start(t.Context()))
		require.NoError(t, listener.Listen(t.Context(), "resource_lifecycle"))
		var pid int
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT pid FROM pg_stat_activity WHERE query = 'LISTEN "resource_lifecycle"' AND datname = current_database()`).Scan(&pid))
		_, err := admin.Exec(t.Context(), "SELECT pg_terminate_backend($1)", pid)
		require.NoError(t, err)
		select {
		case <-listener.Done():
		case <-time.After(500 * time.Millisecond):
			t.Fatal("fatal connection error retained the listener's pool slot")
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		require.NoError(t, db.Ping(ctx))
	})
	t.Run("close clears subscriptions", func(t *testing.T) {
		db := newDB(t)
		listener := db.NewListener()
		defer listener.Close()
		require.NoError(t, listener.Start(t.Context()))
		require.NoError(t, listener.Listen(t.Context(), "resource_cleanup"))
		require.NoError(t, listener.Close())
		<-listener.Done()
		var subscriptions int
		require.NoError(t, db.QueryRow(t.Context(), "SELECT count(*) FROM pg_listening_channels()").Scan(&subscriptions))
		require.Zero(t, subscriptions, "pool must not retain abandoned LISTEN subscriptions")
	})
	t.Run("notify honors query timeout", func(t *testing.T) {
		db := newDB(t)
		db.SetQueryTimeout(20 * time.Millisecond)
		listener := db.NewListener()
		defer listener.Close()
		require.NoError(t, listener.Start(t.Context()))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		started := time.Now()
		require.ErrorIs(t, listener.Notify(ctx, "resource_notify", "x"), context.DeadlineExceeded)
		require.Less(t, time.Since(started), 500*time.Millisecond)
	})
}

type resourceRow struct {
	grove.BaseModel `grove:"table:resource_rows"`
	ID              int64 `grove:"id,pk"`
}

func TestStreamResourceLifecycle(t *testing.T) {
	db := openPgDB(t, startPostgres(t))
	_, err := db.Exec(t.Context(), "CREATE TABLE resource_rows (id BIGINT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), "INSERT INTO resource_rows VALUES (1)")
	require.NoError(t, err)
	t.Run("transaction stream borrows existing transaction", func(t *testing.T) {
		tx, err := db.BeginTxQuery(t.Context(), nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.NewRaw("INSERT INTO resource_rows VALUES (2)").Exec(t.Context())
		require.NoError(t, err)
		require.NotPanics(t, func() {
			s, streamErr := tx.NewSelect((*resourceRow)(nil)).OrderExpr("id").Stream(t.Context())
			require.NoError(t, streamErr)
			defer s.Close()
			rows, collectErr := s.Collect(t.Context())
			require.NoError(t, collectErr)
			require.Len(t, rows, 2)
			count, countErr := tx.NewSelect((*resourceRow)(nil)).Count(t.Context())
			require.NoError(t, countErr)
			require.Equal(t, int64(2), count)
		})
	})
	t.Run("canceled stream does not fetch", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		s, err := db.NewSelect((*resourceRow)(nil)).Column("id", "pg_sleep(0.5)").Stream(ctx)
		require.NoError(t, err)
		defer s.Close()
		cancel()
		started := time.Now()
		_, err = s.Collect(t.Context())
		require.ErrorIs(t, err, context.Canceled)
		require.Less(t, time.Since(started), 250*time.Millisecond)
	})
	t.Run("consumer deadline cancels fetch", func(t *testing.T) {
		s, err := db.NewSelect((*resourceRow)(nil)).Column("id", "pg_sleep(0.5)").Stream(t.Context())
		require.NoError(t, err)
		defer s.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err = s.Collect(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(started), 250*time.Millisecond)
	})
}

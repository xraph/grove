package sqlpool_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/internal/sqlpool"
	"github.com/xraph/grove/observability/telemetrytest"
)

func TestPoolCapsConnectionsAndShrinksWhenIdle(t *testing.T) {
	db := telemetrytest.SQLDB(t, nil)
	opts := driver.ApplyOptions([]driver.Option{
		driver.WithPoolSize(2),
		driver.WithMaxConnIdleTime(20 * time.Millisecond),
	})
	sqlpool.Configure(db, opts)

	first, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer first.Close()
	second, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer second.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	extra, err := db.Conn(ctx)
	if extra != nil {
		extra.Close()
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, 2, db.Stats().OpenConnections)
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	require.Eventually(t, func() bool {
		return db.Stats().OpenConnections == 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), db.Stats().MaxIdleTimeClosed)
	require.NoError(t, db.PingContext(t.Context()))
}

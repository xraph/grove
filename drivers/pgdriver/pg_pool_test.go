package pgdriver

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/driver"
)

const poolTestDSN = "postgres://user:pass@localhost:5432/db"

func TestBuildPoolConfig(t *testing.T) {
	for _, tt := range []struct {
		name string
		dsn  string
		opts []driver.Option
		max  int32
		idle time.Duration
	}{
		{name: "defaults", dsn: poolTestDSN, max: 10, idle: time.Minute},
		{name: "url", dsn: poolTestDSN + "?pool_max_conns=2&pool_max_conn_idle_time=15s", max: 2, idle: 15 * time.Second},
		{name: "keyword", dsn: "host=localhost dbname=db pool_max_conns=2 pool_max_conn_idle_time=15s", max: 2, idle: 15 * time.Second},
		{name: "escaped key", dsn: poolTestDSN + "?pool%5fmax_conns=2", max: 2, idle: time.Minute},
		{name: "parameter name in database", dsn: poolTestDSN + "pool_max_conns", max: 10, idle: time.Minute},
		{name: "explicit option", dsn: poolTestDSN, opts: []driver.Option{driver.WithPoolSize(3)}, max: 3, idle: time.Minute},
		{name: "option wins even when equal to default", dsn: poolTestDSN + "?pool_max_conns=2&pool_max_conn_idle_time=15s", opts: []driver.Option{driver.WithPoolSize(10), driver.WithMaxConnIdleTime(time.Minute)}, max: 10, idle: time.Minute},
		{name: "zero idle option keeps default", dsn: poolTestDSN, opts: []driver.Option{driver.WithMaxConnIdleTime(0)}, max: 10, idle: time.Minute},
		{name: "invalid size cannot uncap pool", dsn: poolTestDSN, opts: []driver.Option{driver.WithPoolSize(0)}, max: 10, idle: time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _, err := buildPoolConfig(tt.dsn, tt.opts...)
			require.NoError(t, err)
			require.Equal(t, tt.max, cfg.MaxConns)
			require.Zero(t, cfg.MinConns)
			require.Zero(t, cfg.MinIdleConns)
			require.Equal(t, tt.idle, cfg.MaxConnIdleTime)
			require.Equal(t, time.Minute, cfg.HealthCheckPeriod)
			require.NotContains(t, cfg.ConnConfig.RuntimeParams, "pool_max_conns")
		})
	}
}

func TestBuildPoolConfig_PreservesSettingsAndAppliesOptionsOnce(t *testing.T) {
	dsn := poolTestDSN + "?pool_min_conns=2&pool_max_conn_lifetime=20m&pool_health_check_period=10s"
	cfg, _, err := buildPoolConfig(dsn)
	require.NoError(t, err)
	require.Equal(t, int32(2), cfg.MinConns)
	require.Equal(t, 20*time.Minute, cfg.MaxConnLifetime)
	require.Equal(t, 10*time.Second, cfg.HealthCheckPeriod)

	calls := 0
	cfg, opts, err := buildPoolConfig(dsn, driver.WithMinConns(0), driver.WithMaxConnLifetime(time.Hour), driver.WithHealthCheckPeriod(time.Minute), func(o *driver.DriverOptions) {
		calls++
		o.QueryTimeout = time.Second
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Zero(t, cfg.MinConns)
	require.Equal(t, time.Hour, cfg.MaxConnLifetime)
	require.Equal(t, time.Minute, cfg.HealthCheckPeriod)
	require.Equal(t, time.Second, opts.QueryTimeout)
}

func TestBuildPoolConfig_NonPositiveDurationsKeepDSNSettings(t *testing.T) {
	cfg, _, err := buildPoolConfig(poolTestDSN+"?pool_max_conn_lifetime=20m&pool_health_check_period=10s&pool_max_conn_idle_time=15s",
		driver.WithMaxConnLifetime(0), driver.WithHealthCheckPeriod(0), driver.WithMaxConnIdleTime(-time.Second))
	require.NoError(t, err)
	require.Equal(t, 20*time.Minute, cfg.MaxConnLifetime)
	require.Equal(t, 10*time.Second, cfg.HealthCheckPeriod)
	require.Equal(t, 15*time.Second, cfg.MaxConnIdleTime)
}

func TestOpenDriver_PoolLimit(t *testing.T) {
	f := startFakePG(t)
	// The registry is also used by Forge's config-driven extension. Exercise
	// this public path, rather than bypassing defaults with a zero options struct.
	drv, err := grove.OpenDriver(t.Context(), "pg", f.dsn()+"&pool_max_conns=2")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, drv.Close()) })
	db, ok := drv.(*PgDB)
	require.True(t, ok)
	require.Equal(t, int32(2), db.pool.Config().MaxConns)

	for range 2 {
		conn, acquireErr := db.pool.Acquire(t.Context())
		require.NoError(t, acquireErr)
		t.Cleanup(conn.Release)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	conn, err := db.pool.Acquire(ctx)
	if conn != nil {
		conn.Release()
	}
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(2), db.pool.Stat().TotalConns())
}

func TestOpen_IdlePoolShrinksToZero(t *testing.T) {
	f := startFakePG(t)
	db := New()
	require.NoError(t, db.Open(t.Context(), f.dsn(),
		driver.WithMaxConnIdleTime(50*time.Millisecond),
		driver.WithHealthCheckPeriod(10*time.Millisecond)))
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	conns := make([]driver.DedicatedConn, 0, 3)
	for range 3 {
		conn, err := db.AcquireConn(t.Context())
		require.NoError(t, err)
		t.Cleanup(conn.Release)
		conns = append(conns, conn)
	}
	require.Equal(t, int32(3), db.pool.Stat().TotalConns())
	for _, conn := range conns {
		conn.Release()
	}
	require.Eventually(t, func() bool {
		return db.pool.Stat().TotalConns() == 0
	}, 5*time.Second, 10*time.Millisecond)
	// A shrunk pool remains usable on the next request.
	require.NoError(t, db.Ping(t.Context()))
}

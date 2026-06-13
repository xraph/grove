package pgdriver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

const poolTestDSN = "postgres://user:pass@localhost:5432/db"

func TestBuildPoolConfig_DefaultFloor(t *testing.T) {
	// With no explicit PoolSize and no DSN setting, the pool must not
	// inherit pgxpool's max(4, NumCPU) default: polling extensions
	// (dispatch, relay) alone demand ~a dozen connections.
	cfg, err := buildPoolConfig(poolTestDSN, &driver.DriverOptions{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, cfg.MaxConns, int32(defaultMaxConns))
}

func TestBuildPoolConfig_ExplicitOptionWins(t *testing.T) {
	cfg, err := buildPoolConfig(poolTestDSN, &driver.DriverOptions{PoolSize: 7})
	require.NoError(t, err)
	require.Equal(t, int32(7), cfg.MaxConns)
}

func TestBuildPoolConfig_DSNSettingPreserved(t *testing.T) {
	// An explicit pool_max_conns in the DSN must not be overridden by the
	// floor, even when below it.
	cfg, err := buildPoolConfig(poolTestDSN+"?pool_max_conns=7", &driver.DriverOptions{})
	require.NoError(t, err)
	require.Equal(t, int32(7), cfg.MaxConns)
}

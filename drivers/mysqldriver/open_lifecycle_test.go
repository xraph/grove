package mysqldriver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/observability/telemetrytest"
)

func TestFailedOpenReleasesPool(t *testing.T) {
	db := New()
	defer db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, db.Open(ctx, "test@tcp(127.0.0.1:1)/test"), context.Canceled)
	require.Nil(t, db.db, "a failed Open must not retain an unclosed sql.DB")
}

func TestOpenPreservesExistingPool(t *testing.T) {
	db := New()
	db.db = telemetrytest.SQLDB(t, nil)
	existing := db.db
	require.ErrorContains(t, db.Open(t.Context(), "unused"), "already opened")
	require.Same(t, existing, db.db)
	require.NoError(t, db.Ping(t.Context()))
}

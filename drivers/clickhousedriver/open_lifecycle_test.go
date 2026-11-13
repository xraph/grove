package clickhousedriver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/observability/telemetrytest"
)

func TestOpenPreservesExistingPool(t *testing.T) {
	db := New()
	db.db = telemetrytest.SQLDB(t, nil)
	existing := db.db
	require.ErrorContains(t, db.Open(t.Context(), "unused"), "already opened")
	require.Same(t, existing, db.db)
	require.NoError(t, db.Ping(t.Context()))
}

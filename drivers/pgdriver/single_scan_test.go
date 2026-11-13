package pgdriver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
)

type singleScanRow struct {
	grove.BaseModel `grove:"table:jobs"`
	N               int64 `grove:"n"`
}

func TestSingleScanLimitsServerResultWithoutChangingQuery(t *testing.T) {
	server := startFakePG(t)
	db := New()
	require.NoError(t, db.Open(t.Context(), server.dsn()))
	defer db.Close()
	lastQuery := func() string {
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.queries[len(server.queries)-1]
	}
	var row singleScanRow
	q := db.NewSelect(&row).OrderExpr("n").Limit(50).Offset(3)
	require.NoError(t, q.Scan(t.Context()))
	require.Contains(t, lastQuery(), "LIMIT 1 OFFSET 3")
	var rows []singleScanRow
	require.NoError(t, q.Scan(t.Context(), &rows))
	require.Contains(t, lastQuery(), "LIMIT 50 OFFSET 3", "single scan must not change a reusable query")
	require.EqualValues(t, 1, row.N)
	require.Len(t, rows, 1)
}

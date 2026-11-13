package mysqldriver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/observability/telemetrytest"
)

type singleScanRow struct {
	grove.BaseModel `grove:"table:jobs"`
	N               int64 `grove:"n"`
}

func TestSingleScanLimitsServerResultWithoutChangingQuery(t *testing.T) {
	var queries []string
	db := New()
	db.db = telemetrytest.SQLDB(t, func(query string) error { queries = append(queries, query); return nil })
	var row singleScanRow
	q := db.NewSelect(&row).OrderExpr("n").Limit(50).Offset(3)
	require.NoError(t, q.Scan(t.Context()))
	require.Len(t, queries, 1)
	require.Contains(t, queries[0], "LIMIT 1 OFFSET 3")
	var rows []singleScanRow
	require.NoError(t, q.Scan(t.Context(), &rows))
	require.Len(t, queries, 2)
	require.Contains(t, queries[1], "LIMIT 50 OFFSET 3", "single scan must not change a reusable query")
	require.EqualValues(t, 1, row.N)
	require.Len(t, rows, 1)
}

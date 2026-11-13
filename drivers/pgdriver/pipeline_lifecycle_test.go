package pgdriver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/observability/telemetrytest"
	"github.com/xraph/grove/stream"
)

func TestPipelineFailureReleasesSingleConnectionPool(t *testing.T) {
	db := pgdriver.New()
	require.NoError(t, db.Open(t.Context(), startPostgres(t), driver.WithPoolSize(1)))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err := db.Exec(t.Context(), "CREATE TABLE resource_rows (id BIGINT PRIMARY KEY)")
	require.NoError(t, err)
	_, err = db.Exec(t.Context(), "INSERT INTO resource_rows VALUES (1), (2), (3)")
	require.NoError(t, err)
	recorder := telemetrytest.Spans(t)
	traceCtx, parent := telemetrytest.Parent(t.Context())
	defer parent.End()
	source, err := db.NewSelect((*resourceRow)(nil)).OrderExpr("id").StreamBatch(traceCtx, 1)
	require.NoError(t, err)
	defer source.Close()
	failure := errors.New("invalid streamed row")
	mapped := stream.Map(source, func(value any) (any, error) {
		row, ok := value.(*resourceRow)
		require.True(t, ok)
		if row.ID == 2 {
			return nil, failure
		}
		return value, nil
	})
	rows, err := stream.Chunk(mapped, 3).Collect(traceCtx)
	require.ErrorIs(t, err, failure)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	fetches := 0
	for _, span := range telemetrytest.Ended(recorder) {
		if telemetrytest.AttrMap(span)["db.operation.name"] == "FETCH" {
			fetches++
		}
	}
	require.Equal(t, 2, fetches, "failed pipelines must stop issuing FETCH")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, db.Ping(ctx), "Collect must return the stream's only pool connection")
	require.NoError(t, source.Close())
}

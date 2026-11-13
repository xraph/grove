package pgdriver

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/driver"
)

type fetchCountingTx struct {
	driver.Tx
	remaining int
	queries   int
	execs     []string
	commits   int
	rollbacks int
}

func (tx *fetchCountingTx) Exec(_ context.Context, query string, _ ...any) (driver.Result, error) {
	tx.execs = append(tx.execs, query)
	return nil, nil
}
func (tx *fetchCountingTx) Query(context.Context, string, ...any) (driver.Rows, error) {
	tx.queries++
	count := min(2, tx.remaining)
	tx.remaining -= count
	return &fetchCountingRows{remaining: count}, nil
}
func (tx *fetchCountingTx) Commit() error   { tx.commits++; return nil }
func (tx *fetchCountingTx) Rollback() error { tx.rollbacks++; return nil }

type fetchCountingRows struct {
	driver.Rows
	remaining int
}

func (r *fetchCountingRows) Next() bool   { r.remaining--; return r.remaining >= 0 }
func (r *fetchCountingRows) Err() error   { return nil }
func (r *fetchCountingRows) Close() error { return nil }

func TestCursorAvoidsRedundantQueries(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 4, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			tx := &fetchCountingTx{remaining: count}
			c, err := newPgCursor(t.Context(), tx, "SELECT id FROM jobs", nil, 2, true)
			require.NoError(t, err)
			got := 0
			for c.Next() {
				got++
			}
			require.NoError(t, c.Err())
			require.Equal(t, count, got)
			require.Equal(t, count/2+1, tx.queries)
			require.NoError(t, c.Close())
			require.NoError(t, c.Close())
			require.Len(t, tx.execs, 1, "COMMIT already closes an owned server cursor")
			require.Equal(t, 1, tx.commits)
			require.Zero(t, tx.rollbacks)
		})
	}
}

func TestBorrowedCursorStillClosesServerCursor(t *testing.T) {
	tx := &fetchCountingTx{}
	c, err := newPgCursor(t.Context(), tx, "SELECT id FROM jobs", nil, 2, false)
	require.NoError(t, err)
	require.NoError(t, c.Close())
	require.Len(t, tx.execs, 2)
	require.Contains(t, tx.execs[1], "CLOSE ")
	require.Zero(t, tx.commits)
	require.Zero(t, tx.rollbacks)
}

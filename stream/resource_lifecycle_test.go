package stream

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type trackedCursor struct {
	*mockCursor
	closes   int
	closeErr error
}

func (c *trackedCursor) Close() error { c.closes++; c.mockCursor.Close(); return c.closeErr }

func TestBatchCursorAvoidsRedundantFetches(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 4, 5} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			calls := 0
			var cursors []*trackedCursor
			bc := NewBatchCursor(func(offset, limit int) (Cursor, error) {
				require.Equal(t, calls*limit, offset)
				calls++
				data := make([]int, max(0, min(limit, count-offset)))
				for i := range data {
					data[i] = offset + i
				}
				c := &trackedCursor{mockCursor: &mockCursor{data: data}}
				cursors = append(cursors, c)
				return c, nil
			}, 2)
			got, err := New(bc, decodeInt).Collect(t.Context())
			require.NoError(t, err)
			require.Len(t, got, count)
			require.Equal(t, count/2+1, calls, "a short batch already proves exhaustion")
			require.NoError(t, bc.Close())
			for _, c := range cursors {
				require.Equal(t, 1, c.closes)
			}
		})
	}
}

func TestBatchCursorReleasesFailedBatch(t *testing.T) {
	iterErr := errors.New("read failed")
	closeErr := errors.New("close failed")
	c := &trackedCursor{mockCursor: &mockCursor{iterErr: iterErr}, closeErr: closeErr}
	bc := NewBatchCursor(func(_, _ int) (Cursor, error) { return c, nil }, 2)
	require.False(t, bc.Next())
	require.ErrorIs(t, bc.Err(), iterErr)
	require.ErrorIs(t, bc.Err(), closeErr)
	require.Equal(t, 1, c.closes)
	_ = bc.Close()
	require.Equal(t, 1, c.closes)
}

func TestBatchCursorRejectsInvalidSizeWithoutFetching(t *testing.T) {
	for _, size := range []int{0, -1} {
		bc := NewBatchCursor(func(_, _ int) (Cursor, error) { t.Fatal("invalid batch must not query"); return nil, nil }, size)
		require.False(t, bc.Next())
		require.Error(t, bc.Err())
	}
}

func TestTakeDoesNotFetchPastLimit(t *testing.T) {
	for _, n := range []int{-1, 0, 1, 2} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			calls := 0
			bc := NewBatchCursor(func(offset, _ int) (Cursor, error) {
				calls++
				if calls > max(n, 0) {
					return nil, errors.New("unnecessary next query")
				}
				return &mockCursor{data: []int{offset}}, nil
			}, 1)
			got, err := Take(New(bc, decodeInt), n).Collect(t.Context())
			require.NoError(t, err)
			require.Len(t, got, max(n, 0))
			require.Equal(t, max(n, 0), calls)
		})
	}
}

func TestTakeLimitSurvivesComposedTransforms(t *testing.T) {
	s, mc := newIntStream([]int{1, 2, 3, 4, 5, 6, 7, 8})
	got, err := Chunk(Map(Take(Filter(s, func(n int) bool { return n%2 == 0 }), 3), func(n int) (int, error) { return n * 10, nil }), 2).Collect(t.Context())
	require.NoError(t, err)
	require.Equal(t, [][]int{{20, 40}, {60}}, got)
	require.Equal(t, 6, mc.pos, "no extra underlying row may be consumed")
}

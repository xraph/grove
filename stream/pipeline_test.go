package stream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunk_GroupsIntoFixedBatches(t *testing.T) {
	s, mc := newIntStream([]int{1, 2, 3, 4, 5})

	batches, err := Chunk(s, 2).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, [][]int{{1, 2}, {3, 4}, {5}}, batches)
	assert.True(t, mc.closed, "Collect should close the cursor")
}

func TestChunk_ExactMultiple(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4})

	batches, err := Chunk(s, 2).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, [][]int{{1, 2}, {3, 4}}, batches)
}

func TestChunk_ScanErrorOnFirstRowFails(t *testing.T) {
	scanErr := errors.New("scan failed")
	mc := &mockCursor{data: []int{1, 2}, scanErr: scanErr}

	_, err := Chunk(New[int](mc, decodeInt), 3).Collect(t.Context())

	require.ErrorIs(t, err, scanErr)
}

func TestChunk_EmptyCursorYieldsNothing(t *testing.T) {
	s, _ := newIntStream(nil)

	batches, err := Chunk(s, 4).Collect(t.Context())

	require.NoError(t, err)
	assert.Empty(t, batches)
}

func TestFilter_KeepsMatchingRows(t *testing.T) {
	// The last row matches, so Filter never has to skip past the end.
	s, _ := newIntStream([]int{1, 2, 3, 4, 5, 6})

	got, err := Filter(s, func(v int) bool { return v%2 == 0 }).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int{2, 4, 6}, got)
}

func TestFilter_PropagatesDecodeError(t *testing.T) {
	scanErr := errors.New("bad row")
	mc := &mockCursor{data: []int{1}, scanErr: scanErr}

	_, err := Filter(New[int](mc, decodeInt), func(int) bool { return true }).Collect(t.Context())

	require.ErrorIs(t, err, scanErr)
}

func TestFilter_TrailingRejectedRowsAreDropped(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3})

	got, err := Filter(s, func(v int) bool { return v%2 == 0 }).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int{2}, got)
}

func TestTake_WholeStreamWhenNIsLarge(t *testing.T) {
	s, _ := newIntStream([]int{7, 8, 9})

	got, err := Take(s, 3).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int{7, 8, 9}, got)
}

func TestTake_StopsAfterN(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5})

	got, err := Take(s, 2).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, got)
}

func TestMap_PropagatesDecodeError(t *testing.T) {
	scanErr := errors.New("bad row")
	mc := &mockCursor{data: []int{1, 2}, scanErr: scanErr}
	called := false

	_, err := Map(New[int](mc, decodeInt), func(int) (string, error) {
		called = true

		return "", nil
	}).Collect(t.Context())

	require.ErrorIs(t, err, scanErr)
	assert.False(t, called, "fn must not run for a row that failed to decode")
}

func TestMap_PropagatesFnError(t *testing.T) {
	fnErr := errors.New("transform failed")
	s, _ := newIntStream([]int{1, 2})

	_, err := Map(s, func(int) (int, error) { return 0, fnErr }).Collect(t.Context())

	require.ErrorIs(t, err, fnErr)
}

func TestMap_TransformsEveryRow(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3})

	got, err := Map(s, func(v int) (int, error) { return v * 10, nil }).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, []int{10, 20, 30}, got)
}

func TestStream_AllIteratesAndStopsEarly(t *testing.T) {
	s, mc := newIntStream([]int{1, 2, 3})

	var got []int
	for v, err := range s.All {
		require.NoError(t, err)
		got = append(got, v)
		if v == 2 {
			break
		}
	}

	assert.Equal(t, []int{1, 2}, got)
	assert.True(t, mc.closed, "All should close the cursor when the loop breaks")
}

func TestStream_AllYieldsTerminalError(t *testing.T) {
	iterErr := errors.New("connection reset")
	mc := &mockCursor{data: []int{1}, iterErr: iterErr}
	s := New[int](mc, decodeInt)

	var (
		vals []int
		errs []error
	)
	for v, err := range s.All {
		if err != nil {
			errs = append(errs, err)

			continue
		}
		vals = append(vals, v)
	}

	assert.Equal(t, []int{1}, vals)
	require.Len(t, errs, 1)
	require.ErrorIs(t, errs[0], iterErr)
}

func TestFilter_ReportsCursorErrorAfterRejectedRows(t *testing.T) {
	iterErr := errors.New("connection reset")
	mc := &mockCursor{data: []int{2, 3}, iterErr: iterErr}

	got, err := Filter(New[int](mc, decodeInt), func(v int) bool { return v%2 == 0 }).Collect(t.Context())

	require.ErrorIs(t, err, iterErr)
	assert.Equal(t, []int{2}, got)
}

func TestPipeline_ComposesFilterTakeAndChunk(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5, 6, 7, 8})

	evens := Filter(s, func(v int) bool { return v%2 == 0 })
	got, err := Chunk(Take(evens, 3), 2).Collect(t.Context())

	require.NoError(t, err)
	assert.Equal(t, [][]int{{2, 4}, {6}}, got)
}

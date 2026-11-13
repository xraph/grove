package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkStopsFetchingAfterDecodeFailure(t *testing.T) {
	failure := errors.New("invalid row")
	queries := 0
	bc := NewBatchCursor(func(offset, _ int) (Cursor, error) {
		queries++
		if offset == 4 {
			return &mockCursor{}, nil
		}
		return &mockCursor{data: []int{offset}}, nil
	}, 1)
	s := New(bc, func(c Cursor) (int, error) {
		value, err := decodeInt(c)
		if value == 1 {
			return 0, failure
		}
		return value, err
	})
	batches, err := Chunk(s, 3).Collect(t.Context())
	require.ErrorIs(t, err, failure)
	require.Equal(t, [][]int{{0}}, batches)
	require.Equal(t, 2, queries, "a failed row must end database fetching")
}

func TestChunkInvalidSizeDoesNotQuery(t *testing.T) {
	for _, size := range []int{0, -1} {
		queries := 0
		bc := NewBatchCursor(func(_, _ int) (Cursor, error) {
			queries++
			return &mockCursor{data: []int{1}}, nil
		}, 1)
		s := Chunk(New(bc, decodeInt), size)
		require.False(t, s.Next(t.Context()))
		require.Error(t, s.Err())
		require.NoError(t, s.Close())
		require.Zero(t, queries)
	}
}

type denyStreamRow struct{ calls int }

func (h *denyStreamRow) RunStreamRowHook(context.Context, any, any) (int, error) {
	h.calls++
	return hookDeny, nil
}

func TestTransformsPreserveSourceHooksAndCloseOnce(t *testing.T) {
	for _, transform := range []struct {
		name  string
		apply func(*Stream[int]) *Stream[int]
	}{
		{"map", func(s *Stream[int]) *Stream[int] { return Map(s, func(v int) (int, error) { return v, nil }) }},
		{"filter", func(s *Stream[int]) *Stream[int] { return Filter(s, func(int) bool { return true }) }},
		{"take", func(s *Stream[int]) *Stream[int] { return Take(s, 2) }},
		{"chunk", func(s *Stream[int]) *Stream[int] {
			return Map(Chunk(s, 2), func(v []int) (int, error) { return len(v), nil })
		}},
	} {
		t.Run(transform.name, func(t *testing.T) {
			cursor := &trackedCursor{mockCursor: &mockCursor{data: []int{1, 2, 3}}}
			hooks := &denyStreamRow{}
			source := NewWithHooks(cursor, decodeInt, hooks, nil)
			out, err := transform.apply(source).Collect(t.Context())
			require.ErrorContains(t, err, "row denied")
			require.Empty(t, out)
			require.Equal(t, 1, hooks.calls)
			require.Equal(t, 1, cursor.pos)
			require.NoError(t, source.Close())
			require.Equal(t, 1, cursor.closes)
		})
	}
}

func TestTransformDoesNotResumeStoppedSource(t *testing.T) {
	failure := errors.New("decode failed")
	cursor := &trackedCursor{mockCursor: &mockCursor{data: []int{1, 2, 3}}}
	source := New(cursor, func(Cursor) (int, error) { return 0, failure })
	require.False(t, source.Next(t.Context()))
	_, err := Map(source, func(v int) (int, error) { return v, nil }).Collect(t.Context())
	require.ErrorIs(t, err, failure)
	require.Equal(t, 1, cursor.pos)
	require.NoError(t, source.Close())
	require.Equal(t, 1, cursor.closes)
}

func TestCollectReportsCloseFailure(t *testing.T) {
	failure := errors.New("cursor cleanup failed")
	cursor := &trackedCursor{mockCursor: &mockCursor{data: []int{1}}, closeErr: failure}
	source := New(cursor, decodeInt)
	_, err := Map(source, func(v int) (int, error) { return v, nil }).Collect(t.Context())
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, source.Close(), failure)
	require.Equal(t, 1, cursor.closes)
}

func TestTerminalOperationsPreserveCleanupErrors(t *testing.T) {
	failure := errors.New("cursor cleanup failed")
	for _, consume := range []struct {
		name string
		run  func(*Stream[int]) error
	}{
		{"collect", func(s *Stream[int]) error { _, err := s.Collect(t.Context()); return err }},
		{"count", func(s *Stream[int]) error { _, err := s.Count(t.Context()); return err }},
		{"reduce", func(s *Stream[int]) error { _, err := Reduce(s, 0, func(a, b int) int { return a + b }); return err }},
		{"foreach", func(s *Stream[int]) error { return ForEach(s, func(int) error { return nil }) }},
		{"all", func(s *Stream[int]) error {
			for _, err := range s.All {
				if err != nil {
					return err
				}
			}
			return nil
		}},
	} {
		t.Run(consume.name, func(t *testing.T) {
			cursor := &trackedCursor{mockCursor: &mockCursor{data: []int{1}}, closeErr: failure}
			s := New(cursor, decodeInt)
			require.ErrorIs(t, consume.run(s), failure)
			require.ErrorIs(t, s.Close(), failure)
			require.Equal(t, 1, cursor.closes)
		})
	}
}

func TestPipelineClosesOnPanicAndDoesNotReadClosedSource(t *testing.T) {
	cursor := &trackedCursor{mockCursor: &mockCursor{data: []int{1, 2}}}
	source := New(cursor, decodeInt)
	mapped := Map(source, func(int) (int, error) { panic("callback failed") })
	require.Panics(t, func() { _, _ = mapped.Collect(t.Context()) })
	require.NoError(t, source.Close())
	require.Equal(t, 1, cursor.closes)
	var called bool
	_, err := Map(source, func(v int) (int, error) { called = true; return v, nil }).Collect(t.Context())
	require.NoError(t, err)
	require.False(t, called)
	require.Equal(t, 1, cursor.pos)
	require.Equal(t, 1, cursor.closes)
}

func TestPipelinePreservesSkipHooksAndContext(t *testing.T) {
	cursor := &contextAwareCursor{mockCursor: &mockCursor{data: []int{1, 2, 3, 4, 5}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hooks := &mockHookRunner{fn: func(seen context.Context, _ any, value any) (int, error) {
		require.Equal(t, ctx, seen)
		if value == 1 {
			return hookSkip, nil
		}
		return hookAllow, nil
	}}
	s := NewWithHooks(cursor, decodeInt, hooks, nil)
	got, err := Chunk(Take(Map(Filter(s, func(v int) bool { return v%2 == 0 }), func(v int) (int, error) { return v * 10, nil }), 2), 3).Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, [][]int{{20, 40}}, got)
	require.Equal(t, 4, cursor.pos)
	for _, seen := range cursor.contexts {
		require.Equal(t, ctx, seen)
	}
}

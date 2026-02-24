package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock cursor
// ---------------------------------------------------------------------------

// mockCursor simulates a database cursor over a slice of ints.
type mockCursor struct {
	data    []int
	pos     int // starts at 0; first Next() moves to 1
	closed  bool
	scanErr error
	iterErr error // error returned by Err() after iteration
}

func (m *mockCursor) Next() bool {
	if m.closed {
		return false
	}
	m.pos++
	return m.pos <= len(m.data)
}

func (m *mockCursor) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.pos < 1 || m.pos > len(m.data) {
		return errors.New("mockCursor: scan out of range")
	}
	if p, ok := dest[0].(*int); ok {
		*p = m.data[m.pos-1]
	}
	return nil
}

func (m *mockCursor) Columns() ([]string, error) {
	return []string{"value"}, nil
}

func (m *mockCursor) Close() error {
	m.closed = true
	return nil
}

func (m *mockCursor) Err() error {
	return m.iterErr
}

// ---------------------------------------------------------------------------
// Decode function
// ---------------------------------------------------------------------------

func decodeInt(c Cursor) (int, error) {
	var v int
	err := c.Scan(&v)
	return v, err
}

// ---------------------------------------------------------------------------
// Helper: create a stream from a slice of ints
// ---------------------------------------------------------------------------

func newIntStream(data []int) (*Stream[int], *mockCursor) {
	mc := &mockCursor{data: data}
	s := New[int](mc, decodeInt)
	return s, mc
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestStream_NextAndValue(t *testing.T) {
	s, _ := newIntStream([]int{10, 20, 30})
	ctx := context.Background()

	var got []int
	for s.Next(ctx) {
		got = append(got, s.Value())
	}

	require.NoError(t, s.Err())
	assert.Equal(t, []int{10, 20, 30}, got)
}

func TestStream_Collect(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5})
	ctx := context.Background()

	result, err := s.Collect(ctx)

	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, result)
}

func TestStream_Count(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5, 6, 7})
	ctx := context.Background()

	count, err := s.Count(ctx)

	require.NoError(t, err)
	assert.Equal(t, int64(7), count)
}

func TestStream_Close(t *testing.T) {
	s, mc := newIntStream([]int{1, 2, 3})

	err := s.Close()

	require.NoError(t, err)
	assert.True(t, mc.closed, "cursor should be closed")

	// After closing, Next should return false.
	assert.False(t, s.Next(context.Background()), "Next should return false after Close")
}

func TestStream_Close_Idempotent(t *testing.T) {
	s, mc := newIntStream([]int{1})

	err1 := s.Close()
	err2 := s.Close()

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.True(t, mc.closed)
}

func TestStream_EmptyCursor(t *testing.T) {
	s, _ := newIntStream([]int{})
	ctx := context.Background()

	result, err := s.Collect(ctx)

	require.NoError(t, err)
	assert.Empty(t, result, "collect on empty cursor should return nil/empty slice")
}

func TestStream_EmptyCursor_Count(t *testing.T) {
	s, _ := newIntStream([]int{})
	ctx := context.Background()

	count, err := s.Count(ctx)

	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestStream_ContextCancellation(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5})
	ctx, cancel := context.WithCancel(context.Background())

	// Read one value then cancel.
	require.True(t, s.Next(ctx))
	assert.Equal(t, 1, s.Value())

	cancel()

	// Next call after cancellation should return false.
	assert.False(t, s.Next(ctx), "Next should return false after context cancellation")
	assert.Error(t, s.Err(), "Err should report context cancellation")
	assert.ErrorIs(t, s.Err(), context.Canceled)
}

func TestStream_ScanError(t *testing.T) {
	scanErr := errors.New("scan failure")
	mc := &mockCursor{data: []int{1, 2, 3}, scanErr: scanErr}
	s := New[int](mc, decodeInt)
	ctx := context.Background()

	// First Next should fail because decode calls Scan which returns an error.
	assert.False(t, s.Next(ctx), "Next should return false when Scan errors")
	assert.ErrorIs(t, s.Err(), scanErr)
}

func TestStream_CursorError(t *testing.T) {
	iterErr := errors.New("cursor iteration error")
	mc := &mockCursor{data: []int{}, iterErr: iterErr}
	s := New[int](mc, decodeInt)
	ctx := context.Background()

	result, err := s.Collect(ctx)

	assert.Nil(t, result)
	assert.ErrorIs(t, err, iterErr)
}

func TestStream_ForEach(t *testing.T) {
	s, _ := newIntStream([]int{10, 20, 30})

	var collected []int
	err := ForEach[int](s, func(v int) error {
		collected = append(collected, v)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, []int{10, 20, 30}, collected)
}

func TestStream_ForEach_CallbackError(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3})
	callbackErr := errors.New("callback failed")

	count := 0
	err := ForEach[int](s, func(_ int) error {
		count++
		if count == 2 {
			return callbackErr
		}
		return nil
	})

	assert.ErrorIs(t, err, callbackErr)
	assert.Equal(t, 2, count, "ForEach should stop at the first callback error")
}

func TestStream_ForEach_Empty(t *testing.T) {
	s, _ := newIntStream([]int{})

	called := false
	err := ForEach[int](s, func(_ int) error {
		called = true
		return nil
	})

	require.NoError(t, err)
	assert.False(t, called, "callback should not be invoked on an empty stream")
}

func TestStream_Reduce(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3, 4, 5})

	sum, err := Reduce[int, int](s, 0, func(acc int, v int) int {
		return acc + v
	})

	require.NoError(t, err)
	assert.Equal(t, 15, sum, "1+2+3+4+5 = 15")
}

func TestStream_Reduce_Product(t *testing.T) {
	s, _ := newIntStream([]int{2, 3, 4})

	product, err := Reduce[int, int](s, 1, func(acc int, v int) int {
		return acc * v
	})

	require.NoError(t, err)
	assert.Equal(t, 24, product, "2*3*4 = 24")
}

func TestStream_Reduce_Empty(t *testing.T) {
	s, _ := newIntStream([]int{})

	result, err := Reduce[int, int](s, 42, func(acc int, v int) int {
		return acc + v
	})

	require.NoError(t, err)
	assert.Equal(t, 42, result, "reduce on empty stream returns the initial value")
}

func TestStream_Reduce_ToString(t *testing.T) {
	s, _ := newIntStream([]int{1, 2, 3})

	result, err := Reduce[int, string](s, "", func(acc string, v int) string {
		if acc != "" {
			acc += ","
		}
		acc += string(rune('0' + v))
		return acc
	})

	require.NoError(t, err)
	assert.Equal(t, "1,2,3", result)
}

func TestStream_CollectCloseCursor(t *testing.T) {
	s, mc := newIntStream([]int{1, 2})

	_, err := s.Collect(context.Background())

	require.NoError(t, err)
	assert.True(t, mc.closed, "Collect should close the cursor when done")
}

func TestStream_CountClosesCursor(t *testing.T) {
	s, mc := newIntStream([]int{1, 2, 3})

	_, err := s.Count(context.Background())

	require.NoError(t, err)
	assert.True(t, mc.closed, "Count should close the cursor when done")
}

// ===========================================================================
// BatchCursor tests
// ===========================================================================

// batchMockCursor simulates a database cursor over a slice of []any rows,
// implementing the stream.Cursor interface for use in BatchCursor tests.
type batchMockCursor struct {
	rows    [][]any
	cols    []string
	pos     int
	closed  bool
	iterErr error
}

func (m *batchMockCursor) Next() bool {
	if m.closed {
		return false
	}
	m.pos++
	return m.pos <= len(m.rows)
}

func (m *batchMockCursor) Scan(dest ...any) error {
	if m.pos < 1 || m.pos > len(m.rows) {
		return errors.New("batchMockCursor: scan out of range")
	}
	row := m.rows[m.pos-1]
	for i, d := range dest {
		if i >= len(row) {
			break
		}
		if p, ok := d.(*int); ok {
			if v, ok := row[i].(int); ok {
				*p = v
			}
		}
		if p, ok := d.(*string); ok {
			if v, ok := row[i].(string); ok {
				*p = v
			}
		}
	}
	return nil
}

func (m *batchMockCursor) Columns() ([]string, error) {
	return m.cols, nil
}

func (m *batchMockCursor) Close() error {
	m.closed = true
	return nil
}

func (m *batchMockCursor) Err() error {
	return m.iterErr
}

// makeBatchFetcher creates a fetchBatch function that returns cursors
// from a pre-defined set of batches. Each batch is a slice of int rows.
func makeBatchFetcher(batches [][]int) func(offset, limit int) (Cursor, error) {
	batchIdx := 0
	return func(_, _ int) (Cursor, error) {
		if batchIdx >= len(batches) {
			// Return empty cursor to signal end.
			return &batchMockCursor{cols: []string{"value"}}, nil
		}
		batch := batches[batchIdx]
		batchIdx++
		rows := make([][]any, len(batch))
		for i, v := range batch {
			rows[i] = []any{v}
		}
		return &batchMockCursor{rows: rows, cols: []string{"value"}}, nil
	}
}

func TestBatchCursor_MultipleBatches(t *testing.T) {
	// Three batches: [1, 2], [3, 4], [5]
	fetcher := makeBatchFetcher([][]int{{1, 2}, {3, 4}, {5}})
	bc := NewBatchCursor(fetcher, 2)

	var got []int
	for bc.Next() {
		var v int
		require.NoError(t, bc.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, bc.Err())
	assert.Equal(t, []int{1, 2, 3, 4, 5}, got)
}

func TestBatchCursor_SingleBatch(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{{10, 20, 30}})
	bc := NewBatchCursor(fetcher, 5)

	var got []int
	for bc.Next() {
		var v int
		require.NoError(t, bc.Scan(&v))
		got = append(got, v)
	}
	require.NoError(t, bc.Err())
	assert.Equal(t, []int{10, 20, 30}, got)
}

func TestBatchCursor_EmptyBatch(t *testing.T) {
	// Immediately returns empty batch — no rows.
	fetcher := makeBatchFetcher([][]int{})
	bc := NewBatchCursor(fetcher, 10)

	assert.False(t, bc.Next(), "empty batch should stop iteration immediately")
	require.NoError(t, bc.Err())
}

func TestBatchCursor_FirstBatchEmpty(t *testing.T) {
	// The very first fetch returns no rows.
	calls := 0
	fetcher := func(_, _ int) (Cursor, error) {
		calls++
		return &batchMockCursor{cols: []string{"value"}}, nil
	}
	bc := NewBatchCursor(fetcher, 5)

	assert.False(t, bc.Next(), "should return false when first batch is empty")
	require.NoError(t, bc.Err())
	assert.Equal(t, 1, calls, "should call fetchBatch exactly once")
}

func TestBatchCursor_FetchError(t *testing.T) {
	fetchErr := errors.New("database connection lost")
	fetcher := func(_, _ int) (Cursor, error) {
		return nil, fetchErr
	}
	bc := NewBatchCursor(fetcher, 10)

	assert.False(t, bc.Next(), "should return false on fetch error")
	assert.ErrorIs(t, bc.Err(), fetchErr)
}

func TestBatchCursor_FetchErrorOnSecondBatch(t *testing.T) {
	fetchErr := errors.New("timeout on second batch")
	call := 0
	fetcher := func(_, _ int) (Cursor, error) {
		call++
		if call == 1 {
			rows := [][]any{{1}, {2}}
			return &batchMockCursor{rows: rows, cols: []string{"value"}}, nil
		}
		return nil, fetchErr
	}
	bc := NewBatchCursor(fetcher, 2)

	var got []int
	for bc.Next() {
		var v int
		require.NoError(t, bc.Scan(&v))
		got = append(got, v)
	}
	// Should have read first batch successfully.
	assert.Equal(t, []int{1, 2}, got)
	// Then got error on second batch.
	assert.ErrorIs(t, bc.Err(), fetchErr)
}

func TestBatchCursor_Columns(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{{1}})
	bc := NewBatchCursor(fetcher, 5)

	// Before calling Next, current is nil, so Columns returns nil.
	cols, err := bc.Columns()
	require.NoError(t, err)
	assert.Nil(t, cols)

	// After Next, should have columns from the current cursor.
	require.True(t, bc.Next())
	cols, err = bc.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"value"}, cols)
}

func TestBatchCursor_Close(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{{1, 2, 3}})
	bc := NewBatchCursor(fetcher, 5)

	// Advance once to load current cursor.
	require.True(t, bc.Next())

	err := bc.Close()
	require.NoError(t, err)

	// After close, Next should return false.
	assert.False(t, bc.Next(), "Next should return false after Close")
}

func TestBatchCursor_CloseBeforeNext(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{{1}})
	bc := NewBatchCursor(fetcher, 5)

	// Close without ever calling Next.
	err := bc.Close()
	require.NoError(t, err)
	assert.False(t, bc.Next(), "Next should return false after Close")
}

func TestBatchCursor_Scan_NilCurrent(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{})
	bc := NewBatchCursor(fetcher, 5)

	// current is nil before any Next — Scan should return nil error (bc.Err() is nil).
	err := bc.Scan()
	require.NoError(t, err)
}

func TestBatchCursor_Err_NoError(t *testing.T) {
	fetcher := makeBatchFetcher([][]int{{1}})
	bc := NewBatchCursor(fetcher, 5)

	assert.NoError(t, bc.Err(), "Err should be nil before any error occurs")
}

func TestBatchCursor_CursorIterError(t *testing.T) {
	iterErr := errors.New("cursor iteration error")
	fetcher := func(_, _ int) (Cursor, error) {
		return &batchMockCursor{
			rows:    [][]any{{1}},
			cols:    []string{"value"},
			iterErr: iterErr,
		}, nil
	}
	bc := NewBatchCursor(fetcher, 2)

	// First Next succeeds (reads row 1).
	require.True(t, bc.Next())

	// Second Next: current cursor exhausted, Err() returns iterErr.
	assert.False(t, bc.Next())
	assert.ErrorIs(t, bc.Err(), iterErr)
}

// ===========================================================================
// HookRunner integration tests
// ===========================================================================

// mockHookRunner implements stream.HookRunner for testing.
type mockHookRunner struct {
	fn func(ctx context.Context, qc any, row any) (int, error)
}

func (m *mockHookRunner) RunStreamRowHook(ctx context.Context, qc any, row any) (int, error) {
	return m.fn(ctx, qc, row)
}

func TestStream_NewWithHooks_AllowAll(t *testing.T) {
	mc := &mockCursor{data: []int{10, 20, 30}}
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, _ any) (int, error) {
		return 0, nil // Allow
	}}

	s := NewWithHooks[int](mc, decodeInt, hooks, nil)
	ctx := context.Background()

	result, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 20, 30}, result)
}

func TestStream_NewWithHooks_SkipRows(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3, 4, 5}}
	// Skip even numbers.
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, row any) (int, error) {
		v := row.(int)
		if v%2 == 0 {
			return 3, nil // Skip
		}
		return 0, nil // Allow
	}}

	s := NewWithHooks[int](mc, decodeInt, hooks, nil)
	ctx := context.Background()

	result, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 3, 5}, result)
}

func TestStream_NewWithHooks_DenyStopsIteration(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3, 4, 5}}
	// Deny when value == 3.
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, row any) (int, error) {
		v := row.(int)
		if v == 3 {
			return 1, nil // Deny
		}
		return 0, nil // Allow
	}}

	s := NewWithHooks[int](mc, decodeInt, hooks, nil)
	ctx := context.Background()

	var got []int
	for s.Next(ctx) {
		got = append(got, s.Value())
	}
	// Should have collected values before the deny.
	assert.Equal(t, []int{1, 2}, got)
	// Should have an error from the deny.
	assert.Error(t, s.Err())
	assert.Contains(t, s.Err().Error(), "denied by hook")
}

func TestStream_NewWithHooks_HookError(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3}}
	hookErr := errors.New("hook execution failed")
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, row any) (int, error) {
		v := row.(int)
		if v == 2 {
			return 0, hookErr
		}
		return 0, nil
	}}

	s := NewWithHooks[int](mc, decodeInt, hooks, nil)
	ctx := context.Background()

	var got []int
	for s.Next(ctx) {
		got = append(got, s.Value())
	}
	assert.Equal(t, []int{1}, got)
	assert.ErrorIs(t, s.Err(), hookErr)
}

func TestStream_WithHooks_Chaining(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3}}
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, _ any) (int, error) {
		return 0, nil // Allow all
	}}

	s := New[int](mc, decodeInt)
	// WithHooks should return the same stream for chaining.
	returned := s.WithHooks(hooks, "test-qc")
	assert.Same(t, s, returned, "WithHooks should return the same stream pointer")

	ctx := context.Background()
	result, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, result)
}

func TestStream_WithHooks_NilRunner(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3}}
	s := New[int](mc, decodeInt)
	s.WithHooks(nil, nil)

	ctx := context.Background()
	result, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2, 3}, result)
}

func TestStream_NewWithHooks_SkipAllRows(t *testing.T) {
	mc := &mockCursor{data: []int{1, 2, 3}}
	// Skip everything.
	hooks := &mockHookRunner{fn: func(_ context.Context, _ any, _ any) (int, error) {
		return 3, nil // Skip
	}}

	s := NewWithHooks[int](mc, decodeInt, hooks, nil)
	ctx := context.Background()

	result, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestStream_NewWithHooks_QueryContext(t *testing.T) {
	mc := &mockCursor{data: []int{1}}
	var capturedQC any
	hooks := &mockHookRunner{fn: func(_ context.Context, qc any, _ any) (int, error) {
		capturedQC = qc
		return 0, nil
	}}

	qcValue := "my-query-context"
	s := NewWithHooks[int](mc, decodeInt, hooks, qcValue)
	ctx := context.Background()

	_, err := s.Collect(ctx)
	require.NoError(t, err)
	assert.Equal(t, "my-query-context", capturedQC, "query context should be passed to hook runner")
}

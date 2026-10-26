package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeChangeSource struct {
	events     []ChangeEvent[int]
	pos        int
	err        error
	closeErr   error
	closeCalls int
	nextCalls  int
}

func (f *fakeChangeSource) Next(context.Context) bool {
	f.nextCalls++
	if f.pos >= len(f.events) {
		return false
	}
	f.pos++

	return true
}

func (f *fakeChangeSource) Event() ChangeEvent[int] { return f.events[f.pos-1] }
func (f *fakeChangeSource) Err() error              { return f.err }

func (f *fakeChangeSource) Close() error {
	f.closeCalls++

	return f.closeErr
}

func (f *fakeChangeSource) ResumeToken() any {
	if f.pos == 0 {
		return nil
	}

	return f.events[f.pos-1].ResumeToken
}

func intPtr(v int) *int { return &v }

func TestChangeOp_String(t *testing.T) {
	cases := map[ChangeOp]string{
		ChangeInsert:  "INSERT",
		ChangeUpdate:  "UPDATE",
		ChangeDelete:  "DELETE",
		ChangeReplace: "REPLACE",
		ChangeOp(99):  "UNKNOWN",
	}
	for op, want := range cases {
		assert.Equal(t, want, op.String())
	}
}

func TestChangeStream_NextEventAndResumeToken(t *testing.T) {
	src := &fakeChangeSource{events: []ChangeEvent[int]{
		{Operation: ChangeInsert, After: intPtr(1), ResumeToken: "t1"},
		{Operation: ChangeDelete, Before: intPtr(1), ResumeToken: "t2"},
	}}
	cs := NewChangeStream[int](src)
	ctx := t.Context()

	require.True(t, cs.Next(ctx))
	assert.Equal(t, ChangeInsert, cs.Event().Operation)
	assert.Equal(t, 1, *cs.Event().After)
	assert.Equal(t, "t1", cs.ResumeToken())

	require.True(t, cs.Next(ctx))
	assert.Equal(t, ChangeDelete, cs.Event().Operation)
	assert.Nil(t, cs.Event().After)
	assert.Equal(t, "t2", cs.ResumeToken())

	assert.False(t, cs.Next(ctx))
	require.NoError(t, cs.Err())

	// Once done, Next must not poll the source again.
	calls := src.nextCalls
	assert.False(t, cs.Next(ctx))
	assert.Equal(t, calls, src.nextCalls)
}

func TestChangeStream_SourceErrorSurfaces(t *testing.T) {
	srcErr := errors.New("oplog lost")
	cs := NewChangeStream[int](&fakeChangeSource{err: srcErr})

	assert.False(t, cs.Next(t.Context()))
	require.ErrorIs(t, cs.Err(), srcErr)
}

func TestChangeStream_CloseIsIdempotent(t *testing.T) {
	closeErr := errors.New("close failed")
	src := &fakeChangeSource{events: []ChangeEvent[int]{{}}, closeErr: closeErr}
	cs := NewChangeStream[int](src)

	require.ErrorIs(t, cs.Close(), closeErr)
	require.NoError(t, cs.Close(), "second Close is a no-op")
	assert.Equal(t, 1, src.closeCalls)
	assert.False(t, cs.Next(t.Context()), "a closed stream yields nothing")
}

func TestChangeStream_All(t *testing.T) {
	src := &fakeChangeSource{events: []ChangeEvent[int]{
		{Operation: ChangeInsert, After: intPtr(1)},
		{Operation: ChangeUpdate, After: intPtr(2)},
		{Operation: ChangeReplace, After: intPtr(3)},
	}}
	cs := NewChangeStream[int](src)

	var ops []ChangeOp
	for ev, err := range cs.All {
		require.NoError(t, err)
		ops = append(ops, ev.Operation)
	}

	assert.Equal(t, []ChangeOp{ChangeInsert, ChangeUpdate, ChangeReplace}, ops)
	assert.Equal(t, 1, src.closeCalls, "All closes the stream")
}

func TestChangeStream_AllStopsEarlyAndReportsError(t *testing.T) {
	src := &fakeChangeSource{events: []ChangeEvent[int]{{}, {}, {}}}
	n := 0
	for range NewChangeStream[int](src).All {
		n++

		break
	}
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, src.closeCalls)

	srcErr := errors.New("boom")
	var gotErr error
	for _, err := range NewChangeStream[int](&fakeChangeSource{err: srcErr}).All {
		gotErr = err
	}
	require.ErrorIs(t, gotErr, srcErr)
}

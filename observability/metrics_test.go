package observability

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
)

func TestInMemoryCollector_RecordsAndCounts(t *testing.T) {
	c := NewInMemoryCollector()
	boom := errors.New("boom")

	c.RecordQuery("users", "SELECT", 5*time.Millisecond, nil)
	c.RecordQuery("users", "INSERT", time.Millisecond, boom)

	assert.Equal(t, int64(2), c.TotalQueries.Load())
	assert.Equal(t, int64(1), c.TotalErrors.Load())

	entries := c.Entries()
	require.Len(t, entries, 2)
	assert.Equal(t, "users", entries[0].Table)
	assert.Equal(t, "SELECT", entries[0].Operation)
	assert.Equal(t, 5*time.Millisecond, entries[0].Duration)
	require.NoError(t, entries[0].Error)
	require.ErrorIs(t, entries[1].Error, boom)
	assert.False(t, entries[1].Timestamp.IsZero())

	// Entries returns a copy, so mutating it leaves the collector alone.
	entries[0].Table = "changed"
	assert.Equal(t, "users", c.Entries()[0].Table)

	c.Reset()
	assert.Empty(t, c.Entries())
	assert.Zero(t, c.TotalQueries.Load())
	assert.Zero(t, c.TotalErrors.Load())
}

func TestInMemoryCollector_ConcurrentRecord(t *testing.T) {
	c := NewInMemoryCollector()

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { c.RecordQuery("t", "SELECT", 0, nil) })
	}
	wg.Wait()

	assert.Equal(t, int64(20), c.TotalQueries.Load())
	assert.Len(t, c.Entries(), 20)
}

func TestHook_QueryTiming(t *testing.T) {
	c := NewInMemoryCollector()
	h := NewHook(c)
	qc := &hook.QueryContext{Table: "orders", Operation: hook.OpSelect}

	res, err := h.BeforeQuery(t.Context(), qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, res.Decision)
	require.IsType(t, time.Time{}, qc.Values["_obs_start"])

	require.NoError(t, h.AfterQuery(t.Context(), qc, nil))

	entries := c.Entries()
	require.Len(t, entries, 1)
	assert.Equal(t, "orders", entries[0].Table)
	assert.Equal(t, "SELECT", entries[0].Operation)
	assert.GreaterOrEqual(t, entries[0].Duration, time.Duration(0))
}

func TestHook_MutationTiming(t *testing.T) {
	c := NewInMemoryCollector()
	h := NewHook(c)
	qc := &hook.QueryContext{Table: "orders", Operation: hook.OpUpdate, Values: map[string]any{"keep": 1}}

	res, err := h.BeforeMutation(t.Context(), qc, nil)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, res.Decision)
	assert.Equal(t, 1, qc.Values["keep"], "existing values are preserved")

	require.NoError(t, h.AfterMutation(t.Context(), qc, nil, nil))

	entries := c.Entries()
	require.Len(t, entries, 1)
	assert.Equal(t, "UPDATE", entries[0].Operation)
}

func TestHook_AfterWithoutBeforeRecordsNothing(t *testing.T) {
	c := NewInMemoryCollector()
	h := NewHook(c)
	qc := &hook.QueryContext{Table: "orders", Operation: hook.OpDelete}

	require.NoError(t, h.AfterQuery(t.Context(), qc, nil))
	require.NoError(t, h.AfterMutation(t.Context(), qc, nil, nil))

	assert.Empty(t, c.Entries())
}

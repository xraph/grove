package audit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove/hook"
)

type recordingWriter struct {
	entries []*AuditEntry
	err     error
}

func (w *recordingWriter) WriteEntry(_ context.Context, e *AuditEntry) error {
	w.entries = append(w.entries, e)

	return w.err
}

func TestHook_AfterMutationWritesEntry(t *testing.T) {
	w := &recordingWriter{}
	h := NewHook(w)
	data := map[string]any{"id": 1}
	qc := &hook.QueryContext{
		Table:     "accounts",
		Operation: hook.OpInsert,
		RawQuery:  "INSERT INTO accounts",
		TenantID:  "tenant-a",
	}

	require.NoError(t, h.AfterMutation(t.Context(), qc, data, nil))

	require.Len(t, w.entries, 1)
	e := w.entries[0]
	assert.Equal(t, "accounts", e.Table)
	assert.Equal(t, "INSERT", e.Operation)
	assert.Equal(t, "INSERT INTO accounts", e.Query)
	assert.Equal(t, "tenant-a", e.TenantID)
	assert.Equal(t, data, e.Data)
	assert.False(t, e.Timestamp.IsZero())
}

func TestHook_AfterMutationWrapsWriterError(t *testing.T) {
	writeErr := errors.New("disk full")
	h := NewHook(&recordingWriter{err: writeErr})

	err := h.AfterMutation(t.Context(), &hook.QueryContext{Operation: hook.OpDelete}, nil, nil)

	require.ErrorIs(t, err, writeErr)
	assert.Contains(t, err.Error(), "audit: write entry")
}

func TestNewLogHook(t *testing.T) {
	// A nil logger falls back to a no-op logger rather than panicking.
	h := NewLogHook(nil)
	lw, ok := h.writer.(*LogWriter)
	require.True(t, ok)
	require.NotNil(t, lw.Logger)

	require.NoError(t, h.AfterMutation(t.Context(), &hook.QueryContext{Table: "t", Operation: hook.OpUpdate}, nil, nil))
}

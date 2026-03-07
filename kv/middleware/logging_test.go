package middleware_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/middleware"
)

func TestLoggingHook_BeforeQuery_SetsStart(t *testing.T) {
	logger := log.NewTestLogger()
	h := middleware.NewLogging(logger)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "user:1",
	}

	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)

	start, ok := qc.Values["_log_start"].(time.Time)
	require.True(t, ok, "_log_start should be a time.Time in Values")
	assert.WithinDuration(t, time.Now(), start, 100*time.Millisecond)
}

func TestLoggingHook_AfterQuery_LogsFields(t *testing.T) {
	logger := log.NewTestLogger()
	h := middleware.NewLogging(logger)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "user:42",
		Values: map[string]any{
			"_log_start": time.Now().Add(-10 * time.Millisecond),
		},
	}

	err := h.AfterQuery(ctx, qc, nil)
	require.NoError(t, err)

	tl := logger.(*log.TestLogger)
	assert.True(t, tl.AssertHasLog("INFO", "kv command"), "should have logged 'kv command' at INFO level")
}

func TestLoggingHook_NilLogger(t *testing.T) {
	// The LoggingHook stores the logger directly; passing nil causes a panic
	// in AfterQuery when it calls h.logger.Info. BeforeQuery should still work
	// since it only writes to Values without touching the logger.
	h := middleware.NewLogging(nil)

	ctx := context.Background()
	qc := &hook.QueryContext{
		Operation: kv.OpGet,
		RawQuery:  "key",
	}

	// BeforeQuery does not use the logger, so it should not panic.
	result, err := h.BeforeQuery(ctx, qc)
	require.NoError(t, err)
	assert.Equal(t, hook.Allow, result.Decision)
	_, ok := qc.Values["_log_start"]
	assert.True(t, ok, "_log_start should still be set even with nil logger")
}

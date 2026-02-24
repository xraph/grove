// Package middleware provides composable middleware for Grove KV operations.
//
// Each middleware implements hook.PreQueryHook and/or hook.PostQueryHook
// and is registered via kv.WithHook() during store creation.
package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
)

// LoggingHook logs every KV operation with structured fields.
type LoggingHook struct {
	logger *slog.Logger
}

var (
	_ hook.PreQueryHook  = (*LoggingHook)(nil)
	_ hook.PostQueryHook = (*LoggingHook)(nil)
)

// NewLogging creates a new logging middleware.
func NewLogging(logger *slog.Logger) *LoggingHook {
	return &LoggingHook{logger: logger}
}

func (h *LoggingHook) BeforeQuery(ctx context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	qc.Values["_log_start"] = time.Now()
	return &hook.HookResult{Decision: hook.Allow}, nil
}

func (h *LoggingHook) AfterQuery(ctx context.Context, qc *hook.QueryContext, result any) error {
	start, _ := qc.Values["_log_start"].(time.Time)
	h.logger.Info("kv command",
		slog.String("op", kv.CommandName(qc.Operation)),
		slog.String("key", qc.RawQuery),
		slog.Duration("latency", time.Since(start)),
	)
	return nil
}

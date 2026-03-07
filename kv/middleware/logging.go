// Package middleware provides composable middleware for Grove KV operations.
//
// Each middleware implements hook.PreQueryHook and/or hook.PostQueryHook
// and is registered via kv.WithHook() during store creation.
package middleware

import (
	"context"
	"time"

	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
)

// LoggingHook logs every KV operation with structured fields.
type LoggingHook struct {
	logger log.Logger
}

var (
	_ hook.PreQueryHook  = (*LoggingHook)(nil)
	_ hook.PostQueryHook = (*LoggingHook)(nil)
)

// NewLogging creates a new logging middleware.
func NewLogging(logger log.Logger) *LoggingHook {
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
		log.String("op", kv.CommandName(qc.Operation)),
		log.String("key", qc.RawQuery),
		log.Duration("latency", time.Since(start)),
	)
	return nil
}

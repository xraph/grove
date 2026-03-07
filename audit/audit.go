// Package audit provides a PostMutationHook that logs all mutations
// to an audit trail (Chronicle). Every INSERT, UPDATE, DELETE is recorded
// with the table, operation, timestamp, and optionally the changed data.
package audit

import (
	"context"
	"fmt"
	"time"

	log "github.com/xraph/go-utils/log"
	"github.com/xraph/grove/hook"
)

// AuditEntry represents a single audit log entry.
type AuditEntry struct { //nolint:revive // AuditEntry is the established public API name
	Timestamp time.Time `json:"timestamp"`
	Table     string    `json:"table"`
	Operation string    `json:"operation"`
	Query     string    `json:"query,omitempty"`
	TenantID  string    `json:"tenant_id,omitempty"`
	Data      any       `json:"data,omitempty"`
}

// Writer is the interface for persisting audit entries.
// Implementations could write to a database, file, message queue, etc.
type Writer interface {
	WriteEntry(ctx context.Context, entry *AuditEntry) error
}

// LogWriter writes audit entries to a structured logger.
type LogWriter struct {
	Logger log.Logger
}

// WriteEntry logs the audit entry.
func (w *LogWriter) WriteEntry(_ context.Context, entry *AuditEntry) error {
	w.Logger.Info("audit",
		log.String("table", entry.Table),
		log.String("operation", entry.Operation),
		log.Time("timestamp", entry.Timestamp),
		log.String("tenant_id", entry.TenantID),
		log.String("query", entry.Query),
	)
	return nil
}

// Hook is a PostMutationHook that records all mutations.
type Hook struct {
	writer Writer
}

var _ hook.PostMutationHook = (*Hook)(nil)

// NewHook creates a new audit hook with the given writer.
func NewHook(w Writer) *Hook {
	return &Hook{writer: w}
}

// NewLogHook creates an audit hook that logs to a structured logger.
func NewLogHook(logger log.Logger) *Hook {
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	return &Hook{writer: &LogWriter{Logger: logger}}
}

// AfterMutation records the mutation in the audit trail.
func (h *Hook) AfterMutation(ctx context.Context, qc *hook.QueryContext, data, _ any) error {
	entry := &AuditEntry{
		Timestamp: time.Now(),
		Table:     qc.Table,
		Operation: qc.Operation.String(),
		Query:     qc.RawQuery,
		TenantID:  qc.TenantID,
		Data:      data,
	}

	if err := h.writer.WriteEntry(ctx, entry); err != nil {
		return fmt.Errorf("audit: write entry: %w", err)
	}
	return nil
}

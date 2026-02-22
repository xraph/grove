// Package observability provides hooks for exposing query timing and error
// metrics. Currently uses a simple in-memory collector interface that can
// be wired to Prometheus, OpenTelemetry, or any metrics backend.
package observability

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xraph/grove/hook"
)

// Collector is the interface for recording metrics.
type Collector interface {
	// RecordQuery records timing for a query/mutation.
	RecordQuery(table, operation string, duration time.Duration, err error)
}

// InMemoryCollector is a simple in-memory metrics collector for testing.
type InMemoryCollector struct {
	mu      sync.Mutex
	entries []MetricEntry

	TotalQueries atomic.Int64
	TotalErrors  atomic.Int64
}

// MetricEntry represents a single recorded metric.
type MetricEntry struct {
	Table     string
	Operation string
	Duration  time.Duration
	Error     error
	Timestamp time.Time
}

// NewInMemoryCollector creates a new in-memory collector.
func NewInMemoryCollector() *InMemoryCollector {
	return &InMemoryCollector{}
}

func (c *InMemoryCollector) RecordQuery(table, operation string, duration time.Duration, err error) {
	c.TotalQueries.Add(1)
	if err != nil {
		c.TotalErrors.Add(1)
	}
	c.mu.Lock()
	c.entries = append(c.entries, MetricEntry{
		Table:     table,
		Operation: operation,
		Duration:  duration,
		Error:     err,
		Timestamp: time.Now(),
	})
	c.mu.Unlock()
}

// Entries returns all recorded metric entries.
func (c *InMemoryCollector) Entries() []MetricEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]MetricEntry, len(c.entries))
	copy(result, c.entries)
	return result
}

// Reset clears all recorded entries.
func (c *InMemoryCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
	c.TotalQueries.Store(0)
	c.TotalErrors.Store(0)
}

// Hook is a combined PreQueryHook and PostQueryHook that measures query timing.
type Hook struct {
	collector Collector
}

var _ hook.PreQueryHook = (*Hook)(nil)
var _ hook.PostQueryHook = (*Hook)(nil)
var _ hook.PreMutationHook = (*Hook)(nil)
var _ hook.PostMutationHook = (*Hook)(nil)

// NewHook creates a new observability hook with the given collector.
func NewHook(c Collector) *Hook {
	return &Hook{collector: c}
}

// BeforeQuery records the start time.
func (h *Hook) BeforeQuery(ctx context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	qc.Values["_obs_start"] = time.Now()
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// AfterQuery records the duration.
func (h *Hook) AfterQuery(ctx context.Context, qc *hook.QueryContext, result any) error {
	start, ok := qc.Values["_obs_start"].(time.Time)
	if !ok {
		return nil
	}
	h.collector.RecordQuery(qc.Table, qc.Operation.String(), time.Since(start), nil)
	return nil
}

// BeforeMutation records the start time for mutations.
func (h *Hook) BeforeMutation(ctx context.Context, qc *hook.QueryContext, data any) (*hook.HookResult, error) {
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	qc.Values["_obs_start"] = time.Now()
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// AfterMutation records the duration for mutations.
func (h *Hook) AfterMutation(ctx context.Context, qc *hook.QueryContext, data any, result any) error {
	start, ok := qc.Values["_obs_start"].(time.Time)
	if !ok {
		return nil
	}
	h.collector.RecordQuery(qc.Table, qc.Operation.String(), time.Since(start), nil)
	return nil
}

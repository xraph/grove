package middleware

import (
	"context"
	"sync"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
)

// StampedeHook provides singleflight-based cache stampede protection.
// When multiple goroutines request the same key concurrently, only one
// performs the actual fetch; others wait and share the result.
type StampedeHook struct {
	mu    sync.Mutex
	calls map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val any
	err error
}

var _ hook.PreQueryHook = (*StampedeHook)(nil)

// NewStampede creates a new stampede protection middleware.
func NewStampede() *StampedeHook {
	return &StampedeHook{
		calls: make(map[string]*call),
	}
}

func (h *StampedeHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	// Only apply to GET operations.
	if qc.Operation != kv.OpGet {
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	key := qc.RawQuery
	if key == "" {
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	h.mu.Lock()
	if c, ok := h.calls[key]; ok {
		h.mu.Unlock()
		c.wg.Wait()
		// Mark that this was a shared result.
		if qc.Values == nil {
			qc.Values = make(map[string]any)
		}
		qc.Values["_stampede_shared"] = true
		return &hook.HookResult{Decision: hook.Allow}, nil
	}

	c := &call{}
	c.wg.Add(1)
	h.calls[key] = c
	h.mu.Unlock()

	// Store the call reference for cleanup in AfterQuery.
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	qc.Values["_stampede_call"] = c
	qc.Values["_stampede_key"] = key

	return &hook.HookResult{Decision: hook.Allow}, nil
}

// Complete signals that the GET for this key is done.
// This should be called after the operation completes.
func (h *StampedeHook) Complete(key string) {
	h.mu.Lock()
	if c, ok := h.calls[key]; ok {
		delete(h.calls, key)
		h.mu.Unlock()
		c.wg.Done()
	} else {
		h.mu.Unlock()
	}
}

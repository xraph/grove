package middleware

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/xraph/grove/hook"
	kv "github.com/xraph/grove/kv"
	"github.com/xraph/grove/kv/internal/readhook"
)

// StampedeHook shares one raw backend read among concurrent GETs for the same
// store and physical key. Each caller still runs its own policy and decode hooks.
type StampedeHook struct {
	mu    sync.Mutex
	calls map[readhook.Key]*call
}

type call struct {
	done chan struct{}
	val  []byte
	err  error
}

var errReadInterrupted = errors.New("middleware: shared read did not complete")
var _ hook.PreQueryHook = (*StampedeHook)(nil)

// NewStampede creates a hook that deduplicates concurrent GETs.
func NewStampede() *StampedeHook {
	return &StampedeHook{calls: make(map[readhook.Key]*call)}
}

func (h *StampedeHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	if qc.Operation == kv.OpGet {
		readhook.Add(qc, h.wrapRead)
	} else if cacheMutation(qc.Operation) {
		readhook.OnFinish(qc, func() {
			keys, ok := qc.Values["_kv_keys"].([]string)
			if !ok {
				keys = []string{qc.RawQuery}
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			for _, name := range keys {
				delete(h.calls, readhook.Key{Owner: qc.Values["_kv_owner"], Name: name})
			}
		})
	}
	return &hook.HookResult{Decision: hook.Allow}, nil
}

func (h *StampedeHook) wrapRead(next readhook.Read) readhook.Read {
	return func(ctx context.Context, key readhook.Key) ([]byte, error) {
		h.mu.Lock()
		if c, ok := h.calls[key]; ok {
			h.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.done:
				return bytes.Clone(c.val), c.err
			}
		}
		c := &call{done: make(chan struct{}), err: errReadInterrupted}
		h.calls[key] = c
		h.mu.Unlock()
		defer func() {
			h.mu.Lock()
			if h.calls[key] == c {
				delete(h.calls, key)
			}
			close(c.done)
			h.mu.Unlock()
		}()
		c.val, c.err = next(ctx, key)
		return bytes.Clone(c.val), c.err
	}
}

// Complete forgets the current call so a later GET can start a fresh read.
// Existing waiters still receive the result when their read finishes.
//
// Deprecated: the store completes reads automatically; no call is required.
func (h *StampedeHook) Complete(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for k := range h.calls {
		if k.Name == key {
			delete(h.calls, k)
		}
	}
}

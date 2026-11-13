// Package readhook connects KV middleware to the raw read after policy and key resolution.
package readhook

import (
	"context"

	"github.com/xraph/grove/hook"
)

// Key separates physical keys belonging to different stores.
type Key struct {
	Owner any
	Name  string
}

// Read fetches raw bytes for one resolved key.
type Read func(context.Context, Key) ([]byte, error)

// Wrapper surrounds a raw read without bypassing the store's other hooks.
type Wrapper func(Read) Read

// Add registers a read wrapper in hook order.
func Add(qc *hook.QueryContext, wrapper Wrapper) {
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	wrappers, ok := qc.Values["_kv_read_wrappers"].([]Wrapper)
	if !ok {
		wrappers = nil
	}
	qc.Values["_kv_read_wrappers"] = append(wrappers, wrapper)
}

// Run applies wrappers only after every pre-query hook has allowed the read.
func Run(ctx context.Context, qc *hook.QueryContext, name string, read Read) ([]byte, error) {
	wrappers, ok := qc.Values["_kv_read_wrappers"].([]Wrapper)
	if !ok {
		wrappers = nil
	}
	for i := len(wrappers) - 1; i >= 0; i-- {
		read = wrappers[i](read)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return read(ctx, Key{Owner: qc.Values["_kv_owner"], Name: name})
}

// OnFinish registers cleanup that runs even if a later hook or the driver fails.
func OnFinish(qc *hook.QueryContext, fn func()) {
	if qc.Values == nil {
		qc.Values = make(map[string]any)
	}
	callbacks, ok := qc.Values["_kv_finish"].([]func())
	if !ok {
		callbacks = nil
	}
	qc.Values["_kv_finish"] = append(callbacks, fn)
}

// Finish releases operation state and invalidates writes before returning to the caller.
func Finish(qc *hook.QueryContext) {
	callbacks, ok := qc.Values["_kv_finish"].([]func())
	if !ok {
		callbacks = nil
	}
	delete(qc.Values, "_kv_finish")
	for _, fn := range callbacks {
		fn()
	}
}

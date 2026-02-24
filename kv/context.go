package kv

import "github.com/xraph/grove/hook"

// newCommandContext builds a hook.QueryContext for a KV command.
// It maps KV-specific metadata into the shared hook infrastructure.
func newCommandContext(op hook.Operation, keys []string, extra map[string]any) *hook.QueryContext {
	values := make(map[string]any, len(extra)+1)
	for k, v := range extra {
		values[k] = v
	}
	values["_kv_keys"] = keys

	var rawQuery string
	if len(keys) > 0 {
		rawQuery = keys[0]
	}

	return &hook.QueryContext{
		Operation: op,
		Table:     "", // may be set to keyspace prefix by middleware
		RawQuery:  rawQuery,
		Values:    values,
	}
}

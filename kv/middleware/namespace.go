package middleware

import (
	"context"

	"github.com/xraph/grove/hook"
)

// NamespaceHook automatically prepends a prefix to all keys.
// This is useful for multi-tenant isolation at the key level.
type NamespaceHook struct {
	prefix    string
	separator string
}

var _ hook.PreQueryHook = (*NamespaceHook)(nil)

// NewNamespace creates a namespace middleware that prepends the given prefix
// to all keys (e.g., "tenant:acme" → "tenant:acme:user:123").
func NewNamespace(prefix string, separator ...string) *NamespaceHook {
	sep := ":"
	if len(separator) > 0 {
		sep = separator[0]
	}
	return &NamespaceHook{prefix: prefix, separator: sep}
}

func (h *NamespaceHook) BeforeQuery(_ context.Context, qc *hook.QueryContext) (*hook.HookResult, error) {
	// Modify the keys stored in Values.
	if keys, ok := qc.Values["_kv_keys"].([]string); ok {
		modified := make([]string, len(keys))
		for i, k := range keys {
			modified[i] = h.prefix + h.separator + k
		}
		qc.Values["_kv_keys"] = modified
		if len(modified) > 0 {
			qc.RawQuery = modified[0]
		}
	}

	return &hook.HookResult{Decision: hook.Modify}, nil
}

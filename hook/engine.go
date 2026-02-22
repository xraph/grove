package hook

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Scope determines when a hook applies.
type Scope struct {
	// Tables restricts the hook to these tables. Empty means all tables.
	Tables []string

	// Operations restricts the hook to these operations. Empty means all operations.
	Operations []Operation

	// Priority determines execution order (lower = earlier). Default: 100.
	Priority int
}

// registeredHook associates a hook with its scope.
type registeredHook struct {
	hook     any
	scope    Scope
	priority int
}

// Engine manages hook registration and execution.
type Engine struct {
	mu    sync.RWMutex
	hooks []registeredHook
}

// NewEngine creates a new hook engine.
func NewEngine() *Engine {
	return &Engine{}
}

// AddHook registers a hook with the given scope. The hook must implement at
// least one of PreQueryHook, PostQueryHook, PreMutationHook, PostMutationHook.
func (e *Engine) AddHook(h any, scope ...Scope) {
	e.mu.Lock()
	defer e.mu.Unlock()

	s := Scope{Priority: 100}
	if len(scope) > 0 {
		s = scope[0]
		if s.Priority == 0 {
			s.Priority = 100
		}
	}

	e.hooks = append(e.hooks, registeredHook{
		hook:     h,
		scope:    s,
		priority: s.Priority,
	})

	// Keep hooks sorted by priority.
	sort.SliceStable(e.hooks, func(i, j int) bool {
		return e.hooks[i].priority < e.hooks[j].priority
	})
}

// RunPreQuery executes all matching PreQueryHook hooks.
// Returns aggregated extra filters and any deny error.
func (e *Engine) RunPreQuery(ctx context.Context, qc *QueryContext) (*HookResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	aggregated := &HookResult{Decision: Allow}

	for _, rh := range e.hooks {
		if !matches(rh.scope, qc.Table, qc.Operation) {
			continue
		}

		h, ok := rh.hook.(PreQueryHook)
		if !ok {
			continue
		}

		result, err := h.BeforeQuery(ctx, qc)
		if err != nil {
			return nil, fmt.Errorf("hook: pre-query: %w", err)
		}
		if result == nil {
			continue
		}

		switch result.Decision {
		case Deny:
			return result, result.Error
		case Modify:
			aggregated.Decision = Modify
			aggregated.Filters = append(aggregated.Filters, result.Filters...)
		}
	}

	return aggregated, nil
}

// RunPostQuery executes all matching PostQueryHook hooks.
func (e *Engine) RunPostQuery(ctx context.Context, qc *QueryContext, result any) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rh := range e.hooks {
		if !matches(rh.scope, qc.Table, qc.Operation) {
			continue
		}

		h, ok := rh.hook.(PostQueryHook)
		if !ok {
			continue
		}

		if err := h.AfterQuery(ctx, qc, result); err != nil {
			return fmt.Errorf("hook: post-query: %w", err)
		}
	}

	return nil
}

// RunPreMutation executes all matching PreMutationHook hooks.
func (e *Engine) RunPreMutation(ctx context.Context, qc *QueryContext, data any) (*HookResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	aggregated := &HookResult{Decision: Allow}

	for _, rh := range e.hooks {
		if !matches(rh.scope, qc.Table, qc.Operation) {
			continue
		}

		h, ok := rh.hook.(PreMutationHook)
		if !ok {
			continue
		}

		result, err := h.BeforeMutation(ctx, qc, data)
		if err != nil {
			return nil, fmt.Errorf("hook: pre-mutation: %w", err)
		}
		if result == nil {
			continue
		}

		switch result.Decision {
		case Deny:
			return result, result.Error
		case Modify:
			aggregated.Decision = Modify
			aggregated.Filters = append(aggregated.Filters, result.Filters...)
		}
	}

	return aggregated, nil
}

// RunPostMutation executes all matching PostMutationHook hooks.
func (e *Engine) RunPostMutation(ctx context.Context, qc *QueryContext, data any, result any) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rh := range e.hooks {
		if !matches(rh.scope, qc.Table, qc.Operation) {
			continue
		}

		h, ok := rh.hook.(PostMutationHook)
		if !ok {
			continue
		}

		if err := h.AfterMutation(ctx, qc, data, result); err != nil {
			return fmt.Errorf("hook: post-mutation: %w", err)
		}
	}

	return nil
}

// RunStreamRowHook executes all matching StreamRowHook hooks for a single
// streamed row. Returns the decision as an int (matching Decision constants)
// and any error. A Skip decision means the row should be skipped; a Deny
// decision means iteration should stop.
func (e *Engine) RunStreamRowHook(ctx context.Context, qc *QueryContext, row any) (int, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, rh := range e.hooks {
		if !matches(rh.scope, qc.Table, qc.Operation) {
			continue
		}

		h, ok := rh.hook.(StreamRowHook)
		if !ok {
			continue
		}

		decision, err := h.OnStreamRow(ctx, qc, row)
		if err != nil {
			return int(Deny), fmt.Errorf("hook: stream-row: %w", err)
		}
		if decision == Skip || decision == Deny {
			return int(decision), nil
		}
	}

	return int(Allow), nil
}

// matches returns true if the scope matches the given table and operation.
func matches(scope Scope, table string, op Operation) bool {
	if len(scope.Tables) > 0 {
		found := false
		for _, t := range scope.Tables {
			if t == table {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(scope.Operations) > 0 {
		found := false
		for _, o := range scope.Operations {
			if o == op {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

package crdt

import "context"

// SyncHook intercepts changes during sync operations.
// Implement this interface to validate, transform, filter, or audit
// data flowing between nodes during sync.
//
// Use [BaseSyncHook] for a no-op default that you can selectively override.
type SyncHook interface {
	// BeforeInboundChange is called before a remote change is merged locally.
	// Return a modified change to transform it, nil to skip it, or an error to abort.
	BeforeInboundChange(ctx context.Context, change *ChangeRecord) (*ChangeRecord, error)

	// AfterInboundChange is called after a remote change has been merged locally.
	// This is useful for audit logging or triggering side effects.
	AfterInboundChange(ctx context.Context, change *ChangeRecord) error

	// BeforeOutboundChange is called before a local change is sent to a remote peer.
	// Return a modified change to transform it, nil to skip it, or an error to abort.
	BeforeOutboundChange(ctx context.Context, change *ChangeRecord) (*ChangeRecord, error)

	// BeforeOutboundRead is called before changes are returned in a pull response.
	// Receives the full slice; return a filtered or modified slice.
	BeforeOutboundRead(ctx context.Context, changes []ChangeRecord) ([]ChangeRecord, error)
}

// BaseSyncHook provides no-op implementations of all SyncHook methods.
// Embed it in your struct and override only the methods you need:
//
//	type MyHook struct { crdt.BaseSyncHook }
//
//	func (h *MyHook) BeforeInboundChange(ctx context.Context, c *crdt.ChangeRecord) (*crdt.ChangeRecord, error) {
//	    // your logic here
//	    return c, nil
//	}
type BaseSyncHook struct{}

// BeforeInboundChange passes the change through unchanged.
func (BaseSyncHook) BeforeInboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	return c, nil
}

// AfterInboundChange does nothing.
func (BaseSyncHook) AfterInboundChange(_ context.Context, _ *ChangeRecord) error {
	return nil
}

// BeforeOutboundChange passes the change through unchanged.
func (BaseSyncHook) BeforeOutboundChange(_ context.Context, c *ChangeRecord) (*ChangeRecord, error) {
	return c, nil
}

// BeforeOutboundRead passes the slice through unchanged.
func (BaseSyncHook) BeforeOutboundRead(_ context.Context, cs []ChangeRecord) ([]ChangeRecord, error) {
	return cs, nil
}

// SyncHookChain composes multiple SyncHooks into a sequential chain.
// Each hook is called in order. For Before* methods, the output of one
// hook becomes the input of the next. A nil return skips the change.
// An error return aborts the chain immediately.
type SyncHookChain struct {
	hooks []SyncHook
}

// NewSyncHookChain creates a chain with the given hooks.
func NewSyncHookChain(hooks ...SyncHook) *SyncHookChain {
	return &SyncHookChain{hooks: hooks}
}

// Add appends a hook to the chain.
func (c *SyncHookChain) Add(hook SyncHook) {
	if hook != nil {
		c.hooks = append(c.hooks, hook)
	}
}

// Len returns the number of hooks in the chain.
func (c *SyncHookChain) Len() int {
	if c == nil {
		return 0
	}
	return len(c.hooks)
}

// BeforeInboundChange calls each hook in order. If any hook returns nil,
// the change is skipped. If any returns an error, the chain aborts.
func (c *SyncHookChain) BeforeInboundChange(ctx context.Context, change *ChangeRecord) (*ChangeRecord, error) {
	if c == nil || len(c.hooks) == 0 {
		return change, nil
	}
	current := change
	for _, h := range c.hooks {
		var err error
		current, err = h.BeforeInboundChange(ctx, current)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil // Skip this change.
		}
	}
	return current, nil
}

// AfterInboundChange calls each hook in order. If any returns an error,
// subsequent hooks are still called (best-effort notification).
func (c *SyncHookChain) AfterInboundChange(ctx context.Context, change *ChangeRecord) error {
	if c == nil || len(c.hooks) == 0 {
		return nil
	}
	var firstErr error
	for _, h := range c.hooks {
		if err := h.AfterInboundChange(ctx, change); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// BeforeOutboundChange calls each hook in order. If any returns nil,
// the change is skipped. If any returns an error, the chain aborts.
func (c *SyncHookChain) BeforeOutboundChange(ctx context.Context, change *ChangeRecord) (*ChangeRecord, error) {
	if c == nil || len(c.hooks) == 0 {
		return change, nil
	}
	current := change
	for _, h := range c.hooks {
		var err error
		current, err = h.BeforeOutboundChange(ctx, current)
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, nil // Skip this change.
		}
	}
	return current, nil
}

// BeforeOutboundRead calls each hook in order. Each hook receives the
// output of the previous hook. An error aborts the chain.
func (c *SyncHookChain) BeforeOutboundRead(ctx context.Context, changes []ChangeRecord) ([]ChangeRecord, error) {
	if c == nil || len(c.hooks) == 0 {
		return changes, nil
	}
	current := changes
	for _, h := range c.hooks {
		var err error
		current, err = h.BeforeOutboundRead(ctx, current)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

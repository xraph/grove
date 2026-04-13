package crdt

import (
	"context"
	"fmt"
	"time"
)

// Plugin is the CRDT plugin for Grove. It implements the grove plugin.Plugin
// interface along with WithHooks and WithMigrations capabilities.
//
// Create a plugin with New() and register it with the hook engine:
//
//	p := crdt.New(crdt.WithNodeID("node-1"))
//	db.Hooks().AddHook(p, hook.Scope{Tables: []string{"documents"}})
type Plugin struct {
	nodeID        string
	clock         Clock
	merge         *MergeEngine
	tombstoneTTL  time.Duration
	maxClockDrift time.Duration
	tables        []string
	metadata      *MetadataStore
	syncHooks     *SyncHookChain
}

// New creates a new CRDT plugin with the given options.
func New(opts ...Option) *Plugin {
	p := &Plugin{
		merge:         NewMergeEngine(),
		tombstoneTTL:  7 * 24 * time.Hour,
		maxClockDrift: 5 * time.Second,
		syncHooks:     NewSyncHookChain(),
	}
	for _, opt := range opts {
		opt(p)
	}

	// Default clock if not provided.
	if p.clock == nil && p.nodeID != "" {
		p.clock = NewHybridClock(p.nodeID, WithMaxDrift(p.maxClockDrift))
	}

	return p
}

// Name returns the plugin identifier.
func (p *Plugin) Name() string { return "crdt" }

// Init is called when the plugin is registered with a DB.
func (p *Plugin) Init(_ context.Context, _ any) error {
	if p.nodeID == "" {
		return fmt.Errorf("crdt: node ID is required; use crdt.WithNodeID()")
	}
	if p.clock == nil {
		p.clock = NewHybridClock(p.nodeID, WithMaxDrift(p.maxClockDrift))
	}
	return nil
}

// Clock returns the plugin's clock instance.
func (p *Plugin) Clock() Clock { return p.clock }

// NodeID returns the plugin's node identifier.
func (p *Plugin) NodeID() string { return p.nodeID }

// MergeEngine returns the plugin's merge engine.
func (p *Plugin) MergeEngine() *MergeEngine { return p.merge }

// SyncHooks returns the plugin's sync hook chain.
func (p *Plugin) SyncHooks() *SyncHookChain { return p.syncHooks }

// SetExecutor sets the database executor for metadata operations.
// This is called by the hooks when they have access to the database.
func (p *Plugin) SetExecutor(exec Executor) {
	p.metadata = NewMetadataStore(exec)
}

// MetadataStore returns the metadata store (nil until SetExecutor is called).
func (p *Plugin) MetadataStore() *MetadataStore { return p.metadata }

// Inspect returns the full CRDT state for a record. Useful for debugging.
func (p *Plugin) Inspect(ctx context.Context, table, pk string) (*State, error) {
	if p.metadata == nil {
		return nil, fmt.Errorf("crdt: no executor set; call SetExecutor() first")
	}
	return p.metadata.ReadState(ctx, table, pk)
}

// CleanupTombstones removes tombstones older than the configured TTL.
func (p *Plugin) CleanupTombstones(ctx context.Context, table string) error {
	if p.metadata == nil {
		return fmt.Errorf("crdt: no executor set")
	}
	cutoff := time.Now().Add(-p.tombstoneTTL).UnixNano()
	return p.metadata.CleanTombstones(ctx, table, cutoff)
}

// RunTombstoneCleanup starts a background loop that periodically removes
// tombstones older than the configured TTL for the given tables. The loop
// runs until the context is cancelled.
func (p *Plugin) RunTombstoneCleanup(ctx context.Context, interval time.Duration, tables ...string) {
	if p.metadata == nil || len(tables) == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, table := range tables {
					if err := p.CleanupTombstones(ctx, table); err != nil && ctx.Err() == nil {
						// Log but don't stop; cleanup is best-effort.
						continue
					}
				}
			}
		}
	}()
}

// EnsureShadowTable creates the shadow table for the given table if it
// doesn't exist. This should be called during migration or initialization.
func (p *Plugin) EnsureShadowTable(ctx context.Context, table string) error {
	if p.metadata == nil {
		return fmt.Errorf("crdt: no executor set")
	}

	ddl := ShadowTableDDL(table)
	if _, err := p.metadata.executor.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("crdt: create shadow table for %s: %w", table, err)
	}

	idx := ShadowTableSyncIndex(table)
	if _, err := p.metadata.executor.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("crdt: create sync index for %s: %w", table, err)
	}

	return nil
}

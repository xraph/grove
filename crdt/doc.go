// Package crdt provides conflict-free replicated data types for Grove.
//
// The crdt package enables offline-first, multi-node, and eventually-consistent
// use cases by adding an optional CRDT layer on top of Grove's existing ORM.
// It supports three CRDT types (LWW-Register, PN-Counter, OR-Set), multiple
// sync topologies (edge-to-cloud, peer-to-peer, hub-and-spoke), and integrates
// with the Forge ecosystem for routing, streaming, and middleware.
//
// # CRDT Types
//
// Tag struct fields with crdt:"type" to enable CRDT merge semantics:
//
//   - crdt:lww   — Last-Writer-Wins Register. Higher HLC timestamp wins.
//   - crdt:counter — PN-Counter. Per-node increment/decrement, merged by max.
//   - crdt:set   — OR-Set. Add-wins observed-remove set.
//
// Fields without crdt: tags work normally with zero overhead.
//
// # Quick Start
//
//	// 1. Define a CRDT-enabled model
//	type Document struct {
//	    grove.BaseModel `grove:"table:documents,alias:d"`
//	    ID        string   `grove:"id,pk"`
//	    Title     string   `grove:"title,crdt:lww"`
//	    ViewCount int64    `grove:"view_count,crdt:counter"`
//	    Tags      []string `grove:"tags,type:jsonb,crdt:set"`
//	}
//
//	// 2. Create the CRDT plugin
//	plugin := crdt.New(crdt.WithNodeID("node-1"))
//	db.Hooks().AddHook(plugin, hook.Scope{Tables: []string{"documents"}})
//
//	// 3. Sync between nodes
//	syncer := crdt.NewSyncer(plugin,
//	    crdt.WithTransport(crdt.HTTPTransport("https://cloud.example.com/sync")),
//	    crdt.WithSyncTables("documents"),
//	)
//	report, err := syncer.Sync(ctx)
//
// # Forge Integration
//
// When running inside a Forge app, use the grove/extension package with
// WithCRDT to get automatic route registration, SSE streaming, and
// middleware support:
//
//	ext := extension.New(
//	    extension.WithDriver(pgdb),
//	    extension.WithCRDT(crdtPlugin, hook.Scope{Tables: []string{"documents"}}),
//	    extension.WithSyncer(syncer),
//	    extension.WithMigrations(crdt.Migrations),
//	)
//	app.RegisterExtension(ext)
//
//	// Routes registered automatically:
//	//   POST /sync/pull    — pull changes from this node
//	//   POST /sync/push    — push changes to this node
//	//   GET  /sync/stream  — SSE real-time change stream
//
// # Sync Hooks
//
// Implement [SyncHook] to intercept data during sync operations for
// validation, transformation, filtering, or auditing:
//
//	type MyHook struct { crdt.BaseSyncHook }
//
//	func (h *MyHook) BeforeInboundChange(ctx context.Context, c *crdt.ChangeRecord) (*crdt.ChangeRecord, error) {
//	    // validate, transform, or reject incoming changes
//	    return c, nil
//	}
//
//	func (h *MyHook) BeforeOutboundRead(ctx context.Context, changes []crdt.ChangeRecord) ([]crdt.ChangeRecord, error) {
//	    // filter changes before sending to remote peers
//	    return changes, nil
//	}
//
//	plugin := crdt.New(
//	    crdt.WithNodeID("node-1"),
//	    crdt.WithSyncHook(&MyHook{}),
//	)
//
// # Streaming Transport
//
// Use [StreamingTransport] for real-time SSE-based change propagation
// alongside the periodic poll-based sync:
//
//	syncer := crdt.NewSyncer(crdtPlugin,
//	    crdt.WithTransport(crdt.NewStreamingTransport("https://cloud.example.com/sync",
//	        crdt.WithStreamTables("documents"),
//	    )),
//	    crdt.WithSyncInterval(30*time.Second), // Fallback poll interval
//	)
//
//	// Start both: SSE streaming for real-time + periodic poll as fallback
//	go syncer.Run(ctx)         // Periodic pull/push
//	go syncer.StreamSync(ctx)  // SSE real-time (if transport supports it)
//
// # Hybrid Logical Clock
//
// All CRDT operations use a Hybrid Logical Clock (HLC) that combines
// physical wall-clock time with a logical counter and node ID. This
// provides total ordering without coordination between nodes.
//
// # Storage
//
// CRDT metadata is stored in shadow tables (_<table>_crdt) alongside
// the primary table. The primary table schema is never modified.
// Shadow tables are created automatically via migrations or
// [Plugin.EnsureShadowTable].
//
// # Standalone Usage (No Forge)
//
// The crdt package works without Forge. Use [NewHTTPHandler] to create
// a standard http.Handler for sync endpoints:
//
//	handler := crdt.NewHTTPHandler(crdtPlugin)
//	mux.Handle("/sync/", handler)
//
// # Collaborative Text
//
// crdt:text fields are character-level collaborative sequences with
// formatting attributes (the YText analog): [TextState] with origin-span
// addressing, stable cursor positions ([TextRef], [TextState.RefAt],
// [TextState.IndexOf]), Quill-style deltas ([TextState.Delta]) and
// run-coalesced sequential typing. ORM writes reconcile whole strings via
// [TextState.SetString]; transports carry [TextOp] payloads.
//
// # Op Application And Compaction
//
// [ApplyChange] is the canonical ChangeRecord → FieldState fold every
// transport and downstream store uses: it honors all type-specific
// payloads (counter deltas, set/list/text ops, document path writes, and
// full-state carriers via ChangeRecord.State). Long-lived states GC their
// tombstones with the horizon-based Compact methods ([State.Compact],
// [RGAListState.Compact], [ORSetState.Compact], [TextState.Compact]).
//
// # Architecture
//
//	grove/crdt/
//	├── crdt.go          — Core types: CRDTType, State, FieldState, ChangeRecord
//	├── clock.go         — HLC implementation
//	├── register.go      — LWW-Register merge
//	├── counter.go       — PN-Counter merge
//	├── set.go           — OR-Set merge (add-wins)
//	├── list.go          — RGA list merge (cached document order)
//	├── text.go          — Text CRDT (origin-span fragments, formatting)
//	├── document.go      — Nested path-keyed document merge
//	├── merge.go         — MergeEngine: field + state merging
//	├── apply.go         — ApplyChange: canonical op application
//	├── compact.go       — Horizon-based tombstone compaction (GC)
//	├── plugin.go        — Plugin (Grove hook integration)
//	├── hooks.go         — PostMutation/PreQuery hooks for CRDT
//	├── metadata.go      — Shadow table read/write operations
//	├── sync.go          — Syncer: push/pull orchestration + StreamSync
//	├── sync_hooks.go    — SyncHook interface for intercepting sync data
//	├── transport.go     — Transport interface, HTTPClient, StreamingTransport
//	├── server.go        — SyncController + HTTPHandler (standalone)
//	├── options.go       — Functional options
//	├── inspect.go       — Debug/inspect utilities
//	└── migrations.go    — Shadow table DDL
package crdt

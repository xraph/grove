package crdt

import (
	"time"

	log "github.com/xraph/go-utils/log"
)

// Option configures the CRDT plugin.
type Option func(*Plugin)

// WithNodeID sets the unique identifier for this node. Required.
func WithNodeID(id string) Option {
	return func(p *Plugin) { p.nodeID = id }
}

// WithClock sets the clock implementation. Defaults to a HybridClock
// using the node ID.
func WithClock(c Clock) Option {
	return func(p *Plugin) { p.clock = c }
}

// WithTombstoneTTL sets how long tombstones are retained before being
// eligible for garbage collection. Defaults to 7 days.
func WithTombstoneTTL(d time.Duration) Option {
	return func(p *Plugin) { p.tombstoneTTL = d }
}

// WithMaxClockDrift sets the maximum tolerable clock drift between nodes.
// Remote HLC values that exceed this drift from the local clock are clamped.
// Defaults to 5 seconds.
func WithMaxClockDrift(d time.Duration) Option {
	return func(p *Plugin) { p.maxClockDrift = d }
}

// WithTables restricts the CRDT plugin to the specified tables.
// If not set, the plugin operates on all tables that have crdt: tags.
func WithTables(tables ...string) Option {
	return func(p *Plugin) { p.tables = tables }
}

// WithSyncHook adds a sync hook to the plugin. Sync hooks intercept
// changes during sync operations for validation, transformation, filtering,
// or auditing. Multiple hooks are called in registration order.
func WithSyncHook(hook SyncHook) Option {
	return func(p *Plugin) { p.syncHooks.Add(hook) }
}

// --- Syncer Options ---

// SyncerOption configures a Syncer.
type SyncerOption func(*Syncer)

// WithTransport sets the transport for sync communication.
func WithTransport(t Transport) SyncerOption {
	return func(s *Syncer) { s.transport = t }
}

// WithPeers adds multiple peer transports for hub-and-spoke or P2P sync.
func WithPeers(peers ...Transport) SyncerOption {
	return func(s *Syncer) { s.peers = append(s.peers, peers...) }
}

// WithSyncInterval sets the interval for background sync. Defaults to 30 seconds.
func WithSyncInterval(d time.Duration) SyncerOption {
	return func(s *Syncer) { s.interval = d }
}

// WithSyncTables restricts which tables are synced.
func WithSyncTables(tables ...string) SyncerOption {
	return func(s *Syncer) { s.tables = tables }
}

// WithGossipInterval sets the interval for P2P gossip rounds.
func WithGossipInterval(d time.Duration) SyncerOption {
	return func(s *Syncer) { s.gossipInterval = d }
}

// --- SyncController Options ---

// SyncControllerOption configures a SyncController.
type SyncControllerOption func(*SyncController)

// WithControllerSyncHook adds a sync hook to the controller. These hooks
// are called in addition to any plugin-level hooks.
func WithControllerSyncHook(hook SyncHook) SyncControllerOption {
	return func(c *SyncController) { c.hooks.Add(hook) }
}

// WithStreamPollInterval sets how frequently the SSE stream handler
// checks for new changes. Defaults to 1 second.
func WithStreamPollInterval(d time.Duration) SyncControllerOption {
	return func(c *SyncController) { c.streamPollInterval = d }
}

// WithStreamKeepAlive sets the interval for SSE keep-alive comments.
// Defaults to 15 seconds.
func WithStreamKeepAlive(d time.Duration) SyncControllerOption {
	return func(c *SyncController) { c.streamKeepAlive = d }
}

// WithPresenceEnabled enables the presence subsystem on the controller.
// When enabled, the controller creates an in-memory PresenceManager for
// ephemeral awareness data (typing indicators, cursors, user info).
// Disabled by default — zero overhead when not used.
func WithPresenceEnabled(enabled bool) SyncControllerOption {
	return func(c *SyncController) { c.presenceEnabled = enabled }
}

// WithPresenceTTL sets the TTL for presence entries. After this duration
// without a heartbeat, presence entries are automatically removed and a
// "leave" event is broadcast. Defaults to 30 seconds.
func WithPresenceTTL(d time.Duration) SyncControllerOption {
	return func(c *SyncController) { c.presenceTTL = d }
}

// --- StreamingTransport Options ---

// StreamingOption configures a StreamingTransport.
type StreamingOption func(*StreamingTransport)

// WithStreamTables restricts which tables the SSE stream subscribes to.
func WithStreamTables(tables ...string) StreamingOption {
	return func(t *StreamingTransport) { t.tables = tables }
}

// WithStreamReconnect sets the delay before reconnecting after a
// disconnection from the SSE stream. Defaults to 5 seconds.
func WithStreamReconnect(d time.Duration) StreamingOption {
	return func(t *StreamingTransport) { t.reconnectDelay = d }
}

// WithStreamLogger sets the logger for the streaming transport.
func WithStreamLogger(l log.Logger) StreamingOption {
	return func(t *StreamingTransport) { t.logger = l }
}

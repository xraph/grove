package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"
)

// Syncer orchestrates the push-pull sync protocol between nodes.
// It supports single-peer (edge-to-cloud), multi-peer (hub-and-spoke),
// and P2P topologies via the Transport interface.
//
//	syncer := crdt.NewSyncer(db, plugin,
//	    crdt.WithTransport(crdt.HTTPTransport("https://cloud.example.com/sync")),
//	    crdt.WithSyncInterval(30 * time.Second),
//	)
//	go syncer.Run(ctx)
type Syncer struct {
	plugin   *Plugin
	metadata *MetadataStore

	// Single peer transport.
	transport Transport

	// Multiple peers for hub-and-spoke / P2P.
	peers []Transport

	tables         []string
	interval       time.Duration
	gossipInterval time.Duration
	logger         log.Logger
	retryAttempts  int
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration

	// lastSync tracks the last sync HLC per peer (index-based for peers array).
	mu       sync.Mutex
	lastSync map[string]HLC // peer identifier → last synced HLC
}

// NewSyncer creates a new Syncer for the given plugin.
func NewSyncer(plugin *Plugin, opts ...SyncerOption) *Syncer {
	s := &Syncer{
		plugin:         plugin,
		metadata:       plugin.metadata,
		interval:       30 * time.Second,
		retryAttempts:  3,
		retryBaseDelay: 1 * time.Second,
		retryMaxDelay:  30 * time.Second,
		logger:         log.NewNoopLogger(),
		lastSync:       make(map[string]HLC),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Sync performs a single round of sync with all configured peers.
// For each peer: pull remote changes, merge locally, then push local changes.
func (s *Syncer) Sync(ctx context.Context) (*SyncReport, error) {
	report := &SyncReport{}

	transports := s.allTransports()
	if len(transports) == 0 {
		return report, fmt.Errorf("crdt: no transports configured; use WithTransport() or WithPeers()")
	}

	for i, t := range transports {
		peerID := fmt.Sprintf("peer-%d", i)
		r, err := s.syncWithPeerRetry(ctx, t, peerID)
		if err != nil {
			s.logger.Error("crdt: sync failed after retries",
				log.String("peer", peerID),
				log.String("error", err.Error()),
			)
			continue
		}
		report.Pulled += r.Pulled
		report.Pushed += r.Pushed
		report.Merged += r.Merged
	}

	return report, nil
}

// Run starts a background sync loop that runs until the context is cancelled.
func (s *Syncer) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.Sync(ctx); err != nil {
				s.logger.Error("crdt: background sync error", log.String("error", err.Error()))
			}
		}
	}
}

// PushChange sends a single change event to all peers. This is useful
// for CDC-driven sync where changes are pushed in real-time.
// If sync hooks are configured, BeforeOutboundChange is called before pushing.
func (s *Syncer) PushChange(ctx context.Context, table, pk, field string, crdtType CRDTType, value json.RawMessage, clock HLC) error {
	change := ChangeRecord{
		Table:    table,
		PK:       pk,
		Field:    field,
		CRDTType: crdtType,
		HLC:      clock,
		NodeID:   s.plugin.nodeID,
		Value:    value,
	}

	// Run BeforeOutboundChange hook.
	outChange := &change
	if s.plugin.syncHooks != nil {
		var err error
		outChange, err = s.plugin.syncHooks.BeforeOutboundChange(ctx, &change)
		if err != nil {
			return fmt.Errorf("crdt: outbound change hook: %w", err)
		}
		if outChange == nil {
			return nil // Hook says skip this change.
		}
	}

	req := &PushRequest{
		Changes: []ChangeRecord{*outChange},
		NodeID:  s.plugin.nodeID,
	}

	for _, t := range s.allTransports() {
		if _, err := t.Push(ctx, req); err != nil {
			return fmt.Errorf("crdt: push change: %w", err)
		}
	}
	return nil
}

// syncWithPeerRetry wraps syncWithPeer with exponential backoff retry.
func (s *Syncer) syncWithPeerRetry(ctx context.Context, t Transport, peerID string) (*SyncReport, error) {
	var lastErr error
	backoff := s.retryBaseDelay

	for attempt := 0; attempt <= s.retryAttempts; attempt++ {
		r, err := s.syncWithPeer(ctx, t, peerID)
		if err == nil {
			return r, nil
		}
		lastErr = err

		if attempt < s.retryAttempts {
			s.logger.Warn("crdt: sync attempt failed, retrying",
				log.String("peer", peerID),
				log.String("error", err.Error()),
				log.String("backoff", backoff.String()),
			)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			backoff = time.Duration(float64(backoff) * 1.5)
			if backoff > s.retryMaxDelay {
				backoff = s.retryMaxDelay
			}
		}
	}
	return nil, lastErr
}

func (s *Syncer) syncWithPeer(ctx context.Context, t Transport, peerID string) (*SyncReport, error) {
	report := &SyncReport{}

	s.mu.Lock()
	since := s.lastSync[peerID]
	s.mu.Unlock()

	// Phase 1: Pull remote changes.
	pullResp, err := t.Pull(ctx, &PullRequest{
		Tables: s.tables,
		Since:  since,
		NodeID: s.plugin.nodeID,
	})
	if err != nil {
		return nil, fmt.Errorf("pull: %w", err)
	}

	// Merge pulled changes into local state.
	for _, change := range pullResp.Changes {
		if err := s.mergeRemoteChange(ctx, change); err != nil {
			s.logger.Error("crdt: merge remote change failed",
				log.String("table", change.Table),
				log.String("pk", change.PK),
				log.String("error", err.Error()),
			)
			continue
		}
		report.Pulled++
		report.Merged++

		// Update local clock.
		s.plugin.clock.Update(change.HLC)
	}

	// Phase 2: Push local changes since last sync.
	var localChanges []ChangeRecord
	for _, table := range s.tables {
		changes, err := s.metadata.ReadChangesSince(ctx, table, since)
		if err != nil {
			return nil, fmt.Errorf("read local changes for %s: %w", table, err)
		}
		// Filter to only our own changes (don't echo back what we received).
		for _, c := range changes {
			if c.NodeID == s.plugin.nodeID {
				localChanges = append(localChanges, c)
			}
		}
	}

	// Run BeforeOutboundChange hook on each local change.
	var filteredChanges []ChangeRecord
	for _, c := range localChanges {
		if s.plugin.syncHooks != nil {
			processed, err := s.plugin.syncHooks.BeforeOutboundChange(ctx, &c)
			if err != nil {
				s.logger.Error("crdt: outbound change hook error",
					log.String("error", err.Error()),
				)
				continue
			}
			if processed == nil {
				continue // Hook says skip this change.
			}
			filteredChanges = append(filteredChanges, *processed)
		} else {
			filteredChanges = append(filteredChanges, c)
		}
	}

	if len(filteredChanges) > 0 {
		pushResp, err := t.Push(ctx, &PushRequest{
			Changes: filteredChanges,
			NodeID:  s.plugin.nodeID,
		})
		if err != nil {
			return nil, fmt.Errorf("push: %w", err)
		}
		report.Pushed = len(filteredChanges)
		_ = pushResp
	}

	// Update last sync point.
	s.mu.Lock()
	if !pullResp.LatestHLC.IsZero() {
		s.lastSync[peerID] = pullResp.LatestHLC
	}
	s.mu.Unlock()

	return report, nil
}

func (s *Syncer) mergeRemoteChange(ctx context.Context, change ChangeRecord) error {
	if s.metadata == nil {
		return fmt.Errorf("no metadata store")
	}

	// Run BeforeInboundChange sync hook.
	processedChange := &change
	if s.plugin.syncHooks != nil {
		var err error
		processedChange, err = s.plugin.syncHooks.BeforeInboundChange(ctx, &change)
		if err != nil {
			return fmt.Errorf("inbound change hook: %w", err)
		}
		if processedChange == nil {
			return nil // Hook says skip this change.
		}
	}

	if processedChange.Tombstone {
		if err := s.metadata.WriteTombstone(ctx, processedChange.Table, processedChange.PK, processedChange.HLC, processedChange.NodeID); err != nil {
			return err
		}
		// Run AfterInboundChange hook.
		if s.plugin.syncHooks != nil {
			s.plugin.syncHooks.AfterInboundChange(ctx, processedChange) //nolint:errcheck // fire-and-forget post-hook
		}
		return nil
	}

	// Read existing local state for this field.
	localState, err := s.metadata.ReadState(ctx, processedChange.Table, processedChange.PK)
	if err != nil {
		return err
	}

	remoteFS := &FieldState{
		Type:   processedChange.CRDTType,
		HLC:    processedChange.HLC,
		NodeID: processedChange.NodeID,
		Value:  processedChange.Value,
	}

	// Reconstruct type-specific state from the change record.
	if processedChange.CounterDelta != nil {
		cs := NewPNCounterState()
		cs.Increments[processedChange.NodeID] = processedChange.CounterDelta.Increment
		cs.Decrements[processedChange.NodeID] = processedChange.CounterDelta.Decrement
		remoteFS.CounterState = cs
	}

	var localFS *FieldState
	if localState != nil {
		localFS = localState.Fields[processedChange.Field]
	}

	merged, err := s.plugin.merge.MergeField(localFS, remoteFS)
	if err != nil {
		return err
	}

	if err := s.metadata.WriteFieldState(ctx, processedChange.Table, processedChange.PK, processedChange.Field, merged); err != nil {
		return err
	}

	// Run AfterInboundChange hook.
	if s.plugin.syncHooks != nil {
		s.plugin.syncHooks.AfterInboundChange(ctx, processedChange) //nolint:errcheck // fire-and-forget post-hook
	}

	return nil
}

// StreamSync connects to all peers that support SSE streaming and processes
// changes in real-time. This runs alongside the periodic poll-based Sync
// for lower latency on supported transports. Falls back gracefully if a
// transport does not support streaming.
//
// Blocks until the context is cancelled. Start it in a goroutine:
//
//	go syncer.StreamSync(ctx)
func (s *Syncer) StreamSync(ctx context.Context) error {
	transports := s.allTransports()
	if len(transports) == 0 {
		return fmt.Errorf("crdt: no transports configured")
	}

	// Track streaming goroutines.
	type streamResult struct {
		peerID string
		err    error
	}
	results := make(chan streamResult, len(transports))

	streamCount := 0
	for i, t := range transports {
		st, ok := t.(*StreamingTransport)
		if !ok {
			continue
		}
		streamCount++

		peerID := fmt.Sprintf("peer-%d", i)
		s.mu.Lock()
		since := s.lastSync[peerID]
		s.mu.Unlock()

		go func(st *StreamingTransport, pid string, since HLC) {
			err := st.StreamChanges(ctx, since, func(change ChangeRecord) {
				if err := s.mergeRemoteChange(ctx, change); err != nil {
					s.logger.Error("crdt: stream merge error",
						log.String("peer", pid),
						log.String("error", err.Error()),
					)
					return
				}

				// Update local clock and last sync.
				s.plugin.clock.Update(change.HLC)
				s.mu.Lock()
				if change.HLC.After(s.lastSync[pid]) {
					s.lastSync[pid] = change.HLC
				}
				s.mu.Unlock()
			})
			results <- streamResult{peerID: pid, err: err}
		}(st, peerID, since)
	}

	if streamCount == 0 {
		return fmt.Errorf("crdt: no streaming transports configured")
	}

	// Wait for all streams to finish (usually via context cancellation).
	for i := 0; i < streamCount; i++ {
		r := <-results
		if r.err != nil && ctx.Err() == nil {
			s.logger.Error("crdt: stream ended",
				log.String("peer", r.peerID),
				log.String("error", r.err.Error()),
			)
		}
	}

	return ctx.Err()
}

func (s *Syncer) allTransports() []Transport {
	var all []Transport
	if s.transport != nil {
		all = append(all, s.transport)
	}
	all = append(all, s.peers...)
	return all
}

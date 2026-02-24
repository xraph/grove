package kvcrdt

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/grove/crdt"
	"github.com/xraph/grove/kv"
)

// Syncer synchronizes CRDT state between two KV stores.
// It scans for CRDT keys in the primary store and merges them
// bidirectionally with the replica store.
type Syncer struct {
	primary *kv.Store
	replica *kv.Store
	cfg     *syncerConfig

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSyncer creates a new CRDT syncer between two KV stores.
func NewSyncer(primary, replica *kv.Store, opts ...SyncerOption) *Syncer {
	cfg := defaultSyncerConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return &Syncer{
		primary: primary,
		replica: replica,
		cfg:     cfg,
	}
}

// Sync performs a single round of bidirectional CRDT merge.
func (s *Syncer) Sync(ctx context.Context) (*crdt.SyncReport, error) {
	report := &crdt.SyncReport{}

	// Scan primary for CRDT keys.
	var keys []string
	err := s.primary.Scan(ctx, s.cfg.keyPattern, func(key string) error {
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kvcrdt: sync scan primary: %w", err)
	}

	for _, key := range keys {
		if err := s.syncKey(ctx, key, report); err != nil {
			return report, fmt.Errorf("kvcrdt: sync key %s: %w", key, err)
		}
	}

	// Scan replica for keys that might not be in primary.
	err = s.replica.Scan(ctx, s.cfg.keyPattern, func(key string) error {
		// Check if we already synced this key.
		for _, k := range keys {
			if k == key {
				return nil
			}
		}
		return s.syncKeyReverse(ctx, key, report)
	})
	if err != nil {
		return report, fmt.Errorf("kvcrdt: sync scan replica: %w", err)
	}

	return report, nil
}

// Start begins a background sync loop.
func (s *Syncer) Start(ctx context.Context) {
	ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.cfg.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = s.Sync(ctx)
			}
		}
	}()
}

// Stop stops the background sync loop.
func (s *Syncer) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

// syncKey merges a key from primary → replica and replica → primary.
func (s *Syncer) syncKey(ctx context.Context, key string, report *crdt.SyncReport) error {
	primaryRaw, err := s.primary.GetRaw(ctx, key)
	if err != nil && err != kv.ErrNotFound {
		return err
	}

	replicaRaw, err := s.replica.GetRaw(ctx, key)
	if err != nil && err != kv.ErrNotFound {
		return err
	}

	if primaryRaw == nil && replicaRaw == nil {
		return nil
	}

	// Try to merge as a CRDT State (map-style).
	merged, err := mergeRawStates(primaryRaw, replicaRaw)
	if err != nil {
		// If not a CRDT state, just copy from primary to replica.
		if replicaRaw == nil && primaryRaw != nil {
			report.Pushed++
			return s.replica.SetRaw(ctx, key, primaryRaw)
		}
		return nil
	}

	raw, err := json.Marshal(merged)
	if err != nil {
		return err
	}

	// Write merged state to both stores.
	if err := s.primary.SetRaw(ctx, key, raw); err != nil {
		return err
	}
	if err := s.replica.SetRaw(ctx, key, raw); err != nil {
		return err
	}

	report.Merged++
	return nil
}

func (s *Syncer) syncKeyReverse(ctx context.Context, key string, report *crdt.SyncReport) error {
	replicaRaw, err := s.replica.GetRaw(ctx, key)
	if err != nil {
		return err
	}

	primaryRaw, err := s.primary.GetRaw(ctx, key)
	if err != nil && err != kv.ErrNotFound {
		return err
	}

	if primaryRaw == nil && replicaRaw != nil {
		report.Pulled++
		return s.primary.SetRaw(ctx, key, replicaRaw)
	}

	return nil
}

// mergeRawStates attempts to unmarshal two raw byte slices as crdt.State and merge them.
func mergeRawStates(a, b []byte) (*crdt.State, error) {
	if a == nil && b == nil {
		return nil, fmt.Errorf("both states are nil")
	}

	var stateA, stateB *crdt.State

	if a != nil {
		stateA = &crdt.State{}
		if err := json.Unmarshal(a, stateA); err != nil {
			return nil, err
		}
	}

	if b != nil {
		stateB = &crdt.State{}
		if err := json.Unmarshal(b, stateB); err != nil {
			return nil, err
		}
	}

	if stateA == nil {
		return stateB, nil
	}
	if stateB == nil {
		return stateA, nil
	}

	// Merge field by field using LWW per field.
	if stateA.Fields == nil {
		stateA.Fields = make(map[string]*crdt.FieldState)
	}
	for field, fsB := range stateB.Fields {
		fsA, exists := stateA.Fields[field]
		if !exists || fsB.HLC.After(fsA.HLC) {
			stateA.Fields[field] = fsB
		}
	}

	return stateA, nil
}

package crdt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"
)

// SyncController handles CRDT sync operations. It provides handlers for
// pull, push, and streaming endpoints that can be registered with any
// router (Forge, net/http, chi, etc.).
//
// For Forge apps, use grove/extension.WithCRDT() to auto-register routes.
// For standalone use, call NewHTTPHandler() to get an http.Handler.
type SyncController struct {
	plugin             *Plugin
	metadata           *MetadataStore
	hooks              *SyncHookChain
	streamPollInterval time.Duration
	streamKeepAlive    time.Duration
	logger             log.Logger

	// Presence subsystem (nil when disabled).
	presenceEnabled    bool
	presenceTTL        time.Duration
	presenceBufferSize int
	presence           *PresenceManager
	presenceMu         sync.Mutex
	presenceSubs       map[chan PresenceEvent]struct{} // one buffered channel per stream
	presenceLegacy     chan PresenceEvent              // shared channel behind PresenceChannel

	// Time-travel configuration (nil when disabled).
	timeTravel *TimeTravelConfig

	// Room manager (nil when disabled).
	roomManager *RoomManager

	// Plugin chain for CRDT-level interceptors.
	pluginChain *PluginChain

	// Validation config (nil = no validation).
	validation *ValidationConfig

	// Metrics (nil = no metrics collection).
	metrics *Metrics
}

// NewSyncController creates a new sync controller for the given plugin.
func NewSyncController(plugin *Plugin, opts ...SyncControllerOption) *SyncController {
	c := &SyncController{
		plugin:             plugin,
		metadata:           plugin.metadata,
		hooks:              NewSyncHookChain(),
		pluginChain:        NewPluginChain(),
		streamPollInterval: 1 * time.Second,
		streamKeepAlive:    15 * time.Second,
		logger:             log.NewNoopLogger(),
	}
	// Include plugin-level sync hooks.
	if plugin.syncHooks != nil {
		for _, h := range plugin.syncHooks.hooks {
			c.hooks.Add(h)
		}
	}
	for _, opt := range opts {
		opt(c)
	}

	// Initialize presence subsystem if enabled.
	if c.presenceEnabled {
		bufSize := c.presenceBufferSize
		if bufSize <= 0 {
			bufSize = 256
		}
		c.presenceBufferSize = bufSize
		c.presenceSubs = make(map[chan PresenceEvent]struct{})
		c.presenceLegacy = make(chan PresenceEvent, bufSize)
		c.presence = NewPresenceManager(c.presenceTTL, c.broadcastPresence, c.logger)
	}

	// Initialize room manager if enabled (requires presence).
	if c.roomManager != nil && c.presence != nil {
		c.roomManager = NewRoomManager(c.presence, c.logger)
	}

	return c
}

// HandlePull processes a pull request and returns changes since the given HLC.
// This is the core logic used by both Forge and HTTP handlers.
func (c *SyncController) HandlePull(ctx context.Context, req *PullRequest) (*PullResponse, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	if c.metrics != nil {
		c.metrics.PullCount.Add(1)
	}
	pullStart := time.Now()

	allChanges, latestHLC, err := c.readChangesWindow(ctx, req.Tables, req.Since)
	if err != nil {
		return nil, err
	}

	// Update our clock with the remote node's timestamp.
	c.plugin.clock.Update(req.Since)

	// Apply selective sync filter if provided.
	if req.Filter != nil {
		allChanges = applySyncFilter(allChanges, req.Filter)
	}

	// Run BeforeOutboundRead hook.
	filtered, err := c.hooks.BeforeOutboundRead(ctx, allChanges)
	if err != nil {
		return nil, fmt.Errorf("crdt: outbound read hook: %w", err)
	}

	if c.metrics != nil {
		c.metrics.ChangesPulled.Add(int64(len(filtered)))
		c.metrics.PullLatencyNs.Store(time.Since(pullStart).Nanoseconds())
	}

	return &PullResponse{
		Changes:   filtered,
		LatestHLC: latestHLC,
	}, nil
}

// readChangesWindow reads the changes after since for every table and
// returns them with the cursor to resume from. Each table is read with its
// own page limit, so a table that fills its page still has unread rows past
// that page's last cursor position. The window is cut at the earliest such
// page end across all tables; otherwise a newer row in another table would
// move the cursor past the unread rows and the next read would skip them.
//
// The cut and the returned cursor use the rows' cursor positions, not the
// changes' HLCs. A restamped row (see mergePushed) delivers its own, older
// clock as its HLC, but it sorts at its cursor position, so resuming from
// the highest change HLC could land before rows already delivered (pulled
// again, harmless) and the cut must not drop a row whose clock is old but
// whose position is past it. Positions compare (timestamp, counter) only,
// matching the shadow-table cursor.
func (c *SyncController) readChangesWindow(ctx context.Context, tables []string, since HLC) ([]ChangeRecord, HLC, error) {
	var all []ChangeRecord
	var cursors []HLC
	var cutoff *HLC
	for _, table := range tables {
		page, err := c.metadata.readChangesPage(ctx, table, since, DefaultChangesLimit)
		if err != nil {
			return nil, HLC{}, fmt.Errorf("crdt: read changes for %s: %w", table, err)
		}
		all = append(all, page.changes...)
		cursors = append(cursors, page.cursors...)
		if page.full {
			end := page.last
			if cutoff == nil || cursorAfter(*cutoff, end) {
				cutoff = &end
			}
		}
	}

	var latest HLC
	kept := all[:0]
	for i, ch := range all {
		if cutoff != nil && cursorAfter(cursors[i], *cutoff) {
			continue
		}
		kept = append(kept, ch)
		if cursorAfter(cursors[i], latest) {
			latest = cursors[i]
		}
	}
	c.plugin.cursors.observe(latest)
	return kept, latest, nil
}

// cursorAfter reports whether a sorts after b in the shadow-table cursor
// order, which ignores the node id.
func cursorAfter(a, b HLC) bool {
	return a.Timestamp > b.Timestamp || (a.Timestamp == b.Timestamp && a.Counter > b.Counter)
}

// cursorSuccessor returns the first cursor position after h.
func cursorSuccessor(h HLC) HLC {
	if h.Counter == math.MaxUint32 {
		return HLC{Timestamp: h.Timestamp + 1, NodeID: h.NodeID}
	}
	return HLC{Timestamp: h.Timestamp, Counter: h.Counter + 1, NodeID: h.NodeID}
}

// cursorAllocator hands out the cursor positions a sync server restamps
// shadow rows with. It lives on the Plugin so every controller and Syncer
// sharing one plugin draws from the same sequence.
//
// A pull resumes from the highest cursor position it was given. A row whose
// state a push changes must therefore land past every position handed out
// before the write, or a peer that already pulled past it never sees the
// change. next guarantees that within one process: each position is past
// the plugin clock (and so past every HLC the clock has seen within its
// drift bound), past every position this process allocated or delivered,
// past the highest position stored in the row's table (rows written by an
// earlier process or another instance; see cursorBatch for when it is
// read), and past the row's own clock, so a client that resumes from the
// highest change HLC it received never jumps ahead of an unread row either.
//
// writeMu serializes allocation with the write that uses it, so positions
// commit in the order they were allocated and a pull can never see a later
// position while an earlier one is still unwritten. It also serializes the
// read-merge-write of SyncController.HandlePush and the Syncer's merge of
// pulled changes, so two of those merging into one row cannot lose each
// other's merge.
//
// What writeMu does not cover:
//
//   - Local writes. Plugin.AfterMutation reads, merges and writes the
//     server's own rows without the lock and without an allocated
//     position, so a push merging into the same row at the same moment can
//     still lose one of the two merges. The lost-update guarantee is
//     between pushes (and Syncer merges) only. Taking writeMu there would
//     deadlock any merge or metadata hook that writes a CRDT table through
//     grove, because those hooks run under it.
//   - Other instances. Two servers sharing one database each have their
//     own writeMu and allocator, so their positions can interleave out of
//     commit order, and a pull from one can pass a position the other has
//     not committed yet. Reading the table's stored maximum narrows that
//     window but does not close it.
//
// The BeforeMerge, AfterMerge and BeforeMetadataWrite plugin hooks run
// under writeMu, since they sit between the read and the write; a slow one
// stalls every push on the plugin. AfterMetadataWrite and the sync hooks
// run after it is released.
type cursorAllocator struct {
	writeMu sync.Mutex

	mu   sync.Mutex
	high HLC
}

// observe records a cursor position this process handed out to a client.
func (a *cursorAllocator) observe(h HLC) {
	a.mu.Lock()
	if cursorAfter(h, a.high) {
		a.high = h
	}
	a.mu.Unlock()
}

// next returns a fresh cursor position: now, unless that is not past high
// or one of the floors, in which case the first position after the highest
// of them.
func (a *cursorAllocator) next(now HLC, floors ...HLC) HLC {
	a.mu.Lock()
	defer a.mu.Unlock()
	pos := now
	for _, f := range append(floors, a.high) {
		if !cursorAfter(pos, f) {
			pos = cursorSuccessor(f)
			pos.NodeID = now.NodeID
		}
	}
	a.high = pos
	return pos
}

// cursorBatch allocates the cursor positions for one batch of writes: one
// push, or one page of changes a Syncer pulled. It reads each table's
// highest stored position once per batch rather than once per row. Within
// this process the allocator's high mark already covers every position it
// allocated, so after the first write the stored maximum adds nothing; it
// only matters for rows written by an earlier process (a restart) or by
// another instance, and reading it per batch keeps a restarted server past
// its old rows.
type cursorBatch struct {
	plugin *Plugin
	store  *MetadataStore
	maxes  map[string]HLC
}

// newCursorBatch starts a batch that reads stored maxima through store.
func (p *Plugin) newCursorBatch(store *MetadataStore) *cursorBatch {
	return &cursorBatch{plugin: p, store: store, maxes: make(map[string]HLC)}
}

// allocate returns the cursor position for a row of table whose new state
// carries the clock own. The caller holds plugin.cursors.writeMu until the
// row is written.
func (b *cursorBatch) allocate(ctx context.Context, table string, own HLC) (HLC, error) {
	stored, ok := b.maxes[table]
	if !ok {
		var err error
		stored, err = b.store.maxCursor(ctx, table)
		if err != nil {
			return HLC{}, err
		}
		b.maxes[table] = stored
	}
	return b.plugin.cursors.next(b.plugin.clock.Now(), own, stored), nil
}

// applySyncFilter filters changes based on selective sync criteria.
func applySyncFilter(changes []ChangeRecord, filter *SyncFilter) []ChangeRecord {
	if filter == nil {
		return changes
	}

	var pkSet map[string]bool
	if len(filter.PKFilter) > 0 {
		pkSet = make(map[string]bool, len(filter.PKFilter))
		for _, pk := range filter.PKFilter {
			pkSet[pk] = true
		}
	}

	var fieldSet map[string]bool
	if len(filter.FieldFilter) > 0 {
		fieldSet = make(map[string]bool, len(filter.FieldFilter))
		for _, f := range filter.FieldFilter {
			fieldSet[f] = true
		}
	}

	result := make([]ChangeRecord, 0, len(changes))
	for _, ch := range changes {
		if pkSet != nil && !pkSet[ch.PK] {
			continue
		}
		if fieldSet != nil && ch.Field != "" && !fieldSet[ch.Field] {
			continue
		}
		result = append(result, ch)
	}
	return result
}

// ErrPushRejected marks a push the server refused deterministically: it
// failed validation or a BeforeInboundChange hook rejected one of its
// changes. Nothing from the push was merged, and sending the same changes
// again gets the same answer, so clients should not retry it. Test for it
// with errors.Is; the HTTP handlers answer it with 422.
var ErrPushRejected = errors.New("crdt: push rejected")

// pushRejection keeps the original error text and makes errors.Is match
// ErrPushRejected as well as the underlying cause.
type pushRejection struct{ err error }

func (r *pushRejection) Error() string   { return r.err.Error() }
func (r *pushRejection) Unwrap() []error { return []error{r.err, ErrPushRejected} }

// PushErrorStatus is the HTTP status for an error from HandlePush: 422 for
// a deterministic rejection, 500 for anything else.
func PushErrorStatus(err error) int {
	if errors.Is(err, ErrPushRejected) {
		return http.StatusUnprocessableEntity
	}
	return http.StatusInternalServerError
}

// HandlePush processes a push request, merging remote changes locally.
// This is the core logic used by both Forge and HTTP handlers.
//
// Every merge that changes a row's stored state moves the row to a fresh
// cursor position, so a change stamped with a clock older than a peer's pull
// cursor still reaches that peer on its next pull. The change keeps its own
// clock, so no merge result changes on any replica; see mergePushed.
func (c *SyncController) HandlePush(ctx context.Context, req *PushRequest) (*PushResponse, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	// Validate push request.
	if c.validation != nil {
		if err := c.validation.ValidatePushRequest(req); err != nil {
			if c.metrics != nil {
				c.metrics.ValidationErrors.Add(1)
			}
			return nil, &pushRejection{err}
		}
	}

	if c.metrics != nil {
		c.metrics.PushCount.Add(1)
	}
	pushStart := time.Now()

	// Run BeforeInboundChange for the whole batch before merging anything,
	// so a hook rejection fails the push with nothing applied instead of
	// after the earlier changes already merged. A nil result skips that
	// change (it is filtered out and not counted in Merged).
	processed := make([]*ChangeRecord, 0, len(req.Changes))
	for _, change := range req.Changes {
		// Update our clock with each incoming change.
		c.plugin.clock.Update(change.HLC)

		processedChange, err := c.hooks.BeforeInboundChange(ctx, &change)
		if err != nil {
			return nil, &pushRejection{fmt.Errorf("crdt: inbound change hook: %w", err)}
		}
		if processedChange != nil {
			processed = append(processed, processedChange)
		}
	}

	merged := 0
	batch := c.plugin.newCursorBatch(c.metadata)

	for _, processedChange := range processed {
		ok, err := c.mergePushed(ctx, batch, processedChange)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		merged++

		// Run AfterInboundChange hook.
		c.hooks.AfterInboundChange(ctx, processedChange) //nolint:errcheck // fire-and-forget post-hook
	}

	if c.metrics != nil {
		c.metrics.ChangesMerged.Add(int64(merged))
		c.metrics.ChangesPushed.Add(int64(len(req.Changes)))
		c.metrics.PushLatencyNs.Store(time.Since(pushStart).Nanoseconds())
	}

	return &PushResponse{
		Merged:    merged,
		LatestHLC: c.plugin.clock.Now(),
	}, nil
}

// mergePushed merges one pushed change into the shadow table. It reports
// whether the change counts as merged: false only when a plugin skipped it.
//
// Late stamps. Pulls page by each row's cursor position, and a pushed
// change can carry a clock older than positions this server has already
// handed out (a device that was offline pushes edits it made hours ago).
// Before this restamping existed, the merged row kept the older clock as its
// position, so a peer that had pulled past that clock never received the
// merged state. Now every write that changes a row's stored state also
// moves the row to a fresh cursor position from cursorAllocator, past every
// position handed out before the merge, so the next pull of every peer
// returns the row.
//
// The restamp moves only the cursor position (the hlc_ts and hlc_counter
// columns). The state's own clock, which merges, last-writer-wins and
// tombstones resolve by, is stored and delivered unchanged: ChangeRecord.HLC
// is the same value it would have been had the peer pulled the row before
// its cursor moved past it. A replica folds that record exactly as it would
// have then, and folding pulled records does not depend on their order
// (pulls carry full states for sets, lists, text and documents, per-node
// totals for counters, and a register or delete clock otherwise), so
// redelivery changes when a peer converges, never what it converges to. In
// particular:
//
//   - An LWW value that loses leaves the stored state byte-for-byte as it
//     was, so nothing is written and the row does not move. Peers keep the
//     value they have.
//   - An LWW value that wins is written with its own clock, as before, at a
//     fresh position. If its clock is older than a peer's cursor that is what
//     makes it reach the peer; if it is newer the fresh position is past it
//     anyway. Either way the peer compares the same (clock, node) pair the
//     server compared, so it picks the same winner.
//   - A record tombstone is sticky and keeps the latest delete clock. A
//     tombstone no newer than the stored one changes nothing and does not
//     move the row; a newer one is written at a fresh position.
//   - Document paths are LWW registers inside the document's state: a path
//     write that loses leaves the document unchanged and does not move it.
//   - Counters, sets, lists and text move whenever the merge changes their
//     state, which a late op always does unless the server already has it.
//     A late counter increment usually merges into another node's row, so
//     a counter row is pulled with its full state (see rowChange); a delta
//     for the row's own node alone would drop it.
//
// A merge that leaves the state unchanged writes nothing, so a retried push
// does not move rows either.
//
// The read-merge-write runs under the plugin's cursor write lock (see
// cursorAllocator); the AfterMetadataWrite hooks run after it is released.
func (c *SyncController) mergePushed(ctx context.Context, batch *cursorBatch, processedChange *ChangeRecord) (bool, error) {
	ok, written, err := c.mergePushedLocked(ctx, batch, processedChange)
	if err != nil {
		return false, err
	}
	if written != nil {
		c.pluginChain.DispatchAfterMetadataWrite(ctx, written)
	}
	return ok, nil
}

// mergePushedLocked does mergePushed's read-merge-write under the cursor
// write lock. It returns the write event to pass to the AfterMetadataWrite
// hooks, or nil when it wrote no field state.
func (c *SyncController) mergePushedLocked(ctx context.Context, batch *cursorBatch, processedChange *ChangeRecord) (bool, *MetadataWriteEvent, error) {
	// Serialize the read-merge-write with the cursor allocation, so two
	// pushes to one field cannot lose each other's merge and positions
	// commit in allocation order.
	c.plugin.cursors.writeMu.Lock()
	defer c.plugin.cursors.writeMu.Unlock()

	// A tombstoned document-type change carrying a value is a PATH
	// delete inside the nested document, not a record delete. It
	// falls through to ApplyChange below (mirrors sync.go).
	isDocPathDelete := processedChange.CRDTType == TypeDocument && len(processedChange.Value) > 0
	if processedChange.Tombstone && !isDocPathDelete {
		// Only the tombstone rows matter here, so a field row that
		// fails to decode cannot fail the delete.
		deleted, deletedAt, err := c.metadata.readTombstone(ctx, processedChange.Table, processedChange.PK)
		if err != nil {
			return false, nil, fmt.Errorf("crdt: merge tombstone: %w", err)
		}
		if deleted && !processedChange.HLC.After(deletedAt) {
			return true, nil, nil // Already deleted at this clock or later.
		}
		cursor, err := batch.allocate(ctx, processedChange.Table, processedChange.HLC)
		if err != nil {
			return false, nil, fmt.Errorf("crdt: merge tombstone: %w", err)
		}
		if err := c.metadata.WriteTombstoneAt(ctx, processedChange.Table, processedChange.PK, processedChange.HLC, processedChange.NodeID, cursor); err != nil {
			return false, nil, fmt.Errorf("crdt: merge tombstone: %w", err)
		}
		return true, nil, nil
	}

	// Read existing local state.
	localState, err := c.metadata.ReadState(ctx, processedChange.Table, processedChange.PK)
	if err != nil {
		return false, nil, fmt.Errorf("crdt: read state: %w", err)
	}

	// The hook's remote view: the full-state carrier when present,
	// otherwise the value-level projection of the change.
	remoteFS := processedChange.State
	if remoteFS == nil {
		remoteFS = &FieldState{
			Type:   processedChange.CRDTType,
			HLC:    processedChange.HLC,
			NodeID: processedChange.NodeID,
			Value:  processedChange.Value,
		}
		if processedChange.CounterDelta != nil {
			cs := NewPNCounterState()
			cs.Increments[processedChange.NodeID] = processedChange.CounterDelta.Increment
			cs.Decrements[processedChange.NodeID] = processedChange.CounterDelta.Decrement
			remoteFS.CounterState = cs
		}
	}

	var localFS *FieldState
	if localState != nil {
		localFS = localState.Fields[processedChange.Field]
	}

	// Run BeforeMerge plugin hooks.
	mergeEv := &MergeEvent{
		Table:            processedChange.Table,
		PK:               processedChange.PK,
		Field:            processedChange.Field,
		Local:            localFS,
		Remote:           remoteFS,
		ConflictDetected: localFS != nil,
	}
	interceptedRemote, mergeErr := c.pluginChain.DispatchBeforeMerge(ctx, mergeEv)
	if mergeErr != nil {
		return false, nil, fmt.Errorf("crdt: before merge plugin: %w", mergeErr)
	}
	if interceptedRemote == nil {
		return false, nil, nil // Plugin says skip this merge.
	}

	// A plugin that REPLACED the remote view wins verbatim (state-based
	// merge of its substitute); otherwise the change applies through the
	// canonical op-application seam, honoring every typed payload
	// (counter deltas, set/list/text ops, document path writes, state
	// carriers). MergeField on the value projection would drop them.
	var mergedFS *FieldState
	if interceptedRemote != remoteFS {
		mergedFS, err = c.plugin.merge.MergeField(localFS, interceptedRemote)
	} else {
		mergedFS, err = ApplyChange(c.plugin.merge, localFS, processedChange)
	}
	if err != nil {
		return false, nil, fmt.Errorf("crdt: merge field: %w", err)
	}

	// Run AfterMerge plugin hooks.
	mergeEv.Result = mergedFS
	if mergedFS != nil {
		mergeEv.WinnerNodeID = mergedFS.NodeID
	}
	c.pluginChain.DispatchAfterMerge(ctx, mergeEv)

	// Run BeforeMetadataWrite plugin hooks.
	writeEv := &MetadataWriteEvent{
		Table:  processedChange.Table,
		PK:     processedChange.PK,
		Field:  processedChange.Field,
		State:  mergedFS,
		NodeID: processedChange.NodeID,
	}
	mergedFS, err = c.pluginChain.DispatchBeforeMetadataWrite(ctx, writeEv)
	if err != nil {
		return false, nil, fmt.Errorf("crdt: before metadata write plugin: %w", err)
	}
	if mergedFS == nil {
		return false, nil, nil // Plugin says skip this write.
	}

	if sameFieldState(localFS, mergedFS) {
		return true, nil, nil // Nothing changed: no write, no restamp.
	}

	cursor, err := batch.allocate(ctx, processedChange.Table, mergedFS.HLC)
	if err != nil {
		return false, nil, fmt.Errorf("crdt: write state: %w", err)
	}
	if err := c.metadata.WriteFieldStateAt(ctx, processedChange.Table, processedChange.PK, processedChange.Field, mergedFS, cursor); err != nil {
		return false, nil, fmt.Errorf("crdt: write state: %w", err)
	}

	writeEv.State = mergedFS
	return true, writeEv, nil
}

// sameFieldState reports whether a merge left a field's stored state as it
// was. It compares the stored encoding, clock included, so it is exact for
// an LWW register that lost (the merge returns the local register) and
// conservative elsewhere: a state that only re-encodes differently counts
// as changed and is redelivered, which is harmless.
func sameFieldState(local, merged *FieldState) bool {
	if local == nil || merged == nil {
		return false
	}
	a, err := json.Marshal(local)
	if err != nil {
		return false
	}
	b, err := json.Marshal(merged)
	if err != nil {
		return false
	}
	return bytes.Equal(a, b)
}

// StreamChangesSince returns a channel that yields new changes as they appear.
// The caller should poll or watch for changes. This is used by SSE handlers.
func (c *SyncController) StreamChangesSince(ctx context.Context, tables []string, since HLC) (<-chan []ChangeRecord, error) {
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	ch := make(chan []ChangeRecord, 16)
	go func() {
		defer close(ch)

		lastHLC := since
		ticker := time.NewTicker(c.streamPollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				allChanges, windowEnd, err := c.readChangesWindow(ctx, tables, lastHLC)
				if err != nil {
					c.logger.Error("crdt: stream read error",
						log.String("error", err.Error()),
					)
					continue
				}

				if len(allChanges) == 0 {
					continue
				}

				// Run BeforeOutboundRead hook.
				filtered, err := c.hooks.BeforeOutboundRead(ctx, allChanges)
				if err != nil {
					c.logger.Error("crdt: stream outbound hook error",
						log.String("error", err.Error()),
					)
					continue
				}

				// Resume from the window's cursor position, as a pull
				// does. The changes' HLCs are their own clocks, which a
				// restamped row keeps below its position.
				if cursorAfter(windowEnd, lastHLC) {
					lastHLC = windowEnd
				}

				if len(filtered) > 0 {
					select {
					case ch <- filtered:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return ch, nil
}

// --- Presence ---

// HandlePresenceUpdate processes a presence update and returns the resulting event.
// Returns nil if presence is not enabled.
func (c *SyncController) HandlePresenceUpdate(ctx context.Context, update *PresenceUpdate) (*PresenceEvent, error) {
	if c.presence == nil {
		return nil, fmt.Errorf("crdt: presence is not enabled")
	}
	if update.NodeID == "" {
		return nil, fmt.Errorf("crdt: presence update requires node_id")
	}
	if update.Topic == "" {
		return nil, fmt.Errorf("crdt: presence update requires topic")
	}

	// Run BeforePresenceUpdate plugin hooks.
	intercepted, presenceErr := c.pluginChain.DispatchBeforePresenceUpdate(ctx, update)
	if presenceErr != nil {
		return nil, fmt.Errorf("crdt: before presence plugin: %w", presenceErr)
	}
	if intercepted == nil {
		return nil, fmt.Errorf("crdt: presence update rejected by plugin")
	}
	update = intercepted

	// A null data payload means the client is leaving.
	if update.Data == nil || string(update.Data) == "null" {
		event := c.presence.Remove(update.Topic, update.NodeID)
		if event == nil {
			ev := PresenceEvent{
				Type:   PresenceLeave,
				NodeID: update.NodeID,
				Topic:  update.Topic,
			}
			c.pluginChain.DispatchAfterPresenceEvent(ctx, &ev)
			return &ev, nil
		}
		c.pluginChain.DispatchAfterPresenceEvent(ctx, event)
		return event, nil
	}

	event := c.presence.Update(*update)
	c.pluginChain.DispatchAfterPresenceEvent(ctx, &event)
	return &event, nil
}

// HandleGetPresence returns a snapshot of all active presence for a topic.
func (c *SyncController) HandleGetPresence(_ context.Context, topic string) (*PresenceSnapshot, error) {
	if c.presence == nil {
		return nil, fmt.Errorf("crdt: presence is not enabled")
	}
	if topic == "" {
		return nil, fmt.Errorf("crdt: presence query requires topic")
	}

	states := c.presence.Get(topic)
	if states == nil {
		states = []PresenceState{}
	}
	return &PresenceSnapshot{
		Topic:  topic,
		States: states,
	}, nil
}

// Presence returns the presence manager, or nil if presence is disabled.
func (c *SyncController) Presence() *PresenceManager {
	return c.presence
}

// Rooms returns the room manager, or nil if room management is disabled.
func (c *SyncController) Rooms() *RoomManager {
	return c.roomManager
}

// TimeTravelEnabled returns true if the time-travel feature is enabled.
func (c *SyncController) TimeTravelEnabled() bool {
	return c.timeTravel != nil && c.timeTravel.Enabled
}

// AddPlugin registers a CRDT plugin for intercepting operations.
// Plugins are called in registration order. The plugin only needs to
// implement the interceptor interfaces it cares about.
func (c *SyncController) AddPlugin(p CRDTPlugin) {
	c.pluginChain.Add(p)
}

// PluginChain returns the plugin chain for inspection or testing.
func (c *SyncController) PluginChain() *PluginChain {
	return c.pluginChain
}

// Metrics returns the metrics collector, or nil if metrics are disabled.
func (c *SyncController) Metrics() *Metrics {
	return c.metrics
}

// Logger returns the controller's logger.
func (c *SyncController) Logger() log.Logger {
	return c.logger
}

// SubscribePresence returns a channel that receives every presence event
// until ctx is done. Each call gets its own channel, so every stream sees
// every event. Returns nil if presence is disabled. The channel is never
// closed; stop reading it once ctx is done.
func (c *SyncController) SubscribePresence(ctx context.Context) <-chan PresenceEvent {
	if c.presence == nil {
		return nil
	}
	ch := c.addPresenceSub()
	go func() {
		<-ctx.Done()
		c.presenceMu.Lock()
		delete(c.presenceSubs, ch)
		c.presenceMu.Unlock()
	}()
	return ch
}

// PresenceChannel returns one channel shared by every caller, so concurrent
// readers split the events between them. Returns nil if presence is disabled.
//
// Deprecated: use SubscribePresence, which gives each stream every event.
func (c *SyncController) PresenceChannel() <-chan PresenceEvent {
	return c.presenceLegacy
}

func (c *SyncController) addPresenceSub() chan PresenceEvent {
	ch := make(chan PresenceEvent, c.presenceBufferSize)
	c.presenceMu.Lock()
	c.presenceSubs[ch] = struct{}{}
	c.presenceMu.Unlock()
	return ch
}

// broadcastPresence hands event to every subscriber without blocking; a
// subscriber whose buffer is full misses the event. The legacy shared
// channel drops silently once full, since it may have no reader at all.
func (c *SyncController) broadcastPresence(event PresenceEvent) {
	select {
	case c.presenceLegacy <- event:
	default:
	}
	c.presenceMu.Lock()
	defer c.presenceMu.Unlock()
	for ch := range c.presenceSubs {
		select {
		case ch <- event:
		default:
			c.logger.Warn("crdt: presence event dropped (channel full)",
				log.String("topic", event.Topic),
				log.String("node_id", event.NodeID),
			)
		}
	}
}

// Close cleans up the controller's resources (presence manager, etc.).
func (c *SyncController) Close() {
	if c.presence != nil {
		c.presence.Close()
	}
}

// --- HTTP Handler (backward-compatible, no Forge dependency) ---

// NewHTTPHandler creates a standard http.Handler for sync endpoints.
// Use this when not running inside a Forge app. For Forge apps, use
// grove/extension.WithCRDT() which auto-registers routes.
//
// Endpoints:
//   - POST /pull  — remote nodes pull changes from this node
//   - POST /push  — remote nodes push changes to this node
func NewHTTPHandler(plugin *Plugin, opts ...SyncControllerOption) http.Handler {
	ctrl := NewSyncController(plugin, opts...)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pull", ctrl.httpHandlePull)
	mux.HandleFunc("POST /push", ctrl.httpHandlePush)
	if ctrl.presence != nil {
		mux.HandleFunc("POST /presence", ctrl.httpHandlePresenceUpdate)
		mux.HandleFunc("GET /presence", ctrl.httpHandleGetPresence)
	}
	if ctrl.timeTravel != nil && ctrl.timeTravel.Enabled {
		mux.HandleFunc("GET /history", ctrl.httpHandleHistory)
		mux.HandleFunc("POST /history", ctrl.httpHandleHistory)
		mux.HandleFunc("GET /field-history", ctrl.httpHandleFieldHistory)
		mux.HandleFunc("POST /field-history", ctrl.httpHandleFieldHistory)
	}
	if ctrl.roomManager != nil {
		roomHandler := RoomHTTPHandler(ctrl.roomManager)
		mux.Handle("/", roomHandler)
	}
	return mux
}

func (c *SyncController) httpHandlePull(w http.ResponseWriter, r *http.Request) {
	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	resp, err := c.HandlePull(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandlePush(w http.ResponseWriter, r *http.Request) {
	var req PushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	resp, err := c.HandlePush(r.Context(), &req)
	if err != nil {
		writeError(w, PushErrorStatus(err), err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandlePresenceUpdate(w http.ResponseWriter, r *http.Request) {
	var update PresenceUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}

	event, err := c.HandlePresenceUpdate(r.Context(), &update)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(event) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandleGetPresence(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeError(w, http.StatusBadRequest, "crdt: missing topic query parameter")
		return
	}

	snapshot, err := c.HandleGetPresence(r.Context(), topic)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(snapshot) //nolint:errcheck // HTTP response write
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck // HTTP response write
}

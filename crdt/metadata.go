package crdt

import (
	"context"
	"encoding/json"
	"fmt"
)

// MetadataStore reads and writes CRDT metadata in shadow tables.
// It operates via a generic Executor interface so it works with any
// Grove driver (pg, sqlite, turso, etc.).
type MetadataStore struct {
	executor Executor
}

// Executor is the minimal query interface needed by MetadataStore.
// Both grove.DB (via driver) and grove.Tx satisfy this via adapter.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (ExecResult, error)
	QueryContext(ctx context.Context, query string, args ...any) (Rows, error)
}

// TxExecutor extends Executor with transaction support. When the underlying
// executor supports transactions, MetadataStore uses them to wrap multi-field
// writes atomically.
type TxExecutor interface {
	Executor
	BeginTx(ctx context.Context) (TxHandle, error)
}

// TxHandle represents an active transaction.
type TxHandle interface {
	Executor
	Commit() error
	Rollback() error
}

// ExecResult is the result of an exec operation.
type ExecResult interface {
	RowsAffected() (int64, error)
}

// Rows is an iterator over query results.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

// NewMetadataStore creates a new MetadataStore with the given executor.
func NewMetadataStore(exec Executor) *MetadataStore {
	return &MetadataStore{executor: exec}
}

// ShadowTableName returns the shadow table name for a given table.
func ShadowTableName(table string) string {
	return "_" + table + "_crdt"
}

// MetadataRow is a single row in the shadow table.
type MetadataRow struct {
	PKHash    string          `json:"pk_hash"`
	FieldName string          `json:"field_name"`
	HLCTS     int64           `json:"hlc_ts"`
	HLCCount  uint32          `json:"hlc_counter"`
	NodeID    string          `json:"node_id"`
	Tombstone bool            `json:"tombstone"`
	CRDTState json.RawMessage `json:"crdt_state"`
}

// WriteFieldState writes a single field's CRDT state to the shadow table,
// at the cursor position of the state's own clock. Use WriteFieldStateAt to
// place the row at a different cursor position.
func (ms *MetadataStore) WriteFieldState(ctx context.Context, table, pk, field string, fs *FieldState) error {
	return ms.WriteFieldStateAt(ctx, table, pk, field, fs, fs.HLC)
}

// WriteFieldStateAt writes a single field's CRDT state to the shadow table
// and places the row at the given cursor position.
//
// A shadow row has two clocks. The state's own clock (fs.HLC, stored inside
// crdt_state) is the field's authorship stamp: merges and last-writer-wins
// resolution use it, and pulls deliver it as ChangeRecord.HLC. The cursor
// position (the hlc_ts and hlc_counter columns) only decides which pulls
// return the row: ReadChangesSince pages by it. The two are equal unless a
// sync server restamps a row so that pulls issued before a merge see the
// merged state (see SyncController.HandlePush).
func (ms *MetadataStore) WriteFieldStateAt(ctx context.Context, table, pk, field string, fs *FieldState, cursor HLC) error {
	shadowTable := ShadowTableName(table)
	stateJSON, err := json.Marshal(fs)
	if err != nil {
		return fmt.Errorf("crdt: marshal state: %w", err)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (pk_hash, field_name, node_id)
		DO UPDATE SET hlc_ts = $3, hlc_counter = $4, tombstone = $6, crdt_state = $7`,
		shadowTable,
	)

	_, err = ms.executor.ExecContext(ctx, query,
		pk, field, cursor.Timestamp, cursor.Counter, fs.NodeID, false, stateJSON,
	)
	if err != nil {
		return fmt.Errorf("crdt: write field state: %w", err)
	}
	return nil
}

// tombstoneState is the crdt_state stored on a record tombstone row. It
// keeps the delete's own clock, so the row's cursor position can move
// without changing which writes the delete beats. Rows written before it
// existed have a NULL crdt_state and use the cursor columns instead.
type tombstoneState struct {
	HLC    HLC    `json:"hlc"`
	NodeID string `json:"node_id"`
}

// WriteTombstone marks a record as deleted in the shadow table, at the
// cursor position of the delete's own clock.
func (ms *MetadataStore) WriteTombstone(ctx context.Context, table, pk string, clock HLC, nodeID string) error {
	return ms.WriteTombstoneAt(ctx, table, pk, clock, nodeID, clock)
}

// WriteTombstoneAt marks a record as deleted in the shadow table and places
// the tombstone row at the given cursor position. The delete's own clock is
// kept in crdt_state; see WriteFieldStateAt for the two clocks.
func (ms *MetadataStore) WriteTombstoneAt(ctx context.Context, table, pk string, clock HLC, nodeID string, cursor HLC) error {
	shadowTable := ShadowTableName(table)
	stateJSON, err := json.Marshal(tombstoneState{HLC: clock, NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("crdt: marshal tombstone: %w", err)
	}

	query := fmt.Sprintf(
		`INSERT INTO %s (pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state)
		VALUES ($1, '_tombstone', $2, $3, $4, TRUE, $5)
		ON CONFLICT (pk_hash, field_name, node_id)
		DO UPDATE SET hlc_ts = $2, hlc_counter = $3, tombstone = TRUE, crdt_state = $5`,
		shadowTable,
	)

	_, err = ms.executor.ExecContext(ctx, query,
		pk, cursor.Timestamp, cursor.Counter, nodeID, stateJSON,
	)
	if err != nil {
		return fmt.Errorf("crdt: write tombstone: %w", err)
	}
	return nil
}

// rowCursor is a row's cursor position: the hlc_ts and hlc_counter columns
// ReadChangesSince pages by, with the row's node id.
func rowCursor(row *MetadataRow) HLC {
	return HLC{Timestamp: row.HLCTS, Counter: row.HLCCount, NodeID: row.NodeID}
}

// semanticHLC is a row's own clock: the timestamp and counter its stored
// state carries, with the row's node id. A state without a clock (a row
// written by an older version, or a hand-built one) falls back to the
// cursor columns, which every writer before restamping kept equal to it.
func semanticHLC(row *MetadataRow, stored HLC) HLC {
	h := rowCursor(row)
	if stored.Timestamp != 0 || stored.Counter != 0 {
		h.Timestamp = stored.Timestamp
		h.Counter = stored.Counter
	}
	return h
}

// rowTombstoneHLC is the delete clock of a record tombstone row.
func rowTombstoneHLC(row *MetadataRow) HLC {
	var ts tombstoneState
	if len(row.CRDTState) > 0 {
		_ = json.Unmarshal(row.CRDTState, &ts) //nolint:errcheck // a malformed state falls back to the cursor columns
	}
	return semanticHLC(row, ts.HLC)
}

// rowFieldState decodes a field row's stored state and stamps it with the
// row's own clock and node.
func rowFieldState(row *MetadataRow) (FieldState, error) {
	var fs FieldState
	if row.CRDTState != nil {
		if err := json.Unmarshal(row.CRDTState, &fs); err != nil {
			return fs, err
		}
	}
	fs.HLC = semanticHLC(row, fs.HLC)
	fs.NodeID = row.NodeID
	return fs, nil
}

// maxCursor returns the highest cursor position stored in a table's shadow
// table, or the zero HLC when it is empty.
func (ms *MetadataStore) maxCursor(ctx context.Context, table string) (HLC, error) {
	query := fmt.Sprintf(
		`SELECT hlc_ts, hlc_counter FROM %s ORDER BY hlc_ts DESC, hlc_counter DESC LIMIT 1`,
		ShadowTableName(table),
	)
	rows, err := ms.executor.QueryContext(ctx, query)
	if err != nil {
		return HLC{}, fmt.Errorf("crdt: read max cursor: %w", err)
	}
	defer rows.Close()

	var h HLC
	if rows.Next() {
		if err := rows.Scan(&h.Timestamp, &h.Counter); err != nil {
			return HLC{}, fmt.Errorf("crdt: scan max cursor: %w", err)
		}
	}
	return h, rows.Err()
}

// ReadState reads the full CRDT state for a record from the shadow table.
func (ms *MetadataStore) ReadState(ctx context.Context, table, pk string) (*State, error) {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`SELECT pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state
		FROM %s WHERE pk_hash = $1`,
		shadowTable,
	)

	rows, err := ms.executor.QueryContext(ctx, query, pk)
	if err != nil {
		return nil, fmt.Errorf("crdt: read state: %w", err)
	}
	defer rows.Close()

	state := NewState(table, pk)

	for rows.Next() {
		var row MetadataRow
		if err := rows.Scan(
			&row.PKHash, &row.FieldName, &row.HLCTS, &row.HLCCount,
			&row.NodeID, &row.Tombstone, &row.CRDTState,
		); err != nil {
			return nil, fmt.Errorf("crdt: scan row: %w", err)
		}

		if row.FieldName == "_tombstone" && row.Tombstone {
			// Tombstones are sticky and keep the latest delete clock
			// across every node's tombstone row.
			at := rowTombstoneHLC(&row)
			if !state.Tombstone || at.After(state.TombstoneHLC) {
				state.Tombstone = true
				state.TombstoneHLC = at
			}
			continue
		}

		fs, err := rowFieldState(&row)
		if err != nil {
			return nil, fmt.Errorf("crdt: unmarshal field state for %s: %w", row.FieldName, err)
		}

		// Merge with existing field state (multiple nodes may have entries).
		if existing, ok := state.Fields[row.FieldName]; ok {
			engine := NewMergeEngine()
			merged, err := engine.MergeField(existing, &fs)
			if err != nil {
				return nil, fmt.Errorf("crdt: merge field %s: %w", row.FieldName, err)
			}
			state.Fields[row.FieldName] = merged
		} else {
			fsCopy := fs
			state.Fields[row.FieldName] = &fsCopy
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("crdt: iterate rows: %w", err)
	}

	return state, nil
}

// DefaultChangesLimit is the maximum number of change records returned by
// ReadChangesSince when no explicit limit is provided. This prevents
// unbounded result sets on large shadow tables.
const DefaultChangesLimit = 10000

// ReadChangesSince reads change records from the shadow table whose cursor
// position is after since. Used by the sync protocol.
// An optional limit can be provided (first value used); 0 means use
// DefaultChangesLimit. Each shadow row yields one change.
//
// Each change carries the row's own clock (the state's authorship stamp) as
// its HLC, not the cursor position. The two differ for a row a sync server
// restamped, so a caller paging through a table resumes from the cursor
// positions, as SyncController does, not from the HLCs of the changes.
func (ms *MetadataStore) ReadChangesSince(ctx context.Context, table string, since HLC, limits ...int) ([]ChangeRecord, error) {
	limit := DefaultChangesLimit
	if len(limits) > 0 && limits[0] > 0 {
		limit = limits[0]
	}
	page, err := ms.readChangesPage(ctx, table, since, limit)
	if err != nil {
		return nil, err
	}
	return page.changes, nil
}

// changePage is one page of a table's changes after a cursor.
type changePage struct {
	// changes are the page's change records in cursor order, one per row.
	changes []ChangeRecord
	// cursors holds the cursor position of each change's row, parallel to
	// changes.
	cursors []HLC
	// full is true when the page holds limit rows, so rows past last may
	// still be unread.
	full bool
	// last is the cursor position of the page's last row.
	last HLC
}

// readChangesPage reads up to limit shadow rows whose cursor position is
// after since, in cursor order, and turns them into change records.
func (ms *MetadataStore) readChangesPage(ctx context.Context, table string, since HLC, limit int) (changePage, error) {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`SELECT pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state
		FROM %s
		WHERE hlc_ts > $1 OR (hlc_ts = $1 AND hlc_counter > $2)
		ORDER BY hlc_ts, hlc_counter
		LIMIT $3`,
		shadowTable,
	)

	rows, err := ms.executor.QueryContext(ctx, query, since.Timestamp, since.Counter, limit)
	if err != nil {
		return changePage{}, fmt.Errorf("crdt: read changes: %w", err)
	}
	defer rows.Close()

	var page changePage
	for rows.Next() {
		var row MetadataRow
		if err := rows.Scan(
			&row.PKHash, &row.FieldName, &row.HLCTS, &row.HLCCount,
			&row.NodeID, &row.Tombstone, &row.CRDTState,
		); err != nil {
			return changePage{}, fmt.Errorf("crdt: scan change: %w", err)
		}
		cursor := rowCursor(&row)
		page.last = cursor
		page.changes = append(page.changes, rowChange(table, &row))
		page.cursors = append(page.cursors, cursor)
	}
	if err := rows.Err(); err != nil {
		return changePage{}, err
	}
	page.full = len(page.changes) >= limit
	return page, nil
}

// rowChange turns one shadow row into the change record a pull delivers.
func rowChange(table string, row *MetadataRow) ChangeRecord {
	cr := ChangeRecord{
		Table:     table,
		PK:        row.PKHash,
		Field:     row.FieldName,
		HLC:       rowCursor(row),
		NodeID:    row.NodeID,
		Tombstone: row.Tombstone,
	}

	if row.Tombstone {
		cr.HLC = rowTombstoneHLC(row)
		return cr
	}
	if row.CRDTState == nil {
		return cr
	}

	var fs FieldState
	if err := json.Unmarshal(row.CRDTState, &fs); err != nil {
		return cr
	}
	cr.HLC = semanticHLC(row, fs.HLC)
	cr.CRDTType = fs.Type
	cr.Value = fs.Value
	switch fs.Type {
	case TypeCounter:
		// A merged counter row holds every node's totals, but a counter
		// delta carries only one node's, so a delta alone would drop the
		// other nodes' increments: a late increment usually merges into
		// another node's row. The full state carries them all in one
		// record, and every client merges a state carrier before it looks
		// at the delta. The row's own node keeps its delta for clients
		// that predate state carriers, which then see only that node's
		// totals, as they did before.
		state := fs
		cr.State = &state
		cr.CounterDelta = extractCounterDelta(fs.CounterState, row.NodeID)
	case TypeSet, TypeList, TypeDocument, TypeText:
		// State-based propagation: ops can't reconstruct these
		// losslessly from a resolved value (set removes were
		// previously dropped entirely), so carry the full state.
		state := fs
		cr.State = &state
	}
	return cr
}

// WriteFieldStatesAtomic writes multiple field states in a single transaction
// when the executor supports it. Falls back to individual writes otherwise.
func (ms *MetadataStore) WriteFieldStatesAtomic(ctx context.Context, table, pk string, fields map[string]*FieldState) error {
	if len(fields) <= 1 {
		// Single field — no transaction needed.
		for field, fs := range fields {
			if err := ms.WriteFieldState(ctx, table, pk, field, fs); err != nil {
				return err
			}
		}
		return nil
	}

	txExec, ok := ms.executor.(TxExecutor)
	if !ok {
		// Executor doesn't support transactions — fall back to individual writes.
		for field, fs := range fields {
			if err := ms.WriteFieldState(ctx, table, pk, field, fs); err != nil {
				return err
			}
		}
		return nil
	}

	tx, err := txExec.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("crdt: begin tx: %w", err)
	}

	txStore := &MetadataStore{executor: tx}
	for field, fs := range fields {
		if err := txStore.WriteFieldState(ctx, table, pk, field, fs); err != nil {
			rErr := tx.Rollback()
			if rErr != nil {
				return rErr
			}
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("crdt: commit field states: %w", err)
	}
	return nil
}

// CleanTombstones removes tombstones older than the given HLC.
func (ms *MetadataStore) CleanTombstones(ctx context.Context, table string, olderThan int64) error {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`DELETE FROM %s WHERE tombstone = TRUE AND hlc_ts < $1`,
		shadowTable,
	)

	_, err := ms.executor.ExecContext(ctx, query, olderThan)
	if err != nil {
		return fmt.Errorf("crdt: clean tombstones: %w", err)
	}
	return nil
}

func extractCounterDelta(cs *PNCounterState, nodeID string) *CounterDelta {
	if cs == nil {
		return nil
	}
	return &CounterDelta{
		Increment: cs.Increments[nodeID],
		Decrement: cs.Decrements[nodeID],
	}
}

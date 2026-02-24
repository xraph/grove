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

// WriteFieldState writes a single field's CRDT state to the shadow table.
func (ms *MetadataStore) WriteFieldState(ctx context.Context, table, pk, field string, fs *FieldState) error {
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
		pk, field, fs.HLC.Timestamp, fs.HLC.Counter, fs.NodeID, false, stateJSON,
	)
	if err != nil {
		return fmt.Errorf("crdt: write field state: %w", err)
	}
	return nil
}

// WriteTombstone marks a record as deleted in the shadow table.
func (ms *MetadataStore) WriteTombstone(ctx context.Context, table, pk string, clock HLC, nodeID string) error {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`INSERT INTO %s (pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state)
		VALUES ($1, '_tombstone', $2, $3, $4, TRUE, NULL)
		ON CONFLICT (pk_hash, field_name, node_id)
		DO UPDATE SET hlc_ts = $2, hlc_counter = $3, tombstone = TRUE`,
		shadowTable,
	)

	_, err := ms.executor.ExecContext(ctx, query,
		pk, clock.Timestamp, clock.Counter, nodeID,
	)
	if err != nil {
		return fmt.Errorf("crdt: write tombstone: %w", err)
	}
	return nil
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
			state.Tombstone = true
			state.TombstoneHLC = HLC{
				Timestamp: row.HLCTS,
				Counter:   row.HLCCount,
				NodeID:    row.NodeID,
			}
			continue
		}

		var fs FieldState
		if row.CRDTState != nil {
			if err := json.Unmarshal(row.CRDTState, &fs); err != nil {
				return nil, fmt.Errorf("crdt: unmarshal field state for %s: %w", row.FieldName, err)
			}
		}
		fs.HLC = HLC{
			Timestamp: row.HLCTS,
			Counter:   row.HLCCount,
			NodeID:    row.NodeID,
		}
		fs.NodeID = row.NodeID

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

// ReadChangesSince reads all change records from the shadow table that
// happened after the given HLC timestamp. Used by the sync protocol.
func (ms *MetadataStore) ReadChangesSince(ctx context.Context, table string, since HLC) ([]ChangeRecord, error) {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`SELECT pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state
		FROM %s
		WHERE hlc_ts > $1 OR (hlc_ts = $1 AND hlc_counter > $2)
		ORDER BY hlc_ts, hlc_counter`,
		shadowTable,
	)

	rows, err := ms.executor.QueryContext(ctx, query, since.Timestamp, since.Counter)
	if err != nil {
		return nil, fmt.Errorf("crdt: read changes: %w", err)
	}
	defer rows.Close()

	var changes []ChangeRecord
	for rows.Next() {
		var row MetadataRow
		if err := rows.Scan(
			&row.PKHash, &row.FieldName, &row.HLCTS, &row.HLCCount,
			&row.NodeID, &row.Tombstone, &row.CRDTState,
		); err != nil {
			return nil, fmt.Errorf("crdt: scan change: %w", err)
		}

		cr := ChangeRecord{
			Table: table,
			PK:    row.PKHash,
			Field: row.FieldName,
			HLC: HLC{
				Timestamp: row.HLCTS,
				Counter:   row.HLCCount,
				NodeID:    row.NodeID,
			},
			NodeID:    row.NodeID,
			Tombstone: row.Tombstone,
		}

		if row.CRDTState != nil {
			var fs FieldState
			if err := json.Unmarshal(row.CRDTState, &fs); err == nil {
				cr.CRDTType = fs.Type
				cr.Value = fs.Value
				cr.CounterDelta = extractCounterDelta(fs.CounterState, row.NodeID)
				cr.SetOp = extractSetOp(&fs)
			}
		}

		changes = append(changes, cr)
	}

	return changes, rows.Err()
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

func extractSetOp(fs *FieldState) *SetOperation {
	if fs.SetState == nil {
		return nil
	}
	elements := fs.SetState.Elements()
	if len(elements) == 0 {
		return nil
	}
	raw, err := json.Marshal(elements)
	if err != nil {
		return nil
	}
	return &SetOperation{
		Op:       SetOpAdd,
		Elements: raw,
	}
}

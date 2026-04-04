package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// TimeTravelConfig controls the opt-in time-travel feature.
// When enabled, the sync server exposes a /history endpoint for
// querying historical state at any point in time via HLC.
type TimeTravelConfig struct {
	// Enabled controls whether the time-travel endpoints are registered.
	// Defaults to false (disabled).
	Enabled bool

	// MaxHistoryDepth limits how many historical versions to return.
	// 0 means unlimited. Defaults to 100.
	MaxHistoryDepth int
}

// HistoryRequest asks for the state of a record at a specific point in time.
type HistoryRequest struct {
	Table string `json:"table"`
	PK    string `json:"pk"`
	// AtHLC is the point in time to query. If zero, returns current state.
	AtHLC HLC `json:"at_hlc,omitempty"`
}

// HistoryResponse contains the historical state of a record.
type HistoryResponse struct {
	Table string `json:"table"`
	PK    string `json:"pk"`
	AtHLC HLC    `json:"at_hlc"`
	State *State `json:"state"`
}

// FieldHistoryRequest asks for the change history of a specific field.
type FieldHistoryRequest struct {
	Table    string `json:"table"`
	PK       string `json:"pk"`
	Field    string `json:"field"`
	SinceHLC HLC    `json:"since_hlc,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

// FieldHistoryEntry is a single version of a field's state.
type FieldHistoryEntry struct {
	HLC    HLC             `json:"hlc"`
	NodeID string          `json:"node_id"`
	Value  json.RawMessage `json:"value,omitempty"`
	Type   CRDTType        `json:"type"`
}

// FieldHistoryResponse contains the change history for a field.
type FieldHistoryResponse struct {
	Table   string              `json:"table"`
	PK      string              `json:"pk"`
	Field   string              `json:"field"`
	Entries []FieldHistoryEntry `json:"entries"`
}

// ReadStateAt reads the CRDT state for a record as it existed at a specific HLC timestamp.
// This queries the shadow table for all field states with hlc_ts <= the target time.
func (ms *MetadataStore) ReadStateAt(ctx context.Context, table, pk string, at HLC) (*State, error) {
	shadowTable := ShadowTableName(table)

	query := fmt.Sprintf(
		`SELECT pk_hash, field_name, hlc_ts, hlc_counter, node_id, tombstone, crdt_state
		FROM %s
		WHERE pk_hash = $1 AND (hlc_ts < $2 OR (hlc_ts = $2 AND hlc_counter <= $3))
		ORDER BY hlc_ts DESC, hlc_counter DESC`,
		shadowTable,
	)

	rows, err := ms.executor.QueryContext(ctx, query, pk, at.Timestamp, at.Counter)
	if err != nil {
		return nil, fmt.Errorf("crdt: read state at %s: %w", at, err)
	}
	defer rows.Close()

	state := NewState(table, pk)
	engine := NewMergeEngine()

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
				continue
			}
		}
		fs.HLC = HLC{
			Timestamp: row.HLCTS,
			Counter:   row.HLCCount,
			NodeID:    row.NodeID,
		}
		fs.NodeID = row.NodeID

		if existing, ok := state.Fields[row.FieldName]; ok {
			merged, mergeErr := engine.MergeField(existing, &fs)
			if mergeErr == nil {
				state.Fields[row.FieldName] = merged
			}
		} else {
			fsCopy := fs
			state.Fields[row.FieldName] = &fsCopy
		}
	}

	return state, rows.Err()
}

// ReadFieldHistory reads the change history for a specific field.
func (ms *MetadataStore) ReadFieldHistory(ctx context.Context, table, pk, field string, since HLC, limit int) ([]FieldHistoryEntry, error) {
	shadowTable := ShadowTableName(table)

	if limit <= 0 {
		limit = 100
	}

	query := fmt.Sprintf(
		`SELECT hlc_ts, hlc_counter, node_id, crdt_state
		FROM %s
		WHERE pk_hash = $1 AND field_name = $2 AND (hlc_ts > $3 OR (hlc_ts = $3 AND hlc_counter > $4))
		ORDER BY hlc_ts DESC, hlc_counter DESC
		LIMIT $5`,
		shadowTable,
	)

	rows, err := ms.executor.QueryContext(ctx, query, pk, field, since.Timestamp, since.Counter, limit)
	if err != nil {
		return nil, fmt.Errorf("crdt: read field history: %w", err)
	}
	defer rows.Close()

	var entries []FieldHistoryEntry
	for rows.Next() {
		var hlcTS int64
		var hlcCount uint32
		var nodeID string
		var stateJSON json.RawMessage

		if err := rows.Scan(&hlcTS, &hlcCount, &nodeID, &stateJSON); err != nil {
			return nil, fmt.Errorf("crdt: scan history: %w", err)
		}

		entry := FieldHistoryEntry{
			HLC:    HLC{Timestamp: hlcTS, Counter: hlcCount, NodeID: nodeID},
			NodeID: nodeID,
		}

		if stateJSON != nil {
			var fs FieldState
			if err := json.Unmarshal(stateJSON, &fs); err == nil {
				entry.Value = fs.Value
				entry.Type = fs.Type
			}
		}

		entries = append(entries, entry)
	}

	return entries, rows.Err()
}

// --- Time-Travel Option ---

// WithTimeTravelEnabled enables the opt-in time-travel feature.
// When enabled, the sync server exposes /history and /field-history endpoints.
func WithTimeTravelEnabled(enabled bool) SyncControllerOption {
	return func(c *SyncController) {
		if c.timeTravel == nil {
			c.timeTravel = &TimeTravelConfig{}
		}
		c.timeTravel.Enabled = enabled
	}
}

// WithTimeTravelMaxDepth sets the maximum number of history entries returned.
func WithTimeTravelMaxDepth(depth int) SyncControllerOption {
	return func(c *SyncController) {
		if c.timeTravel == nil {
			c.timeTravel = &TimeTravelConfig{}
		}
		c.timeTravel.MaxHistoryDepth = depth
	}
}

// --- HTTP Handlers for Time-Travel ---

// HandleHistory returns the state of a record at a specific point in time.
func (c *SyncController) HandleHistory(ctx context.Context, req *HistoryRequest) (*HistoryResponse, error) {
	if c.timeTravel == nil || !c.timeTravel.Enabled {
		return nil, fmt.Errorf("crdt: time-travel is not enabled")
	}
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	// Run BeforeHistoryRead plugin hooks.
	if err := c.pluginChain.DispatchBeforeHistoryRead(ctx, req.Table, req.PK, req.AtHLC); err != nil {
		return nil, fmt.Errorf("crdt: history read denied: %w", err)
	}

	var state *State
	var err error

	if req.AtHLC.IsZero() {
		state, err = c.metadata.ReadState(ctx, req.Table, req.PK)
	} else {
		state, err = c.metadata.ReadStateAt(ctx, req.Table, req.PK, req.AtHLC)
	}
	if err != nil {
		return nil, fmt.Errorf("crdt: read historical state: %w", err)
	}

	// Run AfterHistoryRead plugin hooks (for redaction/transformation).
	state, err = c.pluginChain.DispatchAfterHistoryRead(ctx, req.Table, req.PK, state)
	if err != nil {
		return nil, fmt.Errorf("crdt: history read plugin: %w", err)
	}

	return &HistoryResponse{
		Table: req.Table,
		PK:    req.PK,
		AtHLC: req.AtHLC,
		State: state,
	}, nil
}

// HandleFieldHistory returns the change history of a specific field.
func (c *SyncController) HandleFieldHistory(ctx context.Context, req *FieldHistoryRequest) (*FieldHistoryResponse, error) {
	if c.timeTravel == nil || !c.timeTravel.Enabled {
		return nil, fmt.Errorf("crdt: time-travel is not enabled")
	}
	if c.metadata == nil {
		return nil, fmt.Errorf("crdt: metadata store not initialized")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if c.timeTravel.MaxHistoryDepth > 0 && limit > c.timeTravel.MaxHistoryDepth {
		limit = c.timeTravel.MaxHistoryDepth
	}

	entries, err := c.metadata.ReadFieldHistory(ctx, req.Table, req.PK, req.Field, req.SinceHLC, limit)
	if err != nil {
		return nil, fmt.Errorf("crdt: read field history: %w", err)
	}

	return &FieldHistoryResponse{
		Table:   req.Table,
		PK:      req.PK,
		Field:   req.Field,
		Entries: entries,
	}, nil
}

func (c *SyncController) httpHandleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		// GET with query params.
		table := r.URL.Query().Get("table")
		pk := r.URL.Query().Get("pk")
		if table == "" || pk == "" {
			writeError(w, http.StatusBadRequest, "crdt: table and pk are required")
			return
		}
		req := &HistoryRequest{Table: table, PK: pk}

		// Parse optional at_hlc.
		if tsStr := r.URL.Query().Get("at_ts"); tsStr != "" {
			ts, err := strconv.ParseInt(tsStr, 10, 64)
			if err == nil {
				req.AtHLC.Timestamp = ts
			}
		}
		if cStr := r.URL.Query().Get("at_c"); cStr != "" {
			c, err := strconv.ParseUint(cStr, 10, 32)
			if err == nil {
				req.AtHLC.Counter = uint32(c)
			}
		}
		if node := r.URL.Query().Get("at_node"); node != "" {
			req.AtHLC.NodeID = node
		}

		resp, err := c.HandleHistory(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
		return
	}

	// POST with JSON body.
	var req HistoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}
	resp, err := c.HandleHistory(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

func (c *SyncController) httpHandleFieldHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		table := r.URL.Query().Get("table")
		pk := r.URL.Query().Get("pk")
		field := r.URL.Query().Get("field")
		if table == "" || pk == "" || field == "" {
			writeError(w, http.StatusBadRequest, "crdt: table, pk, and field are required")
			return
		}
		req := &FieldHistoryRequest{Table: table, PK: pk, Field: field}
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			l, err := strconv.Atoi(limitStr)
			if err == nil {
				req.Limit = l
			}
		}

		resp, err := c.HandleFieldHistory(r.Context(), req)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
		return
	}

	var req FieldHistoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("crdt: invalid request: %v", err))
		return
	}
	resp, err := c.HandleFieldHistory(r.Context(), &req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck // HTTP response write
}

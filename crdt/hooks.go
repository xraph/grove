package crdt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xraph/grove/hook"
)

// Ensure Plugin implements the hook interfaces at compile time.
var (
	_ hook.PostMutationHook = (*Plugin)(nil)
	_ hook.PreQueryHook     = (*Plugin)(nil)
)

// AfterMutation is called after every INSERT, UPDATE, or DELETE on a
// CRDT-enabled table. It writes CRDT metadata to the shadow table.
func (p *Plugin) AfterMutation(ctx context.Context, qc *hook.QueryContext, data any, _ any) error {
	if p.metadata == nil {
		return nil
	}

	// Determine which fields have CRDT tags by checking PrivacyColumns
	// (reusing the existing column metadata mechanism).
	crdtFields := extractCRDTFields(qc)
	if len(crdtFields) == 0 {
		return nil
	}

	pk := extractPK(qc)
	if pk == "" {
		return nil
	}

	clock := p.clock.Now()

	switch qc.Operation {
	case hook.OpInsert, hook.OpUpdate:
		return p.writeFieldStates(ctx, qc.Table, pk, crdtFields, data, clock)
	case hook.OpDelete:
		return p.metadata.WriteTombstone(ctx, qc.Table, pk, clock, p.nodeID)
	}

	return nil
}

// BeforeQuery is a no-op by default. In merge-on-read mode it could
// inject filters, but the default merge-on-write approach doesn't need it.
func (p *Plugin) BeforeQuery(_ context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// writeFieldStates writes CRDT metadata for each CRDT-tagged field.
func (p *Plugin) writeFieldStates(ctx context.Context, table, pk string, fields map[string]CRDTType, data any, clock HLC) error {
	values := extractFieldValues(data)

	for fieldName, crdtType := range fields {
		value, hasValue := values[fieldName]

		var fs *FieldState
		switch crdtType {
		case TypeLWW:
			raw, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("crdt: marshal %s: %w", fieldName, err)
			}
			fs = &FieldState{
				Type:   TypeLWW,
				HLC:    clock,
				NodeID: p.nodeID,
				Value:  raw,
			}

		case TypeCounter:
			// For counters, read existing state and apply delta.
			existing, err := p.metadata.ReadState(ctx, table, pk)
			if err != nil {
				return fmt.Errorf("crdt: read counter state: %w", err)
			}

			counter := NewPNCounterState()
			if existing != nil {
				if existingFS, ok := existing.Fields[fieldName]; ok {
					counter = CounterFromFieldState(existingFS)
				}
			}

			if hasValue {
				delta := toInt64(value)
				currentValue := counter.Value()
				diff := delta - currentValue
				if diff > 0 {
					counter.Increment(p.nodeID, diff)
				} else if diff < 0 {
					counter.Decrement(p.nodeID, -diff)
				}
			}

			fs = counter.ToFieldState(clock, p.nodeID)

		case TypeSet:
			// For sets, read existing state and reconcile.
			existing, err := p.metadata.ReadState(ctx, table, pk)
			if err != nil {
				return fmt.Errorf("crdt: read set state: %w", err)
			}

			set := NewORSetState()
			if existing != nil {
				if existingFS, ok := existing.Fields[fieldName]; ok {
					set = SetFromFieldState(existingFS)
				}
			}

			if hasValue {
				// Add new elements from the current value.
				elements := toSlice(value)
				for _, elem := range elements {
					_ = set.Add(elem, p.nodeID, clock)
				}
			}

			fs = set.ToFieldState(clock, p.nodeID)
		}

		if fs != nil {
			if err := p.metadata.WriteFieldState(ctx, table, pk, fieldName, fs); err != nil {
				return err
			}
		}
	}

	return nil
}

// extractCRDTFields extracts field names and their CRDT types from the
// query context. It uses the Values map where CRDT configuration is stored.
func extractCRDTFields(qc *hook.QueryContext) map[string]CRDTType {
	fields := make(map[string]CRDTType)
	if qc.Values == nil {
		return fields
	}

	if crdtMap, ok := qc.Values["_crdt_fields"]; ok {
		if m, ok := crdtMap.(map[string]string); ok {
			for name, typ := range m {
				if ValidCRDTType(typ) {
					fields[name] = CRDTType(typ)
				}
			}
		}
	}

	return fields
}

// extractPK extracts the primary key value from the query context.
func extractPK(qc *hook.QueryContext) string {
	if qc.Values != nil {
		if pk, ok := qc.Values["_crdt_pk"]; ok {
			return fmt.Sprintf("%v", pk)
		}
	}
	return ""
}

// extractFieldValues extracts field values from the mutation data.
func extractFieldValues(data any) map[string]any {
	values := make(map[string]any)
	if data == nil {
		return values
	}

	// Try to extract from a map.
	if m, ok := data.(map[string]any); ok {
		return m
	}

	// For struct types, use JSON round-trip as fallback.
	raw, err := json.Marshal(data)
	if err != nil {
		return values
	}
	_ = json.Unmarshal(raw, &values)
	return values
}

// toInt64 converts a value to int64.
func toInt64(v any) int64 {
	switch val := v.(type) {
	case int:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case json.Number:
		n, _ := val.Int64()
		return n
	default:
		return 0
	}
}

// toSlice converts a value to a slice of any.
func toSlice(v any) []any {
	switch val := v.(type) {
	case []any:
		return val
	case []string:
		result := make([]any, len(val))
		for i, s := range val {
			result[i] = s
		}
		return result
	default:
		return nil
	}
}

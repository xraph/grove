package crdt

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/xraph/grove/hook"
)

// Ensure Plugin implements the hook interfaces at compile time.
var (
	_ hook.PreMutationHook  = (*Plugin)(nil)
	_ hook.PostMutationHook = (*Plugin)(nil)
	_ hook.PreQueryHook     = (*Plugin)(nil)
)

// BeforeMutation refuses expression-based writes whose resulting values are
// unavailable to the CRDT hook. Model writes carry their schema and values.
func (p *Plugin) BeforeMutation(_ context.Context, qc *hook.QueryContext, _ any) (*hook.HookResult, error) {
	if qc.RawMutation {
		model := *qc
		model.Columns = nil
		if len(extractCRDTFields(&model)) > 0 {
			return nil, fmt.Errorf("crdt: expression mutations require explicit CRDT operations or model values")
		}
	}
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// AfterMutation is called after every INSERT, UPDATE, or DELETE on a
// CRDT-enabled table. It writes CRDT metadata to the shadow table.
func (p *Plugin) AfterMutation(ctx context.Context, qc *hook.QueryContext, data, result any) error {
	if p.metadata == nil {
		return nil
	}
	if affected, ok := result.(interface{ RowsAffected() (int64, error) }); ok {
		if count, err := affected.RowsAffected(); err == nil && count == 0 {
			return nil
		}
	}
	value := modelValue(reflect.ValueOf(data))
	if value.IsValid() && (value.Kind() == reflect.Slice || value.Kind() == reflect.Array) {
		for i := 0; i < value.Len(); i++ {
			if err := p.AfterMutation(ctx, qc, value.Index(i).Interface(), nil); err != nil {
				return err
			}
		}
		return nil
	}
	local := *qc
	if local.ModelType == nil && value.IsValid() {
		local.ModelType = value.Type()
	}
	fields := extractCRDTFields(&local)
	if len(fields) == 0 {
		return nil
	}
	pk, err := extractMutationPK(&local, data, result)
	if err != nil {
		return err
	}
	ctx, unlock := p.cursors.lock(ctx)
	defer unlock()
	clock, err := p.newCursorBatch(p.metadata).allocate(ctx, local.Table, HLC{})
	if err != nil {
		return fmt.Errorf("crdt: allocate mutation clock: %w", err)
	}
	switch local.Operation {
	case hook.OpInsert, hook.OpUpdate:
		return p.writeFieldStates(ctx, local.Table, pk, fields, data, clock)
	case hook.OpDelete:
		return p.metadata.WriteTombstoneAt(ctx, local.Table, pk, clock, p.nodeID, clock)
	}
	return nil
}

// BeforeQuery is a no-op by default. In merge-on-read mode it could
// inject filters, but the default merge-on-write approach doesn't need it.
func (p *Plugin) BeforeQuery(_ context.Context, _ *hook.QueryContext) (*hook.HookResult, error) {
	return &hook.HookResult{Decision: hook.Allow}, nil
}

// writeFieldStates writes CRDT metadata for each CRDT-tagged field.
// When multiple fields are written, they are committed atomically if the
// underlying executor supports transactions.
func (p *Plugin) writeFieldStates(ctx context.Context, table, pk string, fields map[string]CRDTType, data any, clock HLC) error {
	values := extractFieldValues(data)

	// Build all field states first, then write atomically.
	pending := make(map[string]*FieldState, len(fields))

	for fieldName, crdtType := range fields {
		value, hasValue := values[fieldName]
		if !hasValue {
			continue
		}
		if (crdtType == TypeList || crdtType == TypeSet) && value != nil {
			v := modelValue(reflect.ValueOf(value))
			if v.IsValid() && v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
				return fmt.Errorf("crdt: %s field %s requires a slice value", crdtType, fieldName)
			}
		}

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
				delta, err := toInt64(value)
				if err != nil {
					return fmt.Errorf("crdt: counter field %s: %w", fieldName, err)
				}
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
				elements := toSlice(value)
				desired := make(map[string]bool, len(elements))
				for _, elem := range elements {
					key, err := marshalElement(elem)
					if err != nil {
						return fmt.Errorf("crdt: encode set element: %w", err)
					}
					desired[key] = true
				}
				for _, elem := range set.Elements() {
					if !desired[string(elem)] {
						if err := set.Remove(elem); err != nil {
							return fmt.Errorf("crdt: remove set element: %w", err)
						}
					}
				}
				for _, elem := range elements {
					if err := set.Add(elem, p.nodeID, clock); err != nil {
						return fmt.Errorf("crdt: add set element: %w", err)
					}
				}
			}

			fs = set.ToFieldState(clock, p.nodeID)

		case TypeList:
			existing, err := p.metadata.ReadState(ctx, table, pk)
			if err != nil {
				return fmt.Errorf("crdt: read list state: %w", err)
			}
			list := NewRGAListState()
			if existing != nil {
				list = ListFromFieldState(existing.Fields[fieldName])
			}
			if list == nil {
				list = NewRGAListState()
			}
			elements := toSlice(value)
			ids := list.NodeIDs()
			// Preserve a common prefix so unchanged items retain their identities.
			keep := 0
			for keep < len(ids) && keep < len(elements) {
				raw, err := json.Marshal(elements[keep])
				if err != nil {
					return fmt.Errorf("crdt: encode list element: %w", err)
				}
				if string(list.Nodes[rgaNodeKey(ids[keep])].Value) != string(raw) {
					break
				}
				keep++
			}
			for _, id := range ids[keep:] {
				list.Delete(id)
			}
			parent := HLC{}
			if keep > 0 {
				parent = ids[keep-1]
			}
			at := clock
			for _, elem := range elements[keep:] {
				at = cursorSuccessor(at)
				if err := list.Insert(elem, parent, p.nodeID, at); err != nil {
					return fmt.Errorf("crdt: insert list element: %w", err)
				}
				parent = at
			}
			fs = list.ToFieldState(at, p.nodeID)
		case TypeDocument:
			raw, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("crdt: encode document: %w", err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				return fmt.Errorf("crdt: document requires an object: %w", err)
			}
			doc := NewDocumentCRDTState()
			doc.ResetHLC = &clock
			for path, v := range fields {
				if err := doc.SetField(path, v, clock, p.nodeID); err != nil {
					return fmt.Errorf("crdt: set document path: %w", err)
				}
			}
			fs = doc.ToFieldState(clock, p.nodeID)

		case TypeText:
			// For text, read existing state and reconcile the whole-string
			// write into minimal insert/delete ops (prefix/suffix diff).
			existing, err := p.metadata.ReadState(ctx, table, pk)
			if err != nil {
				return fmt.Errorf("crdt: read text state: %w", err)
			}

			txt := NewTextState()
			if existing != nil {
				if existingFS, ok := existing.Fields[fieldName]; ok {
					if reconstructed := TextFromFieldState(existingFS); reconstructed != nil {
						txt = reconstructed
					}
				}
			}

			if hasValue {
				// Only reconcile actual strings: a failed assertion would
				// yield "" and SetString("") tombstones the whole document —
				// a destructive wipe that would replicate as valid ops.
				if s, ok := value.(string); ok {
					if _, err := txt.SetString(s, p.nodeID, clock); err != nil {
						return fmt.Errorf("crdt: reconcile text %s: %w", fieldName, err)
					}
				} else {
					return fmt.Errorf("crdt: text field %s requires a string value, got %T", fieldName, value)
				}
			}

			fs = txt.ToFieldState(clock, p.nodeID)
		}

		if fs != nil {
			pending[fieldName] = fs
		}
	}

	if len(pending) == 0 {
		return nil
	}
	own := clock
	for _, fs := range pending {
		if fs.HLC.After(own) {
			own = fs.HLC
		}
	}
	names := make([]string, 0, len(pending))
	for name := range pending {
		names = append(names, name)
	}
	sort.Strings(names)
	cursors := make(map[string]HLC, len(pending))
	batch := p.newCursorBatch(p.metadata)
	for _, name := range names {
		cursor, err := batch.allocate(ctx, table, own)
		if err != nil {
			return fmt.Errorf("crdt: allocate field cursor: %w", err)
		}
		cursors[name] = cursor
	}
	return p.metadata.writeFieldStatesAtomic(ctx, table, pk, pending, cursors)
}

// toInt64 accepts integer model values and lossless JSON numbers.
func toInt64(value any) (int64, error) {
	if number, ok := value.(json.Number); ok {
		return number.Int64()
	}
	v := modelValue(reflect.ValueOf(value))
	if v.IsValid() {
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return v.Int(), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if n := v.Uint(); n <= math.MaxInt64 {
				return int64(n), nil
			}
		case reflect.Float32, reflect.Float64:
			n := v.Float()
			if n >= math.MinInt64 && n < float64(math.MaxInt64) && math.Trunc(n) == n {
				return int64(n), nil
			}
		}
	}
	return 0, fmt.Errorf("crdt: counter requires an integer in the int64 range, got %v", value)
}

// toSlice converts a value to a slice of any.
func toSlice(v any) []any {
	value := modelValue(reflect.ValueOf(v))
	if !value.IsValid() || (value.Kind() != reflect.Slice && value.Kind() != reflect.Array) {
		return nil
	}
	result := make([]any, value.Len())
	for i := range result {
		result[i] = value.Index(i).Interface()
	}
	return result
}

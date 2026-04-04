package crdt

import (
	"encoding/json"
	"fmt"
	"strings"
)

// InspectResult is a human-readable representation of a record's CRDT state.
// Useful for debugging and testing.
type InspectResult struct {
	Table     string                   `json:"table"`
	PK        string                   `json:"pk"`
	Tombstone bool                     `json:"tombstone"`
	Fields    map[string]*InspectField `json:"fields"`
}

// InspectField is a human-readable representation of a single field's state.
type InspectField struct {
	Type   CRDTType `json:"type"`
	NodeID string   `json:"node_id"`
	HLC    HLC      `json:"hlc"`

	// LWW fields
	Value any `json:"value,omitempty"`

	// Counter fields
	CounterValue int64               `json:"counter_value,omitempty"`
	NodeCounters map[string][2]int64 `json:"node_counters,omitempty"` // nodeID → [inc, dec]

	// Set fields
	Elements []any `json:"elements,omitempty"`

	// List fields
	ListLength int `json:"list_length,omitempty"`

	// Document fields
	DocPaths []string `json:"doc_paths,omitempty"`
}

// InspectState converts a State into a human-readable InspectResult.
func InspectState(state *State) *InspectResult {
	if state == nil {
		return nil
	}

	result := &InspectResult{
		Table:     state.Table,
		PK:        state.PK,
		Tombstone: state.Tombstone,
		Fields:    make(map[string]*InspectField),
	}

	for name, fs := range state.Fields {
		field := &InspectField{
			Type:   fs.Type,
			NodeID: fs.NodeID,
			HLC:    fs.HLC,
		}

		switch fs.Type {
		case TypeLWW:
			if fs.Value != nil {
				var v any
				json.Unmarshal(fs.Value, &v) //nolint:errcheck // best-effort display value
				field.Value = v
			}

		case TypeCounter:
			cs := CounterFromFieldState(fs)
			field.CounterValue = cs.Value()
			field.NodeCounters = make(map[string][2]int64)
			for node, inc := range cs.Increments {
				field.NodeCounters[node] = [2]int64{inc, cs.Decrements[node]}
			}
			for node, dec := range cs.Decrements {
				if _, ok := field.NodeCounters[node]; !ok {
					field.NodeCounters[node] = [2]int64{0, dec}
				}
			}

		case TypeSet:
			ss := SetFromFieldState(fs)
			for _, elem := range ss.Elements() {
				var v any
				json.Unmarshal(elem, &v) //nolint:errcheck // best-effort display value // best-effort display value
				field.Elements = append(field.Elements, v)
			}

		case TypeList:
			ls := ListFromFieldState(fs)
			if ls != nil {
				field.ListLength = ls.Len()
				for _, elem := range ls.Elements() {
					var v any
					json.Unmarshal(elem, &v) //nolint:errcheck // best-effort display value
					field.Elements = append(field.Elements, v)
				}
			}

		case TypeDocument:
			ds := DocumentFromFieldState(fs)
			if ds != nil {
				field.DocPaths = ds.Paths()
				field.Value = ds.Resolve()
			}
		}

		result.Fields[name] = field
	}

	return result
}

// String returns a human-readable summary of the inspect result.
func (r *InspectResult) String() string {
	if r == nil {
		return "<nil>"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "CRDT State for %s[%s]", r.Table, r.PK)
	if r.Tombstone {
		b.WriteString(" [TOMBSTONED]")
	}
	b.WriteString("\n")

	for name, field := range r.Fields {
		fmt.Fprintf(&b, "  %s (%s): ", name, field.Type)
		switch field.Type {
		case TypeLWW:
			fmt.Fprintf(&b, "%v (node=%s, hlc=%s)", field.Value, field.NodeID, field.HLC)
		case TypeCounter:
			fmt.Fprintf(&b, "%d", field.CounterValue)
			for node, counts := range field.NodeCounters {
				fmt.Fprintf(&b, " [%s: +%d -%d]", node, counts[0], counts[1])
			}
		case TypeSet:
			fmt.Fprintf(&b, "%v", field.Elements)
		case TypeList:
			fmt.Fprintf(&b, "[%d items] %v", field.ListLength, field.Elements)
		case TypeDocument:
			fmt.Fprintf(&b, "{%d paths} %v", len(field.DocPaths), field.Value)
		}
		b.WriteString("\n")
	}

	return b.String()
}

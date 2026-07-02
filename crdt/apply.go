package crdt

import (
	"encoding/json"
	"fmt"
)

// ApplyChange folds one ChangeRecord into a field's local state and returns
// the merged result. It is the canonical op-application seam: transports,
// stores and downstream systems (e.g. fabriq's document plane) fold update
// logs through it so every replica applies an op identically.
//
// Application is type-aware:
//
//   - a change carrying State (a full FieldState) merges state-based;
//   - counter changes fold CounterDelta as that node's delta snapshot;
//   - set changes fold SetOp (adds tag entries; removes mark observed tags —
//     exact when the op names Tags, otherwise every local tag older than the
//     op's HLC, the legacy observed-remove approximation);
//   - list changes fold ListOp (insert/delete/move); deletes for unseen node
//     ids are kept as tombstones so late-arriving inserts stay deleted;
//   - text changes fold TextOp (insert/delete/format);
//   - lww and document changes fold Value (document values are {path, value}
//     path writes, or path deletes when the record is tombstoned).
//
// local may be nil (first op for the field). The change is never mutated.
func ApplyChange(engine *MergeEngine, local *FieldState, c *ChangeRecord) (*FieldState, error) {
	if c == nil {
		return nil, fmt.Errorf("crdt: apply nil change")
	}
	if engine == nil {
		engine = NewMergeEngine()
	}
	if local != nil && local.Type != c.CRDTType {
		return nil, fmt.Errorf("crdt: cannot apply %s change onto %s field", c.CRDTType, local.Type)
	}

	// Full-state carrier: pure state-based merge.
	if c.State != nil {
		if c.State.Type != c.CRDTType {
			return nil, fmt.Errorf("crdt: change type %s carries %s state", c.CRDTType, c.State.Type)
		}
		return engine.MergeField(local, c.State)
	}

	switch c.CRDTType {
	case TypeLWW:
		remote := &FieldState{Type: TypeLWW, HLC: c.HLC, NodeID: c.NodeID, Value: c.Value}
		return engine.MergeField(local, remote)

	case TypeCounter:
		if c.CounterDelta == nil {
			return nil, fmt.Errorf("crdt: counter change missing counter_delta")
		}
		cs := NewPNCounterState()
		cs.Increments[c.NodeID] = c.CounterDelta.Increment
		cs.Decrements[c.NodeID] = c.CounterDelta.Decrement
		remote := &FieldState{Type: TypeCounter, HLC: c.HLC, NodeID: c.NodeID, CounterState: cs}
		return engine.MergeField(local, remote)

	case TypeSet:
		if c.SetOp == nil {
			return nil, fmt.Errorf("crdt: set change missing set_op")
		}
		remote, err := setOpState(local, c)
		if err != nil {
			return nil, err
		}
		return engine.MergeField(local, remote)

	case TypeList:
		if c.ListOp == nil {
			return nil, fmt.Errorf("crdt: list change missing list_op")
		}
		remote, err := listOpState(c)
		if err != nil {
			return nil, err
		}
		return engine.MergeField(local, remote)

	case TypeText:
		if c.TextOp == nil {
			return nil, fmt.Errorf("crdt: text change missing text_op")
		}
		txt := TextFromFieldState(local)
		if txt == nil {
			txt = NewTextState()
		}
		if err := txt.Apply(c.TextOp, c.NodeID, c.HLC); err != nil {
			return nil, err
		}
		return txt.ToFieldState(pickNewer(local, c)), nil

	case TypeDocument:
		return applyDocumentChange(engine, local, c)

	default:
		return nil, fmt.Errorf("crdt: apply unknown type: %s", c.CRDTType)
	}
}

// setOpState builds the one-op remote OR-Set state for a set change.
func setOpState(local *FieldState, c *ChangeRecord) (*FieldState, error) {
	var elements []json.RawMessage
	if len(c.SetOp.Elements) > 0 {
		if err := json.Unmarshal(c.SetOp.Elements, &elements); err != nil {
			return nil, fmt.Errorf("crdt: set_op elements: %w", err)
		}
	}

	remote := NewORSetState()
	switch c.SetOp.Op {
	case SetOpAdd:
		tag := Tag{NodeID: c.NodeID, HLC: c.HLC}
		for _, el := range elements {
			remote.Entries[string(el)] = []Tag{tag}
		}

	case SetOpRemove:
		if len(c.SetOp.Tags) > 0 {
			// Exact observed-remove: the op names the tags it saw.
			for _, tag := range c.SetOp.Tags {
				remote.Removed[tagKey(tag)] = true
			}
		} else {
			// Legacy remove (no tags on the wire): remove every local tag
			// for these elements that is older than the remove itself.
			// Concurrent-or-newer adds survive (add-wins).
			localSet := SetFromFieldState(local)
			if localSet != nil {
				for _, el := range elements {
					for _, tag := range localSet.Entries[string(el)] {
						if c.HLC.After(tag.HLC) {
							remote.Removed[tagKey(tag)] = true
						}
					}
				}
			}
		}

	default:
		return nil, fmt.Errorf("crdt: unknown set op %q", c.SetOp.Op)
	}
	return remote.ToFieldState(c.HLC, c.NodeID), nil
}

// listOpState builds the one-op remote RGA state for a list change.
func listOpState(c *ChangeRecord) (*FieldState, error) {
	remote := NewRGAListState()
	switch c.ListOp.Op {
	case ListOpInsert:
		id := c.ListOp.NodeID
		if id.IsZero() {
			id = c.HLC
		}
		remote.Nodes[rgaNodeKey(id)] = &RGANode{
			ID:       id,
			NodeID:   c.NodeID,
			ParentID: c.ListOp.ParentID,
			Value:    c.ListOp.Value,
		}

	case ListOpDelete:
		// Keep the tombstone even when the insert hasn't arrived yet:
		// MergeList's tombstone-OR makes the late insert stay deleted.
		remote.Nodes[rgaNodeKey(c.ListOp.NodeID)] = &RGANode{
			ID:        c.ListOp.NodeID,
			NodeID:    c.NodeID,
			Tombstone: true,
		}

	case ListOpMove:
		// Move = tombstone the old id + re-insert the value under the new
		// parent with the op's HLC as the new id.
		remote.Nodes[rgaNodeKey(c.ListOp.NodeID)] = &RGANode{
			ID:        c.ListOp.NodeID,
			NodeID:    c.NodeID,
			Tombstone: true,
		}
		remote.Nodes[rgaNodeKey(c.HLC)] = &RGANode{
			ID:       c.HLC,
			NodeID:   c.NodeID,
			ParentID: c.ListOp.ParentID,
			Value:    c.ListOp.Value,
		}

	default:
		return nil, fmt.Errorf("crdt: unknown list op %q", c.ListOp.Op)
	}
	return remote.ToFieldState(c.HLC, c.NodeID), nil
}

// documentPathOp is the {path, value} payload document changes carry.
type documentPathOp struct {
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

// applyDocumentChange folds a document path write or path delete.
func applyDocumentChange(engine *MergeEngine, local *FieldState, c *ChangeRecord) (*FieldState, error) {
	if len(c.Value) == 0 {
		return nil, fmt.Errorf("crdt: document change missing value")
	}
	var op documentPathOp
	if err := json.Unmarshal(c.Value, &op); err != nil {
		return nil, fmt.Errorf("crdt: document change value: %w", err)
	}
	if op.Path == "" {
		return nil, fmt.Errorf("crdt: document change missing path")
	}

	doc := DocumentFromFieldState(local)
	if doc == nil {
		doc = NewDocumentCRDTState()
	}

	if c.Tombstone {
		// Path delete, LWW-guarded: only delete what the op could have
		// observed (mirrors crdt-js applyDocumentFieldDelete).
		if existing := doc.GetField(op.Path); existing != nil && c.HLC.After(existing.HLC) {
			doc.DeleteField(op.Path)
		}
		return doc.ToFieldState(pickNewer(local, c)), nil
	}

	remote := NewDocumentCRDTState()
	remote.SetFieldState(op.Path, &FieldState{
		Type:   TypeLWW,
		HLC:    c.HLC,
		NodeID: c.NodeID,
		Value:  op.Value,
	})
	merged, err := MergeDocument(doc, remote)
	if err != nil {
		return nil, fmt.Errorf("crdt: apply document path %q: %w", op.Path, err)
	}
	_ = engine // document merge is path-wise; engine kept for symmetry
	return merged.ToFieldState(pickNewer(local, c)), nil
}

// pickNewer returns the HLC/node pair of whichever of local state or the
// incoming change is newer — the merged field's authorship stamp.
func pickNewer(local *FieldState, c *ChangeRecord) (HLC, string) {
	if local != nil && local.HLC.After(c.HLC) {
		return local.HLC, local.NodeID
	}
	return c.HLC, c.NodeID
}

package crdt

import "fmt"

// MergeEngine resolves concurrent writes by dispatching to the appropriate
// CRDT merge function based on field type.
type MergeEngine struct{}

// NewMergeEngine creates a new MergeEngine.
func NewMergeEngine() *MergeEngine {
	return &MergeEngine{}
}

// MergeField merges a single field's state from two sources.
// Both states must have the same CRDT type.
func (m *MergeEngine) MergeField(local, remote *FieldState) (*FieldState, error) {
	if local == nil {
		return remote, nil
	}
	if remote == nil {
		return local, nil
	}
	if local.Type != remote.Type {
		return nil, fmt.Errorf("crdt: cannot merge different types: %s vs %s", local.Type, remote.Type)
	}

	switch local.Type {
	case TypeLWW:
		localReg := LWWFromFieldState(local)
		remoteReg := LWWFromFieldState(remote)
		winner := MergeLWW(localReg, remoteReg)
		return winner.ToFieldState(), nil

	case TypeCounter:
		localCounter := CounterFromFieldState(local)
		remoteCounter := CounterFromFieldState(remote)
		merged := MergeCounter(localCounter, remoteCounter)
		// Use the higher HLC for the merged state.
		clock := local.HLC
		nodeID := local.NodeID
		if remote.HLC.After(local.HLC) {
			clock = remote.HLC
			nodeID = remote.NodeID
		}
		return merged.ToFieldState(clock, nodeID), nil

	case TypeSet:
		localSet := SetFromFieldState(local)
		remoteSet := SetFromFieldState(remote)
		merged := MergeSet(localSet, remoteSet)
		clock := local.HLC
		nodeID := local.NodeID
		if remote.HLC.After(local.HLC) {
			clock = remote.HLC
			nodeID = remote.NodeID
		}
		return merged.ToFieldState(clock, nodeID), nil

	default:
		return nil, fmt.Errorf("crdt: unknown type: %s", local.Type)
	}
}

// MergeState merges two full record states. Fields present in only one state
// are kept as-is. Fields present in both are merged using MergeField.
// Tombstones are resolved by taking the one with the higher HLC.
func (m *MergeEngine) MergeState(local, remote *State) (*State, error) {
	if local == nil {
		return remote, nil
	}
	if remote == nil {
		return local, nil
	}

	merged := NewState(local.Table, local.PK)

	// Collect all field names.
	allFields := make(map[string]bool)
	for name := range local.Fields {
		allFields[name] = true
	}
	for name := range remote.Fields {
		allFields[name] = true
	}

	for name := range allFields {
		localField := local.Fields[name]
		remoteField := remote.Fields[name]

		result, err := m.MergeField(localField, remoteField)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", name, err)
		}
		merged.Fields[name] = result
	}

	// Merge tombstone: higher HLC wins.
	switch {
	case local.Tombstone && remote.Tombstone:
		merged.Tombstone = true
		if remote.TombstoneHLC.After(local.TombstoneHLC) {
			merged.TombstoneHLC = remote.TombstoneHLC
		} else {
			merged.TombstoneHLC = local.TombstoneHLC
		}
	case remote.Tombstone:
		// Remote tombstone vs local non-tombstone:
		// Tombstone wins only if it's newer than all local field writes.
		latestLocal := latestHLC(local)
		if remote.TombstoneHLC.After(latestLocal) {
			merged.Tombstone = true
			merged.TombstoneHLC = remote.TombstoneHLC
		}
	case local.Tombstone:
		latestRemote := latestHLC(remote)
		if local.TombstoneHLC.After(latestRemote) {
			merged.Tombstone = true
			merged.TombstoneHLC = local.TombstoneHLC
		}
	}

	return merged, nil
}

// latestHLC returns the highest HLC across all fields in a state.
func latestHLC(s *State) HLC {
	var latest HLC
	for _, fs := range s.Fields {
		if fs.HLC.After(latest) {
			latest = fs.HLC
		}
	}
	return latest
}

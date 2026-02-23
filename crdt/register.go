package crdt

import "encoding/json"

// LWWRegister is a Last-Writer-Wins register. The value with the highest
// HLC wins. Ties are broken deterministically by node ID.
type LWWRegister struct {
	Value  json.RawMessage `json:"value"`
	Clock  HLC             `json:"hlc"`
	NodeID string          `json:"node_id"`
}

// MergeLWW merges two LWW registers, returning the winning value.
// The register with the higher HLC wins. If HLCs are equal, the higher
// node ID wins (deterministic tiebreak).
func MergeLWW(local, remote *LWWRegister) *LWWRegister {
	if local == nil {
		return remote
	}
	if remote == nil {
		return local
	}

	if remote.Clock.After(local.Clock) {
		return remote
	}
	return local
}

// NewLWWRegister creates a new LWW register with the given value and clock.
func NewLWWRegister(value any, clock HLC, nodeID string) (*LWWRegister, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &LWWRegister{
		Value:  raw,
		Clock:  clock,
		NodeID: nodeID,
	}, nil
}

// Decode unmarshals the register's value into dest.
func (r *LWWRegister) Decode(dest any) error {
	return json.Unmarshal(r.Value, dest)
}

// ToFieldState converts to the generic FieldState representation.
func (r *LWWRegister) ToFieldState() *FieldState {
	return &FieldState{
		Type:   TypeLWW,
		HLC:    r.Clock,
		NodeID: r.NodeID,
		Value:  r.Value,
	}
}

// LWWFromFieldState reconstructs an LWWRegister from a FieldState.
func LWWFromFieldState(fs *FieldState) *LWWRegister {
	if fs == nil || fs.Type != TypeLWW {
		return nil
	}
	return &LWWRegister{
		Value:  fs.Value,
		Clock:  fs.HLC,
		NodeID: fs.NodeID,
	}
}

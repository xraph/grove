package crdt

// PNCounterState holds the per-node increment and decrement maps for a
// PN-Counter (Positive-Negative Counter). The global value is:
//
//	sum(all increments) - sum(all decrements)
//
// Each node only ever writes to its own entry; merging takes the max of
// each node's counters.
type PNCounterState struct {
	// Increments maps nodeID → total increments from that node.
	Increments map[string]int64 `json:"inc"`

	// Decrements maps nodeID → total decrements from that node.
	Decrements map[string]int64 `json:"dec"`
}

// NewPNCounterState creates an empty PN-Counter state.
func NewPNCounterState() *PNCounterState {
	return &PNCounterState{
		Increments: make(map[string]int64),
		Decrements: make(map[string]int64),
	}
}

// Value returns the current counter value (sum of increments minus sum of decrements).
func (c *PNCounterState) Value() int64 {
	var inc, dec int64
	for _, v := range c.Increments {
		inc += v
	}
	for _, v := range c.Decrements {
		dec += v
	}
	return inc - dec
}

// Increment adds delta to the given node's increment counter.
func (c *PNCounterState) Increment(nodeID string, delta int64) {
	if delta <= 0 {
		return
	}
	c.Increments[nodeID] += delta
}

// Decrement adds delta to the given node's decrement counter.
func (c *PNCounterState) Decrement(nodeID string, delta int64) {
	if delta <= 0 {
		return
	}
	c.Decrements[nodeID] += delta
}

// MergeCounter merges two PN-Counter states by taking the max of each
// node's increments and decrements. This is commutative, associative,
// and idempotent.
func MergeCounter(local, remote *PNCounterState) *PNCounterState {
	if local == nil {
		return remote
	}
	if remote == nil {
		return local
	}

	merged := NewPNCounterState()

	// Merge increments: take max per node.
	for node, v := range local.Increments {
		merged.Increments[node] = v
	}
	for node, v := range remote.Increments {
		if existing, ok := merged.Increments[node]; !ok || v > existing {
			merged.Increments[node] = v
		}
	}

	// Merge decrements: take max per node.
	for node, v := range local.Decrements {
		merged.Decrements[node] = v
	}
	for node, v := range remote.Decrements {
		if existing, ok := merged.Decrements[node]; !ok || v > existing {
			merged.Decrements[node] = v
		}
	}

	return merged
}

// ToFieldState converts to the generic FieldState representation.
func (c *PNCounterState) ToFieldState(clock HLC, nodeID string) *FieldState {
	return &FieldState{
		Type:         TypeCounter,
		HLC:          clock,
		NodeID:       nodeID,
		CounterState: c,
	}
}

// CounterFromFieldState reconstructs a PNCounterState from a FieldState.
func CounterFromFieldState(fs *FieldState) *PNCounterState {
	if fs == nil || fs.Type != TypeCounter {
		return nil
	}
	if fs.CounterState == nil {
		return NewPNCounterState()
	}
	return fs.CounterState
}

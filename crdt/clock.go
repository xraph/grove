package crdt

import (
	"fmt"
	"sync"
	"time"
)

// HLC is a Hybrid Logical Clock value. It combines a physical timestamp
// (wall clock) with a logical counter to provide a totally ordered, causally
// consistent clock that requires no coordination between nodes.
type HLC struct {
	// Timestamp is the physical time in nanoseconds since Unix epoch.
	Timestamp int64 `json:"ts"`

	// Counter is the logical counter, incremented when the physical clock
	// hasn't advanced since the last event.
	Counter uint32 `json:"c"`

	// NodeID identifies the node that produced this clock value.
	NodeID string `json:"node"`
}

// IsZero returns true if the HLC has not been set.
func (h HLC) IsZero() bool {
	return h.Timestamp == 0 && h.Counter == 0 && h.NodeID == ""
}

// Compare returns -1 if h < other, 0 if equal, 1 if h > other.
// Ordering: Timestamp first, then Counter, then NodeID (lexicographic tiebreak).
func (h HLC) Compare(other HLC) int {
	if h.Timestamp < other.Timestamp {
		return -1
	}
	if h.Timestamp > other.Timestamp {
		return 1
	}
	if h.Counter < other.Counter {
		return -1
	}
	if h.Counter > other.Counter {
		return 1
	}
	if h.NodeID < other.NodeID {
		return -1
	}
	if h.NodeID > other.NodeID {
		return 1
	}
	return 0
}

// After returns true if h is strictly after other.
func (h HLC) After(other HLC) bool {
	return h.Compare(other) > 0
}

func (h HLC) String() string {
	return fmt.Sprintf("HLC{ts:%d c:%d node:%s}", h.Timestamp, h.Counter, h.NodeID)
}

// Clock is the interface for generating HLC values.
type Clock interface {
	// Now returns a new HLC value that is causally after all previously
	// observed values.
	Now() HLC

	// Update merges a received remote HLC into the local clock state,
	// ensuring the next Now() is causally after both the local and remote values.
	Update(remote HLC)
}

// HybridClock is the default Clock implementation using a Hybrid Logical Clock.
type HybridClock struct {
	mu       sync.Mutex
	nodeID   string
	last     HLC
	maxDrift time.Duration
	nowFn    func() time.Time // injectable for testing
}

// NewHybridClock creates a new HLC for the given node.
func NewHybridClock(nodeID string, opts ...ClockOption) *HybridClock {
	c := &HybridClock{
		nodeID:   nodeID,
		maxDrift: 5 * time.Second,
		nowFn:    time.Now,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClockOption configures a HybridClock.
type ClockOption func(*HybridClock)

// WithMaxDrift sets the maximum tolerable clock drift. Update() will return
// values clamped to this drift from the physical clock.
func WithMaxDrift(d time.Duration) ClockOption {
	return func(c *HybridClock) { c.maxDrift = d }
}

// WithNowFunc overrides the wall clock source (useful for testing).
func WithNowFunc(fn func() time.Time) ClockOption {
	return func(c *HybridClock) { c.nowFn = fn }
}

// Now produces a new HLC that is causally after the last observed value.
func (c *HybridClock) Now() HLC {
	c.mu.Lock()
	defer c.mu.Unlock()

	physicalNow := c.nowFn().UnixNano()

	if physicalNow > c.last.Timestamp {
		c.last = HLC{
			Timestamp: physicalNow,
			Counter:   0,
			NodeID:    c.nodeID,
		}
	} else {
		c.last = HLC{
			Timestamp: c.last.Timestamp,
			Counter:   c.last.Counter + 1,
			NodeID:    c.nodeID,
		}
	}
	return c.last
}

// Update merges a remote HLC into the local state. The next call to Now()
// will return a value that is causally after both local and remote.
func (c *HybridClock) Update(remote HLC) {
	c.mu.Lock()
	defer c.mu.Unlock()

	physicalNow := c.nowFn().UnixNano()

	// Clamp remote timestamp to prevent runaway clocks.
	maxAllowed := physicalNow + c.maxDrift.Nanoseconds()
	remoteTS := remote.Timestamp
	if remoteTS > maxAllowed {
		remoteTS = maxAllowed
	}

	switch {
	case physicalNow > c.last.Timestamp && physicalNow > remoteTS:
		c.last = HLC{
			Timestamp: physicalNow,
			Counter:   0,
			NodeID:    c.nodeID,
		}
	case c.last.Timestamp == remoteTS:
		counter := c.last.Counter
		if remote.Counter > counter {
			counter = remote.Counter
		}
		c.last = HLC{
			Timestamp: c.last.Timestamp,
			Counter:   counter + 1,
			NodeID:    c.nodeID,
		}
	case c.last.Timestamp > remoteTS:
		c.last = HLC{
			Timestamp: c.last.Timestamp,
			Counter:   c.last.Counter + 1,
			NodeID:    c.nodeID,
		}
	default:
		c.last = HLC{
			Timestamp: remoteTS,
			Counter:   remote.Counter + 1,
			NodeID:    c.nodeID,
		}
	}
}

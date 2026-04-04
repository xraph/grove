package crdt

import (
	"sync/atomic"
	"time"
)

// Metrics collects CRDT operational metrics. It uses atomic counters
// for lock-free concurrent access. Register a MetricsCollector to
// receive periodic snapshots, or read counters directly.
type Metrics struct {
	// Sync metrics.
	PullCount     atomic.Int64
	PushCount     atomic.Int64
	PullLatencyNs atomic.Int64 // last pull latency in nanoseconds
	PushLatencyNs atomic.Int64 // last push latency in nanoseconds
	PullErrors    atomic.Int64
	PushErrors    atomic.Int64
	ChangesPulled atomic.Int64
	ChangesPushed atomic.Int64
	ChangesMerged atomic.Int64

	// Conflict metrics.
	ConflictsTotal atomic.Int64
	ConflictsLWW   atomic.Int64
	ConflictsSet   atomic.Int64
	ConflictsList  atomic.Int64

	// Presence metrics.
	ActiveRooms        atomic.Int64
	ActiveParticipants atomic.Int64
	PresenceUpdates    atomic.Int64

	// Connection metrics.
	SSEConnections atomic.Int64
	WSConnections  atomic.Int64

	// Validation metrics.
	ValidationErrors atomic.Int64
}

// MetricsSnapshot is a point-in-time copy of all metrics.
type MetricsSnapshot struct {
	Timestamp time.Time `json:"timestamp"`

	PullCount     int64 `json:"pull_count"`
	PushCount     int64 `json:"push_count"`
	PullLatencyMs int64 `json:"pull_latency_ms"`
	PushLatencyMs int64 `json:"push_latency_ms"`
	PullErrors    int64 `json:"pull_errors"`
	PushErrors    int64 `json:"push_errors"`
	ChangesPulled int64 `json:"changes_pulled"`
	ChangesPushed int64 `json:"changes_pushed"`
	ChangesMerged int64 `json:"changes_merged"`

	ConflictsTotal int64 `json:"conflicts_total"`
	ConflictsLWW   int64 `json:"conflicts_lww"`
	ConflictsSet   int64 `json:"conflicts_set"`
	ConflictsList  int64 `json:"conflicts_list"`

	ActiveRooms        int64 `json:"active_rooms"`
	ActiveParticipants int64 `json:"active_participants"`
	PresenceUpdates    int64 `json:"presence_updates"`

	SSEConnections   int64 `json:"sse_connections"`
	WSConnections    int64 `json:"ws_connections"`
	ValidationErrors int64 `json:"validation_errors"`
}

// Snapshot returns a point-in-time copy of all metrics.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Timestamp:          time.Now(),
		PullCount:          m.PullCount.Load(),
		PushCount:          m.PushCount.Load(),
		PullLatencyMs:      m.PullLatencyNs.Load() / int64(time.Millisecond),
		PushLatencyMs:      m.PushLatencyNs.Load() / int64(time.Millisecond),
		PullErrors:         m.PullErrors.Load(),
		PushErrors:         m.PushErrors.Load(),
		ChangesPulled:      m.ChangesPulled.Load(),
		ChangesPushed:      m.ChangesPushed.Load(),
		ChangesMerged:      m.ChangesMerged.Load(),
		ConflictsTotal:     m.ConflictsTotal.Load(),
		ConflictsLWW:       m.ConflictsLWW.Load(),
		ConflictsSet:       m.ConflictsSet.Load(),
		ConflictsList:      m.ConflictsList.Load(),
		ActiveRooms:        m.ActiveRooms.Load(),
		ActiveParticipants: m.ActiveParticipants.Load(),
		PresenceUpdates:    m.PresenceUpdates.Load(),
		SSEConnections:     m.SSEConnections.Load(),
		WSConnections:      m.WSConnections.Load(),
		ValidationErrors:   m.ValidationErrors.Load(),
	}
}

// Reset zeroes all counters.
func (m *Metrics) Reset() {
	m.PullCount.Store(0)
	m.PushCount.Store(0)
	m.PullLatencyNs.Store(0)
	m.PushLatencyNs.Store(0)
	m.PullErrors.Store(0)
	m.PushErrors.Store(0)
	m.ChangesPulled.Store(0)
	m.ChangesPushed.Store(0)
	m.ChangesMerged.Store(0)
	m.ConflictsTotal.Store(0)
	m.ConflictsLWW.Store(0)
	m.ConflictsSet.Store(0)
	m.ConflictsList.Store(0)
	m.ActiveRooms.Store(0)
	m.ActiveParticipants.Store(0)
	m.PresenceUpdates.Store(0)
	m.SSEConnections.Store(0)
	m.WSConnections.Store(0)
	m.ValidationErrors.Store(0)
}

// RecordConflict increments the conflict counter for the given CRDT type.
func (m *Metrics) RecordConflict(crdtType CRDTType) {
	m.ConflictsTotal.Add(1)
	switch crdtType {
	case TypeLWW:
		m.ConflictsLWW.Add(1)
	case TypeSet:
		m.ConflictsSet.Add(1)
	case TypeList:
		m.ConflictsList.Add(1)
	}
}

// WithMetrics enables metrics collection on the sync controller.
func WithMetrics() SyncControllerOption {
	return func(c *SyncController) {
		c.metrics = &Metrics{}
	}
}

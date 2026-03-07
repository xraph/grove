package crdt

import (
	"encoding/json"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"
)

// PresenceManager manages ephemeral presence state for connected clients.
// State is kept entirely in-memory (never persisted to the database) and
// cleaned up automatically via TTL expiry. It is safe for concurrent use.
type PresenceManager struct {
	mu       sync.RWMutex
	states   map[string]map[string]*PresenceState // topic → nodeID → state
	ttl      time.Duration
	onChange func(event PresenceEvent)
	logger   log.Logger
	done     chan struct{}
}

// NewPresenceManager creates a presence manager with the given TTL and
// change callback. The callback is invoked on join, update, and leave
// events (including TTL-based expiry). It starts a background goroutine
// for TTL cleanup; call Close() to stop it.
func NewPresenceManager(ttl time.Duration, onChange func(PresenceEvent), logger log.Logger) *PresenceManager {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	pm := &PresenceManager{
		states:   make(map[string]map[string]*PresenceState),
		ttl:      ttl,
		onChange: onChange,
		logger:   logger,
		done:     make(chan struct{}),
	}
	go pm.cleanupLoop()
	return pm
}

// Update upserts a presence entry for the given node and topic. Returns
// the resulting event ("join" for new entries, "update" for existing).
func (pm *PresenceManager) Update(update PresenceUpdate) PresenceEvent {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()
	topic := update.Topic
	nodeID := update.NodeID

	topicMap, ok := pm.states[topic]
	if !ok {
		topicMap = make(map[string]*PresenceState)
		pm.states[topic] = topicMap
	}

	eventType := PresenceJoin
	if _, exists := topicMap[nodeID]; exists {
		eventType = PresenceUpdateEvt
	}

	topicMap[nodeID] = &PresenceState{
		NodeID:    nodeID,
		Topic:     topic,
		Data:      update.Data,
		UpdatedAt: now,
		ExpiresAt: now.Add(pm.ttl),
	}

	event := PresenceEvent{
		Type:   eventType,
		NodeID: nodeID,
		Topic:  topic,
		Data:   update.Data,
	}

	if pm.onChange != nil {
		pm.onChange(event)
	}

	return event
}

// Remove explicitly removes a node's presence from a topic. Returns a
// "leave" event. No-op if the entry doesn't exist.
func (pm *PresenceManager) Remove(topic, nodeID string) *PresenceEvent {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.removeLocked(topic, nodeID)
}

// RemoveNode removes all presence entries for the given node (e.g., on
// SSE disconnect). Broadcasts a "leave" event for each removed entry.
func (pm *PresenceManager) RemoveNode(nodeID string) []PresenceEvent {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var events []PresenceEvent
	for topic := range pm.states {
		if ev := pm.removeLocked(topic, nodeID); ev != nil {
			events = append(events, *ev)
		}
	}
	return events
}

// Get returns all active presence states for a topic.
func (pm *PresenceManager) Get(topic string) []PresenceState {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	topicMap, ok := pm.states[topic]
	if !ok {
		return nil
	}

	result := make([]PresenceState, 0, len(topicMap))
	for _, state := range topicMap {
		result = append(result, *state)
	}
	return result
}

// GetTopicsForNode returns all topics that a node has presence in.
func (pm *PresenceManager) GetTopicsForNode(nodeID string) []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var topics []string
	for topic, topicMap := range pm.states {
		if _, ok := topicMap[nodeID]; ok {
			topics = append(topics, topic)
		}
	}
	return topics
}

// Close stops the background cleanup goroutine.
func (pm *PresenceManager) Close() {
	close(pm.done)
}

// removeLocked removes a single entry. Must be called with pm.mu held.
func (pm *PresenceManager) removeLocked(topic, nodeID string) *PresenceEvent {
	topicMap, ok := pm.states[topic]
	if !ok {
		return nil
	}
	if _, exists := topicMap[nodeID]; !exists {
		return nil
	}

	delete(topicMap, nodeID)
	if len(topicMap) == 0 {
		delete(pm.states, topic)
	}

	event := &PresenceEvent{
		Type:   PresenceLeave,
		NodeID: nodeID,
		Topic:  topic,
	}

	if pm.onChange != nil {
		pm.onChange(*event)
	}

	return event
}

// cleanupLoop runs periodically to remove expired presence entries.
func (pm *PresenceManager) cleanupLoop() {
	// Check at half the TTL interval for timely cleanup.
	interval := pm.ttl / 2
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.done:
			return
		case <-ticker.C:
			pm.cleanup()
		}
	}
}

// cleanup removes all expired entries and broadcasts leave events.
func (pm *PresenceManager) cleanup() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()

	for topic, topicMap := range pm.states {
		for nodeID, state := range topicMap {
			if now.After(state.ExpiresAt) {
				delete(topicMap, nodeID)

				pm.logger.Debug("presence expired",
					log.String("topic", topic),
					log.String("node_id", nodeID),
				)

				if pm.onChange != nil {
					pm.onChange(PresenceEvent{
						Type:   PresenceLeave,
						NodeID: nodeID,
						Topic:  topic,
					})
				}
			}
		}
		if len(topicMap) == 0 {
			delete(pm.states, topic)
		}
	}
}

// MarshalEvent serializes a PresenceEvent to JSON bytes for SSE transport.
func MarshalPresenceEvent(event PresenceEvent) ([]byte, error) {
	return json.Marshal(event)
}

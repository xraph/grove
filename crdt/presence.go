package crdt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/xraph/go-utils/log"
)

// PresenceManager manages ephemeral presence state for connected clients.
// State is kept entirely in-memory (never persisted to the database) and
// cleaned up automatically via TTL expiry. It is safe for concurrent use.
type PresenceManager struct {
	mu        sync.RWMutex
	states    map[string]map[string]*PresenceState // topic → nodeID → state
	ttl       time.Duration
	onChange  func(event PresenceEvent)
	logger    log.Logger
	done      chan struct{}
	closeOnce sync.Once
	events    []PresenceEvent
	notifying bool
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
	event := pm.updateLocked(update)
	pm.mu.Unlock()
	pm.notify()
	return event
}

// Patch atomically merges an object into a participant's presence data.
// Only supplied top-level keys change; null values are retained. It creates
// an entry when absent and rejects non-object payloads without changing state.
func (pm *PresenceManager) Patch(update PresenceUpdate) (PresenceEvent, error) {
	event, err := pm.patch(update)
	if err == nil {
		pm.notify()
	}
	return event, err
}

// patch queues notification, leaving delivery to the caller after its locks are released.
func (pm *PresenceManager) patch(update PresenceUpdate) (PresenceEvent, error) {
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(update.Data, &patch); err != nil {
		return PresenceEvent{}, fmt.Errorf("crdt: decode presence patch: %w", err)
	}
	if patch == nil {
		return PresenceEvent{}, fmt.Errorf("crdt: presence patch must be an object")
	}
	pm.mu.Lock()
	data := make(map[string]json.RawMessage)
	if state := pm.states[update.Topic][update.NodeID]; state != nil && time.Now().Before(state.ExpiresAt) {
		if err := json.Unmarshal(state.Data, &data); err != nil {
			pm.mu.Unlock()
			return PresenceEvent{}, fmt.Errorf("crdt: decode existing presence: %w", err)
		}
	}
	if data == nil {
		data = make(map[string]json.RawMessage)
	}
	for key, value := range patch {
		data[key] = value
	}
	raw, err := json.Marshal(data)
	if err != nil {
		pm.mu.Unlock()
		return PresenceEvent{}, fmt.Errorf("crdt: encode presence patch: %w", err)
	}
	update.Data = raw
	event := pm.updateLocked(update)
	pm.mu.Unlock()
	return event, nil
}

func (pm *PresenceManager) updateLocked(update PresenceUpdate) PresenceEvent {
	now := time.Now()
	topic := update.Topic
	nodeID := update.NodeID

	topicMap, ok := pm.states[topic]
	if !ok {
		topicMap = make(map[string]*PresenceState)
		pm.states[topic] = topicMap
	}

	eventType := PresenceJoin
	if state := topicMap[nodeID]; state != nil && now.Before(state.ExpiresAt) {
		eventType = PresenceUpdateEvt
	}

	topicMap[nodeID] = &PresenceState{
		NodeID:    nodeID,
		Topic:     topic,
		Data:      bytes.Clone(update.Data),
		UpdatedAt: now,
		ExpiresAt: now.Add(pm.ttl),
	}

	event := PresenceEvent{
		Type:   eventType,
		NodeID: nodeID,
		Topic:  topic,
		Data:   bytes.Clone(update.Data),
	}
	pm.enqueueLocked(event)

	return event
}

// Remove explicitly removes a node's presence from a topic. Returns a
// "leave" event. No-op if the entry doesn't exist.
func (pm *PresenceManager) Remove(topic, nodeID string) *PresenceEvent {
	pm.mu.Lock()
	event := pm.removeLocked(topic, nodeID)
	pm.mu.Unlock()
	pm.notify()
	return event
}

// RemoveNode removes all presence entries for the given node (e.g., on
// SSE disconnect). Broadcasts a "leave" event for each removed entry.
func (pm *PresenceManager) RemoveNode(nodeID string) []PresenceEvent {
	pm.mu.Lock()

	var events []PresenceEvent
	for topic := range pm.states {
		if ev := pm.removeLocked(topic, nodeID); ev != nil {
			events = append(events, *ev)
		}
	}
	pm.mu.Unlock()
	pm.notify()
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
	now := time.Now()
	for _, state := range topicMap {
		if now.Before(state.ExpiresAt) {
			copyState := *state
			copyState.Data = bytes.Clone(state.Data)
			result = append(result, copyState)
		}
	}
	return result
}

// GetTopicsForNode returns all topics that a node has presence in.
func (pm *PresenceManager) GetTopicsForNode(nodeID string) []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var topics []string
	for topic, topicMap := range pm.states {
		if state := topicMap[nodeID]; state != nil && time.Now().Before(state.ExpiresAt) {
			topics = append(topics, topic)
		}
	}
	return topics
}

// Close stops the background cleanup goroutine.
func (pm *PresenceManager) Close() {
	pm.closeOnce.Do(func() { close(pm.done) })
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

	pm.enqueueLocked(*event)

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

	now := time.Now()

	for topic, topicMap := range pm.states {
		for nodeID, state := range topicMap {
			if now.After(state.ExpiresAt) {
				delete(topicMap, nodeID)

				pm.logger.Debug("presence expired",
					log.String("topic", topic),
					log.String("node_id", nodeID),
				)

				pm.enqueueLocked(PresenceEvent{
					Type:   PresenceLeave,
					NodeID: nodeID,
					Topic:  topic,
				})
			}
		}
		if len(topicMap) == 0 {
			delete(pm.states, topic)
		}
	}
	pm.mu.Unlock()
	pm.notify()
}

func (pm *PresenceManager) enqueueLocked(event PresenceEvent) {
	if pm.onChange != nil {
		event.Data = bytes.Clone(event.Data)
		pm.events = append(pm.events, event)
	}
}

// Drain callbacks in mutation order, outside the state lock. A callback may
// read or update presence; reentrant updates are drained by the outer caller.
func (pm *PresenceManager) notify() {
	pm.mu.Lock()
	if pm.notifying {
		pm.mu.Unlock()
		return
	}
	pm.notifying = true
	completed := false
	defer func() {
		// A caller may recover a callback panic. Keep future notifications
		// usable in that case; callbacks always execute with mu unlocked.
		if !completed {
			pm.mu.Lock()
			pm.notifying = false
			pm.mu.Unlock()
		}
	}()
	for len(pm.events) > 0 {
		event := pm.events[0]
		pm.events[0] = PresenceEvent{}
		pm.events = pm.events[1:]
		pm.mu.Unlock()
		pm.onChange(event)
		pm.mu.Lock()
	}
	pm.events = nil
	pm.notifying = false
	completed = true
	pm.mu.Unlock()
}

// MarshalEvent serializes a PresenceEvent to JSON bytes for SSE transport.
func MarshalPresenceEvent(event PresenceEvent) ([]byte, error) {
	return json.Marshal(event)
}

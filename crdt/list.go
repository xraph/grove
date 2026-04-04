package crdt

import (
	"encoding/json"
	"sort"
)

// RGANode is a single element in the RGA (Replicated Growable Array).
// Each node has a unique ID (NodeID + HLC), an optional parent reference
// for causal ordering, and a tombstone flag for deletions.
type RGANode struct {
	// ID uniquely identifies this node in the list.
	ID HLC `json:"id"`
	// NodeID is the node that created this element.
	NodeID string `json:"node_id"`
	// ParentID references the node after which this element was inserted.
	// Zero HLC means this is inserted at the head of the list.
	ParentID HLC `json:"parent_id"`
	// Value is the JSON-encoded element value.
	Value json.RawMessage `json:"value"`
	// Tombstone marks this node as deleted.
	Tombstone bool `json:"tombstone,omitempty"`
}

// RGAListState holds the full state of an RGA list.
type RGAListState struct {
	// Nodes stores all nodes (including tombstoned) keyed by their ID string.
	Nodes map[string]*RGANode `json:"nodes"`
	// Order caches the sorted node ID keys for iteration.
	// Recomputed on merge; not persisted.
	order []string `json:"-"`
}

// NewRGAListState creates an empty RGA list.
func NewRGAListState() *RGAListState {
	return &RGAListState{
		Nodes: make(map[string]*RGANode),
	}
}

// rgaNodeKey returns a deterministic string key for an RGA node ID.
func rgaNodeKey(id HLC) string {
	return id.String()
}

// Insert adds a new element after the given parentID.
// If parentID is zero, the element is prepended to the list.
func (l *RGAListState) Insert(value any, parentID HLC, nodeID string, clock HLC) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	node := &RGANode{
		ID:       clock,
		NodeID:   nodeID,
		ParentID: parentID,
		Value:    raw,
	}
	l.Nodes[rgaNodeKey(clock)] = node
	l.order = nil // Invalidate cache.
	return nil
}

// Delete marks the node with the given ID as tombstoned.
func (l *RGAListState) Delete(id HLC) {
	key := rgaNodeKey(id)
	if node, ok := l.Nodes[key]; ok {
		node.Tombstone = true
	}
	l.order = nil
}

// Move moves an element to a new position after the given parentID.
// Implemented as tombstone + re-insert with new ID.
func (l *RGAListState) Move(id, newParentID HLC, nodeID string, clock HLC) {
	key := rgaNodeKey(id)
	if node, ok := l.Nodes[key]; ok {
		// Tombstone old position.
		node.Tombstone = true
		// Re-insert at new position with same value but new ID.
		newNode := &RGANode{
			ID:       clock,
			NodeID:   nodeID,
			ParentID: newParentID,
			Value:    node.Value,
		}
		l.Nodes[rgaNodeKey(clock)] = newNode
	}
	l.order = nil
}

// Elements returns the visible (non-tombstoned) elements in order.
func (l *RGAListState) Elements() []json.RawMessage {
	ordered := l.sortedNodes()
	var result []json.RawMessage
	for _, node := range ordered {
		if !node.Tombstone {
			result = append(result, node.Value)
		}
	}
	return result
}

// Len returns the number of visible (non-tombstoned) elements.
func (l *RGAListState) Len() int {
	count := 0
	for _, node := range l.Nodes {
		if !node.Tombstone {
			count++
		}
	}
	return count
}

// NodeIDs returns the IDs of visible elements in order.
// Useful for addressing specific positions.
func (l *RGAListState) NodeIDs() []HLC {
	ordered := l.sortedNodes()
	var result []HLC
	for _, node := range ordered {
		if !node.Tombstone {
			result = append(result, node.ID)
		}
	}
	return result
}

// sortedNodes returns all nodes sorted by the RGA ordering algorithm.
// RGA ordering: nodes are sorted topologically by parent chains, with
// siblings (same parent) sorted by HLC descending (newer inserts first
// among concurrent operations at the same position).
func (l *RGAListState) sortedNodes() []*RGANode {
	if len(l.Nodes) == 0 {
		return nil
	}

	// Build children map: parentKey → []*RGANode.
	children := make(map[string][]*RGANode)
	zeroKey := rgaNodeKey(HLC{})
	for _, node := range l.Nodes {
		parentKey := rgaNodeKey(node.ParentID)
		children[parentKey] = append(children[parentKey], node)
	}

	// Sort children at each level: higher HLC first (newer concurrent inserts appear first).
	for key := range children {
		c := children[key]
		sort.Slice(c, func(i, j int) bool {
			return c[i].ID.After(c[j].ID)
		})
	}

	// DFS traversal starting from root (zero parent).
	var result []*RGANode
	var dfs func(parentKey string)
	dfs = func(parentKey string) {
		for _, child := range children[parentKey] {
			result = append(result, child)
			dfs(rgaNodeKey(child.ID))
		}
	}
	dfs(zeroKey)

	return result
}

// MergeList merges two RGA list states. The result contains all nodes from
// both lists with tombstones preserved. This is commutative, associative,
// and idempotent.
func MergeList(local, remote *RGAListState) *RGAListState {
	if local == nil {
		return remote
	}
	if remote == nil {
		return local
	}

	merged := NewRGAListState()

	// Copy all local nodes.
	for key, node := range local.Nodes {
		cp := *node
		merged.Nodes[key] = &cp
	}

	// Merge remote nodes.
	for key, remoteNode := range remote.Nodes {
		if localNode, exists := merged.Nodes[key]; exists {
			// Both have the same node. Tombstone wins (if either is tombstoned).
			if remoteNode.Tombstone {
				localNode.Tombstone = true
			}
		} else {
			cp := *remoteNode
			merged.Nodes[key] = &cp
		}
	}

	return merged
}

// ListOp represents a list operation for the sync transport.
type ListOp struct {
	Op       ListOpType      `json:"op"`
	NodeID   HLC             `json:"node_id,omitempty"`
	ParentID HLC             `json:"parent_id,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
}

// ListOpType identifies the list operation.
type ListOpType string

const (
	ListOpInsert ListOpType = "insert"
	ListOpDelete ListOpType = "delete"
	ListOpMove   ListOpType = "move"
)

// ToFieldState converts to the generic FieldState representation.
func (l *RGAListState) ToFieldState(clock HLC, nodeID string) *FieldState {
	elements := l.Elements()
	raw, err := json.Marshal(elements)
	if err != nil {
		return nil
	}
	return &FieldState{
		Type:      TypeList,
		HLC:       clock,
		NodeID:    nodeID,
		Value:     raw,
		ListState: l,
	}
}

// ListFromFieldState reconstructs an RGAListState from a FieldState.
func ListFromFieldState(fs *FieldState) *RGAListState {
	if fs == nil || fs.Type != TypeList {
		return nil
	}
	if fs.ListState == nil {
		return NewRGAListState()
	}
	return fs.ListState
}

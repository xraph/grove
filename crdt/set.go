package crdt

import (
	"encoding/json"
	"sort"
	"strings"
)

// ORSetState holds the state for an Observed-Remove Set with add-wins
// semantics. Each element is tracked with a unique tag (nodeID + HLC) so
// that concurrent add and remove of the same element resolves to the element
// being present (the add wins).
type ORSetState struct {
	// Entries maps element (JSON-encoded) → set of tags that added it.
	// An element is in the set if it has at least one tag not in Removed.
	Entries map[string][]Tag `json:"entries"`

	// Removed tracks tags that have been observed and removed.
	Removed map[string]bool `json:"removed"`
}

// Tag uniquely identifies an add operation.
type Tag struct {
	NodeID string `json:"node"`
	HLC    HLC    `json:"hlc"`
}

// tagKey returns a deterministic string key for a Tag.
func tagKey(t Tag) string {
	return t.NodeID + ":" + t.HLC.String()
}

// removedKey scopes a removal to one element. Tags are per-ADD, not
// per-(element, add) — a multi-element add shares one tag — so removals
// keyed by tag alone would leak across elements added together.
func removedKey(elem string, t Tag) string {
	return elem + "|" + tagKey(t)
}

// tagRemoved reports whether elem's tag is removed, honoring both the
// element-scoped key and the legacy tag-only key (states written before
// element scoping).
func (s *ORSetState) tagRemoved(elem string, t Tag) bool {
	return s.Removed[removedKey(elem, t)] || s.Removed[tagKey(t)]
}

// NewORSetState creates an empty OR-Set.
func NewORSetState() *ORSetState {
	return &ORSetState{
		Entries: make(map[string][]Tag),
		Removed: make(map[string]bool),
	}
}

// Add inserts an element into the set with the given tag.
func (s *ORSetState) Add(element any, nodeID string, clock HLC) error {
	key, err := marshalElement(element)
	if err != nil {
		return err
	}
	tag := Tag{NodeID: nodeID, HLC: clock}
	s.Entries[key] = append(s.Entries[key], tag)
	return nil
}

// Remove removes an element by marking all its current tags as removed.
func (s *ORSetState) Remove(element any) error {
	key, err := marshalElement(element)
	if err != nil {
		return err
	}
	for _, tag := range s.Entries[key] {
		s.Removed[removedKey(key, tag)] = true
	}
	return nil
}

// Elements returns the effective set of elements (those with at least one
// non-removed tag).
func (s *ORSetState) Elements() []json.RawMessage {
	var result []json.RawMessage
	keys := make([]string, 0, len(s.Entries))
	for k := range s.Entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		tags := s.Entries[key]
		if s.hasActiveTags(key, tags) {
			result = append(result, json.RawMessage(key))
		}
	}
	return result
}

// Contains returns true if the element is in the effective set.
func (s *ORSetState) Contains(element any) (bool, error) {
	key, err := marshalElement(element)
	if err != nil {
		return false, err
	}
	tags, ok := s.Entries[key]
	if !ok {
		return false, nil
	}
	return s.hasActiveTags(key, tags), nil
}

func (s *ORSetState) hasActiveTags(elem string, tags []Tag) bool {
	for _, tag := range tags {
		if !s.tagRemoved(elem, tag) {
			return true
		}
	}
	return false
}

// MergeSet merges two OR-Set states. The result is the union of all entries
// with the union of all removed tags. This is commutative, associative, and
// idempotent.
func MergeSet(local, remote *ORSetState) *ORSetState {
	if local == nil {
		return canonicalORSet(remote)
	}
	if remote == nil {
		return canonicalORSet(local)
	}
	local, remote = canonicalORSet(local), canonicalORSet(remote)

	merged := NewORSetState()

	// Merge entries: union of all tags per element.
	for elem, tags := range local.Entries {
		merged.Entries[elem] = append(merged.Entries[elem], tags...)
	}
	for elem, tags := range remote.Entries {
		merged.Entries[elem] = append(merged.Entries[elem], tags...)
	}

	// Deduplicate tags per element.
	for elem, tags := range merged.Entries {
		merged.Entries[elem] = deduplicateTags(tags)
	}

	// Merge removed: union.
	for key, v := range local.Removed {
		if v {
			merged.Removed[key] = true
		}
	}
	for key, v := range remote.Removed {
		if v {
			merged.Removed[key] = true
		}
	}

	return merged
}

func deduplicateTags(tags []Tag) []Tag {
	seen := make(map[string]bool, len(tags))
	result := make([]Tag, 0, len(tags))
	for _, t := range tags {
		key := tagKey(t)
		if !seen[key] {
			seen[key] = true
			result = append(result, t)
		}
	}
	return result
}

func marshalElement(element any) (string, error) {
	b, err := json.Marshal(element)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ToFieldState converts to the generic FieldState representation.
func (s *ORSetState) ToFieldState(clock HLC, nodeID string) *FieldState {
	elements := s.Elements()
	raw, err := json.Marshal(elements)
	if err != nil {
		return nil
	}
	return &FieldState{
		Type:     TypeSet,
		HLC:      clock,
		NodeID:   nodeID,
		Value:    raw,
		SetState: s,
	}
}

// SetFromFieldState reconstructs an ORSetState from a FieldState.
func SetFromFieldState(fs *FieldState) *ORSetState {
	if fs == nil || fs.Type != TypeSet {
		return nil
	}
	if fs.SetState == nil {
		return NewORSetState()
	}
	return canonicalORSet(fs.SetState)
}

// canonicalORSet returns s with every element key in the form
// ORSetState.Add uses (json.Marshal bytes). States written before keys were
// canonical can hold an element under the raw bytes a client sent, such as
// "a<b" or an object with unsorted fields; left alone, that element would
// show up twice next to its canonical twin and a remove could miss it.
// Removed markers are copied to the new key so a removed element stays
// removed. Returns s itself when nothing needs re-keying.
func canonicalORSet(s *ORSetState) *ORSetState {
	if s == nil {
		return nil
	}
	var rekey map[string]string
	for k := range s.Entries {
		if !mayNeedRekey(k) {
			continue
		}
		c, err := canonicalElementKey(json.RawMessage(k))
		if err != nil || c == k {
			continue
		}
		if rekey == nil {
			rekey = make(map[string]string)
		}
		rekey[k] = c
	}
	if rekey == nil {
		return s
	}

	out := NewORSetState()
	for k, tags := range s.Entries {
		if c, ok := rekey[k]; ok {
			k = c
		}
		out.Entries[k] = append(out.Entries[k], tags...)
	}
	for k, tags := range out.Entries {
		out.Entries[k] = deduplicateTags(tags)
	}
	for k, v := range s.Removed {
		if v {
			out.Removed[k] = true
		}
	}
	for old, c := range rekey {
		for _, t := range s.Entries[old] {
			if s.Removed[removedKey(old, t)] {
				out.Removed[removedKey(c, t)] = true
			}
		}
	}
	return out
}

// mayNeedRekey is a cheap filter for canonicalORSet: json.Marshal output
// never holds a raw <, > or &, U+2028 or U+2029, and only objects can have
// their fields out of order.
func mayNeedRekey(key string) bool {
	return strings.ContainsFunc(key, func(r rune) bool {
		switch r {
		case '<', '>', '&', '{', 0x2028, 0x2029:
			return true
		}
		return false
	})
}
